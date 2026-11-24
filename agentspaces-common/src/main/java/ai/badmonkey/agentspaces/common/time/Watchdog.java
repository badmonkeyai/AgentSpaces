/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.badmonkey.agentspaces.common.time;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * The redundant liveness check of every node loop, flush timer and timer thread in a process
 * (srad/agentspaces-event-loop.md §3.9, EL-H-01 to EL-H-15). It finds a loop that does not run at all, which the loop's
 * own backstop cannot find, and re-initialises it without losing queued work.
 *
 * <p>Three modes, one set of checks:
 * <ul>
 *   <li><b>Threaded</b> ({@link #system()}, one per process): a platform daemon thread {@code agentspaces-watchdog},
 *       allowlisted (§3.8), started with the first registration and stopped with the last. It parks for the period
 *       and re-arms from the end of each check (EL-T-13), reads monotonic time from its time source, and never
 *       schedules through the timer thread, so a dead timer or saturated carriers cannot stop it.</li>
 *   <li><b>Manual</b> ({@link #manual}): no thread; {@code VirtualTime} calls {@link #tick()} each time its advance
 *       crosses {@link #nextTickNanos()}, so the watchdog judges deadlines in virtual time (WD-7).</li>
 *   <li><b>None</b> ({@link #none()}): registrations are accepted and never judged, for a time source with no
 *       watchdog of its own.</li>
 * </ul>
 *
 * <p>The checks on each tick, per watched thing (§3.9): dead (the thread of a run marked started is not alive),
 * wedged (a run past the wedge bound: re-initialise, fence the participant), slow (a run past the slow bound: counted
 * once per run, named), missed wake (a wake pending past the grace with nothing running or dispatched), stuck dispatch
 * (a dispatched drain that never started, for the wedge bound), and missed fire (no run since the last tick and the
 * armed deadline plus the grace passed). The timer thread is judged first; when it is restarted the loops' missed
 * fires are not judged on that tick, because the restart dispatches their deadlines.
 *
 * <p>Invariants: WD-1 (no protocol work, no lock a participant can hold: every read is of volatile state), WD-5
 * (three re-initialisations of one thing within the window, or a fenced participant past the fence limit, escalates
 * once and never loops silently), WD-6 (the timer thread restarts a starved threaded watchdog; its uncaught-exception
 * handler restarts it after a throw; both counted).
 */
public final class Watchdog {

    private static final System.Logger LOG = System.getLogger(Watchdog.class.getName());

    /** The watchdog's bounds (decision 4, EL-H-03; properties per EL-Q-MNT-02). */
    public record Options(Duration period, Duration grace, Duration slow, Duration wedge, int escalateAfter,
                          Duration escalateWindow, Duration fenceLimit) {

        /** Checks the bounds. */
        public Options {
            Objects.requireNonNull(period, "period");
            Objects.requireNonNull(grace, "grace");
            Objects.requireNonNull(slow, "slow");
            Objects.requireNonNull(wedge, "wedge");
            Objects.requireNonNull(escalateWindow, "escalateWindow");
            Objects.requireNonNull(fenceLimit, "fenceLimit");
            if (period.isNegative() || period.isZero()) {
                throw new IllegalArgumentException("the watchdog period must be positive: " + period);
            }
            if (escalateAfter < 1) {
                throw new IllegalArgumentException("escalateAfter must be at least 1: " + escalateAfter);
            }
        }

        /**
         * Decision 4 (a): a 2 s period, a 250 ms grace, the 100 ms slow bound (EL-X-10), the 5 s wedge bound, three
         * re-initialisations in 10 minutes, and a fenced participant 2 minutes.
         *
         * @return the defaults
         */
        public static Options defaults() {
            return new Options(Duration.ofSeconds(2), Duration.ofMillis(250), Duration.ofMillis(100),
                    Duration.ofSeconds(5), 3, Duration.ofMinutes(10), Duration.ofMinutes(2));
        }

        /**
         * The defaults, each overridden by a system property when set: {@code agentspaces.watchdog.period-millis},
         * {@code .grace-millis}, {@code .slow-millis}, {@code .wedge-millis}, {@code .escalate-after},
         * {@code .escalate-window-millis}, {@code .fence-limit-millis}.
         *
         * @return the options
         */
        public static Options fromSystemProperties() {
            Options d = defaults();
            return new Options(millis("period-millis", d.period), millis("grace-millis", d.grace),
                    millis("slow-millis", d.slow), millis("wedge-millis", d.wedge),
                    Integer.getInteger("agentspaces.watchdog.escalate-after", d.escalateAfter),
                    millis("escalate-window-millis", d.escalateWindow), millis("fence-limit-millis", d.fenceLimit));
        }

        /**
         * These options with another period.
         *
         * @param p the period
         * @return the options
         */
        public Options withPeriod(Duration p) {
            return new Options(p, grace, slow, wedge, escalateAfter, escalateWindow, fenceLimit);
        }

        /**
         * These options with another wedge bound.
         *
         * @param w the bound
         * @return the options
         */
        public Options withWedge(Duration w) {
            return new Options(period, grace, slow, w, escalateAfter, escalateWindow, fenceLimit);
        }

        /**
         * These options with another fence limit.
         *
         * @param f the limit
         * @return the options
         */
        public Options withFenceLimit(Duration f) {
            return new Options(period, grace, slow, wedge, escalateAfter, escalateWindow, f);
        }

        private static Duration millis(String key, Duration fallback) {
            Long v = Long.getLong("agentspaces.watchdog." + key);
            return v == null ? fallback : Duration.ofMillis(v);
        }
    }

    /**
     * One finding: a fault that led to a re-initialisation or a restart, or an escalation.
     *
     * @param kind        the check's id ({@link Watched.Reason#id()}, {@code timer-restart}, {@code escalation})
     * @param name        the watched thing's name
     * @param participant the participant running at the time, or ""
     * @param atNanos     when, on the time source's monotonic base
     */
    public record Detection(String kind, String name, String participant, long atNanos) {
    }

    /**
     * What the watchdog did since it was made.
     *
     * @param ticks             checks run
     * @param detections        faults found that led to a re-initialisation (slow runs are not detections)
     * @param detectionsByKind  the same by check
     * @param reinits           re-initialisations
     * @param slowRuns          runs found past the slow bound while still running, once per run
     * @param slowRunsBy        the same by participant name
     * @param slowOperations    other slow operations reported to it (a virtual-time task, EL-H-10)
     * @param escalations       escalations (WD-5)
     * @param restartsStarved   restarts of this watchdog's thread by the timer thread (WD-6)
     * @param restartsThrown    restarts of this watchdog's thread after a check threw (WD-6)
     * @param lateTicks         threaded checks that began more than the grace after their park ended: the process (or
     *                          this thread) was not running, so ages are judged from the check's own start
     *                          (srad/agentspaces-event-loop.md §10.10)
     */
    public record Stats(long ticks, long detections, Map<String, Long> detectionsByKind, long reinits,
                        long slowRuns, Map<String, Long> slowRunsBy, long slowOperations, long escalations,
                        long restartsStarved, long restartsThrown, long lateTicks) {
    }

    /**
     * A Raft leader the watchdog checks on each tick (srad/agentspaces-event-loop.md EL-H-09, EL-SP-24): out of the
     * loop, so a leader whose loop is wedged or whose timer was lost is still found. Every read must be lock-free
     * (WD-1); every value is on the time source's monotonic base.
     */
    public interface LeaseHolder {

        /**
         * A stable name for logs and events.
         *
         * @return the name
         */
        String name();

        /**
         * Whether it believes it leads now.
         *
         * @return whether leading
         */
        boolean leading();

        /**
         * When its current lease advertisement was issued.
         *
         * @return the issue instant
         */
        long leaseIssuedNanos();

        /**
         * The span of its advertised lease; 0 when none was advertised.
         *
         * @return the span
         */
        long leaseSpanNanos();

        /**
         * When its next heartbeat is due, or {@link Long#MAX_VALUE} when none is armed.
         *
         * @return the deadline
         */
        long heartbeatDueNanos();

        /**
         * Its minimum election timeout: a leader whose heartbeat is this late may already have been replaced.
         *
         * @return the span
         */
        long minElectionTimeoutNanos();

        /** Republishes its lease advertisement now (its participant is asked to run). */
        void forceRefresh();

        /** Steps down to follower. Called on a virtual thread of its own, never the watchdog's. */
        void stepDown();
    }

    private final List<LeaseHolder> leases = new CopyOnWriteArrayList<>();

    /**
     * An operation that holds a leased entry while it runs, renewing the lease (an {@code @OrderedTake} handler, a
     * connector's held operation; srad/agentspaces-event-loop.md §10.16, owner decision 2 of 2026-11-22). The watchdog
     * reports one held past {@link #heldReport()} by name, again each time its hold doubles, so a hung handler of a
     * still-bound agent is visible while it keeps its entry.
     *
     * @param agent      the agent or connector
     * @param method     the handler or operation
     * @param entry      the entry it holds
     * @param sinceNanos when it took the entry, on this watchdog's monotonic base
     */
    public record Held(String agent, String method, String entry, long sinceNanos) {
        /** Checks the fields. */
        public Held {
            Objects.requireNonNull(agent, "agent");
            Objects.requireNonNull(method, "method");
            Objects.requireNonNull(entry, "entry");
        }
    }

    /** The default report bound of a held operation; {@code agentspaces.watchdog.held-report-millis} sets another. */
    public static final Duration DEFAULT_HELD_REPORT = Duration.ofMinutes(1);

    private static final class HeldEntry {
        final Held held;
        long nextReportNanos;

        HeldEntry(Held held, long firstReportNanos) {
            this.held = held;
            this.nextReportNanos = firstReportNanos;
        }
    }

    private final List<HeldEntry> held = new CopyOnWriteArrayList<>();
    private final AtomicLong heldReports = new AtomicLong();
    private volatile Duration heldReport = Duration.ofMillis(Long.getLong("agentspaces.watchdog.held-report-millis",
            DEFAULT_HELD_REPORT.toMillis()));

    /**
     * Watches an operation that holds a leased entry (see {@link Held}) until the registration is closed.
     *
     * @param operation the operation
     * @return its registration; close it when the operation returns
     */
    public Registration registerHeld(Held operation) {
        Objects.requireNonNull(operation, "operation");
        if (mode == Mode.NONE) {
            return () -> { };
        }
        HeldEntry entry = new HeldEntry(operation, heldReport.toNanos());
        held.add(entry);
        return () -> held.remove(entry);
    }

    /**
     * How long a held operation runs before the watchdog first reports it.
     *
     * @return the bound
     */
    public Duration heldReport() {
        return heldReport;
    }

    /**
     * Sets how long a held operation runs before the watchdog first reports it.
     *
     * @param bound the bound; positive
     * @return this watchdog
     */
    public Watchdog heldReport(Duration bound) {
        if (bound.isNegative() || bound.isZero()) {
            throw new IllegalArgumentException("the held-operation report bound must be positive: " + bound);
        }
        this.heldReport = bound;
        return this;
    }

    /**
     * Reports of held operations ({@code core.watchdog.held-reports}): not detections, which count this node's own
     * loop faults, but slow operations of application code.
     *
     * @return the count
     */
    public long heldReports() {
        return heldReports.get();
    }

    /** One held-operation check; reads only the record and its own report deadline. */
    private void judgeHeld(HeldEntry e, long now) {
        long heldFor = now - e.held.sinceNanos();
        if (heldFor < e.nextReportNanos) {
            return;
        }
        e.nextReportNanos = Math.max(heldFor, e.nextReportNanos) * 2; // again each time the hold doubles
        heldReports.incrementAndGet();
        slowOperations.incrementAndGet();
        WatchdogEvent.commit(e.held.agent(), "held-operation", e.held.method(), heldFor / 1_000_000);
        LOG.log(System.Logger.Level.WARNING, "watchdog: slow operation: " + e.held.agent() + "." + e.held.method()
                + " has held entry " + e.held.entry() + " for " + heldFor / 1_000_000 + " ms, renewing its lease;"
                + " another member takes the entry only when it returns, fails or its hold limit stops the renewal");
    }
    private final AtomicLong leaseForced = new AtomicLong();
    private final AtomicLong stepdownsForced = new AtomicLong();

    /**
     * Starts checking a Raft leader's lease on every tick (EL-H-09): when it leads and its lease advertisement is past
     * half its span plus the grace, or its heartbeat is unarmed or overdue by more than the grace, the watchdog
     * forces a republish ({@code core.raft.lease-forced}); when its heartbeat is overdue by its minimum election
     * timeout, it steps the leader down ({@code core.raft.stepdowns-forced}). The leader's own lease guard steps it
     * down sooner (owner answer 2, 2026-11-22); this is the redundant check.
     *
     * @param holder the leader
     * @return its registration
     */
    public Registration registerLease(LeaseHolder holder) {
        Objects.requireNonNull(holder, "holder");
        if (mode == Mode.NONE) {
            return () -> { };
        }
        leases.add(holder);
        return () -> leases.remove(holder);
    }

    /**
     * Lease republishes the watchdog forced ({@code core.raft.lease-forced}).
     *
     * @return the count
     */
    public long leaseForced() {
        return leaseForced.get();
    }

    /**
     * Leaders the watchdog stepped down ({@code core.raft.stepdowns-forced}).
     *
     * @return the count
     */
    public long stepdownsForced() {
        return stepdownsForced.get();
    }

    /** One lease check (EL-H-09); reads only lock-free values of the holder. */
    private void judgeLease(LeaseHolder h, long now, Options o) {
        if (!h.leading()) {
            return;
        }
        long grace = o.grace().toNanos();
        long heartbeat = h.heartbeatDueNanos();
        if (heartbeat != Long.MAX_VALUE && now - heartbeat >= h.minElectionTimeoutNanos()) {
            stepdownsForced.incrementAndGet();
            remember(new Detection("raft-stepdown", h.name(), "", now));
            WatchdogEvent.commit(h.name(), "raft-stepdown", "", 0);
            LOG.log(System.Logger.Level.WARNING, h.name() + ": the leader's heartbeat is "
                    + (now - heartbeat) / 1_000_000 + " ms overdue, at least its minimum election timeout; stepping"
                    + " it down");
            Thread.ofVirtual().name("watchdog-raft-stepdown").start(h::stepDown);
            return;
        }
        long span = h.leaseSpanNanos();
        boolean stale = span > 0 && now - h.leaseIssuedNanos() > span / 2 + grace;
        if (stale || heartbeat == Long.MAX_VALUE || now - heartbeat > grace) {
            leaseForced.incrementAndGet();
            remember(new Detection("raft-lease", h.name(), "", now));
            WatchdogEvent.commit(h.name(), "raft-lease", "", 0);
            h.forceRefresh();
        }
    }

    /** A watched thing's place; closing it stops the judging. */
    public interface Registration extends AutoCloseable {
        @Override
        void close();
    }

    private static final AtomicLong TOTAL_DETECTIONS = new AtomicLong();
    private static final AtomicLong TOTAL_REINITS = new AtomicLong();
    private static final AtomicLong TOTAL_REGISTRATIONS = new AtomicLong();
    private static final Watchdog NONE = new Watchdog(null, null, Options.defaults(), Mode.NONE);
    private static final int KEPT_DETECTIONS = 256;

    private enum Mode { THREADED, MANUAL, NONE }

    private final String threadName;
    private final TimeSource time;
    private final Mode mode;
    private volatile Options options;
    private final List<Entry> entries = new CopyOnWriteArrayList<>();
    private volatile Entry timerEntry;
    private final Object checkLock = new Object();
    private final AtomicLong ticks = new AtomicLong();
    private final AtomicLong detections = new AtomicLong();
    private final AtomicLong reinits = new AtomicLong();
    private final AtomicLong slowRuns = new AtomicLong();
    private final AtomicLong slowOperations = new AtomicLong();
    private final AtomicLong escalations = new AtomicLong();
    private final AtomicLong restartsStarved = new AtomicLong();
    private final AtomicLong restartsThrown = new AtomicLong();
    private final AtomicLong lateTicks = new AtomicLong();
    /**
     * The instant ages are judged from at the earliest: set by a threaded check that began late, after a pause of the
     * process. Guarded by {@link #checkLock}; {@link Long#MIN_VALUE} until the first late check.
     */
    private long judgeFrom = Long.MIN_VALUE;
    /** Whether the previous threaded check began late. Guarded by {@link #checkLock}. */
    private boolean lastLate;
    private final Map<String, AtomicLong> detectionsByKind = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> slowRunsBy = new ConcurrentHashMap<>();
    private final ArrayDeque<Detection> recent = new ArrayDeque<>();
    private volatile boolean scripted;
    private volatile Runnable beforeCheck;
    private volatile long nextTick = Long.MAX_VALUE;

    // Threaded mode; guarded by this.
    private Thread thread;
    private long threadGeneration;
    private volatile Thread liveThread;
    private volatile long heartbeat;

    private Watchdog(String threadName, TimeSource time, Options options, Mode mode) {
        this.threadName = threadName;
        this.time = time;
        this.options = Objects.requireNonNull(options, "options");
        this.mode = mode;
    }

    /**
     * The process's watchdog, over {@link TimeSource#system()}, with its bounds from system properties.
     *
     * @return the one per process
     */
    public static Watchdog system() {
        return SystemTime.INSTANCE.watchdog();
    }

    /**
     * A threaded watchdog over a {@link SystemTime}, which watches its timer thread and is watched by it (WD-6).
     * Production code uses {@link #system()}; tests make one over {@link SystemTime#isolated(String)}.
     *
     * @param threadName the watchdog thread's name
     * @param time       the time source
     * @param options    the bounds
     * @return the watchdog, attached to the source
     */
    public static Watchdog threaded(String threadName, SystemTime time, Options options) {
        Watchdog w = new Watchdog(Objects.requireNonNull(threadName, "threadName"), time, options, Mode.THREADED);
        time.attach(w);
        w.timerEntry = new Entry(time.timerView(), 0);
        return w;
    }

    /**
     * A watchdog with no thread: whoever drives the time source calls {@link #tick()} at {@link #nextTickNanos()}
     * ({@code VirtualTime} does, on each advance).
     *
     * @param time    the time source whose monotonic time it judges in
     * @param options the bounds
     * @return the watchdog
     */
    public static Watchdog manual(TimeSource time, Options options) {
        return new Watchdog(null, Objects.requireNonNull(time, "time"), options, Mode.MANUAL);
    }

    /**
     * A watchdog that judges nothing.
     *
     * @return the shared instance
     */
    public static Watchdog none() {
        return NONE;
    }

    /**
     * Detections in this process by watchdogs not {@linkplain #markScripted() marked scripted} (EL-Q-REL-11:
     * {@code core.watchdog.detections}, goal 0).
     *
     * @return the count
     */
    public static long totalDetections() {
        return TOTAL_DETECTIONS.get();
    }

    /**
     * Re-initialisations in this process by watchdogs not marked scripted ({@code core.watchdog.reinits}, goal 0).
     *
     * @return the count
     */
    public static long totalReinits() {
        return TOTAL_REINITS.get();
    }

    /**
     * Registrations made in this process with any watchdog that judges, so a reader can tell "none found" from
     * "none watched".
     *
     * @return the count
     */
    public static long totalRegistrations() {
        return TOTAL_REGISTRATIONS.get();
    }

    /**
     * Starts judging a thing. A threaded watchdog starts its thread with the first registration.
     *
     * @param watched the thing
     * @return its registration
     */
    public Registration register(Watched watched) {
        Objects.requireNonNull(watched, "watched");
        if (mode == Mode.NONE) {
            return () -> { };
        }
        TOTAL_REGISTRATIONS.incrementAndGet();
        Entry e = new Entry(watched, time.monoNanos());
        synchronized (this) {
            boolean first = entries.isEmpty();
            entries.add(e);
            if (first) {
                nextTick = time.monoNanos() + options.period().toNanos();
            }
            if (mode == Mode.THREADED && (thread == null || !thread.isAlive())) {
                startThreadLocked();
            }
        }
        return () -> unregister(e);
    }

    private void unregister(Entry e) {
        synchronized (this) {
            if (!entries.remove(e) || !entries.isEmpty()) {
                return;
            }
            nextTick = Long.MAX_VALUE;
            if (mode == Mode.THREADED && thread != null) {
                threadGeneration++; // stopped with the last registration (EL-H-01)
                LockSupport.unpark(thread);
                thread = null;
                liveThread = null;
            }
        }
    }

    /**
     * The bounds in force.
     *
     * @return the options
     */
    public Options options() {
        return options;
    }

    /**
     * Replaces the bounds; a test tunes a virtual-time watchdog with it.
     *
     * @param next the options
     * @return this watchdog
     */
    public Watchdog options(Options next) {
        this.options = Objects.requireNonNull(next, "next");
        return this;
    }

    /**
     * Marks this watchdog's findings as scripted faults, so they are left out of {@link #totalDetections()} and
     * {@link #totalReinits()}; the spoilers and the watchdog's own tests mark theirs.
     *
     * @return this watchdog
     */
    public Watchdog markScripted() {
        this.scripted = true;
        return this;
    }

    /**
     * Test hook (EL-SP-23): runs on the watchdog's thread before each check; a hook that parks starves the watchdog,
     * one that throws kills its thread.
     *
     * @param hook the hook, or null
     */
    public void beforeCheck(Runnable hook) {
        this.beforeCheck = hook;
    }

    /**
     * The monotonic instant of the next check in manual mode, or {@link Long#MAX_VALUE} when nothing is registered.
     *
     * @return the instant
     */
    public long nextTickNanos() {
        return nextTick;
    }

    /** One check of every watched thing, on the caller's thread, at the time source's now; re-armed from its end. */
    public void tick() {
        if (mode == Mode.NONE) {
            return;
        }
        Runnable hook = beforeCheck;
        if (hook != null && mode == Mode.MANUAL) {
            hook.run();
        }
        check(0); // the driver calls at the virtual instant it is due: never late
    }

    /**
     * Counts a slow operation found outside a loop: a virtual-time task that ran past the slow bound in real time
     * (EL-H-10, WD-7).
     *
     * @param name  what ran
     * @param nanos how long, in real time
     */
    public void reportSlow(String name, long nanos) {
        slowOperations.incrementAndGet();
        LOG.log(System.Logger.Level.DEBUG, () -> "slow operation: " + name + " ran " + nanos / 1_000_000 + " ms of"
                + " real time (slow bound " + options.slow().toMillis() + " ms)");
    }

    /**
     * The counters.
     *
     * @return a snapshot
     */
    public Stats stats() {
        return new Stats(ticks.get(), detections.get(), snapshot(detectionsByKind), reinits.get(), slowRuns.get(),
                snapshot(slowRunsBy), slowOperations.get(), escalations.get(), restartsStarved.get(),
                restartsThrown.get(), lateTicks.get());
    }

    /**
     * The most recent findings, oldest first (at most 256).
     *
     * @return the findings
     */
    public List<Detection> detections() {
        synchronized (recent) {
            return List.copyOf(recent);
        }
    }

    /**
     * The watchdog's thread now, in threaded mode, or null.
     *
     * @return the thread
     */
    public Thread thread() {
        return liveThread;
    }

    /**
     * Told by the timer thread on each of its wakes (WD-6): restarts a threaded watchdog whose heartbeat is older than
     * three periods.
     *
     * @param nowNanos the timer's monotonic now
     */
    void timerWoke(long nowNanos) {
        if (mode != Mode.THREADED || liveThread == null) {
            return;
        }
        if (nowNanos - heartbeat <= 3 * options.period().toNanos()) {
            return;
        }
        synchronized (this) {
            if (thread == null || nowNanos - heartbeat <= 3 * options.period().toNanos()) {
                return;
            }
            restartsStarved.incrementAndGet();
            LOG.log(System.Logger.Level.WARNING, "the watchdog's heartbeat is " + (nowNanos - heartbeat) / 1_000_000
                    + " ms old (over three periods): the timer thread restarts it (WD-6)");
            Thread old = thread;
            startThreadLocked();
            LockSupport.unpark(old);
        }
    }

    /**
     * When the timer thread must next look at this watchdog's heartbeat (WD-6): three periods past the last
     * heartbeat plus a millisecond, or {@link Long#MAX_VALUE} when no thread runs.
     *
     * @return the instant on the monotonic base
     */
    long heartbeatDeadline() {
        if (mode != Mode.THREADED || liveThread == null) {
            return Long.MAX_VALUE;
        }
        return heartbeat + 3 * options.period().toNanos() + 1_000_000L;
    }

    private void startThreadLocked() {
        long generation = ++threadGeneration;
        Thread t = Thread.ofPlatform().name(threadName).daemon(true).unstarted(() -> threadLoop(generation));
        t.setUncaughtExceptionHandler(this::threadDied);
        thread = t;
        liveThread = t;
        heartbeat = time.monoNanos();
        t.start();
        if (time instanceof SystemTime system) {
            system.nudgeTimer(); // an idle timer re-reads its wait with the watchdog's heartbeat in it
        }
    }

    private void threadDied(Thread t, Throwable e) {
        LOG.log(System.Logger.Level.ERROR, "the watchdog's check threw; its thread restarts (WD-6)", e);
        synchronized (this) {
            if (thread != t) {
                return; // already replaced
            }
            restartsThrown.incrementAndGet();
            if (entries.isEmpty()) {
                thread = null;
                liveThread = null;
                return;
            }
            startThreadLocked();
        }
    }

    private boolean current(long generation) {
        synchronized (this) {
            return generation == threadGeneration && !entries.isEmpty();
        }
    }

    private void threadLoop(long generation) {
        while (current(generation)) {
            heartbeat = time.monoNanos();
            long until = time.monoNanos() + options.period().toNanos();
            long left;
            while ((left = until - time.monoNanos()) > 0) {
                LockSupport.parkNanos(this, left);
                if (!current(generation)) {
                    return;
                }
            }
            Runnable hook = beforeCheck;
            if (hook != null) {
                hook.run();
            }
            if (!current(generation)) {
                return;
            }
            check(time.monoNanos() - until);
        }
    }

    /**
     * One check. {@code lateNanos} is how long after its park ended a threaded check began. Past the grace, after a
     * check that was on time, the process, or at least this thread, was paused (a stop-the-world collection, a frozen
     * container, a debugger): the loops could not run either, so the time that passed meanwhile is no evidence against
     * them, and every age this check and later ones judge (a wedge, a missed fire or wake, a stuck dispatch, a fence)
     * counts from this check's start at the earliest. A dead thread is still judged. A loop that stays stuck after the
     * pause is found one bound after it. Lateness on consecutive checks is load, not a pause, and moves nothing; and
     * the start moves at most once per wedge bound, so a wedge is found within twice the bound plus a period however
     * the pauses fall (srad/agentspaces-event-loop.md §10.10).
     */
    private void check(long lateNanos) {
        synchronized (checkLock) {
            long now = time.monoNanos();
            ticks.incrementAndGet();
            Options o = options;
            boolean late = lateNanos > o.grace().toNanos();
            if (late) {
                lateTicks.incrementAndGet();
                if (!lastLate && (judgeFrom == Long.MIN_VALUE || now - judgeFrom >= o.wedge().toNanos())) {
                    judgeFrom = now;
                    LOG.log(System.Logger.Level.INFO, "watchdog: this check began " + lateNanos / 1_000_000
                            + " ms after its park ended (a pause of the process); ages are judged from now");
                }
            }
            lastLate = late;
            boolean timerRestarted = false;
            Entry timer = timerEntry;
            if (timer != null && !entries.isEmpty()) {
                timerRestarted = judge(timer, now, o, false);
            }
            for (Entry e : entries) {
                judge(e, now, o, timerRestarted);
            }
            for (Entry e : entries) {
                try {
                    e.watched.watchdogTick(now); // the pool's grow and shrink, a loop's stretch (§3.10)
                } catch (RuntimeException ex) {
                    LOG.log(System.Logger.Level.WARNING, "watchdog: the tick of " + e.watched.name() + " threw", ex);
                }
            }
            for (LeaseHolder h : leases) {
                try {
                    judgeLease(h, now, o);
                } catch (RuntimeException ex) {
                    LOG.log(System.Logger.Level.WARNING, "watchdog: a lease check threw", ex);
                }
            }
            for (HeldEntry h : held) {
                judgeHeld(h, now);
            }
            nextTick = entries.isEmpty() ? Long.MAX_VALUE : time.monoNanos() + o.period().toNanos();
            heartbeat = time.monoNanos();
        }
    }

    /** Judges one thing; true when it was re-initialised. */
    private boolean judge(Entry e, long now, Options o, boolean skipMissedFire) {
        Watched w = e.watched;
        long runs = w.runs();
        long started = w.runStartedNanos();
        Thread t = w.thread();
        int queued = w.queued();
        boolean dispatched = w.dispatched();
        long armed = w.armedNanos();
        long raised = w.wakeRaisedNanos();
        String participant = w.runningIn();
        boolean progressed = runs != e.lastRuns;
        e.lastRuns = runs;
        Watched.Reason reason = null;
        long progress = w.progressNanos();
        // A run clears itself before its thread ends, so a thread is dead only while it is still the thing's thread:
        // a run on a thread of its own (one virtual thread per task) can end, and its thread with it, between the
        // reads above and this check, and that run is finished, not dead (srad/agentspaces-event-loop.md §10.13).
        if (t != null && !t.isAlive() && (started != 0 || queued > 0 || armed != Long.MAX_VALUE) && w.thread() == t) {
            reason = Watched.Reason.DEAD;
        } else if (started != 0) {
            long ran = now - judgedFrom(started);
            // Wedged is judged from the run's last progress: a step that has not returned, not a run of slow steps.
            long stuck = now - judgedFrom(progress == 0 ? started : progress);
            if (stuck > o.wedge().toNanos() && e.wedgedFor != started) {
                reason = Watched.Reason.WEDGED;
                e.wedgedFor = started;
            } else if (ran > o.slow().toNanos() && e.slowFor != started) {
                e.slowFor = started;
                slowRuns.incrementAndGet();
                String who = participant.isEmpty() ? w.name() : participant;
                slowRunsBy.computeIfAbsent(who, k -> new AtomicLong()).incrementAndGet();
                LOG.log(System.Logger.Level.DEBUG, () -> w.name() + " has run " + ran / 1_000_000 + " ms in "
                        + (participant.isEmpty() ? "its run" : participant) + " (slow bound " + o.slow().toMillis()
                        + " ms)");
            }
            e.dispatchedSince = 0;
        } else {
            if (queued > 0 && raised != 0 && !dispatched && now - judgedFrom(raised) > o.grace().toNanos()) {
                reason = Watched.Reason.MISSED_WAKE;
            }
            if (reason == null && dispatched && !progressed) {
                if (e.dispatchedSince == 0) {
                    e.dispatchedSince = now;
                } else if (now - judgedFrom(e.dispatchedSince) > o.wedge().toNanos()) {
                    reason = Watched.Reason.STUCK_DISPATCH;
                }
            } else {
                e.dispatchedSince = 0;
            }
            if (reason == null && !skipMissedFire && !progressed && !dispatched && raised == 0
                    && armed != Long.MAX_VALUE && now - judgedFrom(armed) > o.grace().toNanos()) {
                reason = Watched.Reason.MISSED_FIRE;
            }
        }
        boolean reinitialised = false;
        if (reason != null) {
            reinitialise(e, reason, participant, t, now, o);
            reinitialised = true;
        }
        long fenced = w.fencedSinceNanos();
        if (fenced != 0 && now - judgedFrom(fenced) > o.fenceLimit().toNanos() && !e.escalated) {
            escalate(e, "participant " + w.runningIn() + " fenced by a re-initialisation has not returned within "
                    + o.fenceLimit().toSeconds() + " s", now);
        }
        return reinitialised;
    }

    /** The later of an instant and {@link #judgeFrom}: a pause of the process is not held against a loop. */
    private long judgedFrom(long instant) {
        return judgeFrom == Long.MIN_VALUE || instant - judgeFrom >= 0 ? instant : judgeFrom;
    }

    private void reinitialise(Entry e, Watched.Reason reason, String participant, Thread t, long now, Options o) {
        Watched w = e.watched;
        detections.incrementAndGet();
        reinits.incrementAndGet();
        detectionsByKind.computeIfAbsent(reason.id(), k -> new AtomicLong()).incrementAndGet();
        if (!scripted) {
            TOTAL_DETECTIONS.incrementAndGet();
            TOTAL_REINITS.incrementAndGet();
        }
        remember(new Detection(reason.id(), w.name(), participant, now));
        StringBuilder stack = new StringBuilder();
        if (t != null && t.isAlive() && reason == Watched.Reason.WEDGED) {
            for (StackTraceElement frame : t.getStackTrace()) {
                stack.append("\n    at ").append(frame);
            }
        }
        LOG.log(System.Logger.Level.WARNING, "watchdog: " + w.name() + " " + reason.id()
                + (participant.isEmpty() ? "" : " in " + participant)
                + (t == null ? "" : " on " + t.getName() + " (" + t.getState() + ")")
                + "; re-initialising (generation " + (w.generation() + 1) + ")" + stack);
        WatchdogEvent.commit(w.name(), reason.id(), participant, 0);
        try {
            w.reinitialise(reason);
        } catch (RuntimeException ex) {
            LOG.log(System.Logger.Level.ERROR, "re-initialising " + w.name() + " threw", ex);
        }
        e.reinitTimes.addLast(now);
        while (!e.reinitTimes.isEmpty() && now - e.reinitTimes.peekFirst() > o.escalateWindow().toNanos()) {
            e.reinitTimes.removeFirst();
        }
        if (e.reinitTimes.size() >= o.escalateAfter() && !e.escalated) {
            escalate(e, e.reinitTimes.size() + " re-initialisations within " + o.escalateWindow().toMinutes()
                    + " minutes (last: " + reason.id() + ")", now);
        }
    }

    private void escalate(Entry e, String why, long now) {
        e.escalated = true;
        escalations.incrementAndGet();
        remember(new Detection("escalation", e.watched.name(), e.watched.runningIn(), now));
        LOG.log(System.Logger.Level.ERROR, "watchdog: " + e.watched.name() + " escalated: " + why + " (WD-5)");
        WatchdogEvent.commit(e.watched.name(), "escalation", e.watched.runningIn(), 0);
        try {
            e.watched.escalate(why);
        } catch (RuntimeException ex) {
            LOG.log(System.Logger.Level.ERROR, "escalating " + e.watched.name() + " threw", ex);
        }
    }

    private void remember(Detection d) {
        synchronized (recent) {
            recent.addLast(d);
            while (recent.size() > KEPT_DETECTIONS) {
                recent.removeFirst();
            }
        }
    }

    private static Map<String, Long> snapshot(Map<String, AtomicLong> counts) {
        Map<String, Long> out = new TreeMap<>();
        counts.forEach((k, v) -> out.put(k, v.get()));
        return Map.copyOf(out);
    }

    /** One watched thing and what the watchdog remembers of it between ticks; touched under the check lock. */
    private static final class Entry {
        final Watched watched;
        final long since;
        final ArrayDeque<Long> reinitTimes = new ArrayDeque<>();
        long lastRuns;
        long wedgedFor;
        long slowFor;
        long dispatchedSince;
        boolean escalated;

        Entry(Watched watched, long since) {
            this.watched = watched;
            this.since = since;
            this.lastRuns = watched.runs();
        }
    }

    /** For tests of the timer: the names of the things registered now. */
    List<String> registered() {
        List<String> names = new ArrayList<>();
        for (Entry e : entries) {
            names.add(e.watched.name());
        }
        return names;
    }
}
