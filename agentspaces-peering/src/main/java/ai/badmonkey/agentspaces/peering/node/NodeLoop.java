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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.api.spi.DueWork;
import ai.badmonkey.agentspaces.common.time.LatencyRing;
import ai.badmonkey.agentspaces.common.time.LoopPool;
import ai.badmonkey.agentspaces.common.time.SerialExecutor;
import ai.badmonkey.agentspaces.common.time.TimeSource;
import ai.badmonkey.agentspaces.common.time.Watchdog;
import ai.badmonkey.agentspaces.common.time.Watched;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * A node's loop (srad/agentspaces-event-loop.md §3.3): one sequential process that runs each participant when it is
 * due, when it is asked, or when the backstop sweeps, and arms one timer at the earliest next due, capped by the
 * backstop.
 *
 * <p>In CSP terms the loop reads two channels. The wake channel holds one slot, {@link #pending}: a wake on a full
 * channel is absorbed (NL-1). The timer channel is the one armed {@link TimeSource.Scheduled}. A run is the process's
 * select: it takes what is ready and runs the participants that are due, in order. Runs go through a
 * {@link SerialExecutor}, so participants of one node never run concurrently (NL-2). There is no mailbox per object.
 *
 * <p>Invariants (§3.3):
 * <ul>
 *   <li>NL-1: at most one run is pending or running; wakes during a run cause one more run.</li>
 *   <li>NL-2: participants never run concurrently with each other.</li>
 *   <li>NL-3: the armed timer is at or before the earliest participant deadline and {@code now + backstop}.</li>
 *   <li>NL-4: a participant that throws is logged with its name, counted and re-armed at the backstop; the others
 *       run.</li>
 *   <li>NL-5: {@link #close()} stops the loop by a flag; "inside my own run" is a scoped binding set by the run (a
 *       {@link ThreadLocal} bound for the run's extent, since the build targets Java 21 where {@code ScopedValue} is a
 *       preview API), never a thread name.</li>
 *   <li>NL-6: {@link #stats()}.</li>
 *   <li>NL-7: no wake is lost: a participant's {@code asked} flag is set before {@link #pending} is tested, and a run
 *       clears {@link #pending} before it reads any flag or deadline.</li>
 *   <li>NL-8: within a run, protocol participants of every group run before the other kinds; groups in id order.</li>
 *   <li>NL-9: the loop publishes a heartbeat in volatile fields at run start and end.</li>
 * </ul>
 *
 * <p>The watchdog (§3.9) judges the loop through {@link Watched}; a started loop registers with its time source's
 * watchdog and leaves it on {@link #close()}. {@link #reinitialise} bumps the generation, moves the pending wake,
 * the {@code asked} flags and the queued runs to a fresh serial executor, re-arms the timer and asks every
 * participant once (WD-2); the participant a wedged run is inside is fenced until that run returns, and a run of an
 * old generation re-arms nothing and its deadlines are discarded (WD-3), so NL-1 and NL-2 hold across generations.
 * A participant that throws on three consecutive runs is re-registered with fresh state and backed off (EL-H-07); an
 * application participant over the slow bound in 3 of its last 10 runs moves to its own process, a serial executor of
 * its own that reports its next due back over a one-slot channel (WD-8, EL-H-11). {@link #health()} reads OK,
 * DEGRADED or DOWN (EL-H-14).
 *
 * <p>Where runs execute (phase 4, decision 2): on a time source with a {@linkplain TimeSource#loopPool() loop pool}
 * (the process's {@code SystemTime}), the serial executor drains on the pool and the armed timer fires there too
 * ({@link TimeSource#scheduleOnLoop}), so loop work never waits behind application code on the virtual-thread
 * carriers (R11, EL-O-02); on a virtual source, drains are tasks of that source, so an advance sees them finish.
 *
 * <p>Overrun and the stretch (§3.10, EL-O-04, EL-O-05): on each watchdog tick the loop judges its lateness. With the
 * pool at its max and p99 lateness over the stretch bound for three ticks, it stretches its local periods ×2, then
 * ×4; after 30 s in band it halves the stretch. Only participants registered with a stretching {@link Timing} move,
 * never past their cap (a SPEC floor); before a stretch applies, every wire-visible participant's refresh action runs
 * with its stretched period, and the outboxes are flushed, so no peer reads a lapse (refresh before slowdown).
 */
public final class NodeLoop implements AutoCloseable, Watched {

    /**
     * How a participant's period may move under a stretch (§3.10, EL-O-04 to EL-O-07, EL-F-01).
     *
     * @param stretches whether its deadlines stretch with the loop's local periods
     * @param capNanos  the longest period a stretch may give it (a SPEC floor, or a promise a peer relies on), 0 for
     *                  no cap beyond the stretch factor; a period already longer is left as it is
     * @param refresh   for a wire-visible participant, its refresh action, run on the loop before a stretch applies
     *                  with the stretched period in nanoseconds: it renews its leases and re-issues its advertisements
     *                  with spans covering at least twice that period (within each lease's own maximum); null when
     *                  no peer judges its cadence
     */
    public record Timing(boolean stretches, long capNanos, java.util.function.LongConsumer refresh) {

        /** Never stretches: the default, for protocol cadences peers judge and anything not marked otherwise. */
        public static final Timing FIXED = new Timing(false, 0, null);

        /** Checks the cap. */
        public Timing {
            if (capNanos < 0) {
                throw new IllegalArgumentException("capNanos " + capNanos);
            }
        }

        /**
         * A local period: it stretches, at most to the cap.
         *
         * @param cap the longest stretched period, or {@link Duration#ZERO} for none beyond the factor
         * @return the timing
         */
        public static Timing local(Duration cap) {
            return new Timing(true, cap.toNanos(), null);
        }

        /**
         * A period a peer judges: it stretches only after its refresh action ran for the new period (EL-O-05).
         *
         * @param cap     the longest stretched period, or {@link Duration#ZERO} for none beyond the factor
         * @param refresh the refresh action, given the stretched period in nanoseconds
         * @return the timing
         */
        public static Timing wireVisible(Duration cap, java.util.function.LongConsumer refresh) {
            return new Timing(true, cap.toNanos(), Objects.requireNonNull(refresh, "refresh"));
        }

        /**
         * Whether a peer judges this participant's cadence (EL-O-07).
         *
         * @return whether it has a refresh action
         */
        public boolean wireVisible() {
            return refresh != null;
        }
    }

    /** The order participants run in within one run (NL-8). */
    public enum Kind {
        /** Trust, membership, gossip, anti-entropy, the self ad: run first in every run. */
        PROTOCOL,
        /** Capability, storage and application participants. */
        OTHER
    }

    /**
     * What the loop did since it started (NL-6). Lateness is the start of a timer-started run minus the deadline its
     * timer was armed for.
     *
     * @param runs                runs, scheduled and direct
     * @param timerWakes          runs the armed timer started
     * @param eventWakes          runs a wake started
     * @param directRuns          runs of {@link #tick()}
     * @param backstopRuns        runs that swept every participant because the backstop came due
     * @param backstopFoundWork   per participant name, the sweeps that found it reporting a deadline earlier than the
     *                            one the loop held, or doing work it said fell due before that deadline
     *                            ({@link #foundOverdue}), with no wake (a missed wake; EL-Q-REL-03)
     * @param participantThrows   per participant name, the runs that threw (NL-4)
     * @param slowRuns            runs over {@link PeerNode#SLOW_RUN_NANOS} of real time
     * @param overruns            timer fires that found a run pending or running (§3.10)
     * @param latenessSamples     timer-started runs whose lateness is in the three figures below (the last 4,096)
     * @param latenessP50Nanos    the median lateness
     * @param latenessP99Nanos    the 99th percentile lateness
     * @param latenessMaxNanos    the largest lateness
     * @param busyNanos           real time spent in runs, summed
     * @param generation          the loop's generation (bumped by a re-initialisation; 0 until the watchdog lands)
     * @param backstopOnlyRuns    backstop runs in which no participant was due or asked: the sweep alone. A deadline
     *                            that falls on a sweep counts as a backstop run but not here, so "wakes less these"
     *                            counts every run that had work (§5.4's {@code core.idle.wakes})
     * @param slowParticipants    per participant name, its runs over the slow bound of real time (EL-H-11)
     * @param backedOff           participants re-registered and backed off after three throws in a row (EL-H-07)
     * @param offloaded           participants moved to their own process after repeated slow runs (WD-8)
     * @param reregistrations     re-registrations after three throws in a row, summed
     * @param dueRuns             per participant name, scheduled runs that called it because its deadline came (a
     *                            timer wake's cause)
     * @param askedRuns           per participant name, scheduled runs that called it because a wake asked it (an
     *                            event wake's cause)
     * @param wakeCalls           wakes raised, by who raised them: a participant's name for its own
     *                            {@code Registration.wake()}, {@code group:<id>} or {@code group:<id>/<kind>} for a
     *                            group's, {@code all} for every participant's
     * @param stretch             the stretch factor on local periods now, 1 for none (EL-O-04)
     * @param stretches           steps up of the stretch
     * @param restores            steps down of the stretch
     * @param refreshes           refresh actions run before a stretch applied (EL-O-05)
     * @param refreshFailures     refresh actions that threw; their participant keeps its unstretched period
     * @param stretchUnrefreshed  wire-visible deadlines whose stretch would have outrun the period their refresh
     *                            covered (a period that grew after the refresh); each is held to the covered period,
     *                            so none goes unrefreshed ({@code core.loop.stretch-unrefreshed}, goal 0)
     * @param wireVisible         participants whose cadence a peer judges (EL-O-07, EL-F-01)
     * @param stretchable         participants whose periods stretch
     */
    public record Stats(long runs, long timerWakes, long eventWakes, long directRuns, long backstopRuns,
                        Map<String, Long> backstopFoundWork, Map<String, Long> participantThrows, long slowRuns,
                        long overruns, long latenessSamples, long latenessP50Nanos, long latenessP99Nanos,
                        long latenessMaxNanos, long busyNanos, long generation, long backstopOnlyRuns,
                        Map<String, Long> slowParticipants, List<String> backedOff, List<String> offloaded,
                        long reregistrations, Map<String, Long> dueRuns, Map<String, Long> askedRuns,
                        Map<String, Long> wakeCalls, int stretch, long stretches, long restores, long refreshes,
                        long refreshFailures, long stretchUnrefreshed, List<String> wireVisible,
                        List<String> stretchable) {

        /**
         * Backstop sweeps that found work, over every participant.
         *
         * @return the sum of {@link #backstopFoundWork()}
         */
        public long backstopFoundTotal() {
            return backstopFoundWork.values().stream().mapToLong(Long::longValue).sum();
        }
    }

    /** The loop's health (EL-H-14). */
    public enum HealthState {
        /** Every participant runs on the loop and none is backed off. */
        OK,
        /**
         * A participant is backed off after repeated throws, or runs in its own process after repeated slow runs, or a
         * stretch is in force.
         */
        DEGRADED,
        /** The watchdog escalated: repeated re-initialisations, or a fenced participant that never returned. */
        DOWN
    }

    /**
     * The loop's health and why.
     *
     * @param state   the state
     * @param reasons one sentence per cause, empty when OK
     */
    public record Health(HealthState state, List<String> reasons) {
        /** Copies the reasons. */
        public Health {
            Objects.requireNonNull(state, "state");
            reasons = List.copyOf(reasons);
        }

        static final Health OK = new Health(HealthState.OK, List.of());
    }

    /**
     * The default backstop (decision 3 a): 1 s through phases 1 to 5, raised to 15 s at the phase-6 gate once the
     * seam audit (EL-A-02) found no test that needed the sweep and every wake seam had a test that fails when it is
     * cut. 15 s is half the 30 s keepalive bound and a third of the 45 s member TTL floor (decision 17), so a missed
     * wake still heals well inside any TTL a peer judges; "backstop runs that found work" stays asserted at 0.
     */
    public static final Duration DEFAULT_BACKSTOP = Duration.ofSeconds(15);

    /** A participant run or a whole run over this much real time is slow (EL-X-10, EL-H-11). */
    static final long SLOW_NANOS = 100_000_000L;

    /** Consecutive throws after which a participant is re-registered and backed off (EL-H-07). */
    static final int THROWS_BEFORE_BACKOFF = 3;

    /** The first back-off of a participant that keeps throwing; protocol participants never pass it (EL-H-07). */
    static final long FIRST_BACKOFF_NANOS = 1_000_000_000L;

    /** Slow runs among a participant's last {@link #SLOW_WINDOW} that move it to its own process (WD-8). */
    static final int SLOW_TO_OFFLOAD = 3;

    /** The window of recent runs the slow count reads. */
    static final int SLOW_WINDOW = 10;

    private static final long NO_RESULT = Long.MIN_VALUE;

    /** A participant that reports a deadline at or before now is run again no sooner than this (a spin guard). */
    static final long MIN_RERUN_NANOS = 1_000_000L;

    /**
     * A backstop sweep counts a participant as missed only when it now reports a deadline this much earlier, plus
     * {@link #SLEW_DIVISOR}'s share of how long the loop held the deadline.
     */
    static final long MISSED_SLACK_NANOS = 1_000_000L;
    /**
     * A participant whose deadline is a wall instant converts it to the monotonic base on each run (§3.6); between two
     * runs the wall clock may slew against the monotonic one by up to 500 ppm (TS-6), so the same wall deadline can
     * convert up to a 2,000th of the hold earlier. That is not a missed wake: over TCP a 32 s anti-entropy round and a
     * 30 s member TTL were counted as missed 6 times in 5 minutes with a 1 ms slack alone, on a host whose wall clock
     * moved 1.3 ms against the monotonic one in 90 s (srad/agentspaces-event-loop.md §10.8).
     */
    static final long SLEW_DIVISOR = 2_000L;

    private static final System.Logger LOG = System.getLogger(NodeLoop.class.getName());
    private static final ThreadLocal<Frame> INSIDE = new ThreadLocal<>();
    /**
     * The loop whose direct run ({@link #tick()}) sent the request an offloaded participant's process is running
     * (WD-8): {@link #directRun()} answers for that request as it would on the loop, or a hand-driven node's
     * {@code onTick} adapter, which runs only on a direct run when no tick period is set, would never run again once
     * moved (§10.19).
     */
    private static final ThreadLocal<NodeLoop> OFFLOADED_DIRECT = new ThreadLocal<>();
    private static final Comparator<Participant> ORDER = Comparator
            .comparing((Participant p) -> p.kind)
            .thenComparing(p -> p.group)
            .thenComparingLong(p -> p.seq);

    private final String name;
    private final TimeSource time;
    private final long backstopNanos;
    private final LongSupplier realNanos;
    private final Executor drainer;
    /** Where a participant moved off the loop runs (WD-8): a process of its own on a virtual thread, never the pool. */
    private final Executor offloadDrainer;
    private final LoopPool pool;
    private volatile AutoCloseable poolUse;
    private final Map<String, AtomicLong> wakeCallsBy = new ConcurrentHashMap<>();
    // The stretch (§3.10): the factor in force, the one asked for, and the counters (EL-O-09).
    private volatile int stretch = 1;
    private volatile int requestedStretch = 1;
    private final StretchRule stretchRule = new StretchRule();
    private final LatencyRing lateWindow = new LatencyRing(1024);
    private long lateMark;
    private final AtomicLong stretches = new AtomicLong();
    private final AtomicLong restores = new AtomicLong();
    private final AtomicLong refreshes = new AtomicLong();
    private final AtomicLong refreshFailures = new AtomicLong();
    private final AtomicLong stretchUnrefreshed = new AtomicLong();
    private volatile Runnable afterRefresh = () -> { };
    private volatile SerialExecutor serial;
    private final LoopMeter meter = new LoopMeter();
    private final AtomicBoolean pending = new AtomicBoolean();
    private final AtomicBoolean timerFired = new AtomicBoolean();
    private final AtomicLong sequence = new AtomicLong();
    private final AtomicLong directRuns = new AtomicLong();
    /** Whether a test drops wakes on purpose here, so its finds stay out of the process total (EL-Q-REL-03). */
    private volatile boolean scripted;
    /** The last {@link #TRACE_RUNS} participant runs (EL-Q-MNT-01). */
    private final Trace trace = new Trace();
    private final AtomicLong backstopRuns = new AtomicLong();
    private final AtomicLong backstopOnlyRuns = new AtomicLong();
    private final AtomicLong overruns = new AtomicLong();
    private final Map<String, AtomicLong> backstopFound = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> throwsByName = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> slowByName = new ConcurrentHashMap<>();
    private final AtomicLong reregistrations = new AtomicLong();
    private final Object tableLock = new Object();
    private final Object idleMonitor = new Object();
    private final Object healthLock = new Object();
    private volatile Participant[] table = new Participant[0];
    private volatile boolean started;
    private volatile boolean closed;
    private final AtomicLong generation = new AtomicLong();
    /** The run in progress of the current generation, or null (NL-9's heartbeat; WD-3's fence token). */
    private final AtomicReference<Run> current = new AtomicReference<>();
    private volatile long pendingSince;
    private volatile String fencedName = "";
    private volatile long fencedSince;
    private volatile String escalated;
    private volatile Watchdog.Registration watch;
    private final AtomicBoolean dropNextDispatch = new AtomicBoolean();
    private final AtomicLong healthVersion = new AtomicLong();
    private long healthSeen = -1;
    private volatile Health health = Health.OK;
    private volatile Consumer<Health> healthListener;

    // Touched only inside a run (serial, NL-2), and by close() for the cancel.
    private volatile Arm arm;
    private boolean swept;
    /**
     * Set by a re-initialisation (WD-4): the first run of the new generation refreshes every wire-visible participant
     * whose deadline passed while the loop was stopped, and flushes, before any participant runs.
     */
    private volatile boolean refreshOverdue;
    private long backstopAt;
    private volatile long firedFor;

    // The heartbeat (NL-9): written at run start and run end, read without a lock.
    private final AtomicLong heartbeatRuns = new AtomicLong();
    private volatile long armedNanos = DueWork.NEVER;

    /**
     * Makes a loop that has not started; {@link #tick()} works before {@link #start()}.
     *
     * @param name     the owner's name, for logs ("node:&lt;peer&gt;")
     * @param time     the time source
     * @param backstop the backstop period
     */
    public NodeLoop(String name, TimeSource time, Duration backstop) {
        this(name, time, backstop, System::nanoTime);
    }

    /**
     * Makes a loop with an injected real-time reading for its busy and slow-run figures (tests only).
     *
     * @param name      the owner's name
     * @param time      the time source
     * @param backstop  the backstop period
     * @param realNanos the real-time reading the busy and slow figures use
     */
    NodeLoop(String name, TimeSource time, Duration backstop, LongSupplier realNanos) {
        this.name = Objects.requireNonNull(name, "name");
        this.time = Objects.requireNonNull(time, "time");
        if (backstop.isNegative() || backstop.isZero()) {
            throw new IllegalArgumentException("the backstop must be positive: " + backstop);
        }
        this.backstopNanos = backstop.toNanos();
        this.realNanos = realNanos;
        // A drain (and a re-initialised loop's fresh drain) runs on the loop pool (decision 2), or on a virtual
        // source, as a task of that source.
        this.pool = time.loopPool();
        this.offloadDrainer = task -> time.schedule(Duration.ZERO, task);
        this.drainer = pool != null ? pool : offloadDrainer;
        this.serial = new SerialExecutor(name, drainer);
    }

    /**
     * Adds a participant. It is asked at once, so it runs on the loop's next run.
     *
     * @param participant its stable name (DW-4)
     * @param kind        its kind, for the run order (NL-8)
     * @param group       its group's id, for the run order and {@link #wakeGroup}; "" for node-wide work
     * @param work        the work
     * @return its registration
     */
    public DueWork.Registration register(String participant, Kind kind, String group, DueWork work) {
        return register(participant, kind, group, work, Timing.FIXED);
    }

    /**
     * Adds a participant with a timing for the stretch (EL-O-07). It is asked at once.
     *
     * @param participant its stable name (DW-4)
     * @param kind        its kind, for the run order (NL-8)
     * @param group       its group's id; "" for node-wide work
     * @param work        the work
     * @param timing      how its period may stretch, and its refresh action when a peer judges it
     * @return its registration
     */
    public DueWork.Registration register(String participant, Kind kind, String group, DueWork work, Timing timing) {
        Participant p = new Participant(Objects.requireNonNull(participant, "participant"),
                Objects.requireNonNull(kind, "kind"), Objects.requireNonNull(group, "group"),
                sequence.getAndIncrement(), Objects.requireNonNull(work, "work"),
                Objects.requireNonNull(timing, "timing"));
        synchronized (tableLock) {
            Participant[] next = Arrays.copyOf(table, table.length + 1);
            next[next.length - 1] = p;
            Arrays.sort(next, ORDER);
            table = next;
        }
        nudge();
        return p;
    }

    /** Starts the loop: the first run asks every participant, then the loop arms its own timer. */
    public void start() {
        if (closed) {
            return;
        }
        boolean first = !started;
        started = true;
        for (Participant p : table) {
            p.asked.set(true);
        }
        if (first) {
            if (pool != null) {
                poolUse = pool.open(); // the pool keeps its min while a node is open
            }
            watch = time.watchdog().register(this); // judged from its first run (EL-H-02)
            ai.badmonkey.agentspaces.common.time.LoopTotals.loopStarted();
        }
        nudge();
    }

    /**
     * Sets what runs after a stretch's refresh actions and before the stretch applies: the node flushes its outboxes,
     * so the refreshed leases and advertisements are on the wire first (EL-O-05).
     *
     * @param flush the flush
     */
    public void afterRefresh(Runnable flush) {
        this.afterRefresh = Objects.requireNonNull(flush, "flush");
    }

    /**
     * The stretch factor on local periods now, 1 for none (EL-O-04).
     *
     * @return the factor
     */
    public int stretch() {
        return stretch;
    }

    /**
     * Asks the loop to move its stretch to a factor; the next run refreshes the wire-visible participants and then
     * applies it (EL-O-05). The watchdog's tick asks through the stretch rule; tests and spoilers ask directly
     * (EL-SP-29).
     *
     * @param factor 1, 2 or 4 (a power of two up to the pool's cap)
     */
    void requestStretch(int factor) {
        if (factor < 1 || Integer.bitCount(factor) != 1) {
            throw new IllegalArgumentException("a stretch factor is a power of two: " + factor);
        }
        requestedStretch = factor;
        nudge();
    }

    /**
     * Sets the listener told of each change of {@link #health()} (EL-H-14); it runs on the thread that changed it (a
     * run, or the watchdog's) and must not block.
     *
     * @param listener the listener, or null
     */
    public void onHealth(Consumer<Health> listener) {
        this.healthListener = listener;
    }

    /**
     * OK, DEGRADED (a backed-off or offloaded participant) or DOWN (escalated), with reasons (EL-H-14).
     *
     * @return the health now
     */
    public Health health() {
        refreshHealth();
        return health;
    }

    /**
     * Asks every participant of a group to run again (GroupRuntime.wake(), EL-L-15).
     *
     * @param group the group id
     */
    public void wakeGroup(String group) {
        countWake("group:" + group);
        for (Participant p : table) {
            if (p.group.equals(group)) {
                p.asked.set(true);
            }
        }
        nudge();
    }

    /**
     * Asks the participants of one kind in a group to run again: an inbound working frame asks a group's registrants,
     * not its protocol participants, which have their own deadlines and change signals (EL-L-03).
     *
     * @param group the group id
     * @param kind  the kind
     */
    public void wakeGroup(String group, Kind kind) {
        countWake("group:" + group + "/" + kind.name().toLowerCase(java.util.Locale.ROOT));
        for (Participant p : table) {
            if (p.kind == kind && p.group.equals(group)) {
                p.asked.set(true);
            }
        }
        nudge();
    }

    /** Asks every participant to run again. */
    public void wakeAll() {
        countWake("all");
        for (Participant p : table) {
            p.asked.set(true);
        }
        nudge();
    }

    /**
     * Runs every participant now (DW-1 makes the early call harmless), returning when the run ends: on the caller's
     * thread, serial with the loop's own runs (NL-2), and inline when called from inside this loop's own run (EL-L-16),
     * where the participant running now is left out so it does not re-enter itself.
     */
    public void tick() {
        if (closed) {
            return;
        }
        if (insideRun()) {
            runBody(true, false);
            return;
        }
        serial.runExclusive(() -> runBody(true, false));
    }

    /**
     * Whether the calling thread is inside this loop's run (NL-5).
     *
     * @return whether it is
     */
    public boolean insideRun() {
        Frame f = INSIDE.get();
        return f != null && f.loop == this;
    }

    /**
     * Whether the calling thread is inside a run that {@link #tick()} started, which calls every participant whether
     * or not it is due. The {@code onTick} adapter reads it: a tick runs every adapter, as a tick did before the loop
     * (EL-L-04, EL-L-05).
     *
     * @return whether the current run is a direct one
     */
    public boolean directRun() {
        Frame f = INSIDE.get();
        return f != null && f.loop == this && f.direct || OFFLOADED_DIRECT.get() == this;
    }

    /**
     * Tells the loop, from inside a participant's run, that the run just did work of its own that was due at
     * {@code dueNanos} on the loop's monotonic base (srad/agentspaces-event-loop.md EL-Q-REL-03, §10.47). A participant
     * that decides "due now" for itself from inputs no wake covers calls it before doing that work. When the run was a
     * backstop sweep and the loop held a later deadline, the sweep counts as one that found work, as a sweep that moves
     * the deadline earlier does; a sweep that did the work and returned the same deadline was not counted before. Outside
     * a run it does nothing.
     *
     * @param dueNanos when the work was due, on the loop's monotonic base
     */
    public static void foundOverdue(long dueNanos) {
        long[] report = OVERDUE.get();
        if (report[0] == 0 || dueNanos - report[1] < 0) {
            report[0] = 1;
            report[1] = dueNanos;
        }
    }

    /** The overdue work the participant running on this thread reported ({@link #foundOverdue}): [reported, due]. */
    private static final ThreadLocal<long[]> OVERDUE = ThreadLocal.withInitial(() -> new long[2]);

    /**
     * Waits until no run is in flight, for at most {@code max} of real time; at once from inside this loop's own run.
     * {@code close()} of a node uses it to let a run in flight finish before the node's state goes.
     *
     * @param max the longest wait
     * @return whether no run was in flight when it returned
     * @throws InterruptedException if interrupted while waiting
     */
    public boolean awaitIdle(Duration max) throws InterruptedException {
        if (insideRun()) {
            return false;
        }
        // Real time by design (EL-X-04): close waits for real work in flight, whatever the loop's source.
        long deadline = ai.badmonkey.agentspaces.common.time.TimeSource.system().monoNanos() + max.toNanos();
        synchronized (idleMonitor) {
            while (current.get() != null) {
                long left = deadline - ai.badmonkey.agentspaces.common.time.TimeSource.system().monoNanos();
                if (left <= 0) {
                    return false;
                }
                java.util.concurrent.TimeUnit.NANOSECONDS.timedWait(idleMonitor, left);
            }
        }
        return true;
    }

    /** Stops the loop: no run starts after this, and the armed timer is cancelled (NL-5). A run in flight ends. */
    @Override
    public void close() {
        closed = true;
        Arm armed = arm;
        if (armed != null && armed.handle != null) {
            armed.handle.cancel();
        }
        armedNanos = DueWork.NEVER;
        Watchdog.Registration w = watch;
        if (w != null) {
            w.close();
        }
        AutoCloseable use = poolUse;
        poolUse = null;
        if (use != null) {
            try {
                use.close();
            } catch (Exception e) {
                // a pool mark's close does not fail
            }
        }
    }

    /**
     * Whether {@link #close()} was called.
     *
     * @return whether the loop is closed
     */
    public boolean closed() {
        return closed;
    }

    /**
     * Marks this loop's backstop finds as scripted: a test that drops a wake on purpose, so the backstop's find is the
     * assertion and not a fault, keeps it out of {@link ai.badmonkey.agentspaces.common.time.LoopTotals} (the
     * end-of-build read of EL-Q-REL-03). The loop's own {@link #stats()} still count it.
     *
     * @return this loop
     */
    public NodeLoop markScripted() {
        this.scripted = true;
        return this;
    }

    /** How many participant runs the debug trace keeps (EL-Q-MNT-01). */
    public static final int TRACE_RUNS = 64;

    /**
     * One participant run in the debug trace (srad/agentspaces-event-loop.md EL-Q-MNT-01).
     *
     * @param participant the participant's name
     * @param dueNanos    the deadline it held when the run began, on the loop's monotonic base, or
     *                    {@link DueWork#NEVER}
     * @param ranAtNanos  the run's start on the loop's monotonic base
     * @param tookNanos   the real time the participant took
     * @param cause       why it ran: {@code due}, {@code asked}, {@code sweep} (the backstop), {@code all} (a direct
     *                    run or a wall step) or {@code offloaded}
     * @param next        the deadline it returned, or {@link DueWork#NEVER}; {@code -1} when it threw
     */
    public record TraceEntry(String participant, long dueNanos, long ranAtNanos, long tookNanos, String cause,
                             long next) {

        /** One line: the participant, how late or early against its deadline, what it took, and why it ran. */
        public String line() {
            String due = dueNanos == DueWork.NEVER ? "never"
                    : String.format(java.util.Locale.ROOT, "%+.3f ms", (ranAtNanos - dueNanos) / 1e6);
            String nxt = next == -1 ? "threw" : next == DueWork.NEVER ? "never"
                    : String.format(java.util.Locale.ROOT, "+%.3f ms", (next - ranAtNanos) / 1e6);
            return String.format(java.util.Locale.ROOT, "%s at %d ns: due %s, took %.3f ms, %s, next %s",
                    participant, ranAtNanos, due, tookNanos / 1e6, cause, nxt);
        }
    }

    /** A fixed ring of the last runs; written by the loop's one runner, read on request. */
    private static final class Trace {
        private final TraceEntry[] ring = new TraceEntry[TRACE_RUNS];
        private int next;
        private long total;

        synchronized void add(TraceEntry e) {
            ring[next] = e;
            next = (next + 1) % ring.length;
            total++;
        }

        synchronized List<TraceEntry> snapshot() {
            List<TraceEntry> out = new java.util.ArrayList<>(ring.length);
            for (int i = 0; i < ring.length; i++) {
                TraceEntry e = ring[(next + i) % ring.length];
                if (e != null) {
                    out.add(e);
                }
            }
            return out;
        }

        synchronized long total() {
            return total;
        }
    }

    /**
     * The last {@value #TRACE_RUNS} participant runs, oldest first (srad/agentspaces-event-loop.md EL-Q-MNT-01): what
     * ran, against which deadline, when, for how long, and why. Kept always (a fixed ring, one entry per participant
     * run), read only on request.
     *
     * @return the trace
     */
    public List<TraceEntry> trace() {
        return trace.snapshot();
    }

    /**
     * Logs the trace on request at INFO, one line per run under a header naming the loop (EL-Q-MNT-01), and returns
     * the text logged.
     *
     * @return the text
     */
    public String logTrace() {
        List<TraceEntry> entries = trace.snapshot();
        StringBuilder text = new StringBuilder();
        text.append("node loop ").append(name).append(": the last ").append(entries.size()).append(" of ")
                .append(trace.total()).append(" participant runs, oldest first (generation ").append(generation.get())
                .append(")");
        for (TraceEntry e : entries) {
            text.append('\n').append("  ").append(e.line());
        }
        LOG.log(System.Logger.Level.INFO, text.toString());
        return text.toString();
    }

    /**
     * The loop's counters (NL-6).
     *
     * @return a snapshot
     */
    public Stats stats() {
        LoopMeter.Figures base = meter.snapshot();
        return new Stats(base.runs(), base.timerWakes(), base.eventWakes(), directRuns.get(), backstopRuns.get(),
                snapshot(backstopFound), snapshot(throwsByName), base.slowRuns(), overruns.get(),
                base.latenessSamples(), base.latenessP50Nanos(), base.latenessP99Nanos(), base.latenessMaxNanos(),
                base.busyNanos(), generation.get(), backstopOnlyRuns.get(), snapshot(slowByName),
                names(p -> p.backoffNanos != 0), names(p -> p.offloaded), reregistrations.get(),
                counts(p -> p.dueRuns), counts(p -> p.askedRuns), snapshot(wakeCallsBy), stretch, stretches.get(),
                restores.get(), refreshes.get(), refreshFailures.get(), stretchUnrefreshed.get(),
                names(p -> p.timing.wireVisible()), names(p -> p.timing.stretches()));
    }

    private Map<String, Long> counts(java.util.function.ToLongFunction<Participant> count) {
        Map<String, Long> out = new TreeMap<>();
        for (Participant p : table) {
            long n = count.applyAsLong(p);
            if (n != 0) {
                out.merge(p.name, n, Long::sum);
            }
        }
        return Map.copyOf(out);
    }

    private List<String> names(java.util.function.Predicate<Participant> test) {
        List<String> out = new ArrayList<>();
        for (Participant p : table) {
            if (test.test(p)) {
                out.add(p.name);
            }
        }
        return List.copyOf(out);
    }

    // The heartbeat (NL-9), read by the watchdog with no lock.

    /**
     * Completed runs.
     *
     * @return the count
     */
    public long heartbeatRuns() {
        return heartbeatRuns.get();
    }

    /**
     * When the run in progress started on the time source's monotonic base, or 0 when none runs.
     *
     * @return the start
     */
    @Override
    public long runStartedNanos() {
        Run r = current.get();
        return r == null ? 0 : r.startedNanos;
    }

    /**
     * When the run in progress last made progress (its start, or the start or end of its latest participant), or 0
     * when none runs: the watchdog judges the wedge bound from it, so a run of several slow participants, none of them
     * stuck, is not wedged (srad/agentspaces-event-loop.md §10.10).
     *
     * @return the instant on the time source's monotonic base
     */
    @Override
    public long progressNanos() {
        Run r = current.get();
        return r == null ? 0 : r.progressNanos;
    }

    /**
     * The participant running now, or "".
     *
     * @return its name
     */
    @Override
    public String runningIn() {
        Run r = current.get();
        return r == null ? "" : r.in;
    }

    /**
     * The deadline the loop's timer is armed for, or {@link DueWork#NEVER}.
     *
     * @return the deadline
     */
    @Override
    public long armedNanos() {
        return armedNanos;
    }

    /**
     * The thread running the current run, or null.
     *
     * @return the thread
     */
    public Thread runThread() {
        Run r = current.get();
        return r == null ? null : r.thread;
    }

    // Watched (§3.9): every method reads volatile state and takes no lock (WD-1).

    @Override
    public String name() {
        return name;
    }

    @Override
    public long runs() {
        return heartbeatRuns.get();
    }

    @Override
    public Thread thread() {
        return runThread();
    }

    @Override
    public int queued() {
        return (pending.get() ? 1 : 0) + serial.queued();
    }

    @Override
    public boolean dispatched() {
        SerialExecutor s = serial;
        if (s.dispatched() || s.owner() != null) {
            return true; // a drain is on its way or between tasks: lateness, not a fault
        }
        Arm a = arm;
        return a != null && !a.fired && a.handle != null && a.handle.dispatched(); // fired, waiting for a carrier
    }

    @Override
    public long wakeRaisedNanos() {
        return pending.get() ? pendingSince : 0;
    }

    @Override
    public long fencedSinceNanos() {
        return fencedSince;
    }

    @Override
    public long generation() {
        return generation.get();
    }

    /**
     * Re-initialises the loop (WD-2, WD-3, EL-H-04, EL-H-05): a new generation, a fresh serial executor that takes
     * every queued run, the timer cancelled, the pending wake cleared and every participant asked, then one nudge, so
     * the next run is a backstop run that re-arms the timer. That run first runs the refresh action of each
     * wire-visible participant whose deadline passed while the loop was stopped, and flushes (WD-4, EL-H-06). The participant a wedged run is inside stays fenced (its
     * {@code inRun} flag holds) until that run returns; the old thread is interrupted, best effort. Runs on the
     * watchdog's thread and never runs a participant (EL-H-13).
     *
     * @param reason why
     */
    @Override
    public void reinitialise(Reason reason) {
        if (closed) {
            return;
        }
        generation.incrementAndGet();
        Run stale = current.getAndSet(null);
        if (stale != null && !stale.in.isEmpty() && stale.thread.isAlive()) {
            fencedName = stale.in;
            long now = time.monoNanos();
            fencedSince = now == 0 ? 1 : now;
        }
        if (stale != null && reason == Reason.WEDGED && stale.thread.isAlive()) {
            stale.thread.interrupt(); // best effort (WD-2)
        }
        synchronized (idleMonitor) {
            idleMonitor.notifyAll();
        }
        Arm armed = arm;
        if (armed != null && armed.handle != null) {
            armed.handle.cancel();
        }
        arm = null;
        armedNanos = DueWork.NEVER;
        swept = false; // the first run of the new generation sweeps every participant, as a backstop run does
        refreshOverdue = true; // and first refreshes the wire-visible ones a stopped loop left overdue (WD-4)
        SerialExecutor old = serial;
        SerialExecutor fresh = new SerialExecutor(name, drainer);
        serial = fresh;
        pending.set(false);
        pendingSince = 0;
        for (Participant p : table) {
            p.asked.set(true);
        }
        old.abandon(fresh); // queued runs move over; a drain still in the old one takes nothing more
        LOG.log(System.Logger.Level.INFO, name + " re-initialised (" + reason.id() + "), generation "
                + generation.get() + (stale == null || stale.in.isEmpty() ? "" : "; fenced " + stale.in));
        nudge();
    }

    /**
     * Turns the loop's health DOWN (WD-5, EL-H-08); the listener hears it.
     *
     * @param reason why
     */
    @Override
    public void escalate(String reason) {
        escalated = reason;
        healthVersion.incrementAndGet();
        refreshHealth();
    }

    private void countWake(String by) {
        wakeCallsBy.computeIfAbsent(by, k -> new AtomicLong()).incrementAndGet();
    }

    /** Test hook (EL-SP-20): the next nudge sets the pending flag and dispatches nothing, a lost nudge. */
    void dropNextDispatch() {
        dropNextDispatch.set(true);
    }

    private void refreshHealth() {
        Health changed = null;
        Consumer<Health> listener;
        synchronized (healthLock) {
            long version = healthVersion.get();
            if (version == healthSeen) {
                return;
            }
            healthSeen = version;
            Health next;
            String down = escalated;
            if (down != null) {
                next = new Health(HealthState.DOWN, List.of(down));
            } else {
                List<String> reasons = new ArrayList<>();
                for (Participant p : table) {
                    if (p.backoffNanos != 0) {
                        reasons.add(p.name + " backed off after " + THROWS_BEFORE_BACKOFF + " throws in a row");
                    }
                    if (p.offloaded) {
                        reasons.add(p.name + " runs in its own process after repeated slow runs");
                    }
                }
                if (stretch > 1) {
                    reasons.add("local periods stretched x" + stretch + " under sustained overrun (EL-O-04)");
                }
                next = reasons.isEmpty() ? Health.OK : new Health(HealthState.DEGRADED, reasons);
            }
            if (!next.equals(health)) {
                health = next;
                changed = next;
            }
            listener = healthListener;
        }
        if (changed != null && listener != null) {
            try {
                listener.accept(changed);
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "a loop health listener of " + name + " threw", e);
            }
        }
    }

    /**
     * Whether a run is waiting to start (the wake channel's one slot is full).
     *
     * @return whether a run is pending
     */
    public boolean pendingRun() {
        return pending.get();
    }

    /**
     * The participants in run order, by name.
     *
     * @return their names
     */
    public List<String> participants() {
        List<String> names = new ArrayList<>();
        for (Participant p : table) {
            names.add(p.name);
        }
        return List.copyOf(names);
    }

    /**
     * The deadline the loop holds for a participant, or {@link DueWork#NEVER} when it has none or none is registered
     * under the name.
     *
     * @param participant the name
     * @return its deadline
     */
    public long dueOf(String participant) {
        for (Participant p : table) {
            if (p.name.equals(participant)) {
                return p.due;
            }
        }
        return DueWork.NEVER;
    }

    private void nudge() {
        if (closed || !started) {
            return;
        }
        if (pending.compareAndSet(false, true)) {
            long now = time.monoNanos();
            pendingSince = now == 0 ? 1 : now;
            if (dropNextDispatch.getAndSet(false)) {
                return; // scripted: a nudge raised and never dispatched (EL-SP-20)
            }
            serial.execute(this::scheduledRun);
        }
    }

    private void onTimer(Arm fired) {
        if (closed || fired != arm) {
            return; // a timer replaced after it began to fire: its replacement carries the deadline
        }
        if (current.get() != null || pending.get()) {
            overruns.incrementAndGet();
        }
        fired.fired = true;
        firedFor = fired.at;
        timerFired.set(true);
        nudge();
    }

    private void scheduledRun() {
        pending.set(false); // NL-7: cleared before any flag or deadline is read
        pendingSince = 0;
        if (closed) {
            return;
        }
        runBody(false, true);
    }

    private void runBody(boolean all, boolean scheduled) {
        Frame outer = INSIDE.get();
        boolean nested = outer != null && outer.loop == this;
        long realStart = realNanos.getAsLong();
        long now = time.monoNanos();
        Run run = nested ? outer.run : new Run(generation.get(), Thread.currentThread(), now == 0 ? 1 : now);
        INSIDE.set(new Frame(this, all || nested && outer.direct, run));
        if (!nested) {
            current.set(run);
        }
        boolean fatal = false;
        boolean stale = false;
        try {
            if (scheduled) {
                if (timerFired.getAndSet(false)) {
                    meter.timerWake(now - firedFor);
                    lateWindow.record(now - firedFor);
                } else {
                    meter.eventWake();
                }
            } else if (!nested) {
                directRuns.incrementAndGet();
            }
            if (!nested && refreshOverdue && run.generation == generation.get()) {
                refreshOverdue = false;
                refreshOverdue(now);
            }
            if (!nested && requestedStretch != stretch && run.generation == generation.get()) {
                applyStretch(now);
            }
            boolean sweep = !swept || now - backstopAt >= 0;
            boolean work = false;
            for (Participant p : table) {
                if (run.generation != generation.get()) {
                    stale = true; // a re-initialisation took over: this run touches nothing more (WD-3)
                    break;
                }
                if (p.closed || p.inRun) {
                    continue; // closed, re-entered from its own run, or fenced by a re-initialisation
                }
                if (p.offloaded) {
                    work |= offloadedStep(p, now, all, sweep);
                    continue;
                }
                boolean asked = p.asked.getAndSet(false);
                boolean due = p.due - now <= 0 && p.due != DueWork.NEVER;
                if (p.backoffUntil != 0 && !all && now - p.backoffUntil < 0) {
                    continue; // backed off after repeated throws (EL-H-07); its deadline is the back-off's end
                }
                // While a stretch is in force the backstop's sweep passes a stretching participant by: it judges its
                // own clock (DW-1), and a sweep every backstop would run it at its unstretched period (EL-O-04).
                boolean sweepsIt = sweep && !(stretch > 1 && p.timing.stretches());
                if (!(all || asked || due || sweepsIt)) {
                    continue;
                }
                work |= asked || due;
                if (scheduled && !nested) {
                    if (due) {
                        p.dueRuns++;
                    } else if (asked) {
                        p.askedRuns++;
                    }
                }
                long held = p.due;
                long next;
                p.inRun = true;
                String before = run.in;
                run.in = p.name;
                run.progress(time.monoNanos()); // the wedge bound is judged per step (§10.10)
                long participantStart = realNanos.getAsLong();
                boolean threw = false;
                long[] overdue = OVERDUE.get();
                long outerReported = overdue[0];
                long outerDue = overdue[1];
                overdue[0] = 0;
                boolean reported;
                long reportedDue;
                try {
                    next = p.work.run(now);
                    ranClean(p);
                } catch (VirtualMachineError e) {
                    fatal = true; // the thread dies with the run marked started: the watchdog's dead check
                    throw e;
                } catch (Throwable e) {
                    threw = true;
                    next = threw(p, now, e);
                } finally {
                    reported = overdue[0] != 0;
                    reportedDue = overdue[1];
                    overdue[0] = outerReported; // a nested run's report stays with its own participant
                    overdue[1] = outerDue;
                    p.inRun = false;
                    if (!fatal) {
                        run.in = before; // a fatal error leaves the name for the watchdog's report
                        run.progress(time.monoNanos());
                    }
                }
                long took = realNanos.getAsLong() - participantStart;
                trace.add(new TraceEntry(p.name, held, now, took,
                        due ? "due" : asked ? "asked" : all ? "all" : "sweep", threw ? -1 : next));
                slowAccounting(p, took);
                if (run.generation != generation.get()) {
                    // This participant was fenced and its old run has returned: its deadline is discarded, and the
                    // next run of the new generation asks it again (WD-3).
                    p.asked.set(true);
                    if (p.name.equals(fencedName)) {
                        fencedSince = 0;
                        fencedName = "";
                    }
                    stale = true;
                    break;
                }
                if (next != DueWork.NEVER && next - now < MIN_RERUN_NANOS) {
                    next = now + MIN_RERUN_NANOS;
                }
                next = stretched(p, now, next, due);
                boolean sweptOnly = sweepsIt && !all && !asked && !due;
                long slack = MISSED_SLACK_NANOS + Math.max(0, now - p.dueSetAt) / SLEW_DIVISOR;
                // Found work: the sweep returned a deadline earlier than the one the loop held, or the participant
                // said it did work of its own that fell due before that deadline (foundOverdue, §10.47).
                boolean earlier = next != DueWork.NEVER && (held == DueWork.NEVER || next - (held - slack) < 0);
                boolean overdueWork = reported && (held == DueWork.NEVER || reportedDue - (held - slack) < 0);
                if (sweptOnly && p.backoffNanos == 0 && (earlier || overdueWork)) {
                    backstopFound.computeIfAbsent(p.name, k -> new AtomicLong()).incrementAndGet();
                    ai.badmonkey.agentspaces.common.time.LoopTotals.found(p.name, scripted); // the end-of-build read
                }
                p.due = next;
                p.dueSetAt = now;
            }
            if (!stale) {
                if (sweep) {
                    backstopAt = now + backstopNanos;
                    if (!all && swept) {
                        backstopRuns.incrementAndGet();
                        if (!work) {
                            backstopOnlyRuns.incrementAndGet();
                        }
                    }
                    swept = true;
                }
                if (!nested) {
                    rearm(now);
                }
            }
        } finally {
            if (fatal) {
                INSIDE.remove();
            } else {
                if (nested) {
                    INSIDE.set(outer);
                } else {
                    long took = realNanos.getAsLong() - realStart;
                    meter.ran(took);
                    heartbeatRuns.incrementAndGet();
                    if (outer == null) {
                        INSIDE.remove();
                    } else {
                        INSIDE.set(outer);
                    }
                    if (current.compareAndSet(run, null)) {
                        synchronized (idleMonitor) {
                            idleMonitor.notifyAll();
                        }
                    } else {
                        nudge(); // a stale run returned late: the new generation asks what it left (WD-3)
                    }
                    refreshHealth();
                }
            }
        }
    }

    /** A participant ran clean: its throw count and any back-off end (EL-H-07). */
    private void ranClean(Participant p) {
        p.consecutiveThrows = 0;
        if (p.backoffNanos != 0) {
            p.backoffNanos = 0;
            p.backoffUntil = 0;
            healthVersion.incrementAndGet();
            LOG.log(System.Logger.Level.INFO, "participant " + p.name + " of " + name + " runs clean again");
        }
    }

    /** NL-4, EL-H-07: counted, logged, re-armed at the backstop; on the third throw in a row, backed off. */
    private long threw(Participant p, long now, Throwable e) {
        throwsByName.computeIfAbsent(p.name, k -> new AtomicLong()).incrementAndGet();
        meter.failed();
        p.consecutiveThrows++;
        if (p.consecutiveThrows < THROWS_BEFORE_BACKOFF) {
            LOG.log(p.consecutiveThrows == 1 ? System.Logger.Level.WARNING : System.Logger.Level.DEBUG,
                    "participant " + p.name + " of " + name + " threw; re-armed at the backstop", e);
            return now + backstopNanos;
        }
        long cap = p.kind == Kind.PROTOCOL ? FIRST_BACKOFF_NANOS : Math.max(backstopNanos, FIRST_BACKOFF_NANOS);
        boolean first = p.backoffNanos == 0;
        p.backoffNanos = first ? FIRST_BACKOFF_NANOS : Math.min(p.backoffNanos * 2, cap);
        p.asked.set(false); // re-registered with fresh state
        p.backoffUntil = now + p.backoffNanos;
        if (p.backoffUntil == 0) {
            p.backoffUntil = 1;
        }
        if (first) {
            reregistrations.incrementAndGet();
            healthVersion.incrementAndGet();
            LOG.log(System.Logger.Level.WARNING, "participant " + p.name + " of " + name + " threw "
                    + p.consecutiveThrows + " times in a row; re-registered and backed off " + p.backoffNanos
                    / 1_000_000 + " ms (EL-H-07)", e);
        } else {
            LOG.log(System.Logger.Level.DEBUG, "participant " + p.name + " of " + name + " threw again", e);
        }
        return p.backoffUntil;
    }

    /** EL-H-11, WD-8: a run over the slow bound is counted and named; 3 of the last 10 move it off the loop. */
    private void slowAccounting(Participant p, long tookNanos) {
        boolean slow = tookNanos > SLOW_NANOS;
        p.slowRing = ((p.slowRing << 1) | (slow ? 1 : 0)) & ((1 << SLOW_WINDOW) - 1);
        if (!slow) {
            return;
        }
        long n = slowByName.computeIfAbsent(p.name, k -> new AtomicLong()).incrementAndGet();
        LOG.log(n == 1 ? System.Logger.Level.WARNING : System.Logger.Level.DEBUG, "participant " + p.name + " of "
                + name + " ran " + tookNanos / 1_000_000 + " ms (slow bound " + SLOW_NANOS / 1_000_000 + " ms)");
        if (p.kind == Kind.OTHER && !p.offloaded && Integer.bitCount(p.slowRing) >= SLOW_TO_OFFLOAD) {
            p.process = new SerialExecutor(name + "/" + p.name, offloadDrainer); // off the pool too (WD-8)
            p.offloaded = true;
            healthVersion.incrementAndGet();
            LOG.log(System.Logger.Level.WARNING, "participant " + p.name + " of " + name + " was slow in "
                    + Integer.bitCount(p.slowRing) + " of its last " + SLOW_WINDOW + " runs; it now runs as its own"
                    + " process and reports its next due over a one-slot channel (WD-8)");
        }
    }

    /**
     * An offloaded participant's turn in a run (WD-8): take the next due its process reported, and when it is due,
     * asked or swept and no request is in flight, send it one request. Returns whether it had work.
     */
    private boolean offloadedStep(Participant p, long now, boolean all, boolean sweep) {
        long reported = p.result.getAndSet(NO_RESULT);
        if (reported != NO_RESULT) {
            p.due = reported;
            p.dueSetAt = now;
        }
        boolean asked = p.asked.getAndSet(false);
        boolean due = p.due != DueWork.NEVER && p.due - now <= 0;
        if (!(all || asked || due || sweep) || p.inFlight) {
            if (asked && p.inFlight) {
                p.askedWhileInFlight = true;
            }
            return false;
        }
        p.inFlight = true;
        p.askedWhileInFlight = false;
        p.due = DueWork.NEVER; // the process's answer sets it
        Frame sender = INSIDE.get();
        boolean direct = sender != null && sender.loop == this && sender.direct; // a tick() asks it as a tick would
        p.process.execute(() -> {
            long next;
            if (direct) {
                OFFLOADED_DIRECT.set(this);
            }
            try {
                next = p.work.run(now);
                p.consecutiveThrows = 0;
            } catch (VirtualMachineError e) {
                throw e;
            } catch (Throwable e) {
                throwsByName.computeIfAbsent(p.name, k -> new AtomicLong()).incrementAndGet();
                meter.failed();
                LOG.log(System.Logger.Level.DEBUG, "offloaded participant " + p.name + " of " + name + " threw", e);
                next = now + backstopNanos;
            } finally {
                OFFLOADED_DIRECT.remove();
            }
            if (next != DueWork.NEVER && next - now < MIN_RERUN_NANOS) {
                next = now + MIN_RERUN_NANOS;
            }
            p.result.set(next);
            p.inFlight = false;
            if (p.askedWhileInFlight) {
                p.asked.set(true);
            }
            nudge(); // the loop's next run takes the answer
        });
        return asked || due;
    }

    /**
     * A participant's next deadline under the stretch in force (EL-O-04): a stretching participant's new period is
     * multiplied by the factor, at most to its cap; a held deadline is kept while the participant reports the same one
     * (an early run, DW-1), so asking it again does not push it out again. A wire-visible participant stretches only
     * once its refresh ran for the factor (EL-O-05).
     */
    private long stretched(Participant p, long now, long raw, boolean ranDue) {
        Timing t = p.timing;
        if (!t.stretches() || raw == DueWork.NEVER) {
            return raw;
        }
        boolean changed = raw != p.rawDue;
        if (raw - now > 0 && (ranDue || p.basePeriod == 0) && changed) {
            p.basePeriod = raw - now; // a whole period, read when the deadline came
        }
        int f = stretch;
        if (f <= 1) {
            p.rawDue = raw;
            p.rawFactor = 1;
            return raw;
        }
        if (!changed && p.rawFactor == f && p.due != DueWork.NEVER) {
            return p.due;
        }
        p.rawDue = raw;
        p.rawFactor = f;
        long period = raw - now;
        if (period <= 0) {
            return raw;
        }
        if (t.wireVisible() && p.refreshedFor < f) {
            return raw; // its refresh threw, or it joined after the stretch: it keeps its unstretched period
        }
        long s = period > Long.MAX_VALUE / 8 ? period : period * f;
        if (t.capNanos() > 0) {
            s = Math.min(s, Math.max(period, t.capNanos()));
        }
        if (t.wireVisible() && s > Math.max(period, p.refreshedPeriod)) {
            // Its period grew after its refresh ran: the stretch would outrun what the refresh covered. It is held to
            // the covered period, and counted (core.loop.stretch-unrefreshed reads what would have gone unrefreshed).
            stretchUnrefreshed.incrementAndGet();
            s = Math.max(period, p.refreshedPeriod);
        }
        return now + s;
    }

    /**
     * WD-4, EL-H-06, EL-O-05: in the first run after a re-initialisation, each wire-visible participant whose deadline
     * passed while the loop was stopped has its refresh run with the period in force (renewing its lease or re-issuing
     * its advertisement at once), then the outboxes are flushed, before any participant of the run is called. A fenced
     * participant (its old run has not returned) is passed by; it is asked again when it returns.
     */
    private void refreshOverdue(long now) {
        int refreshed = 0;
        for (Participant p : table) {
            if (p.closed || p.inRun || !p.timing.wireVisible() || p.due == DueWork.NEVER || p.due - now > 0) {
                continue;
            }
            long period = p.basePeriod;
            if (stretch > 1 && p.refreshedFor >= stretch) {
                period = Math.max(period, p.refreshedPeriod);
            }
            if (period <= 0) {
                period = backstopNanos;
            }
            Run r = current.get();
            String before = r == null ? "" : r.in;
            if (r != null) {
                r.in = p.name + "/refresh";
            }
            try {
                p.timing.refresh().accept(period);
                refreshes.incrementAndGet();
                refreshed++;
            } catch (RuntimeException e) {
                refreshFailures.incrementAndGet();
                LOG.log(System.Logger.Level.WARNING, "the refresh of " + p.name + " of " + name + " threw after a"
                        + " re-initialisation; it runs in this run as usual (WD-4)", e);
            } finally {
                if (r != null) {
                    r.in = before;
                }
            }
        }
        if (refreshed == 0) {
            return;
        }
        try {
            afterRefresh.run();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "the flush after the refreshes of " + name + " threw", e);
        }
        LOG.log(System.Logger.Level.INFO, name + " ran " + refreshed + " refresh actions of wire-visible participants"
                + " a stopped loop left overdue, before its first run after the re-initialisation (WD-4)");
    }

    /**
     * Moves the stretch to the factor asked for, inside a run (EL-O-04, EL-O-05): up, after every wire-visible
     * participant's refresh action ran for its stretched period and the outboxes were flushed; down, at once, asking
     * every stretching participant again so its period comes back now (§3.10's restore).
     */
    private void applyStretch(long now) {
        int want = requestedStretch;
        int have = stretch;
        if (want == have) {
            return;
        }
        if (want > have) {
            int refreshed = 0;
            for (Participant p : table) {
                if (p.closed || !p.timing.wireVisible() || p.refreshedFor >= want) {
                    continue;
                }
                long period = p.basePeriod;
                long s = period > Long.MAX_VALUE / 8 ? period : period * want;
                if (p.timing.capNanos() > 0) {
                    s = Math.min(s, Math.max(period, p.timing.capNanos()));
                }
                Run r = current.get();
                String before = r == null ? "" : r.in;
                if (r != null) {
                    r.in = p.name + "/refresh";
                }
                try {
                    p.timing.refresh().accept(s);
                    p.refreshedPeriod = s;
                    p.refreshedFor = want;
                    refreshes.incrementAndGet();
                    refreshed++;
                } catch (RuntimeException e) {
                    refreshFailures.incrementAndGet();
                    LOG.log(System.Logger.Level.WARNING, "the refresh of " + p.name + " of " + name + " threw before a"
                            + " stretch; it keeps its unstretched period (EL-O-05)", e);
                } finally {
                    if (r != null) {
                        r.in = before;
                    }
                }
            }
            try {
                afterRefresh.run(); // the refreshed leases and advertisements go out before the stretch applies
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "the flush after the refreshes of " + name + " threw", e);
            }
            stretch = want;
            stretches.incrementAndGet();
            LOG.log(System.Logger.Level.INFO, name + " stretched its local periods x" + want + " under sustained"
                    + " overrun, after " + refreshed + " refresh actions (EL-O-04, EL-O-05)");
        } else {
            stretch = want;
            restores.incrementAndGet();
            for (Participant p : table) {
                if (p.timing.stretches()) {
                    p.rawFactor = 0;
                    p.asked.set(true); // its period comes back at once
                    if (p.refreshedFor > want) {
                        p.refreshedFor = want; // a later stretch refreshes it again
                    }
                }
            }
            LOG.log(System.Logger.Level.INFO, name + " restored its local periods to x" + want + " (lateness in band;"
                    + " §3.10)");
        }
        healthVersion.incrementAndGet();
    }

    /**
     * The stretch check of the watchdog's tick (§3.10, EL-O-04): the lateness of this tick's timer-started runs, with
     * the age of a run waiting to start, judged by the stretch rule against the pool's state. It only decides and
     * wakes the loop; the next run refreshes and applies (EL-H-13).
     *
     * @param nowNanos the watchdog's monotonic now
     */
    @Override
    public void watchdogTick(long nowNanos) {
        if (closed || !started || pool == null) {
            return;
        }
        LatencyRing.Figures f = lateWindow.since(lateMark);
        lateMark = f.total();
        long p99 = f.p99Nanos();
        long since = pendingSince;
        if (pending.get() && since != 0) {
            p99 = Math.max(p99, nowNanos - since);
        }
        int want = stretchRule.judge(nowNanos, p99, pool.atMax(), pool.options(), requestedStretch);
        if (want != requestedStretch) {
            requestedStretch = want;
            nudge();
        }
    }

    /**
     * The stretch rule of §3.10, apart from the loop so a test can drive it on its own clock: with the pool at its
     * max and p99 lateness over the stretch bound for the stretch ticks, double the factor up to the cap; once lateness
     * has been in band for the clear time, halve it each tick until none is left, so ×4 clears within the clear time
     * plus a period (EL-SP-27).
     */
    static final class StretchRule {
        private int over;
        private long inBandSince = Long.MIN_VALUE;

        int judge(long now, long p99, boolean atMax, LoopPool.Options o, int current) {
            long bound = o.stretchLateness().toNanos();
            if (p99 > bound) {
                inBandSince = Long.MIN_VALUE;
                if (!atMax) {
                    over = 0; // the pool can still grow: no stretch yet
                    return current;
                }
                over++;
                if (over >= o.stretchTicks() && current < o.stretchCap()) {
                    over = 0;
                    return current * 2;
                }
                return current;
            }
            over = 0;
            if (current <= 1) {
                inBandSince = Long.MIN_VALUE;
                return 1;
            }
            if (inBandSince == Long.MIN_VALUE) {
                inBandSince = now;
                return current;
            }
            if (now - inBandSince >= o.stretchClear().toNanos()) {
                return current / 2; // after the clear time in band, halved each tick until none is left
            }
            return current;
        }
    }

    /** Arms the one timer at the earliest deadline, capped by the backstop (NL-3); replaced, never stacked. */
    private void rearm(long now) {
        if (closed || !started) {
            return;
        }
        long at = backstopAt;
        for (Participant p : table) {
            // A fenced participant's held deadline is stale until its old run returns (WD-3); arming on it would spin.
            if (!p.closed && !p.inRun && p.due != DueWork.NEVER && p.due - at < 0) {
                at = p.due;
            }
        }
        Arm current = arm;
        if (current != null && current.at == at && !current.fired) {
            return; // already armed there
        }
        if (current != null && current.handle != null) {
            current.handle.cancel(); // a fire already under way finds itself replaced and does nothing
        }
        Arm next = new Arm(at);
        arm = next; // before scheduling, so a fire at once finds itself current
        armedNanos = at;
        next.handle = time.scheduleOnLoop(at, () -> onTimer(next)); // the timer hands it to the loop pool (§3.5)
        if (closed) {
            next.handle.cancel();
        }
    }

    /** One run: its generation, its thread and start, and the participant inside it now (NL-9, WD-3). */
    private static final class Run {
        final long generation;
        final Thread thread;
        final long startedNanos;
        volatile String in = "";
        /** When the run last made progress: its start, then each participant's start and end (§10.10). */
        volatile long progressNanos;

        Run(long generation, Thread thread, long startedNanos) {
            this.generation = generation;
            this.thread = thread;
            this.startedNanos = startedNanos;
            this.progressNanos = startedNanos;
        }

        void progress(long nowNanos) {
            progressNanos = nowNanos == 0 ? 1 : nowNanos;
        }
    }

    /** "Inside my own run" for the thread (NL-5): the loop, whether the run is direct, and the run. */
    private record Frame(NodeLoop loop, boolean direct, Run run) {
    }

    /** One arming of the loop's timer. */
    private static final class Arm {
        final long at;
        volatile boolean fired;
        volatile TimeSource.Scheduled handle;

        Arm(long at) {
            this.at = at;
        }
    }

    private void remove(Participant p) {
        synchronized (tableLock) {
            List<Participant> next = new ArrayList<>(Arrays.asList(table));
            next.remove(p);
            table = next.toArray(new Participant[0]);
        }
    }

    private static Map<String, Long> snapshot(Map<String, AtomicLong> counts) {
        Map<String, Long> out = new TreeMap<>();
        counts.forEach((k, v) -> out.put(k, v.get()));
        return Map.copyOf(out);
    }

    private final class Participant implements DueWork.Registration {
        final String name;
        final Kind kind;
        final String group;
        final long seq;
        final DueWork work;
        final AtomicBoolean asked = new AtomicBoolean(true);
        volatile long due = DueWork.NEVER;
        // When the loop took the deadline it holds, for the missed-wake slack; inside runs only.
        long dueSetAt;
        volatile boolean closed;
        // Written inside runs; read by a re-initialised generation's runs and rearm, so volatile (the fence, WD-2).
        volatile boolean inRun;
        int consecutiveThrows;
        volatile long backoffNanos;
        volatile long backoffUntil;
        int slowRing;
        // Offloaded to its own process (WD-8): the request in flight and the one-slot answer channel.
        volatile boolean offloaded;
        volatile boolean inFlight;
        volatile boolean askedWhileInFlight;
        volatile SerialExecutor process;
        final AtomicLong result = new AtomicLong(NO_RESULT);
        final Timing timing;
        // The per-participant wake counters (written inside runs only; read by stats) and the wakes it raised.
        volatile long dueRuns;
        volatile long askedRuns;
        final AtomicLong wakeCalls = new AtomicLong();
        // The stretch: the deadline it last reported, the factor applied to it, its last whole period, and the factor
        // its refresh action last ran for (inside runs only).
        long rawDue = DueWork.NEVER;
        int rawFactor = 1;
        volatile long basePeriod;
        volatile int refreshedFor = 1;
        volatile long refreshedPeriod;

        Participant(String name, Kind kind, String group, long seq, DueWork work, Timing timing) {
            this.name = name;
            this.kind = kind;
            this.group = group;
            this.seq = seq;
            this.work = work;
            this.timing = timing;
        }

        @Override
        public void wake() {
            wakeCalls.incrementAndGet();
            countWake(name);
            asked.set(true); // NL-7: before the pending flag is tested
            nudge();
        }

        @Override
        public void close() {
            closed = true;
            remove(this);
        }
    }
}
