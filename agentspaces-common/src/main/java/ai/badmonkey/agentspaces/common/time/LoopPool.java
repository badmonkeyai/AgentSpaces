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
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * The elastic loop pool, {@code agentspaces-loop} (srad/agentspaces-event-loop.md §3.10, decision 2, EL-O-02,
 * EL-O-03): the per-process platform threads that node runs and flushes execute on, apart from the application's
 * virtual-thread carriers, so application load on the carriers cannot delay a probe (R11). Each node's
 * {@link SerialExecutor} drains here, so a node's participants still never run concurrently (NL-2).
 *
 * <p>Size: at least {@link Options#min()} threads while any node is {@linkplain #open() open} and none otherwise; at
 * most {@link Options#max()} (by default min(8, max(2, cores / 2))). The first thread starts with the first task;
 * after that the pool changes size only on the watchdog's tick ({@link #watchdogTick}, the "pool" check of §3.9):
 * <ul>
 *   <li><b>grow</b> one thread per tick while runs waited more than the grow bound (20 ms) at p99 for two ticks, or
 *       while more runs were queued than threads serve them at two ticks running, up to the max (one tick that lands
 *       while co-resident nodes' runs fall due together on its grid is a moment, not a load; §10.35). A thread busy past the stuck bound (5 s, a
 *       wedged participant the watchdog is fencing) does not count towards the max, so a wedge cannot take the pool
 *       from every other node (counted as a compensation);</li>
 *   <li><b>prompt growth</b>: a run that has waited past the grow bound while a thread is held by one run longer than
 *       that bound gets one more thread at once, up to the max, without waiting for the tick: the timer thread checks
 *       it ({@link #promptCheckAt()}, {@link #promptCheck}). One node's slow run then cannot hold every other node's
 *       runs past the dispatch band (EL-SP-08, EL-Q-SEC-04). Bursts of short runs still grow a thread a tick;</li>
 *   <li><b>shrink</b>: every thread idle over the idle bound (60 s) ends at the next tick, down to the min, so a burst's
 *       threads are gone within 60 s plus one period of its end (EL-SP-28, EL-Q-PRF-08). Idle means busy less than
 *       1% of the bound with no pressure in it, judged as growth is: no two ticks running with waits over the grow
 *       bound or more runs queued than threads (a lone tick is a moment, §10.47). Co-resident nodes' deadlines fall
 *       together, and a trickle of short runs landing at once touches every thread, so none would ever be idle for
 *       60 s in the literal sense (srad/agentspaces-event-loop.md §10.21). With no node open there is no
 *       tick, and an idle thread ends on its own after the bound.</li>
 * </ul>
 * Each change is counted and logged at DEBUG (EL-O-03). The pool is a {@link Watched} of its time source's watchdog
 * while a node is open (EL-H-02); it reports no run of its own, since every run on it is judged by its node's loop.
 *
 * <p>In CSP terms the pool is not a process of its own: it is the set of carriers the nodes' sequential processes run
 * on, and its queue is the channel those processes' drains are dispatched over (principle 9). The pool takes its own
 * short lock only; the watchdog only tries it, and skips a tick rather than wait (WD-1).
 */
public final class LoopPool implements Executor, Watched {

    private static final System.Logger LOG = System.getLogger(LoopPool.class.getName());

    /**
     * The pool's bounds and the stretch bounds its nodes read (decision 2, EL-O-10).
     *
     * @param min             threads kept while a node is open (1)
     * @param max             the most threads (min(8, max(2, cores / 2)))
     * @param idle            a thread idle this long ends, down to the min (60 s)
     * @param growLateness    runs waiting longer than this at p99 for {@code growTicks} ticks grow the pool (20 ms)
     * @param growTicks       consecutive ticks over the grow bound before a thread is added (2)
     * @param stuck           a thread busy longer than this does not count towards the max (the wedge bound, 5 s)
     * @param stretchLateness a node at the pool's max with p99 lateness over this for {@code stretchTicks} ticks
     *                        stretches its local periods (100 ms)
     * @param stretchTicks    consecutive ticks over the stretch bound before each step of the stretch (3)
     * @param stretchClear    lateness in band this long halves a stretch (30 s)
     * @param stretchCap      the largest stretch factor (4)
     */
    public record Options(int min, int max, Duration idle, Duration growLateness, int growTicks, Duration stuck,
                          Duration stretchLateness, int stretchTicks, Duration stretchClear, int stretchCap) {

        /** Checks the bounds. */
        public Options {
            Objects.requireNonNull(idle, "idle");
            Objects.requireNonNull(growLateness, "growLateness");
            Objects.requireNonNull(stuck, "stuck");
            Objects.requireNonNull(stretchLateness, "stretchLateness");
            Objects.requireNonNull(stretchClear, "stretchClear");
            if (min < 0 || max < 1 || min > max) {
                throw new IllegalArgumentException("the loop pool needs 0 <= min <= max and max >= 1: min " + min
                        + ", max " + max);
            }
            if (idle.isNegative() || idle.isZero() || growTicks < 1 || stretchTicks < 1) {
                throw new IllegalArgumentException("the idle bound and the tick counts must be positive");
            }
            if (stretchCap < 1 || Integer.bitCount(stretchCap) != 1) {
                throw new IllegalArgumentException("the stretch cap is a power of two, 1 for none: " + stretchCap);
            }
        }

        /**
         * Decision 2 (a) for a host with this many cores.
         *
         * @param cores the cores the JVM sees
         * @return the defaults
         */
        public static Options defaults(int cores) {
            return new Options(1, defaultMax(cores), Duration.ofSeconds(60), Duration.ofMillis(20), 2,
                    Duration.ofSeconds(5), Duration.ofMillis(100), 3, Duration.ofSeconds(30), 4);
        }

        /**
         * The defaults for this JVM, each overridden by a system property when set (§7.2):
         * {@code agentspaces.loop.pool-max} (0 for the default), {@code .pool-idle-millis},
         * {@code .grow-lateness-millis}, {@code .stretch-lateness-millis}, {@code .stretch-cap}.
         *
         * @return the options
         */
        public static Options fromSystemProperties() {
            Options d = defaults(Runtime.getRuntime().availableProcessors());
            int max = Integer.getInteger("agentspaces.loop.pool-max", 0);
            return new Options(d.min, max <= 0 ? d.max : max, millis("pool-idle-millis", d.idle),
                    millis("grow-lateness-millis", d.growLateness), d.growTicks, d.stuck,
                    millis("stretch-lateness-millis", d.stretchLateness), d.stretchTicks, d.stretchClear,
                    Integer.getInteger("agentspaces.loop.stretch-cap", d.stretchCap));
        }

        /**
         * These options with another max.
         *
         * @param m the max
         * @return the options
         */
        public Options withMax(int m) {
            return new Options(min, m, idle, growLateness, growTicks, stuck, stretchLateness, stretchTicks,
                    stretchClear, stretchCap);
        }

        /**
         * These options with another idle bound.
         *
         * @param d the bound
         * @return the options
         */
        public Options withIdle(Duration d) {
            return new Options(min, max, d, growLateness, growTicks, stuck, stretchLateness, stretchTicks,
                    stretchClear, stretchCap);
        }

        private static Duration millis(String key, Duration fallback) {
            Long v = Long.getLong("agentspaces.loop." + key);
            return v == null ? fallback : Duration.ofMillis(v);
        }
    }

    /**
     * The default max for a host: min(8, max(2, cores / 2)) (decision 2).
     *
     * @param cores the cores the JVM sees
     * @return the max
     */
    public static int defaultMax(int cores) {
        return Math.min(8, Math.max(2, cores / 2));
    }

    /**
     * What the pool did (EL-O-09, {@code core.loop.pool-threads}).
     *
     * @param threads        threads now
     * @param idleThreads    threads waiting for work now
     * @param peakThreads    the most threads at once since the pool was made
     * @param min            the min now (0 while no node is open)
     * @param max            the max
     * @param tasks          tasks run
     * @param grows          threads added by the tick
     * @param shrinks        threads that ended after the idle bound
     * @param replaced       threads started in place of one that died of an error
     * @param compensations  grows past the max in place of a thread stuck past the stuck bound
     * @param queued         tasks waiting now
     * @param waits          how long tasks waited for a thread (the most recent 4,096)
     * @param lastTickP99    the p99 wait the last tick judged, with the oldest queued task's age
     * @param atMax          whether the last tick found the pool at its max
     * @param users          nodes open on the pool
     * @param skippedTicks   ticks that found the pool's lock busy and judged nothing (WD-1)
     * @param retiring       threads a tick told to end that have not ended yet
     * @param promptGrows    threads added at once for a run held past the grow bound (EL-SP-08)
     */
    public record Stats(int threads, int idleThreads, int peakThreads, int min, int max, long tasks, long grows,
                        long shrinks, long replaced, long compensations, int queued, LatencyRing.Figures waits,
                        long lastTickP99, boolean atMax, int users, long skippedTicks, int retiring,
                        long promptGrows) {
    }

    private final String name;
    private final java.util.function.Supplier<Watchdog> watchdog;
    private final LongSupplier clock;
    private volatile Options options;
    private final ReentrantLock lock = new ReentrantLock();
    /** Guarded by {@link #lock}. */
    private final ArrayDeque<Item> queue = new ArrayDeque<>();
    /** Guarded by {@link #lock}. */
    private final List<Worker> workers = new ArrayList<>();
    /**
     * Idle threads, the most recently idle on top: new work wakes the top one, so a trickle of runs keeps one thread
     * busy and the surplus idle long enough to end (EL-SP-28; a FIFO hand-off rotated every thread and none ended).
     * Guarded by {@link #lock}.
     */
    private final ArrayDeque<Worker> idleStack = new ArrayDeque<>();
    /** Guarded by {@link #lock}. */
    private int idle;
    /** Guarded by {@link #lock}. */
    private int users;
    /** Guarded by {@link #lock}. */
    private int made;
    private Watchdog.Registration registration;
    private final LatencyRing waits = new LatencyRing(4096);
    private final AtomicLong tasks = new AtomicLong();
    private final AtomicLong grows = new AtomicLong();
    private final AtomicLong shrinks = new AtomicLong();
    private final AtomicLong replaced = new AtomicLong();
    private final AtomicLong compensations = new AtomicLong();
    private final AtomicLong skippedTicks = new AtomicLong();
    private final AtomicLong promptGrows = new AtomicLong();
    /** When the timer next checks for a held run (prompt growth), or {@link Long#MAX_VALUE}. */
    private volatile long promptCheckAt = Long.MAX_VALUE;
    private volatile Runnable backlogged = () -> { };
    // Mirrors for readers that take no lock.
    private volatile int threadsMirror;
    private volatile int idleMirror;
    private volatile int queuedMirror;
    private volatile int peak;
    private volatile int usersMirror;
    private volatile int retiringMirror;
    private volatile boolean atMax;
    private volatile long lastTickP99;
    // The tick's own state: touched by the watchdog's check only, one at a time.
    private long waitsMark;
    private int overTicks;
    /** Consecutive ticks that found more runs queued than threads serving them (EL-O-03, two ticks as the p99 rule). */
    private int queuedTicks;
    /** The latest tick whose waits passed the grow bound (or a run queued), or {@link Long#MIN_VALUE}. */
    private long lastPressure = Long.MIN_VALUE;

    /** A thread busy less than this share of the idle bound is idle (one in a hundred). */
    static final int IDLE_BUSY_DIVISOR = 100;

    /**
     * Makes a pool.
     *
     * @param name     the threads' name prefix ({@code agentspaces-loop} for the process's)
     * @param watchdog the watchdog that judges the pool while a node is open, on the same monotonic base as the clock
     * @param options  the bounds
     * @param clock    the monotonic reading waits and idle times are judged on (the time source's, or a test's)
     */
    public LoopPool(String name, java.util.function.Supplier<Watchdog> watchdog, Options options,
                    LongSupplier clock) {
        this.name = Objects.requireNonNull(name, "name");
        this.watchdog = Objects.requireNonNull(watchdog, "watchdog");
        this.options = Objects.requireNonNull(options, "options");
        this.clock = Objects.requireNonNull(clock, "clock");
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
     * Replaces the bounds (a test, or a host that reads them late); the size follows at the next tick.
     *
     * @param next the bounds
     */
    public void options(Options next) {
        this.options = Objects.requireNonNull(next, "next");
    }

    /**
     * Marks one node open on the pool: while any is, the pool keeps its min and its watchdog judges it.
     *
     * @return closes the mark, once
     */
    public AutoCloseable open() {
        boolean first;
        lock.lock();
        try {
            users++;
            usersMirror = users;
            first = users == 1;
        } finally {
            lock.unlock();
        }
        if (first) {
            followUsers();
        }
        java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();
        return () -> {
            if (closed.compareAndSet(false, true)) {
                release();
            }
        };
    }

    private void release() {
        boolean last;
        lock.lock();
        try {
            users--;
            usersMirror = users;
            last = users == 0;
            if (last) {
                for (Worker w : workers) {
                    w.wake.signal(); // idle threads re-read their wait: with no node open the min is 0
                }
            }
        } finally {
            lock.unlock();
        }
        if (last) {
            followUsers();
        }
    }

    /**
     * Brings the watchdog registration in line with the nodes open now: registered while any is, closed once none is.
     * It reads the count under this monitor, so a node closing while another opens leaves the pool watched whichever
     * reaches here first (review defect R2, srad/agentspaces-event-loop.md §10.23: the closing node used to close the
     * opening node's new registration, and the open pool was never ticked again).
     */
    private void followUsers() {
        synchronized (this) {
            boolean open = usersMirror > 0;
            if (open && registration == null) {
                registration = watchdog.get().register(this);
            } else if (!open && registration != null) {
                Watchdog.Registration r = registration;
                registration = null;
                r.close();
            }
        }
    }

    @Override
    public void execute(Runnable task) {
        Objects.requireNonNull(task, "task");
        Item item = new Item(task, clock.getAsLong());
        boolean tell = false;
        lock.lock();
        try {
            queue.add(item);
            queuedMirror = queue.size();
            int taking = idle; // idle threads, and threads woken that have not taken their run yet
            if (!idleStack.isEmpty()) {
                idleStack.pop().wake.signal(); // the most recently idle thread
            } else if (idle == 0 && workers.size() < Math.max(1, minLocked())) {
                startWorkerLocked(); // the first thread, or up to the min; growth past it waits for the tick
                taking = 1;
            }
            // More runs wait than threads will take at once: the timer looks again at the grow bound. A run handed to
            // the pool while its one woken thread has not yet taken an earlier run arms it too (review defect R3).
            if (queue.size() > taking && promptCheckAt == Long.MAX_VALUE) {
                promptCheckAt = item.enqueued + options.growLateness().toNanos();
                tell = true;
            }
        } finally {
            lock.unlock();
        }
        if (tell) {
            backlogged.run(); // the timer re-reads its wait with the check in it
        }
    }

    /**
     * Sets what the pool runs when a run waits with no thread free (the timer of its time source re-reads its wait).
     *
     * @param hook the hook
     */
    void onBacklog(Runnable hook) {
        this.backlogged = Objects.requireNonNull(hook, "hook");
    }

    /**
     * When the timer thread next checks for a run held past the grow bound, or {@link Long#MAX_VALUE}.
     *
     * @return the instant on the pool's clock
     */
    public long promptCheckAt() {
        return promptCheckAt;
    }

    /**
     * The prompt check (EL-SP-08): when a queued run has waited past the grow bound, no thread is free, and a thread is
     * held by one run longer than the bound, one thread starts now, up to the max (threads stuck past the stuck bound
     * do not count). Called by the timer thread at {@link #promptCheckAt()}; it only tries the pool's lock.
     *
     * @param nowNanos the monotonic now, on the pool's clock
     */
    public void promptCheck(long nowNanos) {
        if (!lock.tryLock()) {
            promptCheckAt = nowNanos + 1_000_000L;
            return;
        }
        try {
            Options o = options;
            long grow = o.growLateness().toNanos();
            Item head = queue.peek();
            if (head == null || idle > 0) {
                promptCheckAt = Long.MAX_VALUE;
                return;
            }
            if (nowNanos - head.enqueued < grow) {
                promptCheckAt = head.enqueued + grow;
                return;
            }
            int held = 0;
            int stuck = 0;
            for (Worker w : workers) {
                long busy = w.busySince;
                if (busy != 0 && nowNanos - busy > grow) {
                    held++;
                    if (nowNanos - busy > o.stuck().toNanos()) {
                        stuck++;
                    }
                }
            }
            if (held > 0 && workers.size() - stuck < o.max()) {
                startWorkerLocked();
                promptGrows.incrementAndGet();
                if (workers.size() > o.max()) {
                    compensations.incrementAndGet();
                }
                int size = workers.size();
                int h = held;
                LOG.log(System.Logger.Level.DEBUG, () -> name + " grew to " + size + " threads at once: a run waited "
                        + (nowNanos - head.enqueued) / 1_000_000 + " ms with " + h + " threads held by one run");
            }
            promptCheckAt = nowNanos + grow; // the next look, while runs still wait
        } finally {
            lock.unlock();
        }
    }

    /**
     * Whether a run waits for a thread now. A node's {@link SerialExecutor} drained here hands its thread back after a
     * task while one does, so a node woken by every run cannot hold a thread from every other node (review defect R1,
     * srad/agentspaces-event-loop.md §10.23). Reads a mirror; takes no lock.
     *
     * @return whether the queue holds a run
     */
    public boolean waiting() {
        return queuedMirror > 0;
    }

    /**
     * Threads now.
     *
     * @return the count
     */
    public int threads() {
        return threadsMirror;
    }

    /**
     * Whether the last tick found the pool at its max (the stretch rule reads it, EL-O-04).
     *
     * @return whether at the max
     */
    public boolean atMax() {
        return atMax;
    }

    /**
     * The counters.
     *
     * @return a snapshot
     */
    public Stats stats() {
        Options o = options;
        return new Stats(threadsMirror, idleMirror, peak, usersMirror > 0 ? o.min() : 0, o.max(), tasks.get(),
                grows.get(), shrinks.get(), replaced.get(), compensations.get(), queuedMirror, waits.snapshot(),
                lastTickP99, atMax, usersMirror, skippedTicks.get(), retiringMirror, promptGrows.get());
    }

    /**
     * The pool check of the watchdog's tick (§3.9, EL-O-03): judges the waits since the last tick and grows or
     * shrinks by at most one thread. It only tries the pool's lock and skips the tick when another holds it (WD-1).
     *
     * @param nowNanos the monotonic now, on the pool's clock
     */
    @Override
    public void watchdogTick(long nowNanos) {
        if (!lock.tryLock()) {
            skippedTicks.incrementAndGet();
            return;
        }
        try {
            Options o = options;
            LatencyRing.Figures f = waits.since(waitsMark);
            waitsMark = f.total();
            Item head = queue.peek();
            long headAge = head == null ? 0 : nowNanos - head.enqueued;
            long p99 = Math.max(f.p99Nanos(), headAge);
            lastTickP99 = p99;
            overTicks = p99 > o.growLateness().toNanos() ? overTicks + 1 : 0;
            for (Worker w : workers) {
                w.sample(nowNanos, o.idle().toNanos());
            }
            int stuck = 0;
            for (Worker w : workers) {
                long busy = w.busySince;
                if (busy != 0 && nowNanos - busy > o.stuck().toNanos()) {
                    stuck++;
                }
            }
            int serving = workers.size() - stuck;
            int queued = queue.size();
            queuedTicks = queued > serving ? queuedTicks + 1 : 0;
            // Pressure is judged as growth is, on two ticks running (EL-O-03, §10.35): a lone tick that sampled runs
            // queued, or one wait past the grow bound, is a moment. Co-resident loops that swept together in a burst
            // keep sweeping together, so such a tick comes round after it, and counted alone it restarted every surplus
            // thread's 60 s (EL-SP-28's soak failure, srad/agentspaces-event-loop.md §10.47).
            if (overTicks >= o.growTicks() || queuedTicks >= o.growTicks()) {
                lastPressure = nowNanos;
            }
            if (serving < o.max() && (overTicks >= o.growTicks() || queuedTicks >= o.growTicks())) {
                startWorkerLocked();
                grows.incrementAndGet();
                if (workers.size() > o.max()) {
                    compensations.incrementAndGet();
                }
                int size = workers.size();
                int servedBy = serving;
                LOG.log(System.Logger.Level.DEBUG, () -> name + " grew to " + size + " threads (p99 wait "
                        + p99 / 1_000_000 + " ms, " + queued + " queued, " + servedBy + " serving; EL-O-03)");
                serving++;
            } else {
                boolean told = false;
                for (Worker w : workers) {
                    if (workers.size() - retiring() <= minLocked()) {
                        break;
                    }
                    if (w.idling && !w.retire && idleLong(w, nowNanos, o)) {
                        w.retire = true; // ends when it wakes
                        idleStack.remove(w);
                        w.wake.signal();
                        told = true;
                    }
                }
                if (told) {
                    retiringMirror = retiring();
                }
            }
            atMax = serving >= o.max();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Whether an idle thread has been idle over the bound: idle the whole bound, or born before it and busy under 1%
     * of it, with no tick of it under pressure (waits past the grow bound, or more runs queued than threads serve them,
     * at two ticks running, as growth judges).
     */
    private boolean idleLong(Worker w, long now, Options o) {
        long bound = o.idle().toNanos();
        if (w.idleSince != 0 && now - w.idleSince >= bound) {
            return true;
        }
        if (now - w.born < bound || (lastPressure != Long.MIN_VALUE && now - lastPressure < bound)) {
            return false;
        }
        long busy = w.busyOver(now, bound);
        return busy >= 0 && busy < bound / IDLE_BUSY_DIVISOR;
    }

    private int retiring() {
        int n = 0;
        for (Worker w : workers) {
            if (w.retire) {
                n++;
            }
        }
        return n;
    }

    private int minLocked() {
        return users > 0 ? options.min() : 0;
    }

    private void startWorkerLocked() {
        Worker w = new Worker();
        Thread t = Thread.ofPlatform().name(name + "-" + (++made)).daemon(true).unstarted(w::run);
        t.setUncaughtExceptionHandler((thread, e) -> LOG.log(System.Logger.Level.ERROR, "a " + name
                + " thread died; the pool starts another while work is queued", e));
        w.thread = t;
        workers.add(w);
        threadsMirror = workers.size();
        if (workers.size() > peak) {
            peak = workers.size();
        }
        t.start();
    }

    /**
     * Whether an idle thread ends now: when the tick told it to (one a tick while a node is open), or, with no node
     * open and so no tick, when it has idled past the bound on its own.
     */
    private boolean retireLocked(Worker w, long now) {
        if (w.retire) {
            return true;
        }
        return users == 0 && w.idleSince != 0 && now - w.idleSince >= options.idle().toNanos();
    }

    private void ended(Worker w, boolean died) {
        lock.lock();
        try {
            if (!workers.remove(w)) {
                return;
            }
            threadsMirror = workers.size();
            retiringMirror = retiring();
            if (died) {
                if (!queue.isEmpty() && workers.size() < options.max()) {
                    startWorkerLocked();
                    replaced.incrementAndGet();
                }
            } else {
                shrinks.incrementAndGet();
                int size = workers.size();
                LOG.log(System.Logger.Level.DEBUG, () -> name + " shrank to " + size + " threads (EL-O-03)");
            }
        } finally {
            lock.unlock();
        }
    }

    /** One pooled thread: takes a task, runs it, and waits for the next until it has idled past the bound. */
    private final class Worker {
        final Condition wake = lock.newCondition();
        final long born = clock.getAsLong();
        /** Busy time of finished tasks, summed. */
        final AtomicLong busyTotal = new AtomicLong();
        /** (tick instant, busy total then), oldest first, kept for one idle bound: touched by the tick only. */
        final ArrayDeque<long[]> samples = new ArrayDeque<>();
        volatile Thread thread;
        volatile long busySince;
        volatile long idleSince;
        volatile boolean idling;
        volatile boolean retire;

        void run() {
            boolean died = true;
            try {
                while (true) {
                    Item item = take();
                    if (item == null) {
                        died = false;
                        return;
                    }
                    long start = clock.getAsLong();
                    waits.record(start - item.enqueued);
                    busySince = start == 0 ? 1 : start;
                    try {
                        item.task.run();
                    } catch (VirtualMachineError e) {
                        throw e; // the thread dies; the pool starts another while work is queued
                    } catch (Throwable e) {
                        LOG.log(System.Logger.Level.WARNING, "a task on " + name + " threw", e);
                    } finally {
                        busyTotal.addAndGet(Math.max(0, clock.getAsLong() - start));
                        busySince = 0;
                        tasks.incrementAndGet();
                        Thread.interrupted(); // an interrupt aimed at a run (a wedge, WD-2) ends with it
                    }
                }
            } finally {
                ended(this, died);
            }
        }

        /** The busy time so far, a running task's part included. */
        long busyNow(long now) {
            long b = busySince;
            return busyTotal.get() + (b == 0 ? 0 : Math.max(0, now - b));
        }

        /** The tick's sample of the busy time, keeping one idle bound of them. */
        void sample(long now, long bound) {
            samples.addLast(new long[] {now, busyNow(now)});
            while (samples.size() > 1) { // keep the newest sample at least a bound old, and every later one
                java.util.Iterator<long[]> it = samples.iterator();
                it.next();
                if (now - it.next()[0] >= bound) {
                    samples.removeFirst();
                } else {
                    break;
                }
            }
        }

        /** Busy time over the last bound, from the oldest sample at least a bound old, or -1 when none is. */
        long busyOver(long now, long bound) {
            long[] from = null;
            for (long[] s : samples) {
                if (now - s[0] >= bound) {
                    from = s;
                } else {
                    break;
                }
            }
            return from == null ? -1 : busyNow(now) - from[1];
        }

        /** The next task, or null when this thread is to end. */
        private Item take() {
            lock.lock();
            try {
                idleSince = 0;
                while (true) {
                    Item item = queue.poll();
                    if (item != null) {
                        queuedMirror = queue.size();
                        return item;
                    }
                    long now = clock.getAsLong();
                    if (retireLocked(this, now)) {
                        return null;
                    }
                    if (idleSince == 0) {
                        idleSince = now == 0 ? 1 : now;
                    }
                    idle++;
                    idleMirror = idle;
                    idling = true;
                    idleStack.push(this);
                    try {
                        if (users == 0) {
                            // no node open, so no watchdog tick: the thread ends on its own after the idle bound
                            long left = options.idle().toNanos() - (now - idleSince);
                            if (left > 0) {
                                wake.awaitNanos(left);
                            }
                        } else {
                            wake.await(); // new work or the tick (an idle thread's end, EL-O-03) wakes this one
                        }
                    } catch (InterruptedException e) {
                        // a stale interrupt; carry on
                    } finally {
                        idleStack.remove(this); // gone already when work or the tick woke it
                        idling = false;
                        idle--;
                        idleMirror = idle;
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private record Item(Runnable task, long enqueued) {
    }

    // Watched: the pool has no run of its own to judge; its check is the tick above (§3.9's "pool" row).

    @Override
    public String name() {
        return "loop-pool:" + name;
    }

    @Override
    public long runs() {
        return tasks.get();
    }

    @Override
    public long runStartedNanos() {
        return 0;
    }

    @Override
    public String runningIn() {
        return "";
    }

    @Override
    public long armedNanos() {
        return Long.MAX_VALUE;
    }

    @Override
    public Thread thread() {
        return null;
    }

    @Override
    public int queued() {
        return 0;
    }

    @Override
    public boolean dispatched() {
        return false;
    }

    @Override
    public long generation() {
        return 0;
    }

    @Override
    public void reinitialise(Reason reason) {
        // nothing to rebuild: a dead thread is replaced at once, a stuck one is compensated by the tick
    }
}
