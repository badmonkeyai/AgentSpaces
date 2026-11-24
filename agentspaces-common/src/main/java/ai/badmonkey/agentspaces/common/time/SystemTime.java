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
import java.time.Instant;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The production {@link TimeSource} (srad/agentspaces-event-loop.md §3.1, §3.5): wall time from the system clock,
 * monotonic time from {@link System#nanoTime()}, and one platform timer thread per process, {@code agentspaces-timer},
 * holding a delay queue.
 *
 * <p>The timer thread never runs a task body (TS-2): at each deadline it starts the task on a virtual thread. It starts
 * with the first task that has to wait, so a JVM that schedules nothing never starts it (TS-4). A task with no delay
 * skips the timer thread and starts at once. Tasks due at the same instant are dispatched in the order they were
 * scheduled (TS-3).
 *
 * <p>The watchdog watches this thread and this thread watches the watchdog (§3.9, EL-H-12, EL-Q-AVL-02): the timer
 * is a {@link Watched} ("timer") whose death or stall the watchdog finds and repairs by {@link #restartTimer()}, which
 * keeps every pending deadline; and on each of its own wakes the timer reads the watchdog's heartbeat and restarts a
 * starved watchdog (WD-6). A timer thread that dies is also replaced by the next {@code schedule}.
 *
 * <p>The wall-step signal (TS-6, EL-C-12): on every wake the timer compares (wall - monotonic) with its last reading;
 * a move of more than {@link TimeSource#WALL_STEP_THRESHOLD} is an NTP step or a suspend, and each
 * {@link #onWallStep} listener is told on a virtual thread of its own. The timer wakes at least at the earliest armed
 * deadline (a node's loop always arms its backstop), so a step is seen within one timer wake.
 *
 * <p>Node runs and flushes ({@link #scheduleOnLoop}) go to the process's elastic {@linkplain #loopPool() loop pool},
 * {@code agentspaces-loop}, apart from the application's virtual-thread carriers (decision 2, EL-O-02); every other
 * task starts on a virtual thread. The timer records each timed task's dispatch latency, its start on its thread
 * less its deadline ({@code core.timer.dispatch-latency}, EL-X-10, {@link #dispatchLatency()}).
 *
 * <p>Not built: the alignment slack of co-resident deadlines (EL-L-12, a SHOULD; srad/agentspaces-event-loop.md §10.47
 * records why).
 */
public final class SystemTime implements TimeSource {

    /** The one per process. */
    static final SystemTime INSTANCE = new SystemTime("agentspaces-timer");

    private static final System.Logger LOG = System.getLogger(SystemTime.class.getName());

    /**
     * The OS lets a parked thread wake late by a share of its wait (macOS: about a quarter, at most about 5 ms, measured
     * 2026-11-21), which put a 20 ms flush 4 to 5 ms late. A wait longer than this is cut short by
     * {@link #MAX_EARLY_NANOS} or a quarter of it, whichever is less, and the rest is waited again: a short wait wakes
     * nearly on time, at the cost of two or three timer wakes per deadline (srad/agentspaces-event-loop.md §10.6).
     */
    static final long SPLIT_ABOVE_NANOS = 2_000_000L;
    /** The most a long wait is cut short by. */
    static final long MAX_EARLY_NANOS = 5_000_000L;

    private static final int PENDING = 0;
    private static final int STARTED = 1;
    private static final int CANCELLED = 2;

    private final String threadName;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final PriorityQueue<Task> queue = new PriorityQueue<>();
    private final AtomicLong sequence = new AtomicLong();
    private final Thread.Builder taskThreads = Thread.ofVirtual().name("agentspaces-task-", 0);
    /** Guarded by {@link #lock}. */
    private Thread timer;
    /** Bumped by each restart; a timer thread of an older generation stops. Guarded by {@link #lock}. */
    private long timerGeneration;
    private final AtomicLong dispatched = new AtomicLong();
    private final AtomicLong timerRestarts = new AtomicLong();
    // The timer's heartbeat for the watchdog (NL-9's shape): read with no lock.
    private volatile Thread liveTimer;
    private volatile long iterations;
    private volatile long headDue = Long.MAX_VALUE;
    private volatile int size;
    private volatile Error scriptedFault;
    private volatile Watchdog watchdog;
    private final Watched timerView = new TimerView();
    private final java.util.List<java.util.function.Consumer<Duration>> wallListeners =
            new java.util.concurrent.CopyOnWriteArrayList<>();
    /** (wall - monotonic) at the last check, in nanoseconds; {@link Long#MIN_VALUE} before the first. */
    private volatile long lastSkewNanos = Long.MIN_VALUE;
    private final AtomicLong wallSteps = new AtomicLong();
    private final java.util.List<java.util.function.Consumer<Duration>> lateListeners =
            new java.util.concurrent.CopyOnWriteArrayList<>();
    private final AtomicLong lateWakes = new AtomicLong();
    /** The instant the timer last slept to, read at its next wake; {@link Long#MAX_VALUE} when it slept with none. */
    private volatile long sleptToNanos = Long.MAX_VALUE;
    private final LoopPool loopPool;
    private final LatencyRing dispatchLatency = new LatencyRing(4096);

    /**
     * A separate instance, for tests of this class only; production code uses {@link TimeSource#system()}.
     *
     * @param threadName the timer thread's name
     */
    SystemTime(String threadName) {
        this.threadName = Objects.requireNonNull(threadName, "threadName");
        String pool = "agentspaces-timer".equals(threadName) ? "agentspaces-loop" : threadName + "-loop";
        this.loopPool = new LoopPool(pool, this::watchdog, LoopPool.Options.fromSystemProperties(), System::nanoTime);
        loopPool.onBacklog(this::poolBacklogged);
    }

    /** A run waits on the pool with no thread free: the timer re-reads its wait, which now holds the prompt check. */
    private void poolBacklogged() {
        lock.lock();
        try {
            if (timer == null || !timer.isAlive()) {
                startTimerLocked();
            } else {
                changed.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * The process's elastic loop pool, {@code agentspaces-loop} (an isolated source has its own, named after its
     * timer), where node runs and flushes execute (decision 2).
     *
     * @return the pool
     */
    @Override
    public LoopPool loopPool() {
        return loopPool;
    }

    /**
     * How late timed tasks started on their thread against their deadlines, the most recent 4,096
     * ({@code core.timer.dispatch-latency}, EL-X-10): the timer's own lateness plus the wait for a pool thread or a
     * carrier.
     *
     * @return the figures
     */
    public LatencyRing.Figures dispatchLatency() {
        return dispatchLatency.snapshot();
    }

    /**
     * The dispatch latencies recorded since a count {@link #dispatchCount()} returned.
     *
     * @param fromCount the count the window starts at
     * @return the figures
     */
    public LatencyRing.Figures dispatchLatencySince(long fromCount) {
        return dispatchLatency.since(fromCount);
    }

    /**
     * Dispatch latencies recorded so far, to open a window for {@link #dispatchLatencySince}.
     *
     * @return the count
     */
    public long dispatchCount() {
        return dispatchLatency.count();
    }

    /**
     * A separate system time source with its own timer thread, for tests that script a fault on a timer (EL-SP-22)
     * without touching the process's; production code uses {@link TimeSource#system()}.
     *
     * @param threadName the timer thread's name
     * @return the source
     */
    public static SystemTime isolated(String threadName) {
        return new SystemTime(threadName);
    }

    /**
     * This source's watchdog: for the process's source, the process watchdog {@code agentspaces-watchdog}; for an
     * isolated source, the one attached by {@link Watchdog#threaded}, or else one made here.
     *
     * @return the watchdog
     */
    @Override
    public Watchdog watchdog() {
        Watchdog w = watchdog;
        if (w != null) {
            return w;
        }
        synchronized (this) {
            if (watchdog == null) {
                Watchdog.threaded(this == INSTANCE ? "agentspaces-watchdog" : threadName + "-watchdog", this,
                        Watchdog.Options.fromSystemProperties());
            }
            return watchdog;
        }
    }

    /** Attaches the watchdog this source's timer thread watches (WD-6); once. */
    synchronized void attach(Watchdog w) {
        if (watchdog != null) {
            throw new IllegalStateException("a watchdog is attached to " + threadName + " already");
        }
        watchdog = w;
    }

    /** The timer thread as the watchdog sees it. */
    Watched timerView() {
        return timerView;
    }

    /**
     * Test hook (EL-SP-22): the timer thread throws the error at its next wake and dies, as a thread does on an
     * {@code Error} the dispatch could not contain.
     *
     * @param fault the error
     */
    public void scriptTimerFault(Error fault) {
        scriptedFault = Objects.requireNonNull(fault, "fault");
        lock.lock();
        try {
            changed.signal();
        } finally {
            lock.unlock();
        }
    }

    /** Wakes an idle timer thread so it re-reads its wait (a watchdog started, WD-6). */
    void nudgeTimer() {
        if (lock.tryLock()) {
            try {
                changed.signal();
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Starts a new timer thread that takes over every pending deadline; the old thread, if alive, stops at its next
     * wake. The watchdog calls it for a dead or stalled timer (EL-Q-AVL-02).
     */
    public void restartTimer() {
        lock.lock();
        try {
            timerRestarts.incrementAndGet();
            Thread old = timer;
            startTimerLocked();
            if (old != null) {
                old.interrupt(); // best effort: an old thread stuck in a wait wakes and sees it is replaced
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Restarts of the timer thread by {@link #restartTimer()}.
     *
     * @return the count
     */
    public long timerRestarts() {
        return timerRestarts.get();
    }

    /**
     * The timer thread now, or null when none was started.
     *
     * @return the thread
     */
    public Thread timerThread() {
        return liveTimer;
    }

    private void startTimerLocked() {
        long generation = ++timerGeneration;
        Thread t = Thread.ofPlatform().name(threadName).daemon(true).unstarted(() -> timerLoop(generation));
        t.setUncaughtExceptionHandler((thread, e) -> LOG.log(System.Logger.Level.ERROR, "the timer thread " + threadName
                + " died; the next schedule or the watchdog's next tick starts another with every pending deadline",
                e));
        timer = t;
        liveTimer = t;
        t.start();
    }

    @Override
    public Instant instant() {
        return Instant.now();
    }

    @Override
    public long millis() {
        return System.currentTimeMillis();
    }

    @Override
    public long monoNanos() {
        return System.nanoTime();
    }

    @Override
    public boolean virtual() {
        return false;
    }

    @Override
    public AutoCloseable onWallStep(java.util.function.Consumer<Duration> step) {
        Objects.requireNonNull(step, "step");
        synchronized (wallListeners) {
            if (wallListeners.isEmpty()) {
                // The reading steps are judged from; nothing was checked while nobody listened, so start afresh.
                lastSkewNanos = Long.MIN_VALUE;
                checkWallStep(System.currentTimeMillis(), System.nanoTime());
            }
            wallListeners.add(step);
        }
        return () -> wallListeners.remove(step);
    }

    @Override
    public AutoCloseable onLateWake(java.util.function.Consumer<Duration> late) {
        Objects.requireNonNull(late, "late");
        lateListeners.add(late);
        return () -> lateListeners.remove(late);
    }

    /**
     * Late wakes this source has raised ({@link TimeSource#onLateWake}).
     *
     * @return the count since the source was made
     */
    public long lateWakes() {
        return lateWakes.get();
    }

    /**
     * Raises a late wake when the timer woke more than {@link TimeSource#LATE_WAKE_THRESHOLD} after the instant it
     * slept to and no wall step was raised at this wake (SPEC v0.1.37 §9.4): a frozen process (SIGSTOP, a long GC or
     * VM pause) or a monotonic clock that counted a sleep. The timer calls it on every wake; tests call it with their
     * own readings.
     *
     * @param sleptToNanos the instant the timer slept to, or {@link Long#MAX_VALUE} for none
     * @param monoNanos    the monotonic reading at the wake
     * @param stepped      whether this wake raised a wall step
     * @return how late the wake was, when raised, or null
     */
    Duration checkLateWake(long sleptToNanos, long monoNanos, boolean stepped) {
        if (stepped || sleptToNanos == Long.MAX_VALUE || monoNanos - sleptToNanos <= LATE_WAKE_THRESHOLD.toNanos()) {
            return null;
        }
        Duration late = Duration.ofNanos(monoNanos - sleptToNanos);
        lateWakes.incrementAndGet();
        for (java.util.function.Consumer<Duration> listener : lateListeners) {
            taskThreads.start(() -> {
                try {
                    listener.accept(late);
                } catch (Throwable e) {
                    LOG.log(System.Logger.Level.WARNING, "a late-wake listener threw", e);
                }
            });
        }
        return late;
    }

    /**
     * Wall steps this source has raised (TS-6).
     *
     * @return the count since the source was made
     */
    public long wallSteps() {
        return wallSteps.get();
    }

    /** Test hook: sets the reading the next check is judged from, raising nothing. */
    void primeWallReading(long wallMillis, long monoNanos) {
        lastSkewNanos = wallMillis * 1_000_000L - monoNanos;
    }

    /**
     * Compares (wall - monotonic) with the last reading and raises a wall step when it moved by more than the
     * threshold (TS-6). The timer calls it on every wake with the system's readings; tests call it with their own.
     *
     * @param wallMillis the wall reading
     * @param monoNanos  the monotonic reading
     * @return the step raised, or null
     */
    Duration checkWallStep(long wallMillis, long monoNanos) {
        long skew;
        try {
            skew = Math.subtractExact(Math.multiplyExact(wallMillis, 1_000_000L), monoNanos);
        } catch (ArithmeticException e) {
            return null;
        }
        long last = lastSkewNanos;
        lastSkewNanos = skew;
        if (last == Long.MIN_VALUE) {
            return null;
        }
        long moved = skew - last;
        if (Math.abs(moved) <= WALL_STEP_THRESHOLD.toNanos()) {
            return null; // a slew, or nothing: wall-anchored deadlines absorb it on their next run (§3.6)
        }
        Duration step = Duration.ofNanos(moved);
        wallSteps.incrementAndGet();
        for (java.util.function.Consumer<Duration> listener : wallListeners) {
            taskThreads.start(() -> {
                try {
                    listener.accept(step);
                } catch (Throwable e) {
                    LOG.log(System.Logger.Level.WARNING, "a wall-step listener threw", e);
                }
            });
        }
        return step;
    }

    @Override
    public void sleep(Duration duration) throws InterruptedException {
        long nanos = duration.toNanos();
        if (nanos > 0) {
            Thread.sleep(Duration.ofNanos(nanos));
        }
    }

    @Override
    public Scheduled schedule(Duration delay, Runnable task) {
        Objects.requireNonNull(delay, "delay");
        long nanos = delay.isNegative() ? 0 : saturatedNanos(delay);
        long deadline;
        try {
            deadline = Math.addExact(System.nanoTime(), nanos);
        } catch (ArithmeticException e) {
            deadline = Long.MAX_VALUE;
        }
        return scheduleAt(deadline, task);
    }

    @Override
    public Scheduled scheduleAt(long monoDeadlineNanos, Runnable task) {
        return enqueue(monoDeadlineNanos, task, false);
    }

    @Override
    public Scheduled scheduleOnLoop(long monoDeadlineNanos, Runnable task) {
        return enqueue(monoDeadlineNanos, task, true);
    }

    private Scheduled enqueue(long monoDeadlineNanos, Runnable task, boolean onLoop) {
        Objects.requireNonNull(task, "task");
        Task t = new Task(monoDeadlineNanos, sequence.getAndIncrement(), task, onLoop);
        if (monoDeadlineNanos == Long.MAX_VALUE) {
            return t; // never due (DueWork.NEVER): held by nobody, cancellable, never run
        }
        if (monoDeadlineNanos - System.nanoTime() <= 0) {
            dispatch(t); // due already: no hop through the timer thread
            return t;
        }
        t.timed = true;
        lock.lock();
        try {
            queue.add(t);
            size = queue.size();
            if (queue.peek() == t) {
                headDue = t.due;
            }
            if (timer == null || !timer.isAlive()) {
                startTimerLocked();
            } else if (queue.peek() == t) {
                changed.signal(); // a new head: the timer re-reads its wait
            }
        } finally {
            lock.unlock();
        }
        return t;
    }

    /**
     * Whether the timer thread is running (TS-4: a process that never scheduled a delayed task has none).
     *
     * @return whether it has been started and is alive
     */
    boolean timerStarted() {
        lock.lock();
        try {
            return timer != null && timer.isAlive();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Tasks this source has started on virtual threads.
     *
     * @return the count since the source was made
     */
    long dispatched() {
        return dispatched.get();
    }

    /**
     * Tasks waiting in the delay queue.
     *
     * @return the queue's size
     */
    int queued() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    private void timerLoop(long generation) {
        while (true) {
            Error fault = scriptedFault;
            if (fault != null) {
                scriptedFault = null;
                throw fault; // EL-SP-22: the thread dies; the queue keeps every deadline
            }
            iterations++;
            long sleptTo = sleptToNanos;
            sleptToNanos = Long.MAX_VALUE;
            Duration stepped = null;
            if (!wallListeners.isEmpty()) {
                stepped = checkWallStep(System.currentTimeMillis(), System.nanoTime()); // TS-6, on every wake
            }
            if (!lateListeners.isEmpty()) {
                checkLateWake(sleptTo, System.nanoTime(), stepped != null); // a frozen process is a wake (§9.4)
            }
            Watchdog w = watchdog;
            if (w != null) {
                w.timerWoke(System.nanoTime()); // WD-6: the timer watches the watchdog
            }
            long poolCheck = loopPool.promptCheckAt();
            if (poolCheck != Long.MAX_VALUE && System.nanoTime() - poolCheck >= 0) {
                loopPool.promptCheck(System.nanoTime()); // a run held past the grow bound: a thread at once (EL-SP-08)
            }
            Task due;
            lock.lock();
            try {
                if (generation != timerGeneration) {
                    return; // replaced by a restart
                }
                Task head = queue.peek();
                headDue = head == null ? Long.MAX_VALUE : head.due;
                // WD-6 must not depend on there being work: while a threaded watchdog runs, the timer also wakes when
                // the watchdog's heartbeat would be three periods old, even with nothing queued (§10.9).
                long watchBy = w == null ? Long.MAX_VALUE : w.heartbeatDeadline();
                long watchWait = watchBy == Long.MAX_VALUE ? Long.MAX_VALUE : Math.max(1, watchBy - System.nanoTime());
                long poolBy = loopPool.promptCheckAt();
                if (poolBy != Long.MAX_VALUE) {
                    watchWait = Math.min(watchWait, Math.max(1, poolBy - System.nanoTime()));
                }
                if (head == null) {
                    if (watchWait == Long.MAX_VALUE) {
                        changed.await();
                    } else {
                        sleptToNanos = System.nanoTime() + watchWait;
                        changed.awaitNanos(watchWait);
                    }
                    continue;
                }
                long wait = head.due - System.nanoTime();
                if (wait > 0) {
                    long split = wait > SPLIT_ABOVE_NANOS ? wait - Math.min(wait / 4, MAX_EARLY_NANOS) : wait;
                    long sleep = Math.min(split, watchWait);
                    sleptToNanos = System.nanoTime() + sleep;
                    changed.awaitNanos(sleep);
                    continue;
                }
                due = queue.poll();
                size = queue.size();
                Task next = queue.peek();
                headDue = next == null ? Long.MAX_VALUE : next.due;
            } catch (InterruptedException e) {
                // Interrupted only by a restart, which the generation check above sees; otherwise carry on.
                continue;
            } finally {
                lock.unlock();
            }
            try {
                dispatch(due);
            } catch (Throwable e) {
                LOG.log(System.Logger.Level.ERROR, "the timer could not start a task; it keeps running", e);
            }
        }
    }

    private void dispatch(Task t) {
        if (!t.state.compareAndSet(PENDING, STARTED)) {
            return; // cancelled
        }
        dispatched.incrementAndGet();
        Runnable body = () -> {
            if (t.timed) {
                dispatchLatency.record(System.nanoTime() - t.due); // EL-X-10: the deadline to the task's start
            }
            try {
                t.task.run();
            } catch (VirtualMachineError e) {
                if (t.onLoop) {
                    throw e; // the pool replaces the thread; the watchdog judges the loop it ran
                }
                LOG.log(System.Logger.Level.ERROR, "a scheduled task threw", e);
            } catch (Throwable e) {
                LOG.log(System.Logger.Level.WARNING, "a scheduled task threw", e);
            }
        };
        if (t.onLoop) {
            loopPool.execute(body); // a node run or a flush: off the application's carriers (decision 2)
        } else {
            taskThreads.start(body);
        }
    }

    private boolean remove(Task t) {
        lock.lock();
        try {
            boolean removed = queue.remove(t);
            size = queue.size();
            Task head = queue.peek();
            headDue = head == null ? Long.MAX_VALUE : head.due;
            return removed;
        } finally {
            lock.unlock();
        }
    }

    /** The timer thread as the watchdog judges it: no lock taken (WD-1). */
    private final class TimerView implements Watched {
        @Override
        public String name() {
            return "timer:" + threadName;
        }

        @Override
        public long runs() {
            return iterations;
        }

        @Override
        public long runStartedNanos() {
            return 0; // the timer only dispatches; it has no runs to judge as slow or wedged
        }

        @Override
        public String runningIn() {
            return "";
        }

        @Override
        public long armedNanos() {
            return size == 0 ? Long.MAX_VALUE : headDue;
        }

        @Override
        public Thread thread() {
            return liveTimer;
        }

        @Override
        public int queued() {
            return 0; // deadlines are armed, not wakes
        }

        @Override
        public boolean dispatched() {
            return false;
        }

        @Override
        public long generation() {
            return timerRestarts.get();
        }

        @Override
        public void reinitialise(Reason reason) {
            restartTimer();
        }
    }

    private static long saturatedNanos(Duration d) {
        try {
            return d.toNanos();
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private final class Task implements Scheduled, Comparable<Task> {
        final long due;
        final long seq;
        final Runnable task;
        final boolean onLoop;
        /** Waited in the delay queue (its deadline was ahead when scheduled): its dispatch latency is recorded. */
        volatile boolean timed;
        final AtomicInteger state = new AtomicInteger(PENDING);

        Task(long due, long seq, Runnable task, boolean onLoop) {
            this.due = due;
            this.seq = seq;
            this.task = task;
            this.onLoop = onLoop;
        }

        @Override
        public boolean cancel() {
            if (!state.compareAndSet(PENDING, CANCELLED)) {
                return false;
            }
            remove(this);
            return true;
        }

        @Override
        public long dueNanos() {
            return due;
        }

        @Override
        public boolean dispatched() {
            return state.get() == STARTED;
        }

        @Override
        public int compareTo(Task o) {
            int c = Long.compare(due - o.due, 0); // nanoTime order, overflow-safe within 292 years
            return c != 0 ? c : Long.compare(seq, o.seq);
        }
    }
}
