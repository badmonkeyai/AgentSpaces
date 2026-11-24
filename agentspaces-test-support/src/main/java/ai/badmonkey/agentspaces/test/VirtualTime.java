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
package ai.badmonkey.agentspaces.test;

import ai.badmonkey.agentspaces.common.time.TimeSource;
import ai.badmonkey.agentspaces.common.time.Watchdog;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A {@link TimeSource} that moves only when a test moves it (srad/agentspaces-event-loop.md §3.1, TS-5).
 *
 * <p>{@link #advance(Duration)} runs every task due within the span in deadline order (ties in the order they were
 * scheduled, TS-3), each on its own virtual thread, and releases every sleeper whose deadline passes. After each one
 * it waits until every thread this source started has finished or parked again in {@link #sleep(Duration)}: explicit
 * quiescence, so the next task starts on a settled world and the advance returns on one. A task that neither
 * finishes nor parks within the quiescence bound (5 s of real time by default) fails the advance with the thread's
 * name and stack (WD-7); a task that throws fails the advance after it settles.
 *
 * <p>A task scheduled with no delay, or at a deadline already past, runs at the next advance, which may be an advance
 * of zero ({@link #runDue()}): nothing runs between advances, so a test reads a state no thread is changing.
 *
 * <p>{@link #stepWall(Duration)} moves wall time alone, as an NTP step does, and raises the wall-step signal (TS-6) on
 * this source and every node view: each {@link #onWallStep} listener runs as a task at the next advance.
 * {@link #offset(Duration)} and {@link #node()} are views for one node of a fleet: a node view's wall clock can be
 * skewed and stepped alone ({@link NodeTime#stepWall}), and an unwatched node view ({@link #unwatchedNode()}) can be
 * suspended and resumed ({@link NodeTime#suspend()}, {@link NodeTime#resume()}): its monotonic time stops while the
 * fleet's moves on, as a laptop's does in a suspend or a process's in a long pause, and the resume raises the step.
 *
 * <p>Work on threads this source did not start (a test's own thread, a reader thread) is not part of quiescence.
 * {@link #waitOn} waits on a monitor until a notify or an advance past its deadline (group W); {@link #run(Duration,
 * Duration, java.util.concurrent.Callable)} runs a blocking call as a task of this source while the test's thread
 * advances, for a call (a take with a settle window) that waits on this source.
 *
 * <p>The source carries its own watchdog ({@link #watchdog()}, srad/agentspaces-event-loop.md WD-7, EL-H-10): loops
 * started on it register there, and each advance runs the watchdog's check whenever it crosses the watchdog's next
 * tick, after the tasks due at that instant, so the watchdog judges deadlines in virtual time. Each released task's
 * real time to settle is bounded by the quiescence bound, and one that ran past the watchdog's slow bound is reported
 * by name ({@link #slowTasks()}).
 */
public final class VirtualTime implements TimeSource {

    /** The default real-time bound on one task's settling. */
    public static final Duration DEFAULT_QUIESCENCE_BOUND = Duration.ofSeconds(5);

    private static final int PENDING = 0;
    private static final int STARTED = 1;
    private static final int CANCELLED = 2;
    private static final long MONO_START = 1_000_000_000_000L; // 1,000 s: never 0, so 0 can mean "not set"

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition settled = lock.newCondition();
    private final ReentrantLock advancing = new ReentrantLock();
    /** Guarded by {@link #lock}. */
    private final PriorityQueue<Item> queue = new PriorityQueue<>();
    /** Threads this source started that are alive, and whether each is parked in {@link #sleep}. Guarded by lock. */
    private final Map<Thread, Boolean> tracked = new LinkedHashMap<>();
    /** Tracked threads parked in {@link #waitOn} (a subset of the parked ones). Guarded by {@link #lock}. */
    private final Map<Thread, Parked> waitingOnMonitor = new java.util.HashMap<>();
    /** Tracked threads running, neither finished nor parked. Guarded by {@link #lock}. */
    private int active;
    /** Guarded by {@link #lock}. */
    private final List<Throwable> failures = new ArrayList<>();
    private final ThreadLocal<Boolean> mine = new ThreadLocal<>();
    private final AtomicLong sequence = new AtomicLong();
    private final AtomicLong tasksRun = new AtomicLong();
    private final Instant wallAtStart;
    private volatile long mono = MONO_START;
    private volatile long wallStepNanos;
    private volatile Duration quiescenceBound = DEFAULT_QUIESCENCE_BOUND;
    private final Watchdog watchdog = Watchdog.manual(this, Watchdog.Options.defaults());
    /** Guarded by {@link #lock}. */
    private final List<String> slowTasks = new ArrayList<>();
    private final List<java.util.function.Consumer<Duration>> wallListeners =
            new java.util.concurrent.CopyOnWriteArrayList<>();
    private final List<NodeTime> views = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final AtomicLong wallSteps = new AtomicLong();
    /** Threads this source did not start that wait on it now, in {@link #sleep} or {@link #waitOn}. */
    private final java.util.Set<Thread> outsideWaiters = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private final List<Runnable> outsideWaitListeners = new java.util.concurrent.CopyOnWriteArrayList<>();

    private VirtualTime(Instant wallAtStart) {
        this.wallAtStart = wallAtStart;
    }

    /**
     * A virtual time source whose wall clock starts at the given instant.
     *
     * @param start the wall instant now
     * @return the source
     */
    public static VirtualTime startingAt(Instant start) {
        return new VirtualTime(Objects.requireNonNull(start, "start"));
    }

    /**
     * A virtual time source starting at the same fixed epoch instant as {@link TestClock#create()}.
     *
     * @return the source
     */
    public static VirtualTime create() {
        return new VirtualTime(Instant.ofEpochMilli(1_000_000_000_000L));
    }

    @Override
    public Instant instant() {
        return wallAtStart.plusNanos(mono - MONO_START + wallStepNanos);
    }

    @Override
    public long monoNanos() {
        return mono;
    }

    @Override
    public boolean virtual() {
        return true;
    }

    /**
     * This source's watchdog: it judges in virtual time and ticks as advances cross its period (WD-7).
     *
     * @return the watchdog
     */
    @Override
    public Watchdog watchdog() {
        return watchdog;
    }

    /**
     * The released tasks that ran past the watchdog's slow bound in real time before settling, by name, oldest first
     * (EL-H-10).
     *
     * @return their names
     */
    public List<String> slowTasks() {
        lock.lock();
        try {
            return List.copyOf(slowTasks);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Sets the real-time bound on one task's settling (WD-7).
     *
     * @param bound the bound
     * @return this source
     */
    public VirtualTime quiescenceBound(Duration bound) {
        this.quiescenceBound = Objects.requireNonNull(bound, "bound");
        return this;
    }

    @Override
    public Scheduled schedule(Duration delay, Runnable task) {
        Objects.requireNonNull(delay, "delay");
        long nanos = delay.isNegative() ? 0 : delay.toNanos();
        long deadline;
        try {
            deadline = Math.addExact(mono, nanos);
        } catch (ArithmeticException e) {
            deadline = Long.MAX_VALUE;
        }
        return scheduleAt(deadline, task);
    }

    @Override
    public Scheduled scheduleAt(long monoDeadlineNanos, Runnable task) {
        Objects.requireNonNull(task, "task");
        Item item = new Item(monoDeadlineNanos, sequence.getAndIncrement(), task, null, false);
        if (monoDeadlineNanos == Long.MAX_VALUE) {
            return item; // never due
        }
        lock.lock();
        try {
            queue.add(item);
        } finally {
            lock.unlock();
        }
        return item;
    }

    @Override
    public void sleep(Duration duration) throws InterruptedException {
        long nanos = duration.toNanos();
        if (nanos <= 0) {
            return;
        }
        boolean trackedThread = Boolean.TRUE.equals(mine.get());
        Item item = new Item(mono + nanos, sequence.getAndIncrement(), null, new CountDownLatch(1), trackedThread);
        lock.lock();
        try {
            queue.add(item);
            if (trackedThread) {
                tracked.put(Thread.currentThread(), true);
                active--;
                settled.signalAll();
            }
        } finally {
            lock.unlock();
        }
        if (!trackedThread) {
            outsideWaitStarts();
        }
        try {
            item.released.await();
        } catch (InterruptedException e) {
            lock.lock();
            try {
                if (queue.remove(item) && trackedThread) {
                    // Not released by an advance: this thread runs again on its own.
                    tracked.put(Thread.currentThread(), false);
                    active++;
                }
            } finally {
                lock.unlock();
            }
            throw e;
        } finally {
            if (!trackedThread) {
                outsideWaiters.remove(Thread.currentThread());
            }
        }
    }

    /**
     * Whether a thread this source did not start (a test's own thread) waits on it now, in {@link #sleep} or
     * {@link #waitOn}: such a wait ends only when something advances this source, so a fleet whose test calls a
     * blocking API from its own thread moves the time for it meanwhile (srad/agentspaces-event-loop.md EL-Q-TST-03).
     *
     * @param thread the thread
     * @return whether it waits on this source now
     */
    public boolean waitingOutside(Thread thread) {
        return outsideWaiters.contains(thread);
    }

    /**
     * Registers a listener told, on the waiting thread and before it parks, each time a thread this source did not start
     * begins a wait on it. It must return at once (it runs with the waiter's monitor held); a driver it wakes reads
     * {@link #waitingOutside}.
     *
     * @param listener the listener
     * @return closes the registration
     */
    public AutoCloseable onOutsideWait(Runnable listener) {
        Objects.requireNonNull(listener, "listener");
        outsideWaitListeners.add(listener);
        return () -> outsideWaitListeners.remove(listener);
    }

    private void outsideWaitStarts() {
        outsideWaiters.add(Thread.currentThread());
        for (Runnable listener : outsideWaitListeners) {
            listener.run();
        }
    }

    /**
     * Waits on a monitor the caller holds until notified or until an advance passes the deadline (group W): the deadline
     * is a task on this source that notifies the monitor, so the waiter is released in deadline order with every other
     * task. A waiter on a thread this source started counts as parked meanwhile, as a sleeper does; when the deadline
     * releases it, it counts as running again before the advance moves on. One woken by another thread's notify is
     * seen by its thread state (no longer waiting) and waited for, so an advance settles it too.
     */
    @Override
    public boolean waitOn(Object monitor, long monoDeadlineNanos) throws InterruptedException {
        return parkOn(monitor, monoDeadlineNanos, this::scheduleAt, this::monoNanos);
    }

    /** The state of one {@link #waitOn}. */
    private static final class Parked {
        volatile boolean due;
        /** Set the moment the wait returns, before the waiter counts itself again. */
        volatile boolean awake;
        /** Guarded by the source's lock. */
        boolean rejoined;
    }

    boolean parkOn(Object monitor, long deadline, java.util.function.BiFunction<Long, Runnable, Scheduled> arm,
                   java.util.function.LongSupplier now) throws InterruptedException {
        Objects.requireNonNull(monitor, "monitor");
        if (!Thread.holdsLock(monitor)) {
            throw new IllegalMonitorStateException("waitOn needs the monitor held");
        }
        if (deadline - now.getAsLong() <= 0) {
            return false;
        }
        Thread self = Thread.currentThread();
        boolean trackedThread = Boolean.TRUE.equals(mine.get());
        Parked parked = new Parked();
        Scheduled timer = arm.apply(deadline, () -> {
            synchronized (monitor) {
                parked.due = true;
                if (trackedThread) {
                    rejoin(self, parked); // before the notify: the advance waits for the released waiter
                }
                monitor.notifyAll();
            }
        });
        if (trackedThread) {
            lock.lock();
            try {
                tracked.put(self, true);
                waitingOnMonitor.put(self, parked);
                active--;
                settled.signalAll();
            } finally {
                lock.unlock();
            }
        }
        if (!trackedThread) {
            outsideWaitStarts();
        }
        try {
            monitor.wait();
        } finally {
            parked.awake = true;
            if (trackedThread) {
                rejoin(self, parked);
            } else {
                outsideWaiters.remove(self);
            }
            timer.cancel();
        }
        return !parked.due;
    }

    private void rejoin(Thread thread, Parked parked) {
        lock.lock();
        try {
            if (!parked.rejoined) {
                parked.rejoined = true;
                tracked.put(thread, false);
                waitingOnMonitor.remove(thread);
                active++;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * What {@link #run} returns: the call's value and the virtual time it took.
     *
     * @param value the value
     * @param took  monotonic time from the call's start to its return
     * @param <T>   the value's type
     */
    public record Outcome<T>(T value, Duration took) {
    }

    /**
     * Starts a blocking call as a task of this source, due now: it begins at the next advance (an advance of zero
     * will do), its waits on this source park it, and every advance settles it. For a fleet whose test moves time by
     * its own steps (ticks), where {@link #run} would move it alone.
     *
     * @param call the call
     * @param <T>  its value
     * @return completes with the call's value and the virtual time it took, or with what it threw
     */
    public <T> java.util.concurrent.CompletableFuture<Outcome<T>> start(java.util.concurrent.Callable<T> call) {
        Objects.requireNonNull(call, "call");
        java.util.concurrent.CompletableFuture<Outcome<T>> done = new java.util.concurrent.CompletableFuture<>();
        scheduleAt(mono, () -> {
            long started = mono;
            try {
                T value = call.call();
                done.complete(new Outcome<>(value, Duration.ofNanos(mono - started)));
            } catch (Throwable e) {
                done.completeExceptionally(e);
            }
        });
        return done;
    }

    /**
     * Runs a blocking call as a task of this source (so its sleeps and waits on this source park it and every advance
     * settles it) and advances {@code step} at a time until it returns: a take whose settle window or poll waits on
     * this source, called from a test that must also move time. The call's own waits must be on this source; one that
     * waits in real time fails the advance at the quiescence bound, naming its stack.
     *
     * @param step  each advance
     * @param limit the most virtual time to advance before giving up
     * @param call  the call
     * @param <T>   its value
     * @return its value and the virtual time it took
     * @throws AssertionError when it neither returned nor threw within the limit, or threw
     */
    public <T> Outcome<T> run(Duration step, Duration limit, java.util.concurrent.Callable<T> call) {
        Objects.requireNonNull(step, "step");
        Objects.requireNonNull(call, "call");
        if (step.isZero() || step.isNegative()) {
            throw new IllegalArgumentException("step must be positive: " + step);
        }
        long started = mono;
        java.util.concurrent.CompletableFuture<Outcome<T>> done = start(call);
        runDue();
        long end = started + limit.toNanos();
        while (!done.isDone() && mono < end) {
            advance(step);
        }
        if (!done.isDone()) {
            throw new AssertionError("the call did not return within " + limit + " of virtual time");
        }
        try {
            return done.join();
        } catch (java.util.concurrent.CompletionException e) {
            throw new AssertionError("the call threw", e.getCause());
        }
    }

    /**
     * Moves time forward, running what falls due on the way (TS-5).
     *
     * @param span how far; not negative
     */
    public void advance(Duration span) {
        Objects.requireNonNull(span, "span");
        if (span.isNegative()) {
            throw new IllegalArgumentException("virtual time never goes backwards: " + span);
        }
        advanceTo(mono + span.toNanos());
    }

    /**
     * Runs every task already due and releases every sleeper already due, without moving time.
     */
    public void runDue() {
        advanceTo(mono);
    }

    /**
     * Moves time forward to a monotonic instant, running what falls due on the way.
     *
     * @param monoTarget the target on {@link #monoNanos()}'s base; a target already passed runs what is due
     */
    public void advanceTo(long monoTarget) {
        if (Boolean.TRUE.equals(mine.get())) {
            throw new IllegalStateException("advance from inside a task of this time source would wait on itself");
        }
        advancing.lock();
        List<Throwable> thrown;
        try {
            lock.lock();
            try {
                while (true) {
                    Item head = queue.peek();
                    long check = watchdog.nextTickNanos();
                    if (check <= monoTarget && (head == null || check < head.due)) {
                        // The watchdog's tick, after every task due at or before it (WD-7).
                        if (check > mono) {
                            mono = check;
                        }
                        watchdog.tick();
                        awaitSettled(null, 0);
                        continue;
                    }
                    if (head == null || head.due > monoTarget) {
                        break;
                    }
                    queue.poll();
                    if (head.due > mono) {
                        mono = head.due;
                    }
                    long releasedAt = System.nanoTime();
                    if (head.released != null) {
                        if (head.trackedSleeper) {
                            tracked.put(head.sleeperThread, false);
                            active++;
                        }
                        head.released.countDown();
                    } else if (head.state.compareAndSet(PENDING, STARTED)) {
                        launch(head);
                    } else {
                        continue; // cancelled
                    }
                    awaitSettled(head, releasedAt);
                }
                if (monoTarget > mono) {
                    mono = monoTarget;
                }
                thrown = List.copyOf(failures);
                failures.clear();
            } finally {
                lock.unlock();
            }
        } finally {
            advancing.unlock();
        }
        if (!thrown.isEmpty()) {
            AssertionError error = new AssertionError("a task threw while virtual time advanced", thrown.get(0));
            thrown.stream().skip(1).forEach(error::addSuppressed);
            throw error;
        }
    }

    /**
     * Moves wall time alone, as an NTP step does; monotonic time and every deadline stay where they are.
     *
     * @param step the step, either way
     */
    public void stepWall(Duration step) {
        Objects.requireNonNull(step, "step");
        wallStepNanos += step.toNanos();
        raise(wallListeners, step);
        for (NodeTime view : views) {
            raise(view.listeners, step);
        }
    }

    @Override
    public AutoCloseable onWallStep(java.util.function.Consumer<Duration> step) {
        Objects.requireNonNull(step, "step");
        wallListeners.add(step);
        return () -> wallListeners.remove(step);
    }

    /**
     * Wall steps raised on this source or any of its node views (TS-6).
     *
     * @return the count
     */
    public long wallSteps() {
        return wallSteps.get();
    }

    /** Raises a step past the threshold to each listener, as a task due now (TS-6). */
    private void raise(List<java.util.function.Consumer<Duration>> listeners, Duration step) {
        if (step.abs().compareTo(WALL_STEP_THRESHOLD) <= 0) {
            return;
        }
        wallSteps.incrementAndGet();
        for (java.util.function.Consumer<Duration> listener : listeners) {
            scheduleAt(mono, () -> listener.accept(step));
        }
    }

    /**
     * A view for one node, watched by this source's watchdog, with its own wall skew and wall steps.
     *
     * @return the view
     */
    public NodeTime node() {
        NodeTime view = new NodeTime(this, true);
        views.add(view);
        return view;
    }

    /**
     * A view for one node that this source's watchdog does not watch ({@link Watchdog#none()}), which can also be
     * suspended: a suspended node's deadlines are judged on its own stopped clock, which the shared watchdog cannot.
     *
     * @return the view
     */
    public NodeTime unwatchedNode() {
        NodeTime view = new NodeTime(this, false);
        views.add(view);
        return view;
    }

    /**
     * One node's view of a {@link VirtualTime} (see the class comment). Its monotonic time is the base's less the time
     * it spent suspended; its wall time is the base's plus its skew. Tasks scheduled through a suspended view are held
     * and run, each once, after {@link #resume()}, in deadline order, as a node's timers do when it wakes.
     */
    public static final class NodeTime implements TimeSource {
        private final VirtualTime base;
        private final boolean watched;
        final List<java.util.function.Consumer<Duration>> listeners =
                new java.util.concurrent.CopyOnWriteArrayList<>();
        final List<java.util.function.Consumer<Duration>> lateListeners =
                new java.util.concurrent.CopyOnWriteArrayList<>();
        private volatile long skewNanos;
        private volatile long lagNanos;
        private volatile boolean suspended;
        /** Whether the hold is a freeze, in which the monotonic clock keeps counting, or a suspend, in which not. */
        private volatile boolean counting;
        private volatile long frozenMono;
        private long suspendedAt;
        private final List<ViewTask> held = new ArrayList<>();

        private NodeTime(VirtualTime base, boolean watched) {
            this.base = base;
            this.watched = watched;
        }

        /**
         * Skews this node's wall clock by a further amount without raising a step, as a host whose clock was always
         * off by that much.
         *
         * @param skew the skew to add
         * @return this view
         */
        public NodeTime skew(Duration skew) {
            skewNanos += skew.toNanos();
            return this;
        }

        /**
         * Steps this node's wall clock alone, as an NTP step on its host does, and raises the step to its listeners.
         *
         * @param step the step, either way
         */
        public void stepWall(Duration step) {
            Objects.requireNonNull(step, "step");
            skewNanos += step.toNanos();
            base.raise(listeners, step);
        }

        /**
         * Stops this node's monotonic clock and holds its tasks and sleepers, as a suspend or a long pause does; its
         * wall clock and the fleet's time move on.
         */
        public synchronized void suspend() {
            hold(false);
        }

        /**
         * Holds this node's tasks and sleepers while its monotonic clock keeps counting, as a stop-the-world pause or a
         * frozen container does (EL-SP-05): after {@link #resume()} each task that fell due meanwhile runs once, at
         * once, and no wall step is raised, since wall and monotonic time moved together.
         */
        public synchronized void freeze() {
            hold(true);
        }

        private void hold(boolean keepCounting) {
            if (watched) {
                throw new IllegalStateException("a node view the shared watchdog watches cannot be held;"
                        + " build the node on unwatchedNode()");
            }
            if (suspended) {
                return;
            }
            frozenMono = base.mono - lagNanos;
            suspendedAt = base.mono;
            counting = keepCounting;
            suspended = true;
        }

        /**
         * Starts this node's clock again: its monotonic time resumes where it stopped, its wall time is the fleet's,
         * so (wall - monotonic) moved by the time it was suspended, and a step past the threshold is raised; each held
         * task is armed again on the resumed clock (one overdue becomes due now).
         *
         * @return how long it was suspended
         */
        public Duration resume() {
            List<ViewTask> again;
            long span;
            synchronized (this) {
                if (!suspended) {
                    return Duration.ZERO;
                }
                span = base.mono - suspendedAt;
                if (!counting) {
                    lagNanos += span; // the monotonic clock did not count the suspend
                }
                suspended = false;
                again = new ArrayList<>(held);
                held.clear();
            }
            again.sort(java.util.Comparator.comparingLong((ViewTask t) -> t.due).thenComparingLong(t -> t.seq));
            for (ViewTask t : again) {
                t.arm();
            }
            if (!counting) {
                base.raise(listeners, Duration.ofNanos(span)); // (wall - monotonic) moved by the suspend
            } else if (span > LATE_WAKE_THRESHOLD.toNanos()) {
                // A freeze past the 30 s bound: the node's timer wakes that late with no wall step, as a SIGSTOP's or a
                // long pause's does on SystemTime, and the node is told of a late wake (SPEC v0.1.37 §9.4).
                Duration late = Duration.ofNanos(span);
                for (java.util.function.Consumer<Duration> listener : lateListeners) {
                    base.scheduleAt(base.mono, () -> listener.accept(late));
                }
            }
            return Duration.ofNanos(span);
        }

        /**
         * Whether this view is suspended.
         *
         * @return whether it is
         */
        public boolean suspended() {
            return suspended;
        }

        @Override
        public Instant instant() {
            return base.instant().plusNanos(skewNanos);
        }

        @Override
        public long monoNanos() {
            return suspended && !counting ? frozenMono : base.mono - lagNanos;
        }

        @Override
        public Scheduled schedule(Duration delay, Runnable task) {
            Objects.requireNonNull(delay, "delay");
            long nanos = delay.isNegative() ? 0 : delay.toNanos();
            long deadline;
            try {
                deadline = Math.addExact(monoNanos(), nanos);
            } catch (ArithmeticException e) {
                deadline = Long.MAX_VALUE;
            }
            return scheduleAt(deadline, task);
        }

        @Override
        public Scheduled scheduleAt(long monoDeadlineNanos, Runnable task) {
            Objects.requireNonNull(task, "task");
            if (monoDeadlineNanos == Long.MAX_VALUE) {
                return base.scheduleAt(Long.MAX_VALUE, task);
            }
            ViewTask t = new ViewTask(monoDeadlineNanos, base.sequence.getAndIncrement(), task);
            t.arm();
            return t;
        }

        @Override
        public void sleep(Duration duration) throws InterruptedException {
            long deadline = monoNanos() + duration.toNanos();
            while (true) {
                if (suspended) {
                    base.sleep(Duration.ofSeconds(1)); // a sleeper of a suspended node wakes only after the resume
                    continue;
                }
                long left = deadline - monoNanos();
                if (left <= 0) {
                    return;
                }
                base.sleep(Duration.ofNanos(left));
            }
        }

        @Override
        public boolean virtual() {
            return true;
        }

        /** As {@link VirtualTime#waitOn}, the deadline on this node's clock (held while it is suspended). */
        @Override
        public boolean waitOn(Object monitor, long monoDeadlineNanos) throws InterruptedException {
            return base.parkOn(monitor, monoDeadlineNanos, this::scheduleAt, this::monoNanos);
        }

        @Override
        public AutoCloseable onWallStep(java.util.function.Consumer<Duration> step) {
            Objects.requireNonNull(step, "step");
            listeners.add(step);
            return () -> listeners.remove(step);
        }

        /**
         * Registers a listener told when this view resumes from a {@link #freeze()} longer than
         * {@link TimeSource#LATE_WAKE_THRESHOLD}: its timers wake that late, with no wall step.
         */
        @Override
        public AutoCloseable onLateWake(java.util.function.Consumer<Duration> late) {
            Objects.requireNonNull(late, "late");
            lateListeners.add(late);
            return () -> lateListeners.remove(late);
        }

        @Override
        public Watchdog watchdog() {
            return watched ? base.watchdog() : Watchdog.none();
        }

        /** A task scheduled through the view: armed on the base at its deadline plus the view's lag. */
        private final class ViewTask implements Scheduled {
            final long due;
            final long seq;
            final Runnable task;
            final AtomicInteger state = new AtomicInteger(PENDING);
            volatile Scheduled inner;

            ViewTask(long due, long seq, Runnable task) {
                this.due = due;
                this.seq = seq;
                this.task = task;
            }

            void arm() {
                long at;
                try {
                    at = Math.addExact(due, lagNanos);
                } catch (ArithmeticException e) {
                    at = Long.MAX_VALUE - 1;
                }
                inner = base.scheduleAt(at, this::fire);
            }

            void fire() {
                if (state.get() != PENDING) {
                    return;
                }
                synchronized (NodeTime.this) {
                    if (suspended) {
                        held.add(this); // runs after the resume
                        return;
                    }
                }
                if (monoNanos() - due < 0) {
                    arm(); // the view's clock fell behind by a suspend since it was armed
                    return;
                }
                if (state.compareAndSet(PENDING, STARTED)) {
                    task.run();
                }
            }

            @Override
            public boolean cancel() {
                if (!state.compareAndSet(PENDING, CANCELLED)) {
                    return false;
                }
                Scheduled i = inner;
                if (i != null) {
                    i.cancel();
                }
                synchronized (NodeTime.this) {
                    held.remove(this);
                }
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
        }
    }

    /**
     * A view of this source whose wall clock is skewed by the offset, for one node of a fleet; monotonic time,
     * scheduling and sleeps are this source's.
     *
     * @param skew the wall offset
     * @return the view
     */
    public NodeTime offset(Duration skew) {
        Objects.requireNonNull(skew, "skew");
        return node().skew(skew);
    }

    /**
     * Tasks and sleepers waiting for a deadline.
     *
     * @return how many
     */
    public int pending() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Tasks this source has started since it was made.
     *
     * @return the count
     */
    public long tasksRun() {
        return tasksRun.get();
    }


    private void launch(Item item) {
        tasksRun.incrementAndGet();
        Thread thread = Thread.ofVirtual().name("virtual-time-task-" + item.seq).unstarted(() -> {
            mine.set(Boolean.TRUE);
            try {
                item.task.run();
            } catch (Throwable e) {
                lock.lock();
                try {
                    failures.add(e);
                } finally {
                    lock.unlock();
                }
            } finally {
                lock.lock();
                try {
                    tracked.remove(Thread.currentThread());
                    active--;
                    settled.signalAll();
                } finally {
                    lock.unlock();
                }
            }
        });
        tracked.put(thread, false);
        active++;
        thread.start();
    }

    /**
     * Waits, with {@link #lock} held, until no tracked thread runs; fails past the real-time bound (WD-7), and reports a
     * released item that took past the watchdog's slow bound to settle (EL-H-10).
     */
    private void awaitSettled(Item released, long releasedAt) {
        long left = quiescenceBound.toNanos();
        try {
            awaitSettledWithin(left);
        } finally {
            if (released != null) {
                long took = System.nanoTime() - releasedAt;
                if (took > watchdog.options().slow().toNanos()) {
                    String what = released.task != null
                            ? "task " + released.seq + " (" + released.task.getClass().getName() + ")"
                            : "sleeper " + released.sleeperThread.getName();
                    slowTasks.add(what);
                    if (slowTasks.size() > 1_024) {
                        slowTasks.remove(0);
                    }
                    watchdog.reportSlow(what, took);
                }
            }
        }
    }

    private void awaitSettledWithin(long left) {
        while (active > 0 || wokenWaiter()) {
            if (left <= 0) {
                StringBuilder names = new StringBuilder();
                for (Map.Entry<Thread, Boolean> e : tracked.entrySet()) {
                    if (!e.getValue()) {
                        Thread t = e.getKey();
                        names.append("\n  ").append(t.getName()).append(" (").append(t.getState()).append(')');
                        for (StackTraceElement frame : t.getStackTrace()) {
                            names.append("\n    at ").append(frame);
                        }
                    }
                }
                throw new IllegalStateException("virtual time did not settle within " + quiescenceBound
                        + " of real time; still running:" + names);
            }
            try {
                // A waiter woken by a notify is not counted until it holds its monitor again: look again shortly.
                long nap = active > 0 ? left : Math.min(left, 100_000L);
                long napped = nap - settled.awaitNanos(nap);
                left -= napped;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while virtual time settled", e);
            }
        }
    }

    /**
     * Whether a thread counted as parked in {@link #waitOn} has been woken by another thread's notify and not yet
     * counted again: it is no longer waiting (it is blocked on its monitor or running). Under {@link #lock}.
     */
    private boolean wokenWaiter() {
        for (Map.Entry<Thread, Parked> e : waitingOnMonitor.entrySet()) {
            if (e.getValue().awake) {
                return true; // out of its wait, about to count itself again
            }
            Thread.State state = e.getKey().getState();
            if (state != Thread.State.WAITING && state != Thread.State.TIMED_WAITING
                    && state != Thread.State.TERMINATED) {
                return true; // notified: blocked on its monitor, or running
            }
        }
        return false;
    }

    private final class Item implements Scheduled, Comparable<Item> {
        final long due;
        final long seq;
        final Runnable task;
        final CountDownLatch released;
        final Thread sleeperThread;
        final boolean trackedSleeper;
        final AtomicInteger state = new AtomicInteger(PENDING);

        Item(long due, long seq, Runnable task, CountDownLatch released, boolean trackedSleeper) {
            this.due = due;
            this.seq = seq;
            this.task = task;
            this.released = released;
            this.sleeperThread = released == null ? null : Thread.currentThread();
            this.trackedSleeper = trackedSleeper;
        }

        @Override
        public boolean cancel() {
            if (released != null || !state.compareAndSet(PENDING, CANCELLED)) {
                return false;
            }
            lock.lock();
            try {
                queue.remove(this);
            } finally {
                lock.unlock();
            }
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
        public int compareTo(Item o) {
            int c = Long.compare(due, o.due);
            return c != 0 ? c : Long.compare(seq, o.seq);
        }
    }
}
