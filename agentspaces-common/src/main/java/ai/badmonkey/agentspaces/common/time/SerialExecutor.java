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

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Runs tasks one at a time, in the order they were given (srad/agentspaces-event-loop.md §3.5). In CSP terms it is the
 * channel a node's sequential process reads: {@link #execute} puts a task on it, and at most one drain takes from it
 * at a time, started through the {@code drainer} (phase 1: a virtual thread from the time source; phase 4: the loop
 * pool).
 *
 * <p>{@link #runExclusive} runs a task on the caller's own thread while no drain runs, so a caller that has to wait
 * for its work (the public {@code tick()}) is serial with the drains without handing its work to another thread and
 * waiting for it there.
 *
 * <p>A watchdog that re-initialises the owner moves the queue to a fresh executor with {@link #abandon}: queued tasks
 * move over, later {@code execute} and {@code runExclusive} calls go to the successor, and a drain still running here
 * (a wedged one) takes nothing more when it returns (srad/agentspaces-event-loop.md WD-2). A
 * {@link VirtualMachineError} from a task is not contained: the drain's thread dies with the executor still marked
 * owned by it, which is what the watchdog's dead-thread check finds (EL-SP-20); every other throwable is logged and
 * the drain goes on.
 */
public final class SerialExecutor implements Executor {

    private static final System.Logger LOG = System.getLogger(SerialExecutor.class.getName());

    private final String name;
    private final Executor drainer;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition released = lock.newCondition();
    /** Guarded by {@link #lock}. */
    private final ArrayDeque<Runnable> queue = new ArrayDeque<>();
    /** A drain or an exclusive caller holds the executor. Guarded by {@link #lock}. */
    private boolean owned;
    /** A drain has been handed to the drainer and has not started. Guarded by {@link #lock}. */
    private boolean drainDispatched;
    /** Set once by {@link #abandon}. Guarded by {@link #lock}; read without it through the volatile. */
    private volatile SerialExecutor successor;
    private volatile Thread owner;
    // Mirrors for the watchdog, which takes no lock (WD-1).
    private volatile int size;
    private volatile boolean dispatchedMirror;

    /**
     * Makes a serial executor.
     *
     * @param name    the owner's name, for logs
     * @param drainer starts a drain; it must run the drain on another thread (a virtual thread or a pool)
     */
    public SerialExecutor(String name, Executor drainer) {
        this.name = Objects.requireNonNull(name, "name");
        this.drainer = Objects.requireNonNull(drainer, "drainer");
    }

    @Override
    public void execute(Runnable task) {
        Objects.requireNonNull(task, "task");
        boolean start;
        SerialExecutor next;
        lock.lock();
        try {
            next = successor;
            if (next == null) {
                queue.add(task);
                size = queue.size();
                start = dispatchLocked();
            } else {
                start = false;
            }
        } finally {
            lock.unlock();
        }
        if (next != null) {
            next.execute(task);
            return;
        }
        if (start) {
            drainer.execute(this::drain);
        }
    }

    /**
     * Runs the task on the calling thread once no drain or other exclusive caller holds the executor, and holds it
     * until the task returns; queued tasks run after it. A caller already holding the executor (from inside a task)
     * runs the task at once.
     *
     * @param task the task
     */
    public void runExclusive(Runnable task) {
        Objects.requireNonNull(task, "task");
        if (owner == Thread.currentThread()) {
            task.run();
            return;
        }
        SerialExecutor next;
        lock.lock();
        try {
            while (owned && successor == null) {
                released.awaitUninterruptibly();
            }
            next = successor;
            if (next == null) {
                owned = true;
                owner = Thread.currentThread();
            }
        } finally {
            lock.unlock();
        }
        if (next != null) {
            next.runExclusive(task);
            return;
        }
        try {
            task.run();
        } finally {
            release();
        }
    }

    /**
     * Whether the calling thread holds the executor now (inside a drained task or an exclusive call).
     *
     * @return whether this thread owns it
     */
    public boolean ownedByCurrentThread() {
        return owner == Thread.currentThread();
    }

    /**
     * The thread holding the executor now, or null.
     *
     * @return the owner
     */
    public Thread owner() {
        return owner;
    }

    /**
     * Tasks waiting to run.
     *
     * @return the queue's length
     */
    public int queued() {
        return size;
    }

    /**
     * Whether a drain was handed to the drainer and has not started yet.
     *
     * @return whether one is dispatched
     */
    public boolean dispatched() {
        return dispatchedMirror;
    }

    /**
     * Moves every queued task to the successor and sends every later {@code execute} and {@code runExclusive} there;
     * a drain running here takes nothing more (srad/agentspaces-event-loop.md WD-2). Idempotent for one successor.
     *
     * @param next the fresh executor
     */
    public void abandon(SerialExecutor next) {
        Objects.requireNonNull(next, "next");
        if (next == this) {
            throw new IllegalArgumentException("an executor cannot succeed itself");
        }
        java.util.List<Runnable> moved;
        lock.lock();
        try {
            if (successor != null) {
                return;
            }
            successor = next;
            moved = java.util.List.copyOf(queue);
            queue.clear();
            size = 0;
            drainDispatched = false;
            dispatchedMirror = false;
            released.signalAll(); // an exclusive caller waiting here goes to the successor
        } finally {
            lock.unlock();
        }
        for (Runnable task : moved) {
            next.execute(task);
        }
    }

    /**
     * Whether {@link #abandon} has moved this executor's work to a successor.
     *
     * @return whether abandoned
     */
    public boolean abandoned() {
        return successor != null;
    }

    private boolean dispatchLocked() {
        if (owned || drainDispatched || queue.isEmpty() || successor != null) {
            return false;
        }
        drainDispatched = true;
        dispatchedMirror = true;
        return true;
    }

    private void drain() {
        lock.lock();
        try {
            drainDispatched = false;
            dispatchedMirror = false;
            if (owned || successor != null) {
                return; // an exclusive caller holds it; its release dispatches a fresh drain
            }
            owned = true;
            owner = Thread.currentThread();
        } finally {
            lock.unlock();
        }
        while (true) {
            Runnable next;
            lock.lock();
            try {
                next = successor == null ? queue.poll() : null;
                size = queue.size();
                if (next == null) {
                    owned = false;
                    owner = null;
                    released.signalAll();
                    return;
                }
            } finally {
                lock.unlock();
            }
            try {
                next.run();
            } catch (VirtualMachineError e) {
                throw e; // the drain dies still marked owner: the watchdog's dead-thread check (EL-SP-20)
            } catch (Throwable e) {
                LOG.log(System.Logger.Level.WARNING, "a task on " + name + "'s serial executor threw", e);
            }
            if (drainer instanceof LoopPool pool && pool.waiting() && handBack()) {
                return;
            }
        }
    }

    /**
     * Hands the drain's thread back to the loop pool while another node's run waits for one, and queues this drain
     * behind it: a node woken by every run would otherwise drain until its queue ran dry and hold a pool thread from
     * every co-resident node, which at the pool's max waited for the stuck bound (review defect R1,
     * srad/agentspaces-event-loop.md §10.23). Order and one-at-a-time are kept: the drain is dispatched, not owned, as
     * after any {@link #execute}, so an exclusive caller may run first and its release finds the drain dispatched.
     *
     * @return true when the drain was handed back; false when nothing is left to drain
     */
    private boolean handBack() {
        lock.lock();
        try {
            if (successor != null || queue.isEmpty()) {
                return false;
            }
            owned = false;
            owner = null;
            drainDispatched = true;
            dispatchedMirror = true;
            released.signalAll();
        } finally {
            lock.unlock();
        }
        drainer.execute(this::drain);
        return true;
    }

    private void release() {
        boolean start;
        lock.lock();
        try {
            owned = false;
            owner = null;
            released.signalAll();
            start = dispatchLocked();
        } finally {
            lock.unlock();
        }
        if (start) {
            drainer.execute(this::drain);
        }
    }
}
