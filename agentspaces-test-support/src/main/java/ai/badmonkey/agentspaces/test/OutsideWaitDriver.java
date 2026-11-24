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

import java.util.Objects;
import java.util.concurrent.locks.LockSupport;

/**
 * Moves a fleet's {@link VirtualTime} while the test's own thread waits on it (srad/agentspaces-event-loop.md
 * EL-Q-TST-03). A test that calls a blocking API from its thread (a linear swap whose take waits, a read that waits for a
 * load) parks in {@link VirtualTime#waitOn} or {@link VirtualTime#sleep}, and only an advance releases it; on a
 * hand-moved clock the same wait ran out in real time. The driver wakes when the owner starts such a wait
 * ({@link VirtualTime#onOutsideWait}) and runs the fleet's step (settle, advance a little, deliver) for as long as the
 * owner still waits, so the call returns at its deadline in fleet time, or sooner when what it waits for arrives.
 *
 * <p>Nothing runs while the owner is not waiting on the time: the test's own ticks stay the only other mover. A step
 * that throws stops the driver and interrupts the owner; {@link #rethrow()} hands the failure to the test's next tick.
 */
public final class OutsideWaitDriver implements AutoCloseable {

    private final VirtualTime time;
    private final Thread owner;
    private final Runnable step;
    private final Thread driver;
    private final AutoCloseable registration;
    private volatile boolean closed;
    private volatile Throwable failure;

    private OutsideWaitDriver(VirtualTime time, Thread owner, Runnable step) {
        this.time = Objects.requireNonNull(time, "time");
        this.owner = Objects.requireNonNull(owner, "owner");
        this.step = Objects.requireNonNull(step, "step");
        this.driver = Thread.ofVirtual().name("outside-wait-driver").unstarted(this::drive);
        this.registration = time.onOutsideWait(() -> {
            if (Thread.currentThread() == owner) {
                LockSupport.unpark(driver);
            }
        });
        driver.start();
    }

    /**
     * Starts a driver for the calling thread's waits.
     *
     * @param time the fleet's time
     * @param step one step of the fleet while the owner waits: it must check {@link VirtualTime#waitingOutside} again
     *             after settling and advance only while the owner still waits
     * @return the driver
     */
    public static OutsideWaitDriver forCurrentThread(VirtualTime time, Runnable step) {
        return new OutsideWaitDriver(time, Thread.currentThread(), step);
    }

    /** The thread whose waits this driver serves. */
    public Thread owner() {
        return owner;
    }

    /** Whether the owner waits on the time now. */
    public boolean ownerWaiting() {
        return time.waitingOutside(owner);
    }

    private void drive() {
        while (!closed) {
            LockSupport.park(this);
            while (!closed && failure == null && time.waitingOutside(owner)) {
                try {
                    step.run();
                } catch (Throwable t) {
                    failure = t;
                    owner.interrupt(); // the owner's wait would otherwise wait for an advance that never comes
                }
            }
        }
    }

    /** Throws what a step threw, once, as an {@link AssertionError}. */
    public void rethrow() {
        Throwable t = failure;
        if (t != null) {
            failure = null;
            throw new AssertionError("the fleet failed while it moved time for a waiting call", t);
        }
    }

    @Override
    public void close() {
        closed = true;
        try {
            registration.close();
        } catch (Exception ignored) {
            // a list removal does not fail
        }
        LockSupport.unpark(driver);
    }
}
