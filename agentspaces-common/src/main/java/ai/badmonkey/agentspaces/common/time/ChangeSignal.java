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

import java.util.Objects;

/**
 * A change counter a waiter can wait on through a time source (srad/agentspaces-event-loop.md EL-W-03): what turns a
 * polling wait into a change wait. Whoever changes what the waiter looks at calls {@link #run()} (it is a
 * {@link Runnable}, so it registers directly as a listener, such as a space's {@code onChangeSignal}); the waiter reads
 * {@link #seen()} before it looks and {@link #await} after, so a change between the look and the wait is never lost.
 *
 * <p>{@link #run()} takes only this object's own monitor, briefly, and calls nothing else, so it may be called while
 * the caller holds another lock (a space signals under its own); a waiter never calls out while it holds this monitor.
 */
public final class ChangeSignal implements Runnable {

    private final TimeSource time;
    /** Guarded by this. */
    private long count;

    /**
     * @param time the source waits are reckoned and parked on
     */
    public ChangeSignal(TimeSource time) {
        this.time = Objects.requireNonNull(time, "time");
    }

    /** Signals a change: every waiter wakes and looks again. */
    @Override
    public synchronized void run() {
        count++;
        notifyAll();
    }

    /**
     * The change count to pass to {@link #await}, read before the look.
     *
     * @return the count
     */
    public synchronized long seen() {
        return count;
    }

    /**
     * Waits until a change after {@code seen} or the monotonic deadline, whichever is first.
     *
     * @param seen              the count read before the look
     * @param monoDeadlineNanos the deadline on the source's monotonic base
     * @return true when a change came, false when the deadline passed first
     * @throws InterruptedException when interrupted
     */
    public synchronized boolean await(long seen, long monoDeadlineNanos) throws InterruptedException {
        while (count == seen) {
            if (!time.waitOn(this, monoDeadlineNanos) && count == seen) {
                return false;
            }
        }
        return true;
    }

    /**
     * The source this signal's waits use.
     *
     * @return the source
     */
    public TimeSource time() {
        return time;
    }
}
