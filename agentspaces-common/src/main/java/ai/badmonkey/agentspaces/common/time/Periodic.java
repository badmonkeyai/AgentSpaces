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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * A renewer as a task on a time source (srad/agentspaces-event-loop.md EL-W-02): the body runs once each period, each
 * run scheduled one period after the last one ended, until the body answers false, throws, or the handle is closed.
 * It replaces a virtual thread that slept in a loop, so a virtual time source releases each renewal on its advance,
 * and the renewal's cadence and the lease it renews are judged on the same source.
 *
 * <p>A run is never concurrent with another of the same handle; {@link #close()} cancels the next run and returns at
 * once (a run in progress finishes; it sees {@link #closed()} if it checks).
 */
public final class Periodic implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(Periodic.class.getName());

    private final TimeSource time;
    private final Duration period;
    private final String name;
    private final BooleanSupplier body;
    private final AtomicLong runs = new AtomicLong();
    private final Object lock = new Object();
    /** Guarded by {@link #lock}. */
    private TimeSource.Scheduled next;
    private volatile boolean closed;

    private Periodic(TimeSource time, Duration period, String name, BooleanSupplier body) {
        this.time = time;
        this.period = period;
        this.name = name;
        this.body = body;
    }

    /**
     * Starts a renewer whose first run is one period from now.
     *
     * @param time   the source
     * @param period the period; at least a millisecond is used
     * @param name   for the log
     * @param body   the renewal; answers whether to keep going
     * @return the handle
     */
    public static Periodic every(TimeSource time, Duration period, String name, BooleanSupplier body) {
        Objects.requireNonNull(time, "time");
        Objects.requireNonNull(period, "period");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(body, "body");
        Duration p = period.compareTo(Duration.ofMillis(1)) < 0 ? Duration.ofMillis(1) : period;
        Periodic periodic = new Periodic(time, p, name, body);
        periodic.arm();
        return periodic;
    }

    private void arm() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            next = time.schedule(period, this::fire);
        }
    }

    private void fire() {
        if (closed) {
            return;
        }
        boolean again = false;
        try {
            again = body.getAsBoolean();
            runs.incrementAndGet();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, name + ": the renewal threw; it stops", e);
        } finally {
            // An Error leaves here with again false too: the renewer reads closed rather than alive and never armed.
            if (again) {
                arm();
            } else {
                closed = true;
            }
        }
    }

    /**
     * How many runs have finished.
     *
     * @return the count
     */
    public long runs() {
        return runs.get();
    }

    /**
     * Whether it has stopped: closed, or its body answered false or threw.
     *
     * @return whether it stopped
     */
    public boolean closed() {
        return closed;
    }

    @Override
    public void close() {
        synchronized (lock) {
            closed = true;
            if (next != null) {
                next.cancel();
            }
        }
    }
}
