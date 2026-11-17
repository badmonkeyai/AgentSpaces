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
package ai.badmonkey.agentspaces.peering.gossip;

import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * One pacing parameter of replication (srad/replication-by-entry.md RE-F-03, RE-Q-REL-09): a value with a floor and a
 * ceiling that a runtime setter moves, and that moves down only after it has stayed up for one hold. The hold is read
 * when a raise happens, and is one anti-entropy period for every pace the replication layer keeps, so a value an
 * operator or a later adjuster raised serves at least one period before a lowering takes effect; a lowering asked for
 * sooner waits, and the last one asked wins. A raise always takes effect at once, since moving a pace up is the safe
 * direction (fewer frames, more patience). Setting a value outside the bounds is refused with the parameter's name, and
 * nothing changes.
 *
 * <p>The value a node starts with may lie outside the bounds (a group's founding may name a period below the floor);
 * the bounds bind the setter only, so a group keeps working as founded until someone moves it.
 */
public final class Pace {

    private final String name;
    private final long floor;
    private final long ceiling;
    private final LongSupplier clock;
    private final LongSupplier hold;
    private long value;
    private long raisedAt = Long.MIN_VALUE;
    private long heldUntil = Long.MIN_VALUE;
    private Long pending;
    private long raises;
    private long lowerings;
    private long deferred;

    /**
     * A pace.
     *
     * @param name    the parameter's name, as an error and a status page give it
     * @param floor   the least value the setter accepts
     * @param ceiling the greatest value the setter accepts
     * @param initial the starting value, which may lie outside the bounds
     * @param clock   the clock, in milliseconds
     * @param hold    how long a raised value stays before a lowering takes effect, read at each raise
     */
    public Pace(String name, long floor, long ceiling, long initial, LongSupplier clock, LongSupplier hold) {
        this.name = Objects.requireNonNull(name, "name");
        if (floor > ceiling) {
            throw new IllegalArgumentException(name + ": a floor above its ceiling");
        }
        this.floor = floor;
        this.ceiling = ceiling;
        this.value = initial;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.hold = Objects.requireNonNull(hold, "hold");
    }

    /**
     * Moves the pace. A raise takes effect at once and starts a hold; a lowering takes effect at once unless a hold is
     * running, in which case it waits for the hold's end.
     *
     * @param next the new value, from the floor to the ceiling
     * @return the value in effect now
     * @throws IllegalArgumentException outside the bounds
     */
    public synchronized long set(long next) {
        if (next < floor || next > ceiling) {
            throw new IllegalArgumentException(name + " must be from " + floor + " to " + ceiling + ", not " + next);
        }
        long now = clock.getAsLong();
        settle(now);
        if (next > value) {
            value = next;
            pending = null;
            raisedAt = now;
            heldUntil = now + Math.max(0, hold.getAsLong());
            raises++;
        } else if (next < value) {
            if (now < heldUntil) {
                pending = next; // RE-Q-REL-09: up stays up for one hold before it comes down
                deferred++;
            } else {
                value = next;
                pending = null;
                lowerings++;
            }
        } else {
            pending = null; // asking for the value in effect cancels a waiting lowering
        }
        return value;
    }

    /** The value in effect now, after any lowering whose hold has ended. */
    public synchronized long get() {
        settle(clock.getAsLong());
        return value;
    }

    private void settle(long now) {
        if (pending != null && now >= heldUntil) {
            value = pending;
            pending = null;
            lowerings++;
        }
    }

    /** The parameter's name. */
    public String name() {
        return name;
    }

    /** A snapshot for a status page or an adjuster. */
    public synchronized Snapshot snapshot() {
        settle(clock.getAsLong());
        return new Snapshot(name, value, floor, ceiling, pending, raisedAt == Long.MIN_VALUE ? null : raisedAt,
                heldUntil == Long.MIN_VALUE ? null : heldUntil, raises, lowerings, deferred);
    }

    /**
     * What a pace holds.
     *
     * @param name      the parameter
     * @param value     the value in effect
     * @param floor     the least value the setter accepts
     * @param ceiling   the greatest value the setter accepts
     * @param pending   a lowering waiting for the hold's end, or null
     * @param raisedAt  when it was last raised, or null
     * @param heldUntil when the last raise's hold ends, or null
     * @param raises    raises so far
     * @param lowerings lowerings that took effect so far
     * @param deferred  lowerings that had to wait for a hold
     */
    public record Snapshot(String name, long value, long floor, long ceiling, Long pending, Long raisedAt,
                           Long heldUntil, long raises, long lowerings, long deferred) {
    }
}
