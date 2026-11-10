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
package ai.badmonkey.agentspaces.common.hlc;

import java.time.InstantSource;
import java.util.Objects;

/**
 * A hybrid logical clock (Kulkarni et al.): issues {@link HlcTimestamp}s that stay
 * close to wall-clock time while remaining strictly monotonic on this node and
 * causally consistent with timestamps received from other nodes.
 *
 * <p>Call {@link #now()} to stamp a local event. Call {@link #update(HlcTimestamp)}
 * whenever a timestamp arrives from a remote node, so subsequent local stamps sort
 * after everything this node has observed. Every merge point first asks
 * {@link #isAcceptable(HlcTimestamp)} and ignores the frame, record, or claim that
 * carries a refused stamp.
 *
 * <p>This class is thread-safe.
 */
public final class HybridLogicalClock {

    /**
     * How far ahead of local wall time a remote physical stamp may pull this
     * clock. The merge clamps a remote physical component that reaches {@code
     * wall + MAX_DRIFT_MILLIS} to one millisecond inside that ceiling (HLC
     * logical counter, 2026-11-08, Rust team report), so a carry after the merge
     * lands at most on the ceiling, and a peer (honest but badly skewed, or
     * hostile and forging {@code Long.MAX_VALUE}) cannot poison this node's clock and make every stamp it later issues
     * sort after all honest timestamps. Ten minutes comfortably covers real
     * inter-node skew while bounding the damage from a bad stamp.
     */
    public static final long MAX_DRIFT_MILLIS = 600_000L;

    /**
     * The largest logical counter a remote stamp may carry, 2^16 (HLC logical
     * counter, 2026-11-08, Rust team report). The counter resets every
     * millisecond, so an honest node never comes near this bound, and this
     * clock carries into the physical part when its own counter reaches it. A remote
     * stamp above the bound is refused at every merge point, in every
     * implementation: one stamp just under the drift ceiling with the counter
     * at its maximum otherwise froze the clock on that stamp for up to
     * {@link #MAX_DRIFT_MILLIS}, so every later frame read as a replay, lease
     * renewals tied, and contested takes went to other replicas.
     */
    public static final int LOGICAL_LIMIT = 65_536;

    private final InstantSource wallClock;
    private final String node;

    private long lastPhysical;
    private int lastLogical;

    /**
     * Creates a clock for the given node.
     *
     * @param wallClock the wall-clock source (inject a test clock in tests)
     * @param node      the node identifier stamped into every timestamp
     * @throws IllegalArgumentException if the node is empty, holds a colon, a control character,
     *         or a lone surrogate, or runs past {@link HlcTimestamp#MAX_NODE_UNITS}, so that no
     *         stamp could carry it
     */
    public HybridLogicalClock(InstantSource wallClock, String node) {
        this.wallClock = Objects.requireNonNull(wallClock, "wallClock");
        this.node = Objects.requireNonNull(node, "node");
        // HLC round four, 2026-11-08 (decision D6): a peer never emits a stamp whose node breaks
        // the grammar, so a bad node fails here, where the clock is built, before any stamp.
        if (!HlcTimestamp.canonicalNode(node)) {
            throw new IllegalArgumentException(
                    "node must be non-empty and contain no ':', no control character, and no lone surrogate: "
                            + node);
        }
        // HLC round seven, 2026-11-08 (SPEC §9.1, S6): every stamp this clock issues stays one
        // definite text, whatever its physical and logical parts grow to.
        if (node.length() > HlcTimestamp.MAX_NODE_UNITS) {
            throw new IllegalArgumentException("node holds at most " + HlcTimestamp.MAX_NODE_UNITS + " UTF-16 units");
        }
    }

    /**
     * Whether a remote stamp may merge into a clock: its logical counter is at
     * most {@link #LOGICAL_LIMIT} (HLC logical counter, 2026-11-08, Rust team
     * report). The one check every merge point shares.
     *
     * @param remote a timestamp received from another node
     * @return true when {@link #update(HlcTimestamp)} accepts it
     */
    public static boolean isAcceptable(HlcTimestamp remote) {
        return remote.logical() <= LOGICAL_LIMIT;
    }

    /** Returns a timestamp for a local event, strictly greater than any issued before. */
    public synchronized HlcTimestamp now() {
        long wall = wallClock.instant().toEpochMilli();
        if (wall > lastPhysical) {
            lastPhysical = wall;
            lastLogical = 0;
        } else {
            advance();
        }
        return new HlcTimestamp(lastPhysical, lastLogical, node);
    }

    /**
     * Merges a remote timestamp into this clock, then returns a fresh local timestamp
     * guaranteed to sort after both the remote timestamp and all prior local ones.
     *
     * @param remote a timestamp received from another node
     * @return a fresh local timestamp causally after {@code remote}
     * @throws IllegalArgumentException if {@code remote} is not
     *         {@linkplain #isAcceptable(HlcTimestamp) acceptable}; the clock is unchanged
     */
    public synchronized HlcTimestamp update(HlcTimestamp remote) {
        Objects.requireNonNull(remote, "remote");
        if (!isAcceptable(remote)) {
            // HLC logical counter, 2026-11-08 (Rust team report): merge points
            // check isAcceptable and drop the carrier first, so reaching here
            // is a caller's bug and must not touch the clock.
            throw new IllegalArgumentException("logical counter above " + LOGICAL_LIMIT + ": " + remote);
        }
        long wall = wallClock.instant().toEpochMilli();
        // Clamp the remote physical component to a bounded lead over local wall
        // time. Without this, a single stamp carrying Long.MAX_VALUE would pin
        // lastPhysical there permanently and every subsequent stamp this node
        // issues would sort after all honest ones (clock poisoning). Honest
        // skew is well under MAX_DRIFT_MILLIS, so legitimate stamps are never
        // clamped; the logical counter still preserves causal order within the
        // window. HLC logical counter, 2026-11-08 (Rust team report): the clamp
        // sits one millisecond inside the ceiling, so a carry after merging a
        // stamp at the ceiling with the counter at LOGICAL_LIMIT lands on the
        // ceiling. Receivers strike only a stamp past it (ASF-020), so they
        // never strike this node for a stamp it merged honestly. HLC canonical
        // stamps, 2026-11-08: a receiver whose wall runs behind this node's
        // sees that ceiling past its own, so receivers strike only beyond a
        // skew margin past their ceiling (PeerNode.FAR_FUTURE_SKEW_MARGIN_MILLIS).
        long remotePhysical = Math.min(remote.physical(), wall + MAX_DRIFT_MILLIS - 1);
        long maxPhysical = Math.max(wall, Math.max(lastPhysical, remotePhysical));
        if (maxPhysical == lastPhysical && maxPhysical == remotePhysical) {
            lastLogical = Math.max(lastLogical, remote.logical());
            advance();
        } else if (maxPhysical == lastPhysical) {
            advance();
        } else if (maxPhysical == remotePhysical) {
            lastPhysical = maxPhysical;
            lastLogical = remote.logical();
            advance();
        } else {
            lastPhysical = maxPhysical;
            lastLogical = 0;
        }
        return new HlcTimestamp(lastPhysical, lastLogical, node);
    }

    /**
     * Advances the clock by one logical step. At {@link #LOGICAL_LIMIT} the
     * counter carries: the physical part advances one millisecond and the
     * counter resets to 0 (HLC logical counter, 2026-11-08, Rust team report).
     * The clock therefore never issues the same stamp twice and never issues a
     * counter another node would refuse. Callers hold the monitor.
     */
    private void advance() {
        if (lastLogical >= LOGICAL_LIMIT) {
            lastPhysical++;
            lastLogical = 0;
        } else {
            lastLogical++;
        }
    }

    /** Returns the node identifier this clock stamps into timestamps. */
    public String node() {
        return node;
    }
}
