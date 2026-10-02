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
 * after everything this node has observed.
 *
 * <p>This class is thread-safe.
 */
public final class HybridLogicalClock {

    /**
     * How far ahead of local wall time a remote physical stamp may pull this
     * clock. A remote node whose physical component exceeds {@code wall +
     * MAX_DRIFT_MILLIS} is clamped to that ceiling before merging, so a peer
     * (honest but badly skewed, or hostile and forging {@code Long.MAX_VALUE})
     * cannot poison this node's clock and make every stamp it later issues
     * sort after all honest timestamps. Ten minutes comfortably covers real
     * inter-node skew while bounding the damage from a bad stamp.
     */
    public static final long MAX_DRIFT_MILLIS = 600_000L;

    private final InstantSource wallClock;
    private final String node;

    private long lastPhysical;
    private int lastLogical;

    /**
     * Creates a clock for the given node.
     *
     * @param wallClock the wall-clock source (inject a test clock in tests)
     * @param node      the node identifier stamped into every timestamp
     */
    public HybridLogicalClock(InstantSource wallClock, String node) {
        this.wallClock = Objects.requireNonNull(wallClock, "wallClock");
        this.node = Objects.requireNonNull(node, "node");
    }

    /** Returns a timestamp for a local event, strictly greater than any issued before. */
    public synchronized HlcTimestamp now() {
        long wall = wallClock.instant().toEpochMilli();
        if (wall > lastPhysical) {
            lastPhysical = wall;
            lastLogical = 0;
        } else {
            lastLogical = incrementLogical(lastLogical);
        }
        return new HlcTimestamp(lastPhysical, lastLogical, node);
    }

    /**
     * Merges a remote timestamp into this clock, then returns a fresh local timestamp
     * guaranteed to sort after both the remote timestamp and all prior local ones.
     *
     * @param remote a timestamp received from another node
     * @return a fresh local timestamp causally after {@code remote}
     */
    public synchronized HlcTimestamp update(HlcTimestamp remote) {
        Objects.requireNonNull(remote, "remote");
        long wall = wallClock.instant().toEpochMilli();
        // Clamp the remote physical component to a bounded lead over local wall
        // time. Without this, a single stamp carrying Long.MAX_VALUE would pin
        // lastPhysical there permanently and every subsequent stamp this node
        // issues would sort after all honest ones (clock poisoning). Honest
        // skew is well under MAX_DRIFT_MILLIS, so legitimate stamps are never
        // clamped; the logical counter still preserves causal order within the
        // window.
        long remotePhysical = Math.min(remote.physical(), wall + MAX_DRIFT_MILLIS);
        long maxPhysical = Math.max(wall, Math.max(lastPhysical, remotePhysical));
        int logical;
        if (maxPhysical == lastPhysical && maxPhysical == remotePhysical) {
            logical = incrementLogical(Math.max(lastLogical, remote.logical()));
        } else if (maxPhysical == lastPhysical) {
            logical = incrementLogical(lastLogical);
        } else if (maxPhysical == remotePhysical) {
            logical = incrementLogical(remote.logical());
        } else {
            logical = 0;
        }
        lastPhysical = maxPhysical;
        lastLogical = logical;
        return new HlcTimestamp(lastPhysical, lastLogical, node);
    }

    /**
     * Advances the logical counter by one, saturating at {@link Integer#MAX_VALUE}
     * rather than overflowing to a negative value (which the {@link HlcTimestamp}
     * constructor would reject). Reaching the ceiling requires 2^31 stamps at the
     * same physical millisecond, which only a poisoned clock could sustain; when
     * wall time next advances past {@code lastPhysical}, the counter resets to 0.
     */
    private static int incrementLogical(int logical) {
        return logical == Integer.MAX_VALUE ? Integer.MAX_VALUE : logical + 1;
    }

    /** Returns the node identifier this clock stamps into timestamps. */
    public String node() {
        return node;
    }
}
