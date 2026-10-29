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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.time.InstantSource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A host's say over what a RELAY-role node forwards (SPEC §5.4,
 * ISSUE-PeerRobustness-2, R2-3). The node asks it once per {@code RELAY_FRAME}
 * it would forward, after it has checked that the target is a member of the
 * group the frame names in its own view; the policy sees the group, the
 * outer frame's sender, the target, and the inner frame's byte length, and a
 * {@code false} drops the frame and counts the drop in
 * {@link PeerNode#relayStats()}. The quota's shape (per group per day, per
 * sender per minute, a paid tier) is the host's business; the default
 * {@link #ALLOW} forwards everything the membership check allows.
 *
 * <p>Called on the receive thread: it must return promptly and must not block
 * on the network or on a lock a frame handler may hold. A policy that throws
 * drops the frame (fail closed), and the node logs the failure at most once a
 * minute.
 */
@FunctionalInterface
public interface RelayPolicy {

    /** Forwards every frame the membership check allows. */
    RelayPolicy ALLOW = (group, from, target, bytes) -> true;

    /**
     * Whether to forward one relayed frame.
     *
     * @param group  the group the frame names
     * @param from   the member that asked for the relay (the outer frame's sender)
     * @param target the member the inner frame is for
     * @param bytes  the inner frame's length in bytes
     * @return {@code true} to forward, {@code false} to drop
     */
    boolean allow(GroupId group, PeerId from, PeerId target, int bytes);

    /**
     * Whether to forward a frame of this kind (LB-R-11): the inner envelope's kind, a pipe frame as
     * {@code PIPE_DATA:<capability>} (so the content swarm's pieces are
     * {@code PIPE_DATA:aspace:cap/content-swarm}), as {@link PeerNode#relayBytesByKind} counts them.
     * Consulted before {@link #allow}; the default forwards every kind.
     *
     * @param group the group the frame names
     * @param kind  the frame's kind
     * @return {@code true} to forward, {@code false} to drop
     */
    default boolean allowKind(GroupId group, String kind) {
        return true;
    }

    /**
     * This policy, refusing the given kinds as well (LB-R-11): {@code refusingKinds("PIPE_DATA:aspace:cap/content-swarm")}
     * keeps the content swarm's pieces off a metered relay while every other frame still passes this policy.
     *
     * @param kinds the kinds to refuse, as {@link #allowKind} names them
     * @return the composed policy
     */
    default RelayPolicy refusingKinds(String... kinds) {
        java.util.Set<String> refused = java.util.Set.of(kinds);
        RelayPolicy self = this;
        return new RelayPolicy() {
            @Override
            public boolean allow(GroupId group, PeerId from, PeerId target, int bytes) {
                return self.allow(group, from, target, bytes);
            }

            @Override
            public boolean allowKind(GroupId group, String kind) {
                return !refused.contains(kind) && self.allowKind(group, kind);
            }
        };
    }

    /**
     * A per-group daily byte budget (ISSUE-BootstrapCore FR-2, the relay budget; SRAD
     * discovery-bootstrap DB-S-06): the inner bytes forwarded in each group are summed per UTC
     * day on the system clock, and a frame that would take the day's total past the budget is
     * refused, as is every frame after it until the next day. The count holds one entry per
     * group this node relays in, which is bounded by the groups it joined. Compose it with a
     * policy of your own by calling it from there.
     *
     * @param bytesPerGroupPerDay the budget
     * @return the policy
     */
    static RelayPolicy dailyBudget(long bytesPerGroupPerDay) {
        return dailyBudget(bytesPerGroupPerDay, InstantSource.system());
    }

    /**
     * As {@link #dailyBudget(long)}, judging the day on the given clock.
     *
     * @param bytesPerGroupPerDay the budget
     * @param clock               the clock the day is read from
     * @return the policy
     */
    static RelayPolicy dailyBudget(long bytesPerGroupPerDay, InstantSource clock) {
        if (bytesPerGroupPerDay < 0) {
            throw new IllegalArgumentException("the relay budget must not be negative: " + bytesPerGroupPerDay);
        }
        java.util.Objects.requireNonNull(clock, "clock");
        final class Day {
            long day;
            long bytes;
        }
        Map<GroupId, Day> days = new ConcurrentHashMap<>();
        return (group, from, target, bytes) -> {
            long today = Math.floorDiv(clock.millis(), 86_400_000L);
            Day day = days.computeIfAbsent(group, g -> new Day());
            synchronized (day) {
                if (day.day != today) {
                    day.day = today;
                    day.bytes = 0;
                }
                if (day.bytes + bytes > bytesPerGroupPerDay) {
                    return false;
                }
                day.bytes += bytes;
                return true;
            }
        };
    }
}
