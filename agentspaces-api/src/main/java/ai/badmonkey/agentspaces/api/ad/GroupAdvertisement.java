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
package ai.badmonkey.agentspaces.api.ad;

import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * The founding document of a peer group (spec §5.1). The GroupID is the hash of
 * this advertisement's canonical bytes, which makes group names self-certifying:
 * possession of the founding document proves the name.
 *
 * @param id               advertisement identifier
 * @param issuer           the founding peer
 * @param group            the self-certifying GroupID
 * @param issued           issue instant
 * @param ttl              cache time-to-live
 * @param name             human-readable group name
 * @param membershipPolicy who may join
 * @param defaultStrategy  default conflict strategy for the group's spaces
 * @param gossip           gossip parameters for the group's bus
 */
public record GroupAdvertisement(
        String id,
        PeerId issuer,
        GroupId group,
        Instant issued,
        Duration ttl,
        String name,
        MembershipPolicy membershipPolicy,
        ConflictStrategyType defaultStrategy,
        GossipParameters gossip) implements Advertisement {

    /** Group membership policies (spec §5.1). */
    public enum MembershipPolicy {
        /** Anyone may join. Bootstrap and public groups; Sybil-exposed by construction. */
        OPEN,
        /** Joining requires a credential signed by a founder or admin key. */
        INVITE,
        /** Joining is decided by a pluggable validator SPI (e.g. a DID credential check). */
        POLICY
    }

    /**
     * Gossip-bus parameters for a group (spec §5.3). The fan-out bounds how
     * many peers a rumor reaches per round; the period drives the group's
     * anti-entropy cadence. Membership views are full and leased, so there is
     * no partial-view size to tune (v0.1.10 removed {@code maxViewSize}).
     *
     * @param fanout peers contacted per rumor round
     * @param period gossip round period
     */
    public record GossipParameters(int fanout, Duration period) {
        public GossipParameters {
            Objects.requireNonNull(period, "period");
            if (fanout <= 0 || period.isZero() || period.isNegative()) {
                throw new IllegalArgumentException("gossip parameters must be positive");
            }
        }

        /** Returns sensible defaults: fanout 3, period 1s. */
        public static GossipParameters defaults() {
            return new GossipParameters(3, Duration.ofSeconds(1));
        }
    }

    public GroupAdvertisement {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(issuer, "issuer");
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(issued, "issued");
        Objects.requireNonNull(ttl, "ttl");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(membershipPolicy, "membershipPolicy");
        Objects.requireNonNull(defaultStrategy, "defaultStrategy");
        Objects.requireNonNull(gossip, "gossip");
    }
}
