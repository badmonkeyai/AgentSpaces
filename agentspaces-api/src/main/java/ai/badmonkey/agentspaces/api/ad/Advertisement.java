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

import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.time.Duration;
import java.time.Instant;

/**
 * A typed, TTL-bearing discovery document (spec §6.1). Everything discoverable in
 * AgentSpaces, peers, groups, spaces, capabilities, and agents, is described by an
 * advertisement; discovery, trust, and binding all operate on them (spec P3).
 *
 * <p>Advertisements circulate wrapped in a {@link SignedAdvertisement}; receivers
 * verify the signature before caching or acting. Freshness is the liveness
 * mechanism: caches MUST evict expired advertisements and MUST NOT forward them
 * (spec §6.2), and issuers refresh by re-publishing with a new {@link #issued()}.
 */
public sealed interface Advertisement
        permits PeerAdvertisement, GroupAdvertisement, SpaceAdvertisement,
        CapabilityAdvertisement, AgentCard, AssetCard, RevocationAdvertisement,
        CredentialRevocation {

    /** Returns the advertisement's URI-style identifier. */
    String id();

    /** Returns the publishing peer. */
    PeerId issuer();

    /** Returns the group this advertisement is scoped to. */
    GroupId group();

    /** Returns the issue instant that the TTL counts from. */
    Instant issued();

    /** Returns the time-to-live bounding cache lifetime from {@link #issued()}. */
    Duration ttl();

    /** Returns the instant this advertisement expires. */
    default Instant expiresAt() {
        return issued().plus(ttl());
    }

    /**
     * Tests whether this advertisement has expired at the given moment.
     *
     * @param now the moment to test
     * @return {@code true} when {@code now} is at or after {@link #expiresAt()}
     */
    default boolean expired(Instant now) {
        return !now.isBefore(expiresAt());
    }
}
