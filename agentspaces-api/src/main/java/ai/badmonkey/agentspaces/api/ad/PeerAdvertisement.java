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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Describes a peer: how to reach it and which topology roles it offers
 * (spec §5.4, §6.1).
 *
 * @param id            advertisement identifier
 * @param issuer        the peer describing itself
 * @param group         the group scope
 * @param issued        issue instant
 * @param ttl           cache time-to-live
 * @param endpoints     reachability, in priority order preference
 * @param roles         topology roles the peer offers
 * @param resourceHints free-form capacity hints (cpu, battery, uptime, …)
 */
public record PeerAdvertisement(
        String id,
        PeerId issuer,
        GroupId group,
        Instant issued,
        Duration ttl,
        List<Endpoint> endpoints,
        Set<PeerRole> roles,
        Map<String, String> resourceHints) implements Advertisement {

    /**
     * One way of reaching a peer.
     *
     * @param transport the transport scheme, e.g. {@code tcp} or {@code ws}
     * @param address   a transport-specific address
     * @param priority  lower dials first
     */
    public record Endpoint(String transport, String address, int priority) {
        public Endpoint {
            Objects.requireNonNull(transport, "transport");
            Objects.requireNonNull(address, "address");
        }
    }

    /** Optional topology roles a peer may serve for its group (spec §5.4). */
    public enum PeerRole {
        /** Larger ad-cache; answers discovery queries for constrained peers. */
        RENDEZVOUS,
        /** Forwards traffic for NAT-restricted peers. */
        RELAY
    }

    public PeerAdvertisement {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(issuer, "issuer");
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(issued, "issued");
        Objects.requireNonNull(ttl, "ttl");
        endpoints = List.copyOf(Objects.requireNonNull(endpoints, "endpoints"));
        roles = Set.copyOf(Objects.requireNonNull(roles, "roles"));
        resourceHints = java.util.Collections.unmodifiableMap(
                new java.util.LinkedHashMap<>(Objects.requireNonNull(resourceHints, "resourceHints"))); // signed in iteration order
    }
}
