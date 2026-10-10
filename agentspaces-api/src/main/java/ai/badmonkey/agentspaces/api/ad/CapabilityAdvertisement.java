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
import java.util.Map;
import java.util.Objects;

/**
 * Offers a capability service to a group (spec §8). Third parties define
 * capabilities by minting a type URI and publishing a mini-spec; the core
 * enumerates none (spec P6). The freshness of this advertisement answers "is this
 * capability still offered?" with no failure-detection round trips.
 *
 * @param id             advertisement identifier
 * @param issuer         the providing peer
 * @param group          the group scope
 * @param issued         issue instant
 * @param ttl            cache time-to-live
 * @param capabilityType the capability type URI, e.g. {@code aspace:cap/vote}
 * @param version        the capability protocol version
 * @param binding        endpoint binding: a space name the provider watches, or a
 *                       pipe address for direct high-rate protocols
 * @param parameters     capability-specific parameters
 * @param costHints      free-form cost hints for consumers choosing a provider
 */
public record CapabilityAdvertisement(
        String id,
        PeerId issuer,
        GroupId group,
        Instant issued,
        Duration ttl,
        String capabilityType,
        String version,
        String binding,
        Map<String, String> parameters,
        Map<String, String> costHints) implements Advertisement {

    public CapabilityAdvertisement {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(issuer, "issuer");
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(issued, "issued");
        Objects.requireNonNull(ttl, "ttl");
        Objects.requireNonNull(capabilityType, "capabilityType");
        Objects.requireNonNull(version, "version");
        Objects.requireNonNull(binding, "binding");
        parameters = java.util.Collections.unmodifiableMap(
                new java.util.LinkedHashMap<>(Objects.requireNonNull(parameters, "parameters"))); // signed in iteration order
        costHints = java.util.Collections.unmodifiableMap(
                new java.util.LinkedHashMap<>(Objects.requireNonNull(costHints, "costHints"))); // signed in iteration order
    }
}
