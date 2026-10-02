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
import java.util.List;
import java.util.Objects;

/**
 * The founding document of a space within a group (spec §7.5). The SpaceID is the
 * hash of this advertisement's canonical bytes. The schema hints declare the shape
 * of data the space carries, so agents can judge relevance before attaching.
 *
 * @param id          advertisement identifier
 * @param issuer      the founding peer
 * @param group       the group scope
 * @param issued      issue instant
 * @param ttl         cache time-to-live; founders keep the space alive by renewing
 * @param spaceName   the space's name within the group
 * @param schemaHints schema names of the entry types the space expects
 * @param strategy    the conflict strategy governing exclusive takes
 */
public record SpaceAdvertisement(
        String id,
        PeerId issuer,
        GroupId group,
        Instant issued,
        Duration ttl,
        String spaceName,
        List<String> schemaHints,
        ConflictStrategyType strategy,
        Admission admission,
        Replication replication) implements Advertisement {

    /**
     * How a space admits writers and takers (SPEC §7.5). {@link #GROUP}
     * admits every group member, the default. {@link #ALLOWLIST} admits only
     * the agents named in {@code allowedAgents}; a member outside the list may
     * still read. {@link #CREDENTIAL} admits an agent for a scope while a
     * live, issuer-signed space credential naming it and the scope is present
     * in the space. {@link #AUTHORIZER} admits an agent when the space's
     * {@code Authorizer} permits {@code SPACE_WRITE} or {@code SPACE_TAKE} for
     * its peer in the scope of the space name. The codec writes enum names,
     * so the two v0.1.11 constants are additive on the wire.
     */
    public enum Admission { GROUP, ALLOWLIST, CREDENTIAL, AUTHORIZER }

    /**
     * How a space replicates (SPEC §7.3). {@link #FULL} replicates every
     * entry to every member; {@link #TAG_SHARDED} lets each replica hold only
     * the slice its tag predicate selects.
     */
    public enum Replication { FULL, TAG_SHARDED }

    /**
     * The pre-v0.1.10 shape: group-membership admission and full replication.
     */
    public SpaceAdvertisement(String id, PeerId issuer, GroupId group, Instant issued,
                              Duration ttl, String spaceName, List<String> schemaHints,
                              ConflictStrategyType strategy) {
        this(id, issuer, group, issued, ttl, spaceName, schemaHints, strategy,
                Admission.GROUP, Replication.FULL);
    }

    public SpaceAdvertisement {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(issuer, "issuer");
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(issued, "issued");
        Objects.requireNonNull(ttl, "ttl");
        Objects.requireNonNull(spaceName, "spaceName");
        Objects.requireNonNull(strategy, "strategy");
        schemaHints = List.copyOf(Objects.requireNonNull(schemaHints, "schemaHints"));
        admission = admission == null ? Admission.GROUP : admission;
        replication = replication == null ? Replication.FULL : replication;
    }
}
