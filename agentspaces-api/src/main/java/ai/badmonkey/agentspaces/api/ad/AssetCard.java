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
 * A card describing a data asset (spec §6.1a, the v0.1.1 addendum): the sibling
 * of the {@link AgentCard} for peers that provide data rather than pursue
 * goals. A connector peer, one per warehouse, database, search cluster, broker,
 * or web service, publishes one AssetCard per asset it holds; agents discover
 * assets exactly as they discover agents, structurally through the ad-cache or
 * by meaning through the semantic-discovery capability, and the lease is the
 * liveness contract: a connector that goes down stops refreshing and its assets
 * age out of every cache.
 *
 * @param id          advertisement identifier
 * @param issuer      the connector peer holding the asset
 * @param group       the group scope; visibility follows the group perimeter
 * @param issued      issue instant
 * @param ttl         cache time-to-live
 * @param asset       the asset's name, unique within the issuing peer
 * @param uri         the source-system URI, e.g. {@code postgres://ops/public.orders}
 * @param description human- and LLM-readable description of what the asset holds
 * @param shape       the declared shape of data: an entry schema name, table
 *                    structure, index mapping, or message format identifier
 * @param freshness   the update cadence or freshness window, ISO-8601 duration
 *                    or free-form ({@code PT5M}, {@code daily})
 * @param costHints   free-form size and cost hints (rows, bytes, credits, ...)
 * @param access      the access binding ({@code mode=read-only}, query
 *                    capability, entitlement mapping, ...)
 */
public record AssetCard(
        String id,
        PeerId issuer,
        GroupId group,
        Instant issued,
        Duration ttl,
        String asset,
        String uri,
        String description,
        String shape,
        String freshness,
        Map<String, String> costHints,
        Map<String, String> access) implements Advertisement {

    public AssetCard {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(issuer, "issuer");
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(issued, "issued");
        Objects.requireNonNull(ttl, "ttl");
        Objects.requireNonNull(asset, "asset");
        Objects.requireNonNull(uri, "uri");
        Objects.requireNonNull(description, "description");
        Objects.requireNonNull(shape, "shape");
        Objects.requireNonNull(freshness, "freshness");
        costHints = Map.copyOf(Objects.requireNonNull(costHints, "costHints"));
        access = Map.copyOf(Objects.requireNonNull(access, "access"));
    }
}
