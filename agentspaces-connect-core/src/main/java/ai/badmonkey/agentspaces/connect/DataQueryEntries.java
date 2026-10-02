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
package ai.badmonkey.agentspaces.connect;

import ai.badmonkey.agentspaces.common.codec.Multibase;
import ai.badmonkey.agentspaces.common.crypto.Digests;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The {@code aspace:cap/data-query} entry types (spec §6.1a): agents write
 * {@link DataQuery} entries, connectors take them and complete with
 * {@link DataResult} entries, and the space gives both sides retries,
 * observability, and decoupling for free. The canonical query hash is the key
 * to the economics (ENTERPRISE §3): identical queries share one hash, so one
 * result under its freshness lease serves every asker in the window, and the
 * source system sees one execution instead of one per agent per poll.
 */
public final class DataQueryEntries {

    /**
     * One query awaiting a connector.
     *
     * @param queryHash  the canonical hash of (asset, parameters)
     * @param asset      the asset name from the card
     * @param parameters the query parameters
     * @param requester  the asking agent, for the audit trail
     */
    public record DataQuery(String queryHash, String asset,
                            Map<String, String> parameters, String requester) {
    }

    /**
     * One answered query, leased for its freshness window.
     *
     * @param queryHash the canonical hash the result answers
     * @param asset     the asset name
     * @param rows      result rows; column name to value
     * @param error     an error message when the query failed
     * @param servedBy  the connector that executed against the source
     */
    public record DataResult(String queryHash, String asset,
                             List<Map<String, String>> rows, String error,
                             String servedBy) {
    }

    /**
     * One change a materializing provider pushed (spec §6.1a), leased for the
     * asset's freshness window so the lease is the retention policy. Subscribe
     * with {@code Template.of(AssetChange.class).where("asset", eq(name))} for
     * complex event processing over the source's events.
     *
     * @param asset     the asset name from its card
     * @param key       the changed record's key within the asset
     * @param row       the changed record; column name to value
     * @param servedBy  the connector that materialized the change
     */
    public record AssetChange(String asset, String key, Map<String, String> row,
                              String servedBy) {
    }

    private DataQueryEntries() {
    }

    /**
     * The canonical query hash: multibase(sha-256) over the asset name and the
     * parameters in sorted key order, so parameter ordering never splits the
     * cache.
     *
     * @param asset      the asset name
     * @param parameters the query parameters
     * @return the canonical hash
     */
    public static String canonicalHash(String asset, Map<String, String> parameters) {
        StringBuilder canonical = new StringBuilder(asset);
        new TreeMap<>(parameters).forEach((key, value) ->
                canonical.append('|').append(key).append('=').append(value));
        return Multibase.base58btc(Digests.sha256(
                canonical.toString().getBytes(StandardCharsets.UTF_8)));
    }
}
