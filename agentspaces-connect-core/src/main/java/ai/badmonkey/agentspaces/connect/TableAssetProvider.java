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

import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The reference {@link AssetProvider}: an in-memory table. Real connectors
 * adapt a driver (JDBC, a warehouse REST API, a search client) behind the same
 * two methods; this one holds its rows in memory so the SDK, the protocol, and
 * the pull-once economics all test and demo without a source system. Query
 * parameters are column equality filters plus {@code limit}; the execution
 * counter is what the economics assertions read.
 */
public final class TableAssetProvider implements AssetProvider {

    private final String asset;
    private final String uri;
    private final String description;
    private final String shape;
    private final Duration freshness;
    private final List<Map<String, String>> rows;
    private final AtomicInteger executions = new AtomicInteger();

    /**
     * Creates the provider.
     *
     * @param asset       the asset name
     * @param uri         the source URI shown on the card
     * @param description the card's human- and LLM-readable description
     * @param shape       the declared shape of data
     * @param freshness   the freshness window, the result lease
     * @param rows        the table rows; column name to value
     */
    public TableAssetProvider(String asset, String uri, String description,
                              String shape, Duration freshness,
                              List<Map<String, String>> rows) {
        this.asset = Objects.requireNonNull(asset, "asset");
        this.uri = Objects.requireNonNull(uri, "uri");
        this.description = Objects.requireNonNull(description, "description");
        this.shape = Objects.requireNonNull(shape, "shape");
        this.freshness = Objects.requireNonNull(freshness, "freshness");
        this.rows = List.copyOf(Objects.requireNonNull(rows, "rows"));
    }

    /** How many queries actually executed against this table. */
    public int executions() {
        return executions.get();
    }

    @Override
    public List<AssetCard> describeAssets(GroupId group, PeerId issuer, Instant now) {
        return List.of(new AssetCard(
                "aspace://" + group.value() + "/asset/" + asset,
                issuer, group, now, Duration.ofMinutes(15),
                asset, uri, description, shape, freshness.toString(),
                Map.of("rows", String.valueOf(rows.size())),
                Map.of("mode", "read-only", "query", "aspace:cap/data-query")));
    }

    @Override
    public QueryResult query(String assetName, Map<String, String> parameters) {
        if (!asset.equals(assetName)) {
            return QueryResult.failed("no asset named '" + assetName + "'");
        }
        executions.incrementAndGet();
        int limit = Integer.MAX_VALUE;
        List<Map<String, String>> matched = new ArrayList<>();
        for (Map.Entry<String, String> parameter : parameters.entrySet()) {
            if (parameter.getKey().equals("limit")) {
                limit = Integer.parseInt(parameter.getValue());
            }
        }
        for (Map<String, String> row : rows) {
            boolean matches = true;
            for (Map.Entry<String, String> parameter : parameters.entrySet()) {
                if (parameter.getKey().equals("limit")) {
                    continue;
                }
                if (!parameter.getValue().equals(row.get(parameter.getKey()))) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                matched.add(row);
                if (matched.size() >= limit) {
                    break;
                }
            }
        }
        return QueryResult.of(matched);
    }
}
