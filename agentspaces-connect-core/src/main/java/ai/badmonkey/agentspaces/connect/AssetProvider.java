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

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * The connector SPI (ENTERPRISE §6): one implementation per source system.
 * A provider does two things, and the SDK does everything else: enumerate the
 * assets this connector holds as {@link AssetCard}s (spec §6.1a), and answer
 * one query against one asset. The {@link ConnectorRuntime} publishes the
 * cards under leases, serves the {@code aspace:cap/data-query} protocol
 * through the space, and keeps the connector honest about freshness, so a
 * provider implementation is typically a thin adapter over a driver the
 * source system already ships.
 *
 * <p>Providers hold the source credentials; agents never see them (the
 * license-consolidation posture from ENTERPRISE §5).
 */
public interface AssetProvider {

    /**
     * Describes the assets this connector currently holds.
     *
     * @param group  the group the cards are scoped to
     * @param issuer the connector's peer id
     * @param now    the issue instant for the cards
     * @return one card per asset
     */
    List<AssetCard> describeAssets(GroupId group, PeerId issuer, Instant now);

    /**
     * Answers one query.
     *
     * @param asset      the asset name from the card
     * @param parameters the query parameters; shapes are asset-specific and
     *                   declared on the card
     * @return the result, with rows on success or an error message
     */
    QueryResult query(String asset, Map<String, String> parameters);

    /**
     * Runs one operation the asset's card allowlists in {@code invoke-ops}
     * (srad/agentspaces-js.md JS-C-03, appendix F). The {@link ConnectorRuntime}
     * refuses an operation the card does not list before this is called, and
     * passes the invocation's id so the target can deduplicate a retry
     * (JS-C-04). The default refuses every operation: a provider that exposes
     * none needs no override.
     *
     * @param asset        the asset name from the card
     * @param operation    the operation, one of the card's {@code invoke-ops}
     * @param params       the operation's parameters, by declared name
     * @param invocationId the invocation's id, constant across retries
     * @return the result, or an error naming the rule that refused it
     */
    default InvokeResult invoke(String asset, String operation, Map<String, String> params,
                                String invocationId) {
        return InvokeResult.failed("unknown-operation", "provider",
                "the provider of '" + asset + "' exposes no operations");
    }

    /**
     * One operation's answer.
     *
     * @param ok     whether the operation ran and succeeded
     * @param result the operation's result on success; {@code rule} and
     *               {@code detail} on an error
     * @param error  the appendix F error code when {@code ok} is false
     *               ({@code unknown-operation}, {@code bad-params},
     *               {@code refused-scope}, {@code target-error},
     *               {@code timeout}); {@code null} on success
     */
    record InvokeResult(boolean ok, Map<String, String> result, String error) {

        public InvokeResult {
            result = Map.copyOf(java.util.Objects.requireNonNull(result, "result"));
        }

        /** Returns a successful result. */
        public static InvokeResult of(Map<String, String> result) {
            return new InvokeResult(true, result, null);
        }

        /** Returns a failed result naming the rule that refused the operation. */
        public static InvokeResult failed(String error, String rule, String detail) {
            return new InvokeResult(false, Map.of("rule", rule, "detail", detail), error);
        }
    }

    /**
     * One query's answer.
     *
     * @param rows  the result rows; column name to value
     * @param error an error message, when the query could not be answered
     */
    record QueryResult(List<Map<String, String>> rows, String error) {

        /** Returns a successful result. */
        public static QueryResult of(List<Map<String, String>> rows) {
            return new QueryResult(rows, null);
        }

        /** Returns a failed result. */
        public static QueryResult failed(String error) {
            return new QueryResult(List.of(), error);
        }
    }
}
