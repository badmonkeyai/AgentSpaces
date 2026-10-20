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

package ai.badmonkey.agentspaces.agent.remote;

import ai.badmonkey.agentspaces.api.ad.AssetCard;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * One operation an {@link AssetCard} allowlists, listed beside the card
 * actions of {@link RemoteActions} so a planner sees {@code reserveStock} on
 * {@code workers://inventory/do/Inventory} as an available action (SPEC
 * §10.6; srad/agentspaces-js.md JS-C-11). A read operation answers a data
 * query; an invoke operation runs through the {@code aspace:cap/asset-invoke}
 * protocol of {@code agentspaces-connect-core}, which this module does not
 * depend on: the registry's {@link RemoteActions#assetInvoker(AssetInvoker)}
 * hook carries the call, and a host wires {@code AssetInvocations} to it.
 */
public final class AssetOperation {

    /** Whether the operation reads (a data query) or invokes (an asset invocation). */
    public enum Kind { READ, INVOKE }

    private final AssetCard card;
    private final String operation;
    private final Kind kind;
    private final AssetInvoker invoker;

    AssetOperation(AssetCard card, String operation, Kind kind, AssetInvoker invoker) {
        this.card = Objects.requireNonNull(card, "card");
        this.operation = Objects.requireNonNull(operation, "operation");
        this.kind = Objects.requireNonNull(kind, "kind");
        this.invoker = invoker;
    }

    /** The asset's card. */
    public AssetCard card() {
        return card;
    }

    /** The operation's name, as the card's {@code read-ops} or {@code invoke-ops} lists it. */
    public String operation() {
        return operation;
    }

    public Kind kind() {
        return kind;
    }

    /** The action's name for a planner: {@code <operation> on <uri>}. */
    public String name() {
        return operation + " on " + card.uri();
    }

    /** The card's description. */
    public String description() {
        return card.description();
    }

    /**
     * Runs the operation through the registry's invoker and waits for the
     * connector's result.
     *
     * @param params  the operation's parameters, validated by the connector against its manifest
     * @param timeout how long to wait for the result
     * @return the result map, or empty when no result arrived in time
     * @throws IllegalStateException when no invoker is wired (the host has not registered
     *                               {@code AssetInvocations} of {@code agentspaces-connect-core})
     */
    public Optional<Map<String, Object>> invoke(Map<String, Object> params, Duration timeout) {
        if (invoker == null) {
            throw new IllegalStateException("no asset invoker wired: register AssetInvocations::invoke "
                    + "(agentspaces-connect-core) through RemoteActions.assetInvoker");
        }
        return invoker.invoke(card, operation, params, timeout);
    }

    @Override
    public String toString() {
        return kind.name().toLowerCase(java.util.Locale.ROOT) + " " + name();
    }

    /**
     * Carries an asset operation to the connector; {@code agentspaces-connect-core}'s
     * {@code AssetInvocations} is the implementation, wired by the host.
     */
    @FunctionalInterface
    public interface AssetInvoker {
        /**
         * @param card      the asset's card
         * @param operation the operation
         * @param params    its parameters
         * @param timeout   how long to wait
         * @return the result, or empty on timeout
         */
        Optional<Map<String, Object>> invoke(AssetCard card, String operation, Map<String, Object> params,
                                             Duration timeout);
    }
}
