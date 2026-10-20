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
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.Subscription;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;

/**
 * The {@code aspace:cap/asset-invoke} capability (srad/agentspaces-js.md
 * JS-C-03, JS-C-10, appendix F): an agent writes an {@link AssetInvocation}
 * naming an asset, one of its card's {@code invoke-ops}, and parameters; the
 * connector takes it under the claim lattice with a lease, runs the
 * operation, writes an {@link AssetInvocationResult} correlated by
 * {@code invocationId}, and completes the take. The entries carry a version
 * field and readers ignore fields they do not know (JS-F-06), so later
 * operation metadata needs no migration. Both records encode byte-identically
 * in TypeScript ({@code @agentspaces/connect}) and are pinned by the golden
 * vectors {@code asset_invocation_cbor} and {@code asset_invocation_result_cbor}.
 */
public final class AssetInvocations {

    /** The protocol version the entries carry (appendix F). */
    public static final int VERSION = 1;

    /** The capability type a card names in {@code access.invoke}. */
    public static final String CAPABILITY = "aspace:cap/asset-invoke";

    /** The {@code access} key listing the operations a card allows, as a comma list. */
    public static final String INVOKE_OPS = "invoke-ops";

    /** The default lease on an invocation entry awaiting a connector. */
    public static final Duration DEFAULT_INVOCATION_LEASE = Duration.ofMinutes(5);

    /** The default wait for a connector's result. */
    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    /**
     * One operation an agent asks a connector to run.
     *
     * @param v            the protocol version; receivers ignore unknown fields
     * @param asset        the asset name from the card
     * @param operation    the operation, one of the card's {@code invoke-ops}
     * @param params       the operation's parameters, by declared name
     * @param invocationId the invocation's id, minted once by the client and
     *                     constant across retries and takers, so a target
     *                     deduplicates on it (JS-C-04)
     */
    public record AssetInvocation(int v, String asset, String operation,
                                  Map<String, String> params, String invocationId) {

        public AssetInvocation {
            Objects.requireNonNull(asset, "asset");
            Objects.requireNonNull(operation, "operation");
            params = Map.copyOf(Objects.requireNonNull(params, "params"));
            Objects.requireNonNull(invocationId, "invocationId");
        }

        /** An invocation at the current version with a fresh id. */
        public static AssetInvocation of(String asset, String operation, Map<String, String> params) {
            return new AssetInvocation(VERSION, asset, operation, params, UUID.randomUUID().toString());
        }
    }

    /**
     * The connector's answer, correlated by {@code invocationId}.
     *
     * @param v            the protocol version
     * @param invocationId the invocation answered
     * @param asset        the asset name
     * @param operation    the operation
     * @param ok           whether the operation ran and succeeded
     * @param result       the operation's result on success; {@code rule} (the
     *                     rule that refused) and {@code detail} on an error
     * @param error        the appendix F error code when {@code ok} is false;
     *                     {@code null} on success
     */
    public record AssetInvocationResult(int v, String invocationId, String asset, String operation,
                                        boolean ok, Map<String, String> result, String error) {

        public AssetInvocationResult {
            Objects.requireNonNull(invocationId, "invocationId");
            Objects.requireNonNull(asset, "asset");
            Objects.requireNonNull(operation, "operation");
            result = Map.copyOf(Objects.requireNonNull(result, "result"));
        }

        /** The result of running an invocation, from the provider's answer. */
        public static AssetInvocationResult of(AssetInvocation invocation, AssetProvider.InvokeResult outcome) {
            return new AssetInvocationResult(VERSION, invocation.invocationId(), invocation.asset(),
                    invocation.operation(), outcome.ok(), outcome.result(),
                    outcome.ok() ? null : outcome.error() == null ? "target-error" : outcome.error());
        }

        /** An error result naming the rule that refused the invocation. */
        public static AssetInvocationResult refused(AssetInvocation invocation, String error,
                                                    String rule, String detail) {
            return new AssetInvocationResult(VERSION, invocation.invocationId(), invocation.asset(),
                    invocation.operation(), false, Map.of("rule", rule, "detail", detail), error);
        }
    }

    private AssetInvocations() {
    }

    /**
     * The operations a card allows: its {@code invoke-ops} comma list, trimmed.
     *
     * @param card the asset's card
     * @return the allowed operation names, in card order
     */
    public static java.util.List<String> invokeOps(AssetCard card) {
        String listed = card.access().get(INVOKE_OPS);
        if (listed == null || listed.isBlank()) {
            return java.util.List.of();
        }
        return java.util.Arrays.stream(listed.split(",")).map(String::trim)
                .filter(s -> !s.isEmpty()).toList();
    }

    /**
     * Invokes one operation on an asset (JS-C-10): writes an
     * {@link AssetInvocation} into {@code space} and completes the future with
     * the {@link AssetInvocationResult} the card's issuer writes for it. A
     * result from any other peer is ignored (ASF-009). The future fails with a
     * {@link java.util.concurrent.TimeoutException} after
     * {@link #DEFAULT_TIMEOUT}.
     *
     * @param space     the space invocations and results flow through (the
     *                  connector's task space)
     * @param card      the asset's card, naming the asset and its connector
     * @param operation the operation, one of the card's {@code invoke-ops}
     * @param params    the operation's parameters
     * @return the connector's result
     */
    public static CompletableFuture<AssetInvocationResult> invoke(Space space, AssetCard card, String operation,
                                                                  Map<String, String> params) {
        return invoke(space, card, operation, params, DEFAULT_TIMEOUT);
    }

    /**
     * {@link #invoke(Space, AssetCard, String, Map)} with an explicit wait.
     *
     * @param space     the space invocations and results flow through
     * @param card      the asset's card
     * @param operation the operation
     * @param params    the operation's parameters
     * @param timeout   how long to wait for the connector's result
     * @return the connector's result
     */
    public static CompletableFuture<AssetInvocationResult> invoke(Space space, AssetCard card, String operation,
                                                                  Map<String, String> params, Duration timeout) {
        return retry(space, card, AssetInvocation.of(card.asset(), operation, params), timeout);
    }

    /**
     * Writes an invocation again under its original id (a retry, JS-C-04) and
     * awaits the result; the connector answers from its remembered result
     * rather than running the target twice.
     *
     * @param space      the space invocations and results flow through
     * @param card       the asset's card
     * @param invocation the invocation to retry, id and all
     * @param timeout    how long to wait for the connector's result
     * @return the connector's result
     */
    public static CompletableFuture<AssetInvocationResult> retry(Space space, AssetCard card,
                                                                 AssetInvocation invocation, Duration timeout) {
        Objects.requireNonNull(space, "space");
        Objects.requireNonNull(card, "card");
        Objects.requireNonNull(timeout, "timeout");
        PeerId connector = card.issuer();
        Template<AssetInvocationResult> results = Template.of(AssetInvocationResult.class)
                .where("invocationId", eq(invocation.invocationId()));
        CompletableFuture<AssetInvocationResult> answer = new CompletableFuture<>();
        // Subscribe before writing so the answer cannot slip between the write and the wait.
        Subscription subscription = space.notify(results, event -> {
            if (event.kind() == SpaceEvent.Kind.WRITTEN && event.issuer().peer().equals(connector)) {
                answer.complete(event.entry());
            }
        }, Lease.of(timeout.plus(Duration.ofMinutes(1))));
        try {
            space.write(invocation, Lease.of(DEFAULT_INVOCATION_LEASE));
            // The result may have landed before the subscription (a retry of an answered invocation).
            space.readAllIssued(results, 16).stream()
                    .filter(issued -> issued.issuer().peer().equals(connector))
                    .findFirst().ifPresent(issued -> answer.complete(issued.entry()));
        } catch (RuntimeException e) {
            subscription.close();
            answer.completeExceptionally(e);
            return answer;
        }
        return answer.orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS)
                .whenComplete((result, failure) -> subscription.close());
    }
}
