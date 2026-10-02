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

import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.Subscription;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.connect.DataQueryEntries.DataQuery;
import ai.badmonkey.agentspaces.connect.DataQueryEntries.DataResult;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;

/**
 * The agent side of {@code aspace:cap/data-query} (ENTERPRISE §3): fetch data
 * by asset name and parameters, local-first. A live result under the same
 * canonical hash answers immediately and costs the source system nothing;
 * otherwise the client writes a query entry and awaits the connector's result.
 * The counters make the economics observable: {@code cacheHits} is money the
 * abstraction returned.
 */
public final class DataQueryClient {

    /**
     * One fetch's answer.
     *
     * @param result    the result entry
     * @param fromCache whether a live space result answered with no new query
     */
    public record Fetched(DataResult result, boolean fromCache) {
    }

    private final Space dataSpace;
    private final String requester;
    /** Whether a result written by {@code issuer} for {@code asset} is trusted. */
    private final BiPredicate<AgentId, String> trust;
    private final AtomicInteger cacheHits = new AtomicInteger();
    private final AtomicInteger queriesSent = new AtomicInteger();

    /**
     * Creates a client that accepts a result from any admitted peer. Suitable
     * for a fleet where every member is trusted to answer honestly; a fleet
     * that is not should name its connectors via
     * {@link #DataQueryClient(Space, String, Set)}.
     *
     * @param dataSpace the space queries and results flow through
     * @param requester the asking agent's name, stamped into queries
     */
    public DataQueryClient(Space dataSpace, String requester) {
        this(dataSpace, requester, Set.of());
    }

    /**
     * Creates a client that consumes only results written by the named
     * connector peers (ASF-009): a result forged by any other admitted peer is
     * ignored, for both the cache check and the awaited answer. The trusted
     * peers come from the connectors' signed {@code AssetCard}s
     * ({@code card.issuer()}), discovered through the group's discovery
     * service.
     *
     * @param dataSpace         the space queries and results flow through
     * @param requester         the asking agent's name, stamped into queries
     * @param trustedConnectors connector peers whose results to accept; empty
     *                          accepts any peer's result
     */
    public DataQueryClient(Space dataSpace, String requester, Set<PeerId> trustedConnectors) {
        this(dataSpace, requester, trustOf(Set.copyOf(trustedConnectors)));
    }

    /**
     * Creates a client whose trust in a result is the fleet's {@link Authorizer}
     * (TODO-EFG §4, TODO item 6): a {@code DataResult} counts — for the cache
     * check and for the awaited answer — only when its space-authenticated
     * issuer's peer is permitted {@code CONNECTOR_SERVE} for
     * {@code result.asset()}. Under the OIDC profiles that is the identity
     * provider's {@code aspace:connector-serve:<asset>} scope; under the
     * membership profiles it is admission plus any {@code connector-serve}
     * grant. The trusted-set constructor remains for fleets that name their
     * connectors directly.
     *
     * @param dataSpace  the space queries and results flow through
     * @param requester  the asking agent's name, stamped into queries
     * @param authorizer decides {@code CONNECTOR_SERVE} per result issuer and asset
     */
    public DataQueryClient(Space dataSpace, String requester, Authorizer authorizer) {
        this(dataSpace, requester, authorizedBy(Objects.requireNonNull(authorizer, "authorizer")));
    }

    private DataQueryClient(Space dataSpace, String requester,
                            BiPredicate<AgentId, String> trust) {
        this.dataSpace = Objects.requireNonNull(dataSpace, "dataSpace");
        this.requester = Objects.requireNonNull(requester, "requester");
        this.trust = trust;
    }

    private static BiPredicate<AgentId, String> trustOf(Set<PeerId> trustedConnectors) {
        return (issuer, asset) -> trustedConnectors.isEmpty()
                || trustedConnectors.contains(issuer.peer());
    }

    private static BiPredicate<AgentId, String> authorizedBy(Authorizer authorizer) {
        return (issuer, asset) -> authorizer.permits(issuer.peer(),
                Authorizer.Operation.CONNECTOR_SERVE, asset == null ? "" : asset);
    }

    /**
     * Fetches one asset query, pull-once.
     *
     * @param asset      the asset name from its card
     * @param parameters the query parameters
     * @param timeout    how long to wait for a connector when the cache misses
     * @return the answer, or empty when no connector answered in time
     */
    public Optional<Fetched> fetch(String asset, Map<String, String> parameters,
                                   Duration timeout) {
        Objects.requireNonNull(asset, "asset");
        Objects.requireNonNull(parameters, "parameters");
        String hash = DataQueryEntries.canonicalHash(asset, parameters);
        Template<DataResult> results =
                Template.of(DataResult.class).where("queryHash", eq(hash));
        Optional<DataResult> live = readTrusted(results);
        if (live.isPresent()) {
            cacheHits.incrementAndGet();
            return Optional.of(new Fetched(live.get(), true));
        }
        // Subscribe before writing the query so the answer cannot slip between
        // the write and the wait; the listener applies the same issuer filter.
        CompletableFuture<DataResult> answer = new CompletableFuture<>();
        try (Subscription sub = dataSpace.notify(results, event -> {
            if (event.kind() == SpaceEvent.Kind.WRITTEN
                    && trusted(event.issuer(), event.entry())) {
                answer.complete(event.entry());
            }
        }, Lease.of(timeout.plus(Duration.ofMinutes(1))))) {
            queriesSent.incrementAndGet();
            dataSpace.write(new DataQuery(hash, asset, Map.copyOf(parameters), requester),
                    Lease.of(Duration.ofMinutes(1)));
            // The result may have landed between the cache check and the
            // subscription; re-check once now that we are subscribed.
            readTrusted(results).ifPresent(answer::complete);
            return Optional.of(new Fetched(await(answer, results, timeout), false));
        } catch (TimeoutException e) {
            return Optional.empty();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (java.util.concurrent.ExecutionException e) {
            return Optional.empty(); // cannot happen: only complete() is used
        }
    }

    /** How often the wait re-reads the space for a result whose block has not yet arrived. */
    private static final Duration READ_THROUGH_INTERVAL = Duration.ofMillis(100);

    /**
     * Waits for the subscription to deliver the result, re-reading the space
     * between checks. A bulk result travels content-addressed (spec §6.1a):
     * its record arrives before its block, and the event stream only fires
     * once the block is held locally, so the periodic read is what pulls the
     * block through the {@code BlockExchange} and completes the answer.
     */
    private DataResult await(CompletableFuture<DataResult> answer,
                             Template<DataResult> results, Duration timeout)
            throws InterruptedException, TimeoutException,
            java.util.concurrent.ExecutionException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                throw new TimeoutException("no connector answered within " + timeout);
            }
            try {
                return answer.get(Math.min(remaining, READ_THROUGH_INTERVAL.toNanos()),
                        TimeUnit.NANOSECONDS);
            } catch (TimeoutException slice) {
                readTrusted(results).ifPresent(answer::complete);
            }
        }
    }

    private Optional<DataResult> readTrusted(Template<DataResult> template) {
        return dataSpace.readAllIssued(template, 16).stream()
                .filter(r -> trusted(r.issuer(), r.entry()))
                .map(Space.Issued::entry)
                .findFirst();
    }

    private boolean trusted(AgentId issuer, DataResult result) {
        return trust.test(issuer, result == null ? null : result.asset());
    }

    /** Fetches answered by a live space result, with no source execution. */
    public int cacheHits() {
        return cacheHits.get();
    }

    /** Fetches that wrote a query entry for a connector to execute. */
    public int queriesSent() {
        return queriesSent.get();
    }
}
