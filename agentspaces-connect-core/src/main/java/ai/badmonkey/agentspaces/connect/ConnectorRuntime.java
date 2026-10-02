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
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.connect.DataQueryEntries.AssetChange;
import ai.badmonkey.agentspaces.connect.DataQueryEntries.DataQuery;
import ai.badmonkey.agentspaces.connect.DataQueryEntries.DataResult;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.AdvertisementSigner;
import ai.badmonkey.agentspaces.identity.PeerIdentity;

import java.time.Duration;
import java.time.InstantSource;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;

/**
 * Runs one connector (ENTERPRISE §6): publishes the provider's AssetCards
 * under leases so agents can discover the holdings, and serves the
 * {@code aspace:cap/data-query} protocol from the data space with pull-once
 * semantics: before executing against the source, the runtime checks whether a
 * live result for the same canonical hash already exists, and an existing
 * result costs the source nothing. Each asset's card declares its freshness
 * window, and that window is the result entry's lease, so staleness policy is
 * data the connector operator sets per asset rather than code.
 *
 * <p>A provider that is also a {@link MaterializingAssetProvider} gets the
 * push style too (spec §6.1a): the runtime writes each change the provider
 * emits into the change space as an {@link AssetChange} under the same
 * per-asset freshness lease, so the lease is the retention policy and
 * subscribers see the source's events through {@code Space.notify}.
 */
public final class ConnectorRuntime implements AutoCloseable {

    private static final System.Logger LOG =
            System.getLogger(ConnectorRuntime.class.getName());

    private final Space dataSpace;
    private final Space changeSpace;
    private final DiscoveryService discovery;
    private final PeerIdentity identity;
    private final String connectorName;
    private final AssetProvider provider;
    private final GroupId group;
    private final InstantSource clock;
    private final Duration defaultFreshness;
    private final AdvertisementSigner signer = new AdvertisementSigner();
    private final Map<String, Duration> freshnessByAsset = new HashMap<>();
    private final AtomicInteger executed = new AtomicInteger();
    private final AtomicInteger servedFromSpace = new AtomicInteger();
    private final AtomicInteger materialized = new AtomicInteger();
    /** Optional (TODO-EFG §4): when set, only assets this connector's own peer is
     * permitted {@code CONNECTOR_SERVE} for are advertised. */
    private volatile Authorizer authorizer;
    private volatile boolean running = true;
    private Thread loop;
    private AutoCloseable feed;

    /**
     * Creates the runtime.
     *
     * @param dataSpace        the space queries and results flow through
     * @param discovery        the group's discovery service for card publication
     * @param identity         the connector's peer identity
     * @param connectorName    the connector's agent name, stamped into results
     * @param provider         the source-system adapter
     * @param group            the group the cards are scoped to
     * @param clock            the time source
     * @param defaultFreshness the result lease when a card declares no
     *                         parseable freshness window
     */
    public ConnectorRuntime(Space dataSpace, DiscoveryService discovery,
                            PeerIdentity identity, String connectorName,
                            AssetProvider provider, GroupId group,
                            InstantSource clock, Duration defaultFreshness) {
        this(dataSpace, dataSpace, discovery, identity, connectorName, provider, group,
                clock, defaultFreshness);
    }

    /**
     * Creates the runtime with a separate space for materialized changes
     * (spec §6.1a): a {@link MaterializingAssetProvider}'s changes are written
     * there as {@link AssetChange} entries, each leased for its asset's
     * freshness window.
     *
     * @param dataSpace        the space queries and results flow through
     * @param changeSpace      the space materialized changes are pushed into
     * @param discovery        the group's discovery service for card publication
     * @param identity         the connector's peer identity
     * @param connectorName    the connector's agent name, stamped into results
     * @param provider         the source-system adapter
     * @param group            the group the cards are scoped to
     * @param clock            the time source
     * @param defaultFreshness the result and change lease when a card declares
     *                         no parseable freshness window
     */
    public ConnectorRuntime(Space dataSpace, Space changeSpace, DiscoveryService discovery,
                            PeerIdentity identity, String connectorName,
                            AssetProvider provider, GroupId group,
                            InstantSource clock, Duration defaultFreshness) {
        this.dataSpace = Objects.requireNonNull(dataSpace, "dataSpace");
        this.changeSpace = Objects.requireNonNull(changeSpace, "changeSpace");
        this.discovery = Objects.requireNonNull(discovery, "discovery");
        this.identity = Objects.requireNonNull(identity, "identity");
        this.connectorName = Objects.requireNonNull(connectorName, "connectorName");
        this.provider = Objects.requireNonNull(provider, "provider");
        this.group = Objects.requireNonNull(group, "group");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.defaultFreshness = Objects.requireNonNull(defaultFreshness, "defaultFreshness");
    }

    /**
     * Gives the runtime the fleet's {@link Authorizer} (TODO-EFG §4, TODO
     * item 6). From then on {@link #start()} and every {@link #refreshCards()}
     * publish a card only for an asset the connector's own peer is permitted
     * {@code CONNECTOR_SERVE} for — under the OIDC profiles the identity
     * provider's {@code aspace:connector-serve:<asset>} scope — and log the
     * assets they refuse to advertise, so a connector deployed with a token
     * that does not cover a source never claims to serve it. Call before
     * {@link #start()}; without an authorizer every described asset is
     * advertised, as before.
     *
     * @param authorizer decides {@code CONNECTOR_SERVE} for this peer per asset
     * @return this runtime
     */
    public ConnectorRuntime authorizer(Authorizer authorizer) {
        this.authorizer = Objects.requireNonNull(authorizer, "authorizer");
        return this;
    }

    /**
     * Publishes the current AssetCards, starts serving queries, and, for a
     * {@link MaterializingAssetProvider}, starts the change feed.
     */
    public synchronized void start() {
        refreshCards();
        if (loop == null) {
            loop = Thread.ofVirtual().name("connector-" + connectorName)
                    .start(this::serveLoop);
        }
        if (feed == null && provider instanceof MaterializingAssetProvider materializing) {
            feed = materializing.materialize(this::push);
        }
    }

    /**
     * Re-publishes the provider's AssetCards with fresh issue times (P2). Call
     * on the same cadence as other leased state.
     */
    public void refreshCards() {
        Authorizer gate = authorizer;
        for (AssetCard card : provider.describeAssets(group, identity.peerId(),
                clock.instant())) {
            if (gate != null && !gate.permits(identity.peerId(),
                    Authorizer.Operation.CONNECTOR_SERVE, card.asset())) {
                LOG.log(System.Logger.Level.WARNING, "connector " + connectorName
                        + " is not permitted CONNECTOR_SERVE for asset '" + card.asset()
                        + "'; not advertising it");
                continue;
            }
            freshnessByAsset.put(card.asset(), parseFreshness(card.freshness()));
            discovery.publish(signer.sign(card, identity));
        }
    }

    /** How many queries this connector executed against its source. */
    public int executed() {
        return executed.get();
    }

    /** How many queries a live space result answered with no source execution. */
    public int servedFromSpace() {
        return servedFromSpace.get();
    }

    /** How many source changes this connector materialized into the change space. */
    public int materialized() {
        return materialized.get();
    }

    @Override
    public synchronized void close() {
        running = false;
        if (feed != null) {
            try {
                feed.close();
            } catch (Exception e) {
                LOG.log(System.Logger.Level.WARNING, "change feed close failed", e);
            }
            feed = null;
        }
        if (loop != null) {
            loop.interrupt();
            loop = null;
        }
    }

    // ---------------------------------------------------------------- internals

    private void serveLoop() {
        while (running) {
            Optional<TakenEntry<DataQuery>> taken;
            try {
                taken = dataSpace.take(Template.of(DataQuery.class),
                        Lease.of(Duration.ofMinutes(1)), Duration.ofSeconds(1));
            } catch (RuntimeException e) {
                if (running) {
                    LOG.log(System.Logger.Level.WARNING, "query take failed", e);
                }
                continue;
            }
            if (taken.isEmpty()) {
                continue;
            }
            answer(taken.get());
        }
    }

    private void answer(TakenEntry<DataQuery> taken) {
        DataQuery query = taken.entry();
        // ASF-009: the asker's queryHash is untrusted input. Recompute the
        // canonical hash for the dedupe check and the stored result, so a
        // client-chosen hash can neither transplant a result nor pre-poison the
        // hash honest askers will compute.
        String hash = DataQueryEntries.canonicalHash(query.asset(), query.parameters());
        if (!hash.equals(query.queryHash())) {
            LOG.log(System.Logger.Level.WARNING,
                    "dropping query with forged hash for asset " + query.asset());
            completeQuietly(taken);
            return;
        }
        Duration freshness = freshnessByAsset.getOrDefault(query.asset(), defaultFreshness);
        try {
            // Pull once: a live result for this hash already serves the window —
            // but only a result this connector itself issued counts. A result
            // written by any other peer (a poisoning attempt, or another
            // connector) never suppresses execution against the source.
            boolean alreadyAnswered = dataSpace.readAllIssued(Template.of(DataResult.class)
                            .where("queryHash", eq(hash)), 16).stream()
                    .anyMatch(r -> r.issuer().peer().equals(identity.peerId()));
            if (alreadyAnswered) {
                servedFromSpace.incrementAndGet();
                dataSpace.complete(taken);
                return;
            }
            AssetProvider.QueryResult result = provider.query(
                    query.asset(), query.parameters());
            executed.incrementAndGet();
            dataSpace.complete(taken, new DataResult(hash, query.asset(),
                    result.rows(), result.error(), connectorName),
                    Lease.of(freshness));
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING,
                    "query failed for asset " + query.asset(), e);
            try {
                dataSpace.complete(taken, new DataResult(hash, query.asset(),
                        java.util.List.of(), String.valueOf(e.getMessage()),
                        connectorName), Lease.of(Duration.ofSeconds(30)));
            } catch (RuntimeException lapsed) {
                // The take lease lapsed; the query reappears for a retry.
            }
        }
    }

    /**
     * Spec §6.1a, the materializing style: one source change becomes one typed
     * entry whose lease is the asset's freshness window, so retention is the
     * lease and a connector that stops pushing leaves nothing stale behind.
     */
    private void push(MaterializingAssetProvider.Change change) {
        if (!running) {
            return;
        }
        Duration freshness = freshnessByAsset.getOrDefault(change.asset(), defaultFreshness);
        try {
            changeSpace.write(new AssetChange(change.asset(), change.key(), change.row(),
                    connectorName), Lease.of(freshness));
            materialized.incrementAndGet();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING,
                    "materializing change " + change.key() + " of asset "
                            + change.asset() + " failed", e);
        }
    }

    private void completeQuietly(TakenEntry<DataQuery> taken) {
        try {
            dataSpace.complete(taken);
        } catch (RuntimeException lapsed) {
            // The take lease lapsed; nothing to do.
        }
    }

    private Duration parseFreshness(String freshness) {
        try {
            return Duration.parse(freshness);
        } catch (RuntimeException e) {
            return defaultFreshness;
        }
    }
}
