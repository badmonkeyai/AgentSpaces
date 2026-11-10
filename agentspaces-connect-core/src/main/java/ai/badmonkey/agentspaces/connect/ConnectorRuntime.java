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
import ai.badmonkey.agentspaces.api.space.Matchers;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.Subscription;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.connect.AssetInvocations.AssetInvocation;
import ai.badmonkey.agentspaces.connect.AssetInvocations.AssetInvocationResult;
import ai.badmonkey.agentspaces.connect.DataQueryEntries.AssetChange;
import ai.badmonkey.agentspaces.connect.DataQueryEntries.DataQuery;
import ai.badmonkey.agentspaces.connect.DataQueryEntries.DataResult;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.AdvertisementSigner;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

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
 *
 * <p>The runtime also serves {@code aspace:cap/asset-invoke}
 * (srad/agentspaces-js.md JS-C-03, appendix F) from the invocation space
 * (the data space unless {@link #invocationSpace(Space)} names another): an
 * {@link AssetInvocation} for an advertised asset is taken under the claim
 * lattice with a lease, run through {@link AssetProvider#invoke} only when
 * the asset's card lists the operation in {@code invoke-ops} (JS-C-07) and,
 * under an authorizer, only when the requesting peer is permitted
 * {@code CONNECTOR_INVOKE} in {@code <asset>:<operation>} (JS-C-02); the
 * {@link AssetInvocationResult} is written and the take completed. An
 * invocation answered once is remembered for {@link #invocationRetention}
 * so a retry under the same id never runs the target here a second time.
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
    private final Map<String, Duration> freshnessByAsset = new java.util.concurrent.ConcurrentHashMap<>();
    private final AtomicInteger executed = new AtomicInteger();
    private final AtomicInteger servedFromSpace = new AtomicInteger();
    private final AtomicInteger materialized = new AtomicInteger();
    private final AtomicInteger invoked = new AtomicInteger();
    private final AtomicInteger refused = new AtomicInteger();
    private final AtomicInteger repeated = new AtomicInteger();
    private final AtomicInteger mirrored = new AtomicInteger();
    private final AtomicInteger renewals = new AtomicInteger();
    private final AtomicInteger withdrawn = new AtomicInteger();
    /** This connector's live query results by asset, so a source change can withdraw them (CC-L-09). */
    private final Map<String, List<Issued>> resultsByAsset = new ConcurrentHashMap<>();
    /** When set, the peers that serve the same assets as replicas of this connector (CC-L-08). */
    private volatile Predicate<PeerId> replicas;
    private Subscription mirror;
    /** The operations each advertised asset's card allows (appendix F {@code invoke-ops}). */
    private final Map<String, Set<String>> invokeOpsByAsset = new ConcurrentHashMap<>();
    /** Answered invocations by id, kept for {@link #invocationRetention} so a retry never re-runs the target. */
    private final Map<String, Answered> answered = new ConcurrentHashMap<>();
    /** Optional (TODO-EFG §4): when set, only assets this connector's own peer is
     * permitted {@code CONNECTOR_SERVE} for are advertised. */
    private volatile Authorizer authorizer;
    private volatile Space invocationSpace;
    private volatile Duration invokeLease = Duration.ofSeconds(30);
    private volatile Duration invocationRetention = Duration.ofHours(24);
    private volatile boolean running = true;
    private Thread loop;
    private Thread invokeLoop;
    private AutoCloseable feed;

    /** One remembered answer. */
    private record Answered(AssetInvocationResult result, Instant at) {
    }

    /** One query result this connector wrote, with the instant its lease lapses. */
    private record Issued(EntryHandle handle, Instant until) {
    }

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
        this.invocationSpace = dataSpace;
    }

    /**
     * Names the space invocations and their results flow through (appendix
     * F: the connector's task space); the data space unless set. Call before
     * {@link #start()}.
     *
     * @param space the invocation space
     * @return this runtime
     */
    public ConnectorRuntime invocationSpace(Space space) {
        this.invocationSpace = Objects.requireNonNull(space, "space");
        return this;
    }

    /**
     * Sets the take lease on an invocation (appendix F: 30 s by default).
     *
     * @param lease the lease; the operation must finish within it
     * @return this runtime
     */
    public ConnectorRuntime invokeLease(Duration lease) {
        this.invokeLease = Objects.requireNonNull(lease, "lease");
        return this;
    }

    /**
     * Sets how long an answered invocation's result is remembered so a retry
     * under the same id is answered without a second run (24 h by default).
     *
     * @param retention the retention
     * @return this runtime
     */
    public ConnectorRuntime invocationRetention(Duration retention) {
        this.invocationRetention = Objects.requireNonNull(retention, "retention");
        return this;
    }

    /**
     * Names the peers that run as replicas of this connector: they serve the same assets from the
     * same source under their own identities (CC-L-08). Before running an invocation, the runtime
     * looks for a result a replica already wrote for the same id and answers with it; and it
     * mirrors each replica's result under its own identity, because a requester accepts a result
     * only from the issuer of the card it invoked through. Call before {@link #start()}; without
     * replicas a result from any other peer never counts, as before.
     *
     * @param replicas decides whether a peer is a replica of this connector
     * @return this runtime
     */
    public ConnectorRuntime replicas(Predicate<PeerId> replicas) {
        this.replicas = Objects.requireNonNull(replicas, "replicas");
        return this;
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
     * Publishes the current AssetCards, starts serving queries and
     * invocations, and, for a {@link MaterializingAssetProvider}, starts the
     * change feed.
     */
    public synchronized void start() {
        refreshCards();
        if (loop == null) {
            loop = Thread.ofVirtual().name("connector-" + connectorName)
                    .start(this::serveLoop);
        }
        if (invokeLoop == null) {
            invokeLoop = Thread.ofVirtual().name("connector-invoke-" + connectorName)
                    .start(this::invokeLoop);
        }
        if (feed == null && provider instanceof MaterializingAssetProvider materializing) {
            feed = materializing.materialize(this::push);
        }
        if (mirror == null && replicas != null) {
            mirror = invocationSpace.notify(Template.of(AssetInvocationResult.class), this::mirror,
                    Lease.of(Duration.ofDays(3650)));
        }
    }

    /**
     * Withdraws this connector's live query results for an asset, so the next query for it
     * reaches the source instead of a result that predates a source change (CC-L-09).
     *
     * @param asset the asset
     * @return how many results were withdrawn
     */
    public int withdrawResults(String asset) {
        List<Issued> held = resultsByAsset.remove(asset);
        if (held == null) {
            return 0;
        }
        Instant now = clock.instant();
        int n = 0;
        synchronized (held) {
            for (Issued issued : held) {
                if (issued.until().isAfter(now)) {
                    try {
                        issued.handle().cancel();
                        n++;
                    } catch (RuntimeException lapsed) {
                        // The lease lapsed meanwhile; nothing to withdraw.
                    }
                }
            }
        }
        withdrawn.addAndGet(n);
        return n;
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
            invokeOpsByAsset.put(card.asset(), Set.copyOf(AssetInvocations.invokeOps(card)));
            discovery.publish(signer.sign(card, identity));
        }
    }

    /** How many invocations ran against the provider. */
    public int invoked() {
        return invoked.get();
    }

    /** How many invocations were answered with an error result without running. */
    public int refused() {
        return refused.get();
    }

    /** How many retried invocations were answered from the remembered result. */
    public int repeated() {
        return repeated.get();
    }

    /** How many queries this connector executed against its source. */
    public int executed() {
        return executed.get();
    }

    /** How many queries a live space result answered with no source execution. */
    public int servedFromSpace() {
        return servedFromSpace.get();
    }

    /** How many replica results this connector wrote again under its own identity. */
    public int mirrored() {
        return mirrored.get();
    }

    /** How many times a running invocation's claim was renewed. */
    public int renewals() {
        return renewals.get();
    }

    /** How many live query results {@link #withdrawResults(String)} took back. */
    public int withdrawn() {
        return withdrawn.get();
    }

    /** How many source changes this connector materialized into the change space. */
    public int materialized() {
        return materialized.get();
    }

    @Override
    public synchronized void close() {
        running = false;
        if (mirror != null) {
            mirror.close();
            mirror = null;
        }
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
        if (invokeLoop != null) {
            invokeLoop.interrupt();
            invokeLoop = null;
        }
    }

    // ---------------------------------------------------------------- internals

    private void serveLoop() {
        while (running) {
            Optional<TakenEntry<DataQuery>> taken;
            try {
                // Only the queries for assets this connector advertises: several connectors may
                // share one data space (a vector store and a knowledge graph, say), and a take
                // of every DataQuery would let one of them claim, and fail, the other's queries.
                Template<DataQuery> mine = ownQueries();
                if (mine == null) {
                    Thread.sleep(Duration.ofSeconds(1));
                    continue;
                }
                taken = dataSpace.take(mine, Lease.of(Duration.ofMinutes(1)), Duration.ofSeconds(1));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
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

    /** The take template for this connector's assets, or null while it advertises none. */
    private Template<DataQuery> ownQueries() {
        Object[] assets = freshnessByAsset.keySet().toArray();
        if (assets.length == 0) {
            return null;
        }
        return Template.of(DataQuery.class).where("asset", Matchers.in(assets));
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
            EntryHandle handle = dataSpace.complete(taken, new DataResult(hash, query.asset(),
                    result.rows(), result.error(), connectorName),
                    Lease.of(freshness));
            track(query.asset(), handle, freshness);
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

    private void track(String asset, EntryHandle handle, Duration lease) {
        if (handle == null) {
            return;
        }
        Instant now = clock.instant();
        List<Issued> held = resultsByAsset.computeIfAbsent(asset, a -> new ArrayList<>());
        synchronized (held) {
            held.removeIf(issued -> !issued.until().isAfter(now));
            held.add(new Issued(handle, now.plus(lease)));
        }
    }

    private void completeQuietly(TakenEntry<DataQuery> taken) {
        try {
            dataSpace.complete(taken);
        } catch (RuntimeException lapsed) {
            // The take lease lapsed; nothing to do.
        }
    }

    // ------------------------------------------------------------ asset-invoke

    private void invokeLoop() {
        while (running) {
            Space space = invocationSpace;
            Optional<TakenEntry<AssetInvocation>> taken;
            Map<String, PeerId> requesters;
            try {
                Object[] assets = freshnessByAsset.keySet().toArray();
                if (assets.length == 0) {
                    Thread.sleep(Duration.ofSeconds(1));
                    continue;
                }
                Template<AssetInvocation> mine = Template.of(AssetInvocation.class)
                        .where("asset", Matchers.in(assets));
                // A space other than a replicated one gives no record of a claimed entry, so there the
                // requesting peer is read before the take: a template read cannot see an entry while
                // it is claimed (issue #16 §9.3), and the scope is judged against the requesting
                // entry's signer (appendix F).
                requesters = new HashMap<>();
                if (!(space instanceof ReplicatedSpace)) {
                    for (Space.Issued<AssetInvocation> issued : space.readAllIssued(mine, 64)) {
                        requesters.put(issued.entry().invocationId(), issued.issuer().peer());
                    }
                }
                taken = space.take(mine, Lease.of(invokeLease), Duration.ofSeconds(1));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                if (running) {
                    LOG.log(System.Logger.Level.WARNING, "invocation take failed", e);
                }
                continue;
            }
            if (taken.isEmpty()) {
                continue;
            }
            handle(space, taken.get(), requesters);
        }
    }

    /**
     * One invocation under the claim lattice (JS-C-03): refused with an error
     * result naming the rule when the operation is absent from the card's
     * {@code invoke-ops} (JS-C-07) or the requester lacks the invoke scope
     * (JS-C-02); otherwise run once, the result remembered so a retry under
     * the same id is answered without a second run; the result written, then
     * the take completed.
     */
    private void handle(Space space, TakenEntry<AssetInvocation> taken, Map<String, PeerId> requesters) {
        AssetInvocation invocation = taken.entry();
        String id = invocation.invocationId();
        Answered remembered = answered.get(id);
        if (remembered != null) {
            repeated.incrementAndGet();
            // The earlier result still serves the window: complete the retry under it. Otherwise
            // write the remembered result again; the target never runs twice here.
            boolean live = space.readAllIssued(Template.of(AssetInvocationResult.class)
                            .where("invocationId", eq(id)), 16).stream()
                    .anyMatch(r -> r.issuer().peer().equals(identity.peerId()));
            if (live) {
                completeQuietly(space, taken);
            } else {
                finish(space, taken, remembered.result());
            }
            return;
        }
        // The requester is the signer of the entry taken. Any member can write an invocation under an id another
        // member's invocation carries, so a replicated space answers from the taken entry's own record, which it
        // holds while the entry is claimed; the scan's signer by id serves a space whose writers share one peer
        // (PDT 2 follow-ups, 2026-11-09: a store's pin invoke charges its signer).
        PeerId requester = space instanceof ReplicatedSpace replicated
                ? replicated.recordOf(taken.entryId()).map(record -> record.issuer().peer()).orElse(null)
                : requesters.get(id);
        Set<String> allowed = invokeOpsByAsset.getOrDefault(invocation.asset(), Set.of());
        Authorizer gate = authorizer;
        AssetInvocationResult result;
        if (!allowed.contains(invocation.operation())) {
            refused.incrementAndGet();
            result = AssetInvocationResult.refused(invocation, "unknown-operation", AssetInvocations.INVOKE_OPS,
                    "operation '" + invocation.operation() + "' is not in the invoke-ops of asset '"
                            + invocation.asset() + "' (" + (allowed.isEmpty() ? "none" : String.join(",", allowed))
                            + ")");
        } else if (gate != null && (requester == null || !gate.permits(requester,
                Authorizer.Operation.CONNECTOR_INVOKE, invocation.asset() + ":" + invocation.operation()))) {
            refused.incrementAndGet();
            result = AssetInvocationResult.refused(invocation, "refused-scope",
                    "aspace:connector-invoke:" + invocation.asset() + ":" + invocation.operation(),
                    "requester " + (requester == null ? "unknown" : requester.value())
                            + " is not permitted to invoke '" + invocation.operation() + "' on '"
                            + invocation.asset() + "'");
        } else {
            AssetInvocationResult fromReplica = replicaResult(space, id);
            if (fromReplica != null) {
                repeated.incrementAndGet();
                result = fromReplica;
            } else {
                result = runHeld(taken, invocation, requester);
            }
        }
        remember(id, result);
        finish(space, taken, result);
    }

    /** A result a replica already wrote for this invocation id, or null (CC-L-08). */
    private AssetInvocationResult replicaResult(Space space, String id) {
        Predicate<PeerId> peers = replicas;
        if (peers == null) {
            return null;
        }
        return space.readAllIssued(Template.of(AssetInvocationResult.class).where("invocationId", eq(id)), 16)
                .stream()
                .filter(r -> !r.issuer().peer().equals(identity.peerId()) && peers.test(r.issuer().peer()))
                .map(Space.Issued::entry)
                .findFirst().orElse(null);
    }

    /**
     * Writes a replica's result again under this connector's identity, so a requester that invoked
     * through this connector's card reads it (CC-L-08); remembered so a retry here never runs.
     */
    private void mirror(SpaceEvent<AssetInvocationResult> event) {
        Predicate<PeerId> peers = replicas;
        if (!running || peers == null || event.kind() != SpaceEvent.Kind.WRITTEN) {
            return;
        }
        PeerId from = event.issuer().peer();
        AssetInvocationResult result = event.entry();
        if (from.equals(identity.peerId()) || !peers.test(from) || !freshnessByAsset.containsKey(result.asset())
                || answered.containsKey(result.invocationId())) {
            return;
        }
        remember(result.invocationId(), result);
        Duration lease = result.ok()
                ? freshnessByAsset.getOrDefault(result.asset(), defaultFreshness) : Duration.ofSeconds(30);
        try {
            invocationSpace.write(result, Lease.of(lease));
            mirrored.incrementAndGet();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "mirroring result " + result.invocationId() + " failed", e);
        }
    }

    /**
     * Runs an invocation while renewing its claim every half {@link #invokeLease}, so an operation
     * that outlasts the lease (a large upload) keeps the invocation claimed and a retry under the
     * same id waits for this run instead of starting another (CC-L-03).
     */
    private AssetInvocationResult runHeld(TakenEntry<AssetInvocation> taken, AssetInvocation invocation,
                                          PeerId requester) {
        Duration lease = invokeLease;
        Duration every = lease.dividedBy(2).isZero() ? lease : lease.dividedBy(2);
        Thread renewer = Thread.ofVirtual().name("connector-renew-" + invocation.invocationId()).start(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(every);
                } catch (InterruptedException e) {
                    return;
                }
                try {
                    taken.renew(lease);
                    renewals.incrementAndGet();
                } catch (RuntimeException e) {
                    LOG.log(System.Logger.Level.WARNING, "renewing the claim on invocation "
                            + invocation.invocationId() + " failed", e);
                    return;
                }
            }
        });
        try {
            return run(invocation, requester);
        } finally {
            renewer.interrupt();
        }
    }

    private AssetInvocationResult run(AssetInvocation invocation, PeerId requester) {
        AssetProvider.InvokeResult outcome;
        try {
            invoked.incrementAndGet();
            outcome = provider.invoke(invocation.asset(), invocation.operation(), invocation.params(),
                    invocation.invocationId(), requester);
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "operation " + invocation.operation() + " on asset "
                    + invocation.asset() + " failed", e);
            outcome = AssetProvider.InvokeResult.failed("target-error", "target", String.valueOf(e.getMessage()));
        }
        return AssetInvocationResult.of(invocation, outcome);
    }

    /** Writes the result (leased for the asset's freshness window, or 30 s on an error) and completes the take. */
    private void finish(Space space, TakenEntry<AssetInvocation> taken, AssetInvocationResult result) {
        Duration lease = result.ok()
                ? freshnessByAsset.getOrDefault(result.asset(), defaultFreshness) : Duration.ofSeconds(30);
        try {
            space.complete(taken, result, Lease.of(lease));
        } catch (RuntimeException lapsed) {
            LOG.log(System.Logger.Level.WARNING, "completing invocation " + result.invocationId() + " failed", lapsed);
        }
    }

    private void remember(String id, AssetInvocationResult result) {
        Instant now = clock.instant();
        answered.values().removeIf(entry -> Duration.between(entry.at(), now).compareTo(invocationRetention) > 0);
        answered.put(id, new Answered(result, now));
    }

    private void completeQuietly(Space space, TakenEntry<?> taken) {
        try {
            space.complete(taken);
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
