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
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.Subscription;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.connect.DataQueryEntries.AssetChange;
import ai.badmonkey.agentspaces.connect.DataQueryEntries.DataResult;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.space.replicated.SpaceWire;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The connector SDK on a SimNetwork: leased AssetCards, the data-query
 * protocol through the space, and the pull-once economics: identical queries
 * cost the source one execution per freshness window, fleet-wide.
 */
class ConnectorClusterTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zConnect");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zConnect", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "connect",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Wired(PeerNode node, PeerIdentity identity, GroupRuntime runtime,
                         DiscoveryService discovery, ReplicatedSpace data,
                         BlockExchange blocks) {
    }

    private ConnectorRuntime connector;

    @AfterEach
    void tearDown() {
        if (connector != null) {
            connector.close();
        }
        nodes.forEach(PeerNode::close);
    }

    private Wired newPeer(String address, long seed, String... seedAddresses)
            throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        DiscoveryService discovery = new DiscoveryService(
                runtime, new AdCache(codec, clock), codec, identity.peerId());
        // Spec §6.1a: the connector's data space carries bulk results
        // content-addressed, so every replica attaches its block exchange.
        BlockExchange blocks = new BlockExchange(runtime, codec);
        ReplicatedSpace data = DataSpaces.builder(runtime, DataSpaces.DEFAULT_NAME, identity,
                        address, blocks)
                .clock(clock).settleWindow(Duration.ZERO).build();
        nodes.add(node);
        return new Wired(node, identity, runtime, discovery, data, blocks);
    }

    /** A group member running no space at all: it observes the raw space stream. */
    private PeerNode newObserver(String address, long seed, String... seedAddresses)
            throws IOException {
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(clock)
                .randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        nodes.add(node);
        return node;
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
        }
    }

    private TableAssetProvider ordersProvider(Duration freshness) {
        return new TableAssetProvider("orders", "postgres://ops/public.orders",
                "Customer order history with line items and settlement status",
                "com.example.OrderRow#v1", freshness,
                List.of(Map.of("id", "1", "region", "eu", "amount", "120.00"),
                        Map.of("id", "2", "region", "us", "amount", "80.00"),
                        Map.of("id", "3", "region", "eu", "amount", "45.50")));
    }

    @Test
    @Timeout(60)
    void aForgedResultCannotPoisonThePullOnceCache() throws Exception {
        Wired connectorPeer = newPeer("conn", 1);
        Wired agent = newPeer("agent", 2, "conn");
        Wired evil = newPeer("evil", 3, "conn");
        tickAll(4);
        connector = new ConnectorRuntime(connectorPeer.data(), connectorPeer.discovery(),
                connectorPeer.identity(), "pg-connector", ordersProvider(Duration.ofMinutes(5)),
                groupId, clock, Duration.ofMinutes(1));
        connector.start();
        tickAll(2);

        // ASF-009: an admitted peer pre-writes a forged result under the exact
        // canonical hash honest askers compute, posing as the connector.
        String hash = DataQueryEntries.canonicalHash("orders", Map.of("region", "eu"));
        evil.data().write(new DataQueryEntries.DataResult(hash, "orders",
                List.of(Map.of("id", "666", "region", "eu", "amount", "0.00")),
                null, "pg-connector"), Lease.of(Duration.ofMinutes(5)));
        tickAll(4);

        // A client that names its connector ignores the forgery, and the
        // connector still executes against the source: the forged entry
        // suppresses neither the ask nor the pull.
        DataQueryClient client = new DataQueryClient(agent.data(), "analyst",
                java.util.Set.of(connectorPeer.identity().peerId()));
        Optional<DataQueryClient.Fetched> fetched = client.fetch("orders",
                Map.of("region", "eu"), Duration.ofSeconds(10));
        assertThat(fetched).isPresent();
        assertThat(fetched.get().fromCache()).isFalse();
        assertThat(fetched.get().result().rows())
                .as("the answer is the source's rows, not the forgery")
                .noneMatch(row -> "666".equals(row.get("id")))
                .isNotEmpty();
        assertThat(connector.executed()).isEqualTo(1);
    }

    /** TODO-EFG §4 / TODO item 6 (ASF-009): a DataQueryClient built over an Authorizer trusts a result only when its space-authenticated issuer's peer is permitted CONNECTOR_SERVE for the result's asset. */
    @Test
    @Timeout(60)
    void resultsFromAnUnauthorizedConnectorAreIgnored() throws Exception {
        Wired connectorPeer = newPeer("conn", 1);
        Wired agent = newPeer("agent", 2, "conn");
        Wired evil = newPeer("evil", 3, "conn");
        tickAll(4);
        connector = new ConnectorRuntime(connectorPeer.data(), connectorPeer.discovery(),
                connectorPeer.identity(), "pg-connector", ordersProvider(Duration.ofMinutes(5)),
                groupId, clock, Duration.ofMinutes(1));
        connector.start();
        tickAll(2);

        // An admitted peer pre-writes a forged result under the canonical hash.
        String hash = DataQueryEntries.canonicalHash("orders", Map.of("region", "eu"));
        evil.data().write(new DataQueryEntries.DataResult(hash, "orders",
                List.of(Map.of("id", "666", "region", "eu", "amount", "0.00")),
                null, "pg-connector"), Lease.of(Duration.ofMinutes(5)));
        tickAll(4);

        // The authorizer permits exactly the connector peer for this asset.
        PeerId connectorId = connectorPeer.identity().peerId();
        Authorizer authorizer = (peer, operation, scope) ->
                operation == Authorizer.Operation.CONNECTOR_SERVE
                        && "orders".equals(scope) && connectorId.equals(peer);
        DataQueryClient client = new DataQueryClient(agent.data(), "analyst", authorizer);
        Optional<DataQueryClient.Fetched> fetched = client.fetch("orders",
                Map.of("region", "eu"), Duration.ofSeconds(10));
        assertThat(fetched).isPresent();
        assertThat(fetched.get().fromCache()).as("the forgery is not a cache hit").isFalse();
        assertThat(fetched.get().result().rows())
                .noneMatch(row -> "666".equals(row.get("id")))
                .isNotEmpty();
        assertThat(connector.executed()).isEqualTo(1);
        tickAll(4);

        // A client whose authorizer permits nobody consumes nothing — not even
        // the genuine answer now sitting in its replica.
        DataQueryClient distrustful = new DataQueryClient(agent.data(), "auditor",
                (peer, operation, scope) -> false);
        assertThat(distrustful.fetch("orders", Map.of("region", "eu"), Duration.ofSeconds(2)))
                .as("no permitted connector, no answer").isEmpty();
    }

    /** TODO-EFG §4 / TODO item 6: a ConnectorRuntime given an Authorizer advertises only the assets its own peer is permitted CONNECTOR_SERVE for, and picks them up once permitted. */
    @Test
    @Timeout(60)
    void aConnectorRefusesToAdvertiseAssetsItMayNotServe() throws Exception {
        Wired connectorPeer = newPeer("conn", 1);
        Wired agent = newPeer("agent", 2, "conn");
        tickAll(4);

        java.util.Set<String> permittedAssets = java.util.concurrent.ConcurrentHashMap.newKeySet();
        PeerId connectorId = connectorPeer.identity().peerId();
        Authorizer authorizer = (peer, operation, scope) ->
                operation == Authorizer.Operation.CONNECTOR_SERVE
                        && connectorId.equals(peer) && permittedAssets.contains(scope);
        connector = new ConnectorRuntime(connectorPeer.data(), connectorPeer.discovery(),
                connectorPeer.identity(), "pg-connector", ordersProvider(Duration.ofMinutes(5)),
                groupId, clock, Duration.ofMinutes(1)).authorizer(authorizer);
        connector.start();
        tickAll(4);

        assertThat(agent.discovery().find(AssetCard.class, card -> true))
                .as("an asset the connector may not serve is never advertised").isEmpty();
        assertThat(connectorPeer.discovery().find(AssetCard.class, card -> true)).isEmpty();

        // The grant arrives (a fresh token, a new membership grant): the next
        // card refresh advertises the asset.
        permittedAssets.add("orders");
        connector.refreshCards();
        tickAll(4);
        assertThat(agent.discovery().find(AssetCard.class, card -> true))
                .extracting(AssetCard::asset).containsExactly("orders");
    }

    @Test
    @Timeout(60)
    void assetsAreDiscoverableAndQueriesAnswerThroughTheSpace() throws Exception {
        Wired connectorPeer = newPeer("conn", 1);
        Wired agent = newPeer("agent", 2, "conn");
        tickAll(4);

        TableAssetProvider provider = ordersProvider(Duration.ofMinutes(5));
        connector = new ConnectorRuntime(connectorPeer.data(), connectorPeer.discovery(),
                connectorPeer.identity(), "pg-connector", provider, groupId, clock,
                Duration.ofMinutes(1));
        connector.start();
        tickAll(2);

        // The agent discovers the holding through the ad-cache.
        List<AssetCard> cards = agent.discovery().find(AssetCard.class,
                card -> card.description().contains("order history"));
        assertThat(cards).hasSize(1);
        assertThat(cards.get(0).access()).containsEntry("query", "aspace:cap/data-query");

        // And queries it through the space.
        DataQueryClient client = new DataQueryClient(agent.data(), "analyst");
        Optional<DataQueryClient.Fetched> fetched = client.fetch("orders",
                Map.of("region", "eu"), Duration.ofSeconds(10));
        assertThat(fetched).isPresent();
        assertThat(fetched.get().fromCache()).isFalse();
        assertThat(fetched.get().result().rows()).hasSize(2);
        assertThat(fetched.get().result().servedBy()).isEqualTo("pg-connector");
        assertThat(provider.executions()).isEqualTo(1);
    }

    @Test
    @Timeout(60)
    void identicalQueriesAcrossTheFleetCostOneExecution() throws Exception {
        Wired connectorPeer = newPeer("conn", 1);
        Wired agentA = newPeer("a", 2, "conn");
        Wired agentB = newPeer("b", 3, "conn");
        tickAll(4);

        TableAssetProvider provider = ordersProvider(Duration.ofMinutes(5));
        connector = new ConnectorRuntime(connectorPeer.data(), connectorPeer.discovery(),
                connectorPeer.identity(), "pg-connector", provider, groupId, clock,
                Duration.ofMinutes(1));
        connector.start();

        DataQueryClient clientA = new DataQueryClient(agentA.data(), "analyst-a");
        DataQueryClient clientB = new DataQueryClient(agentB.data(), "analyst-b");

        assertThat(clientA.fetch("orders", Map.of("region", "eu"),
                Duration.ofSeconds(10))).isPresent();
        tickAll(4); // the result replicates to every replica

        // B asks the same question, parameter order shuffled: pure cache hit.
        Optional<DataQueryClient.Fetched> second = clientB.fetch("orders",
                Map.of("region", "eu"), Duration.ofSeconds(10));
        assertThat(second).isPresent();
        assertThat(second.get().fromCache()).isTrue();
        assertThat(provider.executions()).isEqualTo(1);
        assertThat(clientB.cacheHits()).isEqualTo(1);

        // A different question executes again.
        assertThat(clientA.fetch("orders", Map.of("region", "us"),
                Duration.ofSeconds(10))).isPresent();
        assertThat(provider.executions()).isEqualTo(2);
    }

    @Test
    @Timeout(60)
    void theFreshnessWindowIsTheStalenessBudget() throws Exception {
        Wired connectorPeer = newPeer("conn", 1);
        Wired agent = newPeer("agent", 2, "conn");
        tickAll(4);

        TableAssetProvider provider = ordersProvider(Duration.ofSeconds(2));
        connector = new ConnectorRuntime(connectorPeer.data(), connectorPeer.discovery(),
                connectorPeer.identity(), "pg-connector", provider, groupId, clock,
                Duration.ofMinutes(1));
        connector.start();

        DataQueryClient client = new DataQueryClient(agent.data(), "analyst");
        assertThat(client.fetch("orders", Map.of("region", "eu"),
                Duration.ofSeconds(10))).isPresent();
        assertThat(client.fetch("orders", Map.of("region", "eu"),
                Duration.ofSeconds(10)).orElseThrow().fromCache()).isTrue();
        assertThat(provider.executions()).isEqualTo(1);

        // The window lapses; the same question honestly costs a new execution.
        clock.advance(Duration.ofSeconds(3));
        assertThat(client.fetch("orders", Map.of("region", "eu"),
                Duration.ofSeconds(10))).isPresent();
        assertThat(provider.executions()).isEqualTo(2);
    }

    @Test
    @Timeout(60)
    void unknownAssetsFailWithAnErrorResult() throws Exception {
        Wired connectorPeer = newPeer("conn", 1);
        Wired agent = newPeer("agent", 2, "conn");
        tickAll(4);

        connector = new ConnectorRuntime(connectorPeer.data(), connectorPeer.discovery(),
                connectorPeer.identity(), "pg-connector",
                ordersProvider(Duration.ofMinutes(5)), groupId, clock,
                Duration.ofMinutes(1));
        connector.start();

        DataQueryClient client = new DataQueryClient(agent.data(), "analyst");
        Optional<DataQueryClient.Fetched> fetched = client.fetch("no-such-table",
                Map.of(), Duration.ofSeconds(10));
        assertThat(fetched).isPresent();
        assertThat(fetched.get().result().error()).contains("no-such-table");
        assertThat(fetched.get().result().rows()).isEmpty();
    }

    /** SPEC §6.1a/§6.2: the AssetCard carries the 15-minute default TTL and the declared freshness, refreshCards re-issues it past the original TTL, and an unrefreshed card ages out. */
    @Test
    @Timeout(60)
    void refreshCardsKeepsAssetsAlive() throws Exception {
        Wired connectorPeer = newPeer("conn", 1);
        Wired agent = newPeer("agent", 2, "conn");
        tickAll(4);

        connector = new ConnectorRuntime(connectorPeer.data(), connectorPeer.discovery(),
                connectorPeer.identity(), "pg-connector", ordersProvider(Duration.ofMinutes(5)),
                groupId, clock, Duration.ofMinutes(1));
        connector.start();
        tickAll(2);

        List<AssetCard> cards = agent.discovery().find(AssetCard.class, card -> true);
        assertThat(cards).hasSize(1);
        assertThat(cards.get(0).ttl()).isEqualTo(Duration.ofMinutes(15));
        assertThat(cards.get(0).freshness()).isEqualTo("PT5M");
        java.time.Instant firstIssued = cards.get(0).issued();

        clock.advance(Duration.ofMinutes(10));
        connector.refreshCards();
        java.time.Instant refreshedAt = clock.instant();
        clock.advance(Duration.ofMinutes(10)); // 20 minutes on: the original card would be gone

        assertThat(agent.discovery().find(AssetCard.class, card -> true))
                .singleElement().extracting(AssetCard::issued)
                .isEqualTo(refreshedAt).isNotEqualTo(firstIssued);

        clock.advance(Duration.ofMinutes(16)); // no further refresh: the lease does the rest
        assertThat(agent.discovery().find(AssetCard.class, card -> true)).isEmpty();
    }

    // ------------------------------------------------------------ §6.1a materializing style

    /** The reference table provider plus a change feed the test drives by hand. */
    private static final class FeedProvider implements MaterializingAssetProvider {
        private final TableAssetProvider table;
        private final AtomicReference<Consumer<Change>> sink = new AtomicReference<>();

        FeedProvider(TableAssetProvider table) {
            this.table = table;
        }

        @Override
        public List<AssetCard> describeAssets(GroupId group, PeerId issuer, Instant now) {
            return table.describeAssets(group, issuer, now);
        }

        @Override
        public QueryResult query(String asset, Map<String, String> parameters) {
            return table.query(asset, parameters);
        }

        @Override
        public AutoCloseable materialize(Consumer<Change> sink) {
            this.sink.set(sink);
            return () -> this.sink.set(null);
        }

        void emit(String key, Map<String, String> row) {
            Consumer<Change> current = sink.get();
            assertThat(current).as("the runtime started the feed").isNotNull();
            current.accept(new Change("orders", key, row));
        }
    }

    /** SPEC §6.1a: a materializing provider pushes each source change as a typed entry leased for the asset's freshness window; a subscriber on another peer reacts to every change, and once the window lapses the entries are gone from every replica. */
    @Test
    @Timeout(60)
    void aMaterializingProviderPushesLeasedEntriesThatSubscribersReact() throws Exception {
        Wired connectorPeer = newPeer("conn", 1);
        Wired agent = newPeer("agent", 2, "conn");
        Wired other = newPeer("other", 3, "conn");
        tickAll(4);

        FeedProvider feed = new FeedProvider(ordersProvider(Duration.ofSeconds(30)));
        connector = new ConnectorRuntime(connectorPeer.data(), connectorPeer.discovery(),
                connectorPeer.identity(), "pg-connector", feed, groupId, clock,
                Duration.ofMinutes(1));
        List<SpaceEvent<AssetChange>> seen = new CopyOnWriteArrayList<>();
        try (Subscription sub = agent.data().notify(
                Template.of(AssetChange.class).where("asset", eq("orders")),
                seen::add, Lease.of(Duration.ofMinutes(5)))) {
            connector.start();

            feed.emit("1", Map.of("id", "1", "region", "eu", "amount", "120.00"));
            feed.emit("2", Map.of("id", "2", "region", "us", "amount", "80.00"));
            feed.emit("1", Map.of("id", "1", "region", "eu", "amount", "125.00"));
            tickAll(2);

            assertThat(connector.materialized()).isEqualTo(3);
            assertThat(seen).hasSize(3)
                    .allMatch(event -> event.kind() == SpaceEvent.Kind.WRITTEN)
                    .allMatch(event -> event.issuer().peer()
                            .equals(connectorPeer.identity().peerId()));
            assertThat(seen).extracting(event -> event.entry().key())
                    .containsExactly("1", "2", "1");
            assertThat(seen.get(2).entry().row()).containsEntry("amount", "125.00");
            assertThat(seen).extracting(event -> event.entry().servedBy())
                    .containsOnly("pg-connector");
        }
        // Every replica holds the three changes while the window is open.
        for (Wired peer : List.of(connectorPeer, agent, other)) {
            assertThat(peer.data().readAll(Template.of(AssetChange.class), 10))
                    .as("replica " + peer.node().peerId().display()).hasSize(3);
        }

        // The lease is the retention policy: the freshness window lapses and
        // the changes age out of every replica with no sweep or message.
        clock.advance(Duration.ofSeconds(31));
        for (Wired peer : List.of(connectorPeer, agent, other)) {
            assertThat(peer.data().readAll(Template.of(AssetChange.class), 10))
                    .as("replica " + peer.node().peerId().display()).isEmpty();
        }
    }

    // ------------------------------------------------------------ §6.1a content-addressed bulk

    /** SPEC §6.1a/§7.1: a bulk DataResult (over the 64 KiB inline limit) travels content-addressed: the record carries a payloadRef and no inline payload, the block lives under its CID, and the client reads every row through the block exchange. */
    @Test
    @Timeout(60)
    void bulkResultsTravelContentAddressed() throws Exception {
        Wired connectorPeer = newPeer("conn", 1);
        Wired agent = newPeer("agent", 2, "conn");
        PeerNode observer = newObserver("obs", 3, "conn");
        tickAll(4);
        // The observer decodes every record the data space gossips.
        List<EntryRecord> records = new CopyOnWriteArrayList<>();
        observer.group(groupId).orElseThrow().gossip().onStream(
                "space:" + DataSpaces.DEFAULT_NAME, (from, itemId, payload) -> {
                    SpaceWire.Delta delta = codec.fromBytes(payload, SpaceWire.Delta.class);
                    if (delta != null && delta.state() != null) {
                        records.add(delta.state().record());
                    }
                });

        List<Map<String, String>> rows = new ArrayList<>();
        String padding = "x".repeat(80);
        for (int i = 0; i < 1_000; i++) {
            rows.add(Map.of("id", String.valueOf(i), "region", "eu", "payload", padding));
        }
        TableAssetProvider provider = new TableAssetProvider("orders",
                "postgres://ops/public.orders", "Bulk order export", "com.example.OrderRow#v1",
                Duration.ofMinutes(5), rows);
        String hash = DataQueryEntries.canonicalHash("orders", Map.of("region", "eu"));
        assertThat(codec.toBytes(new DataResult(hash, "orders", rows, null, "pg-connector")).length)
                .as("the rows serialize past the inline limit")
                .isGreaterThan(EntryRecord.INLINE_PAYLOAD_LIMIT);

        connector = new ConnectorRuntime(connectorPeer.data(), connectorPeer.discovery(),
                connectorPeer.identity(), "pg-connector", provider, groupId, clock,
                Duration.ofMinutes(1));
        connector.start();

        DataQueryClient client = new DataQueryClient(agent.data(), "analyst",
                java.util.Set.of(connectorPeer.identity().peerId()));
        Optional<DataQueryClient.Fetched> fetched = client.fetch("orders",
                Map.of("region", "eu"), Duration.ofSeconds(10));

        assertThat(fetched).isPresent();
        assertThat(fetched.get().result().rows()).hasSize(1_000).containsExactlyElementsOf(rows);
        assertThat(fetched.get().result().servedBy()).isEqualTo("pg-connector");

        // The result record on the wire is a reference, not the payload.
        List<EntryRecord> resultRecords = records.stream()
                .filter(record -> record.type().startsWith(DataResult.class.getName())).toList();
        assertThat(resultRecords).isNotEmpty()
                .allSatisfy(record -> {
                    assertThat(record.payloadRef()).as("payloadRef").isNotNull();
                    assertThat(record.payload()).as("inline payload").isNull();
                });
        String cid = resultRecords.get(0).payloadRef();
        assertThat(connectorPeer.blocks().local(cid)).as("the writer holds the block").isPresent();
        assertThat(agent.blocks().local(cid))
                .as("the client's read pulled the block through the exchange")
                .hasValueSatisfying(bytes -> {
                    assertThat(BlockExchange.cidOf(bytes)).isEqualTo(cid);
                    assertThat(codec.fromBytes(bytes, DataResult.class).rows()).hasSize(1_000);
                });
        // Small entries (the query itself) still travel inline.
        assertThat(records.stream().filter(record ->
                record.type().startsWith(DataQueryEntries.DataQuery.class.getName())))
                .isNotEmpty().allMatch(record -> record.payloadRef() == null);
    }

    @Test
    void canonicalHashesIgnoreParameterOrder() {
        java.util.LinkedHashMap<String, String> forward = new java.util.LinkedHashMap<>();
        forward.put("a", "1");
        forward.put("b", "2");
        java.util.LinkedHashMap<String, String> reverse = new java.util.LinkedHashMap<>();
        reverse.put("b", "2");
        reverse.put("a", "1");
        assertThat(DataQueryEntries.canonicalHash("orders", forward))
                .isEqualTo(DataQueryEntries.canonicalHash("orders", reverse));
        assertThat(DataQueryEntries.canonicalHash("orders", forward))
                .isNotEqualTo(DataQueryEntries.canonicalHash("other", forward));
    }
}
