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
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.connect.AssetInvocations.AssetInvocation;
import ai.badmonkey.agentspaces.connect.AssetInvocations.AssetInvocationResult;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code aspace:cap/asset-invoke} capability on a SimNetwork
 * (srad/agentspaces-js.md JS-C-02, JS-C-03, JS-C-07, JS-C-10, JS-F-06): an
 * agent discovers an asset through its card and invokes an operation in one
 * call; the connector takes the invocation under the claim lattice, runs it
 * once, writes the result, and completes the take; a retry under the same id
 * never runs the target again; an operation absent from the card's
 * {@code invoke-ops} is answered with an error naming the rule and never run;
 * under an authorizer only a requester holding
 * {@code aspace:connector-invoke:<asset>:<operation>} is served; and a reader
 * ignores fields it does not know.
 */
class AssetInvocationsClusterTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zInvoke");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zInvoke", PeerIdentity.generate().peerId(), groupId,
            Instant.EPOCH, Duration.ofDays(1), "invoke",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Wired(PeerNode node, PeerIdentity identity, GroupRuntime runtime,
                         DiscoveryService discovery, ReplicatedSpace data) {
    }

    /** An inventory with one allowlisted operation, {@code reserve}, recording every run by invocation id. */
    private static final class InventoryProvider implements AssetProvider {
        final List<String> reserved = new CopyOnWriteArrayList<>();
        /** Appended to the card id: replicas need distinct ids, since the ad cache keys cards by id. */
        final String idSuffix;

        InventoryProvider() {
            this("");
        }

        InventoryProvider(String idSuffix) {
            this.idSuffix = idSuffix;
        }

        @Override
        public List<AssetCard> describeAssets(GroupId group, PeerId issuer, Instant now) {
            return List.of(new AssetCard("aspace://" + group.value() + "/asset/inventory" + idSuffix, issuer, group, now,
                    Duration.ofMinutes(15), "inventory", "workers://shop/do/Inventory", "Stock by SKU",
                    "com.example.Stock#v1", "PT5M", Map.of("objects", "1"),
                    Map.of("mode", "read-only,invoke", "query", "aspace:cap/data-query",
                            "invoke", AssetInvocations.CAPABILITY, "invoke-ops", "reserve,release",
                            "consistency", "single-writer")));
        }

        @Override
        public QueryResult query(String asset, Map<String, String> parameters) {
            return QueryResult.of(List.of(Map.of("sku", "A-1", "stock", "7")));
        }

        @Override
        public InvokeResult invoke(String asset, String operation, Map<String, String> params, String invocationId) {
            if (!operation.equals("reserve")) {
                return InvokeResult.failed("bad-params", "provider", "release is not implemented");
            }
            reserved.add(invocationId);
            return InvokeResult.of(Map.of("reserved", params.getOrDefault("qty", "0"), "invocationId", invocationId));
        }
    }

    private ConnectorRuntime connector;

    @AfterEach
    void tearDown() {
        if (connector != null) {
            connector.close();
        }
        nodes.forEach(PeerNode::close);
    }

    private Wired newPeer(String address, long seed, String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, clock), codec, identity.peerId());
        ReplicatedSpace data = DataSpaces.builder(runtime, DataSpaces.DEFAULT_NAME, identity, address,
                        new BlockExchange(runtime, codec))
                .clock(clock).settleWindow(Duration.ZERO).build();
        nodes.add(node);
        return new Wired(node, identity, runtime, discovery, data);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
        }
    }

    private static void await(IntSupplier counter, int expected) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (counter.getAsInt() < expected && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(counter.getAsInt()).isEqualTo(expected);
    }

    private static List<AssetInvocationResult> resultsFor(Space space, String invocationId) {
        return space.readAllIssued(Template.of(AssetInvocationResult.class)
                .where("invocationId", eq(invocationId)), 16).stream().map(Space.Issued::entry).toList();
    }

    @Test
    @Timeout(60)
    void anInvocationIsTakenOnceAnsweredAndCompletedAndARetryNeverRunsTheTargetAgain() throws Exception {
        Wired connectorPeer = newPeer("conn", 1);
        Wired agent = newPeer("agent", 2, "conn");
        tickAll(4);
        InventoryProvider provider = new InventoryProvider();
        connector = new ConnectorRuntime(connectorPeer.data(), connectorPeer.discovery(), connectorPeer.identity(),
                "cell-connector", provider, groupId, clock, Duration.ofMinutes(1));
        connector.start();
        tickAll(2);

        AssetCard card = agent.discovery().find(AssetCard.class, c -> c.asset().equals("inventory")).getFirst();
        assertThat(AssetInvocations.invokeOps(card)).containsExactly("reserve", "release");
        AssetInvocationResult result = AssetInvocations.invoke(agent.data(), card, "reserve",
                Map.of("sku", "A-1", "qty", "2"), Duration.ofSeconds(10)).get(15, TimeUnit.SECONDS);
        assertThat(result.ok()).isTrue();
        assertThat(result.error()).isNull();
        assertThat(result.v()).isEqualTo(AssetInvocations.VERSION);
        assertThat(result.asset()).isEqualTo("inventory");
        assertThat(result.operation()).isEqualTo("reserve");
        assertThat(result.result()).containsEntry("reserved", "2").containsEntry("invocationId", result.invocationId());
        assertThat(provider.reserved).as("the target ran once, with the invocation id")
                .containsExactly(result.invocationId());
        assertThat(connector.invoked()).isEqualTo(1);
        // The take completed: the invocation is live nowhere.
        tickAll(2);
        assertThat(agent.data().read(Template.of(AssetInvocation.class))).isEmpty();
        assertThat(connectorPeer.data().read(Template.of(AssetInvocation.class))).isEmpty();
        assertThat(resultsFor(agent.data(), result.invocationId())).hasSize(1);

        // A forced retry under the same id (JS-C-04): answered, the target not run again, one result.
        AssetInvocationResult again = AssetInvocations.retry(agent.data(), card, new AssetInvocation(
                AssetInvocations.VERSION, "inventory", "reserve", Map.of("sku", "A-1", "qty", "2"),
                result.invocationId()), Duration.ofSeconds(10)).get(15, TimeUnit.SECONDS);
        assertThat(again.ok()).isTrue();
        await(connector::repeated, 1);
        assertThat(provider.reserved).hasSize(1);
        assertThat(connector.invoked()).isEqualTo(1);
        tickAll(2);
        assertThat(resultsFor(agent.data(), result.invocationId())).as("one result per invocationId").hasSize(1);
    }

    @Test
    @Timeout(60)
    void anOperationAbsentFromTheCardsInvokeOpsIsRefusedNamingTheRuleAndNeverRun() throws Exception {
        Wired connectorPeer = newPeer("conn", 1);
        Wired agent = newPeer("agent", 2, "conn");
        tickAll(4);
        InventoryProvider provider = new InventoryProvider();
        connector = new ConnectorRuntime(connectorPeer.data(), connectorPeer.discovery(), connectorPeer.identity(),
                "cell-connector", provider, groupId, clock, Duration.ofMinutes(1));
        connector.start();
        tickAll(2);

        AssetCard card = agent.discovery().find(AssetCard.class, c -> true).getFirst();
        AssetInvocationResult result = AssetInvocations.invoke(agent.data(), card, "drop",
                Map.of("table", "stock"), Duration.ofSeconds(10)).get(15, TimeUnit.SECONDS);
        assertThat(result.ok()).isFalse();
        assertThat(result.error()).isEqualTo("unknown-operation");
        assertThat(result.result()).containsEntry("rule", AssetInvocations.INVOKE_OPS);
        assertThat(result.result().get("detail")).contains("'drop' is not in the invoke-ops of asset 'inventory'");
        assertThat(provider.reserved).isEmpty();
        assertThat(connector.refused()).isEqualTo(1);
        assertThat(connector.invoked()).isZero();

        // A listed operation the provider refuses carries the provider's error, and counts as run.
        AssetInvocationResult release = AssetInvocations.invoke(agent.data(), card, "release",
                Map.of(), Duration.ofSeconds(10)).get(15, TimeUnit.SECONDS);
        assertThat(release.ok()).isFalse();
        assertThat(release.error()).isEqualTo("bad-params");
        assertThat(connector.invoked()).isEqualTo(1);
    }

    /** JS-C-02, JS-Q-SEC-11: under an authorizer the connector invokes only for a requesting peer permitted CONNECTOR_INVOKE in {@code <asset>:<operation>}; a member without the scope receives an error result and the connector never touches the binding. */
    @Test
    @Timeout(60)
    void underAnAuthorizerOnlyARequesterHoldingTheInvokeScopeIsServed() throws Exception {
        Wired connectorPeer = newPeer("conn", 1);
        Wired planner = newPeer("planner", 2, "conn");
        Wired stranger = newPeer("stranger", 3, "conn");
        tickAll(4);
        PeerId connectorId = connectorPeer.identity().peerId();
        PeerId plannerId = planner.identity().peerId();
        Authorizer authorizer = (peer, operation, scope) -> switch (operation) {
            case CONNECTOR_SERVE -> connectorId.equals(peer);
            case CONNECTOR_INVOKE -> plannerId.equals(peer) && "inventory:reserve".equals(scope);
            default -> false;
        };
        InventoryProvider provider = new InventoryProvider();
        connector = new ConnectorRuntime(connectorPeer.data(), connectorPeer.discovery(), connectorPeer.identity(),
                "cell-connector", provider, groupId, clock, Duration.ofMinutes(1)).authorizer(authorizer);
        connector.start();
        tickAll(2);

        AssetCard card = planner.discovery().find(AssetCard.class, c -> true).getFirst();
        AssetInvocationResult granted = AssetInvocations.invoke(planner.data(), card, "reserve",
                Map.of("qty", "1"), Duration.ofSeconds(10)).get(15, TimeUnit.SECONDS);
        assertThat(granted.ok()).isTrue();
        assertThat(provider.reserved).hasSize(1);

        AssetInvocationResult refused = AssetInvocations.invoke(stranger.data(), card, "reserve",
                Map.of("qty", "1"), Duration.ofSeconds(10)).get(15, TimeUnit.SECONDS);
        assertThat(refused.ok()).isFalse();
        assertThat(refused.error()).isEqualTo("refused-scope");
        assertThat(refused.result()).containsEntry("rule", "aspace:connector-invoke:inventory:reserve");
        assertThat(provider.reserved).as("the connector never touched the binding").hasSize(1);
        assertThat(connector.refused()).isEqualTo(1);

        // The planner's grant covers reserve alone: release is refused by scope, not by the card.
        AssetInvocationResult release = AssetInvocations.invoke(planner.data(), card, "release",
                Map.of(), Duration.ofSeconds(10)).get(15, TimeUnit.SECONDS);
        assertThat(release.error()).isEqualTo("refused-scope");
        assertThat(release.result()).containsEntry("rule", "aspace:connector-invoke:inventory:release");
    }

    /** JS-F-06: the entries carry {@code v} and a reader ignores fields it does not know. */
    /** A provider whose operation takes a while, so the claim must outlive the invoke lease. */
    private static final class SlowProvider implements AssetProvider {
        final InventoryProvider inner = new InventoryProvider();
        final long millis;

        SlowProvider(long millis) {
            this.millis = millis;
        }

        @Override
        public List<AssetCard> describeAssets(GroupId group, PeerId issuer, Instant now) {
            return inner.describeAssets(group, issuer, now);
        }

        @Override
        public QueryResult query(String asset, Map<String, String> parameters) {
            return inner.query(asset, parameters);
        }

        @Override
        public InvokeResult invoke(String asset, String operation, Map<String, String> params, String invocationId) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return inner.invoke(asset, operation, params, invocationId);
        }
    }

    @Test
    @Timeout(60)
    void aLongOperationKeepsItsClaimRenewedUntilItsResultIsWritten() throws Exception {
        Wired connectorPeer = newPeer("conn", 1);
        Wired agent = newPeer("agent", 2, "conn");
        tickAll(4);
        SlowProvider provider = new SlowProvider(900);
        connector = new ConnectorRuntime(connectorPeer.data(), connectorPeer.discovery(), connectorPeer.identity(),
                "cell-connector", provider, groupId, clock, Duration.ofMinutes(1))
                .invokeLease(Duration.ofMillis(200));
        connector.start();
        tickAll(2);
        AssetCard card = agent.discovery().find(AssetCard.class, c -> c.asset().equals("inventory")).getFirst();
        AssetInvocationResult result = AssetInvocations.invoke(agent.data(), card, "reserve",
                Map.of("sku", "A-1", "qty", "1"), Duration.ofSeconds(10)).get(15, TimeUnit.SECONDS);
        assertThat(result.ok()).isTrue();
        assertThat(provider.inner.reserved).hasSize(1);
        assertThat(connector.renewals()).as("renewed every half lease while the operation ran").isGreaterThanOrEqualTo(3);
    }

    @Test
    @Timeout(90)
    void replicasRunEachInvocationOnceAndMirrorEachOthersResults() throws Exception {
        Wired peerA = newPeer("conn-a", 1);
        Wired peerB = newPeer("conn-b", 2, "conn-a");
        Wired agent = newPeer("agent", 3, "conn-a");
        tickAll(6);
        InventoryProvider providerA = new InventoryProvider("@inv-a");
        InventoryProvider providerB = new InventoryProvider("@inv-b");
        PeerId a = peerA.identity().peerId();
        PeerId b = peerB.identity().peerId();
        connector = new ConnectorRuntime(peerA.data(), peerA.discovery(), peerA.identity(),
                "inv-a", providerA, groupId, clock, Duration.ofMinutes(1)).replicas(b::equals);
        ConnectorRuntime replica = new ConnectorRuntime(peerB.data(), peerB.discovery(), peerB.identity(),
                "inv-b", providerB, groupId, clock, Duration.ofMinutes(1)).replicas(a::equals);
        try {
            connector.start();
            replica.start();
            tickAll(4);
            AssetCard cardA = agent.discovery().find(AssetCard.class,
                    c -> c.asset().equals("inventory") && c.issuer().equals(a)).getFirst();
            AssetCard cardB = agent.discovery().find(AssetCard.class,
                    c -> c.asset().equals("inventory") && c.issuer().equals(b)).getFirst();
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < 6; i++) {
                AssetCard through = i % 2 == 0 ? cardA : cardB;
                AssetInvocationResult result = AssetInvocations.invoke(agent.data(), through, "reserve",
                        Map.of("sku", "A-" + i, "qty", "1"), Duration.ofSeconds(10)).get(15, TimeUnit.SECONDS);
                assertThat(result.ok()).as("answered through either card, whichever replica ran it").isTrue();
                ids.add(result.invocationId());
                tickAll(2);
            }
            // Two replicas can both take one invocation inside the space's settle window; the target's
            // own deduplication on the invocation id (the Worker's once) absorbs that race. Every
            // invocation ran, and the mirrors answered every requester.
            java.util.Set<String> ran = new java.util.HashSet<>(providerA.reserved);
            ran.addAll(providerB.reserved);
            assertThat(ran).containsExactlyInAnyOrderElementsOf(ids);
            int runs = providerA.reserved.size() + providerB.reserved.size();
            assertThat(connector.mirrored() + replica.mirrored()).isPositive();
            // A retry through the other card is answered from the mirrored result, never run again.
            for (String id : ids) {
                AssetInvocationResult again = AssetInvocations.retry(agent.data(), cardB, new AssetInvocation(
                        AssetInvocations.VERSION, "inventory", "reserve", Map.of("sku", "x", "qty", "1"), id),
                        Duration.ofSeconds(10)).get(15, TimeUnit.SECONDS);
                assertThat(again.ok()).isTrue();
            }
            assertThat(providerA.reserved.size() + providerB.reserved.size()).as("a retry never runs again")
                    .isEqualTo(runs);
        } finally {
            replica.close();
        }
    }

    @Test
    @Timeout(90)
    void aLateReplicaAnswersARetryFromTheResultItsPeerAlreadyWrote() throws Exception {
        Wired peerA = newPeer("conn-a", 1);
        Wired peerB = newPeer("conn-b", 2, "conn-a");
        Wired agent = newPeer("agent", 3, "conn-a");
        tickAll(6);
        InventoryProvider providerA = new InventoryProvider("@inv-a");
        InventoryProvider providerB = new InventoryProvider("@inv-b");
        PeerId a = peerA.identity().peerId();
        PeerId b = peerB.identity().peerId();
        connector = new ConnectorRuntime(peerA.data(), peerA.discovery(), peerA.identity(),
                "inv-a", providerA, groupId, clock, Duration.ofMinutes(1)).replicas(b::equals);
        connector.start();
        tickAll(2);
        AssetCard cardA = agent.discovery().find(AssetCard.class,
                c -> c.asset().equals("inventory") && c.issuer().equals(a)).getFirst();
        AssetInvocationResult first = AssetInvocations.invoke(agent.data(), cardA, "reserve",
                Map.of("sku", "A-1", "qty", "1"), Duration.ofSeconds(10)).get(15, TimeUnit.SECONDS);
        tickAll(4);
        // Replica B starts after the result exists: no mirror event reaches it, so its check before
        // running must find A's result.
        ConnectorRuntime replica = new ConnectorRuntime(peerB.data(), peerB.discovery(), peerB.identity(),
                "inv-b", providerB, groupId, clock, Duration.ofMinutes(1)).replicas(a::equals);
        connector.close();
        connector = null;
        try {
            replica.start();
            tickAll(4);
            AssetCard cardB = agent.discovery().find(AssetCard.class,
                    c -> c.asset().equals("inventory") && c.issuer().equals(b)).getFirst();
            AssetInvocationResult again = AssetInvocations.retry(agent.data(), cardB, new AssetInvocation(
                    AssetInvocations.VERSION, "inventory", "reserve", Map.of("sku", "A-1", "qty", "1"),
                    first.invocationId()), Duration.ofSeconds(10)).get(15, TimeUnit.SECONDS);
            assertThat(again.ok()).isTrue();
            assertThat(again.result()).containsEntry("invocationId", first.invocationId());
            assertThat(providerB.reserved).as("the replica never ran it").isEmpty();
            assertThat(replica.repeated()).isEqualTo(1);
        } finally {
            replica.close();
        }
    }

    @Test
    @Timeout(60)
    void withdrawingAnAssetsResultsSendsTheNextQueryToTheSource() throws Exception {
        Wired connectorPeer = newPeer("conn", 1);
        Wired agent = newPeer("agent", 2, "conn");
        tickAll(4);
        connector = new ConnectorRuntime(connectorPeer.data(), connectorPeer.discovery(), connectorPeer.identity(),
                "cell-connector", new InventoryProvider(), groupId, clock, Duration.ofMinutes(1));
        connector.start();
        tickAll(2);
        DataQueryClient client = new DataQueryClient(agent.data(), "analyst");
        assertThat(client.fetch("inventory", Map.of("sku", "A-1"), Duration.ofSeconds(10))).isPresent();
        assertThat(connector.executed()).isEqualTo(1);
        tickAll(2);
        assertThat(client.fetch("inventory", Map.of("sku", "A-1"), Duration.ofSeconds(10)).orElseThrow().fromCache())
                .as("the live result serves the window").isTrue();
        assertThat(connector.withdrawResults("inventory")).isEqualTo(1);
        assertThat(connector.withdrawResults("inventory")).as("nothing left to withdraw").isZero();
        tickAll(4);
        DataQueryClient.Fetched fresh = client.fetch("inventory", Map.of("sku", "A-1"), Duration.ofSeconds(10)).orElseThrow();
        assertThat(fresh.fromCache()).isFalse();
        assertThat(connector.executed()).as("the next query reached the source").isEqualTo(2);
        assertThat(connector.withdrawn()).isEqualTo(1);
    }

    @Test
    void artifactRowsRoundTripAndRejectOrdinaryRows() {
        ArtifactRows.Artifact artifact = new ArtifactRows.Artifact(
                "bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy", 3, 2_500_000L,
                "application/x-ndjson", "inventory/exports/nightly", "2026-11-02T06:00:00Z");
        assertThat(ArtifactRows.parse(artifact.toRow())).contains(artifact);
        assertThat(ArtifactRows.isArtifact(Map.of("root", "bafy", "pieces", "1"))).isTrue();
        assertThat(ArtifactRows.isArtifact(Map.of("sku", "A-1", "stock", "7"))).isFalse();
        assertThat(ArtifactRows.isArtifact(Map.of("root", "zQm", "pieces", "1"))).as("a block CID is not a root").isFalse();
        assertThat(ArtifactRows.isArtifact(Map.of("root", "bafy", "pieces", "many"))).isFalse();
        assertThat(ArtifactRows.parse(Map.of("root", "bafy", "pieces", "2")).orElseThrow().size()).isEqualTo(-1);
    }

    @Test
    void readersIgnoreUnknownFields() {
        byte[] future = codec.toBytes(Map.of("v", 2, "asset", "inventory", "operation", "reserve",
                "params", Map.of("qty", "1"), "invocationId", "id-1", "idempotency", "once",
                "schema", Map.of("qty", "int")));
        AssetInvocation invocation = codec.fromBytes(future, AssetInvocation.class);
        assertThat(invocation).isEqualTo(new AssetInvocation(2, "inventory", "reserve", Map.of("qty", "1"), "id-1"));
        byte[] futureResult = codec.toBytes(Map.of("v", 1, "invocationId", "id-1", "asset", "inventory",
                "operation", "reserve", "ok", true, "result", Map.of("reserved", "1"), "cost", "3"));
        AssetInvocationResult result = codec.fromBytes(futureResult, AssetInvocationResult.class);
        assertThat(result.ok()).isTrue();
        assertThat(result.error()).isNull();
        assertThat(result.result()).containsEntry("reserved", "1");
    }
}
