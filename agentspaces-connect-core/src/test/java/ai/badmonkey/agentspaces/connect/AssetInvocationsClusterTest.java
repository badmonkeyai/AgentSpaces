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
import ai.badmonkey.agentspaces.api.error.LeaseExpiredException;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
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
import ai.badmonkey.agentspaces.measure.Measure;
import ai.badmonkey.agentspaces.measure.Wait;
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

    /**
     * Waits for a counter, measured (srad/feature-measurements.md §3.5). fm: (a) ceiling 30 s (policy; the R2 fix
     * had raised it from 10 s, owner rule 2); the condition is the count itself, which is what the test waits on.
     */
    private static void await(String metric, IntSupplier counter, int expected) {
        Measure.await(metric, Duration.ofSeconds(30), () -> counter.getAsInt() >= expected,
                Wait.with().poll(Duration.ofMillis(10)).diagnostics(() -> "count " + counter.getAsInt() + " of " + expected));
        assertThat(counter.getAsInt()).isEqualTo(expected);
    }

    /**
     * An invocation's answer, waited for as a measured wait on the future rather than a bare {@code get} with a
     * timeout: the condition is the answer itself. fm: (a) ceiling 45 s, unchanged; the answer typically comes in
     * tens of milliseconds.
     */
    private static <T> T answered(String metric, java.util.concurrent.CompletableFuture<T> answer) throws Exception {
        Measure.await(metric, Duration.ofSeconds(45), answer::isDone, Wait.with().poll(Duration.ofMillis(10)));
        return answer.get();
    }

    private static List<AssetInvocationResult> resultsFor(Space space, String invocationId) {
        return space.readAllIssued(Template.of(AssetInvocationResult.class)
                .where("invocationId", eq(invocationId)), 16).stream().map(Space.Issued::entry).toList();
    }

    @Test
    @Timeout(300)
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
        AssetInvocationResult result = answered("connect.invoke.first", AssetInvocations.invoke(agent.data(), card, "reserve",
                Map.of("sku", "A-1", "qty", "2"), Duration.ofSeconds(30)));
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
        AssetInvocationResult again = answered("connect.invoke.retry", AssetInvocations.retry(agent.data(), card, new AssetInvocation(
                AssetInvocations.VERSION, "inventory", "reserve", Map.of("sku", "A-1", "qty", "2"),
                result.invocationId()), Duration.ofSeconds(30)));
        assertThat(again.ok()).isTrue();
        await("connect.invoke.repeat-counted", connector::repeated, 1);
        assertThat(provider.reserved).hasSize(1);
        assertThat(connector.invoked()).isEqualTo(1);
        tickAll(2);
        assertThat(resultsFor(agent.data(), result.invocationId())).as("one result per invocationId").hasSize(1);
    }

    @Test
    @Timeout(300)
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
        AssetInvocationResult result = answered("connect.invoke.refused", AssetInvocations.invoke(agent.data(), card, "drop",
                Map.of("table", "stock"), Duration.ofSeconds(30)));
        assertThat(result.ok()).isFalse();
        assertThat(result.error()).isEqualTo("unknown-operation");
        assertThat(result.result()).containsEntry("rule", AssetInvocations.INVOKE_OPS);
        assertThat(result.result().get("detail")).contains("'drop' is not in the invoke-ops of asset 'inventory'");
        assertThat(provider.reserved).isEmpty();
        assertThat(connector.refused()).isEqualTo(1);
        assertThat(connector.invoked()).isZero();

        // A listed operation the provider refuses carries the provider's error, and counts as run.
        AssetInvocationResult release = answered("connect.invoke.release", AssetInvocations.invoke(agent.data(), card, "release",
                Map.of(), Duration.ofSeconds(30)));
        assertThat(release.ok()).isFalse();
        assertThat(release.error()).isEqualTo("bad-params");
        assertThat(connector.invoked()).isEqualTo(1);
    }

    /** JS-C-02, JS-Q-SEC-11: under an authorizer the connector invokes only for a requesting peer permitted CONNECTOR_INVOKE in {@code <asset>:<operation>}; a member without the scope receives an error result and the connector never touches the binding. */
    @Test
    @Timeout(300)
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
        AssetInvocationResult granted = answered("connect.invoke.scoped", AssetInvocations.invoke(planner.data(), card, "reserve",
                Map.of("qty", "1"), Duration.ofSeconds(30)));
        assertThat(granted.ok()).isTrue();
        assertThat(provider.reserved).hasSize(1);

        AssetInvocationResult refused = answered("connect.invoke.unscoped", AssetInvocations.invoke(stranger.data(), card, "reserve",
                Map.of("qty", "1"), Duration.ofSeconds(30)));
        assertThat(refused.ok()).isFalse();
        assertThat(refused.error()).isEqualTo("refused-scope");
        assertThat(refused.result()).containsEntry("rule", "aspace:connector-invoke:inventory:reserve");
        assertThat(provider.reserved).as("the connector never touched the binding").hasSize(1);
        assertThat(connector.refused()).isEqualTo(1);

        // The planner's grant covers reserve alone: release is refused by scope, not by the card.
        AssetInvocationResult release = answered("connect.invoke.scoped-release", AssetInvocations.invoke(planner.data(), card, "release",
                Map.of(), Duration.ofSeconds(30)));
        assertThat(release.error()).isEqualTo("refused-scope");
        assertThat(release.result()).containsEntry("rule", "aspace:connector-invoke:inventory:release");
    }

    /**
     * The requester is the signer of the invocation the connector took (PDT 2 follow-ups, 2026-11-09, adversary of
     * F1: a store's {@code pin} invoke charges the invocation's signer). A writer that copies another member's
     * invocation id under its own parameters runs them under its own name, so it borrows no grant.
     */
    @Test
    @Timeout(300)
    void anInvocationRunsUnderItsOwnSignerWhenAnotherWriterReusesItsId() throws Exception {
        Wired connectorPeer = newPeer("conn", 1);
        Wired planner = newPeer("planner", 2, "conn");
        Wired stranger = newPeer("stranger", 3, "conn");
        tickAll(4);
        PeerId connectorId = connectorPeer.identity().peerId();
        PeerId plannerId = planner.identity().peerId();
        PeerId strangerId = stranger.identity().peerId();
        Authorizer authorizer = (peer, operation, scope) -> switch (operation) {
            case CONNECTOR_SERVE -> connectorId.equals(peer);
            case CONNECTOR_INVOKE -> plannerId.equals(peer);
            default -> false;
        };
        InventoryProvider inventory = new InventoryProvider();
        List<String> runs = new CopyOnWriteArrayList<>();
        AssetProvider provider = new AssetProvider() {
            @Override
            public List<AssetCard> describeAssets(GroupId group, PeerId issuer, Instant now) {
                return inventory.describeAssets(group, issuer, now);
            }

            @Override
            public QueryResult query(String asset, Map<String, String> parameters) {
                return inventory.query(asset, parameters);
            }

            @Override
            public InvokeResult invoke(String asset, String operation, Map<String, String> params, String invocationId,
                                       PeerId requester) {
                runs.add(params.get("qty") + "@" + (requester == null ? "unknown" : requester.value()));
                return InvokeResult.of(Map.of("reserved", params.get("qty")));
            }
        };
        // planners' invocations and the stranger's copies of their ids, written in both orders, all waiting when
        // the connector starts
        int pairs = 6;
        for (int i = 0; i < pairs; i++) {
            AssetInvocation mine = AssetInvocation.of("inventory", "reserve", Map.of("qty", "1"));
            AssetInvocation copy = new AssetInvocation(mine.v(), "inventory", "reserve", Map.of("qty", "999"),
                    mine.invocationId());
            Wired first = i % 2 == 0 ? planner : stranger;
            Wired second = i % 2 == 0 ? stranger : planner;
            first.data().write(first == planner ? mine : copy, ai.badmonkey.agentspaces.api.space.Lease.of(Duration.ofMinutes(1)));
            tickAll(2);
            second.data().write(second == planner ? mine : copy, ai.badmonkey.agentspaces.api.space.Lease.of(Duration.ofMinutes(1)));
            tickAll(2);
        }
        assertThat(connectorPeer.data().readAllIssued(Template.of(AssetInvocation.class), 32)).hasSize(2 * pairs);
        connector = new ConnectorRuntime(connectorPeer.data(), connectorPeer.discovery(), connectorPeer.identity(),
                "cell-connector", provider, groupId, clock, Duration.ofMinutes(1)).authorizer(authorizer);
        connector.start();
        await("connect.invoke.signer-runs", () -> (int) (connector.invoked() + connector.refused() + connector.repeated()), 2 * pairs);

        assertThat(runs).as("the stranger's parameters never run under the planner's grant")
                .noneMatch(run -> run.startsWith("999@"));
        assertThat(runs).allMatch(run -> run.equals("1@" + plannerId.value()));
        assertThat(runs).doesNotContain("1@" + strangerId.value());
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

    /** An operation that hangs and ignores interrupts, as one stuck in uninterruptible I/O does. */
    private static final class HungProvider implements AssetProvider {
        final InventoryProvider inner = new InventoryProvider();
        final java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        final java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);

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
            entered.countDown();
            while (release.getCount() > 0) {
                try {
                    release.await();
                } catch (InterruptedException ignored) {
                    // hung: the close's interrupt does not end it
                }
            }
            return inner.invoke(asset, operation, params, invocationId);
        }
    }

    /**
     * Wire adversary of phase 2 (srad/agentspaces-event-loop.md §10.14): the claim's renewal ends with the connector.
     * Before the fix the renewer looked only at its own interrupt, which only the operation's return sends, so a
     * closed connector whose operation hung renewed the claim for as long as it hung and no replica could take it.
     */
    @Test
    @Timeout(300)
    void aClosedConnectorStopsRenewingTheClaimOfAHungOperation() throws Exception {
        Wired connectorPeer = newPeer("conn", 1);
        Wired agent = newPeer("agent", 2, "conn");
        tickAll(4);
        HungProvider provider = new HungProvider();
        connector = new ConnectorRuntime(connectorPeer.data(), connectorPeer.discovery(), connectorPeer.identity(),
                "cell-connector", provider, groupId, clock, Duration.ofMinutes(1))
                .invokeLease(Duration.ofMillis(200));
        connector.start();
        try {
            tickAll(2);
            AssetCard card = agent.discovery().find(AssetCard.class, c -> c.asset().equals("inventory")).getFirst();
            AssetInvocations.invoke(agent.data(), card, "reserve", Map.of("sku", "A-1", "qty", "1"),
                    Duration.ofSeconds(30));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (provider.entered.getCount() > 0) {
                assertThat(System.nanoTime()).as("the operation started").isLessThan(deadline);
                tickAll(1);
                Thread.sleep(10);
            }
            Thread.sleep(350); // renewed while the connector runs
            assertThat(connector.renewals()).as("renewed every 100 ms while it ran").isGreaterThanOrEqualTo(2);
            connector.close();
            int atClose = connector.renewals();
            Thread.sleep(1_000); // ten half-leases
            assertThat(connector.renewals() - atClose).as("renewals after the connector closed").isLessThanOrEqualTo(1);
        } finally {
            provider.release.countDown();
        }
    }

    @Test
    @Timeout(300)
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
        AssetInvocationResult result = answered("connect.invoke.renewed", AssetInvocations.invoke(agent.data(), card, "reserve",
                Map.of("sku", "A-1", "qty", "1"), Duration.ofSeconds(30)));
        assertThat(result.ok()).isTrue();
        assertThat(provider.inner.reserved).hasSize(1);
        assertThat(connector.renewals()).as("renewed every half lease while the operation ran").isGreaterThanOrEqualTo(3);
    }

    @Test
    @Timeout(600)
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
                AssetInvocationResult result = answered("connect.replica.invoke", AssetInvocations.invoke(agent.data(), through, "reserve",
                        Map.of("sku", "A-" + i, "qty", "1"), Duration.ofSeconds(30)));
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
                AssetInvocationResult again = answered("connect.replica.retry", AssetInvocations.retry(agent.data(), cardB, new AssetInvocation(
                        AssetInvocations.VERSION, "inventory", "reserve", Map.of("sku", "x", "qty", "1"), id),
                        Duration.ofSeconds(30)));
                assertThat(again.ok()).isTrue();
            }
            assertThat(providerA.reserved.size() + providerB.reserved.size()).as("a retry never runs again")
                    .isEqualTo(runs);
        } finally {
            replica.close();
        }
    }

    /**
     * A space whose next completion with a result is refused the way {@link ReplicatedSpace} refuses a
     * displaced holder's (SPEC §7.4, the settle guarantee): it throws {@link LeaseExpiredException} and writes
     * no result. {@code beforeRefusal} runs first, so a test can land a replica's result in the window between
     * the connector remembering its answer and the refusal. Everything else goes to the replicated space.
     */
    private static final class RefusingSpace implements Space {
        final ReplicatedSpace inner;
        final java.util.concurrent.atomic.AtomicBoolean refuseNext = new java.util.concurrent.atomic.AtomicBoolean(true);
        final java.util.concurrent.CountDownLatch refused = new java.util.concurrent.CountDownLatch(1);
        final java.util.concurrent.atomic.AtomicReference<AssetInvocationResult> refusedResult =
                new java.util.concurrent.atomic.AtomicReference<>();
        volatile java.util.function.Consumer<AssetInvocationResult> beforeRefusal = r -> { };

        RefusingSpace(ReplicatedSpace inner) {
            this.inner = inner;
        }

        @Override public String name() { return inner.name(); }
        @Override public ai.badmonkey.agentspaces.common.id.SpaceId id() { return inner.id(); }
        @Override public <T> ai.badmonkey.agentspaces.api.space.EntryHandle write(T entry, Lease lease) {
            return inner.write(entry, lease);
        }
        @Override public <T> ai.badmonkey.agentspaces.api.space.EntryHandle write(T entry, Lease lease,
                                                                               Map<String, String> tags) {
            return inner.write(entry, lease, tags);
        }
        @Override public <T> java.util.Optional<T> read(Template<T> template) { return inner.read(template); }
        @Override public <T> java.util.Optional<T> read(Template<T> template, Duration timeout) {
            return inner.read(template, timeout);
        }
        @Override public <T> List<T> readAll(Template<T> template, int limit) { return inner.readAll(template, limit); }
        @Override public <T> List<Space.Issued<T>> readAllIssued(Template<T> template, int limit) {
            return inner.readAllIssued(template, limit);
        }
        @Override public <T> java.util.Optional<ai.badmonkey.agentspaces.api.space.TakenEntry<T>> take(
                Template<T> template, Lease takeLease, Duration timeout) {
            return inner.take(template, takeLease, timeout);
        }
        @Override public void complete(ai.badmonkey.agentspaces.api.space.TakenEntry<?> taken) { inner.complete(taken); }
        @Override public <R> ai.badmonkey.agentspaces.api.space.EntryHandle complete(
                ai.badmonkey.agentspaces.api.space.TakenEntry<?> taken, R result, Lease resultLease) {
            if (result instanceof AssetInvocationResult answer && refuseNext.getAndSet(false)) {
                beforeRefusal.accept(answer);
                refusedResult.set(answer);
                refused.countDown();
                throw new LeaseExpiredException("take lease lapsed or lost for entry " + taken.entryId()
                        + " (displaced by a replica's claim, scripted)");
            }
            return inner.complete(taken, result, resultLease);
        }
        @Override public <T> ai.badmonkey.agentspaces.api.space.Subscription notify(
                Template<T> template, ai.badmonkey.agentspaces.api.space.SpaceListener<T> listener, Lease lease) {
            return inner.notify(template, listener, lease);
        }
    }

    /** The result replica A wrote for an invocation B ran and could not complete, told apart by {@code by}. */
    private static AssetInvocationResult resultOfA(AssetInvocationResult refusedAtB) {
        AssetInvocation invocation = new AssetInvocation(AssetInvocations.VERSION, refusedAtB.asset(),
                refusedAtB.operation(), Map.of("sku", "A-1", "qty", "1"), refusedAtB.invocationId());
        return AssetInvocationResult.of(invocation, AssetProvider.InvokeResult.of(
                Map.of("reserved", "1", "invocationId", refusedAtB.invocationId(), "by", "inv-a")));
    }

    /**
     * Ticks until the future completes and returns how long it took, as a measured wait (srad/feature-measurements.md
     * §3.5). fm: (a) ceiling 30 s (owner rule 2), unchanged; the answer comes in tens of milliseconds.
     */
    private long awaitTicking(String metric, java.util.concurrent.CompletableFuture<?> future) {
        return (long) Measure.await(metric, Duration.ofSeconds(30), () -> {
            if (future.isDone()) {
                return true;
            }
            tickAll(1);
            return future.isDone();
        }, Wait.with().poll(Duration.ofMillis(20))).value();
    }

    /**
     * R2, 2026-11-16: a replica whose completion the space refused (a better claim displaced its take, or its
     * claim acknowledged a displaced replica's completion) wrote no result, and its mirror skipped the other
     * replica's result because the id was already answered there. A requester that invoked through its card
     * then waited out its timeout. Here the replica's result lands after the refusal.
     */
    @Test
    @Timeout(600)
    void aReplicaWhoseCompletionIsRefusedAnswersThroughItsCardWithTheResultThatLandsLater() throws Exception {
        Wired peerA = newPeer("conn-a", 1);
        Wired peerB = newPeer("conn-b", 2, "conn-a");
        Wired agent = newPeer("agent", 3, "conn-a");
        tickAll(6);
        InventoryProvider providerB = new InventoryProvider("@inv-b");
        PeerId a = peerA.identity().peerId();
        PeerId b = peerB.identity().peerId();
        RefusingSpace spaceB = new RefusingSpace(peerB.data());
        connector = new ConnectorRuntime(spaceB, peerB.discovery(), peerB.identity(),
                "inv-b", providerB, groupId, clock, Duration.ofMinutes(1)).replicas(a::equals);
        connector.start();
        tickAll(4);
        AssetCard cardB = agent.discovery().find(AssetCard.class,
                c -> c.asset().equals("inventory") && c.issuer().equals(b)).getFirst();
        java.util.concurrent.CompletableFuture<AssetInvocationResult> answer = AssetInvocations.invoke(agent.data(),
                cardB, "reserve", Map.of("sku", "A-1", "qty", "1"), Duration.ofSeconds(30));
        assertThat(spaceB.refused.await(30, TimeUnit.SECONDS)).as("B ran the invocation and was refused").isTrue();
        assertThat(providerB.reserved).hasSize(1);
        assertThat(answer).as("B wrote no result").isNotDone();

        peerA.data().write(resultOfA(spaceB.refusedResult.get()), Lease.of(Duration.ofMinutes(5)));
        long tookMillis = awaitTicking("connect.replica.refused-then-mirror", answer);
        System.out.println("refused completion, replica result later: answered in " + tookMillis + " ms");
        assertThat(answer).as("answered through B's card within the 30 s ceiling").isDone();
        AssetInvocationResult result = answer.get();
        assertThat(result.ok()).isTrue();
        assertThat(result.result()).as("B mirrored A's result").containsEntry("by", "inv-a");
        assertThat(connector.mirrored()).isEqualTo(1);
        assertThat(providerB.reserved).as("the target ran once here").hasSize(1);
        assertThat(peerB.data().readAllIssued(Template.of(AssetInvocationResult.class)
                .where("invocationId", eq(result.invocationId())), 16).stream()
                .filter(r -> r.issuer().peer().equals(b))).as("one result under B").hasSize(1);
    }

    /**
     * As above, with the replica's result landing between the connector remembering its answer and the refusal,
     * so its WRITTEN event was skipped as answered: the refusal itself finds the result and mirrors it.
     */
    @Test
    @Timeout(600)
    void aReplicaWhoseCompletionIsRefusedMirrorsTheResultThatLandedBeforeTheRefusal() throws Exception {
        Wired peerA = newPeer("conn-a", 1);
        Wired peerB = newPeer("conn-b", 2, "conn-a");
        Wired agent = newPeer("agent", 3, "conn-a");
        tickAll(6);
        InventoryProvider providerB = new InventoryProvider("@inv-b");
        PeerId a = peerA.identity().peerId();
        PeerId b = peerB.identity().peerId();
        RefusingSpace spaceB = new RefusingSpace(peerB.data());
        spaceB.beforeRefusal = refusedAtB -> {
            peerA.data().write(resultOfA(refusedAtB), Lease.of(Duration.ofMinutes(5)));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (peerB.data().readAllIssued(Template.of(AssetInvocationResult.class)
                    .where("invocationId", eq(refusedAtB.invocationId())), 16).stream()
                    .noneMatch(r -> r.issuer().peer().equals(a)) && System.nanoTime() < deadline) {
                tickAll(1);
            }
        };
        connector = new ConnectorRuntime(spaceB, peerB.discovery(), peerB.identity(),
                "inv-b", providerB, groupId, clock, Duration.ofMinutes(1)).replicas(a::equals);
        connector.start();
        tickAll(4);
        AssetCard cardB = agent.discovery().find(AssetCard.class,
                c -> c.asset().equals("inventory") && c.issuer().equals(b)).getFirst();
        java.util.concurrent.CompletableFuture<AssetInvocationResult> answer = AssetInvocations.invoke(agent.data(),
                cardB, "reserve", Map.of("sku", "A-1", "qty", "1"), Duration.ofSeconds(30));
        assertThat(spaceB.refused.await(30, TimeUnit.SECONDS)).as("B ran the invocation and was refused").isTrue();
        long tookMillis = awaitTicking("connect.replica.mirror-before-refusal", answer);
        System.out.println("refused completion, replica result before it: answered in " + tookMillis + " ms");
        assertThat(answer).as("answered through B's card within the 30 s ceiling").isDone();
        AssetInvocationResult result = answer.get();
        assertThat(result.result()).as("B mirrored A's result").containsEntry("by", "inv-a");
        assertThat(connector.mirrored()).isEqualTo(1);
        assertThat(providerB.reserved).as("the target ran once here").hasSize(1);
        assertThat(peerB.data().readAllIssued(Template.of(AssetInvocationResult.class)
                .where("invocationId", eq(result.invocationId())), 16).stream()
                .filter(r -> r.issuer().peer().equals(b))).as("one result under B").hasSize(1);
    }

    @Test
    @Timeout(600)
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
        AssetInvocationResult first = answered("connect.replica.first", AssetInvocations.invoke(agent.data(), cardA, "reserve",
                Map.of("sku", "A-1", "qty", "1"), Duration.ofSeconds(30)));
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
            AssetInvocationResult again = answered("connect.replica.late-retry", AssetInvocations.retry(agent.data(), cardB, new AssetInvocation(
                    AssetInvocations.VERSION, "inventory", "reserve", Map.of("sku", "A-1", "qty", "1"),
                    first.invocationId()), Duration.ofSeconds(30)));
            assertThat(again.ok()).isTrue();
            assertThat(again.result()).containsEntry("invocationId", first.invocationId());
            assertThat(providerB.reserved).as("the replica never ran it").isEmpty();
            assertThat(replica.repeated()).isEqualTo(1);
        } finally {
            replica.close();
        }
    }

    @Test
    @Timeout(300)
    void withdrawingAnAssetsResultsSendsTheNextQueryToTheSource() throws Exception {
        Wired connectorPeer = newPeer("conn", 1);
        Wired agent = newPeer("agent", 2, "conn");
        tickAll(4);
        connector = new ConnectorRuntime(connectorPeer.data(), connectorPeer.discovery(), connectorPeer.identity(),
                "cell-connector", new InventoryProvider(), groupId, clock, Duration.ofMinutes(1));
        connector.start();
        tickAll(2);
        DataQueryClient client = new DataQueryClient(agent.data(), "analyst");
        assertThat(client.fetch("inventory", Map.of("sku", "A-1"), Duration.ofSeconds(30))).isPresent();
        assertThat(connector.executed()).isEqualTo(1);
        tickAll(2);
        assertThat(client.fetch("inventory", Map.of("sku", "A-1"), Duration.ofSeconds(30)).orElseThrow().fromCache())
                .as("the live result serves the window").isTrue();
        assertThat(connector.withdrawResults("inventory")).isEqualTo(1);
        assertThat(connector.withdrawResults("inventory")).as("nothing left to withdraw").isZero();
        tickAll(4);
        DataQueryClient.Fetched fresh = client.fetch("inventory", Map.of("sku", "A-1"), Duration.ofSeconds(30)).orElseThrow();
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
