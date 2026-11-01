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
package ai.badmonkey.agentspaces.content;

import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.connect.ConnectorRuntime;
import ai.badmonkey.agentspaces.connect.DataQueryClient;
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

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Group K: a holder advertises a collection as an AssetCard, a member queries
 * it through the connector SDK and receives manifest summaries, and a root
 * published without replication is fetched after its manifest arrives on
 * demand.
 */
class CollectionTest {

    private static final int PIECE = 64 * 1024;

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<ConnectorRuntime> connectors = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zCollect");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zCollect", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "collect",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(PeerNode node, PeerIdentity identity, GroupRuntime runtime, ReplicatedSpace data,
                        DiscoveryService discovery, Content content) {
    }

    @AfterEach
    void tearDown() {
        connectors.forEach(ConnectorRuntime::close);
        nodes.forEach(PeerNode::close);
    }

    private Peer newPeer(String address, long seed, String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        BlockExchange blocks = new BlockExchange(runtime, codec, clock, 0, BlockExchange.DEFAULT_MAX_BLOCK_BYTES);
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, clock), codec, identity.peerId());
        ReplicatedSpace contentSpace = ReplicatedSpace.builder(runtime, "content", identity, "content")
                .clock(clock).settleWindow(Duration.ZERO).blocks(blocks).build();
        Content content = Content.builder(new CapabilityPipes(runtime, codec), runtime, contentSpace, identity.peerId())
                .codec(codec).clock(clock).store(new MemoryPieceStore(clock, 0)).blocks(blocks)
                .random(new Random(seed)).build();
        runtime.onTick(content::tick);
        content.start();
        ReplicatedSpace data = ReplicatedSpace.builder(runtime, "data", identity, "data")
                .clock(clock).settleWindow(Duration.ZERO).blocks(blocks).build();
        nodes.add(node);
        return new Peer(node, identity, runtime, data, discovery, content);
    }

    private void tickAll() {
        nodes.forEach(PeerNode::tick);
        clock.advance(Duration.ofMillis(250));
    }

    private boolean runUntil(Duration max, java.util.function.BooleanSupplier until) throws InterruptedException {
        long deadline = System.nanoTime() + max.toNanos();
        while (!until.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            tickAll();
            Thread.sleep(2);
        }
        return true;
    }

    private static byte[] bytes(int length, long seed) {
        byte[] out = new byte[length];
        new Random(seed).nextBytes(out);
        return out;
    }

    @Test
    void aCollectionCardIsQueriedThroughTheConnectorSdkAndAnUnreplicatedRootIsFetchedOnDemand() throws Exception {
        Peer registry = newPeer("registry", 1);
        Peer member = newPeer("member", 2, "registry");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        PublishOptions unreplicated = PublishOptions.defaults().withPieceSize(PIECE).withReplicate(false);
        Manifest tiles = registry.content().publish(bytes(3 * PIECE + 1, 31),
                unreplicated.withName("tiles-sector-7.bin").withAttrs(Map.of("sector", "7")));
        Manifest roads = registry.content().publish(bytes(2 * PIECE, 32),
                unreplicated.withName("roads-sector-7.bin").withAttrs(Map.of("sector", "7")));
        Manifest other = registry.content().publish(bytes(PIECE, 33),
                unreplicated.withName("tiles-sector-9.bin").withAttrs(Map.of("sector", "9")));
        assertThat(registry.content().spaces().manifest(tiles.root())).as("not replicated").isEmpty();

        ConnectorRuntime connector = new ConnectorRuntime(registry.data(), registry.discovery(), registry.identity(),
                "registry", new CollectionAssetProvider(registry.content(), List.of(
                        CollectionAssetProvider.Collection.all("checkpoints", "every artifact the registry holds"))),
                groupId, clock, Duration.ofMinutes(5));
        connectors.add(connector);
        connector.start();

        // the card reaches the member's discovery cache
        assertThat(runUntil(Duration.ofSeconds(20), () -> !member.discovery()
                .find(AssetCard.class, c -> "content:checkpoints".equals(c.asset())).isEmpty())).isTrue();
        AssetCard card = member.discovery().find(AssetCard.class, c -> "content:checkpoints".equals(c.asset())).get(0);
        assertThat(card.access()).containsEntry("kind", CollectionAssetProvider.KIND).containsEntry("roots", "3");

        // a query through the connector SDK: ticks carry the query to the registry and the result back
        DataQueryClient client = new DataQueryClient(member.data(), "analyst");
        CompletableFuture<Optional<DataQueryClient.Fetched>> answer = CompletableFuture.supplyAsync(() ->
                client.fetch("content:checkpoints", Map.of(CollectionAssetProvider.PARAM_TAG, "7"), Duration.ofSeconds(20)));
        assertThat(runUntil(Duration.ofSeconds(25), answer::isDone)).isTrue();
        Optional<DataQueryClient.Fetched> fetched = answer.get();
        assertThat(fetched).isPresent();
        assertThat(fetched.get().result().error()).isNull();
        assertThat(fetched.get().result().rows()).extracting(r -> r.get("name"))
                .containsExactlyInAnyOrder("tiles-sector-7.bin", "roads-sector-7.bin");
        assertThat(fetched.get().result().rows()).allSatisfy(row -> assertThat(row.get("root")).startsWith("bafkrei"));

        // the member fetches one listed root: its manifest arrives on demand (MANIFEST_WANT), then the pieces
        assertThat(runUntil(Duration.ofSeconds(10), () -> !member.content().spaces().holdings(tiles.root()).isEmpty()))
                .as("the registry's holding for the unreplicated root reached the member").isTrue();
        Fetch fetch = member.content().fetch(tiles.root());
        assertThat(runUntil(Duration.ofSeconds(20), fetch::complete))
                .as("trace=" + fetch.trace() + " member=" + member.content().stats() + " registry=" + registry.content().stats()
                        + " manifestHere=" + member.content().store().manifest(tiles.root()).isPresent()
                        + " failure=" + fetch.failure()).isTrue();
        assertThat(member.content().store().have(tiles.root()).cardinality()).isEqualTo(4);
        assertThat(member.content().describe(other.root())).as("a root never asked for stays unknown here").isEmpty();

        // a page counts complete holders for all its rows from one replica read: the member now holds tiles too
        CollectionAssetProvider all = new CollectionAssetProvider(registry.content(), List.of(
                CollectionAssetProvider.Collection.all("everything", "every artifact the registry holds")));
        assertThat(runUntil(Duration.ofSeconds(10), () -> "2".equals(holdersOf(all, "everything", tiles.root()))))
                .as("tiles: the registry and the member").isTrue();
        assertThat(holdersOf(all, "everything", roads.root())).as("roads: the registry alone").isEqualTo("1");
        assertThat(holdersOf(all, "everything", other.root())).as("the sector 9 tiles: the registry alone").isEqualTo("1");
    }

    private static String holdersOf(CollectionAssetProvider provider, String collection, String root) {
        return provider.query(CollectionAssetProvider.assetName(collection), Map.of(CollectionAssetProvider.PARAM_LIMIT, "100"))
                .rows().stream().filter(r -> root.equals(r.get("root"))).findFirst()
                .map(r -> r.get("holders")).orElse("absent");
    }
}
