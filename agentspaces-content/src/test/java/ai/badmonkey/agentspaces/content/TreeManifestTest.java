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
import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.connect.AssetProvider;
import ai.badmonkey.agentspaces.content.tree.MemoryTreeStore;
import ai.badmonkey.agentspaces.content.tree.NodeFixtures;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * PDT 2 follow-ups, 2026-11-09, F3 (the rest of P2-X-01, PD-X-09): a {@code layout: tree} manifest names a blob
 * tree whose root names every piece, so it carries no piece table. {@code Content.fetch} moves such a root by the
 * tree exchange, {@code describe} and {@code list} show it with its nodes and holders, the collection asset
 * provider lists the tree roots it holds, and a table-less tree manifest that names a flat root never sends a flat
 * fetch after a table it does not have.
 */
class TreeManifestTest {

    private static final int PIECE = 64 * 1024;

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<Peer> peers = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zTreeManifest");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zTreeManifest", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "content",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final Map<PeerId, CapabilityAdvertisement> ads = new ConcurrentHashMap<>();

    private record Peer(PeerNode node, ReplicatedSpace space, Content content, MemoryTreeStore trees) {
        PeerId id() {
            return node.peerId();
        }
    }

    @AfterEach
    void tearDown() {
        peers.forEach(p -> {
            p.content().close();
            p.node().close();
        });
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
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "content", identity, "content")
                .clock(clock).settleWindow(Duration.ZERO).blocks(blocks).build();
        MemoryTreeStore trees = new MemoryTreeStore();
        Content content = Content.builder(new CapabilityPipes(runtime, codec), runtime, space, identity.peerId())
                .codec(codec).clock(clock).store(new MemoryPieceStore(clock, 0)).blocks(blocks).random(new Random(seed))
                .pieceSize(PIECE).treeStore(trees).advertisements(peer -> Optional.ofNullable(ads.get(peer))).build();
        ads.put(identity.peerId(), content.describe(groupId));
        runtime.onTick(content::tick);
        content.start();
        Peer peer = new Peer(node, space, content, trees);
        peers.add(peer);
        return peer;
    }

    private void tickAll() {
        for (Peer p : peers) {
            p.node().tick();
        }
        clock.advance(Duration.ofMillis(250));
    }

    private boolean runUntil(Duration max, BooleanSupplier until) throws InterruptedException {
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

    /** The manifest of a blob tree: its root, a size and piece size that agree, and no piece table. */
    private static Manifest treeManifest(String root, PeerId origin, String name) {
        long size = 3L * PIECE + 1;
        return new Manifest(Manifest.VERSION, root, size, PIECE, 4, null, null, null, name, "application/octet-stream",
                Manifest.SEALING_NONE, null, Manifest.HASHING_PLAIN, origin, 1_000L, Map.of("run", "7"),
                Manifest.LAYOUT_TREE);
    }

    private static byte[] bytes(int n, int seed) {
        byte[] out = new byte[n];
        new Random(seed).nextBytes(out);
        return out;
    }

    /** A holder with a tree, its holding, and its table-less tree manifest; and a member beside it. */
    private Peer[] holderAndMember(NodeFixtures.Tree tree) throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        a.trees().putAll(tree);
        a.content().holdTree(tree.root());
        a.content().spaces().writeManifest(treeManifest(tree.root(), a.id(), "tiles.pmtiles"), Lease.of(Duration.ofHours(1)));
        assertThat(runUntil(Duration.ofSeconds(10), () -> b.content().spaces().manifest(tree.root()).isPresent()
                && !b.content().spaces().holdings(tree.root()).isEmpty())).isTrue();
        return new Peer[]{a, b};
    }

    @Test
    void aTreeManifestsRootFetchesByTheTreeExchange() throws Exception {
        NodeFixtures.Tree tree = NodeFixtures.tree(31, 3, 4, 1_500);
        Peer[] ab = holderAndMember(tree);
        Peer b = ab[1];

        Fetch fetch = b.content().fetch(tree.root());
        assertThat(runUntil(Duration.ofSeconds(20), fetch::complete)).isTrue();
        assertThat(fetch.failure()).isEmpty();
        assertThat(fetch.root()).isEqualTo(tree.root());
        assertThat(fetch.manifest()).hasValueSatisfying(m -> assertThat(m.treeLayout()).isTrue());
        assertThat(b.trees().complete(tree.root())).as("the nodes arrived by the tree exchange").isTrue();
        assertThat(fetch.progress().piecesVerified()).as("the nodes the fetch verified").isEqualTo(tree.nodes().size());
        assertThat(b.content().activeTreeFetches()).isEmpty();
        assertThat(b.content().store().roots()).as("no piece of a tree root enters the piece store").doesNotContain(tree.root());
        assertThatThrownBy(fetch::open).isInstanceOf(IOException.class).hasMessageContaining("blob tree");
        assertThat(b.content().fetch(tree.root()).complete()).as("a second fetch of a held tree is complete at once").isTrue();
    }

    @Test
    void describeAndListShowATreeRootWithItsNodesAndHolders() throws Exception {
        NodeFixtures.Tree tree = NodeFixtures.tree(32, 3, 4, 1_500);
        Peer[] ab = holderAndMember(tree);
        Peer b = ab[1];

        Content.Description seen = b.content().describe(tree.root()).orElseThrow();
        assertThat(seen.manifest().treeLayout()).isTrue();
        assertThat(seen.completeHolders()).isEqualTo(1);
        assertThat(seen.nodes()).as("the nodes a complete holder states").isEqualTo(tree.nodes().size());
        assertThat(seen.held().isEmpty()).isTrue();
        assertThat(b.content().list(100)).extracting(Manifest::root).contains(tree.root());

        assertThat(runUntil(Duration.ofSeconds(20), b.content().fetchTree(tree.root())::complete)).isTrue();
        assertThat(runUntil(Duration.ofSeconds(10), () -> b.content().describe(tree.root()).orElseThrow().completeHolders() == 2))
                .isTrue();
        assertThat(b.content().describe(tree.root()).orElseThrow().nodes()).isEqualTo(tree.nodes().size());
    }

    @Test
    void aCollectionListsTheTreeRootsItHoldsWithTheirLayoutNodesAndHolders() throws Exception {
        NodeFixtures.Tree tree = NodeFixtures.tree(33, 3, 4, 1_500);
        Peer[] ab = holderAndMember(tree);
        Peer a = ab[0];
        Manifest flat = a.content().publish(bytes(2 * PIECE + 5, 33),
                PublishOptions.defaults().withPieceSize(PIECE).withName("flat.bin"));
        CollectionAssetProvider provider = new CollectionAssetProvider(a.content(),
                List.of(CollectionAssetProvider.Collection.all("all", "everything this holder serves")));
        CollectionAssetProvider.Collection all = provider.collections().get(0);

        assertThat(provider.rowCount(all)).isEqualTo(2);
        assertThat(provider.advertises(treeManifest(tree.root(), a.id(), "tiles.pmtiles"))).isTrue();
        AssetCard card = provider.describeAssets(groupId, a.id(), clock.instant()).get(0);
        assertThat(card.access()).containsEntry("roots", "2");
        assertThat(card.shape()).isEqualTo("rows(root,name,mediaType,size,pieces,created,origin,attrs,holders,layout,nodes)");
        assertThat(card.description()).contains("tiles.pmtiles").contains("flat.bin");

        AssetProvider.QueryResult result = provider.query(CollectionAssetProvider.assetName("all"), Map.of());
        assertThat(result.error()).isNull();
        Map<String, Map<String, String>> byRoot = new java.util.HashMap<>();
        result.rows().forEach(r -> byRoot.put(r.get("root"), r));
        assertThat(byRoot).containsOnlyKeys(tree.root(), flat.root());
        assertThat(byRoot.get(tree.root())).containsEntry("layout", Manifest.LAYOUT_TREE)
                .containsEntry("nodes", Integer.toString(tree.nodes().size())).containsEntry("holders", "1")
                .containsEntry("name", "tiles.pmtiles").containsEntry("size", Long.toString(3L * PIECE + 1))
                .containsEntry("attrs", "run=7");
        assertThat(byRoot.get(flat.root())).containsEntry("layout", Manifest.LAYOUT_PIECES).containsEntry("nodes", "0")
                .containsEntry("pieces", "3");
        assertThat(provider.query(CollectionAssetProvider.assetName("all"),
                Map.of(CollectionAssetProvider.PARAM_NAME_PREFIX, "tiles")).rows()).extracting(r -> r.get("root"))
                .containsExactly(tree.root());

        // a tree root this holder no longer holds complete leaves the collection
        a.content().releaseTree(tree.root());
        assertThat(provider.rowCount(all)).isEqualTo(1);
    }

    @Test
    void aTablelessTreeManifestNamingAFlatRootSendsNoFetchAfterATable() throws Exception {
        Peer a = newPeer("a", 4);
        Peer b = newPeer("b", 5, "a");
        Peer m = newPeer("m", 6, "a");
        for (int i = 0; i < 6; i++) {
            tickAll();
        }
        byte[] data = bytes(3 * PIECE + 11, 34);
        // the honest manifest is not replicated, so a fetcher asks a holder for it
        Manifest honest = a.content().publish(data, PublishOptions.defaults().withPieceSize(PIECE).withReplicate(false));
        // a hostile member writes a table-less tree manifest for the flat root, past the writer's own check
        Manifest hostile = treeManifest(honest.root(), m.id(), "flat-as-tree");
        m.space().write(hostile, Lease.of(Duration.ofHours(1)), hostile.tags());
        assertThat(runUntil(Duration.ofSeconds(10), () -> !b.content().spaces().holdings(honest.root()).isEmpty())).isTrue();
        for (int i = 0; i < 8; i++) {
            tickAll();
        }

        assertThat(hostile.violation()).contains("a tree manifest's root is a dag-cbor CID");
        assertThat(b.content().spaces().manifest(honest.root())).as("no member reads the hostile manifest").isEmpty();
        assertThat(b.content().list(100)).extracting(Manifest::root).doesNotContain(honest.root());
        Fetch fetch = b.content().fetch(honest.root());
        assertThat(runUntil(Duration.ofSeconds(20), fetch::complete)).as("the flat fetch reads the holder's manifest").isTrue();
        assertThat(fetch.manifest()).hasValueSatisfying(got -> assertThat(got.treeLayout()).isFalse());
        try (var channel = fetch.open()) {
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(data.length);
            while (buffer.hasRemaining() && channel.read(buffer) > 0) {
                // read to the end
            }
            assertThat(buffer.array()).isEqualTo(data);
        }
        assertThat(b.content().describe(honest.root()).orElseThrow().manifest().treeLayout()).isFalse();
    }
}
