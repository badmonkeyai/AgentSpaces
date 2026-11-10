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

import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.tree.Cids;
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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Reconciliation between two content swarms on {@code SimNetwork} (PDT 2 phase 5, P2-K-02, P2-K-03, P2-K-05,
 * P2-Q-PRF-06; S4 of §6.4): rounds of {@code TREE_WANT} with {@code root ""} and a sketch, each answered by a
 * {@code TREE_END} of reason 5 with the reply in the appended {@code sketch}, go only between level-2 members, find
 * both sides of a difference over the held roots, and stay within the bounds the restated P2-K-02 measures.
 */
class ContentReconcileTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<Peer> peers = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zReconcile");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zReconcile", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "content",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final Map<PeerId, CapabilityAdvertisement> ads = new ConcurrentHashMap<>();

    private record Peer(String name, PeerNode node, Content content, MemoryTreeStore trees) {
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

    private Peer newPeer(String address, long seed, UnaryOperator<Content.Builder> customise, String... seedAddresses) throws IOException {
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
        Content content = customise.apply(Content.builder(new CapabilityPipes(runtime, codec), runtime, space, identity.peerId())
                .codec(codec).clock(clock).store(new MemoryPieceStore(clock, 0)).blocks(blocks).random(new Random(seed))
                .treeStore(trees).advertisements(peer -> Optional.ofNullable(ads.get(peer)))).build();
        ads.put(identity.peerId(), content.describe(groupId));
        runtime.onTick(content::tick);
        content.start();
        Peer peer = new Peer(address, node, content, trees);
        peers.add(peer);
        return peer;
    }

    private void tickAll() {
        peers.forEach(p -> p.node().tick());
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

    private <T> T await(CompletableFuture<T> future) throws Exception {
        assertThat(runUntil(Duration.ofSeconds(60), future::isDone)).as("the reconciliation settles").isTrue();
        return future.get();
    }

    /** As {@link #await}, with the test clock at a tenth of real time, for rounds whose answer takes real work. */
    private <T> T awaitWork(CompletableFuture<T> future) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
        while (!future.isDone() && System.nanoTime() < deadline) {
            tickAll();
            Thread.sleep(25);
        }
        assertThat(future).as("the reconciliation settles").isDone();
        return future.get();
    }

    private static String root(String universe, int i) {
        return Cids.of((universe + ":" + i).getBytes(StandardCharsets.UTF_8), Cids.DAG_CBOR);
    }

    @Test
    void twoHoldersFindEachOthersHeldRootsAndALevelOneMemberIsNeverAsked() throws Exception {
        Peer a = newPeer("a", 1, b -> b.pdtLevel(2));
        Peer b = newPeer("b", 2, x -> x.pdtLevel(2), "a");
        Peer old = newPeer("old", 3, x -> x, "a");
        for (int i = 0; i < 6; i++) {
            NodeFixtures.Tree tree = NodeFixtures.tree(500 + i, 2, 3, 200);
            (i % 2 == 0 ? a : b).trees().putAll(tree);
            (i % 2 == 0 ? a : b).content().holdTree(tree.root());
            if (i == 0) {
                b.trees().putAll(tree);
                b.content().holdTree(tree.root());
            }
        }
        assertThat(runUntil(Duration.ofSeconds(20), () -> a.content().heldRoots().size() == 3 && b.content().heldRoots().size() == 4))
                .isTrue();

        Content.Reconciliation outcome = await(a.content().reconcileHeld(b.id()));
        Set<String> onlyA = new HashSet<>(a.content().heldRoots());
        onlyA.removeAll(b.content().heldRoots());
        Set<String> onlyB = new HashSet<>(b.content().heldRoots());
        onlyB.removeAll(a.content().heldRoots());
        assertThat(outcome.have()).containsExactlyInAnyOrderElementsOf(onlyA).hasSize(2);
        assertThat(outcome.need()).containsExactlyInAnyOrderElementsOf(onlyB).hasSize(3);
        assertThat(outcome.encoding()).isEqualTo("negentropy");

        // rule 12: the form goes only to a level-2 member
        CompletableFuture<Content.Reconciliation> refused = a.content().reconcileHeld(old.id());
        assertThat(refused).isCompletedExceptionally();
        assertThatThrownBy(refused::get).isInstanceOf(ExecutionException.class).hasMessageContaining("pdt level 1");
        assertThat(old.content().stats().treeMessagesDropped()).isZero();
    }

    @Test
    void anArtifactRootComesBackAsTheRawCidOfItsManifest() throws Exception {
        Peer a = newPeer("a", 4, b -> b.pdtLevel(2));
        Peer b = newPeer("b", 5, x -> x.pdtLevel(2), "a");
        byte[] bytes = new byte[200_000];
        new Random(5).nextBytes(bytes);
        String artifact = b.content().publish(bytes).root();
        assertThat(runUntil(Duration.ofSeconds(20), () -> b.content().heldRoots().contains(artifact)
                && a.content().spaces().manifest(artifact).isPresent())).isTrue();

        Content.Reconciliation outcome = await(a.content().reconcileHeld(b.id()));
        assertThat(outcome.need()).containsExactly(artifact);
        assertThat(Cids.parse(artifact).codec()).isEqualTo(Cids.RAW);
    }

    /** Two sets of 100,000 roots that differ by 1,000 at random places, half on each side. */
    private static List<List<String>> hundredThousandDifferingByAThousand(long seed) {
        Random random = new Random(seed);
        Set<Integer> onlyA = new HashSet<>();
        Set<Integer> onlyB = new HashSet<>();
        while (onlyA.size() < 500) {
            onlyA.add(random.nextInt(100_000));
        }
        while (onlyB.size() < 500) {
            int i = random.nextInt(100_000);
            if (!onlyA.contains(i)) {
                onlyB.add(i);
            }
        }
        List<String> a = new ArrayList<>();
        List<String> b = new ArrayList<>();
        for (int i = 0; i < 100_000; i++) {
            if (!onlyB.contains(i)) {
                a.add(root("k", i));
            }
            if (!onlyA.contains(i)) {
                b.add(root("k", i));
            }
        }
        return List.of(a, b);
    }

    @Test
    void aHundredThousandRootsDifferingByAThousandReconcileWithinTheRestatedBounds() throws Exception {
        List<List<String>> sets = hundredThousandDifferingByAThousand(42);
        Peer a = newPeer("a", 6, x -> x.pdtLevel(2).reconcileSet(() -> sets.get(0)));
        Peer b = newPeer("b", 7, x -> x.pdtLevel(2).reconcileSet(() -> sets.get(1)), "a");
        runUntil(Duration.ofSeconds(5), () -> false);

        // P2-K-02 restated (§10.12): roots share no time, so every difference is scattered by digest
        Content.Reconciliation negentropy = awaitWork(a.content().reconcileHeld(b.id()));
        assertThat(negentropy.have()).hasSize(500);
        assertThat(negentropy.need()).hasSize(500);
        assertThat(negentropy.rounds()).as("within the 64 rounds of P2-K-04").isLessThanOrEqualTo(16);
        assertThat(negentropy.bytes()).as("about 900 bytes per difference").isLessThan(1_000L * 1_000);

        // P2-K-05 and P2-Q-PRF-06: RIBLT carries the same difference in two rounds, under 100 KiB
        Content.Reconciliation riblt = awaitWork(a.content().reconcileHeld(b.id(), 1_000));
        assertThat(riblt.encoding()).isEqualTo("riblt");
        assertThat(riblt.have()).containsExactlyInAnyOrderElementsOf(negentropy.have());
        assertThat(riblt.need()).containsExactlyInAnyOrderElementsOf(negentropy.need());
        assertThat(riblt.rounds()).isLessThanOrEqualTo(3);
        assertThat(riblt.bytes()).isLessThan(100L * 1024);
        System.out.println("P2-K-02 measured: negentropy " + negentropy.rounds() + " rounds " + negentropy.bytes()
                + " bytes; riblt " + riblt.rounds() + " rounds " + riblt.bytes() + " bytes");
    }

    @Test
    void aSketchOutsideTheGrammarIsDroppedAndCountedAndStrikesNobody() throws Exception {
        Peer a = newPeer("a", 8, x -> x.pdtLevel(2));
        Peer b = newPeer("b", 9, x -> x.pdtLevel(2), "a");
        runUntil(Duration.ofSeconds(5), () -> false);
        long before = b.content().stats().treeMessagesDropped();
        CapabilityPipes pipes = new CapabilityPipes(a.node().group(groupId).orElseThrow(), codec);
        pipes.send(b.id(), Content.TYPE, codec.toBytes(new Messages.TreeWant(Messages.TREE_WANT, 77, "", List.of(),
                Messages.MAX_TREE_BUDGET, new byte[]{0x40, 0x01})));
        assertThat(runUntil(Duration.ofSeconds(5), () -> b.content().stats().treeMessagesDropped() == before + 1)).isTrue();
        assertThat(a.content().stats().piecesStruck()).isZero();
        assertThat(b.content().stats().piecesStruck()).isZero();
    }
}
