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

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
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
import java.util.Random;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PD-X-02 and PD-Q-SEC-01: a receiver drops and counts a tree message outside its
 * bounds, and a {@code NODES} nobody asked for, instead of acting on it. One peer
 * sends raw payloads under the capability to another that serves a tree.
 */
class TreeBoundsTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<Peer> peers = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zTreeBounds");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zTreeBounds", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "content",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(PeerNode node, GroupRuntime runtime, CapabilityPipes pipes, Content content, MemoryTreeStore trees) {
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
        CapabilityPipes pipes = new CapabilityPipes(runtime, codec);
        MemoryTreeStore trees = new MemoryTreeStore();
        Content content = Content.builder(pipes, runtime, space, identity.peerId())
                .codec(codec).clock(clock).store(new MemoryPieceStore(clock, 0)).blocks(blocks).random(new Random(seed))
                .treeStore(trees).build();
        runtime.onTick(content::tick);
        content.start();
        Peer peer = new Peer(node, runtime, pipes, content, trees);
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

    @Test
    void messagesOutsideTheirBoundsAndUnsolicitedNodesAreDroppedAndCounted() throws Exception {
        Peer x = newPeer("x", 1);
        Peer y = newPeer("y", 2, "x");
        for (int i = 0; i < 6; i++) {
            tickAll();
        }
        NodeFixtures.Tree tree = NodeFixtures.tree(3, 2, 3, 50);
        y.trees().putAll(tree);
        List<String> cids = new ArrayList<>(tree.nodes().keySet());
        var yId = y.node().peerId();

        // a well-formed NODE_WANT is served, and x drops the answer it never asked for
        x.pipes().send(yId, Content.TYPE, codec.toBytes(new Messages.NodeWant(1, List.of(cids.get(0)))));
        assertThat(runUntil(Duration.ofSeconds(10), () -> y.content().stats().nodesServed() == 1)).isTrue();
        assertThat(runUntil(Duration.ofSeconds(10), () -> x.content().stats().treeMessagesDropped() == 1))
                .as("x has no session for req 1").isTrue();

        // a NODES over 4 MiB
        x.pipes().send(yId, Content.TYPE, codec.toBytes(
                Messages.Nodes.plain(2, List.of(new byte[Messages.MAX_NODES_BYTES + 1]), null)));
        // a TREE_WANT with 65 haves
        List<String> haves = new ArrayList<>();
        for (int i = 0; i <= Messages.MAX_HAVES; i++) {
            haves.add(cids.get(0));
        }
        x.pipes().send(yId, Content.TYPE, codec.toBytes(new Messages.TreeWant(3, tree.root(), haves, 1 << 20)));
        // a NODE_WANT with 257 cids
        List<String> many = new ArrayList<>();
        for (int i = 0; i <= Messages.MAX_WANT_CIDS; i++) {
            many.add(cids.get(0));
        }
        x.pipes().send(yId, Content.TYPE, codec.toBytes(new Messages.NodeWant(4, many)));
        // a NODES for a request nobody made
        x.pipes().send(yId, Content.TYPE, codec.toBytes(
                Messages.Nodes.plain(99, List.of(tree.nodes().get(tree.root())), null)));
        // a TREE_END for a request nobody made
        x.pipes().send(yId, Content.TYPE, codec.toBytes(new Messages.TreeEnd(98, List.of(), Messages.END_DONE)));
        // a TREE_END with an unknown reason
        x.pipes().send(yId, Content.TYPE, codec.toBytes(new Messages.TreeEnd(97, List.of(), 9)));

        assertThat(runUntil(Duration.ofSeconds(10), () -> y.content().stats().treeMessagesDropped() == 6))
                .as("dropped: " + y.content().stats()).isTrue();
        Content.Stats stats = y.content().stats();
        assertThat(stats.nodesServed()).as("nothing more was served").isEqualTo(1);
        assertThat(stats.treeRequestsRejected()).isZero();
        assertThat(stats.nodesReceived()).isZero();
        assertThat(y.trees().size()).isEqualTo(tree.nodes().size());
        assertThat(x.trees().size()).as("the unsolicited answer stored nothing").isZero();

        // §6.7 row 3: each side holds one serial queue for the peer that sent it tree frames, and the tick
        // forgets a queue once it has sat idle for TREE_QUEUE_IDLE_MILLIS, so a departed member leaves nothing
        assertThat(y.content().treeQueuesHeld()).isEqualTo(1);
        assertThat(x.content().treeQueuesHeld()).isEqualTo(1);
        for (int i = 0; i * 250L <= Content.TREE_QUEUE_IDLE_MILLIS + 250; i++) {
            tickAll();
        }
        assertThat(y.content().treeQueuesHeld()).isZero();
        assertThat(x.content().treeQueuesHeld()).isZero();
    }
}
