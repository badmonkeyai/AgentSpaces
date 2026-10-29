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
package ai.badmonkey.agentspaces.peering.blocks;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class BlockExchangeTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zBlocks");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zBlocks", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "blocks",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(PeerNode node, BlockExchange blocks) {
    }

    @AfterEach
    void tearDown() {
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
        nodes.add(node);
        return new Peer(node, new BlockExchange(runtime, codec));
    }

    @Test
    void blocksFetchAcrossPeersWithIntegrity() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        for (int i = 0; i < 3; i++) {
            nodes.forEach(PeerNode::tick);
        }

        byte[] payload = new byte[200_000];
        new Random(7).nextBytes(payload);
        String cid = a.blocks().put(payload);

        assertThat(cid).isEqualTo(BlockExchange.cidOf(payload));
        assertThat(b.blocks().local(cid)).isEmpty();
        assertThat(b.blocks().fetch(cid, List.of(a.node().peerId()), 2_000))
                .hasValueSatisfying(bytes -> assertThat(bytes).isEqualTo(payload));
        assertThat(b.blocks().local(cid)).isPresent(); // cached after fetch
    }

    @Test
    void missingBlocksTimeOutEmpty() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        for (int i = 0; i < 3; i++) {
            nodes.forEach(PeerNode::tick);
        }

        assertThat(b.blocks().fetch("zNoSuchBlock", List.of(a.node().peerId()), 200))
                .isEmpty();
    }

    // ---- srad/large-networks-bulk-data.md group B (LB-B-08) ----

    @Test
    void oversizeBlockIsRefusedAtPut() throws Exception {
        Peer a = newPeer("a", 1);
        BlockExchange small = new BlockExchange(
                a.node().joinGroup(groupAd, GroupMembership.Config.defaults(), List.of()),
                codec, clock, 0, 1_000);
        byte[] big = new byte[2_000];
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> small.put(big))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("content swarm")
                .hasMessageContaining("1000");
        assertThat(small.stats().refusedOversize()).isEqualTo(1);
        assertThat(small.put(new byte[1_000])).startsWith("z"); // the cap itself is fine
    }

    @Test
    void lateDeliveryIsKeptForTheNextRead() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        for (int i = 0; i < 3; i++) {
            nodes.forEach(PeerNode::tick);
        }
        byte[] payload = new byte[20_000];
        new Random(3).nextBytes(payload);
        String cid = a.blocks().put(payload);

        network.hold("a", "b"); // the answer is delayed past the waiter's patience
        assertThat(b.blocks().fetch(cid, List.of(a.node().peerId()), 30)).isEmpty();
        assertThat(network.heldCount("a", "b")).isGreaterThanOrEqualTo(1);
        while (network.releaseOne("a", "b")) {
            // deliver what was held, the BLOCK among it
        }
        assertThat(b.blocks().local(cid)).as("a late block is kept (LB-B-01)")
                .hasValueSatisfying(bytes -> assertThat(bytes).isEqualTo(payload));
        assertThat(b.blocks().stats().lateKept()).isEqualTo(1);
        assertThat(b.blocks().stats().droppedUnsolicited()).isZero();
    }

    @Test
    void quotaEvictsUnreferencedBlocksLeastRecentlyServed() throws Exception {
        Peer a = newPeer("a", 1);
        BlockExchange bounded = new BlockExchange(
                a.node().joinGroup(groupAd, GroupMembership.Config.defaults(), List.of()),
                codec, clock, 2_500, 1_000_000);
        byte[] p1 = new byte[1_000];
        byte[] p2 = new byte[1_000];
        byte[] p3 = new byte[1_000];
        new Random(21).nextBytes(p1);
        new Random(22).nextBytes(p2);
        new Random(23).nextBytes(p3);
        String c1 = bounded.put(p1);
        clock.advance(Duration.ofSeconds(1));
        String c2 = bounded.put(p2);
        bounded.retain(c2); // a live entry references p2
        clock.advance(Duration.ofSeconds(1));
        String c3 = bounded.put(p3); // 3,000 bytes held against a 2,500 quota

        assertThat(bounded.size()).isEqualTo(2);
        assertThat(bounded.local(c1)).as("the oldest unreferenced block went (LB-B-04)").isEmpty();
        assertThat(bounded.local(c2)).isPresent();
        assertThat(bounded.local(c3)).isPresent();
        assertThat(bounded.stats().evicted()).isEqualTo(1);
        assertThat(bounded.bytesUsed()).isEqualTo(2_000);

        bounded.release(c2);
        clock.advance(Duration.ofSeconds(1));
        bounded.local(c3); // serve p3 so p2 is the least recently served
        clock.advance(Duration.ofSeconds(1));
        String c4 = bounded.put(new byte[1_000]);
        assertThat(bounded.local(c2)).as("released, so evictable").isEmpty();
        assertThat(bounded.local(c3)).isPresent();
        assertThat(bounded.local(c4)).isPresent();
    }

    @Test
    void secondCandidateIsAskedOnlyAfterTheFirstStaysSilent() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        Peer c = newPeer("c", 3, "a");
        for (int i = 0; i < 4; i++) {
            nodes.forEach(PeerNode::tick);
        }
        byte[] payload = new byte[20_000];
        new Random(5).nextBytes(payload);
        String cid = a.blocks().put(payload); // c holds nothing

        long start = System.nanoTime();
        assertThat(b.blocks().fetch(cid, List.of(c.node().peerId(), a.node().peerId()), 1_500))
                .hasValueSatisfying(bytes -> assertThat(bytes).isEqualTo(payload));
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertThat(elapsedMillis).as("a was asked only after the 750 ms stagger (LB-B-03)")
                .isGreaterThanOrEqualTo(700);
    }
}
