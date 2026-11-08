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
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.AdvertisementSigner;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PD-X-01: the content-swarm advertisement carries {@code trees = "1"}, and a
 * peer learns through the group's discovery cache which members advertise it, so
 * it sends tags 8 to 11 only to those; a member whose advertisement lacks the
 * parameter (a 0.5.0 peer) or has expired does not qualify.
 */
class TreesAdvertisementTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<Peer> peers = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final AdvertisementSigner signer = new AdvertisementSigner();
    private final GroupId groupId = GroupId.of("zTrees");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zTrees", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "content",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(String name, PeerIdentity identity, PeerNode node, GroupRuntime runtime,
                        DiscoveryService discovery, Content content) {
    }

    @AfterEach
    void tearDown() {
        peers.forEach(p -> {
            p.content().close();
            p.discovery().close();
            p.node().close();
        });
    }

    private Peer newPeer(String address, long seed,
                         java.util.function.UnaryOperator<Content.Builder> customise,
                         String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        DiscoveryService discovery = DiscoveryService.create(runtime, codec, identity.peerId(), clock, Set.of());
        BlockExchange blocks = new BlockExchange(runtime, codec, clock, 0, BlockExchange.DEFAULT_MAX_BLOCK_BYTES);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "content", identity, "content")
                .clock(clock).settleWindow(Duration.ZERO).blocks(blocks).build();
        Content content = customise.apply(Content.builder(new CapabilityPipes(runtime, codec), runtime, space, identity.peerId())
                .codec(codec).clock(clock).blocks(blocks).random(new Random(seed)).discovery(discovery)).build();
        runtime.onTick(content::tick);
        content.start();
        Peer peer = new Peer(address, identity, node, runtime, discovery, content);
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
    void theAdvertisementCarriesTreesOne() throws Exception {
        Peer a = newPeer("mem://a", 1, b -> b);
        CapabilityAdvertisement ad = a.content().describe(groupId);
        assertThat(ad.capabilityType()).isEqualTo(Content.TYPE);
        assertThat(ad.parameters()).containsEntry(Content.TREES_PARAMETER, Content.TREES_VERSION)
                .containsKeys("pieceSizes", "sealing", "seedMode");
        assertThat(Content.TREES_PARAMETER).isEqualTo("trees");
        assertThat(Content.TREES_VERSION).isEqualTo("1");
    }

    @Test
    void aPeerLearnsWhichMembersAdvertiseTreesThroughDiscovery() throws Exception {
        Peer a = newPeer("mem://a", 1, b -> b);
        Peer b = newPeer("mem://b", 2, x -> x, "mem://a");
        Peer c = newPeer("mem://c", 3, x -> x, "mem://a");
        PeerId bId = b.identity().peerId();
        PeerId cId = c.identity().peerId();
        assertThat(a.content().advertisesTrees(bId)).as("no advertisement known yet").isFalse();

        // B publishes the advertisement this build describes; C publishes a 0.5.0 shape without "trees".
        b.discovery().publish(signer.sign(b.content().describe(groupId), b.identity()));
        CapabilityAdvertisement older = c.content().describe(groupId);
        Map<String, String> withoutTrees = new HashMap<>(older.parameters());
        withoutTrees.remove(Content.TREES_PARAMETER);
        CapabilityAdvertisement legacy = new CapabilityAdvertisement(older.id(), older.issuer(), older.group(),
                older.issued(), older.ttl(), older.capabilityType(), older.version(), older.binding(),
                Map.copyOf(withoutTrees), older.costHints());
        c.discovery().publish(signer.sign(legacy, c.identity()));

        assertThat(runUntil(Duration.ofSeconds(10), () -> a.content().advertisesTrees(bId))).as("B's ad reaches A").isTrue();
        assertThat(runUntil(Duration.ofSeconds(10), () -> !a.discovery().find(CapabilityAdvertisement.class,
                ad -> cId.equals(ad.issuer())).isEmpty())).as("C's ad reaches A").isTrue();
        assertThat(a.content().advertisesTrees(cId)).as("a 0.5.0 peer never qualifies").isFalse();
        assertThat(a.content().advertisesTrees(a.identity().peerId())).as("A never published its own").isFalse();

        // Past the TTL, B's advertisement no longer counts.
        clock.advance(Content.ADVERTISEMENT_TTL.plusSeconds(1));
        assertThat(a.content().advertisesTrees(bId)).as("an expired advertisement does not qualify").isFalse();
    }

    @Test
    void anExplicitLookupTakesPrecedenceAndTheDefaultIsFalse() throws Exception {
        Peer a = newPeer("mem://a", 1, b -> b);
        CapabilityAdvertisement ad = a.content().describe(groupId);
        PeerId other = PeerIdentity.generate().peerId();
        Peer lookedUp = newPeer("mem://b", 2, b -> b.advertisements(peer -> peer.equals(other) ? Optional.of(ad) : Optional.empty()), "mem://a");
        assertThat(lookedUp.content().advertisesTrees(other)).isTrue();
        assertThat(lookedUp.content().advertisesTrees(a.identity().peerId())).isFalse();
        Peer none = newPeer("mem://c", 3, b -> b.discovery(null), "mem://a");
        assertThat(none.content().advertisesTrees(other)).as("no lookup: no member speaks trees").isFalse();
    }
}
