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
package ai.badmonkey.agentspaces.peering.bootstrap;

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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The opt-in LAN bootstrap beacon (spec §10.1 {@code bootstrap: multicast},
 * roadmap M1): nodes with no configured seeds discover each other from the
 * signed self-advertisements they beacon, admission rules still apply, and a
 * tampered or foreign datagram changes nothing.
 */
class MulticastBeaconTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final SimDatagrams air = new SimDatagrams();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zBeacon");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zBeacon",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
            "beacon", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    /**
     * An in-memory datagram medium: every endpoint hears every other's sends,
     * synchronously on the sender's thread, so discovery-to-admission is
     * deterministic. Stands in for the UDP multicast group.
     */
    static final class SimDatagrams {
        private final List<Endpoint> endpoints = new CopyOnWriteArrayList<>();

        Endpoint endpoint() {
            Endpoint endpoint = new Endpoint();
            endpoints.add(endpoint);
            return endpoint;
        }

        /** Puts a raw datagram on the air, as a stranger's socket would. */
        void inject(byte[] datagram) {
            for (Endpoint endpoint : endpoints) {
                endpoint.receiver.accept(datagram.clone());
            }
        }

        final class Endpoint implements MulticastBeacon.Datagrams {
            private volatile Consumer<byte[]> receiver = d -> {
            };
            final List<byte[]> sent = new CopyOnWriteArrayList<>();

            @Override
            public void send(byte[] datagram) {
                sent.add(datagram);
                for (Endpoint other : endpoints) {
                    if (other != this) {
                        other.receiver.accept(datagram.clone());
                    }
                }
            }

            @Override
            public void onReceive(Consumer<byte[]> receiver) {
                this.receiver = Objects.requireNonNull(receiver);
            }

            @Override
            public void close() {
                endpoints.remove(this);
            }
        }
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode beaconing(String address, long seed) throws IOException {
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(seed)
                .beacon(air.endpoint(), Duration.ofSeconds(1)).build();
        node.listen(network.register(address), address);
        nodes.add(node);
        return node;
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    /** Spec §10.1: two nodes with zero configured seeds hear each other's beacons and converge. */
    @Test
    void nodesWithNoSeedsDiscoverEachOtherAndConverge() throws Exception {
        PeerNode a = beaconing("a", 1);
        PeerNode b = beaconing("b", 2);
        GroupRuntime ra = a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime rb = b.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        assertThat(ra.membership().allMembers()).isEmpty();

        tickAll(3);

        assertThat(ra.membership().allMembers()).extracting(GroupMembership.Member::id)
                .containsExactly(b.peerId());
        assertThat(rb.membership().allMembers()).extracting(GroupMembership.Member::id)
                .containsExactly(a.peerId());
        // The introduction opened real links, so the data plane works.
        List<String> atB = new CopyOnWriteArrayList<>();
        rb.gossip().onStream("news", (from, id, payload) -> atB.add(new String(payload)));
        ra.gossip().publish("news", "n1", "hello over the LAN".getBytes());
        assertThat(atB).containsExactly("hello over the LAN");
    }

    /** Spec §10.1/§11: a datagram whose advertisement is tampered, foreign, or oversized is dropped with no side effect. */
    @Test
    void tamperedForeignAndOversizedDatagramsAreIgnored() throws Exception {
        PeerNode a = beaconing("a", 1);
        GroupRuntime ra = a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        CborCodec codec = CborCodec.defaultCodec();
        PeerIdentity stranger = PeerIdentity.generate();
        PeerAdvertisement ad = new PeerAdvertisement("aspace://zBeacon/peer/" + stranger.peerId(),
                stranger.peerId(), groupId, clock.instant(), Duration.ofMinutes(10),
                List.of(new PeerAdvertisement.Endpoint("mem", "nowhere", 0)), Set.of(), Map.of());
        byte[] adBytes = codec.toBytes(ad);
        byte[] genuine = codec.toBytes(new PeerNode.SignedPeerAd(adBytes, stranger.rawPublicKey(),
                stranger.sign(adBytes)));

        byte[] tampered = adBytes.clone();
        tampered[tampered.length / 2] ^= 0x01;
        air.inject(codec.toBytes(new PeerNode.SignedPeerAd(tampered, stranger.rawPublicKey(),
                stranger.sign(adBytes))));
        assertThat(ra.membership().member(stranger.peerId())).as("bad signature").isEmpty();

        PeerIdentity impostor = PeerIdentity.generate();
        air.inject(codec.toBytes(new PeerNode.SignedPeerAd(adBytes, impostor.rawPublicKey(),
                impostor.sign(adBytes))));
        assertThat(ra.membership().member(stranger.peerId())).as("key/issuer mismatch").isEmpty();

        PeerAdvertisement foreign = new PeerAdvertisement("aspace://zElse/peer/x", stranger.peerId(),
                GroupId.of("zElse"), clock.instant(), Duration.ofMinutes(10), List.of(), Set.of(), Map.of());
        byte[] foreignBytes = codec.toBytes(foreign);
        air.inject(codec.toBytes(new PeerNode.SignedPeerAd(foreignBytes, stranger.rawPublicKey(),
                stranger.sign(foreignBytes))));
        assertThat(ra.membership().allMembers()).as("unjoined group").isEmpty();

        air.inject(new byte[MulticastBeacon.MAX_DATAGRAM + 1]);
        air.inject(new byte[]{9, 9, 9});
        assertThat(ra.membership().allMembers()).isEmpty();

        // The genuine datagram admits (OPEN group) exactly as a bootstrap rumor would.
        air.inject(genuine);
        assertThat(ra.membership().member(stranger.peerId())).isPresent();
    }

    /** Spec §10.1: a beacon-configured node announces one signed self-ad per joined group, once per interval of its clock. */
    @Test
    void theBeaconAnnouncesOncePerIntervalPerGroup() throws Exception {
        SimDatagrams.Endpoint ear = air.endpoint();
        PeerNode a = beaconing("a", 1);
        a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        SimDatagrams.Endpoint mouth = (SimDatagrams.Endpoint) air.endpoints.get(1);
        List<byte[]> heard = new CopyOnWriteArrayList<>();
        ear.onReceive(heard::add);

        a.tick(); // first round announces
        a.tick(); // same instant: not yet
        assertThat(heard).hasSize(1);
        clock.advance(Duration.ofSeconds(1));
        a.tick();
        assertThat(heard).hasSize(2);
        assertThat(mouth.sent).hasSize(2);
        assertThat(heard.get(0).length).isLessThanOrEqualTo(MulticastBeacon.MAX_DATAGRAM);
        // The datagram is exactly a SignedPeerAd for the joined group.
        PeerNode.SignedPeerAd signed = CborCodec.defaultCodec()
                .fromBytes(heard.get(0), PeerNode.SignedPeerAd.class);
        PeerAdvertisement ad = CborCodec.defaultCodec()
                .fromBytes(signed.adBytes(), PeerAdvertisement.class);
        assertThat(ad.issuer()).isEqualTo(a.peerId());
        assertThat(ad.group()).isEqualTo(groupId);
    }
}
