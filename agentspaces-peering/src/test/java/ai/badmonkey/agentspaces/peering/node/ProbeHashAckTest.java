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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.gossip.ReconcilableState;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Probe hashes travel only when they will be read (reviews/IDLE-WORK-2026-11-13.md item 7), and a PING that carries
 * them counts as a reader. The case this pins: member A writes on a cadence to a stream member B does not hold, so A
 * stays active while B, for which those rumors are no activity, backs off. B's PINGs carry hashes; A's ACKs must carry
 * them too, or B never learns from a probe that it missed something on a stream they share.
 */
class ProbeHashAckTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zProbeAck");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zProbeAck",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
            "probe-ack", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final CborCodec codec = CborCodec.defaultCodec();
    private final WireCodec wire = new WireCodec(codec);

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode node(String address, long seed) throws IOException {
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        nodes.add(node);
        return node;
    }

    /** A shared stream whose digest never changes: both sides hold the same thing, so no round has news. */
    private static ReconcilableState quiet() {
        return new ReconcilableState() {
            @Override public byte[] digest() { return "same".getBytes(StandardCharsets.UTF_8); }
            @Override public byte[] deltaFor(byte[] remoteDigest) { return new byte[0]; }
            @Override public void applyDelta(byte[] delta) { }
            @Override public boolean applyDeltaChanged(byte[] delta) { return false; }
        };
    }

    private <T> List<T> bodies(List<byte[]> frames, Envelope.Kind kind, Class<T> type) {
        List<T> out = new ArrayList<>();
        for (byte[] frame : frames) {
            Optional<Envelope> envelope = wire.decode(frame);
            if (envelope.isPresent() && envelope.get().kind() == kind) {
                out.add(codec.fromWire(envelope.get().body(), type));
            }
        }
        return out;
    }

    @Test
    void aBusyMemberAnswersAPingThatCarriesHashesWithHashes() throws Exception {
        PeerNode a = node("a", 1);
        PeerNode b = node("b", 2);
        GroupRuntime ra = a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime rb = b.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        ra.gossip().reconcile("shared", quiet());
        rb.gossip().reconcile("shared", quiet());
        ra.gossip().onStream("busy", (from, id, payload) -> { });

        List<byte[]> aToB = new CopyOnWriteArrayList<>();
        List<byte[]> bToA = new CopyOnWriteArrayList<>();
        network.tap("a", "b", aToB::add);
        network.tap("b", "a", bToA::add);
        int item = 0;
        for (int i = 0; i < 40; i++) {
            // A writes every tick to a stream B does not hold: activity at A, none at B (honest backoff).
            ra.gossip().publish("busy", "p" + item++, "position".getBytes(StandardCharsets.UTF_8));
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
        assertThat(rb.membership().member(a.peerId())).isPresent();
        assertThat(ra.antiEntropyStretched()).as("A writes every tick").isFalse();
        assertThat(rb.antiEntropyStretched()).as("A's busy stream is no activity at B").isTrue();

        aToB.clear();
        bToA.clear();
        for (int i = 0; i < 10; i++) {
            ra.gossip().publish("busy", "p" + item++, "position".getBytes(StandardCharsets.UTF_8));
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
        assertThat(rb.antiEntropyStretched()).isTrue();
        List<Bodies.Ping> pingsFromB = bodies(bToA, Envelope.Kind.PING, Bodies.Ping.class);
        assertThat(pingsFromB).as("B probed A").isNotEmpty();
        assertThat(pingsFromB).as("B, backed off, asks with hashes")
                .allMatch(p -> p.root() != null || p.hashes() != null);
        List<Bodies.Ack> acksFromA = bodies(aToB, Envelope.Kind.ACK, Bodies.Ack.class).stream()
                .filter(ack -> ack.onBehalf() == null).toList();
        assertThat(acksFromA).as("A answered").isNotEmpty();
        assertThat(acksFromA).as("A answers a PING with hashes with its own, though A itself is busy")
                .allMatch(ack -> ack.hashes() != null && ack.hashes().containsKey("shared") || ack.root() != null);
        assertThat(bodies(aToB, Envelope.Kind.PING, Bodies.Ping.class))
                .as("A's own PINGs, while A is busy, still carry none")
                .allMatch(p -> p.root() == null && p.hashes() == null);
    }
}
