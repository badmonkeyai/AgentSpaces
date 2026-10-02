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
package ai.badmonkey.agentspaces.transport.quic;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.DatagramSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The whole fabric over QUIC: two peers join a group through
 * {@code Endpoint("quic", …)} seeds, membership and gossip converge, and a
 * replicated space carries a leased take round-trip — nothing above the
 * transport changes.
 */
class QuicFleetTest {

    @org.junit.jupiter.api.BeforeAll
    static void requireNative() {
        QuicNative.assumeAvailable();
    }

    /** A task the fleet works. */
    public record Job(String jobId, String payload) {
    }

    /** The worked result. */
    public record Done(String jobId, String by) {
    }

    private record Peer(PeerNode node, PeerIdentity identity, GroupRuntime runtime,
                        ReplicatedSpace space, QuicTransport transport) {
    }

    private final List<Peer> peers = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (Peer peer : peers) {
            peer.space().close();
            peer.node().close();
            peer.transport().close();
        }
    }

    @Test
    @Timeout(120)
    void membershipGossipAndLeasedTakesRunUnchangedOverQuic() throws Exception {
        int seedPort = freeUdpPort();
        Peer first = startPeer(seedPort, 0);
        Peer second = startPeer(freeUdpPort(), seedPort);

        // The writer's entry replicates to the taker over QUIC.
        first.space().write(new Job("job-1", "compress the archives"),
                Lease.of(Duration.ofMinutes(10)));
        Optional<TakenEntry<Job>> taken = second.space().take(
                Template.of(Job.class), Lease.of(Duration.ofMinutes(5)),
                Duration.ofSeconds(30));
        assertThat(taken).isPresent();
        assertThat(taken.get().entry().jobId()).isEqualTo("job-1");

        // The completion and its result flow back the other way.
        second.space().complete(taken.get(), new Done("job-1", "second"),
                Lease.of(Duration.ofMinutes(10)));
        Optional<Done> done = awaitDone(first, Duration.ofSeconds(30));
        assertThat(done).isPresent();
        assertThat(done.get().by()).isEqualTo("second");

        // And each membership view holds the other peer, learned over QUIC.
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline && (memberIds(first).isEmpty()
                || memberIds(second).isEmpty())) {
            Thread.sleep(250);
        }
        assertThat(memberIds(first)).contains(second.identity().peerId());
        assertThat(memberIds(second)).contains(first.identity().peerId());
    }

    private static List<ai.badmonkey.agentspaces.common.id.PeerId> memberIds(Peer peer) {
        return peer.runtime().membership().allMembers().stream()
                .map(GroupMembership.Member::id).toList();
    }

    private static Optional<Done> awaitDone(Peer peer, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            Optional<Done> done = peer.space().read(Template.of(Done.class));
            if (done.isPresent()) {
                return done;
            }
            Thread.sleep(200);
        }
        return Optional.empty();
    }

    private static GroupAdvertisement group() {
        GroupId groupId = GroupId.fromFounding(
                "quic-fleet-test-v1".getBytes(StandardCharsets.UTF_8));
        return new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(365), "quic-fleet-test",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
    }

    private Peer startPeer(int port, int seedPort) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).build();
        QuicTransport transport = new QuicTransport();
        node.listen(transport, "127.0.0.1:" + port);
        List<PeerAdvertisement.Endpoint> seeds = seedPort == 0 ? List.of()
                : List.of(new PeerAdvertisement.Endpoint("quic", "127.0.0.1:" + seedPort, 0));
        GroupRuntime runtime = node.joinGroup(group(),
                new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2),
                seeds);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "jobs", identity, "host")
                .settleWindow(Duration.ofMillis(150))
                .build();
        node.startTicking(Duration.ofMillis(250));
        Peer peer = new Peer(node, identity, runtime, space, transport);
        peers.add(peer);
        return peer;
    }

    private static int freeUdpPort() throws IOException {
        try (DatagramSocket socket = new DatagramSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
