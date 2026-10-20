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
package ai.badmonkey.agentspaces.transport.http;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The whole fabric over the HTTP bindings (SRAD agentspaces-js JS-Q-REL-02,
 * JS-H-05, JS-H-09): two peers join through {@code ws}, {@code wss}, or
 * {@code http} seeds, membership and gossip converge, and a replicated space
 * carries a leased take round trip. Over {@code ws} and {@code http} the
 * nodes run SESSION mode, so after the join their frames travel under the
 * session key (SPEC §5.6 v0.1.17); over {@code wss} the handshake attests
 * both ends and a node that requires attestation admits the peer, while it
 * refuses to register the plaintext bindings at all.
 */
class HttpFleetTest {

    public record Job(String jobId, String payload) {
    }

    public record Done(String jobId, String by) {
    }

    private record Peer(PeerNode node, PeerIdentity identity, GroupRuntime runtime, ReplicatedSpace space,
                        AutoCloseable transport) {
    }

    private final List<Peer> peers = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (Peer peer : peers) {
            peer.space().close();
            peer.node().close();
            peer.transport().close();
        }
    }

    @Test
    @Timeout(120)
    void sessionModeFleetOverWs() throws Exception {
        fleet("ws", PeerNode.ChannelAuth.SESSION, false, id -> new WebSocketTransport());
        assertThat(peers.get(0).node().sessionFramesSent() + peers.get(1).node().sessionFramesSent())
                .as("frames travel under the session key once the hellos are exchanged").isPositive();
    }

    @Test
    @Timeout(120)
    void attestedFleetOverWssUnderRequiredAttestation() throws Exception {
        fleet("wss", PeerNode.ChannelAuth.ATTESTED, true, id -> {
            try {
                return new TlsWebSocketTransport(id);
            } catch (GeneralSecurityException e) {
                throw new IllegalStateException(e);
            }
        });
        assertThat(peers.get(0).node().bareFramesSent() + peers.get(1).node().bareFramesSent())
                .as("frames travel bare on the attested channel").isPositive();
    }

    @Test
    @Timeout(120)
    void sessionModeFleetOverTheHttpExchange() throws Exception {
        fleet("http", PeerNode.ChannelAuth.SESSION, false,
                id -> new HttpExchangeTransport().options(HttpOptions.defaults().withLongPoll(Duration.ofSeconds(2))));
        assertThat(peers.get(0).node().sessionFramesSent() + peers.get(1).node().sessionFramesSent()).isPositive();
    }

    @Test
    void aNodeRequiringAttestationRefusesThePlaintextBindings() {
        PeerNode node = PeerNode.builder(PeerIdentity.generate())
                .channelAuth(PeerNode.ChannelAuth.ATTESTED).requireAttestation(true).build();
        try {
            assertThatThrownBy(() -> node.transport(new WebSocketTransport()))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("ws");
            assertThatThrownBy(() -> node.transport(new HttpExchangeTransport()))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("http");
        } finally {
            node.close();
        }
    }

    private void fleet(String scheme, PeerNode.ChannelAuth auth, boolean require,
                       Function<PeerIdentity, Transport> binding) throws Exception {
        int seedPort = Wire.freePort();
        Peer first = startPeer(scheme, seedPort, 0, auth, require, binding);
        Peer second = startPeer(scheme, Wire.freePort(), seedPort, auth, require, binding);

        first.space().write(new Job("job-1", "compress the archives"), Lease.of(Duration.ofMinutes(10)));
        Optional<TakenEntry<Job>> taken = second.space().take(Template.of(Job.class),
                Lease.of(Duration.ofMinutes(5)), Duration.ofSeconds(30));
        assertThat(taken).isPresent();
        assertThat(taken.get().entry().jobId()).isEqualTo("job-1");

        second.space().complete(taken.get(), new Done("job-1", "second"), Lease.of(Duration.ofMinutes(10)));
        Optional<Done> done = awaitDone(first, Duration.ofSeconds(30));
        assertThat(done).isPresent();
        assertThat(done.get().by()).isEqualTo("second");

        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline && (memberIds(first).isEmpty() || memberIds(second).isEmpty())) {
            Thread.sleep(250);
        }
        assertThat(memberIds(first)).contains(second.identity().peerId());
        assertThat(memberIds(second)).contains(first.identity().peerId());
    }

    private static List<PeerId> memberIds(Peer peer) {
        return peer.runtime().membership().allMembers().stream().map(GroupMembership.Member::id).toList();
    }

    private static Optional<Done> awaitDone(Peer peer, Duration timeout) throws InterruptedException {
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
        GroupId groupId = GroupId.fromFounding("http-fleet-test-v1".getBytes(StandardCharsets.UTF_8));
        return new GroupAdvertisement("aspace://" + groupId.value(), PeerIdentity.generate().peerId(), groupId,
                Instant.EPOCH, Duration.ofDays(365), "http-fleet-test", GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
    }

    private Peer startPeer(String scheme, int port, int seedPort, PeerNode.ChannelAuth auth, boolean require,
                           Function<PeerIdentity, Transport> binding) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).channelAuth(auth).requireAttestation(require).build();
        Transport transport = binding.apply(identity);
        node.listen(transport, "127.0.0.1:" + port);
        List<PeerAdvertisement.Endpoint> seeds = seedPort == 0 ? List.of()
                : List.of(new PeerAdvertisement.Endpoint(scheme, "127.0.0.1:" + seedPort, 0));
        GroupRuntime runtime = node.joinGroup(group(),
                new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2), seeds);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "jobs", identity, "host")
                .settleWindow(Duration.ofMillis(150)).build();
        node.startTicking(Duration.ofMillis(250));
        Peer peer = new Peer(node, identity, runtime, space, (AutoCloseable) transport);
        peers.add(peer);
        return peer;
    }
}
