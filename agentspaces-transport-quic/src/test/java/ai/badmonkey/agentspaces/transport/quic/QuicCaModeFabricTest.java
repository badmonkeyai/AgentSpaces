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
import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.ChannelTrust;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.identity.ServingCredential;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;
import ai.badmonkey.agentspaces.test.TestCa;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TODO item 11 (QA2 M): the fabric-level ports of {@code TlsFabricTest} and
 * {@code CaModeFabricTest} over QUIC. Two attested nodes converge over bare
 * frames, and under {@code requireAttestation} a member whose CA leaf is
 * revoked is evicted on its next connection and its frames refused.
 */
class QuicCaModeFabricTest {

    private static final WireCodec WIRE = new WireCodec();

    private final TestCa ca = TestCa.create();

    @BeforeAll
    static void requireNative() {
        QuicNative.assumeAvailable();
    }

    private static int freePort() throws Exception {
        // A free TCP port is a good guess for a free UDP port on loopback.
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.sleep(50);
        }
    }

    private static GroupAdvertisement group(String name) {
        GroupId groupId = GroupId.fromFounding(name.getBytes(StandardCharsets.UTF_8));
        return new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
                name, GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
    }

    private static byte[] signed(PeerIdentity from, GroupId group, Envelope.Kind kind,
                                 PeerId to, int logical, Object body) {
        byte[] encoded = body instanceof byte[] raw ? raw
                : ai.badmonkey.agentspaces.common.codec.CborCodec.defaultCodec().toBytes(body);
        return WIRE.encode(new Envelope(WireCodec.WIRE_VERSION, group, kind, from.peerId(), to,
                new HlcTimestamp(System.currentTimeMillis(), logical, from.peerId().value()),
                encoded), from);
    }

    @Test
    @Timeout(90)
    void twoAttestedNodesConvergeOverBareFramesOnQuic() throws Exception {
        GroupAdvertisement groupAd = group("quic-attested-fabric");
        PeerIdentity identityA = PeerIdentity.generate();
        PeerIdentity identityB = PeerIdentity.generate();
        int portA = freePort();
        int portB = freePort();
        PeerNode a = PeerNode.builder(identityA).channelAuth(PeerNode.ChannelAuth.ATTESTED).build();
        PeerNode b = PeerNode.builder(identityB).channelAuth(PeerNode.ChannelAuth.ATTESTED).build();
        try (QuicTransport transportA = new QuicTransport(identityA);
             QuicTransport transportB = new QuicTransport(identityB)) {
            a.listen(transportA, "127.0.0.1:" + portA);
            b.listen(transportB, "127.0.0.1:" + portB);
            GroupMembership.Config config =
                    new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2);
            GroupRuntime runtimeA = a.joinGroup(groupAd, config, List.of());
            GroupRuntime runtimeB = b.joinGroup(groupAd, config,
                    List.of(new PeerAdvertisement.Endpoint("quic", "127.0.0.1:" + portA, 0)));
            a.startTicking(Duration.ofMillis(200));
            b.startTicking(Duration.ofMillis(200));

            await("membership and bare frames in both directions", () ->
                    runtimeA.membership().member(b.peerId()).isPresent()
                            && runtimeB.membership().member(a.peerId()).isPresent()
                            && a.bareFramesSent() > 0 && b.bareFramesSent() > 0);
            assertThat(a.channelModes()).containsEntry(b.peerId(), "attested");
            assertThat(b.channelModes()).containsEntry(a.peerId(), "attested");
        } finally {
            a.close();
            b.close();
        }
    }

    @Test
    @Timeout(120)
    void aCaRevokedMemberIsEvictedAndRefusedOnItsNextConnection() throws Exception {
        GroupAdvertisement groupAd = group("quic-ca-fabric");
        GroupId groupId = groupAd.group();
        // Long lease and probe timeouts: only the attestation path can evict here.
        GroupMembership.Config slow = new GroupMembership.Config(
                Duration.ofMinutes(10), Duration.ofMinutes(5), 1);

        PeerIdentity serverId = PeerIdentity.generate();
        PeerIdentity clientId = PeerIdentity.generate();
        TestCa.Issued serverLeaf = ca.issue(serverId.rawPublicKey());
        TestCa.Issued clientLeaf = ca.issue(clientId.rawPublicKey());
        ChannelTrust noRevocations = ChannelTrust.of(List.of(ca.certificate()));
        AtomicReference<ChannelTrust> serverTrust = new AtomicReference<>(noRevocations);
        ServingCredential clientCredential =
                new ServingCredential(clientLeaf.key().getPrivate(), clientLeaf.chain(ca));

        int serverPort = freePort();
        int clientPort = freePort();
        PeerNode server = PeerNode.builder(serverId).requireAttestation(true).build();
        PeerNode client = PeerNode.builder(clientId).requireAttestation(true).build();
        QuicTransport serverTransport = QuicTransport.withRefreshableTrust(serverId,
                new ServingCredential(serverLeaf.key().getPrivate(), serverLeaf.chain(ca)),
                serverTrust::get);
        QuicTransport clientTransport = new QuicTransport(clientId, clientCredential, noRevocations);
        QuicTransport revoked = new QuicTransport(clientId, clientCredential, noRevocations);
        try {
            server.listen(serverTransport, "127.0.0.1:" + serverPort);
            client.listen(clientTransport, "127.0.0.1:" + clientPort);
            GroupRuntime atServer = server.joinGroup(groupAd, slow, List.of());
            GroupRuntime atClient = client.joinGroup(groupAd, slow,
                    List.of(new PeerAdvertisement.Endpoint("quic", "127.0.0.1:" + serverPort, 0)));
            server.startTicking(Duration.ofMillis(200));
            client.startTicking(Duration.ofMillis(200));
            await("membership to converge over attested QUIC channels", () ->
                    atServer.membership().member(clientId.peerId()).isPresent()
                            && atClient.membership().member(serverId.peerId()).isPresent());

            List<String> newsAtServer = new CopyOnWriteArrayList<>();
            atServer.gossip().onStream("news", (from, id, payload) ->
                    newsAtServer.add(new String(payload, StandardCharsets.UTF_8)));
            atClient.gossip().publish("news", "n1", "before".getBytes(StandardCharsets.UTF_8));
            await("a frame over the attested channel to land", () -> newsAtServer.contains("before"));

            // The CA revokes the client's leaf; the server's trust learns the CRL,
            // and the client comes back on a fresh connection.
            serverTrust.set(ChannelTrust.withCrls(List.of(ca.certificate()),
                    List.of(ca.crl(clientLeaf.certificate()))));
            client.close();
            TransportConnection fresh = revoked.dial("127.0.0.1:" + serverPort);
            assertThat(fresh.attestedPeer()).as("the client still trusts the server")
                    .contains(serverId.peerId());
            try {
                fresh.send(signed(clientId, groupId, Envelope.Kind.RUMOR, serverId.peerId(), 0,
                        new Bodies.Rumor("news", "n2", 3, "after".getBytes(StandardCharsets.UTF_8))));
                await("the server to evict the CA-revoked member", () ->
                        atServer.membership().member(clientId.peerId()).isEmpty());
                assertThat(server.channelModes()).doesNotContainKey(clientId.peerId());

                fresh.send(signed(clientId, groupId, Envelope.Kind.RUMOR, serverId.peerId(), 1,
                        new Bodies.Rumor("news", "n3", 3, "after-2".getBytes(StandardCharsets.UTF_8))));
                fresh.send(signed(clientId, groupId, Envelope.Kind.PING, serverId.peerId(), 2,
                        new Bodies.Ping(7L)));
                Thread.sleep(500);
                assertThat(newsAtServer).containsExactly("before");
                assertThat(atServer.membership().member(clientId.peerId())).isEmpty();
            } finally {
                fresh.close();
            }
        } finally {
            server.close();
            client.close();
            serverTransport.close();
            clientTransport.close();
            revoked.close();
        }
    }
}
