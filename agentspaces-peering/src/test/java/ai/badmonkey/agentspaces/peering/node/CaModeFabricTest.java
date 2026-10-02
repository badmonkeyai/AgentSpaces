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
import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.ChannelTrust;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.identity.ServingCredential;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.transport.TlsTcpTransport;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.test.TestCa;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The enterprise-CA channel mode end to end (spec v0.1.9: the CA's revocation
 * is the authoritative eject): two nodes with CA-issued credentials converge
 * over real TLS with attestation required; when the server's trust learns a
 * CRL revoking the client's leaf, the client's next handshake attests nothing,
 * the server evicts it from the membership view, and its frames are refused.
 */
class CaModeFabricTest {

    private final TestCa ca = TestCa.create();

    private static int freePort() throws Exception {
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

    @Test
    @Timeout(90)
    void aCaRevokedMemberIsEvictedAndRefusedOnItsNextHandshake() throws Exception {
        GroupId groupId = GroupId.fromFounding("ca-fabric".getBytes(StandardCharsets.UTF_8));
        GroupAdvertisement groupAd = new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
                "ca-fabric", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
        // Long lease and probe timeouts: within this test only the attestation
        // path can evict, never SWIM's probe or the membership TTL.
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
        try {
            server.listen(TlsTcpTransport.withRefreshableTrust(serverId,
                    new ServingCredential(serverLeaf.key().getPrivate(), serverLeaf.chain(ca)),
                    serverTrust::get), "127.0.0.1:" + serverPort);
            client.listen(new TlsTcpTransport(clientId, clientCredential, noRevocations),
                    "127.0.0.1:" + clientPort);
            GroupRuntime atServer = server.joinGroup(groupAd, slow, List.of());
            GroupRuntime atClient = client.joinGroup(groupAd, slow,
                    List.of(new PeerAdvertisement.Endpoint("tls", "127.0.0.1:" + serverPort, 0)));
            server.startTicking(Duration.ofMillis(200));
            client.startTicking(Duration.ofMillis(200));
            await("membership to converge over attested channels", () ->
                    atServer.membership().member(clientId.peerId()).isPresent()
                            && atClient.membership().member(serverId.peerId()).isPresent());

            List<String> newsAtServer = new CopyOnWriteArrayList<>();
            atServer.gossip().onStream("news", (from, id, payload) ->
                    newsAtServer.add(new String(payload, StandardCharsets.UTF_8)));
            atClient.gossip().publish("news", "n1", "before".getBytes(StandardCharsets.UTF_8));
            await("a frame over the attested channel to land", () -> newsAtServer.contains("before"));

            // The CA revokes the client's leaf; the server's trust learns the CRL.
            serverTrust.set(ChannelTrust.withCrls(List.of(ca.certificate()),
                    List.of(ca.crl(clientLeaf.certificate()))));
            // The client reconnects with a fresh handshake (its process restarted, say).
            client.close();
            TlsTcpTransport revoked = new TlsTcpTransport(clientId, clientCredential, noRevocations);
            TransportConnection fresh = revoked.dial("127.0.0.1:" + serverPort);
            assertThat(fresh.attestedPeer()).as("the client still trusts the server").contains(serverId.peerId());
            try {
                InstantSource wall = InstantSource.system();
                fresh.send(TestFrames.signed(clientId, groupId, Envelope.Kind.RUMOR, serverId.peerId(),
                        wall, 0, new Bodies.Rumor("news", "n2", 3,
                                "after".getBytes(StandardCharsets.UTF_8))));

                await("the server to evict the CA-revoked member", () ->
                        atServer.membership().member(clientId.peerId()).isEmpty());
                assertThat(server.channelModes()).doesNotContainKey(clientId.peerId());

                // Its validly signed frames keep arriving on the unattested
                // channel and are refused; its self-introduction admits nothing.
                fresh.send(TestFrames.signed(clientId, groupId, Envelope.Kind.RUMOR, serverId.peerId(),
                        wall, 1, new Bodies.Rumor("news", "n3", 3,
                                "after-2".getBytes(StandardCharsets.UTF_8))));
                fresh.send(TestFrames.signed(clientId, groupId, Envelope.Kind.PING, serverId.peerId(),
                        wall, 2, new Bodies.Ping(7L)));
                Thread.sleep(500);
                assertThat(newsAtServer).containsExactly("before");
                assertThat(atServer.membership().member(clientId.peerId())).isEmpty();
            } finally {
                fresh.close();
            }
        } finally {
            server.close();
            client.close();
        }
    }
}
