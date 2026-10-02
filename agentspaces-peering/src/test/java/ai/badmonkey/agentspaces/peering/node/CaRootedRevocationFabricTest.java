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
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.ChannelTrust;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.identity.ServingCredential;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.membership.RevocationRegistry.RevocationValidator;
import ai.badmonkey.agentspaces.peering.transport.TlsTcpTransport;
import ai.badmonkey.agentspaces.test.TestCa;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.security.cert.CRLReason;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SPEC §5.6 v0.1.13 (TODO-9-10-11 C6, item 9) over real TLS: the CA revokes a
 * member's leaf; the node whose trust refreshes with the CRL re-judges its live
 * connection, roots a peer revocation in the CA with the chain and CRL as
 * evidence, and gossips it; a third member that holds no CRL of its own accepts
 * it on the evidence alone and evicts the revoked peer.
 */
class CaRootedRevocationFabricTest {

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
    void aCrlRefreshRootsAPeerRevocationThatAMemberWithoutTheCrlAccepts() throws Exception {
        GroupId groupId = GroupId.fromFounding("ca-rooted".getBytes(StandardCharsets.UTF_8));
        GroupAdvertisement groupAd = new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
                "ca-rooted", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
        GroupMembership.Config slow = new GroupMembership.Config(Duration.ofMinutes(10), Duration.ofMinutes(5), 1);
        ChannelTrust anchorsOnly = ChannelTrust.of(List.of(ca.certificate()));

        PeerIdentity observerId = PeerIdentity.generate();
        PeerIdentity victimId = PeerIdentity.generate();
        PeerIdentity bystanderId = PeerIdentity.generate();
        TestCa.Issued observerLeaf = ca.issue(observerId.rawPublicKey());
        TestCa.Issued victimLeaf = ca.issue(victimId.rawPublicKey());
        TestCa.Issued bystanderLeaf = ca.issue(bystanderId.rawPublicKey());
        AtomicReference<ChannelTrust> observerTrust = new AtomicReference<>(anchorsOnly);

        PeerNode observer = PeerNode.builder(observerId).requireAttestation(true)
                .channelTrust(observerTrust::get)
                .revocationValidator(RevocationValidator.anyOf(RevocationValidator.founderRooted(),
                        RevocationValidator.caRooted(observerTrust::get)))
                .build();
        PeerNode victim = PeerNode.builder(victimId).requireAttestation(true).build();
        PeerNode bystander = PeerNode.builder(bystanderId).requireAttestation(true)
                .revocationValidator(RevocationValidator.anyOf(RevocationValidator.founderRooted(),
                        RevocationValidator.caRooted(() -> anchorsOnly)))
                .build();
        List<PeerNode> nodes = List.of(observer, victim, bystander);
        try {
            int observerPort = freePort();
            observer.listen(TlsTcpTransport.withRefreshableTrust(observerId,
                    new ServingCredential(observerLeaf.key().getPrivate(), observerLeaf.chain(ca)),
                    observerTrust::get), "127.0.0.1:" + observerPort);
            victim.listen(new TlsTcpTransport(victimId,
                    new ServingCredential(victimLeaf.key().getPrivate(), victimLeaf.chain(ca)), anchorsOnly),
                    "127.0.0.1:" + freePort());
            bystander.listen(new TlsTcpTransport(bystanderId,
                    new ServingCredential(bystanderLeaf.key().getPrivate(), bystanderLeaf.chain(ca)), anchorsOnly),
                    "127.0.0.1:" + freePort());
            List<PeerAdvertisement.Endpoint> seed =
                    List.of(new PeerAdvertisement.Endpoint("tls", "127.0.0.1:" + observerPort, 0));
            GroupRuntime atObserver = observer.joinGroup(groupAd, slow, List.of());
            victim.joinGroup(groupAd, slow, seed);
            GroupRuntime atBystander = bystander.joinGroup(groupAd, slow, seed);
            for (PeerNode node : nodes) {
                node.startTicking(Duration.ofMillis(200));
            }
            await("the fleet to converge", () -> atObserver.membership().member(victimId.peerId()).isPresent()
                    && atObserver.membership().member(bystanderId.peerId()).isPresent()
                    && atBystander.membership().member(observerId.peerId()).isPresent());

            // The CA revokes the victim's leaf; only the observer's trust learns the CRL.
            observerTrust.set(ChannelTrust.withCrls(List.of(ca.certificate()),
                    List.of(ca.crl(Instant.now().minusSeconds(1), Instant.now().plus(Duration.ofDays(1)),
                            CRLReason.KEY_COMPROMISE, victimLeaf.certificate()))));

            await("the observer to root the revocation in the CA",
                    () -> atObserver.revocations().revoked(victimId.peerId()));
            await("the bystander to accept it on the evidence alone",
                    () -> atBystander.revocations().revoked(victimId.peerId()));
            await("both to evict the victim", () -> atObserver.membership().member(victimId.peerId()).isEmpty()
                    && atBystander.membership().member(victimId.peerId()).isEmpty());
            assertThat(atBystander.revocations().revoked(observerId.peerId())).isFalse();
            assertThat(atBystander.membership().member(observerId.peerId())).isPresent();
        } finally {
            new ArrayList<>(nodes).forEach(PeerNode::close);
        }
    }

    /** SPEC §5.6 v0.1.13: the founder's X509_LEAF revocation cuts every link presenting that leaf, and keeps cutting. */
    @Test
    @Timeout(90)
    void aRevokedLeafCannotCarryFramesEvenAfterRedialing() throws Exception {
        PeerIdentity founderId = PeerIdentity.generate();
        PeerIdentity memberId = PeerIdentity.generate();
        GroupId groupId = GroupId.fromFounding("leaf-revoked".getBytes(StandardCharsets.UTF_8));
        GroupAdvertisement groupAd = new GroupAdvertisement("aspace://" + groupId.value(), founderId.peerId(),
                groupId, Instant.EPOCH, Duration.ofDays(1), "leaf", GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
        GroupMembership.Config slow = new GroupMembership.Config(Duration.ofMinutes(10), Duration.ofMinutes(5), 1);
        ChannelTrust anchorsOnly = ChannelTrust.of(List.of(ca.certificate()));
        TestCa.Issued founderLeaf = ca.issue(founderId.rawPublicKey());
        TestCa.Issued memberLeaf = ca.issue(memberId.rawPublicKey());
        PeerNode founder = PeerNode.builder(founderId).requireAttestation(true).build();
        PeerNode member = PeerNode.builder(memberId).requireAttestation(true).build();
        try {
            int founderPort = freePort();
            founder.listen(new TlsTcpTransport(founderId,
                    new ServingCredential(founderLeaf.key().getPrivate(), founderLeaf.chain(ca)), anchorsOnly),
                    "127.0.0.1:" + founderPort);
            member.listen(new TlsTcpTransport(memberId,
                    new ServingCredential(memberLeaf.key().getPrivate(), memberLeaf.chain(ca)), anchorsOnly),
                    "127.0.0.1:" + freePort());
            GroupRuntime atFounder = founder.joinGroup(groupAd, slow, List.of());
            GroupRuntime atMember = member.joinGroup(groupAd, slow,
                    List.of(new PeerAdvertisement.Endpoint("tls", "127.0.0.1:" + founderPort, 0)));
            founder.startTicking(Duration.ofMillis(200));
            member.startTicking(Duration.ofMillis(200));
            await("membership", () -> atFounder.membership().member(memberId.peerId()).isPresent()
                    && atMember.membership().member(founderId.peerId()).isPresent());
            List<String> news = new java.util.concurrent.CopyOnWriteArrayList<>();
            atFounder.gossip().onStream("news", (from, id, payload) ->
                    news.add(new String(payload, StandardCharsets.UTF_8)));
            atMember.gossip().publish("news", "n1", "before".getBytes(StandardCharsets.UTF_8));
            await("a frame before the revocation", () -> news.contains("before"));

            byte[] fingerprint = ai.badmonkey.agentspaces.common.crypto.Digests.sha256(
                    memberLeaf.certificate().getEncoded());
            assertThat(atFounder.revoke(ai.badmonkey.agentspaces.api.ad.CredentialRevocation.Target.x509Leaf(
                    memberLeaf.certificate().getIssuerX500Principal().getName(),
                    memberLeaf.certificate().getSerialNumber().toString(16), fingerprint),
                    ai.badmonkey.agentspaces.api.ad.CredentialRevocation.KEY_COMPROMISE, null)).isPresent();
            Thread.sleep(1000);
            atMember.gossip().publish("news", "n2", "after".getBytes(StandardCharsets.UTF_8));
            Thread.sleep(3000); // the member redials meanwhile; each new link is cut on attach
            assertThat(news).containsExactly("before");
        } finally {
            founder.close();
            member.close();
        }
    }
}
