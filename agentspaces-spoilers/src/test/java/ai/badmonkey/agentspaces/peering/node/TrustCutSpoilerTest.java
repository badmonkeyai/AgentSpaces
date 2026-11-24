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
import ai.badmonkey.agentspaces.measure.Kind;
import ai.badmonkey.agentspaces.measure.Measure;
import ai.badmonkey.agentspaces.measure.Measurement;
import ai.badmonkey.agentspaces.measure.Params;
import ai.badmonkey.agentspaces.measure.Wait;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.membership.RevocationRegistry.RevocationValidator;
import ai.badmonkey.agentspaces.peering.transport.TlsTcpTransport;
import ai.badmonkey.agentspaces.test.TestCa;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.security.cert.CRLReason;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * EL-SP-15 of srad/agentspaces-event-loop.md §5.1 over real TLS on the system timer: an idle node holds a chained
 * channel, and a CRL refresh revokes the peer's certificate. REPAIRED: the link is cut within 1 s, as a tick cut it
 * before (EL-Q-SEC-06). The node's trust supplier is one that is only read, as {@code ReloadingChannelTrust} is, so the
 * cut rides the trust participant's 1 s deadline (EL-L-14) and not a push; the group's probes are 10 minutes apart, so
 * nothing else wakes the node. {@code core.trust.cut-latency} is the time from the refresh to the cut, judged by the
 * store against its goal (1 s) and limit (2 s).
 */
class TrustCutSpoilerTest {

    private final TestCa ca = TestCa.create();

    private static int freePort() throws Exception {
        // below the ephemeral range, so no other process takes it before the test binds it (parallel forks)
        return ai.badmonkey.agentspaces.test.Ports.free();
    }

    @Test
    @Timeout(120)
    void elSp15ACrlRefreshCutsAnIdleNodesChainedChannelWithinOneSecond() throws Exception {
        GroupId groupId = GroupId.fromFounding("el-sp-15".getBytes(StandardCharsets.UTF_8));
        GroupAdvertisement groupAd = new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "el-sp-15",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
        GroupMembership.Config quiet = new GroupMembership.Config(Duration.ofMinutes(10), Duration.ofMinutes(5), 1);
        ChannelTrust anchorsOnly = ChannelTrust.of(List.of(ca.certificate()));
        PeerIdentity nodeId = PeerIdentity.generate();
        PeerIdentity peerId = PeerIdentity.generate();
        TestCa.Issued nodeLeaf = ca.issue(nodeId.rawPublicKey());
        TestCa.Issued peerLeaf = ca.issue(peerId.rawPublicKey());
        AtomicReference<ChannelTrust> trust = new AtomicReference<>(anchorsOnly);
        PeerNode node = PeerNode.builder(nodeId).requireAttestation(true).channelTrust(trust::get)
                .revocationValidator(RevocationValidator.anyOf(RevocationValidator.founderRooted(),
                        RevocationValidator.caRooted(trust::get)))
                .build();
        PeerNode peer = PeerNode.builder(peerId).requireAttestation(true).build();
        try {
            int port = freePort();
            node.listen(TlsTcpTransport.withRefreshableTrust(nodeId,
                    new ServingCredential(nodeLeaf.key().getPrivate(), nodeLeaf.chain(ca)), trust::get),
                    "127.0.0.1:" + port);
            peer.listen(new TlsTcpTransport(peerId,
                    new ServingCredential(peerLeaf.key().getPrivate(), peerLeaf.chain(ca)), anchorsOnly),
                    "127.0.0.1:" + freePort());
            GroupRuntime atNode = node.joinGroup(groupAd, quiet, List.of());
            peer.joinGroup(groupAd, quiet, List.of(new PeerAdvertisement.Endpoint("tls", "127.0.0.1:" + port, 0)));
            node.startTicking(Duration.ofMillis(250));
            peer.startTicking(Duration.ofMillis(250));
            Measure.await("core.spoiler.trust-link-up", Duration.ofSeconds(30),
                    () -> atNode.membership().member(peerId.peerId()).isPresent() && node.connectedTo(peerId.peerId()),
                    Wait.with().poll(Duration.ofMillis(20)).diagnostics(() -> "the chained channel to open"));
            PeerNode.LoopStats before = node.loopStats();

            // the CA revokes the peer's leaf; the node's trust reads the CRL on its next refresh
            trust.set(ChannelTrust.withCrls(List.of(ca.certificate()),
                    List.of(ca.crl(Instant.now().minusSeconds(1), Instant.now().plus(Duration.ofDays(1)),
                            CRLReason.KEY_COMPROMISE, peerLeaf.certificate()))));
            Measurement cut = Measure.await("core.trust.cut-latency", Duration.ofSeconds(30),
                    () -> !node.connectedTo(peerId.peerId()),
                    Wait.with().poll(Duration.ofMillis(5)).kind(Kind.PERF)
                            .params(Params.of("harness", "system", "transport", "tls"))
                            .diagnostics(() -> "the revoked peer's link to be cut"));
            Measure.await("core.spoiler.trust-revocation-rooted", Duration.ofSeconds(30),
                    () -> atNode.revocations().revoked(peerId.peerId()),
                    Wait.with().poll(Duration.ofMillis(20)).diagnostics(() -> "the revocation rooted in the CA"));
            PeerNode.LoopStats after = node.loopStats();
            System.out.printf("EL-SP-15: link cut %.0f ms after the CRL refresh; trust runs found by the backstop %d%n",
                    cut.value(), after.backstopFoundWork() - before.backstopFoundWork());
            assertThat(after.backstopFoundWork() - before.backstopFoundWork())
                    .as("the trust participant's own deadline cut it, not the backstop").isZero();
            assertThat(atNode.membership().member(peerId.peerId())).as("REPAIRED: the revoked peer is out").isEmpty();
        } finally {
            node.close();
            peer.close();
        }
    }
}
