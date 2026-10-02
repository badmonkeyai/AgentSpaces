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
package ai.badmonkey.agentspaces.peering.membership;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.RevocationAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.ChannelTrust;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.RevocationRegistry.RevocationValidator;
import ai.badmonkey.agentspaces.peering.membership.RevocationRegistry.SignedRevocation;
import ai.badmonkey.agentspaces.test.TestCa;
import org.junit.jupiter.api.Test;

import java.security.cert.CRLReason;
import java.security.cert.X509CRL;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SPEC §5.6 v0.1.13 (TODO-9-10-11 C6, item 9): the CA-rooted revocation
 * validator. Any member may relay the CA's word; it stands when the evidence
 * proves the revoked peer's leaf is revoked for a reason withdrawing the
 * identity, and never otherwise; and it never displaces the founder's record.
 */
class CaRootedRevocationTest {

    /** The system clock: the CA material is dated now. */
    private final java.time.InstantSource clock = java.time.InstantSource.system();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final TestCa ca = TestCa.create();
    private final PeerIdentity founder = PeerIdentity.generate();
    private final PeerIdentity relay = PeerIdentity.generate();
    private final PeerIdentity victim = PeerIdentity.generate();
    private final TestCa.Issued victimLeaf = ca.issue(victim.rawPublicKey());
    private final GroupId group = GroupId.of("zCaRooted");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zCaRooted",
            founder.peerId(), group, Instant.EPOCH, Duration.ofDays(1), "ca", GroupAdvertisement.MembershipPolicy.OPEN,
            ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
    /** A receiver with the anchors but no CRL of its own: it must rely on the evidence. */
    private final ChannelTrust anchorsOnly = ChannelTrust.of(List.of(ca.certificate()));

    private RevocationRegistry registry(ChannelTrust trust) {
        return new RevocationRegistry(groupAd, RevocationValidator.anyOf(RevocationValidator.founderRooted(),
                RevocationValidator.caRooted(() -> trust)), codec, clock, ad -> { });
    }

    private SignedRevocation caRevocation(PeerIdentity issuer, PeerId revoked, X509CRL crl, PeerId successor) {
        byte[] evidence = RevocationEvidence.of(victimLeaf.chain(ca), crl).encode(codec);
        RevocationAdvertisement ad = new RevocationAdvertisement("aspace://zCaRooted/revocation/" + revoked.value(),
                issuer.peerId(), group, Instant.now(), Duration.ofDays(30), revoked, "ca", successor, evidence);
        byte[] bytes = codec.toBytes(ad);
        return new SignedRevocation(bytes, issuer.rawPublicKey(), issuer.sign(bytes));
    }

    private X509CRL revoking(CRLReason reason) {
        return ca.crl(Instant.now().minusSeconds(5), Instant.now().plus(Duration.ofDays(1)), reason,
                victimLeaf.certificate());
    }

    @Test
    void aRelayedCaRevocationWithEvidenceIsAcceptedByAReceiverWithoutItsOwnCrl() {
        RevocationRegistry registry = registry(anchorsOnly);
        assertThat(registry.accept(caRevocation(relay, victim.peerId(), revoking(CRLReason.KEY_COMPROMISE), null)))
                .isPresent();
        assertThat(registry.revoked(victim.peerId())).isTrue();
    }

    @Test
    void evidenceThatDoesNotRevokeTheNamedPeerIsRefused() {
        RevocationRegistry registry = registry(anchorsOnly);
        assertThat(registry.accept(caRevocation(relay, victim.peerId(), ca.crl(), null)))
                .as("the leaf is still good").isEmpty();
        PeerId bystander = PeerIdentity.generate().peerId();
        assertThat(registry.accept(caRevocation(relay, bystander, revoking(CRLReason.KEY_COMPROMISE), null)))
                .as("the evidence revokes someone else").isEmpty();
        TestCa impostor = TestCa.create("impostor-ca");
        assertThat(registry.accept(caRevocation(relay, victim.peerId(),
                impostor.crl(victimLeaf.certificate()), null))).as("a CRL from another CA").isEmpty();
        assertThat(registry.accept(caRevocation(relay, victim.peerId(), revoking(CRLReason.SUPERSEDED), null)))
                .as("superseded is about the certificate, not the peer").isEmpty();
        assertThat(registry.accept(caRevocation(relay, victim.peerId(),
                revoking(CRLReason.KEY_COMPROMISE), PeerIdentity.generate().peerId())))
                .as("only the founder names a successor").isEmpty();
        assertThat(registry.revoked(victim.peerId())).isFalse();
    }

    @Test
    void aStaleEvidenceCrlProvesNothing() {
        X509CRL stale = ca.crl(Instant.now().minus(Duration.ofDays(8)), Instant.now().minus(Duration.ofDays(1)),
                CRLReason.KEY_COMPROMISE, victimLeaf.certificate());
        assertThat(registry(anchorsOnly).accept(caRevocation(relay, victim.peerId(), stale, null))).isEmpty();
    }

    @Test
    void aReceiverWithTheCrlAcceptsEvidenceThatCarriesOnlyTheChain() {
        ChannelTrust withCrl = ChannelTrust.withCrls(List.of(ca.certificate()),
                List.of(revoking(CRLReason.PRIVILEGE_WITHDRAWN)));
        assertThat(registry(withCrl).accept(caRevocation(relay, victim.peerId(), null, null))).isPresent();
        assertThat(registry(anchorsOnly).accept(caRevocation(relay, victim.peerId(), null, null)))
                .as("without the CRL anywhere, nothing proves it").isEmpty();
    }

    @Test
    void aCaRootedRecordNeverDisplacesTheFoundersRotation() {
        RevocationRegistry registry = registry(anchorsOnly);
        PeerId successor = PeerIdentity.generate().peerId();
        RevocationAdvertisement rotation = new RevocationAdvertisement(
                "aspace://zCaRooted/revocation/" + victim.peerId().value(), founder.peerId(), group,
                Instant.now(), Duration.ofDays(30), victim.peerId(), "rotation", successor);
        byte[] bytes = codec.toBytes(rotation);
        assertThat(registry.accept(new SignedRevocation(bytes, founder.rawPublicKey(), founder.sign(bytes))))
                .isPresent();
        registry.accept(caRevocation(relay, victim.peerId(), revoking(CRLReason.KEY_COMPROMISE), null));
        assertThat(registry.successorOf(victim.peerId())).contains(successor);
    }

    @Test
    void theFounderRootedDefaultIgnoresCaEvidence() {
        RevocationRegistry founderOnly = new RevocationRegistry(groupAd, RevocationValidator.founderRooted(),
                codec, clock, ad -> { });
        assertThat(founderOnly.accept(caRevocation(relay, victim.peerId(), revoking(CRLReason.KEY_COMPROMISE), null)))
                .isEmpty();
    }

    @Test
    void theEvidenceFieldLeavesFounderRevocationBytesUnchanged() {
        RevocationAdvertisement plain = new RevocationAdvertisement("aspace://x", founder.peerId(), group,
                Instant.EPOCH, Duration.ofDays(1), victim.peerId(), "r", null);
        assertThat(new String(codec.toBytes(plain), java.nio.charset.StandardCharsets.ISO_8859_1))
                .doesNotContain("evidence");
    }
}
