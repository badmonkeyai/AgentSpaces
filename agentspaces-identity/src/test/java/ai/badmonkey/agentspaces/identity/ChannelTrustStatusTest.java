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
package ai.badmonkey.agentspaces.identity;

import ai.badmonkey.agentspaces.test.TestCa;
import org.junit.jupiter.api.Test;

import java.security.cert.CRLReason;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SPEC §5.6 v0.1.13 (TODO-9-10-11 §9.2): {@link ChannelTrust#status} judges a
 * chain at an instant and says why. Each CRL reason, a stale CRL, a CRL from
 * another CA, an evidence CRL supplied with the chain, validity windows, and
 * the online mode's use of cached CRLs.
 */
class ChannelTrustStatusTest {

    private final TestCa ca = TestCa.create();
    private final PeerIdentity peer = PeerIdentity.generate();
    private final TestCa.Issued leaf = ca.issue(peer.rawPublicKey());
    private final Instant now = Instant.now();

    private ChannelTrust trustWith(java.security.cert.X509CRL... crls) {
        return ChannelTrust.withCrls(List.of(ca.certificate()), List.of(crls));
    }

    @Test
    void aGoodChainIsGoodWithOrWithoutCrls() {
        assertThat(ChannelTrust.of(List.of(ca.certificate())).status(leaf.chain(ca), now))
                .isEqualTo(new TrustStatus.Good(peer.peerId()));
        assertThat(trustWith(ca.crl()).status(leaf.chain(ca), now))
                .isEqualTo(new TrustStatus.Good(peer.peerId()));
    }

    @Test
    void eachCrlReasonIsReportedAndOnlyIdentityWithdrawingOnesAuthorizeAPeerRevocation() {
        for (CRLReason reason : List.of(CRLReason.KEY_COMPROMISE, CRLReason.PRIVILEGE_WITHDRAWN,
                CRLReason.CESSATION_OF_OPERATION, CRLReason.SUPERSEDED, CRLReason.AFFILIATION_CHANGED,
                CRLReason.CERTIFICATE_HOLD)) {
            Instant revokedAt = now.minusSeconds(60);
            TrustStatus status = trustWith(ca.crl(revokedAt, now.plus(Duration.ofDays(1)), reason,
                    leaf.certificate())).status(leaf.chain(ca), now);
            assertThat(status).as(reason.name()).isInstanceOf(TrustStatus.Revoked.class);
            TrustStatus.Revoked revoked = (TrustStatus.Revoked) status;
            assertThat(revoked.peer()).isEqualTo(peer.peerId());
            assertThat(revoked.reason()).isEqualTo(reason);
            assertThat(revoked.revokedAt()).isEqualTo(revokedAt.truncatedTo(java.time.temporal.ChronoUnit.SECONDS));
            boolean authorizes = switch (reason) {
                case KEY_COMPROMISE, PRIVILEGE_WITHDRAWN, CESSATION_OF_OPERATION -> true;
                default -> false;
            };
            assertThat(revoked.authorizesPeerRevocation()).as(reason.name()).isEqualTo(authorizes);
        }
    }

    @Test
    void aStaleCrlLeavesTheStatusUndeterminedAndAttestsNothing() {
        ChannelTrust stale = trustWith(ca.crl(now.minus(Duration.ofDays(8)), now.minus(Duration.ofDays(1)),
                CRLReason.KEY_COMPROMISE));
        assertThat(stale.status(leaf.chain(ca), now)).isInstanceOf(TrustStatus.Undetermined.class);
        assertThat(stale.attest(leaf.chain(ca))).isEmpty();
    }

    @Test
    void anEvidenceCrlCountsOnlyWhenTheChainsIssuerSignedIt() {
        ChannelTrust noCrl = ChannelTrust.of(List.of(ca.certificate()));
        assertThat(noCrl.status(leaf.chain(ca), now, List.of(ca.crl(leaf.certificate()))))
                .isInstanceOf(TrustStatus.Revoked.class);
        TestCa impostor = TestCa.create("impostor-ca");
        assertThat(noCrl.status(leaf.chain(ca), now, List.of(impostor.crl(leaf.certificate()))))
                .as("a CRL another CA signed proves nothing about this leaf")
                .isNotInstanceOf(TrustStatus.Revoked.class);
    }

    @Test
    void validityIsJudgedAtTheInstantAsked() {
        TestCa.Issued shortLived = ca.issue(peer.rawPublicKey(), now.minus(Duration.ofHours(2)),
                now.minus(Duration.ofHours(1)));
        ChannelTrust trust = ChannelTrust.of(List.of(ca.certificate()));
        assertThat(trust.status(shortLived.chain(ca), now)).isInstanceOf(TrustStatus.Expired.class);
        assertThat(trust.status(shortLived.chain(ca), now.minus(Duration.ofMinutes(90))))
                .isEqualTo(new TrustStatus.Good(peer.peerId()));
        assertThat(trust.status(shortLived.chain(ca), now.minus(Duration.ofHours(3))))
                .isInstanceOf(TrustStatus.NotYetValid.class);
    }

    @Test
    void untrustedAndMalformedChainsSayWhy() {
        ChannelTrust otherTrust = ChannelTrust.of(List.of(TestCa.create("other-ca").certificate()));
        assertThat(otherTrust.status(leaf.chain(ca), now)).isInstanceOf(TrustStatus.Untrusted.class);
        assertThat(otherTrust.status(new java.security.cert.X509Certificate[0], now))
                .isInstanceOf(TrustStatus.Malformed.class);
    }

    @Test
    void theOnlineModeAlsoConsultsCachedCrls() {
        ChannelTrust online = trustWith(ca.crl(leaf.certificate())).strictOnline();
        assertThat(online.attest(leaf.chain(ca))).as("revoked by the cached CRL").isEmpty();
        assertThat(trustWith(ca.crl()).strictOnline().attest(leaf.chain(ca))).contains(peer.peerId());
    }

    @Test
    void theTrustNamesTheCrlRevokingALeaf() {
        java.security.cert.X509CRL revoking = ca.crl(leaf.certificate());
        assertThat(trustWith(ca.crl(), revoking).crlRevoking(leaf.chain(ca))).contains(revoking);
        assertThat(trustWith(ca.crl()).crlRevoking(leaf.chain(ca))).isEmpty();
    }
}
