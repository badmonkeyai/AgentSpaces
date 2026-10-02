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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Enterprise-CA attestation (plan §3/WS4): a chain attests exactly when it
 * validates to the trusted CA — and the CA's CRL is the authoritative eject: a
 * revoked leaf attests nothing on the very next handshake.
 */
class ChannelTrustTest {

    private final TestCa ca = TestCa.create();
    private final PeerIdentity peer = PeerIdentity.generate();
    private final TestCa.Issued leaf = ca.issue(peer.rawPublicKey());

    @Test
    void aCaIssuedChainAttestsTheCertifiedPeerId() {
        ChannelTrust trust = ChannelTrust.of(List.of(ca.certificate()));
        assertThat(trust.attest(leaf.chain(ca))).contains(peer.peerId());
    }

    @Test
    void aChainFromAnUnknownCaAttestsNothing() {
        TestCa other = TestCa.create();
        ChannelTrust trust = ChannelTrust.of(List.of(other.certificate()));
        assertThat(trust.attest(leaf.chain(ca))).isEmpty();
    }

    @Test
    void aSelfSignedIdentityEndorsedCertificateAttestsNothingUnderCaTrust()
            throws Exception {
        // The default-mode certificate is sound for self-signed attestation but
        // carries no CA endorsement; enterprise trust must refuse it.
        ChannelCertificate selfSigned = ChannelCertificate.generate(peer);
        ChannelTrust trust = ChannelTrust.of(List.of(ca.certificate()));
        assertThat(trust.attest(
                new java.security.cert.X509Certificate[]{selfSigned.certificate()}))
                .isEmpty();
    }

    @Test
    void aRevokedLeafAttestsNothingAndAnUnrevokedOneStillDoes() {
        PeerIdentity other = PeerIdentity.generate();
        TestCa.Issued otherLeaf = ca.issue(other.rawPublicKey());

        ChannelTrust trust = ChannelTrust.withCrls(List.of(ca.certificate()),
                List.of(ca.crl(leaf.certificate())));

        assertThat(trust.attest(leaf.chain(ca)))
                .as("the CA revoked this leaf; the eject is authoritative")
                .isEmpty();
        assertThat(trust.attest(otherLeaf.chain(ca)))
                .as("an unrevoked peer is unaffected by someone else's revocation")
                .contains(other.peerId());
    }
}
