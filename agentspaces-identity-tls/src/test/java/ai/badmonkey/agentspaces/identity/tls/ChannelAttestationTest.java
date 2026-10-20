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

package ai.badmonkey.agentspaces.identity.tls;

import ai.badmonkey.agentspaces.identity.ChannelCertificate;
import ai.badmonkey.agentspaces.identity.ChannelTrust;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import org.junit.jupiter.api.Test;

import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** The shared attestation call of every attesting binding (spec §5.6). */
class ChannelAttestationTest {

    @Test
    void identityEndorsedChainAttestsItsPeer() throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        ChannelCertificate channel = ChannelCertificate.generate(identity);
        assertThat(ChannelAttestation.attest(new X509Certificate[]{channel.certificate()}, null))
                .contains(identity.peerId());
    }

    @Test
    void anEmptyChainOrAFailingTrustIsUnattested() throws Exception {
        assertThat(ChannelAttestation.attest(new X509Certificate[0], null)).isEmpty();
        assertThat(ChannelAttestation.attest(null, null)).isEmpty();
        ChannelCertificate channel = ChannelCertificate.generate(PeerIdentity.generate());
        X509Certificate[] chain = {channel.certificate()};
        assertThat(ChannelAttestation.attest(chain, () -> {
            throw new IllegalStateException("trust store unreadable");
        })).isEmpty();
        // A self-endorsed certificate is no CA-issued chain.
        assertThat(ChannelAttestation.attest(chain, () -> ChannelTrust.of(List.of()))).isEmpty();
    }

    @Test
    void narrowsToX509() throws Exception {
        ChannelCertificate channel = ChannelCertificate.generate(PeerIdentity.generate());
        assertThat(ChannelAttestation.x509(new Certificate[]{channel.certificate()}))
                .containsExactly(channel.certificate());
        assertThat(ChannelAttestation.x509(null)).isEmpty();
        java.security.cert.Certificate notX509 = new java.security.cert.Certificate("other") {
            @Override public byte[] getEncoded() { return new byte[0]; }
            @Override public void verify(java.security.PublicKey key) { }
            @Override public void verify(java.security.PublicKey key, String sigProvider) { }
            @Override public String toString() { return "other"; }
            @Override public java.security.PublicKey getPublicKey() { return null; }
        };
        assertThat(ChannelAttestation.x509(new Certificate[]{notX509})).isEmpty();
    }

    @Test
    void managersServeTheCredentialAndDeferTrust() throws Exception {
        ChannelCertificate channel = ChannelCertificate.generate(PeerIdentity.generate());
        SingleKeyManager keys = new SingleKeyManager(new ai.badmonkey.agentspaces.identity.ServingCredential(
                channel.tlsKey(), new X509Certificate[]{channel.certificate()}));
        String alias = keys.chooseServerAlias("EC", null, null);
        assertThat(keys.getCertificateChain(alias)).containsExactly(channel.certificate());
        assertThat(keys.getPrivateKey(alias)).isEqualTo(channel.tlsKey());
        assertThat(keys.getClientAliases("EC", null)).containsExactly(alias);
        assertThat(keys.chooseClientAlias(new String[]{"EC"}, null, null)).isEqualTo(alias);
        assertThat(keys.chooseEngineClientAlias(new String[]{"EC"}, null, null)).isEqualTo(alias);
        assertThat(keys.chooseEngineServerAlias("EC", null, null)).isEqualTo(alias);
        assertThat(keys.getServerAliases("EC", null)).containsExactly(alias);
        AttestLaterTrustManager trust = new AttestLaterTrustManager();
        X509Certificate[] chain = {channel.certificate()};
        // Every overload accepts: the trust decision is attestation's, after the handshake.
        trust.checkServerTrusted(chain, "ECDHE_ECDSA");
        trust.checkServerTrusted(chain, "ECDHE_ECDSA", (java.net.Socket) null);
        trust.checkServerTrusted(chain, "ECDHE_ECDSA", (javax.net.ssl.SSLEngine) null);
        trust.checkClientTrusted(chain, "ECDHE_ECDSA");
        trust.checkClientTrusted(chain, "ECDHE_ECDSA", (java.net.Socket) null);
        trust.checkClientTrusted(chain, "ECDHE_ECDSA", (javax.net.ssl.SSLEngine) null);
        assertThat(trust.getAcceptedIssuers()).isEmpty();
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new SingleKeyManager(null))
                .isInstanceOf(NullPointerException.class);
    }
}
