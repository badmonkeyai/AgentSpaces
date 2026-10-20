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

import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.ChannelCertificate;
import ai.badmonkey.agentspaces.identity.ChannelTrust;

import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The one attestation call every attesting binding makes after its handshake
 * (spec §5.6): the identity-endorsed mode pins the leaf's PeerID through
 * {@link ChannelCertificate#attest}; the enterprise-CA mode validates the full
 * chain under the current {@link ChannelTrust}, read afresh on every call so a
 * refreshed CRL takes effect on the next handshake.
 */
public final class ChannelAttestation {

    private ChannelAttestation() {
    }

    /**
     * Attests a presented chain.
     *
     * @param chain the chain the handshake presented, leaf first; may be empty
     * @param trust the CA trust source, or null for the identity-endorsed mode
     * @return the attested PeerID, or empty when the chain proves nothing; a
     *         failure inside the trust is read as unattested, never thrown
     */
    public static Optional<PeerId> attest(X509Certificate[] chain, Supplier<ChannelTrust> trust) {
        if (chain == null || chain.length == 0) {
            return Optional.empty(); // no certificate presented: unattested
        }
        try {
            return trust != null ? trust.get().attest(chain) : ChannelCertificate.attest(chain[0]);
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    /**
     * Narrows a presented chain to X.509, leaf first.
     *
     * @param chain the presented certificates, possibly null
     * @return the X.509 chain, or empty when absent or any element is not X.509
     */
    public static X509Certificate[] x509(Certificate[] chain) {
        if (chain == null) {
            return new X509Certificate[0];
        }
        X509Certificate[] x509 = new X509Certificate[chain.length];
        for (int i = 0; i < chain.length; i++) {
            if (!(chain[i] instanceof X509Certificate certificate)) {
                return new X509Certificate[0];
            }
            x509[i] = certificate;
        }
        return x509;
    }
}
