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

package ai.badmonkey.agentspaces.transport.dtls;

import ai.badmonkey.agentspaces.identity.ServingCredential;
import org.bouncycastle.tls.Certificate;
import org.bouncycastle.tls.CertificateRequest;
import org.bouncycastle.tls.ClientCertificateType;
import org.bouncycastle.tls.DefaultTlsClient;
import org.bouncycastle.tls.HeartbeatMode;
import org.bouncycastle.tls.ProtocolVersion;
import org.bouncycastle.tls.RenegotiationPolicy;
import org.bouncycastle.tls.TlsAuthentication;
import org.bouncycastle.tls.TlsCredentials;
import org.bouncycastle.tls.TlsHeartbeat;
import org.bouncycastle.tls.TlsServerCertificate;
import org.bouncycastle.tls.TlsSession;
import org.bouncycastle.tls.crypto.impl.jcajce.JcaTlsCrypto;

/**
 * The dialer's side of the handshake: BouncyCastle's {@link DefaultTlsClient}
 * under the SRAD's policy (§3.3). It offers the configured versions and the
 * AEAD-ECDHE suites, presents the channel credential when asked, offers a
 * zero-length Connection ID (it needs none, its socket is its own, but the
 * listener may issue one), resumes nothing, and refuses renegotiation and
 * Heartbeat. It records the listener's chain and accepts it: attestation runs
 * after the handshake.
 */
final class AspaceDtlsClient extends DefaultTlsClient {

    private final JcaTlsCrypto crypto;
    private final ServingCredential credential;
    private final Certificate certificate;
    private final ProtocolVersion[] versions;
    private final boolean connectionId;
    private final int handshakeTimeoutMillis;
    private volatile Certificate presented;

    AspaceDtlsClient(JcaTlsCrypto crypto, ServingCredential credential, Certificate certificate,
                     ProtocolVersion[] versions, boolean connectionId, int handshakeTimeoutMillis) {
        super(crypto);
        this.crypto = crypto;
        this.credential = credential;
        this.certificate = certificate;
        this.versions = versions;
        this.connectionId = connectionId;
        this.handshakeTimeoutMillis = handshakeTimeoutMillis;
    }

    /** The chain the listener presented; null before the handshake reaches it. */
    Certificate presented() {
        return presented;
    }

    @Override
    protected ProtocolVersion[] getSupportedVersions() {
        return versions;
    }

    @Override
    protected int[] getSupportedCipherSuites() {
        return org.bouncycastle.tls.TlsUtils.getSupportedCipherSuites(crypto, BcSupport.ALL_SUITES);
    }

    @Override
    public TlsAuthentication getAuthentication() {
        return new TlsAuthentication() {
            @Override
            public void notifyServerCertificate(TlsServerCertificate serverCertificate) {
                presented = serverCertificate.getCertificate();
            }

            @Override
            public TlsCredentials getClientCredentials(CertificateRequest request) throws java.io.IOException {
                short wanted = BcSupport.isRsa(credential.key())
                        ? ClientCertificateType.rsa_sign : ClientCertificateType.ecdsa_sign;
                for (short type : request.getCertificateTypes()) {
                    if (type == wanted) {
                        return BcSupport.signer(context, crypto, credential, certificate,
                                request.getSupportedSignatureAlgorithms());
                    }
                }
                return null; // connect unattested rather than fail
            }
        };
    }

    @Override
    protected byte[] getNewConnectionID() {
        return connectionId ? new byte[0] : null;
    }

    @Override
    public TlsSession getSessionToResume() {
        return null; // ASF-021
    }

    @Override
    public boolean requiresExtendedMasterSecret() {
        return true;
    }

    @Override
    public int getRenegotiationPolicy() {
        return RenegotiationPolicy.DENY;
    }

    @Override
    public TlsHeartbeat getHeartbeat() {
        return null;
    }

    @Override
    public short getHeartbeatPolicy() {
        return HeartbeatMode.peer_not_allowed_to_send;
    }

    @Override
    public boolean shouldCheckSigAlgOfPeerCerts() {
        return false; // DT-A-07
    }

    @Override
    public int getHandshakeTimeoutMillis() {
        return handshakeTimeoutMillis;
    }

    @Override
    public int getMaxCertificateChainLength() {
        return BcSupport.MAX_CHAIN;
    }
}
