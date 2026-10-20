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
import org.bouncycastle.tls.DefaultTlsServer;
import org.bouncycastle.tls.HeartbeatMode;
import org.bouncycastle.tls.ProtocolVersion;
import org.bouncycastle.tls.RenegotiationPolicy;
import org.bouncycastle.tls.TlsCredentialedSigner;
import org.bouncycastle.tls.TlsHeartbeat;
import org.bouncycastle.tls.TlsSession;
import org.bouncycastle.tls.TlsUtils;
import org.bouncycastle.tls.crypto.impl.jcajce.JcaTlsCrypto;

import java.util.function.Supplier;

/**
 * The listener's side of the handshake: BouncyCastle's {@link DefaultTlsServer}
 * under the SRAD's policy (§3.3). It presents the channel credential, requests
 * the dialer's without requiring it (an uncertified dialer connects
 * unattested), negotiates only the configured versions and the AEAD-ECDHE
 * suites, issues an 8-byte Connection ID when the dialer offers the extension,
 * and refuses resumption, renegotiation, and Heartbeat. It records the
 * presented chain and accepts it: attestation runs after the handshake.
 */
final class AspaceDtlsServer extends DefaultTlsServer {

    private final JcaTlsCrypto crypto;
    private final ServingCredential credential;
    private final Certificate certificate;
    private final ProtocolVersion[] versions;
    private final Supplier<byte[]> connectionIds;
    private volatile Certificate presented;

    AspaceDtlsServer(JcaTlsCrypto crypto, ServingCredential credential, Certificate certificate,
                     ProtocolVersion[] versions, Supplier<byte[]> connectionIds) {
        super(crypto);
        this.crypto = crypto;
        this.credential = credential;
        this.certificate = certificate;
        this.versions = versions;
        this.connectionIds = connectionIds;
    }

    /** The chain the dialer presented; empty when none. */
    Certificate presented() {
        return presented;
    }

    @Override
    protected ProtocolVersion[] getSupportedVersions() {
        return versions;
    }

    @Override
    protected int[] getSupportedCipherSuites() {
        return BcSupport.suitesFor(crypto, credential.key());
    }

    @Override
    protected TlsCredentialedSigner getECDSASignerCredentials() throws java.io.IOException {
        return BcSupport.isRsa(credential.key()) ? null : signer();
    }

    @Override
    protected TlsCredentialedSigner getRSASignerCredentials() throws java.io.IOException {
        return BcSupport.isRsa(credential.key()) ? signer() : null;
    }

    private TlsCredentialedSigner signer() throws java.io.IOException {
        return BcSupport.signer(context, crypto, credential, certificate,
                context.getSecurityParametersHandshake().getClientSigAlgs());
    }

    @Override
    public CertificateRequest getCertificateRequest() {
        return new CertificateRequest(new short[]{ClientCertificateType.ecdsa_sign, ClientCertificateType.rsa_sign},
                TlsUtils.getDefaultSupportedSignatureAlgorithms(context), null);
    }

    @Override
    public void notifyClientCertificate(Certificate clientCertificate) {
        this.presented = clientCertificate;
    }

    @Override
    protected byte[] getNewConnectionID() {
        return connectionIds == null ? null : connectionIds.get();
    }

    @Override
    public byte[] getNewSessionID() {
        return null; // no session to resume (ASF-021)
    }

    @Override
    public TlsSession getSessionToResume(byte[] sessionID) {
        return null;
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
        // The identity-endorsed certificate is Ed25519-signed; attestation judges it (DT-A-07).
        return false;
    }

    @Override
    public int getHandshakeTimeoutMillis() {
        return 60_000;
    }

    @Override
    public int getMaxCertificateChainLength() {
        return BcSupport.MAX_CHAIN;
    }
}
