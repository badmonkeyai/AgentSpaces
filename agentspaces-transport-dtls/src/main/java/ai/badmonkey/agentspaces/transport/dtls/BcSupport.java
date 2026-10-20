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
import org.bouncycastle.tls.CipherSuite;
import org.bouncycastle.tls.ProtocolVersion;
import org.bouncycastle.tls.SignatureAlgorithm;
import org.bouncycastle.tls.SignatureAndHashAlgorithm;
import org.bouncycastle.tls.TlsContext;
import org.bouncycastle.tls.TlsCredentialedSigner;
import org.bouncycastle.tls.TlsUtils;
import org.bouncycastle.tls.crypto.TlsCertificate;
import org.bouncycastle.tls.crypto.TlsCrypto;
import org.bouncycastle.tls.crypto.TlsCryptoParameters;
import org.bouncycastle.tls.crypto.impl.jcajce.JcaDefaultTlsCredentialedSigner;
import org.bouncycastle.tls.crypto.impl.jcajce.JcaTlsCrypto;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Vector;

/**
 * The policy and the conversions the BouncyCastle client and server share:
 * the AEAD-ECDHE cipher suites (SRAD DT-Q-SEC-02), the protocol versions,
 * the signer that presents a {@link ServingCredential}, and the conversion of
 * presented certificates to {@link X509Certificate}s for attestation. No
 * certificate parsing of its own: the JDK's {@link CertificateFactory} reads
 * every encoding (DT-A-06).
 */
final class BcSupport {

    static final int[] ECDSA_SUITES = {
            CipherSuite.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
            CipherSuite.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
            CipherSuite.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
    };

    static final int[] RSA_SUITES = {
            CipherSuite.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
            CipherSuite.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
            CipherSuite.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
    };

    static final int[] ALL_SUITES = {
            CipherSuite.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
            CipherSuite.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
            CipherSuite.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
            CipherSuite.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
            CipherSuite.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
            CipherSuite.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
    };

    /** Longest chain a peer may present; a channel certificate is one, a CA chain a handful. */
    static final int MAX_CHAIN = 8;

    private BcSupport() {
    }

    static ProtocolVersion[] versions(List<String> names) {
        ProtocolVersion[] versions = new ProtocolVersion[names.size()];
        for (int i = 0; i < names.size(); i++) {
            versions[i] = switch (names.get(i)) {
                case "DTLSv1.2" -> ProtocolVersion.DTLSv12;
                case "DTLSv1.3" -> ProtocolVersion.DTLSv13;
                default -> throw new IllegalArgumentException("unknown DTLS version " + names.get(i));
            };
        }
        return versions;
    }

    static int[] suitesFor(TlsCrypto crypto, PrivateKey key) {
        int[] suites = TlsUtils.getSupportedCipherSuites(crypto, isRsa(key) ? RSA_SUITES : ECDSA_SUITES);
        if (suites.length == 0) {
            throw new IllegalStateException("the crypto provider supports none of the AEAD-ECDHE suites");
        }
        return suites;
    }

    static boolean isRsa(PrivateKey key) {
        return key.getAlgorithm().startsWith("RSA");
    }

    static Certificate certificate(TlsCrypto crypto, X509Certificate[] chain) throws IOException {
        TlsCertificate[] list = new TlsCertificate[chain.length];
        try {
            for (int i = 0; i < chain.length; i++) {
                list[i] = crypto.createCertificate(chain[i].getEncoded());
            }
        } catch (java.security.cert.CertificateEncodingException e) {
            throw new IOException("cannot encode the channel credential", e);
        }
        return new Certificate(list);
    }

    /** The presented chain, leaf first; empty when none was presented or it does not parse. */
    static X509Certificate[] x509(Certificate presented) {
        if (presented == null || presented.isEmpty()) {
            return new X509Certificate[0];
        }
        try {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            X509Certificate[] chain = new X509Certificate[presented.getLength()];
            for (int i = 0; i < chain.length; i++) {
                chain[i] = (X509Certificate) factory.generateCertificate(
                        new ByteArrayInputStream(presented.getCertificateAt(i).getEncoded()));
            }
            return chain;
        } catch (CertificateException | IOException | RuntimeException e) {
            return new X509Certificate[0];
        }
    }

    /** A signer that presents the credential, with a signature scheme the peer offered. */
    @SuppressWarnings("rawtypes")
    static TlsCredentialedSigner signer(TlsContext context, JcaTlsCrypto crypto, ServingCredential credential,
                                        Certificate certificate, Vector peerSigAlgs)
            throws IOException {
        short algorithm = isRsa(credential.key()) ? SignatureAlgorithm.rsa : SignatureAlgorithm.ecdsa;
        SignatureAndHashAlgorithm chosen = TlsUtils.chooseSignatureAndHashAlgorithm(context, peerSigAlgs, algorithm);
        return new JcaDefaultTlsCredentialedSigner(new TlsCryptoParameters(context), crypto,
                credential.key(), certificate, chosen);
    }

    /** {@code host:port}, as the TCP binding parses it (SRAD DT-B-01). */
    static InetSocketAddress parse(String address) {
        int colon = address.lastIndexOf(':');
        if (colon <= 0) {
            throw new IllegalArgumentException("expected host:port, got: " + address);
        }
        String host = address.substring(0, colon);
        if (host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        }
        return new InetSocketAddress(host, Integer.parseInt(address.substring(colon + 1)));
    }

    /**
     * A dialer's socket. Bound to no one address, so it outlives a change of
     * this host's address (DT-K-05), except toward a loopback peer, where it
     * binds the loopback address: macOS gives a wildcard socket an ephemeral
     * port that a socket bound to {@code 127.0.0.1} already holds, and every
     * datagram for that port then reaches the other socket. A dial to a
     * listener holding the port talks to the listener itself and times out.
     */
    static DatagramSocket dialSocket(InetSocketAddress remote) throws SocketException {
        return remote.getAddress().isLoopbackAddress()
                ? new DatagramSocket(new InetSocketAddress(remote.getAddress(), 0))
                : new DatagramSocket();
    }
}
