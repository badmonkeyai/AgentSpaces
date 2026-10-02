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

import ai.badmonkey.agentspaces.common.codec.Multibase;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.id.PeerId;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;

/**
 * The identity-endorsed TLS certificate for channel authentication (spec §5.6).
 * The certificate carries a fresh EC P-256 key for the TLS handshake, and its
 * X.509 signature is the peer's Ed25519 identity signature over the
 * TBSCertificate, with the raw identity key published in the subject CN. The
 * chain of proof at connection time is therefore: the TLS handshake (RFC 8446)
 * proves the remote end holds the P-256 key in the certificate, and the
 * certificate's Ed25519 signature proves the identity endorsed exactly that
 * P-256 key. Verifying both binds the channel to the identity's PeerID, with
 * one identity signature per certificate lifetime instead of one per frame.
 *
 * <p>The identity's private key never enters the TLS machinery: the handshake
 * signs with the throwaway P-256 key, and {@link PeerIdentity#sign} produces
 * the endorsement once, at generation. No CA chain is involved anywhere; the
 * only trust decision a verifier makes is the PeerID pin in {@link #attest},
 * which follows the raw-public-key trust model (RFC 7250) with a certificate
 * as the carrier.
 *
 * <p>Key generation and Ed25519 stay plain JDK; the certificate itself is
 * assembled by BouncyCastle's {@code X509v3CertificateBuilder} (remediation
 * plan WS4 / ASF-023: a maintained library carries the DER, not hand-rolled
 * TLV code), with the identity's Ed25519 endorsement supplied as the content
 * signer so the trust design is unchanged.
 *
 * @param tlsKey      the P-256 private key the TLS handshake signs with
 * @param certificate the identity-endorsed certificate presenting that key
 */
public record ChannelCertificate(PrivateKey tlsKey, X509Certificate certificate) {

    /** Ed25519 signature algorithm, id-Ed25519 1.3.101.112 (RFC 8410). */
    public static final String ED25519_SIG_OID = "1.3.101.112";

    /**
     * Generates a fresh P-256 TLS key and the identity-endorsed certificate
     * presenting it, valid for one year.
     *
     * @param identity the endorsing peer identity
     * @return the channel certificate
     * @throws GeneralSecurityException when the JDK cannot generate or parse
     */
    public static ChannelCertificate generate(PeerIdentity identity)
            throws GeneralSecurityException {
        Objects.requireNonNull(identity, "identity");
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"));
        KeyPair tlsPair = generator.generateKeyPair();

        byte[] serial = new byte[9];
        new SecureRandom().nextBytes(serial);

        Instant now = Instant.now().minus(1, ChronoUnit.HOURS);
        X500Name name = new X500Name(
                "CN=" + Multibase.base58btc(identity.rawPublicKey()));
        X509v3CertificateBuilder builder = new X509v3CertificateBuilder(
                name, new BigInteger(1, serial),
                Date.from(now), Date.from(now.plus(365, ChronoUnit.DAYS)),
                name,
                SubjectPublicKeyInfo.getInstance(tlsPair.getPublic().getEncoded()));
        try {
            byte[] encoded = builder.build(identitySigner(identity)).getEncoded();
            X509Certificate parsed = (X509Certificate) CertificateFactory
                    .getInstance("X.509")
                    .generateCertificate(new ByteArrayInputStream(encoded));
            return new ChannelCertificate(tlsPair.getPrivate(), parsed);
        } catch (IOException e) {
            throw new GeneralSecurityException("certificate assembly failed", e);
        }
    }

    /**
     * Decodes the raw Ed25519 identity key a certificate's subject CN carries,
     * bounded before decoding: a 32-byte key is ~44 Base58 characters, so a
     * multi-megabyte CN cannot burn CPU on the handshake thread (ASF-023).
     *
     * @param certificate the certificate whose CN names the identity
     * @return the raw key, or {@code null} when the CN is absent or malformed
     */
    static byte[] identityKeyFromCn(X509Certificate certificate) {
        String dn = certificate.getSubjectX500Principal().getName();
        if (!dn.startsWith("CN=") || dn.length() > 3 + 64) {
            return null;
        }
        byte[] raw;
        try {
            raw = Multibase.decode(dn.substring(3));
        } catch (RuntimeException e) {
            return null;
        }
        return raw.length == Ed25519.RAW_PUBLIC_KEY_LENGTH ? raw : null;
    }

    /**
     * A BouncyCastle content signer backed by the peer identity's Ed25519 key:
     * the builder feeds it the TBSCertificate bytes and receives the identity
     * endorsement, so the identity's private key never leaves
     * {@link PeerIdentity#sign}.
     */
    private static ContentSigner identitySigner(PeerIdentity identity) {
        ByteArrayOutputStream tbs = new ByteArrayOutputStream();
        // RFC 8410 §3: the Ed25519 AlgorithmIdentifier carries no parameters.
        AlgorithmIdentifier ed25519 =
                new AlgorithmIdentifier(new ASN1ObjectIdentifier(ED25519_SIG_OID));
        return new ContentSigner() {
            @Override
            public AlgorithmIdentifier getAlgorithmIdentifier() {
                return ed25519;
            }

            @Override
            public OutputStream getOutputStream() {
                return tbs;
            }

            @Override
            public byte[] getSignature() {
                return identity.sign(tbs.toByteArray());
            }
        };
    }

    /**
     * Verifies a presented channel certificate and returns the PeerID it binds
     * the channel to: the subject CN must decode to a raw Ed25519 key, and the
     * certificate's signature must be that key's valid Ed25519 signature over
     * the TBSCertificate. Any failure returns empty, and the caller treats the
     * connection as unauthenticated (signed frames only).
     *
     * @param presented the certificate the TLS handshake presented
     * @return the attested PeerID, or empty when the certificate proves nothing
     */
    public static Optional<PeerId> attest(X509Certificate presented) {
        if (presented == null || !ED25519_SIG_OID.equals(presented.getSigAlgOID())) {
            return Optional.empty();
        }
        byte[] raw = identityKeyFromCn(presented);
        if (raw == null) {
            return Optional.empty();
        }
        byte[] tbs;
        try {
            tbs = presented.getTBSCertificate();
        } catch (CertificateException e) {
            return Optional.empty();
        }
        boolean endorsed;
        try {
            endorsed = Ed25519.verify(
                    Ed25519.publicKeyFromRaw(raw), tbs, presented.getSignature());
        } catch (RuntimeException e) {
            return Optional.empty();
        }
        return endorsed ? Optional.of(PeerId.fromPublicKey(raw)) : Optional.empty();
    }
}
