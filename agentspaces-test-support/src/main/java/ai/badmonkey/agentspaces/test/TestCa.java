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
package ai.badmonkey.agentspaces.test;

import ai.badmonkey.agentspaces.common.codec.Multibase;
import ai.badmonkey.agentspaces.common.id.PeerId;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CRLHolder;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v2CRLBuilder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CRLConverter;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.CRLReason;
import java.security.cert.X509CRL;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

/**
 * A miniature certificate authority for tests of the enterprise-CA channel
 * mode: mints a self-signed CA, issues leaf certificates that carry a raw
 * Ed25519 identity key in the subject CN (the PeerID binding a real enrollment
 * pipeline would certify), and issues CRLs revoking chosen leaves.
 */
public final class TestCa {

    private final X500Name name;
    private final KeyPair keys;
    private final X509Certificate certificate;
    private final SecureRandom random = new SecureRandom();

    private TestCa(X500Name name, KeyPair keys, X509Certificate certificate) {
        this.name = name;
        this.keys = keys;
        this.certificate = certificate;
    }

    /** Mints a fresh CA. */
    public static TestCa create() {
        return create("agentspaces-test-ca");
    }

    /**
     * Mints a fresh CA under its own name, for tests needing a second, distinct
     * authority (v0.1.13).
     *
     * @param commonName the CA's CN
     * @return the CA
     */
    public static TestCa create(String commonName) {
        X500Name caName = new X500Name("CN=" + commonName);
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
            generator.initialize(new ECGenParameterSpec("secp256r1"));
            KeyPair keys = generator.generateKeyPair();
            Instant now = Instant.now().minus(Duration.ofHours(1));
            X509v3CertificateBuilder builder = new X509v3CertificateBuilder(
                    caName, BigInteger.valueOf(1),
                    Date.from(now), Date.from(now.plus(Duration.ofDays(30))),
                    caName,
                    SubjectPublicKeyInfo.getInstance(keys.getPublic().getEncoded()))
                    .addExtension(Extension.basicConstraints, true,
                            new BasicConstraints(true))
                    .addExtension(Extension.keyUsage, true,
                            new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));
            X509CertificateHolder holder = builder.build(signer(keys.getPrivate()));
            return new TestCa(caName, keys,
                    new JcaX509CertificateConverter().getCertificate(holder));
        } catch (Exception e) {
            throw new IllegalStateException("cannot mint the test CA", e);
        }
    }

    /** The CA certificate to anchor trust in. */
    public X509Certificate certificate() {
        return certificate;
    }

    /**
     * One issued credential: the leaf's key, its certificate, and the chain.
     *
     * @param key         the leaf's private key
     * @param certificate the CA-signed leaf
     */
    public record Issued(KeyPair key, X509Certificate certificate) {
        /** Leaf-then-CA is what a TLS peer presents. */
        public X509Certificate[] chain(TestCa ca) {
            return new X509Certificate[]{certificate, ca.certificate()};
        }
    }

    /**
     * Issues a leaf certificate binding the given identity key (its raw form
     * in the CN, the enrollment attribute) to a fresh EC TLS key.
     *
     * @param identityKeyRaw the raw Ed25519 identity key to certify
     * @return the issued credential
     */
    public Issued issue(byte[] identityKeyRaw) {
        Instant now = Instant.now().minus(Duration.ofHours(1));
        return issue(identityKeyRaw, now, now.plus(Duration.ofDays(7)));
    }

    /**
     * Issues a leaf valid over a chosen window (v0.1.13), for tests judging a
     * chain at an instant.
     *
     * @param identityKeyRaw the raw Ed25519 identity key to certify
     * @param notBefore      start of validity
     * @param notAfter       end of validity
     * @return the issued credential
     */
    public Issued issue(byte[] identityKeyRaw, Instant notBefore, Instant notAfter) {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
            generator.initialize(new ECGenParameterSpec("secp256r1"));
            KeyPair leafKeys = generator.generateKeyPair();
            byte[] serial = new byte[9];
            random.nextBytes(serial);
            X500Name subject = new X500Name(
                    "CN=" + Multibase.base58btc(identityKeyRaw));
            X509v3CertificateBuilder builder = new X509v3CertificateBuilder(
                    name, new BigInteger(1, serial),
                    Date.from(notBefore), Date.from(notAfter),
                    subject,
                    SubjectPublicKeyInfo.getInstance(
                            leafKeys.getPublic().getEncoded()));
            X509CertificateHolder holder = builder.build(signer(keys.getPrivate()));
            return new Issued(leafKeys,
                    new JcaX509CertificateConverter().getCertificate(holder));
        } catch (Exception e) {
            throw new IllegalStateException("cannot issue a test leaf", e);
        }
    }

    /**
     * Issues a CRL revoking the given leaves (an empty list is a valid,
     * nobody-revoked CRL).
     *
     * @param revokedLeaves leaves to list as key-compromised
     * @return the CRL
     */
    public X509CRL crl(X509Certificate... revokedLeaves) {
        Instant now = Instant.now();
        return crl(now, now.plus(Duration.ofDays(7)), CRLReason.KEY_COMPROMISE, revokedLeaves);
    }

    /**
     * Issues a CRL with chosen update times and reason (v0.1.13): a
     * {@code nextUpdate} in the past makes it stale.
     *
     * @param thisUpdate    the CRL's issue time, also each entry's revocation date
     * @param nextUpdate    when the CRL goes stale
     * @param reason        the reason on every entry
     * @param revokedLeaves leaves to list
     * @return the CRL
     */
    public X509CRL crl(Instant thisUpdate, Instant nextUpdate, CRLReason reason,
                       X509Certificate... revokedLeaves) {
        try {
            X509v2CRLBuilder builder = new X509v2CRLBuilder(name, Date.from(thisUpdate));
            builder.setNextUpdate(Date.from(nextUpdate));
            for (X509Certificate leaf : revokedLeaves) {
                builder.addCRLEntry(leaf.getSerialNumber(), Date.from(thisUpdate), reason.ordinal());
            }
            X509CRLHolder holder = builder.build(signer(keys.getPrivate()));
            return new JcaX509CRLConverter().getCRL(holder);
        } catch (Exception e) {
            throw new IllegalStateException("cannot issue a test CRL", e);
        }
    }

    /** The PeerID a leaf's CN binds, for assertions. */
    public static PeerId peerIdOf(byte[] identityKeyRaw) {
        return PeerId.fromPublicKey(identityKeyRaw);
    }

    private static ContentSigner signer(PrivateKey key) throws Exception {
        return new JcaContentSignerBuilder("SHA256withECDSA").build(key);
    }
}
