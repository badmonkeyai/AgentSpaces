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
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.test.TestCa;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.DERUTF8String;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class ChannelCertificateTest {

    @Test
    void attestBindsTheCertificateToTheEndorsingIdentity() throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        ChannelCertificate channel = ChannelCertificate.generate(identity);

        assertThat(channel.tlsKey().getAlgorithm()).isEqualTo("EC");
        assertThat(channel.certificate().getSigAlgOID())
                .isEqualTo(ChannelCertificate.ED25519_SIG_OID);

        Optional<PeerId> attested = ChannelCertificate.attest(channel.certificate());
        assertThat(attested).contains(identity.peerId());
    }

    @Test
    void attestRejectsACertificateEndorsedByADifferentKeyThanTheCnClaims() throws Exception {
        PeerIdentity honest = PeerIdentity.generate();
        PeerIdentity liar = PeerIdentity.generate();
        ChannelCertificate honestChannel = ChannelCertificate.generate(honest);

        // Re-sign the honest TBS with the liar's key: the CN still names the
        // honest identity, so attestation must fail the endorsement check.
        byte[] tbs = honestChannel.certificate().getTBSCertificate();
        byte[] forgedSig = liar.sign(tbs);
        byte[] forged = reassemble(tbs, forgedSig);
        X509Certificate parsed = (X509Certificate) CertificateFactory.getInstance("X.509")
                .generateCertificate(new ByteArrayInputStream(forged));

        assertThat(ChannelCertificate.attest(parsed)).isEmpty();
    }

    /** SPEC §5.6 attestation: the signature algorithm must be id-Ed25519; any other algorithm attests nothing. */
    @Test
    void attestRejectsAForeignSignatureAlgorithm() throws Exception {
        // A CA-issued leaf (SHA256withECDSA) whose CN correctly names the peer:
        // only the algorithm check can refuse it, and it must. The QUIC module's
        // legacy ephemeral EC certificate is the same shape.
        PeerIdentity identity = PeerIdentity.generate();
        X509Certificate foreign = TestCa.create().issue(identity.rawPublicKey()).certificate();
        assertThat(foreign.getSigAlgOID()).isNotEqualTo(ChannelCertificate.ED25519_SIG_OID);
        assertThat(ChannelCertificate.identityKeyFromCn(foreign))
                .as("the CN itself is well-formed; the algorithm is what fails")
                .isEqualTo(identity.rawPublicKey());

        assertThat(ChannelCertificate.attest(foreign)).isEmpty();
        assertThat(ChannelCertificate.attest(null)).isEmpty();
    }

    /** SPEC §5.6 attestation: the CN must decode (multibase base58btc) to exactly a 32-byte key. */
    @Test
    void attestRejectsACnThatDoesNotDecodeToAThirtyTwoByteKey() throws Exception {
        PeerIdentity identity = PeerIdentity.generate();

        // Not multibase at all, a wrong multibase prefix, a 31-byte and a
        // 33-byte payload, and an over-long CN: each one attests nothing even
        // though the Ed25519 endorsement over the TBS is genuine.
        for (String cn : new String[]{
                "not-a-key",
                "b" + Multibase.base58btc(identity.rawPublicKey()).substring(1),
                Multibase.base58btc(new byte[31]),
                Multibase.base58btc(new byte[33]),
                Multibase.base58btc(new byte[64])}) {
            X509Certificate certificate = identityEndorsedCertificate(identity, cn);
            assertThat(certificate.getSigAlgOID()).isEqualTo(ChannelCertificate.ED25519_SIG_OID);
            assertThat(ChannelCertificate.attest(certificate)).as("CN=%s", cn).isEmpty();
        }

        // Positive control: the same builder with the endorser's own key in the
        // CN attests, so the refusals above are the CN checks and nothing else.
        X509Certificate sound = identityEndorsedCertificate(
                identity, Multibase.base58btc(identity.rawPublicKey()));
        assertThat(ChannelCertificate.attest(sound)).contains(identity.peerId());
    }

    /** SPEC §5.6 attestation: the CN key must be the key whose signature covers the TBSCertificate. */
    @Test
    void attestRejectsACnKeyThatDoesNotMatchTheEndorsingKey() throws Exception {
        PeerIdentity honest = PeerIdentity.generate();
        PeerIdentity liar = PeerIdentity.generate();

        // The CN claims the liar's identity, but the honest key signed the TBS:
        // the liar's key does not verify the endorsement, so nobody is attested
        // (neither the liar, whose key did not sign, nor the honest peer, whose
        // key is not in the CN).
        X509Certificate mismatched = identityEndorsedCertificate(
                honest, Multibase.base58btc(liar.rawPublicKey()));
        assertThat(ChannelCertificate.attest(mismatched)).isEmpty();
    }

    /**
     * Builds an X.509 certificate exactly as {@link ChannelCertificate#generate}
     * does (fresh P-256 key, id-Ed25519 signature algorithm, endorsement by the
     * identity over the TBSCertificate) but with an arbitrary subject CN, so
     * the CN checks can be exercised in isolation.
     */
    private static X509Certificate identityEndorsedCertificate(PeerIdentity endorser, String cn)
            throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"));
        KeyPair tls = generator.generateKeyPair();
        Instant now = Instant.now().minus(1, ChronoUnit.HOURS);
        // The CN goes in as an ASN.1 value, not parsed from a DN string: from
        // 1.86 Bouncy Castle's string parser refuses a CN over ub-common-name
        // (64), but a hostile peer's certificate need not come from that parser,
        // so the over-long case must still reach identityKeyFromCn's guard.
        X500Name name = new X500NameBuilder().addRDN(BCStyle.CN, new DERUTF8String(cn)).build();
        X509v3CertificateBuilder builder = new X509v3CertificateBuilder(
                name, BigInteger.valueOf(7),
                Date.from(now), Date.from(now.plus(1, ChronoUnit.DAYS)),
                name, SubjectPublicKeyInfo.getInstance(tls.getPublic().getEncoded()));
        ByteArrayOutputStream tbs = new ByteArrayOutputStream();
        AlgorithmIdentifier ed25519 = new AlgorithmIdentifier(
                new ASN1ObjectIdentifier(ChannelCertificate.ED25519_SIG_OID));
        ContentSigner signer = new ContentSigner() {
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
                return endorser.sign(tbs.toByteArray());
            }
        };
        byte[] encoded = builder.build(signer).getEncoded();
        return (X509Certificate) CertificateFactory.getInstance("X.509")
                .generateCertificate(new ByteArrayInputStream(encoded));
    }

    @Test
    void theTlsKeySignsAndTheJdkAcceptsTheCertificateShape() throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        ChannelCertificate channel = ChannelCertificate.generate(identity);

        // The P-256 key must be usable for a TLS-style ECDSA signature.
        java.security.Signature ecdsa = java.security.Signature.getInstance("SHA256withECDSA");
        ecdsa.initSign(channel.tlsKey());
        ecdsa.update(new byte[]{1, 2, 3});
        byte[] sig = ecdsa.sign();
        java.security.Signature verify = java.security.Signature.getInstance("SHA256withECDSA");
        verify.initVerify(channel.certificate().getPublicKey());
        verify.update(new byte[]{1, 2, 3});
        assertThat(verify.verify(sig)).isTrue();

        // The JDK's own X.509 verification path accepts the Ed25519 endorsement
        // when handed the identity public key directly.
        channel.certificate().verify(
                ai.badmonkey.agentspaces.common.crypto.Ed25519
                        .publicKeyFromRaw(identity.rawPublicKey()));
    }

    private static byte[] reassemble(byte[] tbs, byte[] signature) {
        byte[] algorithm = new byte[]{0x30, 0x05, 0x06, 0x03, 0x2B, 0x65, 0x70};
        byte[] bitString = new byte[signature.length + 1];
        System.arraycopy(signature, 0, bitString, 1, signature.length);
        byte[] sig = tlv((byte) 0x03, bitString);
        return tlv((byte) 0x30, concat(tbs, algorithm, sig));
    }

    private static byte[] tlv(byte tag, byte[] content) {
        byte[] length;
        if (content.length < 0x80) {
            length = new byte[]{(byte) content.length};
        } else if (content.length < 0x100) {
            length = new byte[]{(byte) 0x81, (byte) content.length};
        } else {
            length = new byte[]{(byte) 0x82, (byte) (content.length >> 8), (byte) content.length};
        }
        byte[] out = new byte[1 + length.length + content.length];
        out[0] = tag;
        System.arraycopy(length, 0, out, 1, length.length);
        System.arraycopy(content, 0, out, 1 + length.length, content.length);
        return out;
    }

    private static byte[] concat(byte[]... parts) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.writeBytes(part);
        }
        return out.toByteArray();
    }
}
