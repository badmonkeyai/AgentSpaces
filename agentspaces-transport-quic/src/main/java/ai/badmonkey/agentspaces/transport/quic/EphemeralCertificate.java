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
package ai.badmonkey.agentspaces.transport.quic;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * A minimal self-signed X.509 certificate for the QUIC handshake, generated
 * with nothing but the JDK: an EC P-256 key pair, a hand-assembled DER
 * certificate structure, and a SHA256withECDSA signature. The JDK performs all
 * cryptography; this class only encodes the certificate's ASN.1 shape.
 *
 * <p>The certificate deliberately authenticates nothing. QUIC's TLS layer
 * requires a server certificate; AgentSpaces peers authenticate each other
 * above the transport through Ed25519-signed envelopes against self-certifying
 * peer ids (spec §11), so the transport certificate is ephemeral, per-process,
 * and unverified by dialers — the same trust model the TCP transport has,
 * plus QUIC's confidentiality.
 *
 * @param key  the private key the QUIC server handshakes with
 * @param cert the matching self-signed certificate
 */
public record EphemeralCertificate(PrivateKey key, X509Certificate cert) {

    private static final DateTimeFormatter UTC_TIME =
            DateTimeFormatter.ofPattern("yyMMddHHmmss'Z'");

    /** ecdsa-with-SHA256, 1.2.840.10045.4.3.2, pre-encoded. */
    private static final byte[] OID_ECDSA_SHA256 = {
            0x06, 0x08, 0x2A, (byte) 0x86, 0x48, (byte) 0xCE, 0x3D, 0x04, 0x03, 0x02};
    /** commonName, 2.5.4.3, pre-encoded. */
    private static final byte[] OID_COMMON_NAME = {0x06, 0x03, 0x55, 0x04, 0x03};

    /**
     * Generates a fresh key pair and certificate, valid for one year.
     *
     * @param commonName the certificate's CN, e.g. the peer's short name
     * @return the ephemeral certificate
     * @throws GeneralSecurityException if the JDK cannot generate or sign
     */
    public static EphemeralCertificate generate(String commonName)
            throws GeneralSecurityException {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(new ECGenParameterSpec("secp256r1"));
        KeyPair pair = generator.generateKeyPair();

        byte[] serial = new byte[9];
        new SecureRandom().nextBytes(serial);
        serial[0] = (byte) (serial[0] & 0x7F); // positive INTEGER

        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC).minusHours(1);
        byte[] algorithm = sequence(OID_ECDSA_SHA256);
        byte[] name = name(commonName);
        byte[] tbs = sequence(concat(
                explicit0(integer(new byte[]{0x02})),      // version v3
                integer(serial),
                algorithm,
                name,                                       // issuer
                sequence(concat(                            // validity
                        utcTime(now), utcTime(now.plusYears(1)))),
                name,                                       // subject = issuer
                pair.getPublic().getEncoded()));            // SPKI, already DER

        Signature signer = Signature.getInstance("SHA256withECDSA");
        signer.initSign(pair.getPrivate());
        signer.update(tbs);
        byte[] signature = signer.sign();

        byte[] certificate = sequence(concat(tbs, algorithm, bitString(signature)));
        X509Certificate parsed = (X509Certificate) CertificateFactory.getInstance("X.509")
                .generateCertificate(new ByteArrayInputStream(certificate));
        return new EphemeralCertificate(pair.getPrivate(), parsed);
    }

    // -------------------------------------------------------------- DER pieces

    /** RDNSequence with a single CN attribute. */
    private static byte[] name(String commonName) {
        byte[] value = commonName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] utf8String = tlv((byte) 0x0C, value);
        byte[] attribute = sequence(concat(OID_COMMON_NAME, utf8String));
        byte[] rdnSet = tlv((byte) 0x31, attribute);
        return sequence(rdnSet);
    }

    private static byte[] utcTime(ZonedDateTime at) {
        return tlv((byte) 0x17,
                UTC_TIME.format(at).getBytes(java.nio.charset.StandardCharsets.US_ASCII));
    }

    private static byte[] integer(byte[] magnitude) {
        return tlv((byte) 0x02, magnitude);
    }

    private static byte[] bitString(byte[] bytes) {
        byte[] padded = new byte[bytes.length + 1];
        System.arraycopy(bytes, 0, padded, 1, bytes.length); // zero unused bits
        return tlv((byte) 0x03, padded);
    }

    private static byte[] explicit0(byte[] inner) {
        return tlv((byte) 0xA0, inner);
    }

    private static byte[] sequence(byte[] content) {
        return tlv((byte) 0x30, content);
    }

    private static byte[] tlv(byte tag, byte[] content) {
        byte[] length = length(content.length);
        byte[] out = new byte[1 + length.length + content.length];
        out[0] = tag;
        System.arraycopy(length, 0, out, 1, length.length);
        System.arraycopy(content, 0, out, 1 + length.length, content.length);
        return out;
    }

    private static byte[] length(int value) {
        if (value < 0x80) {
            return new byte[]{(byte) value};
        }
        if (value < 0x100) {
            return new byte[]{(byte) 0x81, (byte) value};
        }
        return new byte[]{(byte) 0x82, (byte) (value >> 8), (byte) value};
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.writeBytes(part);
        }
        return out.toByteArray();
    }
}
