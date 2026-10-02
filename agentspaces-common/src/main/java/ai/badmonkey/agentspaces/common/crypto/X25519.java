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
package ai.badmonkey.agentspaces.common.crypto;

import javax.crypto.KeyAgreement;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.Objects;

/**
 * X25519 (RFC 7748) helpers over the JDK's built-in XDH provider. Signing
 * identity in AgentSpaces is Ed25519; key agreement for sealed group-key wrap
 * (spec §11, v0.2) uses a separate X25519 keypair per peer, which keeps the
 * signing key out of any decryption path. As with {@link Ed25519}, this class
 * converts between the JDK's X.509 SubjectPublicKeyInfo encoding and the raw
 * 32-byte key that travels on the wire.
 */
public final class X25519 {

    private static final String ALGORITHM = "X25519";

    /**
     * The fixed 12-byte X.509 SubjectPublicKeyInfo prefix for an X25519 public
     * key: SEQUENCE(SEQUENCE(OID 1.3.101.110), BIT STRING of the 32 raw bytes).
     */
    private static final byte[] SPKI_PREFIX = {
            0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x6e, 0x03, 0x21, 0x00
    };

    /** Length of a raw X25519 public key in bytes. */
    public static final int RAW_PUBLIC_KEY_LENGTH = 32;

    private X25519() {
    }

    /** Generates a fresh X25519 keypair. */
    public static KeyPair generate() {
        try {
            return KeyPairGenerator.getInstance(ALGORITHM).generateKeyPair();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("X25519 unavailable in this JVM", e);
        }
    }

    /**
     * Runs the X25519 key agreement.
     *
     * @param privateKey   this side's private key
     * @param rawPublicKey the other side's raw 32-byte public key
     * @return the 32-byte shared secret
     */
    public static byte[] agree(PrivateKey privateKey, byte[] rawPublicKey) {
        Objects.requireNonNull(privateKey, "privateKey");
        try {
            KeyAgreement agreement = KeyAgreement.getInstance(ALGORITHM);
            agreement.init(privateKey);
            agreement.doPhase(publicKeyFromRaw(rawPublicKey), true);
            return agreement.generateSecret();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("X25519 agreement failed", e);
        }
    }

    /**
     * Extracts the raw 32-byte public key from a JDK X25519 {@link PublicKey}.
     *
     * @param publicKey an X25519 public key
     * @return the raw 32-byte key
     * @throws IllegalArgumentException if the key is not an X25519 SPKI encoding
     */
    public static byte[] rawPublicKey(PublicKey publicKey) {
        Objects.requireNonNull(publicKey, "publicKey");
        byte[] encoded = publicKey.getEncoded();
        if (encoded == null
                || encoded.length != SPKI_PREFIX.length + RAW_PUBLIC_KEY_LENGTH
                || !Arrays.equals(encoded, 0, SPKI_PREFIX.length,
                        SPKI_PREFIX, 0, SPKI_PREFIX.length)) {
            throw new IllegalArgumentException("not an X25519 X.509-encoded public key");
        }
        return Arrays.copyOfRange(encoded, SPKI_PREFIX.length, encoded.length);
    }

    /**
     * Reconstructs a JDK {@link PublicKey} from a raw 32-byte X25519 public key.
     *
     * @param raw the raw 32-byte key
     * @return the JDK public key
     * @throws IllegalArgumentException if the input is not 32 bytes or is invalid
     */
    public static PublicKey publicKeyFromRaw(byte[] raw) {
        Objects.requireNonNull(raw, "raw");
        if (raw.length != RAW_PUBLIC_KEY_LENGTH) {
            throw new IllegalArgumentException(
                    "raw X25519 public key must be 32 bytes, got " + raw.length);
        }
        byte[] spki = new byte[SPKI_PREFIX.length + raw.length];
        System.arraycopy(SPKI_PREFIX, 0, spki, 0, SPKI_PREFIX.length);
        System.arraycopy(raw, 0, spki, SPKI_PREFIX.length, raw.length);
        try {
            return KeyFactory.getInstance(ALGORITHM)
                    .generatePublic(new X509EncodedKeySpec(spki));
        } catch (GeneralSecurityException e) {
            throw new IllegalArgumentException("invalid raw X25519 public key", e);
        }
    }
}
