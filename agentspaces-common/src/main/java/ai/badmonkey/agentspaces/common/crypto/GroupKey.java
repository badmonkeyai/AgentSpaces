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

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Objects;
import java.util.Optional;

/**
 * A symmetric group content key (spec §11, v0.2 security): AES-256-GCM over
 * entry payloads, so what gossip and anti-entropy carry across the wire is
 * ciphertext, and only members holding the key can read entry contents. The
 * associated data binds each ciphertext to its entry, so a ciphertext cannot be
 * replayed under a different entry identity.
 *
 * <p>Key distribution is out of band in v0.1: operators provision the key with
 * group credentials (a keystore, a secret manager). Sealed per-member key wrap
 * against peer public keys arrives with the v0.2 security work.
 *
 * <p>Instances are immutable and safe for concurrent use.
 */
public final class GroupKey {

    /** Key length in bytes (AES-256). */
    public static final int KEY_LENGTH = 32;
    private static final int NONCE_LENGTH = 12;
    private static final int TAG_BITS = 128;
    private static final String ALGORITHM = "AES/GCM/NoPadding";
    private static final SecureRandom RANDOM = new SecureRandom();

    private final SecretKeySpec key;
    private final byte[] raw;

    private GroupKey(byte[] raw) {
        this.raw = raw.clone();
        this.key = new SecretKeySpec(this.raw, "AES");
    }

    /**
     * Generates a fresh random key.
     *
     * @return the key
     */
    public static GroupKey generate() {
        byte[] raw = new byte[KEY_LENGTH];
        RANDOM.nextBytes(raw);
        return new GroupKey(raw);
    }

    /**
     * Wraps existing key material.
     *
     * @param raw the 32-byte key
     * @return the key
     */
    public static GroupKey fromBytes(byte[] raw) {
        Objects.requireNonNull(raw, "raw");
        if (raw.length != KEY_LENGTH) {
            throw new IllegalArgumentException(
                    "group key must be " + KEY_LENGTH + " bytes, got " + raw.length);
        }
        return new GroupKey(raw);
    }

    /** Returns a copy of the raw key material, for keystore persistence. */
    public byte[] rawBytes() {
        return raw.clone();
    }

    /**
     * Encrypts a payload. The result is {@code nonce || ciphertext || tag} with a
     * fresh random nonce per call.
     *
     * @param plaintext      the payload
     * @param associatedData authenticated but unencrypted context binding the
     *                       ciphertext (an entry identity)
     * @return the sealed bytes
     */
    /**
     * Random 96-bit nonces make collision risk material near 2^32 encryptions
     * under one key (SPEC §11a.3 / ASF-045), so this key counts its seals:
     * a warning fires with ample headroom, and past the ceiling the key
     * refuses to encrypt at all — a loud rekey demand beats a silent nonce
     * reuse, which would be catastrophic for GCM.
     */
    private static final long SEAL_WARN_THRESHOLD = 1L << 31;
    private static final long SEAL_CEILING = 1L << 32;
    private final java.util.concurrent.atomic.AtomicLong seals =
            new java.util.concurrent.atomic.AtomicLong();

    public byte[] encrypt(byte[] plaintext, byte[] associatedData) {
        long count = seals.incrementAndGet();
        if (count == SEAL_WARN_THRESHOLD) {
            System.getLogger(GroupKey.class.getName()).log(
                    System.Logger.Level.WARNING,
                    "group content key has sealed 2^31 payloads; rotate it well "
                            + "before 2^32 (SPEC 11a.3)");
        }
        if (count > SEAL_CEILING) {
            throw new IllegalStateException(
                    "group content key exhausted its nonce budget (2^32 seals); "
                            + "rotate the key");
        }
        Objects.requireNonNull(plaintext, "plaintext");
        Objects.requireNonNull(associatedData, "associatedData");
        byte[] nonce = new byte[NONCE_LENGTH];
        RANDOM.nextBytes(nonce);
        try {
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, nonce));
            cipher.updateAAD(associatedData);
            byte[] sealed = cipher.doFinal(plaintext);
            byte[] out = new byte[NONCE_LENGTH + sealed.length];
            System.arraycopy(nonce, 0, out, 0, NONCE_LENGTH);
            System.arraycopy(sealed, 0, out, NONCE_LENGTH, sealed.length);
            return out;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("AES-GCM unavailable", e);
        }
    }

    /**
     * Decrypts sealed bytes produced by {@link #encrypt}.
     *
     * @param sealed         the sealed bytes
     * @param associatedData the same associated data the encryptor bound
     * @return the plaintext, or empty when the key, tag, or associated data do
     *         not match
     */
    public Optional<byte[]> decrypt(byte[] sealed, byte[] associatedData) {
        Objects.requireNonNull(sealed, "sealed");
        Objects.requireNonNull(associatedData, "associatedData");
        if (sealed.length <= NONCE_LENGTH) {
            return Optional.empty();
        }
        try {
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, key,
                    new GCMParameterSpec(TAG_BITS, sealed, 0, NONCE_LENGTH));
            cipher.updateAAD(associatedData);
            return Optional.of(cipher.doFinal(sealed, NONCE_LENGTH,
                    sealed.length - NONCE_LENGTH));
        } catch (GeneralSecurityException e) {
            return Optional.empty();
        }
    }
}
