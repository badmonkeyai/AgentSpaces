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

package ai.badmonkey.agentspaces.peering.bootstrap;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * The sealed form of a bootstrap-beacon datagram (SPEC §10.1 v0.1.19 addendum,
 * SRAD dtls-support §3.8): for a group whose members hold a content key, the
 * signed self-advertisement travels AEAD-sealed behind a tag that rotates
 * every {@value #EPOCH_SECONDS} seconds, so a bystander on the segment learns
 * neither the group, the issuer, nor the endpoints, and cannot link one
 * epoch's datagrams to the next. DTLS cannot carry a multicast send; this
 * seals the payload and leaves the carrier alone.
 *
 * <pre>
 * datagram = "ASB2" (4) | tag (16) | nonce (12) | AES-256-GCM(sealKey, nonce, ad = "ASB2" | tag, signedAd)
 * beaconKey | sealKey = HKDF-SHA-256(ikm = contentKey, salt = empty, info = "aspace-beacon-v1", 64 bytes)
 * tag = HMAC-SHA-256(beaconKey, "aspace-beacon-tag" | epoch as 8 bytes)[0..16)
 * epoch = floor(unix seconds / 300)
 * </pre>
 *
 * <p>The opened bytes are the same signed self-advertisement a plain datagram
 * carries, and they go through the same verification and admission.
 */
public final class BeaconSeal {

    /** The first four bytes of every sealed datagram. */
    public static final byte[] MAGIC = "ASB2".getBytes(StandardCharsets.US_ASCII);
    /** The tag rotates this often. */
    public static final long EPOCH_SECONDS = 300;
    static final int TAG_LENGTH = 16;
    static final int NONCE_LENGTH = 12;
    static final int GCM_TAG_BITS = 128;
    /** Bytes a seal adds to the advertisement. */
    public static final int OVERHEAD = MAGIC.length + TAG_LENGTH + NONCE_LENGTH + GCM_TAG_BITS / 8;
    private static final byte[] INFO = "aspace-beacon-v1".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] TAG_LABEL = "aspace-beacon-tag".getBytes(StandardCharsets.US_ASCII);
    private static final SecureRandom RANDOM = new SecureRandom();

    private final byte[] beaconKey;
    private final SecretKeySpec sealKey;

    /**
     * Derives the beacon keys from a group's content key.
     *
     * @param contentKey the group's content key (32 bytes)
     */
    public BeaconSeal(byte[] contentKey) {
        Objects.requireNonNull(contentKey, "contentKey");
        if (contentKey.length < 16) {
            throw new IllegalArgumentException("content key too short: " + contentKey.length + " bytes");
        }
        byte[] okm = hkdfSha256(contentKey, new byte[0], INFO, 64);
        this.beaconKey = Arrays.copyOfRange(okm, 0, 32);
        this.sealKey = new SecretKeySpec(Arrays.copyOfRange(okm, 32, 64), "AES");
    }

    /** Whether a datagram is in the sealed form. */
    public static boolean isSealed(byte[] datagram) {
        return datagram != null && datagram.length > OVERHEAD
                && Arrays.equals(datagram, 0, MAGIC.length, MAGIC, 0, MAGIC.length);
    }

    /** The beacon epoch of an instant. */
    public static long epoch(Instant now) {
        return Math.floorDiv(now.getEpochSecond(), EPOCH_SECONDS);
    }

    /** This group's tag for an epoch. */
    byte[] tag(long epoch) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(beaconKey, "HmacSHA256"));
            mac.update(TAG_LABEL);
            mac.update(ByteBuffer.allocate(8).putLong(epoch).array());
            return Arrays.copyOf(mac.doFinal(), TAG_LENGTH);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HMAC-SHA-256 unavailable", e);
        }
    }

    /**
     * Seals a signed self-advertisement under the current epoch's tag.
     *
     * @param signedAd the plain datagram's bytes
     * @param now      the current time
     * @return the sealed datagram
     */
    public byte[] seal(byte[] signedAd, Instant now) {
        byte[] tag = tag(epoch(now));
        byte[] nonce = new byte[NONCE_LENGTH];
        RANDOM.nextBytes(nonce);
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, sealKey, new GCMParameterSpec(GCM_TAG_BITS, nonce));
            cipher.updateAAD(MAGIC);
            cipher.updateAAD(tag);
            byte[] sealed = cipher.doFinal(signedAd);
            return ByteBuffer.allocate(MAGIC.length + TAG_LENGTH + NONCE_LENGTH + sealed.length)
                    .put(MAGIC).put(tag).put(nonce).put(sealed).array();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("AES-GCM unavailable", e);
        }
    }

    /**
     * Opens a sealed datagram if it carries this group's tag for the current
     * or the previous epoch; one HMAC per epoch tried, and the AEAD only on a match.
     *
     * @param datagram the received bytes
     * @param now      the current time
     * @return the signed self-advertisement, or empty when the tag is not this group's or the seal fails
     */
    public Optional<byte[]> open(byte[] datagram, Instant now) {
        if (!isSealed(datagram)) {
            return Optional.empty();
        }
        byte[] tag = Arrays.copyOfRange(datagram, MAGIC.length, MAGIC.length + TAG_LENGTH);
        long epoch = epoch(now);
        if (!MessageDigest.isEqual(tag, tag(epoch)) && !MessageDigest.isEqual(tag, tag(epoch - 1))) {
            return Optional.empty();
        }
        int nonceAt = MAGIC.length + TAG_LENGTH;
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, sealKey,
                    new GCMParameterSpec(GCM_TAG_BITS, datagram, nonceAt, NONCE_LENGTH));
            cipher.updateAAD(MAGIC);
            cipher.updateAAD(tag);
            int at = nonceAt + NONCE_LENGTH;
            return Optional.of(cipher.doFinal(datagram, at, datagram.length - at));
        } catch (GeneralSecurityException e) {
            return Optional.empty(); // tampered, or a colliding tag under another key
        }
    }

    /** HKDF with HMAC-SHA-256 (RFC 5869): extract, then expand to {@code length} bytes. */
    static byte[] hkdfSha256(byte[] ikm, byte[] salt, byte[] info, int length) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(salt.length == 0 ? new byte[32] : salt, "HmacSHA256"));
            byte[] prk = mac.doFinal(ikm);
            mac.init(new SecretKeySpec(prk, "HmacSHA256"));
            byte[] okm = new byte[length];
            byte[] previous = new byte[0];
            int at = 0;
            for (int counter = 1; at < length; counter++) {
                mac.update(previous);
                mac.update(info);
                mac.update((byte) counter);
                previous = mac.doFinal();
                int n = Math.min(previous.length, length - at);
                System.arraycopy(previous, 0, okm, at, n);
                at += n;
            }
            return okm;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HMAC-SHA-256 unavailable", e);
        }
    }
}
