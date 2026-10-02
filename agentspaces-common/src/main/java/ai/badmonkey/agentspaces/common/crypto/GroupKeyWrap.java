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

import java.security.KeyPair;
import java.security.PrivateKey;
import java.util.Objects;
import java.util.Optional;

/**
 * Sealed per-member group-key wrap (spec §11, v0.2): delivers a
 * {@link GroupKey} to one recipient so only that recipient can open it. The
 * construction is an ECIES-style seal: a fresh ephemeral X25519 keypair per
 * wrap, an RFC 7748 agreement with the recipient's public key, an HKDF
 * (RFC 5869) derivation salted with both public keys, and AES-256-GCM over the
 * key material. Nothing here identifies the recipient on the wire; transport
 * authentication (the signed envelope) says who asked, and this seal ensures
 * only the asker's decryption key can open what comes back.
 */
public final class GroupKeyWrap {

    private static final byte[] INFO =
            "aspace-group-key-wrap-v1".getBytes(java.nio.charset.StandardCharsets.UTF_8);

    /**
     * One sealed group key.
     *
     * @param ephemeralPublicKey the wrap's fresh raw X25519 public key
     * @param sealed             the AES-256-GCM sealed key material
     */
    public record WrappedKey(byte[] ephemeralPublicKey, byte[] sealed) {
    }

    private GroupKeyWrap() {
    }

    /**
     * Wraps a group key for one recipient.
     *
     * @param key                the group content key
     * @param recipientPublicKey the recipient's raw X25519 public key
     * @return the sealed key
     */
    public static WrappedKey wrap(GroupKey key, byte[] recipientPublicKey) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(recipientPublicKey, "recipientPublicKey");
        KeyPair ephemeral = X25519.generate();
        byte[] ephemeralRaw = X25519.rawPublicKey(ephemeral.getPublic());
        GroupKey kek = kekFor(
                X25519.agree(ephemeral.getPrivate(), recipientPublicKey),
                ephemeralRaw, recipientPublicKey);
        return new WrappedKey(ephemeralRaw, kek.encrypt(key.rawBytes(), INFO));
    }

    /**
     * Unwraps a sealed group key.
     *
     * @param wrapped            the sealed key
     * @param recipientPrivate   the recipient's X25519 private key
     * @param recipientPublicKey the recipient's raw X25519 public key
     * @return the group key, or empty when the seal does not open for this key
     */
    public static Optional<GroupKey> unwrap(WrappedKey wrapped, PrivateKey recipientPrivate,
                                            byte[] recipientPublicKey) {
        Objects.requireNonNull(wrapped, "wrapped");
        Objects.requireNonNull(recipientPrivate, "recipientPrivate");
        Objects.requireNonNull(recipientPublicKey, "recipientPublicKey");
        if (wrapped.ephemeralPublicKey() == null || wrapped.sealed() == null
                || wrapped.ephemeralPublicKey().length != X25519.RAW_PUBLIC_KEY_LENGTH) {
            return Optional.empty();
        }
        byte[] shared;
        try {
            shared = X25519.agree(recipientPrivate, wrapped.ephemeralPublicKey());
        } catch (RuntimeException e) {
            return Optional.empty();
        }
        GroupKey kek = kekFor(shared, wrapped.ephemeralPublicKey(), recipientPublicKey);
        return kek.decrypt(wrapped.sealed(), INFO)
                .filter(raw -> raw.length == GroupKey.KEY_LENGTH)
                .map(GroupKey::fromBytes);
    }

    /**
     * Wraps a group key for one recipient, bound to one exchange (spec §11a.2,
     * v2): the binding digest salts the derivation and is the associated data,
     * so a wrap made for one request, holder, requester, group, or epoch opens
     * for no other. Closes the key-substitution gap where any member that saw
     * a requester's public key could answer its request (finding F-1).
     *
     * @param key                the group content key
     * @param recipientPublicKey the recipient's raw X25519 public key
     * @param binding            the canonical bytes naming this exchange
     * @return the sealed key
     */
    public static WrappedKey wrapBound(GroupKey key, byte[] recipientPublicKey, byte[] binding) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(recipientPublicKey, "recipientPublicKey");
        byte[] digest = digest(Objects.requireNonNull(binding, "binding"));
        KeyPair ephemeral = X25519.generate();
        byte[] ephemeralRaw = X25519.rawPublicKey(ephemeral.getPublic());
        GroupKey kek = kekFor(X25519.agree(ephemeral.getPrivate(), recipientPublicKey),
                ephemeralRaw, recipientPublicKey, digest, INFO_V2);
        return new WrappedKey(ephemeralRaw, kek.encrypt(key.rawBytes(), digest));
    }

    /**
     * Unwraps a key sealed by {@link #wrapBound}; the binding must be the one
     * the recipient expects for this exchange, byte for byte.
     *
     * @param wrapped            the sealed key
     * @param recipientPrivate   the recipient's X25519 private key
     * @param recipientPublicKey the recipient's raw X25519 public key
     * @param binding            the canonical bytes naming the expected exchange
     * @return the group key, or empty when the seal does not open for this exchange
     */
    public static Optional<GroupKey> unwrapBound(WrappedKey wrapped, PrivateKey recipientPrivate,
                                                 byte[] recipientPublicKey, byte[] binding) {
        Objects.requireNonNull(wrapped, "wrapped");
        Objects.requireNonNull(recipientPrivate, "recipientPrivate");
        Objects.requireNonNull(recipientPublicKey, "recipientPublicKey");
        byte[] digest = digest(Objects.requireNonNull(binding, "binding"));
        if (wrapped.ephemeralPublicKey() == null || wrapped.sealed() == null
                || wrapped.ephemeralPublicKey().length != X25519.RAW_PUBLIC_KEY_LENGTH) {
            return Optional.empty();
        }
        byte[] shared;
        try {
            shared = X25519.agree(recipientPrivate, wrapped.ephemeralPublicKey());
        } catch (RuntimeException e) {
            return Optional.empty();
        }
        GroupKey kek = kekFor(shared, wrapped.ephemeralPublicKey(), recipientPublicKey,
                digest, INFO_V2);
        return kek.decrypt(wrapped.sealed(), digest)
                .filter(raw -> raw.length == GroupKey.KEY_LENGTH)
                .map(GroupKey::fromBytes);
    }

    /**
     * As {@link #unwrapBound(WrappedKey, PrivateKey, byte[], byte[])}, with the
     * key agreement supplied by the recipient's identity, so its private key
     * never leaves it (per-agent key wrap, SPEC §11a.2, v0.1.13).
     *
     * @param wrapped            the sealed key
     * @param agree              X25519 agreement with the recipient's private key
     * @param recipientPublicKey the recipient's raw X25519 public key
     * @param binding            the canonical bytes naming the expected exchange
     * @return the group key, or empty when the seal does not open for this exchange
     */
    public static Optional<GroupKey> unwrapBound(WrappedKey wrapped,
                                                 java.util.function.UnaryOperator<byte[]> agree,
                                                 byte[] recipientPublicKey, byte[] binding) {
        Objects.requireNonNull(wrapped, "wrapped");
        Objects.requireNonNull(agree, "agree");
        byte[] digest = digest(Objects.requireNonNull(binding, "binding"));
        if (wrapped.ephemeralPublicKey() == null || wrapped.sealed() == null
                || wrapped.ephemeralPublicKey().length != X25519.RAW_PUBLIC_KEY_LENGTH) {
            return Optional.empty();
        }
        byte[] shared;
        try {
            shared = agree.apply(wrapped.ephemeralPublicKey());
        } catch (RuntimeException e) {
            return Optional.empty();
        }
        GroupKey kek = kekFor(shared, wrapped.ephemeralPublicKey(), recipientPublicKey,
                digest, INFO_V2);
        return kek.decrypt(wrapped.sealed(), digest)
                .filter(raw -> raw.length == GroupKey.KEY_LENGTH)
                .map(GroupKey::fromBytes);
    }

    private static final byte[] INFO_V2 =
            "aspace-group-key-wrap-v2".getBytes(java.nio.charset.StandardCharsets.UTF_8);

    private static byte[] digest(byte[] binding) {
        try {
            return java.security.MessageDigest.getInstance("SHA-256").digest(binding);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    /** Derives the key-encryption key, salted with both public keys in order. */
    private static GroupKey kekFor(byte[] shared, byte[] ephemeralRaw, byte[] recipientRaw) {
        return kekFor(shared, ephemeralRaw, recipientRaw, new byte[0], INFO);
    }

    /** Derives a key-encryption key salted with both public keys, then the binding digest. */
    private static GroupKey kekFor(byte[] shared, byte[] ephemeralRaw, byte[] recipientRaw,
                                   byte[] bindingDigest, byte[] info) {
        byte[] salt = new byte[ephemeralRaw.length + recipientRaw.length + bindingDigest.length];
        System.arraycopy(ephemeralRaw, 0, salt, 0, ephemeralRaw.length);
        System.arraycopy(recipientRaw, 0, salt, ephemeralRaw.length, recipientRaw.length);
        System.arraycopy(bindingDigest, 0, salt, ephemeralRaw.length + recipientRaw.length,
                bindingDigest.length);
        return GroupKey.fromBytes(Hkdf.derive(shared, salt, info, GroupKey.KEY_LENGTH));
    }
}
