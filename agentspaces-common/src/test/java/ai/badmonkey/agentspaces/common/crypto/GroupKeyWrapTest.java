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

import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.util.HexFormat;

import static org.assertj.core.api.Assertions.assertThat;

class GroupKeyWrapTest {

    @Test
    void wrapsAndUnwrapsForTheIntendedRecipient() {
        GroupKey key = GroupKey.generate();
        KeyPair recipient = X25519.generate();
        byte[] recipientRaw = X25519.rawPublicKey(recipient.getPublic());

        GroupKeyWrap.WrappedKey wrapped = GroupKeyWrap.wrap(key, recipientRaw);

        assertThat(GroupKeyWrap.unwrap(wrapped, recipient.getPrivate(), recipientRaw))
                .hasValueSatisfying(unwrapped ->
                        assertThat(unwrapped.rawBytes()).isEqualTo(key.rawBytes()));
    }

    @Test
    void aDifferentRecipientCannotUnwrap() {
        GroupKey key = GroupKey.generate();
        KeyPair intended = X25519.generate();
        KeyPair other = X25519.generate();

        GroupKeyWrap.WrappedKey wrapped = GroupKeyWrap.wrap(
                key, X25519.rawPublicKey(intended.getPublic()));

        assertThat(GroupKeyWrap.unwrap(wrapped, other.getPrivate(),
                X25519.rawPublicKey(other.getPublic()))).isEmpty();
    }

    @Test
    void tamperedSealsDoNotOpen() {
        GroupKey key = GroupKey.generate();
        KeyPair recipient = X25519.generate();
        byte[] recipientRaw = X25519.rawPublicKey(recipient.getPublic());
        GroupKeyWrap.WrappedKey wrapped = GroupKeyWrap.wrap(key, recipientRaw);

        byte[] tampered = wrapped.sealed().clone();
        tampered[tampered.length - 1] ^= 0x01;

        assertThat(GroupKeyWrap.unwrap(
                new GroupKeyWrap.WrappedKey(wrapped.ephemeralPublicKey(), tampered),
                recipient.getPrivate(), recipientRaw)).isEmpty();
    }

    @Test
    void everyWrapUsesAFreshEphemeralKey() {
        GroupKey key = GroupKey.generate();
        byte[] recipientRaw = X25519.rawPublicKey(X25519.generate().getPublic());
        assertThat(GroupKeyWrap.wrap(key, recipientRaw).ephemeralPublicKey())
                .isNotEqualTo(GroupKeyWrap.wrap(key, recipientRaw).ephemeralPublicKey());
    }

    @Test
    void x25519RawEncodingRoundTrips() {
        KeyPair pair = X25519.generate();
        byte[] raw = X25519.rawPublicKey(pair.getPublic());
        assertThat(raw).hasSize(X25519.RAW_PUBLIC_KEY_LENGTH);
        assertThat(X25519.rawPublicKey(X25519.publicKeyFromRaw(raw))).isEqualTo(raw);
    }

    @Test
    void agreementIsSymmetric() {
        KeyPair a = X25519.generate();
        KeyPair b = X25519.generate();
        assertThat(X25519.agree(a.getPrivate(), X25519.rawPublicKey(b.getPublic())))
                .isEqualTo(X25519.agree(b.getPrivate(), X25519.rawPublicKey(a.getPublic())));
    }

    /** SPEC §11a.2: the HKDF salt covers the recipient's key, so the right private key with the wrong public key opens nothing. */
    @Test
    void theSaltBindsTheWrapToTheRecipientKey() {
        GroupKey key = GroupKey.generate();
        KeyPair recipient = X25519.generate();
        byte[] recipientRaw = X25519.rawPublicKey(recipient.getPublic());
        byte[] someoneElsesRaw = X25519.rawPublicKey(X25519.generate().getPublic());
        GroupKeyWrap.WrappedKey wrapped = GroupKeyWrap.wrap(key, recipientRaw);

        // Same X25519 agreement (the private key is the recipient's), but the
        // salt is derived from a different public key: the KEK differs.
        assertThat(GroupKeyWrap.unwrap(wrapped, recipient.getPrivate(), someoneElsesRaw)).isEmpty();
        assertThat(GroupKeyWrap.unwrap(wrapped, recipient.getPrivate(), recipientRaw)).isPresent();
    }

    /** SPEC §11a.2: the wrap is HKDF-SHA256(agreement, salt = ephemeral || recipient, info = aspace-group-key-wrap-v1) keying AES-256-GCM. */
    @Test
    void theWrapIsHkdfSaltedEphemeralThenRecipientUnderTheV1Label() {
        GroupKey key = GroupKey.generate();
        KeyPair recipient = X25519.generate();
        byte[] recipientRaw = X25519.rawPublicKey(recipient.getPublic());
        GroupKeyWrap.WrappedKey wrapped = GroupKeyWrap.wrap(key, recipientRaw);
        byte[] label = "aspace-group-key-wrap-v1".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        // Re-derive the KEK from the spec's recipe alone and open the seal
        // without GroupKeyWrap: this pins the label, the salt order and the
        // AEAD to what the specification (and the other clients) implement.
        byte[] shared = X25519.agree(recipient.getPrivate(), wrapped.ephemeralPublicKey());
        GroupKey kek = GroupKey.fromBytes(Hkdf.derive(shared,
                concat(wrapped.ephemeralPublicKey(), recipientRaw), label, GroupKey.KEY_LENGTH));
        assertThat(kek.decrypt(wrapped.sealed(), label))
                .hasValueSatisfying(raw -> assertThat(raw).isEqualTo(key.rawBytes()));

        // Salt order matters: recipient || ephemeral is a different KEK.
        GroupKey reversed = GroupKey.fromBytes(Hkdf.derive(shared,
                concat(recipientRaw, wrapped.ephemeralPublicKey()), label, GroupKey.KEY_LENGTH));
        assertThat(reversed.decrypt(wrapped.sealed(), label)).isEmpty();
        // And so does the label.
        GroupKey otherLabel = GroupKey.fromBytes(Hkdf.derive(shared,
                concat(wrapped.ephemeralPublicKey(), recipientRaw),
                "aspace-group-key-wrap-v2".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                GroupKey.KEY_LENGTH));
        assertThat(otherLabel.decrypt(wrapped.sealed(), label)).isEmpty();
    }

    /** SPEC §11a.2: a reply whose ephemeral key is not a 32-byte X25519 point is refused rather than parsed. */
    @Test
    void aWrappedKeyWithAMalformedEphemeralKeyDoesNotOpen() {
        GroupKey key = GroupKey.generate();
        KeyPair recipient = X25519.generate();
        byte[] recipientRaw = X25519.rawPublicKey(recipient.getPublic());
        GroupKeyWrap.WrappedKey wrapped = GroupKeyWrap.wrap(key, recipientRaw);

        assertThat(GroupKeyWrap.unwrap(
                new GroupKeyWrap.WrappedKey(new byte[31], wrapped.sealed()),
                recipient.getPrivate(), recipientRaw)).isEmpty();
        assertThat(GroupKeyWrap.unwrap(
                new GroupKeyWrap.WrappedKey(new byte[33], wrapped.sealed()),
                recipient.getPrivate(), recipientRaw)).isEmpty();
        assertThat(GroupKeyWrap.unwrap(
                new GroupKeyWrap.WrappedKey(null, wrapped.sealed()),
                recipient.getPrivate(), recipientRaw)).isEmpty();
        assertThat(GroupKeyWrap.unwrap(
                new GroupKeyWrap.WrappedKey(wrapped.ephemeralPublicKey(), null),
                recipient.getPrivate(), recipientRaw)).isEmpty();
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    @Test
    void hkdfMatchesTheRfc5869TestVector() {
        // RFC 5869 Appendix A.1 (SHA-256).
        HexFormat hex = HexFormat.of();
        byte[] okm = Hkdf.derive(
                hex.parseHex("0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b"),
                hex.parseHex("000102030405060708090a0b0c"),
                hex.parseHex("f0f1f2f3f4f5f6f7f8f9"),
                42);
        assertThat(hex.formatHex(okm)).isEqualTo(
                "3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf"
                        + "34007208d5b887185865");
    }

    /** Finding F-1, SPEC §11a.2 v2: a bound wrap opens only under the binding it was sealed with, and never through the unbound v1 path. */
    @Test
    void aBoundWrapOpensOnlyForItsOwnBinding() {
        GroupKey key = GroupKey.generate();
        KeyPair recipient = X25519.generate();
        byte[] recipientRaw = X25519.rawPublicKey(recipient.getPublic());
        byte[] binding = "group|holder|requester|42".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        GroupKeyWrap.WrappedKey wrapped = GroupKeyWrap.wrapBound(key, recipientRaw, binding);

        assertThat(GroupKeyWrap.unwrapBound(wrapped, recipient.getPrivate(), recipientRaw, binding))
                .hasValueSatisfying(opened -> assertThat(opened.rawBytes()).isEqualTo(key.rawBytes()));
        byte[] other = "group|holder|requester|43".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertThat(GroupKeyWrap.unwrapBound(wrapped, recipient.getPrivate(), recipientRaw, other))
                .as("another exchange's binding").isEmpty();
        assertThat(GroupKeyWrap.unwrap(wrapped, recipient.getPrivate(), recipientRaw))
                .as("the unbound v1 path").isEmpty();
    }
}
