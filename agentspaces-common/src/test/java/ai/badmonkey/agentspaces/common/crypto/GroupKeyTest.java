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

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GroupKeyTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void roundTripsUnderTheSameKeyAndAssociatedData() {
        GroupKey key = GroupKey.generate();
        byte[] sealed = key.encrypt(bytes("the payload"), bytes("entry-1"));

        assertThat(sealed).isNotEqualTo(bytes("the payload"));
        assertThat(key.decrypt(sealed, bytes("entry-1")))
                .hasValueSatisfying(plain -> assertThat(plain).isEqualTo(bytes("the payload")));
    }

    @Test
    void aDifferentKeyCannotDecrypt() {
        byte[] sealed = GroupKey.generate().encrypt(bytes("secret"), bytes("aad"));
        assertThat(GroupKey.generate().decrypt(sealed, bytes("aad"))).isEmpty();
    }

    @Test
    void mismatchedAssociatedDataIsRejected() {
        GroupKey key = GroupKey.generate();
        byte[] sealed = key.encrypt(bytes("secret"), bytes("entry-1"));
        assertThat(key.decrypt(sealed, bytes("entry-2"))).isEmpty();
    }

    @Test
    void tamperedCiphertextIsRejected() {
        GroupKey key = GroupKey.generate();
        byte[] sealed = key.encrypt(bytes("secret"), bytes("aad"));
        sealed[sealed.length - 1] ^= 0x01;
        assertThat(key.decrypt(sealed, bytes("aad"))).isEmpty();
    }

    @Test
    void survivesKeystorePersistenceAsRawBytes() {
        GroupKey original = GroupKey.generate();
        GroupKey restored = GroupKey.fromBytes(original.rawBytes());
        byte[] sealed = original.encrypt(bytes("secret"), bytes("aad"));
        assertThat(restored.decrypt(sealed, bytes("aad"))).isPresent();
    }

    @Test
    void noncesAreFreshPerEncryption() {
        GroupKey key = GroupKey.generate();
        assertThat(key.encrypt(bytes("x"), bytes("a")))
                .isNotEqualTo(key.encrypt(bytes("x"), bytes("a")));
    }

    @Test
    void rejectsWrongKeyLength() {
        assertThatThrownBy(() -> GroupKey.fromBytes(new byte[16]))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** SPEC §11a.1: the sealed form is {@code nonce(96 bit) || ciphertext || tag(128 bit)}, plain AES-256-GCM. */
    @Test
    void sealedFormIsNonceThenCiphertextThenTag() throws Exception {
        GroupKey key = GroupKey.generate();
        byte[] plain = bytes("the payload bytes");
        byte[] aad = bytes("space|entry");

        byte[] sealed = key.encrypt(plain, aad);
        assertThat(sealed).hasSize(12 + plain.length + 16);
        assertThat(key.encrypt(new byte[0], aad))
                .as("an empty plaintext still carries the nonce and the tag")
                .hasSize(12 + 16);

        // Independent AES-256-GCM decryption of the layout the spec names: the
        // first 12 bytes are the IV, the rest is ciphertext followed by the tag.
        javax.crypto.Cipher gcm = javax.crypto.Cipher.getInstance("AES/GCM/NoPadding");
        gcm.init(javax.crypto.Cipher.DECRYPT_MODE,
                new javax.crypto.spec.SecretKeySpec(key.rawBytes(), "AES"),
                new javax.crypto.spec.GCMParameterSpec(128, sealed, 0, 12));
        gcm.updateAAD(aad);
        assertThat(gcm.doFinal(sealed, 12, sealed.length - 12)).isEqualTo(plain);

        // Too short to hold nonce + tag: refused, never parsed.
        assertThat(key.decrypt(java.util.Arrays.copyOf(sealed, 12), aad)).isEmpty();
        assertThat(key.decrypt(new byte[0], aad)).isEmpty();
        // The nonce is authenticated by construction: flipping a nonce byte
        // fails like any other tamper.
        byte[] nonceFlipped = sealed.clone();
        nonceFlipped[0] ^= 0x01;
        assertThat(key.decrypt(nonceFlipped, aad)).isEmpty();
    }
}
