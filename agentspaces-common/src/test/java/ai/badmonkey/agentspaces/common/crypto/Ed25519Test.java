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
import java.security.KeyPair;
import java.security.PublicKey;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Ed25519Test {

    @Test
    void signAndVerify() {
        KeyPair keys = Ed25519.generate();
        byte[] message = "advertise this".getBytes(StandardCharsets.UTF_8);

        byte[] sig = Ed25519.sign(keys.getPrivate(), message);

        assertThat(Ed25519.verify(keys.getPublic(), message, sig)).isTrue();
    }

    @Test
    void tamperedMessageFailsVerification() {
        KeyPair keys = Ed25519.generate();
        byte[] message = "advertise this".getBytes(StandardCharsets.UTF_8);
        byte[] sig = Ed25519.sign(keys.getPrivate(), message);

        byte[] tampered = message.clone();
        tampered[0] ^= 1;

        assertThat(Ed25519.verify(keys.getPublic(), tampered, sig)).isFalse();
    }

    @Test
    void wrongKeyFailsVerification() {
        KeyPair signer = Ed25519.generate();
        KeyPair other = Ed25519.generate();
        byte[] message = "advertise this".getBytes(StandardCharsets.UTF_8);
        byte[] sig = Ed25519.sign(signer.getPrivate(), message);

        assertThat(Ed25519.verify(other.getPublic(), message, sig)).isFalse();
    }

    @Test
    void rawPublicKeyRoundTrips() {
        KeyPair keys = Ed25519.generate();

        byte[] raw = Ed25519.rawPublicKey(keys.getPublic());
        PublicKey rebuilt = Ed25519.publicKeyFromRaw(raw);

        assertThat(raw).hasSize(Ed25519.RAW_PUBLIC_KEY_LENGTH);
        assertThat(rebuilt.getEncoded()).isEqualTo(keys.getPublic().getEncoded());

        byte[] message = "roundtrip".getBytes(StandardCharsets.UTF_8);
        byte[] sig = Ed25519.sign(keys.getPrivate(), message);
        assertThat(Ed25519.verify(rebuilt, message, sig)).isTrue();
    }

    @Test
    void privateKeyPkcs8RoundTrips() {
        KeyPair keys = Ed25519.generate();

        var rebuilt = Ed25519.privateKeyFromPkcs8(keys.getPrivate().getEncoded());
        byte[] message = "persisted".getBytes(StandardCharsets.UTF_8);

        assertThat(Ed25519.verify(keys.getPublic(), message, Ed25519.sign(rebuilt, message))).isTrue();
    }

    @Test
    void invalidRawKeyIsRejected() {
        assertThatThrownBy(() -> Ed25519.publicKeyFromRaw(new byte[5]))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void sha256IsStable() {
        byte[] digest = Digests.sha256("abc".getBytes(StandardCharsets.US_ASCII));

        assertThat(digest).hasSize(32);
        // FIPS 180-4 test vector for "abc".
        assertThat(java.util.HexFormat.of().formatHex(digest))
                .isEqualTo("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    }
}
