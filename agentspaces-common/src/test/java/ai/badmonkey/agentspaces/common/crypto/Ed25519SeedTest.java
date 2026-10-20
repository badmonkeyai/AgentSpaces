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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@link Ed25519#fromSeed}: deterministic, the seed is the private key, signatures verify. */
class Ed25519SeedTest {

    private static byte[] seed(int first) {
        byte[] seed = new byte[32];
        for (int i = 0; i < seed.length; i++) {
            seed[i] = (byte) (first + i);
        }
        return seed;
    }

    @Test
    void theSameSeedYieldsTheSameKeyPair() {
        KeyPair one = Ed25519.fromSeed(seed(7));
        KeyPair two = Ed25519.fromSeed(seed(7));
        assertThat(Ed25519.rawPublicKey(one.getPublic())).isEqualTo(Ed25519.rawPublicKey(two.getPublic()));
        assertThat(Ed25519.rawPublicKey(Ed25519.fromSeed(seed(8)).getPublic()))
                .isNotEqualTo(Ed25519.rawPublicKey(one.getPublic()));
    }

    @Test
    void signaturesUnderASeededKeyVerifyWithTheRawPublicKey() {
        KeyPair pair = Ed25519.fromSeed(seed(1));
        byte[] message = "hello".getBytes();
        byte[] signature = Ed25519.sign(pair.getPrivate(), message);
        assertThat(Ed25519.verifyRaw(Ed25519.rawPublicKey(pair.getPublic()), message, signature)).isTrue();
        assertThat(Ed25519.verifyRaw(Ed25519.rawPublicKey(Ed25519.fromSeed(seed(2)).getPublic()), message, signature))
                .isFalse();
    }

    @Test
    void aSeedMustBeThirtyTwoBytes() {
        assertThatThrownBy(() -> Ed25519.fromSeed(new byte[31])).isInstanceOf(IllegalArgumentException.class);
    }
}
