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
package ai.badmonkey.agentspaces.common.codec;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Base58Test {

    @Test
    void knownVectors() {
        assertThat(Base58.encode(new byte[0])).isEmpty();
        assertThat(Base58.encode(new byte[]{0})).isEqualTo("1");
        assertThat(Base58.encode(new byte[]{0, 0, 0})).isEqualTo("111");
        assertThat(Base58.encode("Hello World!".getBytes(StandardCharsets.US_ASCII)))
                .isEqualTo("2NEpo7TZRRrLZSi2U");
    }

    @Test
    void leadingZerosSurviveTheRoundTrip() {
        byte[] input = {0, 0, 1, 2, 3};
        assertThat(Base58.decode(Base58.encode(input))).isEqualTo(input);
    }

    @Test
    void randomRoundTrips() {
        Random random = new Random(42);
        for (int i = 0; i < 200; i++) {
            byte[] input = new byte[random.nextInt(64)];
            random.nextBytes(input);
            assertThat(Base58.decode(Base58.encode(input)))
                    .as("round-trip of case %d", i)
                    .isEqualTo(input);
        }
    }

    @Test
    void decodeRejectsIllegalCharacters() {
        assertThatThrownBy(() -> Base58.decode("0OIl"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void multibasePrefixesAndDecodes() {
        byte[] input = {1, 2, 3, 4};
        String encoded = Multibase.base58btc(input);

        assertThat(encoded).startsWith("z");
        assertThat(Multibase.decode(encoded)).isEqualTo(input);
        assertThatThrownBy(() -> Multibase.decode("b12345"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Multibase.decode(""))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
