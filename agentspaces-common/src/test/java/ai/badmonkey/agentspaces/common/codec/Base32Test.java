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
import java.util.HexFormat;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Base32Test {

    @Test
    void matchesRfc4648VectorsInLowerCaseWithoutPadding() {
        assertThat(Base32.encode(new byte[0])).isEmpty();
        assertThat(Base32.encode("f".getBytes(StandardCharsets.US_ASCII))).isEqualTo("my");
        assertThat(Base32.encode("fo".getBytes(StandardCharsets.US_ASCII))).isEqualTo("mzxq");
        assertThat(Base32.encode("foo".getBytes(StandardCharsets.US_ASCII))).isEqualTo("mzxw6");
        assertThat(Base32.encode("foob".getBytes(StandardCharsets.US_ASCII))).isEqualTo("mzxw6yq");
        assertThat(Base32.encode("fooba".getBytes(StandardCharsets.US_ASCII))).isEqualTo("mzxw6ytb");
        assertThat(Base32.encode("foobar".getBytes(StandardCharsets.US_ASCII))).isEqualTo("mzxw6ytboi");
    }

    @Test
    void decodesEitherCaseAndIgnoresPadding() {
        assertThat(Base32.decode("MZXW6YTBOI======")).isEqualTo("foobar".getBytes(StandardCharsets.US_ASCII));
        assertThat(Base32.decode("mzxw6ytboi")).isEqualTo("foobar".getBytes(StandardCharsets.US_ASCII));
        assertThatThrownBy(() -> Base32.decode("mzxw6ytb0i")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void roundTripsRandomBytes() {
        Random random = new Random(32);
        for (int length = 0; length < 70; length++) {
            byte[] bytes = new byte[length];
            random.nextBytes(bytes);
            assertThat(Base32.decode(Base32.encode(bytes))).isEqualTo(bytes);
        }
    }

    @Test
    void cidV1RawSha256IsTheIpfsString() {
        // The CIDv1 of the raw block "hello world\n"? No: the well-known vector is
        // the empty raw block, bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku,
        // which is sha2-256 of the empty input under multicodec raw (0x55).
        byte[] digest = HexFormat.of().parseHex(
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        byte[] cid = new byte[4 + digest.length];
        cid[0] = 0x01; // CIDv1
        cid[1] = 0x55; // raw
        cid[2] = 0x12; // sha2-256
        cid[3] = 0x20; // 32 bytes
        System.arraycopy(digest, 0, cid, 4, digest.length);
        assertThat(Multibase.base32(cid))
                .isEqualTo("bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku");
        assertThat(Multibase.decode("bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku")).isEqualTo(cid);
    }
}
