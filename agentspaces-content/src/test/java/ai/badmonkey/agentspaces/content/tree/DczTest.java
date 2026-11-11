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
package ai.badmonkey.agentspaces.content.tree;

import ai.badmonkey.agentspaces.common.crypto.Digests;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code dcz} field of {@code NODES.deltas} (PDT 2 phase 4, P2-Z-01, P2-Z-04, P2-F-02; srad/agentspaces-pdt2.md
 * §3.8): a leading format byte, the RFC 9842 header, and one zstd frame over the base as a raw dictionary.
 */
class DczTest {

    private static byte[] random(int length, long seed) {
        byte[] bytes = new byte[length];
        new Random(seed).nextBytes(bytes);
        return bytes;
    }

    private static byte[] oneByteEdit(byte[] base, int at) {
        byte[] target = base.clone();
        target[at] ^= 0x5a;
        return target;
    }

    @Test
    void aOneByteEditOfASixtyFourKibChunkTravelsInAFewDozenBytesAndRebuildsTheChunk() {
        byte[] base = random(64 * 1024, 1);
        byte[] target = oneByteEdit(base, 40_000);
        byte[] frame = Dcz.encode(base, target).orElseThrow();
        assertThat(frame.length).as("format byte, RFC 9842 header, and a small zstd frame").isLessThan(128);
        assertThat(Dcz.decode(base, frame)).isEqualTo(target);
    }

    @Test
    void theFieldStartsWithTheFormatByteThenTheRfc9842HeaderNamingTheBaseBySha256() {
        byte[] base = random(20_000, 2);
        byte[] frame = Dcz.encode(base, oneByteEdit(base, 7)).orElseThrow();
        assertThat(frame[0]).as("P2-F-02: d for zstd").isEqualTo((byte) 'd').isEqualTo(Dcz.ZSTD);
        assertThat(Arrays.copyOfRange(frame, 1, 9)).as("the dcz magic of RFC 9842")
                .containsExactly(0x5e, 0x2a, 0x4d, 0x18, 0x20, 0x00, 0x00, 0x00);
        assertThat(Arrays.copyOfRange(frame, 9, 41)).isEqualTo(Digests.sha256(base));
        assertThat(Arrays.copyOfRange(frame, 41, 45)).as("then one zstd frame").containsExactly(0x28, 0xb5, 0x2f, 0xfd);
        assertThat(Dcz.HEADER_BYTES).isEqualTo(41);
    }

    @Test
    void aFrameOpensOnlyOverTheBaseItNames() {
        byte[] base = random(30_000, 3);
        byte[] frame = Dcz.encode(base, oneByteEdit(base, 100)).orElseThrow();
        byte[] other = base.clone();
        other[0] ^= 1;
        assertThatThrownBy(() -> Dcz.decode(other, frame)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("another base");
    }

    @Test
    void aReaderRefusesAnUnknownFormatByteAndVcdiffByName() {
        byte[] base = random(10_000, 4);
        byte[] frame = Dcz.encode(base, oneByteEdit(base, 1)).orElseThrow();
        byte[] vcdiff = frame.clone();
        vcdiff[0] = Dcz.VCDIFF;
        assertThatThrownBy(() -> Dcz.decode(base, vcdiff)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("VCDIFF")
                .hasMessageContaining("send a zstd frame");
        byte[] unknown = frame.clone();
        unknown[0] = 'x';
        assertThatThrownBy(() -> Dcz.decode(base, unknown)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("format byte 0x78");
        byte[] noMagic = frame.clone();
        noMagic[1] = 0;
        assertThatThrownBy(() -> Dcz.decode(base, noMagic)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("RFC 9842");
        assertThatThrownBy(() -> Dcz.decode(base, Arrays.copyOf(frame, 20))).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bytes");
    }

    @Test
    void aFrameWithTrailingBytesOrABrokenBodyIsRefused() {
        byte[] base = random(10_000, 5);
        byte[] frame = Dcz.encode(base, oneByteEdit(base, 9)).orElseThrow();
        byte[] trailing = Arrays.copyOf(frame, frame.length + 3);
        assertThatThrownBy(() -> Dcz.decode(base, trailing)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("one zstd frame");
        byte[] broken = frame.clone();
        broken[broken.length - 2] ^= (byte) 0xff;
        assertThatThrownBy(() -> Dcz.decode(base, broken)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aBaseThatStartsWithTheZstdDictionaryMagicMakesNoFrameAndOpensNone() {
        byte[] base = random(8_000, 6);
        base[0] = 0x37;
        base[1] = (byte) 0xa4;
        base[2] = 0x30;
        base[3] = (byte) 0xec;
        assertThat(Dcz.encode(base, oneByteEdit(base, 5000))).as("zstd would read the base as a formatted dictionary").isEmpty();
        byte[] plain = random(8_000, 6);
        byte[] frame = Dcz.encode(plain, oneByteEdit(plain, 5000)).orElseThrow();
        System.arraycopy(Digests.sha256(base), 0, frame, 9, 32);
        assertThatThrownBy(() -> Dcz.decode(base, frame)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dictionary magic");
    }
}
