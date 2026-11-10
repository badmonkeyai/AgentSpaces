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
import com.github.luben.zstd.ZstdCompressCtx;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A {@code dcz} frame cannot decompress beyond the chunk bound (PDT 2 phase 4, P2-Q-SEC-05; threat 5 of
 * srad/agentspaces-pdt2.md §6.7): the reader reads the zstd frame header before it decompresses, and refuses a
 * frame without a content size, one whose content size or window passes {@link Dcz#MAX_OUTPUT} (256 KiB), one
 * that names a dictionary id, and one whose blocks write more than its header declared.
 */
class DeltaBoundsTest {

    private static final byte[] BASE = random(4096, 11);

    private static byte[] random(int length, long seed) {
        byte[] bytes = new byte[length];
        new Random(seed).nextBytes(bytes);
        return bytes;
    }

    /** A dcz field around a zstd frame made by hand over {@link #BASE}. */
    private static byte[] field(byte[] zstd) {
        byte[] out = new byte[Dcz.HEADER_BYTES + zstd.length];
        out[0] = Dcz.ZSTD;
        byte[] magic = {0x5e, 0x2a, 0x4d, 0x18, 0x20, 0x00, 0x00, 0x00};
        System.arraycopy(magic, 0, out, 1, magic.length);
        System.arraycopy(Digests.sha256(BASE), 0, out, 9, 32);
        System.arraycopy(zstd, 0, out, Dcz.HEADER_BYTES, zstd.length);
        return out;
    }

    private static byte[] zstd(byte[] target, boolean contentSize, int windowLog) {
        try (ZstdCompressCtx ctx = new ZstdCompressCtx()) {
            ctx.setLevel(3).setContentSize(contentSize).setChecksum(false).setDictID(false);
            if (windowLog > 0) {
                ctx.setWindowLog(windowLog);
            }
            ctx.loadDict(BASE);
            return ctx.compress(target);
        }
    }

    @Test
    void aBombOfZerosPastTheChunkBoundIsRefusedBeforeItDecompresses() {
        byte[] bomb = field(zstd(new byte[1 << 20], true, 0));
        assertThat(bomb.length).as("a 1 MiB output from a frame of a few dozen bytes").isLessThan(200);
        assertThatThrownBy(() -> Dcz.decode(BASE, bomb)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("content size of 1048576 bytes").hasMessageContaining("262144");
        byte[] justOver = field(zstd(new byte[Dcz.MAX_OUTPUT + 1], true, 0));
        assertThatThrownBy(() -> Dcz.decode(BASE, justOver)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("262144");
        byte[] atBound = field(zstd(new byte[Dcz.MAX_OUTPUT], true, 0));
        assertThat(Dcz.decode(BASE, atBound)).hasSize(Dcz.MAX_OUTPUT);
    }

    @Test
    void aFrameThatDeclaresNoContentSizeIsRefused() {
        byte[] unsized = field(zstd(random(2000, 12), false, 0));
        assertThatThrownBy(() -> Dcz.decode(BASE, unsized)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no content size");
    }

    @Test
    void aWindowPastTheChunkBoundIsRefusedWhateverTheContentSize() {
        // a 1 KiB window under 2 KiB of output makes zstd write a window descriptor beside the content size
        byte[] zstd = zstd(random(2048, 13), true, 10);
        int fhd = zstd[4] & 0xff;
        assertThat(fhd & 0x20).as("no single segment: a window descriptor follows").isZero();
        assertThat(Dcz.decode(BASE, field(zstd))).hasSize(2048);
        byte[] wide = zstd.clone();
        wide[5] = (byte) (9 << 3); // exponent 9: a window of 2^19 bytes
        assertThatThrownBy(() -> Dcz.decode(BASE, field(wide))).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("window of 524288 bytes");
    }

    @Test
    void aFrameWhoseBlocksWriteMoreThanItsHeaderDeclaresIsRefused() {
        byte[] zstd = zstd(random(2000, 14), true, 0);
        int fhd = zstd[4] & 0xff;
        assertThat(fhd >> 6).as("a two-byte content size, which counts from 256").isEqualTo(1);
        byte[] lying = zstd.clone();
        lying[5] = (byte) (1000 - 256);
        lying[6] = 0;
        assertThatThrownBy(() -> Dcz.decode(BASE, field(lying))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aFrameThatNamesADictionaryIdIsRefused() {
        byte[] zstd = zstd(random(300, 15), true, 0);
        byte[] named = new byte[zstd.length + 1];
        System.arraycopy(zstd, 0, named, 0, 5);
        named[4] = (byte) ((zstd[4] & 0xfc) | 1);
        named[5] = 7;
        System.arraycopy(zstd, 5, named, 6, zstd.length - 5);
        assertThatThrownBy(() -> Dcz.decode(BASE, field(named))).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dictionary id");
    }

    @Test
    void aFieldOfMoreThanOneFrameIsRefused() {
        byte[] zstd = zstd(random(300, 19), true, 0);
        byte[] twice = new byte[2 * zstd.length];
        System.arraycopy(zstd, 0, twice, 0, zstd.length);
        System.arraycopy(zstd, 0, twice, zstd.length, zstd.length);
        assertThatThrownBy(() -> Dcz.decode(BASE, field(twice))).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("more than one zstd frame");
        byte[] skippable = {0x50, 0x2a, 0x4d, 0x18, 1, 0, 0, 0, 9};
        byte[] padded = new byte[zstd.length + skippable.length];
        System.arraycopy(zstd, 0, padded, 0, zstd.length);
        System.arraycopy(skippable, 0, padded, zstd.length, skippable.length);
        assertThatThrownBy(() -> Dcz.decode(BASE, field(padded))).as("a skippable frame after the frame")
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("more than one zstd frame");
    }

    @Test
    void aBaseOrATargetPastTheChunkBoundMakesNoFrame() {
        assertThat(Dcz.encode(random(Dcz.MAX_OUTPUT + 1, 16), random(100, 17))).isEmpty();
        assertThat(Dcz.encode(BASE, random(Dcz.MAX_OUTPUT + 1, 18))).isEmpty();
        assertThatThrownBy(() -> Dcz.decode(random(Dcz.MAX_OUTPUT + 1, 16), field(zstd(new byte[10], true, 0))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("base of 262145 bytes");
    }
}
