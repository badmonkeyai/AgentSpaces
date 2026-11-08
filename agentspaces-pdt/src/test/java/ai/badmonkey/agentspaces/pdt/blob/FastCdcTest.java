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
package ai.badmonkey.agentspaces.pdt.blob;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** {@link FastCdc} (PD-B-01): the Gear table, determinism, the size bounds, locality, streaming, and the vector {@code pdt_fastcdc_cuts}. */
class FastCdcTest {

    /** The seeded 8 MiB stream of the vector: {@code new Random(42).nextBytes(8 MiB)}. */
    static byte[] vectorStream() {
        byte[] data = new byte[8 * 1024 * 1024];
        new Random(42).nextBytes(data);
        return data;
    }

    /** The first 20 cut points of the vector stream, printed once by {@link #printsTheVector} and pasted here. */
    private static final int[] VECTOR_CUTS = {
            49658, 134067, 236231, 281271, 395473, 481988, 547841, 618292, 692937, 791064,
            862849, 918550, 984814, 1059193, 1149077, 1221087, 1308545, 1378065, 1415479, 1482734
    };

    @Test
    void gearTableComesFromSha256OfEachByte() {
        // sha256(0x00) = 6e340b9c ffb37a98 ...; sha256(0xff) = a8100ae6 aa1940d0 ...
        assertThat(FastCdc.GEAR[0]).isEqualTo(0x6e340b9cffb37a98L);
        assertThat(FastCdc.GEAR[255]).isEqualTo(0xa8100ae6aa1940d0L);
        assertThat(FastCdc.GEAR).hasSize(256);
        assertThat(Arrays.stream(FastCdc.GEAR).distinct().count()).isEqualTo(256);
    }

    @Test
    void masksAndSizesArePinned() {
        assertThat(FastCdc.MIN_SIZE).isEqualTo(16 * 1024);
        assertThat(FastCdc.AVG_SIZE).isEqualTo(64 * 1024);
        assertThat(FastCdc.MAX_SIZE).isEqualTo(256 * 1024);
        assertThat(Long.bitCount(FastCdc.MASK_S)).isEqualTo(18);
        assertThat(Long.bitCount(FastCdc.MASK_L)).isEqualTo(14);
        assertThat(FastCdc.MASK_S).isEqualTo(0xFFFFC00000000000L);
        assertThat(FastCdc.MASK_L).isEqualTo(0xFFFC000000000000L);
    }

    @Test
    void vectorCutsMatchTheLiterals() {
        List<Integer> cuts = FastCdc.cuts(vectorStream());
        assertThat(cuts.size()).isGreaterThan(20);
        for (int i = 0; i < VECTOR_CUTS.length; i++) {
            assertThat(cuts.get(i)).as("cut " + i).isEqualTo(VECTOR_CUTS[i]);
        }
        assertThat(cuts.get(cuts.size() - 1)).isEqualTo(8 * 1024 * 1024);
    }

    @Test
    void printsTheVector() {
        List<Integer> cuts = FastCdc.cuts(vectorStream());
        StringBuilder out = new StringBuilder("pdt_fastcdc_cuts (seed 42, 8 MiB, first 20): ");
        for (int i = 0; i < 20; i++) {
            out.append(cuts.get(i)).append(i < 19 ? ", " : "");
        }
        System.out.println(out + " (" + cuts.size() + " chunks, mean "
                + (8 * 1024 * 1024 / cuts.size()) + " bytes)");
    }

    @Test
    void cutsAreDeterministic() {
        byte[] data = vectorStream();
        assertThat(FastCdc.cuts(data)).isEqualTo(FastCdc.cuts(data.clone()));
    }

    @Test
    void everyChunkRespectsTheBounds() {
        byte[] data = new byte[3 * 1024 * 1024];
        new Random(7).nextBytes(data);
        List<Integer> cuts = FastCdc.cuts(data);
        int start = 0;
        for (int i = 0; i < cuts.size(); i++) {
            int length = cuts.get(i) - start;
            assertThat(length).isLessThanOrEqualTo(FastCdc.MAX_SIZE);
            if (i < cuts.size() - 1) {
                assertThat(length).isGreaterThanOrEqualTo(FastCdc.MIN_SIZE);
            } else {
                assertThat(length).isGreaterThan(0);
            }
            start = cuts.get(i);
        }
        assertThat(start).isEqualTo(data.length);
        assertThat(cuts.size()).isBetween(data.length / FastCdc.MAX_SIZE, data.length / FastCdc.MIN_SIZE + 1);
    }

    @Test
    void shortInputsAreOneChunkAndEmptyIsNone() {
        assertThat(FastCdc.cuts(new byte[0])).isEmpty();
        assertThat(FastCdc.chunksOf(new byte[0])).isEmpty();
        assertThat(FastCdc.cuts(new byte[1])).containsExactly(1);
        assertThat(FastCdc.cuts(new byte[FastCdc.MIN_SIZE])).containsExactly(FastCdc.MIN_SIZE);
        byte[] data = new byte[FastCdc.MIN_SIZE + 1];
        List<Integer> cuts = FastCdc.cuts(data);
        assertThat(cuts.get(cuts.size() - 1)).isEqualTo(data.length);
    }

    @Test
    void uniformBytesCutAtTheMaximum() {
        // A constant stream never matches the mask, so every chunk is MAX until the remainder.
        byte[] data = new byte[FastCdc.MAX_SIZE * 2 + 100];
        Arrays.fill(data, (byte) 0x41);
        assertThat(FastCdc.cuts(data)).containsExactly(FastCdc.MAX_SIZE, 2 * FastCdc.MAX_SIZE, data.length);
    }

    @Test
    void aOneByteEditMovesOnlyTheCutsOfItsChunk() {
        byte[] data = vectorStream();
        List<Integer> before = FastCdc.cuts(data);
        int edit = 3 * 1024 * 1024 + 777;
        data[edit] ^= 0x5A;
        List<Integer> after = FastCdc.cuts(data);
        int chunkEnd = before.stream().filter(c -> c > edit).findFirst().orElseThrow();
        List<Integer> beforeTail = before.subList(before.indexOf(chunkEnd), before.size());
        // The cut that closed the edited chunk, and every cut after it, survive the edit (locality).
        assertThat(after).containsAll(beforeTail);
        assertThat(after.subList(0, before.indexOf(chunkEnd))).isEqualTo(before.subList(0, before.indexOf(chunkEnd)));
    }

    @Test
    void streamingMatchesTheArrayFormWithSmallReads() {
        byte[] data = new byte[2 * 1024 * 1024 + 12345];
        new Random(42).nextBytes(data);
        List<byte[]> expected = FastCdc.chunksOf(data);
        InputStream trickle = new InputStream() {
            private final ByteArrayInputStream in = new ByteArrayInputStream(data);

            @Override
            public int read() {
                return in.read();
            }

            @Override
            public int read(byte[] b, int off, int len) {
                return in.read(b, off, Math.min(len, 1000));
            }
        };
        List<byte[]> streamed = new ArrayList<>();
        Iterator<byte[]> chunks = FastCdc.chunks(trickle);
        while (chunks.hasNext()) {
            streamed.add(chunks.next());
        }
        assertThat(streamed).hasSize(expected.size());
        for (int i = 0; i < expected.size(); i++) {
            assertThat(streamed.get(i)).as("chunk " + i).isEqualTo(expected.get(i));
        }
        List<byte[]> viaChannel = new ArrayList<>();
        Iterator<byte[]> channelChunks = FastCdc.chunks(Channels.newChannel(new ByteArrayInputStream(data)));
        channelChunks.forEachRemaining(viaChannel::add);
        assertThat(viaChannel).hasSize(expected.size());
        assertThat(viaChannel.get(viaChannel.size() - 1)).isEqualTo(expected.get(expected.size() - 1));
    }

    @Test
    void cutAtOffsetEqualsCutOfTheSlice() {
        byte[] data = vectorStream();
        int offset = 1_000_000;
        int length = 700_000;
        byte[] slice = Arrays.copyOfRange(data, offset, offset + length);
        assertThat(FastCdc.cut(data, offset, length)).isEqualTo(FastCdc.cut(slice, 0, length));
        assertThat(FastCdc.cut(data, 0, 0)).isZero();
    }
}
