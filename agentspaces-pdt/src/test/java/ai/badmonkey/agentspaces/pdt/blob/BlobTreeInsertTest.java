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

import ai.badmonkey.agentspaces.pdt.NodeStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.Arrays;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PD-B-06 at CI scale: 200 inserts of 100 bytes at seeded random offsets of a 32 MiB
 * random blob, each applied to the result of the one before; the new chunk and interior
 * bytes per insert stay under 160 KiB at the median and under 1 MiB at the 99th
 * percentile. The row's own run, 256 MiB and 1,000 inserts, is the second test, which
 * runs only with {@code -Dpdt.slow=true} (and a heap of about 2 GiB, for example
 * {@code -DargLine=-Xmx3g}); its figures are recorded in srad §10.3.
 */
class BlobTreeInsertTest {

    private static final int MIB = 1024 * 1024;
    private static final int INSERT_BYTES = 100;

    @Test
    void insertsOfOneHundredBytesShipSmallDeltas() {
        run(32 * MIB, 200, true);
    }

    @Test
    @EnabledIfSystemProperty(named = "pdt.slow", matches = "true")
    void theRowsRunOfTwoHundredFiftySixMebibytesAndAThousandInserts() {
        run(256 * MIB, 1000, false);
    }

    /**
     * Runs the inserts and asserts the row's bounds. With {@code keepReference} the
     * whole content is kept as an array and compared at the end; without it each
     * insert is checked against the previous value's bytes around the offset and the
     * final value is compared with the from-scratch root of its own bytes.
     */
    private static void run(int size, int inserts, boolean keepReference) {
        NodeStore store = NodeStore.inMemory();
        Random random = new Random(42);
        byte[] reference = new byte[size];
        random.nextBytes(reference);
        PBlob blob = PBlob.of(reference, store);
        if (!keepReference) {
            reference = null;
        }
        long baseBytes = store.bytesUsed();
        long[] written = new long[inserts];
        long[] chunkBytes = new long[inserts];
        long[] nodeBytes = new long[inserts];
        int[] chunksWritten = new int[inserts];
        long started = System.nanoTime();
        for (int i = 0; i < inserts; i++) {
            long current = blob.size();
            int offset = random.nextInt((int) current + 1);
            byte[] insert = new byte[INSERT_BYTES];
            random.nextBytes(insert);
            int from = Math.max(0, offset - 100);
            byte[] before = blob.read(from, offset - from);
            byte[] after = blob.read(offset, 100);
            PBlob next = blob.splice(offset, 0, insert);
            written[i] = next.writtenBytes();
            chunkBytes[i] = next.writtenChunkBytes();
            nodeBytes[i] = next.writtenNodeBytes();
            chunksWritten[i] = next.writtenChunks();
            assertThat(next.size()).isEqualTo(current + INSERT_BYTES);
            byte[] window = new byte[before.length + INSERT_BYTES + after.length];
            System.arraycopy(before, 0, window, 0, before.length);
            System.arraycopy(insert, 0, window, before.length, INSERT_BYTES);
            System.arraycopy(after, 0, window, before.length + INSERT_BYTES, after.length);
            assertThat(next.read(from, window.length)).isEqualTo(window);
            if (reference != null) {
                byte[] grown = new byte[reference.length + INSERT_BYTES];
                System.arraycopy(reference, 0, grown, 0, offset);
                System.arraycopy(insert, 0, grown, offset, INSERT_BYTES);
                System.arraycopy(reference, offset, grown, offset + INSERT_BYTES, reference.length - offset);
                reference = grown;
            }
            blob = next;
        }
        long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
        long[] sorted = written.clone();
        Arrays.sort(sorted);
        long median = sorted[inserts / 2];
        long p99 = sorted[(int) Math.ceil(0.99 * inserts) - 1];
        long max = sorted[inserts - 1];
        long total = Arrays.stream(written).sum();
        int oneChunk = 0;
        for (int i = 0; i < inserts; i++) {
            if (chunksWritten[i] == 1) {
                oneChunk++;
            }
        }
        System.out.println("PD-B-06 at " + size / MIB + " MiB, " + inserts + " inserts of " + INSERT_BYTES + " bytes: median " + median
                + " bytes, p99 " + p99 + " bytes, max " + max + " bytes, mean " + total / inserts
                + " bytes; " + oneChunk + " inserts rewrote exactly one chunk; interior bytes mean "
                + Arrays.stream(nodeBytes).sum() / inserts + ", chunk bytes mean " + Arrays.stream(chunkBytes).sum() / inserts
                + "; store grew from " + baseBytes + " to " + store.bytesUsed() + "; " + elapsedMillis + " ms");
        assertThat(median).isLessThan(160 * 1024L);
        assertThat(p99).isLessThan(MIB);
        assertThat(store.bytesUsed() - baseBytes).isEqualTo(total);
        byte[] content = reference != null ? reference : blob.bytes();
        if (reference != null) {
            assertThat(blob.bytes()).isEqualTo(reference);
        }
        assertThat(PBlob.of(content, NodeStore.inMemory()).root()).isEqualTo(blob.root());
    }
}
