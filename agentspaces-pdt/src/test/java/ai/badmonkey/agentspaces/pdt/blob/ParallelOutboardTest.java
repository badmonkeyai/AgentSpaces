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

import ai.badmonkey.agentspaces.pdt.kinds.BoNode;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * PDT 2 phase 4 measures, P2-B-05 (owner ruling of 2026-11-09, srad/agentspaces-pdt2.md §10.14): the identity and
 * the Bao outboard hash over the chunk groups on several threads, in pure Java, and the result equals the one-thread
 * result for every thread count and every size.
 */
class ParallelOutboardTest {

    private static final int GROUP = BoNode.GROUP_BYTES;
    private static final int LEAF = BoNode.LEAF_GROUPS * GROUP;
    private static final int MOST = 3 * LEAF + 5 * GROUP + 17;
    /** One seeded stream; each size hashes a prefix of it. */
    private static final byte[] DATA = seeded(MOST);

    private static byte[] seeded(int size) {
        byte[] bytes = new byte[size];
        new Random(2026_11_09L).nextBytes(bytes);
        return bytes;
    }

    /** The sizes: every edge of a chunk, a group, a hashing claim, a batch, and a leaf, and seeded sizes between. */
    private static List<Integer> sizes() {
        TreeSet<Integer> sizes = new TreeSet<>();
        int[] marks = {Blake3.CHUNK_BYTES, GROUP, 4 * GROUP, 16 * GROUP, 64 * GROUP, LEAF, 2 * LEAF, 3 * LEAF};
        sizes.add(0);
        for (int mark : marks) {
            for (int d = -1; d <= 1; d++) {
                sizes.add(mark + d);
            }
        }
        Random random = new Random(5);
        for (int i = 0; i < 16; i++) {
            sizes.add(random.nextInt(MOST + 1));
        }
        return new ArrayList<>(sizes);
    }

    private static BaoOutboard.Built outboard(int size, int threads) throws IOException {
        return BaoOutboard.of(new ByteArrayInputStream(DATA, 0, size), threads);
    }

    private static void assertSame(BaoOutboard.Built actual, BaoOutboard.Built expected, String what) {
        assertThat(actual.identity()).as("identity, %s", what).isEqualTo(expected.identity());
        assertThat(actual.root()).as("root, %s", what).isEqualTo(expected.root());
        assertThat(actual.size()).as("size, %s", what).isEqualTo(expected.size());
        assertThat(actual.nodes().keySet()).as("nodes, %s", what).containsExactlyElementsOf(expected.nodes().keySet());
        for (Map.Entry<String, byte[]> node : expected.nodes().entrySet()) {
            assertThat(actual.nodes().get(node.getKey())).as("node %s, %s", node.getKey(), what).isEqualTo(node.getValue());
        }
    }

    @Test
    void everyThreadCountGivesTheOneThreadIdentityAndOutboardAtEverySize() throws Exception {
        int cores = Runtime.getRuntime().availableProcessors();
        int[] threadCounts = {2, 3, 4, 5, 7, 8, cores, cores + 3};
        for (int size : sizes()) {
            BaoOutboard.Built one = outboard(size, 1);
            assertThat(one.identity()).as("%d bytes on one thread", size)
                    .isEqualTo(Blake3.identity(Blake3.digest(Arrays.copyOf(DATA, size))));
            for (int threads : threadCounts) {
                assertSame(outboard(size, threads), one, size + " bytes on " + threads + " threads");
            }
            assertSame(BaoOutboard.of(new ByteArrayInputStream(DATA, 0, size)), one, size + " bytes at the default");
        }
    }

    @Test
    void aStreamThatReadsAFewBytesAtATimeHashesAsOneThatReadsWhole() throws Exception {
        int size = 2 * LEAF + 3 * GROUP + 5;
        InputStream trickle = new FilterInputStream(new ByteArrayInputStream(DATA, 0, size)) {
            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                return super.read(b, off, Math.min(len, 7));
            }
        };
        assertSame(BaoOutboard.of(trickle, 4), outboard(size, 1), "a stream of 7-byte reads");
    }

    @Test
    void aThreadCountBelowOneIsRefused() {
        assertThatThrownBy(() -> outboard(10, 0)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("threads");
    }
}
