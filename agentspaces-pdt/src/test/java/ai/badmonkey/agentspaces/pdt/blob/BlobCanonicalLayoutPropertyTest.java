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

import java.util.Arrays;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PD-Q-TST-02 for blobs (PD-B-03): over 200 seeded sequences of splices, deletions,
 * replacements, and appends on a 1 to 2 MiB random blob, the edited value has the
 * root that chunking its content from scratch gives, so the layout depends on the
 * content alone.
 */
class BlobCanonicalLayoutPropertyTest {

    private static final int CASES = 200;
    private static final int MIB = 1024 * 1024;

    private static byte[] spliced(byte[] content, int offset, int remove, byte[] insert) {
        byte[] out = new byte[content.length - remove + insert.length];
        System.arraycopy(content, 0, out, 0, offset);
        System.arraycopy(insert, 0, out, offset, insert.length);
        System.arraycopy(content, offset + remove, out, offset + insert.length, content.length - offset - remove);
        return out;
    }

    @Test
    void editSequencesGiveTheFromScratchRoot() {
        Random random = new Random(42);
        NodeStore store = NodeStore.inMemory();
        int ops = 0;
        for (int c = 0; c < CASES; c++) {
            byte[] expected = new byte[MIB + random.nextInt(MIB)];
            random.nextBytes(expected);
            PBlob blob = PBlob.of(expected, store);
            int steps = 1 + random.nextInt(5);
            for (int s = 0; s < steps; s++) {
                int offset = random.nextInt(expected.length + 1);
                int kind = random.nextInt(4);
                int remove = kind == 0 ? 0 : Math.min(expected.length - offset, 1 + random.nextInt(200_000));
                byte[] insert = kind == 1 ? new byte[0] : new byte[1 + random.nextInt(kind == 3 ? 300_000 : 2_000)];
                random.nextBytes(insert);
                if (kind == 3) {
                    offset = expected.length;
                    remove = 0;
                    blob = blob.append(insert);
                } else {
                    blob = blob.splice(offset, remove, insert);
                }
                expected = spliced(expected, offset, remove, insert);
                ops++;
                assertThat(blob.size()).as("case %d step %d", c, s).isEqualTo(expected.length);
            }
            assertThat(blob.root()).as("case %d after %d edits", c, steps).isEqualTo(PBlob.of(expected, store).root());
            assertThat(blob.bytes()).as("case %d content", c).isEqualTo(expected);
            assertThat(Arrays.equals(blob.read(0, 100), Arrays.copyOf(expected, 100))).isTrue();
        }
        assertThat(ops).isGreaterThan(CASES * 2);
    }
}
