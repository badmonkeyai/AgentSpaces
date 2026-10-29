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
package ai.badmonkey.agentspaces.peering.blocks;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** The file-backed block store (LB-B-05): put, get, restart, order, remove. */
class FileBlockStoreTest {

    @Test
    void blocksSurviveARestartWithTheirSizesAndOrder(@TempDir Path dir) throws Exception {
        FileBlockStore store = new FileBlockStore(dir);
        byte[] a = "alpha".repeat(1000).getBytes();
        byte[] b = "beta".repeat(500).getBytes();
        String ca = BlockExchange.cidOf(a);
        String cb = BlockExchange.cidOf(b);
        assertThat(store.put(ca, a, 1_000)).isTrue();
        assertThat(store.put(cb, b, 2_000)).isTrue();
        assertThat(store.put(ca, a, 3_000)).as("a second put only touches").isFalse();
        assertThat(store.lastAccessMillis(ca)).isEqualTo(3_000);
        assertThat(store.bytesUsed()).isEqualTo(a.length + b.length);
        assertThat(store.get(cb, 4_000)).contains(b);
        assertThat(store.get("zmissing", 4_000)).isEmpty();
        assertThat(Files.list(dir).filter(p -> p.getFileName().toString().endsWith(".tmp")).count()).isZero();

        FileBlockStore reopened = new FileBlockStore(dir);
        assertThat(reopened.cids()).containsExactlyInAnyOrder(ca, cb);
        assertThat(reopened.bytesUsed()).isEqualTo(a.length + b.length);
        assertThat(reopened.get(ca, 5_000)).contains(a);
        assertThat(reopened.lastAccessMillis(cb)).as("the file's modification time stands in for the clock")
                .isGreaterThan(0);
        assertThat(reopened.remove(ca)).isEqualTo(a.length);
        assertThat(reopened.remove(ca)).isEqualTo(-1);
        assertThat(reopened.size()).isEqualTo(1);
        assertThat(Files.exists(dir.resolve(ca))).isFalse();
        assertThat(reopened.kind()).isEqualTo("file");
        assertThat(new MemoryBlockStore().kind()).isEqualTo("memory");
    }
}
