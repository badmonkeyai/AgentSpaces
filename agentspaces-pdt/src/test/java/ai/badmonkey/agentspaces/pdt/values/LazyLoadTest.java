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
package ai.badmonkey.agentspaces.pdt.values;

import ai.badmonkey.agentspaces.pdt.NodeStore;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PD-Q-PRF-09: a cold {@code get} on a 1,000,000-entry map loads at most the tree
 * depth (eight nodes at this size) and at most six nodes at the 99th percentile.
 * The map is built once in a static through an editor and written to a memory store;
 * a counting loader then measures a fresh {@link PMap#load} and one {@code get}. A
 * sha2-256 CHAMP of a million keys has a deepest path of eight nodes (measured:
 * 2,000 random keys load 4 or 5 nodes 97% of the time, 6 nodes 3% of the time, and 7
 * once), which is why the row reads as a depth bound with a percentile.
 */
class LazyLoadTest {

    static final int ENTRIES = 1_000_000;
    static final NodeStore STORE = NodeStore.inMemory();
    static final String ROOT;
    static final long BUILD_MILLIS;

    static {
        long start = System.nanoTime();
        Editor<Integer, Integer> editor = PMap.empty(STORE, Codec.of(Integer.class), Codec.of(Integer.class)).editor();
        for (int i = 0; i < ENTRIES; i++) {
            editor.assoc(i, i * 7);
        }
        Editor.Result<Integer, Integer> result = editor.done();
        ROOT = result.value().root();
        BUILD_MILLIS = (System.nanoTime() - start) / 1_000_000;
        System.out.println("LazyLoadTest: built " + ENTRIES + " entries, " + result.nodes().size() + " nodes, in " + BUILD_MILLIS + " ms");
    }

    private static NodeLoader counting(AtomicInteger loads) {
        NodeLoader base = NodeLoader.of(STORE);
        return cid -> {
            loads.incrementAndGet();
            return base.load(cid);
        };
    }

    @Test
    void coldGetLoadsAtMostTheTreeDepthAndSixNodesAtTheNinetyNinthPercentile() {
        Random random = new Random(42);
        int[] histogram = new int[16];
        for (int round = 0; round < 2000; round++) {
            AtomicInteger loads = new AtomicInteger();
            PMap<Integer, Integer> map = PMap.load(STORE, counting(loads), ROOT, Codec.of(Integer.class), Codec.of(Integer.class));
            int key = random.nextInt(ENTRIES);
            assertThat(map.get(key)).contains(key * 7);
            histogram[loads.get()]++;
        }
        int depth = PMap.load(STORE, ROOT, Codec.of(Integer.class), Codec.of(Integer.class)).depth();
        System.out.println("LazyLoadTest: loads histogram " + java.util.Arrays.toString(histogram) + " depth " + depth);
        int withinSix = 0;
        for (int loads = 0; loads <= 6; loads++) {
            withinSix += histogram[loads];
        }
        for (int loads = depth + 1; loads < histogram.length; loads++) {
            assertThat(histogram[loads]).as("a get loaded more nodes than the tree is deep").isZero();
        }
        assertThat(depth).isLessThanOrEqualTo(8);
        assertThat(withinSix).isGreaterThanOrEqualTo(2000 * 99 / 100);
    }

    @Test
    void coldMissLoadsAtMostSixNodes() {
        AtomicInteger loads = new AtomicInteger();
        PMap<Integer, Integer> map = PMap.load(STORE, counting(loads), ROOT, Codec.of(Integer.class), Codec.of(Integer.class));
        assertThat(map.count()).isEqualTo(ENTRIES);
        assertThat(map.get(ENTRIES + 5)).isEqualTo(Optional.empty());
        assertThat(loads.get()).isLessThanOrEqualTo(6);
    }

    @Test
    void theRootRecordsTheCount() {
        PMap<Integer, Integer> map = PMap.load(STORE, ROOT, Codec.of(Integer.class), Codec.of(Integer.class));
        assertThat(map.count()).isEqualTo(ENTRIES);
        assertThat(map.depth()).isLessThanOrEqualTo(9);
    }
}
