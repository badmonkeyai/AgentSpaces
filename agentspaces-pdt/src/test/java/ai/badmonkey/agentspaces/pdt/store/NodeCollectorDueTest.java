/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.badmonkey.agentspaces.pdt.store;

import ai.badmonkey.agentspaces.pdt.NodeStore;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A collector starts a cycle only when one could remove something (idle-loop O3): a node stored since the last cycle,
 * or a root that last cycle marked from and that is no root now. An idle store is not walked every minute.
 */
class NodeCollectorDueTest {

    private final Random random = new Random(7);
    private final NodeStore store = NodeStore.inMemory();
    private final Set<String> heads = ConcurrentHashMap.newKeySet();
    private final NodeCollector collector = new NodeCollector(store, () -> new HashSet<>(heads), null);

    private String version() {
        String a = TreeFixtures.putChunk(store, TreeFixtures.chunk(random, 24));
        String b = TreeFixtures.putChunk(store, TreeFixtures.chunk(random, 24));
        return TreeFixtures.putParent(store, 1, List.of(a, b));
    }

    @Test
    void anUnchangedStoreIsNotWalkedAgain() {
        heads.add(version());
        assertThat(collector.due()).as("no cycle has run yet").isTrue();
        collector.collect();
        for (int i = 0; i < 5; i++) {
            assertThat(collector.due()).isFalse();
        }
        assertThat(collector.cyclesSkipped()).isEqualTo(5);
    }

    @Test
    void aNodeStoredSinceTheLastCycleMakesOneDueAndItIsCollected() {
        heads.add(version());
        collector.collect();
        String orphan = TreeFixtures.putChunk(store, TreeFixtures.chunk(random, 16));
        assertThat(collector.due()).isTrue();
        collector.collect();
        assertThat(store.has(orphan)).isFalse();
        assertThat(collector.due()).isFalse();
    }

    @Test
    void aRootThatStoppedBeingOneMakesOneDueAndItsNodesGo() {
        String old = version();
        heads.add(old);
        collector.collect();
        String next = version();
        heads.add(next);
        collector.collect();
        heads.remove(old); // the ref moved on: the old version is garbage, with nothing newly stored
        assertThat(collector.due()).isTrue();
        collector.collect();
        assertThat(store.has(old)).isFalse();
        assertThat(store.has(next)).isTrue();
    }

    @Test
    void anUnpinnedRootMakesOneDue() {
        String pinned = version();
        store.pin(pinned);
        collector.collect();
        assertThat(collector.due()).isFalse();
        store.unpin(pinned);
        assertThat(collector.due()).isTrue();
    }

    @Test
    void aCycleRunsAnywayAfterTheBackstopCountOfSkips() {
        heads.add(version());
        collector.collect();
        for (int i = 0; i < NodeCollector.FORCE_CYCLE_AFTER_SKIPS; i++) {
            assertThat(collector.due()).isFalse();
        }
        assertThat(collector.due()).isTrue();
        collector.collect();
        assertThat(collector.due()).isFalse();
    }
}
