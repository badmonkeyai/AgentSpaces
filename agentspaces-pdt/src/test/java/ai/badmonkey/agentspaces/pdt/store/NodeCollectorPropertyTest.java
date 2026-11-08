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
package ai.badmonkey.agentspaces.pdt.store;

import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PD-Q-REL-05 and PD-S-04 for {@link NodeCollector}: over 1,000 seeded random histories
 * of versions with shared subtrees, after a collection every node reachable from a mark
 * root is present and every other node is gone, whatever the tick budget and whatever
 * roots arrive mid-cycle.
 */
class NodeCollectorPropertyTest {

    /** A version: its root and the leaves it shares with earlier ones. */
    private record History(List<String> versionRoots, Set<String> orphans, Set<String> marks, Set<String> heads,
                           Set<String> retainedRoots, List<String> unreferenced) { }

    private static History randomHistory(NodeStore store, Retention retention, Random random) {
        List<String> leaves = new ArrayList<>();
        List<String> subtrees = new ArrayList<>();
        List<String> versionRoots = new ArrayList<>();
        int versions = 1 + random.nextInt(12);
        for (int v = 0; v < versions; v++) {
            int newLeaves = random.nextInt(6);
            for (int i = 0; i < newLeaves; i++) {
                leaves.add(TreeFixtures.putChunk(store, TreeFixtures.chunk(random, 8 + random.nextInt(40))));
            }
            List<String> children = new ArrayList<>();
            int width = 1 + random.nextInt(5);
            for (int i = 0; i < width && !leaves.isEmpty(); i++) {
                children.add(leaves.get(random.nextInt(leaves.size())));
            }
            if (!subtrees.isEmpty() && random.nextBoolean()) {
                children.add(subtrees.get(random.nextInt(subtrees.size())));
            }
            String subtree = children.isEmpty()
                    ? TreeFixtures.putChunk(store, TreeFixtures.chunk(random, 8))
                    : TreeFixtures.putParent(store, 1, new ArrayList<>(new LinkedHashSet<>(children)));
            subtrees.add(subtree);
            List<String> rootChildren = new ArrayList<>(List.of(subtree));
            if (v > 0 && random.nextInt(3) > 0) {
                rootChildren.add(versionRoots.get(random.nextInt(versionRoots.size())));
            }
            versionRoots.add(TreeFixtures.putParent(store, 2, rootChildren));
        }
        Set<String> orphans = new HashSet<>();
        int orphanCount = random.nextInt(5);
        for (int i = 0; i < orphanCount; i++) {
            orphans.add(TreeFixtures.putChunk(store, TreeFixtures.chunk(random, 8 + random.nextInt(40))));
        }
        Set<String> marks = new LinkedHashSet<>();
        Set<String> heads = new LinkedHashSet<>();
        Set<String> retainedRoots = new LinkedHashSet<>();
        List<String> unreferenced = new ArrayList<>();
        for (String root : versionRoots) {
            switch (random.nextInt(5)) {
                case 0 -> {
                    store.pin(root);
                    marks.add(root);
                }
                case 1 -> {
                    heads.add(root);
                    marks.add(root);
                }
                case 2 -> {
                    retention.recordVersion("ref-" + random.nextInt(2), root, 0);
                    retainedRoots.add(root);
                    marks.add(root);
                }
                default -> unreferenced.add(root);
            }
        }
        return new History(versionRoots, orphans, marks, heads, retainedRoots, unreferenced);
    }

    private static Set<String> expectedLive(NodeStore store, Set<String> marks) {
        Set<String> live = new HashSet<>();
        for (String root : marks) {
            live.addAll(store.reachable(root));
        }
        return live;
    }

    @Test
    void aThousandRandomHistoriesKeepEveryReachableNodeAndDropTheRest() {
        Random random = new Random(42);
        int midCyclePins = 0;
        int totalRemoved = 0;
        int multiTickCycles = 0;
        for (int history = 0; history < 1000; history++) {
            NodeStore store = NodeStore.inMemory();
            Retention retention = new Retention(64, Duration.ofHours(1), InstantSource.system());
            History h = randomHistory(store, retention, random);
            AtomicReference<Set<String>> heads = new AtomicReference<>(h.heads());
            int budget = random.nextInt(4) == 0 ? NodeCollector.DEFAULT_VISITS_PER_TICK : 1 + random.nextInt(20);
            NodeCollector collector = new NodeCollector(store, heads::get, retention, budget);
            Set<String> allBefore = store.backend().cids();
            Set<String> marks = new LinkedHashSet<>(h.marks());

            NodeCollector.Tick first = collector.tick();
            int ticks = 1;
            if (!first.cycleComplete() && !h.unreferenced().isEmpty() && random.nextInt(3) == 0) {
                String late = h.unreferenced().get(random.nextInt(h.unreferenced().size()));
                if (!store.complete(late)) {
                    // the sweep already took part of it before anyone wanted it; a pin cannot bring that back
                    late = null;
                }
                if (late == null) {
                    // nothing to add this history
                } else if (random.nextBoolean()) {
                    store.pin(late);
                } else {
                    Set<String> more = new LinkedHashSet<>(heads.get());
                    more.add(late);
                    heads.set(more);
                }
                if (late != null) {
                    marks.add(late);
                    midCyclePins++;
                }
            }
            NodeCollector.Tick tick = first;
            while (!tick.cycleComplete()) {
                tick = collector.tick();
                ticks++;
                assertThat(tick.visited()).isLessThanOrEqualTo(budget);
            }
            if (ticks > 1) {
                multiTickCycles++;
            }
            Set<String> live = expectedLive(store, marks);
            Set<String> after = store.backend().cids();
            assertThat(after).as("history %d: every reachable node survives", history).containsAll(live);
            assertThat(after).as("history %d: every unreachable node is gone", history).isSubsetOf(live);
            for (String root : marks) {
                assertThat(store.complete(root)).as("history %d root %s complete", history, root).isTrue();
            }
            totalRemoved += allBefore.size() - after.size();
            assertThat(collector.phase()).isEqualTo(NodeCollector.Phase.IDLE);

            NodeCollector.Cycle again = collector.collect();
            assertThat(again.removed()).as("history %d: a second cycle removes nothing", history).isZero();
            assertThat(store.backend().cids()).isEqualTo(after);
        }
        assertThat(midCyclePins).isGreaterThan(50);
        assertThat(multiTickCycles).isGreaterThan(300);
        assertThat(totalRemoved).isGreaterThan(1000);
    }

    @Test
    void aTickVisitsAtMostTheBudgetAndResumes() {
        Random random = new Random(42);
        NodeStore store = NodeStore.inMemory();
        String root = TreeFixtures.putTree(store, random, 200, 10, 20);
        for (int i = 0; i < 100; i++) {
            TreeFixtures.putChunk(store, TreeFixtures.chunk(random, 20));
        }
        store.pin(root);
        NodeCollector collector = new NodeCollector(store, Set::of, null, 50);
        int ticks = 0;
        NodeCollector.Tick tick;
        do {
            tick = collector.tick();
            ticks++;
            assertThat(tick.visited()).isLessThanOrEqualTo(50);
        } while (!tick.cycleComplete());
        assertThat(ticks).isGreaterThanOrEqualTo((221 + 321) / 50);
        assertThat(store.size()).isEqualTo(221);
        assertThat(store.complete(root)).isTrue();
    }

    @Test
    void defaultBudgetIsTenThousandAndCollectReportsTheCycle() {
        Random random = new Random(42);
        NodeStore store = NodeStore.inMemory();
        String root = TreeFixtures.putTree(store, random, 30, 5, 20);
        String garbageRoot = TreeFixtures.putTree(store, random, 10, 5, 20);
        store.pin(root);
        NodeCollector collector = new NodeCollector(store, Set::of, null);
        assertThat(collector.visitsPerTick()).isEqualTo(10_000);
        NodeCollector.Cycle cycle = collector.collect();
        assertThat(cycle.ticks()).isEqualTo(1);
        assertThat(cycle.marked()).isEqualTo(30 + 6 + 1);
        assertThat(cycle.removed()).isEqualTo(10 + 2 + 1);
        assertThat(cycle.bytesFreed()).isGreaterThan(200);
        assertThat(store.has(garbageRoot)).isFalse();
        assertThat(store.complete(root)).isTrue();
    }

    /**
     * PD-S-04: a root pinned while a sweep tick runs (here, from inside the backend's
     * first removal, standing in for another thread) is marked before anything under
     * it is removed, within the same tick.
     */
    @Test
    void aRootPinnedDuringASweepTickKeepsItsSubtree() {
        Random random = new Random(42);
        NodeStore inner = NodeStore.inMemory();
        List<String> garbage = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            garbage.add(TreeFixtures.putChunk(inner, TreeFixtures.chunk(random, 20)));
        }
        String chunk = TreeFixtures.putChunk(inner, TreeFixtures.chunk(random, 30));
        String bRoot = TreeFixtures.putParent(inner, 1, List.of(chunk));
        List<String> order = new ArrayList<>(garbage);
        order.add(bRoot);
        order.add(chunk);
        NodeStore[] holder = new NodeStore[1];
        ai.badmonkey.agentspaces.peering.blocks.BlockStore ordered = new ai.badmonkey.agentspaces.peering.blocks.BlockStore() {
            private boolean pinnedOnce;

            @Override
            public java.util.Optional<byte[]> get(String cid, long nowMillis) {
                return inner.backend().get(cid, nowMillis);
            }

            @Override
            public boolean put(String cid, byte[] bytes, long nowMillis) {
                return inner.backend().put(cid, bytes, nowMillis);
            }

            @Override
            public long remove(String cid) {
                if (!pinnedOnce) {
                    pinnedOnce = true;
                    holder[0].pin(bRoot);
                }
                return inner.backend().remove(cid);
            }

            @Override
            public Set<String> cids() {
                return new LinkedHashSet<>(order);
            }

            @Override
            public long lastAccessMillis(String cid) {
                return inner.backend().lastAccessMillis(cid);
            }

            @Override
            public int size() {
                return inner.backend().size();
            }

            @Override
            public long bytesUsed() {
                return inner.backend().bytesUsed();
            }

            @Override
            public String kind() {
                return "ordered";
            }
        };
        NodeStore store = new NodeStore(ordered, InstantSource.system());
        holder[0] = store;
        NodeCollector collector = new NodeCollector(store, Set::of, null);
        NodeCollector.Tick tick = collector.tick();
        assertThat(tick.cycleComplete()).isTrue();
        assertThat(tick.removed()).as("the five garbage chunks go; the root pinned mid-sweep and its chunk stay").isEqualTo(5);
        assertThat(store.pinned()).containsExactly(bRoot);
        assertThat(store.has(bRoot)).isTrue();
        assertThat(store.has(chunk)).isTrue();
        assertThat(store.complete(bRoot)).isTrue();
        for (String g : garbage) {
            assertThat(store.has(g)).isFalse();
        }
    }

    @Test
    void anUndecodableNodeIsKeptAndCounted() {
        NodeStore store = NodeStore.inMemory();
        byte[] junk = {(byte) 0xa1, 0x61, 0x6b, 0x61, 0x71};
        String cid = Cids.of(junk, Cids.DAG_CBOR);
        store.backend().put(cid, junk, 1);
        store.pin(cid);
        NodeCollector collector = new NodeCollector(store, Set::of, null);
        NodeCollector.Tick tick = collector.tick();
        assertThat(tick.undecodable()).isEqualTo(1);
        assertThat(tick.cycleComplete()).isTrue();
        assertThat(store.has(cid)).isTrue();
    }
}
