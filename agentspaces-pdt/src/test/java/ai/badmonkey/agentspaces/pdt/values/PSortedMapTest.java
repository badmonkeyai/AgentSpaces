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

import ai.badmonkey.agentspaces.pdt.Datom;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.kinds.DnNode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * PDT 2 phase 2, P2-D-11 (srad/agentspaces-pdt2.md §3.1.1): the sorted map over the {@code dn}
 * layout with a pluggable comparator, of which the three DataScript indexes are instances. A
 * branch holds the largest key of each child, as DataScript's sorted set writes it; a walk reads
 * only the nodes on its path; a diff reads only the nodes that differ.
 */
class PSortedMapTest {

    private static final long TX = 536870913L;

    private static List<Datom> people(int n) {
        List<Datom> out = new ArrayList<>();
        for (int e = 1; e <= n; e++) {
            out.add(new Datom(e, ":p/age", (long) (20 + e % 50), TX));
            out.add(new Datom(e, ":p/name", "person " + e, TX));
        }
        out.sort(DatomOrder.EAVT);
        return out;
    }

    private static NodeLoader counting(NodeStore store, AtomicInteger reads) {
        return cid -> {
            reads.incrementAndGet();
            return NodeLoader.of(store).load(cid);
        };
    }

    @Test
    void aBulkBuildWritesLeavesAndBranchesThatHoldTheLargestKeyOfEachChild() {
        NodeStore store = NodeStore.inMemory();
        List<Datom> sorted = people(500);
        PSortedMap map = PSortedMap.of(store, DnNode.EAVT, DatomOrder.EAVT, 128, sorted);
        String root = map.root();
        DnNode top = DnNode.decode(root, store.get(root).orElseThrow());
        assertThat(top.level()).isEqualTo(1);
        assertThat(top.index()).isEqualTo(DnNode.EAVT);
        assertThat(top.children()).hasSize(8);
        int total = 0;
        for (int i = 0; i < top.children().size(); i++) {
            DnNode leaf = DnNode.decode(null, store.get(top.children().get(i).cid()).orElseThrow());
            assertThat(leaf.level()).isZero();
            assertThat(leaf.datoms().size()).as("leaves hold between half and all of the branching factor").isBetween(64, 128);
            assertThat(top.datoms().get(i)).isEqualTo(leaf.datoms().get(leaf.datoms().size() - 1));
            total += leaf.datoms().size();
        }
        assertThat(total).isEqualTo(1000);
        PSortedMap back = PSortedMap.load(store, NodeLoader.of(store), DnNode.EAVT, DatomOrder.EAVT, root);
        List<Datom> read = new ArrayList<>();
        back.iterator().forEachRemaining(read::add);
        assertThat(read).isEqualTo(sorted);
        assertThat(back.count()).isEqualTo(1000);
        assertThat(PSortedMap.of(store, DnNode.EAVT, DatomOrder.EAVT, 128, sorted).root()).as("one root per content").isEqualTo(root);
        assertThatThrownBy(() -> PSortedMap.of(store, DnNode.EAVT, DatomOrder.EAVT, 128, List.of(sorted.get(1), sorted.get(0))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anEmptyMapIsOneEmptyLeafAndASmallOneIsOneLeaf() {
        NodeStore store = NodeStore.inMemory();
        PSortedMap empty = PSortedMap.of(store, DnNode.AVET, DatomOrder.AVET, 128, List.of());
        DnNode leaf = DnNode.decode(null, store.get(empty.root()).orElseThrow());
        assertThat(leaf.datoms()).isEmpty();
        assertThat(empty.count()).isZero();
        assertThat(empty.iterator().hasNext()).isFalse();
        PSortedMap small = PSortedMap.of(store, DnNode.EAVT, DatomOrder.EAVT, 128, people(3));
        assertThat(DnNode.decode(null, store.get(small.root()).orElseThrow()).level()).isZero();
    }

    @Test
    void aSeekReadsOnlyThePathToItsFirstKey() {
        NodeStore store = NodeStore.inMemory();
        List<Datom> sorted = people(20_000);
        String root = PSortedMap.of(store, DnNode.EAVT, DatomOrder.EAVT, 128, sorted).root();
        AtomicInteger reads = new AtomicInteger();
        PSortedMap map = PSortedMap.load(store, counting(store, reads), DnNode.EAVT, DatomOrder.EAVT, root);
        Datom probe = new Datom(12_345, ":p/name", "person 12345", TX);
        assertThat(map.contains(probe)).isTrue();
        assertThat(reads.get()).as("depth 3: the root, a branch, a leaf").isEqualTo(3);
        reads.set(0);
        var it = map.seek(new Datom(7_000, ":p/age", Long.MIN_VALUE, 1));
        assertThat(it.next()).isEqualTo(new Datom(7_000, ":p/age", 20L + 7_000 % 50, TX));
        assertThat(it.next().a()).isEqualTo(":p/name");
        assertThat(reads.get()).isEqualTo(3);
        assertThat(map.contains(new Datom(12_345, ":p/name", "someone else", TX))).isFalse();
    }

    @Test
    void aDiffReadsOnlyTheNodesThatDifferAndNamesTheChangedKeys() {
        NodeStore store = NodeStore.inMemory();
        List<Datom> sorted = people(20_000);
        String before = PSortedMap.of(store, DnNode.EAVT, DatomOrder.EAVT, 128, sorted).root();
        List<Datom> changed = new ArrayList<>(sorted);
        Datom gone = sorted.get(30_001);
        Datom added = new Datom(15_000, ":p/name", "renamed", TX + 1);
        changed.remove(gone);
        changed.add(added);
        changed.sort(DatomOrder.EAVT);
        String after = PSortedMap.of(store, DnNode.EAVT, DatomOrder.EAVT, 128, changed).root();
        AtomicInteger reads = new AtomicInteger();
        NodeLoader loader = counting(store, reads);
        PSortedMap a = PSortedMap.load(store, loader, DnNode.EAVT, DatomOrder.EAVT, before);
        PSortedMap b = PSortedMap.load(store, loader, DnNode.EAVT, DatomOrder.EAVT, after);
        PSortedMap.Delta delta = a.diff(b);
        assertThat(delta.removed()).containsExactly(gone);
        assertThat(delta.added()).containsExactly(added);
        // the count stays, so a bulk build keeps its leaf boundaries and only the paths to the changed leaves differ
        assertThat(reads.get()).as("two roots, the changed branches and leaves, of 317 nodes").isLessThanOrEqualTo(8);
        assertThat(a.diff(a).added()).isEmpty();
    }
}
