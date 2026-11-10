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

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.pdt.Datom;
import ai.badmonkey.agentspaces.pdt.Kind;
import ai.badmonkey.agentspaces.pdt.Link;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.kinds.DbRoot;
import ai.badmonkey.agentspaces.pdt.kinds.DnNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PDT 2 phase 2, P2-D-02 to P2-D-04 and P2-D-06 (srad/agentspaces-pdt2.md §3.1): the value of a
 * {@code db} ref. The writer the DataScript adapter calls writes index nodes, the tail, and the
 * root in one batch, spilling a value above 1 KiB as PDT 1 does; a load reads the root alone; the
 * changes since a base follow the index diff and both tails.
 */
class PDbTest {

    private static final long TX = PDb.TX0 + 1;
    private static final byte[] SCHEMA = CborCodec.defaultCodec().toBytes(Map.of(":p/name", Map.of(":db/index", true)));

    private static NodeLoader counting(NodeStore store, AtomicInteger reads) {
        return cid -> {
            reads.incrementAndGet();
            return NodeLoader.of(store).load(cid);
        };
    }

    /** A db over one leaf per index holding {@code index} and a tail holding {@code tail}. */
    private static PDb db(NodeStore store, List<Datom> index, List<Datom> tail, long maxTx) {
        PDb.Writer writer = PDb.writer(store, NodeLoader.of(store));
        String eavt = writer.node(DnNode.EAVT, 0, index.stream().sorted(DatomOrder.EAVT).toList(), List.of());
        String aevt = writer.node(DnNode.AEVT, 0, index.stream().sorted(DatomOrder.AEVT).toList(), List.of());
        String avet = writer.node(DnNode.AVET, 0, List.of(), List.of());
        return writer.root(SCHEMA, 9, maxTx, eavt, aevt, avet, writer.tail(tail));
    }

    @Test
    void theEmptyDatabaseIsARootOverThreeEmptyLeaves() {
        NodeStore store = NodeStore.inMemory();
        PDb empty = PDb.empty(store, NodeLoader.of(store));
        DbRoot root = DbRoot.decode(empty.root(), store.get(empty.root()).orElseThrow());
        assertThat(root.maxEid()).isZero();
        assertThat(root.maxTx()).isEqualTo(PDb.TX0);
        assertThat(root.branching()).isEqualTo(DbRoot.BRANCHING);
        assertThat(root.tail()).isNull();
        assertThat(DnNode.decode(null, store.get(root.eavt().cid()).orElseThrow()).datoms()).isEmpty();
        assertThat(empty.size()).isZero();
        assertThat(empty.tail()).isEmpty();
        assertThat(PDb.empty(store, NodeLoader.of(store)).root()).isEqualTo(empty.root());
    }

    @Test
    void aLoadReadsTheRootAloneAndTheTailOnRequest() {
        NodeStore store = NodeStore.inMemory();
        Datom ada = new Datom(1, ":p/name", "Ada", TX);
        Datom retract = new Datom(1, ":p/name", "Ada", -(TX + 1));
        Datom grace = new Datom(1, ":p/name", "Grace", TX + 1);
        PDb written = db(store, List.of(ada), List.of(retract, grace), TX + 1);
        AtomicInteger reads = new AtomicInteger();
        PDb loaded = PDb.load(store, counting(store, reads), written.root());
        assertThat(loaded.node().maxTx()).isEqualTo(TX + 1);
        assertThat(reads.get()).as("P2-D-04: the root and nothing else").isEqualTo(1);
        assertThat(loaded.size()).isEqualTo(2);
        assertThat(loaded.tail()).containsExactly(retract, grace);
        assertThat(reads.get()).isEqualTo(2);
        assertThat(loaded.read(loaded.node().eavt().cid()).datoms()).containsExactly(ada);
        assertThat(PDb.writer(store, NodeLoader.of(store)).tail(List.of())).as("no tail, no node").isNull();
    }

    @Test
    void aValueAboveOneKibSpillsToAnXNodeAndReadsBackWhole() {
        NodeStore store = NodeStore.inMemory();
        String svg = "<svg>" + "x".repeat(5000) + "</svg>";
        Datom chart = new Datom(3, ":chart/svg", svg, TX);
        PDb written = db(store, List.of(chart), List.of(), TX);
        String eavt = written.node().eavt().cid();
        byte[] leaf = store.get(eavt).orElseThrow();
        assertThat(leaf.length).as("the leaf holds a link, not the text").isLessThan(200);
        Object v = DnNode.decode(eavt, leaf).datoms().get(0).v();
        assertThat(v).isInstanceOf(Link.class);
        assertThat(NodeCodec.kindOf(store.get(((Link) v).cid()).orElseThrow())).isEqualTo(Kind.X);
        assertThat(NodeCodec.links(eavt, leaf)).containsExactly(((Link) v).cid());
        assertThat(written.read(eavt).datoms()).containsExactly(chart);
        assertThat(written.index(DnNode.EAVT).contains(chart)).isTrue();
        assertThat(store.complete(written.root())).isTrue();
    }

    @Test
    void theChangesSinceABaseFollowTheIndexDiffAndBothTails() {
        NodeStore store = NodeStore.inMemory();
        Datom a = new Datom(1, ":p/name", "A", TX);
        Datom b = new Datom(2, ":p/name", "B", TX);
        Datom c = new Datom(3, ":p/name", "C", TX + 1);
        Datom d = new Datom(4, ":p/name", "D", TX + 2);
        Datom e = new Datom(5, ":p/name", "E", TX);
        PDb base = db(store, List.of(a, b), List.of(e), TX);
        PDb head = db(store, List.of(a, c, e), List.of(new Datom(1, ":p/name", "A", -(TX + 2)), d), TX + 2);
        PDb.Changes changes = head.changesSince(base);
        assertThat(changes.added()).containsExactly(c, d);
        assertThat(changes.retracted()).extracting(Datom::e).containsExactly(1L, 2L);
        assertThat(changes.retracted()).allMatch(x -> x.tx() < 0);
        assertThat(head.changesSince(head).added()).isEmpty();
        assertThat(head.changesSince(head).retracted()).isEmpty();
        PDb.Changes back = base.changesSince(head);
        assertThat(back.added()).extracting(Datom::e).containsExactly(1L, 2L);
        assertThat(back.retracted()).extracting(Datom::e).containsExactly(3L, 4L);
    }
}
