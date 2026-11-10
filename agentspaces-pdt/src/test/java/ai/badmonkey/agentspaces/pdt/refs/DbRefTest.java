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
package ai.badmonkey.agentspaces.pdt.refs;

import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.pdt.Datom;
import ai.badmonkey.agentspaces.pdt.kinds.DnNode;
import ai.badmonkey.agentspaces.pdt.values.DatomOrder;
import ai.badmonkey.agentspaces.pdt.values.PDb;
import ai.badmonkey.agentspaces.pdt.values.PSortedMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PDT 2 phase 2, P2-D-03 and P2-D-06 (srad/agentspaces-pdt2.md §3.1.3, §3.1.4, §6.10.3 rule 8):
 * a {@code db} ref writes version 2 heads of kind {@code db} in linear mode by default, and two
 * heads meet in the {@link DbMerge} with their common base from the version graph, where a
 * cardinality-one clash raises one {@link PConflict} through the resolver.
 */
class DbRefTest {

    private static final String NAME = "graph";
    private static final long TX = PDb.TX0 + 1;

    private final RefFleet fleet = new RefFleet();

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    /** The database of {@code datoms}, written by bulk build into {@code over}'s store, after {@code over}. */
    private static PDb db(PDb over, List<Datom> datoms) {
        List<Datom> eavt = new ArrayList<>(datoms);
        eavt.sort(DatomOrder.EAVT);
        List<Datom> aevt = new ArrayList<>(datoms);
        aevt.sort(DatomOrder.AEVT);
        String e = PSortedMap.of(over.store(), DnNode.EAVT, DatomOrder.EAVT, 128, eavt).root();
        String a = PSortedMap.of(over.store(), DnNode.AEVT, DatomOrder.AEVT, 128, aevt).root();
        String v = PSortedMap.of(over.store(), DnNode.AVET, DatomOrder.AVET, 128, List.of()).root();
        long maxTx = Math.max(over.node().maxTx(), datoms.stream().mapToLong(Datom::tx).max().orElse(PDb.TX0));
        return PDb.writer(over.store(), over.loader()).root(over.schema(), 9, maxTx, e, a, v, null);
    }

    private static List<Datom> datoms(PDb db) {
        List<Datom> out = new ArrayList<>();
        db.index(DnNode.EAVT).iterator().forEachRemaining(out::add);
        return out;
    }

    /** A union of both heads' changes; a cardinality-one attribute changed on both sides to two values goes to the resolver. */
    private static final DbMerge UNION = (earlier, base, later, resolver) -> {
        PDb.Changes mine = earlier.changesSince(base);
        PDb.Changes theirs = later.changesSince(base);
        TreeSet<Datom> out = new TreeSet<>(DatomOrder.EAVT);
        out.addAll(datoms(base));
        mine.retracted().forEach(d -> out.removeIf(x -> x.e() == d.e() && x.a().equals(d.a()) && x.v().equals(d.v())));
        theirs.retracted().forEach(d -> out.removeIf(x -> x.e() == d.e() && x.a().equals(d.a()) && x.v().equals(d.v())));
        List<Datom> added = new ArrayList<>(theirs.added());
        for (Datom d : mine.added()) {
            Optional<Datom> clash = theirs.added().stream().filter(t -> t.e() == d.e() && t.a().equals(d.a())).findFirst();
            if (clash.isEmpty()) {
                added.add(d);
            } else if (!clash.get().v().equals(d.v())) {
                resolver.resolve("[" + d.e() + " " + d.a() + "]", Optional.empty(), Optional.of(d.v()), Optional.of(clash.get().v()));
            }
        }
        out.addAll(added);
        return db(earlier, new ArrayList<>(out));
    };

    @Test
    void aSwapWritesAVersionTwoLinearHeadOfKindDbThatAReaderLoads() throws Exception {
        RefFleet.Peer w = fleet.newPeer("w", 1);
        RefFleet.Peer r = fleet.newPeer("r", 2, "w");
        fleet.settle(12);
        Ref<PDb> onW = w.pdt().dbRef(NAME, null);
        Ref<PDb> onR = r.pdt().dbRef(NAME, null);
        assertThat(onW.mode()).as("decision 4: a db ref is linear by default").isEqualTo(PRef.MODE_LINEAR);
        assertThat(onW.kind()).isEqualTo(PRef.KIND_DB);
        Datom ada = new Datom(1, ":p/name", "Ada", TX);
        PDb v1 = onW.swap(db -> db(db, List.of(ada)));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onR.heads().roots().equals(List.of(v1.root())))).isTrue();
        PRef head = RefFleet.headEntries(r, NAME).stream().map(Space.Entry::value).findFirst().orElseThrow();
        assertThat(head.v()).as("rule 8").isEqualTo(PRef.VERSION_2);
        assertThat(head.kind()).isEqualTo(PRef.KIND_DB);
        assertThat(head.mode()).isEqualTo(PRef.MODE_LINEAR);
        assertThat(head.size()).isEqualTo(1);
        assertThat(head.nodes()).as("the small version rides inline").isNotEmpty();
        PDb read = onR.deref();
        assertThat(read.root()).isEqualTo(v1.root());
        assertThat(read.index(DnNode.EAVT).contains(ada)).isTrue();
        Datom grace = new Datom(2, ":p/name", "Grace", TX + 1);
        PDb v2 = onR.swap(db -> db(db, List.of(ada, grace)));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onW.heads().roots().equals(List.of(v2.root())))).isTrue();
        assertThat(datoms(onW.deref())).containsExactly(ada, grace);
    }

    @Test
    void twoHeadsMeetInTheDatomMergeWithTheirCommonBaseAndOneClashRaisesOneConflict() throws Exception {
        RefFleet.Spec multi = RefFleet.Spec.defaults().pdt(b -> b.mode(NAME, PRef.MODE_MULTI));
        RefFleet.Peer a = fleet.newPeer("a", 1, multi);
        RefFleet.Peer b = fleet.newPeer("b", 2, multi, "a");
        RefFleet.Peer r = fleet.newPeer("r", 3, multi, "a");
        fleet.settle(12);
        AtomicReference<String> baseSeen = new AtomicReference<>();
        DbMerge recording = (earlier, base, later, resolver) -> {
            baseSeen.set(base.root());
            return UNION.merge(earlier, base, later, resolver);
        };
        Ref<PDb> onA = a.pdt().dbRef(NAME, UNION);
        Ref<PDb> onB = b.pdt().dbRef(NAME, UNION);
        Ref<PDb> onR = r.pdt().dbRef(NAME, recording);
        List<PConflict> conflicts = new CopyOnWriteArrayList<>();
        onR.onConflict(conflicts::add);
        Datom ada = new Datom(1, ":p/name", "Ada", TX);
        PDb v1 = onA.swap(db -> db(db, List.of(ada)));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(v1.root())
                && onB.heads().roots().equals(List.of(v1.root())) && onR.heads().roots().equals(List.of(v1.root())))).isTrue();
        PDb forkA = db(onA.at(v1.root()), List.of(ada, new Datom(2, ":p/name", "Grace", TX + 1),
                new Datom(3, ":p/name", "from a", TX + 1)));
        RefFleet.writeHead(a, NAME, PRef.KIND_DB, forkA.root(), v1.root(), 2, forkA.size(), Duration.ofMinutes(10));
        fleet.settle(1);
        PDb forkB = db(onB.at(v1.root()), List.of(ada, new Datom(2, ":p/name", "Grete", TX + 1),
                new Datom(4, ":p/name", "from b", TX + 1)));
        RefFleet.writeHead(b, NAME, PRef.KIND_DB, forkB.root(), v1.root(), 2, forkB.size(), Duration.ofMinutes(10));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onR.heads().roots().size() == 2
                && fleet.allHold(forkA.root()) && fleet.allHold(forkB.root()))).isTrue();
        PDb merged = onR.deref();
        assertThat(baseSeen.get()).as("the version graph names v1 as the base").isEqualTo(v1.root());
        assertThat(datoms(merged)).extracting(Datom::v).containsExactly("Ada", "Grete", "from a", "from b");
        assertThat(conflicts).extracting(PConflict::reason).containsExactly(PConflict.LATER_HLC);
        assertThat(conflicts.get(0).key()).isEqualTo("[2 :p/name]");
        assertThat(onA.deref().root()).as("every reader merges to the same root").isEqualTo(merged.root());
    }

    @Test
    void withoutAMergeTheLaterHeadWinsWhole() throws Exception {
        RefFleet.Spec multi = RefFleet.Spec.defaults().pdt(b -> b.mode(NAME, PRef.MODE_MULTI));
        RefFleet.Peer a = fleet.newPeer("a", 1, multi);
        RefFleet.Peer b = fleet.newPeer("b", 2, multi, "a");
        fleet.settle(12);
        Ref<PDb> onA = a.pdt().dbRef(NAME, null);
        Ref<PDb> onB = b.pdt().dbRef(NAME, null);
        PDb v1 = onA.swap(db -> db(db, List.of(new Datom(1, ":p/name", "Ada", TX))));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(v1.root()))).isTrue();
        PDb forkA = db(onA.at(v1.root()), List.of(new Datom(1, ":p/name", "A", TX + 1)));
        RefFleet.writeHead(a, NAME, PRef.KIND_DB, forkA.root(), v1.root(), 2, forkA.size(), Duration.ofMinutes(10));
        fleet.settle(1);
        PDb forkB = db(onB.at(v1.root()), List.of(new Datom(1, ":p/name", "B", TX + 1)));
        RefFleet.writeHead(b, NAME, PRef.KIND_DB, forkB.root(), v1.root(), 2, forkB.size(), Duration.ofMinutes(10));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onA.heads().roots().size() == 2
                && fleet.allHold(forkB.root()))).isTrue();
        assertThat(onA.deref().root()).isEqualTo(forkB.root());
    }
}
