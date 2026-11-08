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
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.blob.PBlob;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.pdt.values.PVector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The merge seams of srad/agentspaces-pdt.md §3.6.1 (PD-R-05, PD-R-06, §6.7
 * row 6): two forked vector heads deref to the later head whole with a
 * {@code WHOLE_HEAD} event until a merge function is registered, which then
 * decides; a blob ref likewise; a head whose base names a root nobody wrote
 * yields {@code NO_COMMON_BASE} and the later head whole; a chain longer than
 * the walk bound between two forks does the same; a merge function that throws
 * falls back to the rule with {@code MERGE_FUNCTION_FAILED}; and a function
 * registered for another kind is refused.
 */
class MergeFunctionTest {

    private final RefFleet fleet = new RefFleet();

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    private static List<String> elements(PVector<String> v) {
        return v.toList();
    }

    @Test
    void forkedVectorHeadsTakeTheLaterHeadWholeUntilAMergeFunctionDecides() throws Exception {
        String name = "log";
        RefFleet.Peer a = fleet.newPeer("a", 1);
        RefFleet.Peer b = fleet.newPeer("b", 2, "a");
        RefFleet.Peer c = fleet.newPeer("c", 3, "a");
        fleet.settle(6);
        Ref<PVector<String>> onA = a.pdt().vectorRef(name, RefFleet.STR);
        Ref<PVector<String>> onB = b.pdt().vectorRef(name, RefFleet.STR);
        Ref<PVector<String>> onC = c.pdt().vectorRef(name, RefFleet.STR);
        PVector<String> v1 = onA.swap(v -> v.conj("x"));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(v1.root()) && !onC.heads().isEmpty())).isTrue();
        fleet.settle(2);
        // the fork: a's successor through swap, b's by hand off the same base, issued later
        PVector<String> v2a = onA.swap(v -> v.conj("a"));
        fleet.settle(1);
        PVector<String> v2b = onB.at(v1.root()).conj("b");
        RefFleet.writeHead(b, name, PRef.KIND_VECTOR, v2b.root(), v1.root(), 2, v2b.count(), Duration.ofMinutes(10));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onC.heads().heads().size() == 2
                && fleet.allHold(v2a.root()) && fleet.allHold(v2b.root()))).isTrue();
        List<PConflict> conflicts = new CopyOnWriteArrayList<>();
        onC.onConflict(conflicts::add);
        PVector<String> whole = onC.deref();
        assertThat(elements(whole)).as("the later head whole").containsExactly("x", "b");
        assertThat(conflicts).extracting(PConflict::reason).containsExactly(PConflict.WHOLE_HEAD);
        assertThat(conflicts.get(0).key()).isNull();
        assertThat(conflicts.get(0).winnerRoot()).isEqualTo(v2b.root());
        assertThat(conflicts.get(0).loserRoot()).isEqualTo(v2a.root());
        long before = c.pdt().stats().conflicts();
        // a registered function decides instead: the elements the later head added after the base follow the earlier head's
        c.pdt().mergeFor(name, PRef.KIND_VECTOR, (MergeFunction<PVector<String>>) (base, earlier, later) -> {
            assertThat(base).as("the common base reaches the function").isNotNull();
            PVector<String> out = earlier;
            for (long i = base.count(); i < later.count(); i++) {
                out = out.conj(later.nth(i));
            }
            return out;
        });
        PVector<String> merged = onC.deref();
        assertThat(elements(merged)).containsExactly("x", "a", "b");
        assertThat(c.pdt().stats().conflicts()).as("no conflict event when the function decides").isEqualTo(before);
        // the same function registered for a map is refused
        assertThatThrownBy(() -> c.pdt().mergeFor(name, PRef.KIND_MAP, (MergeFunction<PMap<String, String>>) (base, e, l) -> e))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("vector");
        // a function that throws leaves the rule in charge and says so
        c.pdt().mergeFor(name, PRef.KIND_VECTOR, (MergeFunction<PVector<String>>) (base, earlier, later) -> {
            throw new IllegalStateException("boom");
        });
        conflicts.clear();
        assertThat(elements(onC.deref())).containsExactly("x", "b");
        assertThat(conflicts).extracting(PConflict::reason).containsExactly(PConflict.MERGE_FUNCTION_FAILED);
        assertThat(c.pdt().stats().mergeFunctionFailures()).isEqualTo(1);
        System.out.printf("PD-R-05: forked vector heads [x,a] and [x,b]: whole head [x,b], merge function [x,a,b], failing function back to [x,b]%n");
    }

    @Test
    void forkedBlobHeadsTakeTheLaterHeadWholeUntilAMergeFunctionDecides() throws Exception {
        String name = "doc";
        RefFleet.Peer a = fleet.newPeer("a", 1);
        RefFleet.Peer b = fleet.newPeer("b", 2, "a");
        fleet.settle(6);
        Ref<PBlob> onA = a.pdt().blobRef(name);
        Ref<PBlob> onB = b.pdt().blobRef(name);
        byte[] one = "one\n".getBytes(StandardCharsets.UTF_8);
        PBlob v1 = onA.swap(blob -> PBlob.of(one, blob.store()));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(v1.root()) && !onB.heads().isEmpty())).isTrue();
        fleet.settle(2);
        PBlob v2a = onA.swap(blob -> PBlob.of("one\ntwo-a\n".getBytes(StandardCharsets.UTF_8), blob.store()));
        fleet.settle(1);
        PBlob v2b = PBlob.of("one\ntwo-b\n".getBytes(StandardCharsets.UTF_8), b.store());
        RefFleet.writeHead(b, name, PRef.KIND_BLOB, v2b.root(), v1.root(), 2, v2b.size(), Duration.ofMinutes(10));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onB.heads().heads().size() == 2
                && fleet.allHold(v2a.root()) && fleet.allHold(v2b.root()))).isTrue();
        List<PConflict> conflicts = new CopyOnWriteArrayList<>();
        onB.onConflict(conflicts::add);
        assertThat(new String(onB.deref().bytes(), StandardCharsets.UTF_8)).isEqualTo("one\ntwo-b\n");
        assertThat(conflicts).extracting(PConflict::reason).containsExactly(PConflict.WHOLE_HEAD);
        b.pdt().mergeFor(name, PRef.KIND_BLOB, (MergeFunction<PBlob>) (base, earlier, later) -> {
            byte[] e = earlier.bytes();
            byte[] l = later.bytes();
            byte[] out = new byte[e.length + l.length];
            System.arraycopy(e, 0, out, 0, e.length);
            System.arraycopy(l, 0, out, e.length, l.length);
            return PBlob.of(out, earlier.store());
        });
        assertThat(new String(onB.deref().bytes(), StandardCharsets.UTF_8)).isEqualTo("one\ntwo-a\none\ntwo-b\n");
        System.out.printf("PD-R-05: forked blob heads: whole head then the registered concatenation%n");
    }

    @Test
    void aHeadWhoseBaseNamesAnUnrelatedRootYieldsNoCommonBaseAndTheLaterHeadWhole() throws Exception {
        String name = "facts";
        RefFleet.Spec quick = RefFleet.Spec.defaults().pdt(b -> b.loadTimeout(Duration.ofMillis(300)));
        RefFleet.Peer a = fleet.newPeer("a", 1, quick);
        RefFleet.Peer b = fleet.newPeer("b", 2, quick, "a");
        fleet.settle(6);
        Ref<PMap<String, String>> onA = a.pdt().mapRef(name, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onB = b.pdt().mapRef(name, RefFleet.STR, RefFleet.STR);
        PMap<String, String> v1 = onA.swap(m -> m.assoc("k0", "v0"));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(v1.root()) && !onB.heads().isEmpty())).isTrue();
        fleet.settle(2);
        // a head computed off a root nobody ever wrote (§6.7 row 6): its nodes are real, its base is not
        byte[] nobody = new byte[48];
        new Random(5).nextBytes(nobody);
        String unrelated = Cids.of(nobody, Cids.DAG_CBOR);
        PMap<String, String> stray = onB.at(v1.root()).assoc("k0", "stray").assoc("ks", "s");
        List<byte[]> nodes = new java.util.ArrayList<>();
        for (String cid : b.store().reachable(stray.root())) {
            nodes.add(b.store().get(cid).orElseThrow());
        }
        PRef head = new PRef(PRef.VERSION, name, PRef.KIND_MAP, stray.root(), List.of(unrelated), 2, 2, nodes,
                PRef.MODE_MULTI, PRef.SEALING_NONE, null, null);
        assertThat(head.violation()).isEmpty();
        b.refSpace().write(head, Lease.of(Duration.ofMinutes(10)), head.tags());
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onA.heads().heads().size() == 2 && fleet.allHold(stray.root()))).isTrue();
        fleet.settle(4);
        List<PConflict> conflicts = new CopyOnWriteArrayList<>();
        onA.onConflict(conflicts::add);
        long started = System.nanoTime();
        PMap<String, String> seen = onA.deref();
        long waited = (System.nanoTime() - started) / 1_000_000;
        assertThat(seen.root()).as("the later head wins whole").isEqualTo(stray.root());
        assertThat(seen.get("k0")).contains("stray");
        assertThat(conflicts).extracting(PConflict::reason).containsExactly(PConflict.NO_COMMON_BASE);
        assertThat(conflicts.get(0).key()).isNull();
        assertThat(waited).as("no base fetch is attempted for a root the walks do not share").isLessThan(250);
        assertThat(onB.deref().root()).isEqualTo(stray.root());
        System.out.printf("PD-R-06: an unrelated base gave NO_COMMON_BASE in %d ms and the later head whole on both peers%n", waited);
    }

    @Test
    void aChainLongerThanTheWalkBoundBetweenTwoForksYieldsNoCommonBase() throws Exception {
        String name = "facts";
        RefFleet.Peer a = fleet.newPeer("a", 1);
        RefFleet.Peer b = fleet.newPeer("b", 2, "a");
        fleet.settle(6);
        Ref<PMap<String, String>> onA = a.pdt().mapRef(name, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onB = b.pdt().mapRef(name, RefFleet.STR, RefFleet.STR);
        PMap<String, String> v1 = onA.swap(m -> m.assoc("k0", "v0"));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(v1.root()) && !onB.heads().isEmpty())).isTrue();
        fleet.settle(2);
        // b forks once off v1 and never moves; a's side advances past the walk bound without ever reading b's head
        PMap<String, String> v2b = onB.at(v1.root()).assoc("kb", "b");
        RefFleet.writeHead(b, name, PRef.KIND_MAP, v2b.root(), v1.root(), 2, v2b.count(), Duration.ofMinutes(10));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onA.heads().roots().equals(List.of(v2b.root())) && fleet.allHold(v2b.root()))).isTrue();
        int steps = Refs.MAX_BASE_STEPS + 6;
        PMap<String, String> last = v1;
        for (int i = 0; i < steps; i++) {
            PMap<String, String> next = last.assoc("k" + (i + 1), "v");
            RefFleet.writeHead(a, name, PRef.KIND_MAP, next.root(), last.root(), 2 + i, next.count(), Duration.ofMinutes(10));
            last = next;
            if (i % 10 == 9) {
                fleet.tickAll();
            }
        }
        PMap<String, String> latest = last;
        boolean settled = fleet.runUntil(Duration.ofSeconds(20), () -> fleet.allHold(latest.root()) && onB.heads().heads().size() == 2);
        assertThat(settled).as("a holds %s, b holds %s, b heads %s of %d entries, b stats %s", a.store().complete(latest.root()),
                b.store().complete(latest.root()), onB.heads().roots(), RefFleet.headEntries(b, name).size(), b.pdt().stats()).isTrue();
        fleet.settle(2);
        List<PConflict> onBConflicts = new CopyOnWriteArrayList<>();
        onB.onConflict(onBConflicts::add);
        PMap<String, String> seenB = onB.deref();
        assertThat(seenB.root()).as("past 64 steps the later head wins whole").isEqualTo(latest.root());
        assertThat(seenB.get("kb")).isEmpty();
        assertThat(onBConflicts).extracting(PConflict::reason).containsExactly(PConflict.NO_COMMON_BASE);
        assertThat(onA.deref().root()).isEqualTo(latest.root());
        assertThat(RefFleet.headEntries(b, name).size()).isGreaterThan(Refs.MAX_BASE_STEPS);
        System.out.printf("PD-R-06: %d versions on one side of a fork: NO_COMMON_BASE and the later head whole, %d live entries%n",
                steps, RefFleet.headEntries(b, name).size());
    }
}
