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
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The bounds a hostile or broken head meets (srad/agentspaces-pdt.md PD-P-04,
 * §6.7 rows 4 and 6, S7, review of 2026-11-08): a head whose root is not a CID
 * is ignored and counted and never throws out of deref, swap, the tick, or the
 * space's delivery; a head at the seq ceiling leaves every successor valid and
 * a head above it is ignored; several unloadable heads cost one load timeout
 * together, and their fetches and roots are dropped once the heads go.
 */
class HeadBoundsTest {

    private static final String NAME = "facts";

    private final RefFleet fleet = new RefFleet();

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    @Test
    void aHeadWhoseRootIsNotACidIsIgnoredAndCountedAndNothingThrows() throws Exception {
        RefFleet.Spec quick = RefFleet.Spec.defaults().pdt(b -> b.loadTimeout(Duration.ofMillis(300)));
        RefFleet.Peer a = fleet.newPeer("a", 1, quick);
        RefFleet.Peer b = fleet.newPeer("b", 2, quick, "a");
        fleet.settle(6);
        Ref<PMap<String, String>> onA = a.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onB = b.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        PMap<String, String> v1 = onA.swap(m -> m.assoc("k1", "v1"));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(v1.root()) && !onB.heads().isEmpty())).isTrue();
        PRef bad = new PRef(PRef.VERSION, NAME, PRef.KIND_MAP, "not-a-cid", List.of(), 2, 1, null,
                PRef.MODE_MULTI, PRef.SEALING_NONE, null, null);
        assertThat(bad.violation()).contains("root is not a CID");
        // the writer's own delivery sees the record first: the write returns and nothing escapes the space
        a.refSpace().write(bad, Lease.of(Duration.ofMinutes(10)), bad.tags());
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> RefFleet.headEntries(b, NAME).size() == 2)).isTrue();
        for (int round = 0; round < 3; round++) {
            assertThat(onB.deref().root()).isEqualTo(round == 0 ? v1.root() : onB.heads().latest().root());
            assertThat(onA.deref().get("k1")).contains("v1");
            b.pdt().refs().tick();
            fleet.settle(4);
        }
        PMap<String, String> v2 = onB.swap(m -> m.assoc("k2", "v2"));
        assertThat(v2.count()).isEqualTo(2);
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onA.deref().root().equals(v2.root()))).isTrue();
        assertThat(b.pdt().stats().headsIgnored()).as("the malformed head is counted once on b").isEqualTo(1);
        assertThat(a.pdt().stats().headsIgnored()).isEqualTo(1);
        // a base that is not a CID is named too, so the head never reaches the walk
        PRef badBase = new PRef(PRef.VERSION, NAME, PRef.KIND_MAP, v2.root(), List.of("bafy"), 3, 2, null,
                PRef.MODE_MULTI, PRef.SEALING_NONE, null, null);
        assertThat(badBase.violation()).contains("base root is not a CID");
        System.out.printf("PD-P-04: a head with root 'not-a-cid' was ignored on both peers (headsIgnored=%d) and every deref, swap, and tick returned%n",
                b.pdt().stats().headsIgnored());
    }

    @Test
    void aHeadAtTheSeqCeilingLeavesSuccessorsValidAndAHeadAboveItIsIgnored() throws Exception {
        RefFleet.Peer a = fleet.newPeer("a", 1);
        RefFleet.Peer b = fleet.newPeer("b", 2, "a");
        fleet.settle(6);
        Ref<PMap<String, String>> onA = a.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onB = b.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        PMap<String, String> v1 = onA.swap(m -> m.assoc("k1", "v1"));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(v1.root()) && !onB.heads().isEmpty())).isTrue();
        fleet.settle(2);
        // a head above the ceiling is dropped before anything reads it
        PMap<String, String> over = onA.at(v1.root()).assoc("k9", "v9");
        PRef above = new PRef(PRef.VERSION, NAME, PRef.KIND_MAP, over.root(), List.of(v1.root()), Long.MAX_VALUE, 2, null,
                PRef.MODE_MULTI, PRef.SEALING_NONE, null, null);
        assertThat(above.violation()).contains("seq above " + PRef.MAX_SEQ);
        a.refSpace().write(above, Lease.of(Duration.ofMinutes(10)), above.tags());
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> RefFleet.headEntries(b, NAME).size() == 2)).isTrue();
        assertThat(onB.deref().root()).isEqualTo(v1.root());
        assertThat(b.pdt().stats().headsIgnored()).isEqualTo(1);
        // a head at the ceiling is valid, and every successor saturates at the ceiling rather than wrapping
        PMap<String, String> v2 = onA.at(v1.root()).assoc("k2", "v2");
        RefFleet.writeHead(a, NAME, PRef.KIND_MAP, v2.root(), v1.root(), PRef.MAX_SEQ, v2.count(), Duration.ofMinutes(10));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(v2.root()) && onB.deref().root().equals(v2.root()))).isTrue();
        PMap<String, String> v3 = onB.swap(m -> m.assoc("k3", "v3"));
        PRef written = RefFleet.headEntries(b, NAME).stream().map(e -> e.value()).filter(h -> h.root().equals(v3.root())).findFirst().orElseThrow();
        assertThat(written.seq()).isEqualTo(PRef.MAX_SEQ);
        assertThat(written.violation()).isEmpty();
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(v3.root()) && onA.deref().root().equals(v3.root()))).isTrue();
        PMap<String, String> v4 = onA.swap(m -> m.assoc("k4", "v4"));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onB.deref().root().equals(v4.root()))).isTrue();
        assertThat(onB.deref().get("k4")).contains("v4");
        assertThat(onB.deref().count()).isEqualTo(4);
        System.out.printf("PRef bounds: seq %d accepted, Long.MAX_VALUE ignored, successors saturate at %d and every peer follows%n",
                PRef.MAX_SEQ, written.seq());
    }

    @Test
    void severalUnloadableHeadsCostOneLoadTimeoutAndTheirFetchesAreReaped() throws Exception {
        RefFleet.Spec quick = RefFleet.Spec.defaults().pdt(b -> b.loadTimeout(Duration.ofMillis(300)));
        RefFleet.Peer a = fleet.newPeer("a", 1, quick);
        RefFleet.Peer b = fleet.newPeer("b", 2, quick, "a");
        fleet.settle(6);
        Ref<PMap<String, String>> onA = a.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onB = b.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        PMap<String, String> v1 = onA.swap(m -> m.assoc("k1", "v1"));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(v1.root()) && !onB.heads().isEmpty())).isTrue();
        Random random = new Random(9);
        List<EntryHandle> handles = new ArrayList<>();
        List<String> phantoms = new ArrayList<>();
        int n = 10;
        for (int i = 0; i < n; i++) {
            byte[] nobody = new byte[48];
            random.nextBytes(nobody);
            String phantom = Cids.of(nobody, Cids.DAG_CBOR);
            phantoms.add(phantom);
            PRef unloadable = new PRef(PRef.VERSION, NAME, PRef.KIND_MAP, phantom, List.of(), 2, 1, null,
                    PRef.MODE_MULTI, PRef.SEALING_NONE, null, null);
            handles.add(a.refSpace().write(unloadable, Lease.of(Duration.ofMinutes(10)), unloadable.tags()));
        }
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> RefFleet.headEntries(b, NAME).size() == n + 1)).isTrue();
        long started = System.nanoTime();
        PMap<String, String> seen = onB.deref();
        long waited = (System.nanoTime() - started) / 1_000_000;
        assertThat(seen.root()).isEqualTo(v1.root());
        assertThat(waited).as("ten unloadable heads waited one load timeout together").isGreaterThanOrEqualTo(250).isLessThan(1_200);
        // swarm retries (ROBUSTNESS.md §10.3 P1): roots still loading are late, apart from the heads ignored for a fault
        assertThat(b.pdt().stats().headsIgnored()).isZero();
        assertThat(b.pdt().refs().headsLate().total()).as("each late entry counts once per cause").isBetween((long) n, 2L * n);
        started = System.nanoTime();
        assertThat(onB.deref().root()).isEqualTo(v1.root());
        assertThat((System.nanoTime() - started) / 1_000_000).as("a second deref does not wait").isLessThan(250);
        // the sessions fail at the load timeout on the clock and are reaped; once the heads go, nothing of them stays
        for (EntryHandle handle : handles) {
            handle.cancel();
        }
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> RefFleet.headEntries(b, NAME).size() == 1
                && b.content().stats().treeFetchesActive() == 0)).as("every phantom fetch ended").isTrue();
        fleet.settle(4);
        assertThat(b.pdt().refs().liveRoots()).doesNotContainAnyElementsOf(phantoms);
        assertThat(b.content().activeTreeFetches()).isEmpty();
        assertThat(onB.deref().root()).isEqualTo(v1.root());
        System.out.printf("PD-V-06 / §6.7 row 4: %d phantom heads cost one deref %d ms; after their cancel 0 fetches active, %d live roots%n",
                n, waited, b.pdt().refs().liveRoots().size());
    }
}
