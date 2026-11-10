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
import ai.badmonkey.agentspaces.pdt.values.PMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Head leases and retained history (srad/agentspaces-pdt.md PD-R-08, PD-R-09,
 * PD-Q-REL-07, PD-S-03): the writer renews its head at half-life; when the
 * writer dies and the lease lapses a follower that holds the root rewrites the
 * head and every deref is unchanged; history and at walk the retained versions,
 * of which retention keeps 64. After the review of 2026-11-08: a head whose only
 * successor never loads stays live and keeps its value on every peer (PD-R-03,
 * S7), and a rewrite keeps the original head's merge order on present readers
 * and late joiners alike (PD-R-08, PD-R-05).
 */
class HeadLifecycleTest {

    private static final String NAME = "facts";
    private static final Duration HEAD_LEASE = Duration.ofSeconds(20);

    private final RefFleet fleet = new RefFleet();

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    private static RefFleet.Spec shortLease() {
        return RefFleet.Spec.defaults().pdt(b -> b.headLease(HEAD_LEASE));
    }

    @Test
    void aFollowerRewritesALapsedHeadAndDerefIsUnchangedOnEveryPeer() throws Exception {
        RefFleet.Peer a = fleet.newPeer("a", 1, shortLease());
        RefFleet.Peer b = fleet.newPeer("b", 2, shortLease(), "a");
        RefFleet.Peer c = fleet.newPeer("c", 3, shortLease(), "a");
        fleet.settle(6);
        Ref<PMap<String, String>> onA = a.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onB = b.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onC = c.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        String root = null;
        for (int i = 0; i < 3; i++) {
            int n = i;
            root = onA.swap(m -> m.assoc("k" + n, "v" + n)).root();
        }
        String head = root;
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(head))).isTrue();
        assertThat(onB.deref().root()).isEqualTo(head);
        assertThat(onC.deref().root()).isEqualTo(head);

        // the live writer renews at half-life: well past one lease the head still stands and nobody rewrote it
        fleet.settle((int) (HEAD_LEASE.toMillis() * 2 / 250));
        assertThat(RefFleet.headEntries(b, NAME)).hasSize(1);
        assertThat(RefFleet.headEntries(b, NAME).get(0).issuer().peer()).isEqualTo(a.id());
        assertThat(b.pdt().stats().headsRewritten() + c.pdt().stats().headsRewritten()).isZero();
        assertThat(onB.deref().root()).isEqualTo(head);

        // the writer dies; its head lapses on the clock; a follower that holds the root rewrites it
        fleet.kill(a);
        assertThat(fleet.runUntil(Duration.ofSeconds(30), () -> {
            List<?> onBHeads = RefFleet.headEntries(b, NAME);
            List<?> onCHeads = RefFleet.headEntries(c, NAME);
            return !onBHeads.isEmpty() && !onCHeads.isEmpty()
                    && b.pdt().stats().headsRewritten() + c.pdt().stats().headsRewritten() >= 1;
        })).as("a follower rewrote the lapsed head").isTrue();
        fleet.settle(4);
        assertThat(onB.deref().root()).isEqualTo(head);
        assertThat(onC.deref().root()).isEqualTo(head);
        assertThat(onB.heads().heads()).hasSize(1);
        assertThat(onB.heads().latest().seq()).isEqualTo(3);
        assertThat(RefFleet.headEntries(b, NAME)).allSatisfy(e -> {
            assertThat(e.value().root()).isEqualTo(head);
            assertThat(e.issuer().peer()).isNotEqualTo(a.id());
            assertThat(e.value().nodes()).isNull();
        });
        // the rewritten head renews in turn: it outlives another lease
        fleet.settle((int) (HEAD_LEASE.toMillis() * 2 / 250));
        assertThat(RefFleet.headEntries(c, NAME)).isNotEmpty();
        assertThat(onC.deref().root()).isEqualTo(head);
        PMap<String, String> after = onB.swap(m -> m.assoc("k3", "v3"));
        assertThat(after.count()).isEqualTo(4);
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onC.heads().roots().equals(List.of(after.root())))).isTrue();
        assertThat(onC.deref().get("k3")).contains("v3");
        System.out.printf("PD-Q-REL-07: writer killed, %d ms lease lapsed, rewrites b=%d c=%d, deref unchanged, next swap seq %d%n",
                HEAD_LEASE.toMillis(), b.pdt().stats().headsRewritten(), c.pdt().stats().headsRewritten(), onC.heads().maxSeq());
    }

    @Test
    void anUnloadableSuccessorLeavesThePriorHeadLiveAndDerefUnchangedOnEveryPeer() throws Exception {
        RefFleet.Spec quick = RefFleet.Spec.defaults().pdt(b -> b.loadTimeout(Duration.ofMillis(300)));
        RefFleet.Peer a = fleet.newPeer("a", 1, quick);
        RefFleet.Peer b = fleet.newPeer("b", 2, quick, "a");
        RefFleet.Peer f = fleet.newPeer("f", 3, quick, "a");
        fleet.settle(6);
        Ref<PMap<String, String>> onA = a.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onB = b.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onF = f.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        PMap<String, String> v1 = onA.swap(m -> m.assoc("k1", "v1"));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(v1.root()) && !onB.heads().isEmpty() && !onF.heads().isEmpty())).isTrue();

        // an authorized member writes a successor of v1 whose nodes nobody ever gets, then dies: the crash-after-swap shape
        byte[] nobody = new byte[48];
        new Random(9).nextBytes(nobody);
        String phantom = Cids.of(nobody, Cids.DAG_CBOR);
        PRef unloadable = new PRef(PRef.VERSION, NAME, PRef.KIND_MAP, phantom, List.of(v1.root()), 2, 1, null,
                PRef.MODE_MULTI, PRef.SEALING_NONE, null, null);
        f.refSpace().write(unloadable, Lease.of(Duration.ofMinutes(10)), unloadable.tags());
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> RefFleet.headEntries(a, NAME).size() == 2
                && RefFleet.headEntries(b, NAME).size() == 2)).isTrue();
        fleet.kill(f);
        assertThat(onB.deref().root()).isEqualTo(v1.root());
        // well past the cancel grace: v1 is still the head everywhere, because its only successor never loaded
        fleet.settle(24);
        assertThat(RefFleet.headEntries(a, NAME)).as("a kept v1 live").extracting(e -> e.value().root()).contains(v1.root());
        assertThat(RefFleet.headEntries(b, NAME)).as("b sees v1 live").extracting(e -> e.value().root()).contains(v1.root());
        assertThat(onA.deref().root()).isEqualTo(v1.root());
        assertThat(onB.deref().root()).isEqualTo(v1.root());
        assertThat(onB.deref().count()).isEqualTo(1);
        // swarm retries (ROBUSTNESS.md §10.3 P1): the phantom head is left out and counted as late, never as a fault
        assertThat(b.pdt().stats().headsIgnored()).isZero();
        assertThat(b.pdt().stats().headsLate()).as("the phantom head is late: its fetch runs on and is retried").isPositive();
        // the next swap continues the history from v1 rather than restarting the ref
        PMap<String, String> v2 = onA.swap(m -> m.assoc("k2", "v2"));
        assertThat(v2.count()).isEqualTo(2);
        assertThat(RefFleet.headEntries(a, NAME).stream().filter(e -> e.value().root().equals(v2.root())).findFirst().orElseThrow().value())
                .satisfies(h -> {
                    assertThat(h.seq()).isEqualTo(2);
                    assertThat(h.base()).containsExactly(v1.root());
                });
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(v2.root()) && onB.deref().root().equals(v2.root()))).isTrue();
        // v1 is now below the head and the writer cancels it after the grace
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> RefFleet.headEntries(b, NAME).stream()
                .noneMatch(e -> e.value().root().equals(v1.root())))).as("v1 cancelled once v2 superseded it").isTrue();
        assertThat(onB.deref().count()).isEqualTo(2);
        System.out.printf("S7 / PD-R-03: an unloadable successor left v1 live on every peer; the next swap wrote seq 2 over base [v1]%n");
    }

    @Test
    void aSwapBackToAnAncestorsContentStaysTheHeadOnEveryPeer() throws Exception {
        RefFleet.Peer a = fleet.newPeer("a", 1);
        RefFleet.Peer b = fleet.newPeer("b", 2, "a");
        fleet.settle(6);
        Ref<PMap<String, String>> onA = a.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onB = b.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        // b writes v0, then a writes v1 over it, v2 adds a note, and v3 removes it, all within the cancel grace: b's v0
        // entry and a's v1 entry are still live when v3 lands on v1's root (the shape the Clojure test reproduced)
        PMap<String, String> v0 = onB.swap(m -> m.assoc("k0", "v0"));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(v0.root()) && onA.deref().root().equals(v0.root()))).isTrue();
        PMap<String, String> v1 = onA.swap(m -> m.assoc("k1", "v1"));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(v1.root()) && onB.deref().root().equals(v1.root()))).isTrue();
        PMap<String, String> v2 = onA.swap(m -> m.assoc("note", "n"));
        PMap<String, String> v3 = onA.swap(m -> m.dissoc("note"));
        assertThat(v3.root()).as("content addressing: the revert has the ancestor's root").isEqualTo(v1.root());
        assertThat(v2.root()).isNotEqualTo(v1.root());
        Heads heads = onA.heads();
        assertThat(heads.roots()).as("the revert is the head on the writer, by its seq").containsExactly(v1.root());
        assertThat(heads.latest().seq()).isEqualTo(4);
        assertThat(onA.deref().count()).isEqualTo(2);
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onB.heads().roots().equals(List.of(v1.root()))
                && onB.heads().latest().seq() == 4 && onB.deref().count() == 2)).as("the reader derefs the reverted content").isTrue();
        assertThat(onB.deref().get("k1")).contains("v1");
        assertThat(b.pdt().stats().headsIgnored()).isZero();
        assertThat(a.pdt().stats().headsIgnored()).isZero();
        // through the grace the revert stays the only head on both peers (v0 and the older v1 entry are never heads again),
        // and then the writers cancel v0, v2, and the displaced v1 entry: the entries settle on the revert alone
        for (int tick = 0; tick < 12; tick++) {
            fleet.settle(1);
            assertThat(onA.heads().roots()).as("a at tick " + tick).containsExactly(v1.root());
            assertThat(onB.heads().roots()).as("b at tick " + tick).containsExactly(v1.root());
        }
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> RefFleet.headEntries(a, NAME).size() == 1
                && RefFleet.headEntries(b, NAME).size() == 1)).as("one live entry").isTrue();
        assertThat(RefFleet.headEntries(b, NAME).get(0).value().seq()).isEqualTo(4);
        assertThat(onB.deref().root()).isEqualTo(v1.root());
        // the next swap, by the other peer, continues the history from seq 4 over the reverted root
        PMap<String, String> v4 = onB.swap(m -> m.assoc("k4", "v4"));
        PRef written = RefFleet.headEntries(b, NAME).stream().map(e -> e.value()).filter(h -> h.root().equals(v4.root())).findFirst().orElseThrow();
        assertThat(written.seq()).isEqualTo(5);
        assertThat(written.base()).containsExactly(v1.root());
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onA.deref().root().equals(v4.root()) && onA.heads().roots().equals(List.of(v4.root())))).isTrue();
        assertThat(onA.deref().count()).isEqualTo(3);
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> RefFleet.headEntries(a, NAME).size() == 1)).as("the revert cancelled once v4 superseded it").isTrue();
        System.out.printf("PD-R-02 / PD-R-03: a revert to v1's content kept v1's root as the head at seq 4 on both peers; the next swap wrote seq 5 over it%n");
    }

    @Test
    void aRewriteKeepsTheMergeOrderOfTheOriginalHeadOnEveryReaderAndALateJoiner() throws Exception {
        RefFleet.Peer a = fleet.newPeer("a", 1);
        RefFleet.Peer b = fleet.newPeer("b", 2, "a");
        RefFleet.Peer r = fleet.newPeer("r", 3, "a");
        fleet.settle(6);
        Ref<PMap<String, String>> onA = a.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onB = b.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onR = r.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        PMap<String, String> v1 = onA.swap(m -> m.assoc("k0", "v0"));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(v1.root()) && !onB.heads().isEmpty() && !onR.heads().isEmpty())).isTrue();
        fleet.settle(2);
        // a fork with a both-side conflict on kc: v2a issued first under a short lease, v2b later under a long one
        PMap<String, String> v2a = onA.at(v1.root()).assoc("kc", "a").assoc("ka", "a");
        RefFleet.writeHead(a, NAME, PRef.KIND_MAP, v2a.root(), v1.root(), 2, v2a.count(), Duration.ofSeconds(5));
        fleet.settle(1);
        PMap<String, String> v2b = onB.at(v1.root()).assoc("kc", "b").assoc("kb", "b");
        RefFleet.writeHead(b, NAME, PRef.KIND_MAP, v2b.root(), v1.root(), 2, v2b.count(), Duration.ofMinutes(10));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onR.heads().heads().size() == 2 && fleet.allHold(v2a.root()) && fleet.allHold(v2b.root()))).isTrue();
        List<PConflict> conflicts = new CopyOnWriteArrayList<>();
        onR.onConflict(conflicts::add);
        PMap<String, String> before = onR.deref();
        assertThat(before.get("kc")).as("the later issued head wins the key").contains("b");
        assertThat(conflicts).extracting(PConflict::reason).containsExactly(PConflict.LATER_HLC);
        // v2a's lease lapses on the clock and a holder rewrites it under a fresh signature and stamp
        fleet.settle(40);
        long rewrites = a.pdt().stats().headsRewritten() + b.pdt().stats().headsRewritten() + r.pdt().stats().headsRewritten();
        assertThat(rewrites).as("someone rewrote the lapsed head").isGreaterThanOrEqualTo(1);
        PRef rewritten = RefFleet.headEntries(r, NAME).stream().map(e -> e.value()).filter(h -> h.root().equals(v2a.root())).findFirst().orElseThrow();
        assertThat(rewritten.issuedAttr()).as("the rewrite carries the original stamp").isNotNull();
        assertThat(rewritten.issuerAttr()).isNotNull();
        PMap<String, String> after = onR.deref();
        assertThat(after.root()).as("the deref did not move with the rewrite").isEqualTo(before.root());
        assertThat(after.get("kc")).contains("b");
        assertThat(onR.heads().heads().get(0).root()).as("v2a still orders first").isEqualTo(v2a.root());
        assertThat(onR.heads().heads().get(0).issuer().peer()).as("the original issuer stays on the head").isEqualTo(a.id());
        // a late joiner that only ever sees the rewritten entry orders the heads the same way
        RefFleet.Peer l = fleet.newPeer("l", 9, "a");
        Ref<PMap<String, String>> onL = l.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> onL.heads().heads().size() == 2
                && l.store().complete(v2a.root()) && l.store().complete(v2b.root()))).isTrue();
        fleet.settle(2);
        PMap<String, String> joined = onL.deref();
        assertThat(joined.root()).as("the late joiner derefs the same root").isEqualTo(after.root());
        assertThat(joined.get("kc")).contains("b");
        System.out.printf("PD-R-08: %d rewrite(s) of the lapsed head kept kc=b on the present reader and the late joiner%n", rewrites);
    }

    /**
     * HLC canonical stamps, 2026-11-08: a {@code pdt.issued} attr orders a head only when it is a
     * canonical, acceptable stamp. Java read "0<physical>:0:n" as a stamp and ordered by a counter
     * above 65536, so a head carrying either sorted after every honest one and won every
     * contested key. The attr is advisory, so such a head falls back to its entry's own stamp, and
     * the later honest head wins. HLC round six, 2026-11-08: each stamp lies five minutes ahead,
     * inside the far-future margin of ruling R2, so only the rule under test makes it fall back.
     */
    @ParameterizedTest
    @ValueSource(strings = {"%d:65537:zEvil", "0%d:0:zEvil"})
    void aPdtIssuedAttrThatIsNoAcceptableStampFallsBackToTheEntrysOwnStamp(String hostile) throws Exception {
        assertHeadOrdersByItsOwnStamp(java.util.Map.of(PRef.ISSUED_ATTR, hostile.formatted(ahead(Duration.ofMinutes(5)))));
    }

    /**
     * HLC round four, 2026-11-08 (decision D2): a {@code pdt.issuer} that does not parse as an
     * agent id makes the head fall back to its entry's own stamp and issuer together, however
     * acceptable its {@code pdt.issued}. Nothing is refused or counted for it; the attrs are
     * advisory payload and only order. Honoured, this stamp five minutes ahead would win every key.
     */
    @Test
    void aPdtIssuerAttrThatIsNoAgentIdFallsBackToTheEntrysOwnStampAndIssuer() throws Exception {
        assertHeadOrdersByItsOwnStamp(java.util.Map.of(PRef.ISSUED_ATTR, aheadStamp(Duration.ofMinutes(5)),
                PRef.ISSUER_ATTR, "no-agent-id"));
    }

    /**
     * HLC round five, 2026-11-08 (decision E10): a {@code pdt.issuer} that is present and null
     * makes the head fall back to its entry's own stamp and issuer together, however acceptable
     * its {@code pdt.issued}; pairing the stamp ahead with the entry's issuer instead would win
     * every key. A {@code pdt.issued} that is present and null falls back the same way. The head
     * still decodes and orders, and nothing is refused or counted for the attrs.
     */
    @ParameterizedTest
    @ValueSource(strings = {PRef.ISSUER_ATTR, PRef.ISSUED_ATTR})
    void aNullPdtIssuerOrPdtIssuedAttrFallsBackToTheEntrysOwnStampAndIssuer(String nulled) throws Exception {
        java.util.Map<String, Object> attrs = new java.util.HashMap<>();
        attrs.put(PRef.ISSUED_ATTR, aheadStamp(Duration.ofMinutes(5)));
        attrs.put(PRef.ISSUER_ATTR, "zEvil/app");
        attrs.put(nulled, null);
        assertHeadOrdersByItsOwnStamp(attrs);
    }

    /**
     * HLC round six, 2026-11-08 (ruling R1): under {@code pdt.issued} and {@code pdt.issuer} any
     * application value travels, and one that is no text makes the head fall back to its entry's
     * own stamp and issuer together. The head decodes on every replica and nothing counts it. The
     * byte string is the base64 of "zEvi/app", an agent id Java once read from it.
     */
    @ParameterizedTest
    @ValueSource(strings = {"bytes", "list", "map", "integer", "boolean", "float"})
    void aPdtIssuerThatIsNoTextFallsBackToTheEntrysOwnStampAndIssuer(String shape) throws Exception {
        java.util.Map<String, Object> attrs = new java.util.HashMap<>();
        attrs.put(PRef.ISSUED_ATTR, aheadStamp(Duration.ofMinutes(5)));
        attrs.put(PRef.ISSUER_ATTR, nonText(shape));
        assertHeadOrdersByItsOwnStamp(attrs);
    }

    /**
     * The control of the fallback tests above: an acceptable {@code pdt.issued} five minutes
     * ahead with an agent id under {@code pdt.issuer} orders the head after the later honest one,
     * so the head wins the contested key. Each fallback test therefore shows its rule at work.
     */
    @Test
    void anAcceptablePdtIssuedAheadOrdersTheHeadByItAndWinsTheKey() throws Exception {
        assertHeadOrdersByTheAttrs(java.util.Map.of(PRef.ISSUED_ATTR, aheadStamp(Duration.ofMinutes(5)),
                PRef.ISSUER_ATTR, "zEvil/app"));
    }

    /**
     * HLC round six, 2026-11-08 (ruling R2): a {@code pdt.issued} whose physical time lies beyond
     * the reader's wall plus {@code MAX_DRIFT} plus the five-second skew margin is no acceptable
     * stamp, so the head falls back to its entry's own stamp and issuer and can no longer win
     * every conflict by stamping ahead.
     */
    @Test
    void aPdtIssuedBeyondTheFarFutureMarginFallsBackToTheEntrysOwnStampAndIssuer() throws Exception {
        assertHeadOrdersByItsOwnStamp(java.util.Map.of(PRef.ISSUED_ATTR, aheadStamp(Duration.ofMinutes(11)),
                PRef.ISSUER_ATTR, "zEvil/app"));
    }

    /**
     * HLC round six, 2026-11-08 (ruling R2): a reader judges {@code pdt.issued} against its own
     * wall at each read, so a head whose attr lies past the bound orders by its entry's own stamp
     * until the wall comes within the bound, and by its attrs from then on. The version this
     * engine records for the root follows the head, so {@code version} and a rewrite of the lapsed
     * head (PD-R-08) carry the order the head has; the record once kept the first fallback stamp.
     */
    @Test
    void aPdtIssuedTheWallComesWithinTheBoundOfOrdersTheHeadAndItsRecordedVersionFromThen() throws Exception {
        Fork fork = forkWithAttrs(java.util.Map.of(PRef.ISSUED_ATTR, aheadStamp(Duration.ofMinutes(11)),
                PRef.ISSUER_ATTR, "zEvil/app"));
        Ref<PMap<String, String>> onR = fork.reader().pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        assertThat(onR.heads().heads().get(0).root()).as("past the bound the head orders by its own stamp")
                .isEqualTo(fork.attributed());
        assertThat(onR.deref().get("kc")).contains("b");
        fleet.clock.advance(Duration.ofMinutes(2));
        Heads.Head last = onR.heads().heads().get(1);
        assertThat(last.root()).as("within the bound the head orders by its attrs").isEqualTo(fork.attributed());
        assertThat(last.issuer().peer()).isEqualTo(EVIL);
        assertThat(onR.deref().get("kc")).contains("a");
        Heads.Head version = fork.reader().pdt().refs().version(NAME, fork.attributed()).orElseThrow();
        assertThat(version.issued()).as("the recorded version orders as the head does").isEqualTo(last.issued());
        assertThat(version.issuer()).isEqualTo(last.issuer());
        assertThat(fork.reader().pdt().stats().headsIgnored()).isZero();
    }

    /**
     * HLC round six, 2026-11-08 (ruling R2), at its bound: a {@code pdt.issued} exactly at the
     * reader's wall plus {@code MAX_DRIFT} plus five seconds is acceptable, and one millisecond
     * later is not. The margin is the far-future margin a frame's stamp is judged by.
     */
    @Test
    void theFarFutureBoundOfPdtIssuedIsTheWallPlusMaxDriftPlusFiveSeconds() {
        long wall = 1_800_000_000_000L;
        long bound = wall + ai.badmonkey.agentspaces.common.hlc.HybridLogicalClock.MAX_DRIFT_MILLIS + 5_000L;
        assertThat(Refs.ISSUED_SKEW_MARGIN_MILLIS)
                .isEqualTo(ai.badmonkey.agentspaces.peering.node.PeerNode.FAR_FUTURE_SKEW_MARGIN_MILLIS);
        assertThat(Refs.issuedAttrAcceptable(new ai.badmonkey.agentspaces.common.hlc.HlcTimestamp(bound, 0, "n"), wall))
                .isTrue();
        assertThat(Refs.issuedAttrAcceptable(new ai.badmonkey.agentspaces.common.hlc.HlcTimestamp(bound + 1, 0, "n"), wall))
                .isFalse();
        assertThat(Refs.issuedAttrAcceptable(new ai.badmonkey.agentspaces.common.hlc.HlcTimestamp(wall, 65_536, "n"), wall))
                .isTrue();
        assertThat(Refs.issuedAttrAcceptable(new ai.badmonkey.agentspaces.common.hlc.HlcTimestamp(wall, 65_537, "n"), wall))
                .isFalse();
    }

    /**
     * HLC round six, 2026-11-08 (ruling R1): a head decodes, through the read every entry
     * payload takes, with any application value under {@code pdt.issued} or {@code pdt.issuer},
     * null included; the value stays as it arrived, so the head re-encodes to its own bytes; and
     * a value that is no text reads as no reserved attr. Java once read an integer as its text,
     * a byte string as base64, and dropped the head for an array or a map.
     */
    @Test
    void anyApplicationValueUnderAReservedAttrDecodesAndReadsAsNoTextWhenItIsNone() throws Exception {
        String[] shapes = {"null", "bytes", "list", "map", "integer", "boolean", "float"};
        for (String shape : shapes) {
            java.util.Map<String, Object> attrs = new java.util.LinkedHashMap<>();
            attrs.put("origin", "test");
            attrs.put(PRef.ISSUED_ATTR, "99999999999999:0:zEvil");
            attrs.put(PRef.ISSUER_ATTR, "null".equals(shape) ? null : nonText(shape));
            byte[] wire = rawHead(attrs);
            PRef head = CBOR.fromDocument(wire, PRef.class);
            assertThat(head.violation()).as("a %s attr", shape).isEmpty();
            assertThat(head.attrs()).as("a %s attr", shape).containsKey(PRef.ISSUER_ATTR);
            assertThat(head.attrs().get("origin")).isEqualTo("test");
            assertThat(head.issuerAttr()).as("a %s attr", shape).isNull();
            assertThat(head.issuedAttr()).isEqualTo("99999999999999:0:zEvil");
            assertThat(CBOR.toBytes(head)).as("a %s attr re-encodes to its bytes", shape).isEqualTo(wire);
        }
    }

    /**
     * HLC round six, 2026-11-08 (ruling R1): under any key but the two reserved ones an attr is
     * text, and null or any other item makes the head undecodable, which every implementation
     * counts in {@code headsIgnored}.
     */
    @Test
    void aValueThatIsNoTextUnderAnOrdinaryAttrMakesTheHeadUndecodable() throws Exception {
        String[] shapes = {"null", "bytes", "list", "map", "integer", "boolean", "float"};
        for (String shape : shapes) {
            java.util.Map<String, Object> attrs = new java.util.LinkedHashMap<>();
            attrs.put("origin", "null".equals(shape) ? null : nonText(shape));
            byte[] wire = rawHead(attrs);
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> CBOR.fromDocument(wire, PRef.class))
                    .as("a %s attr", shape).isInstanceOf(java.io.UncheckedIOException.class);
        }
    }

    /**
     * HLC round six, 2026-11-08 (rulings R1 and R3): a signed ref-space entry of the name whose
     * payload decodes to no {@code PRef} (an ordinary attr that is no text, or a payload that is
     * no record at all) gives no head, and the reader counts it in {@code headsIgnored} once
     * while it stays live, as Python, TypeScript and Ruby count it.
     */
    @Test
    void anUndecodableHeadEntryIsIgnoredAndCountedOnceWhileItStaysLive() throws Exception {
        RefFleet.Peer reader = fleet.newPeer("r", 1);
        RefFleet.Peer writer = fleet.newPeer("w", 2, RefFleet.Spec.defaults()
                .refSpace(b -> b.schemaRegistry(new HeadAlias(ForeignHead.class, String.class))), "r");
        fleet.settle(6);
        Ref<PMap<String, String>> onR = reader.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        String root = Cids.of(new byte[] {1}, Cids.DAG_CBOR);
        java.util.Map<String, Object> attrs = new java.util.LinkedHashMap<>();
        attrs.put("x", null);
        java.util.Map<String, String> tags = java.util.Map.of(PRef.NAME_TAG, NAME, PRef.ROOT_TAG, root);
        writer.refSpace().write(new ForeignHead(PRef.VERSION, NAME, PRef.KIND_MAP, root, List.of(), 1, 0,
                PRef.MODE_MULTI, PRef.SEALING_NONE, attrs), Lease.of(Duration.ofMinutes(10)), tags);
        writer.refSpace().write("no head", Lease.of(Duration.ofMinutes(10)), tags);
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> reader.refSpace().knownEntries() == 2)).isTrue();

        assertThat(onR.heads().isEmpty()).isTrue();
        assertThat(reader.pdt().stats().headsIgnored()).isEqualTo(2);
        fleet.settle(4);
        assertThat(onR.heads().isEmpty()).isTrue();
        assertThat(reader.pdt().stats().headsIgnored()).as("counted once while live").isEqualTo(2);
    }

    /** A head record with attrs of any value, written field by field in the order of {@link PRef}. */
    record ForeignHead(int v, String name, String kind, String root, List<String> base, long seq, long size,
                       String mode, String sealing, java.util.Map<String, Object> attrs) {
    }

    /** A schema registry that names the given types as {@link PRef}, so a peer writes foreign payloads as heads. */
    private static final class HeadAlias implements ai.badmonkey.agentspaces.api.spi.SchemaRegistry {
        private final ai.badmonkey.agentspaces.space.local.SimpleSchemaRegistry names =
                new ai.badmonkey.agentspaces.space.local.SimpleSchemaRegistry();
        private final java.util.Set<Class<?>> aliased;

        HeadAlias(Class<?>... aliased) {
            this.aliased = java.util.Set.of(aliased);
        }

        @Override
        public String register(Class<?> entryType) {
            return names.register(aliased.contains(entryType) ? PRef.class : entryType);
        }

        @Override
        public String schemaNameOf(Class<?> entryType) {
            return names.schemaNameOf(aliased.contains(entryType) ? PRef.class : entryType);
        }

        @Override
        public java.util.Optional<Class<?>> classFor(String schemaName) {
            return names.classFor(schemaName);
        }
    }

    /**
     * HLC round six, 2026-11-08 (ruling R1): the 4 KiB attrs bound counts the UTF-8 bytes of the
     * keys and of the text values, so "é" counts two bytes, and a value that is no text under a
     * reserved key counts nothing. Every replica ignores a head one byte over the bound.
     */
    @Test
    void theAttrsBoundCountsTheUtf8BytesOfKeysAndTextValues() throws Exception {
        String reserved = PRef.ISSUER_ATTR;
        int fits = PRef.MAX_ATTRS_BYTES - "a".length() - reserved.length();
        Object[] reservedValues = {1_000_000, true, new byte[] {1, 2, 3}, List.of("x".repeat(64))};
        for (Object value : reservedValues) {
            java.util.Map<String, Object> atBound = new java.util.LinkedHashMap<>();
            atBound.put("a", "é".repeat(fits / 2) + "x".repeat(fits % 2));
            atBound.put(reserved, value);
            assertThat(CBOR.fromDocument(rawHead(atBound), PRef.class).violation()).as("%s at the bound", value).isEmpty();
            java.util.Map<String, Object> over = new java.util.LinkedHashMap<>();
            over.put("a", "é".repeat(fits / 2) + "x".repeat(fits % 2 + 1));
            over.put(reserved, value);
            assertThat(CBOR.fromDocument(rawHead(over), PRef.class).violation()).as("%s one byte over the bound", value)
                    .contains("attrs larger than " + PRef.MAX_ATTRS_BYTES + " bytes");
        }
    }

    private static final ai.badmonkey.agentspaces.common.codec.CborCodec CBOR =
            ai.badmonkey.agentspaces.common.codec.CborCodec.defaultCodec();

    /** An application value of the given shape that is no text. */
    private static Object nonText(String shape) {
        return switch (shape) {
            case "bytes" -> new byte[] {(byte) 0xcc, 0x4b, (byte) 0xe2, (byte) 0xfd, (byte) 0xaa, 0x69};
            case "list" -> List.of("zEvil/app");
            case "map" -> java.util.Map.of("zEvil", "app");
            case "integer" -> 5;
            case "boolean" -> true;
            case "float" -> 1.5;
            default -> throw new IllegalArgumentException(shape);
        };
    }

    /** The fleet's wall time plus the lead, in milliseconds. */
    private long ahead(Duration lead) {
        return fleet.clock.millis() + lead.toMillis();
    }

    /** A canonical stamp the lead ahead of the fleet's wall time. */
    private String aheadStamp(Duration lead) {
        return new ai.badmonkey.agentspaces.common.hlc.HlcTimestamp(ahead(lead), 0, "zEvil").encoded();
    }

    /**
     * The wire bytes of a head record with the given attrs, its fields in record order and its
     * attrs in the order given, as a peer that writes any CBOR item would write them.
     */
    private static byte[] rawHead(java.util.Map<String, Object> attrs) throws Exception {
        com.fasterxml.jackson.databind.ObjectMapper raw =
                new com.fasterxml.jackson.databind.ObjectMapper(new com.fasterxml.jackson.dataformat.cbor.CBORFactory());
        java.util.Map<String, Object> record = new java.util.LinkedHashMap<>();
        record.put("v", PRef.VERSION);
        record.put("name", NAME);
        record.put("kind", PRef.KIND_MAP);
        record.put("root", Cids.of(new byte[] {1}, Cids.DAG_CBOR));
        record.put("base", List.of());
        record.put("seq", 1);
        record.put("size", 0);
        record.put("mode", PRef.MODE_MULTI);
        record.put("sealing", PRef.SEALING_NONE);
        record.put("attrs", attrs);
        return raw.writeValueAsBytes(record);
    }

    /** The peer named in the reserved attrs of the tests above. */
    private static final ai.badmonkey.agentspaces.common.id.PeerId EVIL = ai.badmonkey.agentspaces.common.id.PeerId.of("zEvil");

    /** A head written with the given reserved attrs orders by its entry's own stamp, so the later honest head wins. */
    private void assertHeadOrdersByItsOwnStamp(java.util.Map<String, ?> attrs) throws Exception {
        Fork fork = forkWithAttrs(attrs);
        Ref<PMap<String, String>> onR = fork.reader().pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Heads.Head first = onR.heads().heads().get(0);
        assertThat(first.root()).as("the head with the refused attr orders by its own stamp").isEqualTo(fork.attributed());
        assertThat(first.issuer().peer()).as("and keeps its entry's issuer").isEqualTo(fork.writer());
        assertThat(onR.deref().get("kc")).as("the later honest head wins the key").contains("b");
        assertThat(fork.reader().pdt().stats().headsIgnored()).as("the advisory attrs refuse and count nothing").isZero();
    }

    /** A head written with acceptable reserved attrs orders by them, after the later honest head, and wins the key. */
    private void assertHeadOrdersByTheAttrs(java.util.Map<String, ?> attrs) throws Exception {
        Fork fork = forkWithAttrs(attrs);
        Ref<PMap<String, String>> onR = fork.reader().pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Heads.Head last = onR.heads().heads().get(1);
        assertThat(last.root()).as("the head orders by its attrs").isEqualTo(fork.attributed());
        assertThat(last.issuer().peer()).as("and carries the attributed issuer").isEqualTo(EVIL);
        assertThat(onR.deref().get("kc")).as("the head ordered by its attrs wins the key").contains("a");
        assertThat(fork.reader().pdt().stats().headsIgnored()).isZero();
    }

    /** The reader of a fork, the root of the head written with attrs, and the peer that wrote it. */
    private record Fork(RefFleet.Peer reader, String attributed, ai.badmonkey.agentspaces.common.id.PeerId writer) {
    }

    /**
     * A fork on a reader r: a writes v2a with the given attrs, then b writes v2b, which both set
     * the key kc; returns the fork once r holds both heads.
     */
    private Fork forkWithAttrs(java.util.Map<String, ?> attrs) throws Exception {
        RefFleet.Peer a = fleet.newPeer("a", 1);
        RefFleet.Peer b = fleet.newPeer("b", 2, "a");
        RefFleet.Peer r = fleet.newPeer("r", 3, "a");
        fleet.settle(6);
        Ref<PMap<String, String>> onA = a.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onB = b.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onR = r.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        PMap<String, String> v1 = onA.swap(m -> m.assoc("k0", "v0"));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(v1.root()) && !onB.heads().isEmpty() && !onR.heads().isEmpty())).isTrue();
        fleet.settle(2);
        PMap<String, String> v2a = onA.at(v1.root()).assoc("kc", "a").assoc("ka", "a");
        RefFleet.writeHead(a, NAME, PRef.KIND_MAP, v2a.root(), v1.root(), 2, v2a.count(), Duration.ofMinutes(10),
                attrs);
        fleet.settle(1);
        PMap<String, String> v2b = onB.at(v1.root()).assoc("kc", "b").assoc("kb", "b");
        RefFleet.writeHead(b, NAME, PRef.KIND_MAP, v2b.root(), v1.root(), 2, v2b.count(), Duration.ofMinutes(10));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onR.heads().heads().size() == 2 && fleet.allHold(v2a.root()) && fleet.allHold(v2b.root()))).isTrue();
        return new Fork(r, v2a.root(), a.id());
    }

    @Test
    void historyAndAtWalkRetainedVersionsAndRetentionKeepsSixtyFour() throws Exception {
        // the age rule would keep every version of the last hour; retain by count alone so 64 governs
        RefFleet.Spec byCount = RefFleet.Spec.defaults().pdt(b -> b.retain(64, Duration.ZERO));
        RefFleet.Peer a = fleet.newPeer("a", 1, byCount);
        RefFleet.Peer b = fleet.newPeer("b", 2, byCount, "a");
        fleet.settle(6);
        Ref<PMap<String, String>> onA = a.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onB = b.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        int versions = 70;
        String[] roots = new String[versions];
        for (int i = 0; i < versions; i++) {
            int n = i;
            roots[i] = onA.swap(m -> m.assoc("k" + n, "v" + n)).root();
            if (i % 10 == 9) {
                fleet.tickAll();
            }
        }
        String last = roots[versions - 1];
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> fleet.allHold(last) && onB.heads().roots().equals(List.of(last)))).isTrue();

        assertThat(a.pdt().retention().versions(NAME)).as("retention keeps 64 versions").hasSize(64);
        assertThat(a.pdt().retention().versions(NAME).get(0).root()).isEqualTo(roots[versions - 64]);

        List<Heads.Head> history = onA.history(10);
        assertThat(history).hasSize(10);
        for (int i = 0; i < 10; i++) {
            assertThat(history.get(i).root()).as("newest first").isEqualTo(roots[versions - 2 - i]);
            assertThat(history.get(i).seq()).isEqualTo(versions - 1 - i);
        }
        assertThat(onA.history(1000)).as("every earlier version is still held on the writer").hasSize(versions - 1);
        PMap<String, String> v20 = onA.at(roots[19]);
        assertThat(v20.count()).isEqualTo(20);
        assertThat(v20.get("k19")).contains("v19");
        assertThat(v20.get("k20")).isEmpty();
        assertThat(onA.deref().root()).isEqualTo(last);

        // the reader took every version inline and holds the same history
        List<Heads.Head> onBHistory = onB.history(5);
        assertThat(onBHistory).extracting(Heads.Head::root)
                .containsExactly(roots[versions - 2], roots[versions - 3], roots[versions - 4], roots[versions - 5], roots[versions - 6]);
        assertThat(onB.at(roots[0]).count()).isEqualTo(1);
        assertThat(b.pdt().retention().versions(NAME)).hasSize(64);
        System.out.printf("PD-R-09: %d versions, history(10) newest first from seq %d, at(v20) has 20 entries, retention %d on both peers%n",
                versions, history.get(0).seq(), a.pdt().retention().versions(NAME).size());
    }
}
