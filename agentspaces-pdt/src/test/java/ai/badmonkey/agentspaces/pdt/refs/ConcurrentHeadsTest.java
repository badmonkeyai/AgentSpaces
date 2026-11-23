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
import ai.badmonkey.agentspaces.api.space.Matchers;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.measure.Direction;
import ai.badmonkey.agentspaces.measure.Kind;
import ai.badmonkey.agentspaces.measure.Measure;
import ai.badmonkey.agentspaces.measure.Params;
import ai.badmonkey.agentspaces.measure.Rec;
import ai.badmonkey.agentspaces.measure.Unit;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Scenario 5 of srad/agentspaces-pdt.md (§6.4 S4, PD-Q-REL-02, PD-R-02, PD-R-05):
 * five writers swap one multi-mode ref concurrently while eight readers
 * deref; once gossip settles every peer derefs the same root, and every reader
 * counted the same conflicts, because the heads, the base walk, and the rule
 * are the same everywhere; a reader that joins after the writers finish and a
 * reader restarted over its store (its version memory gone) deref the same
 * root too, because every version on a path from a head to the common base
 * stays a live entry while the fork lasts (PD-R-06, review of 2026-11-08), and
 * a version whose entry left anyway is read from its chain node (PDT 2 phase 1,
 * P2-V-02, P2-V-04).
 */
class ConcurrentHeadsTest {

    private static final String NAME = "ledger";
    private static final int WRITERS = 5;
    private static final int READERS = 8;
    private static final int SWAPS = 100;
    private static final int KEYS = 40;
    /**
     * Three seeded repeats take about 250 s at a 1-minute load of 5 (about 80 s each on the inline fabric, most of
     * it in the node ticks, and 20 to 36 s of it in the settle wait; concurrent heads, srad/agentspaces-pdt2.md
     * §10.20), where they took 9 s each before the swarm retries; {@code -Dpdt.repeats=10} runs
     * the row's ten, and {@code -Dpdt.repeats=20} the twenty loaded repeats of PDT 2 phase 1, P2-V-04, in which the
     * late joiner walks the version chain (P2-V-02) past every version whose entry is gone. On 2026-11-10 the three
     * took 70 to 75 s of wall and 124 to 131 s of CPU with every frame signed, and 20 to 24 s and 48 to 55 s once
     * {@link RefFleet} ran {@code ChannelAuth.SESSION} (SESSION test fleets, ROBUSTNESS.md §10.10).
     */
    private static final int REPEATS = Integer.getInteger("pdt.repeats", 3);

    @Test
    void fiveWritersAndEightReadersConvergeOnOneRootWithEqualConflicts() throws Exception {
        long started = System.nanoTime();
        for (int repeat = 0; repeat < REPEATS; repeat++) {
            long seed = Long.getLong("pdt.seedBase", 1000L) + repeat;
            RefFleet fleet = new RefFleet();
            try {
                run(fleet, seed, repeat);
            } finally {
                fleet.close();
            }
        }
        System.out.printf("PD-Q-REL-02: %d seeded repeats in %d ms%n", REPEATS, (System.nanoTime() - started) / 1_000_000);
    }

    /**
     * R1 probe P10 (reviews/RBE-REGRESSION-MATRIX.md §3; replication by entry phase 1, RE-M-08): a head and its
     * successor reach a follower in one {@code PULL_RESP}, so their WRITTEN events fire after both are in. The follower
     * hears the ref's space only through anti-entropy (its rumour handler for the ref space drops every item), so the
     * two heads come in the one batch a pull carries. The first head carries its delta inline; the successor's delta
     * passes the inline bound, so the follower fetches it. The follower must end on the successor alone, as the writer
     * and a live follower do, fetch the successor once and the inline head never, and tell its watchers each root at
     * most once, the successor last.
     */
    @Test
    void aHeadAndItsSuccessorInOnePullRespLeaveTheFollowerOnTheSuccessor() throws Exception {
        // On the text form of §5.3 the pull carries the two heads in the map's order, not the order written: fresh
        // fleets until both orders ran. On the audit of annex I.6 (replication by entry, phase 4) the follower fetches
        // them by id and the answer carries them newest first (RE-I-06), so the successor's event always fires first.
        Set<String> orders = new HashSet<>();
        for (int run = 0; run < 16 && orders.size() < 2; run++) {
            orders.add(headAndSuccessorInOnePull(4100 + 10L * run, false));
        }
        assertThat(orders).as("the successor's event fired first in some run, and the head's in another")
                .containsExactlyInAnyOrder("first", "second");
        for (int run = 0; run < 2; run++) {
            assertThat(headAndSuccessorInOnePull(4100 + 500 + 10L * run, true)).as("newest first under the audit")
                    .isEqualTo("second");
        }
    }

    /** One run of P10 on a fresh fleet; returns which head's WRITTEN event fired first on the follower. */
    private static String headAndSuccessorInOnePull(long seed, boolean audit) throws Exception {
        RefFleet fleet = new RefFleet().audit(audit);
        try {
            // the writer cancels its displaced head only after a long grace, so both heads are live when the pull runs
            RefFleet.Peer writer = fleet.newPeer("w", seed,
                    RefFleet.Spec.defaults().pdt(b -> b.cancelGrace(Duration.ofMinutes(5))));
            RefFleet.Peer follower = fleet.newPeer("f", seed + 1, "w");
            RefFleet.Peer live = fleet.newPeer("g", seed + 2, "w");
            fleet.settle(12);
            Ref<PMap<String, String>> ref = writer.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            Ref<PMap<String, String>> onFollower = follower.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            Ref<PMap<String, String>> onLive = live.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            List<String> delivered = new CopyOnWriteArrayList<>();
            onFollower.watch(value -> delivered.add(value.root()));
            // at each head WRITTEN event: its root, and the live head entries the follower's space holds then
            List<String> eventRoots = new CopyOnWriteArrayList<>();
            List<Integer> headsAtEvent = new CopyOnWriteArrayList<>();
            follower.refSpace().notify(Template.of(PRef.class).whereTag(PRef.NAME_TAG, Matchers.eq(NAME)), event -> {
                if (event.kind() == SpaceEvent.Kind.WRITTEN) {
                    eventRoots.add(event.entry().root());
                    headsAtEvent.add(RefFleet.headEntries(follower, NAME).size());
                }
            }, Lease.of(Duration.ofHours(1)));
            // the follower hears the ref space only through anti-entropy: an inbound link filter loses every rumor on the
            // stream before the follower's node reads it, where the test once replaced the shipped handler
            // (srad/spoiler-peers.md SP-S-01); a batch would carry other frames too, so none may carry such a rumor
            String refStream = "space:" + RefFleet.REF_SPACE;
            java.util.concurrent.atomic.AtomicLong deafened = new java.util.concurrent.atomic.AtomicLong();
            java.util.concurrent.atomic.AtomicLong batched = new java.util.concurrent.atomic.AtomicLong();
            fleet.network.filter(null, "f", frame -> {
                ai.badmonkey.agentspaces.test.FrameHeader h = frame.header().orElse(null);
                if (h != null && h.kind().equals("RUMOR") && h.carriesStream(refStream)) {
                    deafened.incrementAndGet();
                    return ai.badmonkey.agentspaces.test.LinkFilter.Verdict.drop();
                }
                if (h != null && h.kind().equals("BATCH") && h.carriesStream(refStream)) {
                    batched.incrementAndGet();
                }
                return ai.badmonkey.agentspaces.test.LinkFilter.Verdict.pass();
            });
            long pullsBefore = follower.runtime().gossip().pacingSignals().pullRespReceived();

            PMap<String, String> first = ref.swap(m -> m.assoc("small", "x"));
            Random random = new Random(seed + 3);
            PMap<String, String> second = ref.swap(m -> {
                PMap<String, String> out = m;
                for (int i = 0; i * 120 < 3 * PRef.INLINE_DELTA_BYTES; i++) {
                    out = out.assoc("big-" + i, RefFleet.value(100, random));
                }
                return out;
            });
            assertThat(RefFleet.headEntries(writer, NAME)).as("both heads live on the writer").hasSize(2);
            assertThat(writer.pdt().refs().heads(NAME).roots()).containsExactly(second.root());

            assertThat(fleet.runUntil("pdt.heads.successor-followed", Duration.ofSeconds(60), () -> onFollower.heads().roots().equals(List.of(second.root()))
                    && follower.store().complete(second.root()) && delivered.contains(second.root())
                    && onLive.heads().roots().equals(List.of(second.root()))))
                    .as("the follower and the live follower end on the successor and hold it").isTrue();
            fleet.settle(8);

            assertThat(follower.runtime().gossip().pacingSignals().pullRespReceived()).as("the heads came by a pull")
                    .isGreaterThan(pullsBefore);
            assertThat(deafened.get()).as("the filter lost the ref space's rumors to the follower").isPositive();
            assertThat(batched.get()).as("no batch carried a ref-space rumor past the filter").isZero();
            assertThat(eventRoots).as("a WRITTEN per head").containsExactlyInAnyOrder(first.root(), second.root());
            assertThat(headsAtEvent).as("both heads were in when the first event fired: one batch").containsOnly(2);
            assertThat(onFollower.heads().roots()).as("the successor alone is a head").containsExactly(second.root());
            assertThat(onFollower.deref().root()).isEqualTo(second.root()).isEqualTo(onLive.deref().root())
                    .isEqualTo(ref.deref().root());
            assertThat(follower.store().complete(first.root())).as("the inline head's nodes were stored").isTrue();
            assertThat(follower.pdt().stats().deltasFetched()).as("the successor fetched once, the inline head never")
                    .isEqualTo(1);
            assertThat(delivered).as("each root told to the watchers at most once").doesNotHaveDuplicates();
            assertThat(delivered).as("the successor last").last().isEqualTo(second.root());
            assertThat(delivered).isSubsetOf(first.root(), second.root());
            String firstEvent = eventRoots.get(0).equals(first.root()) ? "first" : "second";
            System.out.printf("P10 heads in one PULL_RESP (seed %d): %s head's event first; watchers heard %s; heads at the"
                            + " events %s; %d deltas and %d bases fetched%n", seed, firstEvent,
                    delivered.stream().map(r -> r.equals(first.root()) ? "first" : "second").toList(), headsAtEvent,
                    follower.pdt().stats().deltasFetched(), follower.pdt().stats().basesFetched());
            return firstEvent;
        } finally {
            fleet.close();
        }
    }

    private static void missedBase(PConflict conflict, List<PConflict> missed) {
        if (PConflict.NO_COMMON_BASE.equals(conflict.reason())) {
            missed.add(conflict);
        }
    }

    private void run(RefFleet fleet, long seed, int repeat) throws Exception {
        long fleetStart = System.nanoTime();
        List<RefFleet.Peer> writers = new ArrayList<>();
        List<RefFleet.Peer> readers = new ArrayList<>();
        RefFleet.Peer first = fleet.newPeer("w0", seed);
        writers.add(first);
        for (int i = 1; i < WRITERS; i++) {
            writers.add(fleet.newPeer("w" + i, seed + i, "w0"));
        }
        for (int i = 0; i < READERS; i++) {
            readers.add(fleet.newPeer("r" + i, seed + 100 + i, "w0"));
        }
        fleet.settle(12);
        long fleetMillis = (System.nanoTime() - fleetStart) / 1_000_000;
        // concurrent heads (srad/agentspaces-pdt2.md §10.20): a writer's swap over a merge that missed its base writes the
        // later head whole, and every peer then converges on a value that lost the other heads' writes, so the run counts
        // every merge on every peer, the writers' swaps too
        List<PConflict> missedBases = new CopyOnWriteArrayList<>();
        List<Ref<PMap<String, String>>> writerRefs = new ArrayList<>();
        for (RefFleet.Peer w : writers) {
            Ref<PMap<String, String>> ref = w.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            ref.onConflict(c -> missedBase(c, missedBases));
            writerRefs.add(ref);
        }
        List<Ref<PMap<String, String>>> readerRefs = new ArrayList<>();
        List<List<PConflict>> conflictsSeen = new ArrayList<>();
        for (RefFleet.Peer r : readers) {
            Ref<PMap<String, String>> ref = r.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            List<PConflict> seen = new CopyOnWriteArrayList<>();
            ref.onConflict(seen::add);
            ref.onConflict(c -> missedBase(c, missedBases));
            readerRefs.add(ref);
            conflictsSeen.add(seen);
        }
        long t0 = System.nanoTime();
        // the writers run on their own threads, so swaps overlap and heads fork (S4), and each swap waits for the
        // next gossip round (a tick) as a real writer's swaps spread over gossip periods; the test thread ticks
        List<Thread> threads = new ArrayList<>();
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        for (int w = 0; w < WRITERS; w++) {
            int writer = w;
            Ref<PMap<String, String>> ref = writerRefs.get(w);
            Random random = new Random(seed * 31 + w);
            threads.add(Thread.ofVirtual().name("writer-" + w).start(() -> {
                try {
                    for (int round = 0; round < SWAPS; round++) {
                        String key = "k" + random.nextInt(KEYS);
                        String value = "w" + writer + ":" + round + ":" + RefFleet.value(8, random);
                        ref.swap(m -> m.assoc(key, value));
                        long tick = fleet.ticks.get();
                        while (fleet.ticks.get() == tick) {
                            Thread.sleep(1);
                        }
                    }
                } catch (Throwable t) {
                    failures.add(t);
                }
            }));
        }
        int derefs = 0;
        while (threads.stream().anyMatch(Thread::isAlive)) {
            fleet.tickAll();
            if (derefs++ % 20 == 19) {
                for (Ref<PMap<String, String>> ref : readerRefs) {
                    assertThat(ref.deref().count()).isLessThanOrEqualTo(KEYS);
                }
            }
            Thread.sleep(1);
        }
        assertThat(failures).isEmpty();
        long swapMillis = (System.nanoTime() - t0) / 1_000_000;
        long swapTicks = fleet.ticks.get();
        long writerTickMillis = writers.stream().mapToLong(p -> RefFleet.loopBusyNanos(p)).sum() / 1_000_000;
        long readerTickMillis = readers.stream().mapToLong(p -> RefFleet.loopBusyNanos(p)).sum() / 1_000_000;
        int writerConflicts = 0;
        // concurrent heads (srad/agentspaces-pdt2.md §10.20): the writers are done, but the cancels of superseded heads
        // still travel by lazy repair, and a member that hears one cancel before another reads an older entry as a head
        // again for a while; the fleet has settled once every peer derefs one root and keeps it for a cancel grace and a
        // repair window, and it must settle within that and the longest retry of a head fetch or a chain node
        long quiet = Refs.CANCEL_GRACE_MILLIS
                + fleet.peers.stream().mapToLong(p -> p.runtime().gossip().repairWindowMillis()).max().orElseThrow();
        Duration bound = Duration.ofMillis(quiet + Refs.UNLOADABLE_BACKOFF * RefFleet.DEADLINE.toMillis() * 3 / 2);
        List<Ref<PMap<String, String>>> everyRef = new ArrayList<>(writerRefs);
        everyRef.addAll(readerRefs);
        long t1 = System.nanoTime();
        long settleFrom = fleet.clock.millis();
        String[] agreed = {null};
        long[] agreedSince = {0};
        int[] polls = {0};
        assertThat(fleet.runFor(bound, () -> {
            polls[0]++;
            Set<String> held = new HashSet<>();
            for (Ref<PMap<String, String>> ref : everyRef) {
                held.add(ref.deref().root());
            }
            String root = held.size() == 1 ? held.iterator().next() : null;
            if (root == null || !root.equals(agreed[0])) {
                agreed[0] = root;
                agreedSince[0] = fleet.clock.millis();
                return false;
            }
            return fleet.clock.millis() - agreedSince[0] >= quiet;
        })).as("every peer derefs one root and keeps it").isTrue();
        long settleMillis = (System.nanoTime() - t1) / 1_000_000;
        long settleFleetMillis = agreedSince[0] - settleFrom;
        Heads settled = readers.get(0).pdt().refs().heads(NAME);
        Set<String> roots = new HashSet<>();
        Set<Long> readerConflicts = new HashSet<>();
        Map<String, String> reference = null;
        List<String> firstReaderConflicts = new ArrayList<>();
        for (int i = 0; i < readers.size(); i++) {
            long before = readers.get(i).pdt().stats().conflicts();
            int seenBefore = conflictsSeen.get(i).size();
            PMap<String, String> value = readerRefs.get(i).deref();
            roots.add(value.root());
            readerConflicts.add(readers.get(i).pdt().stats().conflicts() - before);
            for (PConflict c : conflictsSeen.get(i).subList(seenBefore, conflictsSeen.get(i).size())) {
                assertThat(c.reason()).as("every final merge found its common base").isEqualTo(PConflict.LATER_HLC);
                assertThat(c.key()).isNotNull();
                if (i == 0) {
                    firstReaderConflicts.add(describe(c));
                }
            }
            Map<String, String> plain = value.toMap();
            if (reference == null) {
                reference = plain;
            } else {
                assertThat(plain).as(readers.get(i).name()).isEqualTo(reference);
            }
        }
        for (int w = 0; w < WRITERS; w++) {
            roots.add(writerRefs.get(w).deref().root());
            writerConflicts += (int) writers.get(w).pdt().stats().conflicts();
        }
        assertThat(roots).as("one deref root on 13 peers").hasSize(1);
        assertThat(missedBases).as("every merge on every peer, the writers' swaps too, found its common base").isEmpty();
        assertThat(readerConflicts).as("the final deref counted the same conflicts on every reader").hasSize(1);
        long perReader = readerConflicts.iterator().next();
        for (List<PConflict> seen : conflictsSeen) {
            for (PConflict c : seen) {
                assertThat(c.name()).isEqualTo(NAME);
                assertThat(c.winnerRoot()).isNotEqualTo(c.loserRoot());
            }
        }
        assertThat(reference).hasSizeLessThanOrEqualTo(KEYS);
        // a late joiner and a restarted reader (same store, empty version memory) see the same heads and deref the same root
        String referenceRoot = roots.iterator().next();
        RefFleet.Peer restarted = readers.get(READERS - 1);
        fleet.kill(restarted);
        RefFleet.Peer again = fleet.newPeer("r-again", seed + 200, RefFleet.Spec.defaults().store(restarted.store()), "w0");
        RefFleet.Peer joiner = fleet.newPeer("l", seed + 201, "w0");
        Ref<PMap<String, String>> onAgain = again.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onJoiner = joiner.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        List<String> settledRoots = settled.roots();
        // The joiner and the restarted reader are judged by the fleet's own rule: they have settled once they hold the
        // settled heads complete and deref the reference root, and keep both for the quiet window. A single moment of
        // agreement is not enough (R2, 2026-11-16): a peer without the version memory of the run can read an older live
        // entry as a head again after the heads first agreed, until the chain nodes or the cancels that settle it arrive
        // (one traced failure held 9 heads at its deref where the readers held 6, two ticks after it had held the 6), and
        // a deref whose base fetch failed merges the later head whole until the fetch is retried. Either way it counted
        // other conflicts than the readers, or derefed another root, for a while; both converge, as
        // srad/agentspaces-pdt2.md §10.20 records for the fleet.
        long[] steadySince = {-1};
        int[] waitPolls = {0};
        long joinFrom = fleet.clock.millis();
        // a merge without its base (a base fetch that failed, retried by a later deref) is not settled either, even when
        // the later head whole happens to give the reference root
        java.util.concurrent.atomic.AtomicBoolean withoutBase = new java.util.concurrent.atomic.AtomicBoolean();
        try (var noBase = onJoiner.onConflict(c -> withoutBase.compareAndSet(false, PConflict.NO_COMMON_BASE.equals(c.reason())));
             var noBaseAgain = onAgain.onConflict(c -> withoutBase.compareAndSet(false, PConflict.NO_COMMON_BASE.equals(c.reason())))) {
            assertThat(fleet.runFor(bound.plusMillis(quiet), () -> {
                waitPolls[0]++;
                boolean steady = onAgain.heads().roots().equals(settledRoots)
                        && onJoiner.heads().roots().equals(settledRoots)
                        && settledRoots.stream().allMatch(root -> joiner.store().complete(root) && again.store().complete(root))
                        && onJoiner.deref().root().equals(referenceRoot)
                        && onAgain.deref().root().equals(referenceRoot);
                steady &= !withoutBase.getAndSet(false); // judged after this poll's derefs, so they count too
                if (!steady) {
                    steadySince[0] = -1;
                    return false;
                }
                if (steadySince[0] < 0) {
                    steadySince[0] = fleet.clock.millis();
                }
                return fleet.clock.millis() - steadySince[0] >= quiet;
            })).as("the joiner and the restarted reader hold the settled heads and deref the reference root for the quiet window")
                    .isTrue();
        }
        long joinSettleMillis = steadySince[0] - joinFrom;
        // The two settle times are fleet-clock spans of a protocol bound (the cancel grace, the repair window and the
        // retries, asserted above on the fleet clock); they are recorded so a slower repair shows as a number
        // (srad/spoiler-peers.md §3.5, "Measurement").
        Measure.record("pdt.heads.settle", Unit.MS, Direction.LOWER, settleFleetMillis,
                Rec.with().kind(Kind.COUNT).params(Params.of("clock", "fleet", "writers", WRITERS, "readers", READERS)));
        Measure.record("pdt.heads.join-settle", Unit.MS, Direction.LOWER, joinSettleMillis,
                Rec.with().kind(Kind.COUNT).params(Params.of("clock", "fleet", "writers", WRITERS, "readers", READERS)));
        // the final deref, with no tick since the last check: the heads it merges are the settled ones
        List<PConflict> joinerConflicts = new CopyOnWriteArrayList<>();
        onJoiner.onConflict(joinerConflicts::add);
        List<String> joinerHeads = onJoiner.heads().roots();
        assertThat(onJoiner.deref().root()).as("late joiner").isEqualTo(referenceRoot);
        assertThat(onAgain.deref().root()).as("restarted reader").isEqualTo(referenceRoot);
        assertThat(joinerConflicts).as("the joiner's merge found the common base").extracting(PConflict::reason)
                .doesNotContain(PConflict.NO_COMMON_BASE);
        assertThat(joinerConflicts).as("the joiner's final merge counted the first reader's conflicts (joiner heads %s,"
                        + " settled heads %s, chain nodes the joiner left out %d; first reader %s; joiner %s)",
                joinerHeads.equals(settledRoots) ? "equal to the" : joinerHeads, settledRoots,
                joiner.pdt().refs().chainNodesMissed(), firstReaderConflicts,
                joinerConflicts.stream().map(ConcurrentHeadsTest::describe).sorted().toList())
                .hasSize((int) perReader);
        long basesFetched = fleet.peers.stream().mapToLong(p -> p.pdt().stats().basesFetched()).sum();
        long deltasFetched = fleet.peers.stream().mapToLong(p -> p.pdt().stats().deltasFetched()).sum();
        System.out.printf("PD-Q-REL-02 repeat %d: %d swaps by %d writers over %d ticks (fleet %d ms, swaps %d ms, one root after %d ms"
                        + " of fleet time, settle %d ms in %d polls;"
                        + " node ticks writers %d ms, readers %d ms) ended on %d head(s) at seq %d: one deref root on 13 peers, %d conflicts per reader"
                        + " on the final deref, %d conflicts across the writers, %d keys, %d deltas and %d bases fetched fleet-wide, %d ref entries known,"
                + " %d live; late joiner and restarted reader deref the same root with %d conflicts (joiner fetched %d bases;"
                + " they settled after %d ms of fleet time, %d polls)%n",
                repeat, SWAPS * WRITERS, WRITERS, swapTicks, fleetMillis, swapMillis, settleFleetMillis, settleMillis, polls[0],
                writerTickMillis, readerTickMillis,
                settled.heads().size(), settled.maxSeq(), perReader, writerConflicts, reference.size(), deltasFetched, basesFetched,
                readers.get(0).refSpace().knownEntries(), RefFleet.headEntries(readers.get(0), NAME).size(), joinerConflicts.size(),
                joiner.pdt().stats().basesFetched(), joinSettleMillis, waitPolls[0]);
    }

    /** A conflict as its pair of heads and key, for a failure message that compares two readers. */
    private static String describe(PConflict c) {
        return c.winnerRoot().substring(7, 13) + "<" + c.loserRoot().substring(7, 13) + ":" + c.key();
    }
}
