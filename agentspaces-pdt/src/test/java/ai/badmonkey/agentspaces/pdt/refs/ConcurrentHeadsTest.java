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
     * Three seeded repeats fit the 60 s budget (about 9 s each on the inline fabric); {@code -Dpdt.repeats=10} runs
     * the row's ten, and {@code -Dpdt.repeats=20} the twenty loaded repeats of PDT 2 phase 1, P2-V-04, in which the
     * late joiner walks the version chain (P2-V-02) past every version whose entry is gone.
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
        List<Ref<PMap<String, String>>> writerRefs = new ArrayList<>();
        for (RefFleet.Peer w : writers) {
            writerRefs.add(w.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR));
        }
        List<Ref<PMap<String, String>>> readerRefs = new ArrayList<>();
        List<List<PConflict>> conflictsSeen = new ArrayList<>();
        for (RefFleet.Peer r : readers) {
            Ref<PMap<String, String>> ref = r.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            List<PConflict> seen = new CopyOnWriteArrayList<>();
            ref.onConflict(seen::add);
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
        long writerTickMillis = writers.stream().mapToLong(p -> fleet.nodeTickNanos.get(p.name()).get()).sum() / 1_000_000;
        long readerTickMillis = readers.stream().mapToLong(p -> fleet.nodeTickNanos.get(p.name()).get()).sum() / 1_000_000;
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
        for (int i = 0; i < readers.size(); i++) {
            long before = readers.get(i).pdt().stats().conflicts();
            int seenBefore = conflictsSeen.get(i).size();
            PMap<String, String> value = readerRefs.get(i).deref();
            roots.add(value.root());
            readerConflicts.add(readers.get(i).pdt().stats().conflicts() - before);
            for (PConflict c : conflictsSeen.get(i).subList(seenBefore, conflictsSeen.get(i).size())) {
                assertThat(c.reason()).as("every final merge found its common base").isEqualTo(PConflict.LATER_HLC);
                assertThat(c.key()).isNotNull();
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
        assertThat(fleet.runFor(bound, () -> onAgain.heads().roots().equals(settledRoots)
                && onJoiner.heads().roots().equals(settledRoots)
                && settledRoots.stream().allMatch(root -> joiner.store().complete(root) && again.store().complete(root))))
                .as("the joiner and the restarted reader see the settled heads and hold them").isTrue();
        fleet.settle(2);
        List<PConflict> joinerConflicts = new CopyOnWriteArrayList<>();
        onJoiner.onConflict(joinerConflicts::add);
        assertThat(onJoiner.deref().root()).as("late joiner").isEqualTo(referenceRoot);
        assertThat(onAgain.deref().root()).as("restarted reader").isEqualTo(referenceRoot);
        assertThat(joinerConflicts).as("the joiner's merge found the common base").extracting(PConflict::reason)
                .doesNotContain(PConflict.NO_COMMON_BASE);
        assertThat(joinerConflicts).hasSize((int) perReader);
        long basesFetched = fleet.peers.stream().mapToLong(p -> p.pdt().stats().basesFetched()).sum();
        long deltasFetched = fleet.peers.stream().mapToLong(p -> p.pdt().stats().deltasFetched()).sum();
        System.out.printf("PD-Q-REL-02 repeat %d: %d swaps by %d writers over %d ticks (fleet %d ms, swaps %d ms, one root after %d ms"
                        + " of fleet time, settle %d ms in %d polls;"
                        + " node ticks writers %d ms, readers %d ms) ended on %d head(s) at seq %d: one deref root on 13 peers, %d conflicts per reader"
                        + " on the final deref, %d conflicts across the writers, %d keys, %d deltas and %d bases fetched fleet-wide, %d ref entries known,"
                + " %d live; late joiner and restarted reader deref the same root with %d conflicts (joiner fetched %d bases)%n",
                repeat, SWAPS * WRITERS, WRITERS, swapTicks, fleetMillis, swapMillis, settleFleetMillis, settleMillis, polls[0],
                writerTickMillis, readerTickMillis,
                settled.heads().size(), settled.maxSeq(), perReader, writerConflicts, reference.size(), deltasFetched, basesFetched,
                readers.get(0).refSpace().knownEntries(), RefFleet.headEntries(readers.get(0), NAME).size(), joinerConflicts.size(),
                joiner.pdt().stats().basesFetched());
    }
}
