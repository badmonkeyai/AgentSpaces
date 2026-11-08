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
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Linear mode (srad/agentspaces-pdt.md §3.6, §6.4 S5, PD-R-04): a swap takes the
 * head under the space's conflict strategy and completes the take with the
 * successor, so two writers never produce two heads, and a writer that dies
 * between take and complete releases the take at its lease. After the review
 * of 2026-11-08: a writer that never read the ref takes twice before a first
 * head, two first heads merge key-wise over the empty value and the next swap
 * folds them, and a reader that never deref'd reads the taken version rather
 * than the empty value.
 */
class LinearRefTest {

    private static final String NAME = "ledger";

    private final RefFleet fleet = new RefFleet();

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    private static RefFleet.Spec linear() {
        return RefFleet.Spec.defaults().pdt(b -> b.mode(NAME, PRef.MODE_LINEAR)
                .take(Duration.ofSeconds(5), Duration.ofMillis(300), 100));
    }

    @Test
    void twoWritersOnALinearRefNeverProduceTwoHeads() throws Exception {
        RefFleet.Peer a = fleet.newPeer("a", 1, linear());
        RefFleet.Peer b = fleet.newPeer("b", 2, linear(), "a");
        RefFleet.Peer c = fleet.newPeer("c", 3, linear(), "a");
        fleet.settle(6);
        Ref<PMap<String, String>> onA = a.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onB = b.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onC = c.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        assertThat(onA.mode()).isEqualTo(PRef.MODE_LINEAR);
        PMap<String, String> v1 = onA.swap(m -> m.assoc("first", "a"));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(v1.root()))).isTrue();
        assertThat(RefFleet.headEntries(a, NAME).get(0).value().mode()).isEqualTo(PRef.MODE_LINEAR);

        int perWriter = 20;
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        AtomicInteger maxHeads = new AtomicInteger();
        List<Thread> threads = new ArrayList<>();
        for (int w = 0; w < 2; w++) {
            Ref<PMap<String, String>> ref = w == 0 ? onA : onB;
            String who = w == 0 ? "a" : "b";
            threads.add(Thread.ofVirtual().start(() -> {
                try {
                    for (int i = 0; i < perWriter; i++) {
                        int n = i;
                        ref.swap(m -> m.assoc(who + "-" + n, who));
                    }
                } catch (Throwable t) {
                    failures.add(t);
                }
            }));
        }
        while (threads.stream().anyMatch(Thread::isAlive)) {
            fleet.tickAll();
            for (RefFleet.Peer p : fleet.peers) {
                // visible heads: the one head, or none while a writer holds the take
                int heads = p.pdt().refs().heads(NAME).heads().size();
                maxHeads.accumulateAndGet(heads, Math::max);
                assertThat(RefFleet.headEntries(p, NAME).size()).as("live head entries at " + p.name()).isLessThanOrEqualTo(1);
            }
            Thread.sleep(1);
        }
        assertThat(failures).isEmpty();
        assertThat(maxHeads.get()).as("never two heads").isEqualTo(1);
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> {
            Heads heads = onC.heads();
            return heads.heads().size() == 1 && heads.maxSeq() == 1 + 2 * perWriter && fleet.allHold(heads.latest().root());
        })).as("the reader sees the final head").isTrue();
        PMap<String, String> finalValue = onC.deref();
        assertThat(finalValue.count()).isEqualTo(1 + 2 * perWriter);
        assertThat(onA.deref().root()).isEqualTo(finalValue.root());
        assertThat(onB.deref().root()).isEqualTo(finalValue.root());
        assertThat(RefFleet.headEntries(c, NAME)).hasSize(1);
        assertThat(a.pdt().stats().conflicts() + b.pdt().stats().conflicts() + c.pdt().stats().conflicts()).isZero();
        System.out.printf("PD-R-04: 2 writers, %d linear swaps each, seq %d, at most %d head visible at any poll%n",
                perWriter, onC.heads().maxSeq(), maxHeads.get());
    }

    @Test
    void aFreshWriterUnderAnotherWritersTakeRetriesAndAFirstHeadRaceHealsOnTheNextSwap() throws Exception {
        RefFleet.Spec twoTries = RefFleet.Spec.defaults().pdt(b -> b.mode(NAME, PRef.MODE_LINEAR)
                .take(Duration.ofSeconds(5), Duration.ofMillis(300), 1));
        RefFleet.Peer a = fleet.newPeer("a", 1, twoTries);
        RefFleet.Peer b = fleet.newPeer("b", 2, twoTries, "a");
        fleet.settle(6);
        Ref<PMap<String, String>> onA = a.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        PMap<String, String> v1 = onA.swap(m -> m.assoc("first", "a"));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> a.store().complete(v1.root()) && RefFleet.headEntries(b, NAME).size() == 1)).isTrue();
        // a takes the head as a slow swap would; b's handle is created only now, so b never saw a version or the take
        Template<PRef> template = Template.of(PRef.class).whereTag(PRef.NAME_TAG, Matchers.eq(NAME));
        Optional<TakenEntry<PRef>> taken = a.refSpace().take(template, Lease.of(Duration.ofMinutes(10)), Duration.ofSeconds(1));
        assertThat(taken).isPresent();
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> RefFleet.headEntries(b, NAME).isEmpty())).isTrue();
        Ref<PMap<String, String>> onB = b.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        long started = System.nanoTime();
        Thread writer = Thread.ofVirtual().start(() -> {
            try {
                onB.swap(m -> m.assoc("second", "b"));
            } catch (Throwable t) {
                failures.add(t);
            }
        });
        while (writer.isAlive()) {
            fleet.tickAll();
            Thread.sleep(1);
        }
        long waited = (System.nanoTime() - started) / 1_000_000;
        // the space hides a taken entry, so a handle that never saw the ref cannot tell a take from an empty ref: it takes
        // twice before it writes a first head (the residual race of PD-R-04, recorded in the SRAD)
        assertThat(failures).isEmpty();
        assertThat(waited).as("two takes timed out before the first head").isGreaterThanOrEqualTo(550);
        List<PRef> onBEntries = RefFleet.headEntries(b, NAME).stream().map(e -> e.value()).toList();
        assertThat(onBEntries).hasSize(1);
        assertThat(onBEntries.get(0).seq()).isEqualTo(1);
        assertThat(onBEntries.get(0).base()).isEmpty();
        // the take completes with its version: two first heads exist, and the next swap folds them into one head
        // with every key kept, because two first versions merge key-wise over the empty value
        PRef same = taken.get().entry().withoutNodes();
        a.refSpace().complete(taken.get(), same, Lease.of(Duration.ofMinutes(10)), same.tags());
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onB.heads().heads().size() == 2 && fleet.allHold(v1.root()))).isTrue();
        assertThat(onB.deref().count()).as("both first versions are read, neither dropped").isEqualTo(2);
        assertThat(a.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR).deref().count()).isEqualTo(2);
        PMap<String, String> v3 = onB.swap(m -> m.assoc("third", "b"));
        assertThat(v3.count()).isEqualTo(3);
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> onB.heads().roots().equals(List.of(v3.root()))
                && onA.heads().roots().equals(List.of(v3.root())))).as("one head again").isTrue();
        PRef healed = RefFleet.headEntries(b, NAME).stream().map(e -> e.value()).filter(h -> h.root().equals(v3.root())).findFirst().orElseThrow();
        assertThat(healed.base()).hasSize(2);
        assertThat(healed.seq()).isEqualTo(2);
        assertThat(onA.deref().get("first")).contains("a");
        assertThat(onA.deref().get("second")).contains("b");
        assertThat(onA.deref().get("third")).contains("b");
        System.out.printf("PD-R-04: a fresh writer took twice (%d ms) before a first head; the next swap folded the two first heads into seq %d with %d keys%n",
                waited, healed.seq(), v3.count());
    }

    @Test
    void aReaderThatNeverDerefdReadsTheTakenVersionWhileAnotherWriterHoldsTheTake() throws Exception {
        RefFleet.Peer a = fleet.newPeer("a", 1, linear());
        RefFleet.Peer b = fleet.newPeer("b", 2, linear(), "a");
        fleet.settle(6);
        Ref<PMap<String, String>> onA = a.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onB = b.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        PMap<String, String> v1 = onA.swap(m -> m.assoc("first", "a"));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(v1.root()) && RefFleet.headEntries(b, NAME).size() == 1)).isTrue();
        Template<PRef> template = Template.of(PRef.class).whereTag(PRef.NAME_TAG, Matchers.eq(NAME));
        assertThat(a.refSpace().take(template, Lease.of(Duration.ofSeconds(30)), Duration.ofSeconds(1))).isPresent();
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> RefFleet.headEntries(b, NAME).isEmpty())).isTrue();
        assertThat(onB.heads().isEmpty()).as("a take hides the head").isTrue();
        PMap<String, String> seen = onB.deref();
        assertThat(seen.root()).as("the taken version is still the ref's value").isEqualTo(v1.root());
        assertThat(seen.count()).isEqualTo(1);
        System.out.printf("PD-R-04: a reader that never deref'd read the taken version (%d entries) while the take stood%n", seen.count());
    }

    @Test
    void aWriterKilledBetweenTakeAndCompleteReleasesTheTakeAtItsLease() throws Exception {
        RefFleet.Peer a = fleet.newPeer("a", 1, linear());
        RefFleet.Peer b = fleet.newPeer("b", 2, linear(), "a");
        fleet.settle(6);
        Ref<PMap<String, String>> onA = a.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        Ref<PMap<String, String>> onB = b.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
        PMap<String, String> v1 = onA.swap(m -> m.assoc("first", "a"));
        assertThat(fleet.runUntil(Duration.ofSeconds(10), () -> fleet.allHold(v1.root()) && !onB.heads().isEmpty())).isTrue();

        // a takes the head as a swap would, then dies before completing it
        Template<PRef> template = Template.of(PRef.class).whereTag(PRef.NAME_TAG, Matchers.eq(NAME));
        Optional<TakenEntry<PRef>> taken = a.refSpace().take(template, Lease.of(Duration.ofSeconds(3)), Duration.ofSeconds(1));
        assertThat(taken).isPresent();
        fleet.settle(2);
        assertThat(RefFleet.headEntries(b, NAME)).as("the take is visible on b").isEmpty();
        fleet.kill(a);

        List<Throwable> failures = new CopyOnWriteArrayList<>();
        Thread writer = Thread.ofVirtual().start(() -> {
            try {
                onB.swap(m -> m.assoc("second", "b"));
            } catch (Throwable t) {
                failures.add(t);
            }
        });
        // b's take waits while the claim stands; the lease lapses on the test clock and the entry reappears
        long started = System.nanoTime();
        while (writer.isAlive()) {
            fleet.tickAll();
            Thread.sleep(1);
            assertThat(System.nanoTime() - started).as("b's swap finished").isLessThan(Duration.ofSeconds(30).toNanos());
        }
        assertThat(failures).isEmpty();
        PMap<String, String> v2 = onB.deref();
        assertThat(v2.get("first")).contains("a");
        assertThat(v2.get("second")).contains("b");
        assertThat(onB.heads().maxSeq()).isEqualTo(2);
        assertThat(RefFleet.headEntries(b, NAME)).hasSize(1);
        System.out.printf("PD-R-04: the take of the dead writer lapsed on the clock and b's swap completed at seq %d%n", onB.heads().maxSeq());
    }
}
