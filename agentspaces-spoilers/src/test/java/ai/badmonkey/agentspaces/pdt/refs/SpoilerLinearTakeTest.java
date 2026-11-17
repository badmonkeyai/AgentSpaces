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
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.spoilers.Action;
import ai.badmonkey.agentspaces.spoilers.Ceiling;
import ai.badmonkey.agentspaces.spoilers.ItemKind;
import ai.badmonkey.agentspaces.spoilers.Script;
import ai.badmonkey.agentspaces.spoilers.ScriptRunner;
import ai.badmonkey.agentspaces.spoilers.Trigger;
import ai.badmonkey.agentspaces.test.FleetInvariants;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The write path of SC-I4's hypothesis 3 (srad/spoiler-peers.md §3.8): a linear swap whose take lands while a version
 * between a loaded ancestor and the taken head is still loading. {@code Refs.heads} folds such an ancestor away for a
 * read; the swap picks its merge heads on its own path ({@code swapLinear}), and a merge it builds is written, so an
 * ancestor read there as a head would put a removed key back for the whole fleet, not for one read.
 *
 * <p>The fork, every entry written by {@code w1} beside the versions it lists (no take, so they stay live, as a
 * re-offering spoiler or a stale replica leaves them): Z; A over Z adds {@code ka}; X over A, too large to travel
 * inline; T over X, every node inline, drops {@code ka}; F over Z adds {@code kf}. The readers' heads are T and F and
 * {@code ka} is gone. A late joiner's fetch of X's root node is held until its swap returns, and the joiner swaps.
 *
 * <p>The ref space orders the take by a bid, highest seq first, so the take claims T. The bid only picks which live
 * entry the take claims first; any order is a legal outcome of a take, and the space's default order is the hash of
 * random entry ids, which a scenario cannot fix.
 */
class SpoilerLinearTakeTest {

    private static final String NAME = "ledger";
    private static final String SPOILER = "w1";
    private static final String JOINER = "l";

    private static RefFleet.Spec linear() {
        return RefFleet.Spec.defaults()
                .pdt(b -> b.mode(NAME, PRef.MODE_LINEAR).take(Duration.ofSeconds(120), Duration.ofMillis(300), 100)
                        .loadTimeout(Duration.ofSeconds(5)))
                .refSpace(b -> b.strategy(ConflictStrategyType.AUCTION)
                        .bidFunction(v -> v instanceof PRef ref ? -ref.seq() : 0.0));
    }

    @Test
    void aTakeWithALateMiddleVersionWritesNoAncestorBackAsAHead() throws Exception {
        long seed = 9500;
        RefFleet fleet = new RefFleet();
        Thread.UncaughtExceptionHandler previous = Thread.getDefaultUncaughtExceptionHandler();
        try {
            RefFleet.Peer w1 = fleet.newPeer(SPOILER, seed, linear());
            RefFleet.Peer r0 = fleet.newPeer("r0", seed + 2, linear(), SPOILER);
            fleet.settle(12);
            Ref<PMap<String, String>> on1 = w1.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            Ref<PMap<String, String>> onReader = r0.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);

            // Z, A and X by linear swaps (each takes the one before), then Z and A offered again beside them
            PMap<String, String> z = on1.swap(m -> m.assoc("base", "z"));
            long zSeq = seqOf(w1, z.root());
            PMap<String, String> a = on1.swap(m -> m.assoc("ka", "a"));
            long aSeq = seqOf(w1, a.root());
            Random random = new Random(seed);
            PMap<String, String> x = on1.swap(m -> {
                PMap<String, String> out = m;
                for (int i = 0; i * 120 < 3 * PRef.INLINE_DELTA_BYTES; i++) {
                    out = out.assoc("big-" + i, RefFleet.value(100, random));
                }
                return out;
            });
            long xSeq = seqOf(w1, x.root());
            assertThat(RefFleet.headEntries(w1, NAME).stream().filter(e -> e.value().root().equals(x.root()))
                    .allMatch(e -> e.value().nodes() == null)).as("X is too large to travel inline").isTrue();
            SpoilerConcurrentHeadsTest.writeWholeHead(w1, z, List.of(), zSeq, PRef.MODE_LINEAR);
            SpoilerConcurrentHeadsTest.writeWholeHead(w1, a, List.of(z.root()), aSeq, PRef.MODE_LINEAR);
            PMap<String, String> tValue = PMap.empty(w1.store(), RefFleet.STR, RefFleet.STR).assoc("base", "z").assoc("kt", "t");
            String t = SpoilerConcurrentHeadsTest.writeWholeHead(w1, tValue, List.of(x.root()), xSeq + 1, PRef.MODE_LINEAR);
            PMap<String, String> fValue = PMap.empty(w1.store(), RefFleet.STR, RefFleet.STR).assoc("base", "z").assoc("kf", "f");
            String f = SpoilerConcurrentHeadsTest.writeWholeHead(w1, fValue, List.of(z.root()), zSeq + 1, PRef.MODE_LINEAR);

            Duration bound = Duration.ofMillis(Refs.CANCEL_GRACE_MILLIS + Refs.UNLOADABLE_BACKOFF * RefFleet.DEADLINE.toMillis());
            assertThat(fleet.runFor(bound, () -> RefFleet.headEntries(r0, NAME).size() == 5
                    && new TreeSet<>(onReader.heads().roots()).equals(new TreeSet<>(List.of(t, f)))))
                    .as("the reader holds the five live entries and the heads T and F").isTrue();
            PMap<String, String> readersValue = onReader.deref();
            assertThat(readersValue.toMap()).as("the readers' value").containsEntry("kt", "t").containsEntry("kf", "f")
                    .doesNotContainKey("ka");

            // the late joiner, whose fetch of X's root node waits until its swap has returned
            AtomicLong swapped = new AtomicLong();
            RefFleetProbe probe = new RefFleetProbe(fleet, NAME, Set.of(SPOILER));
            Thread.setDefaultUncaughtExceptionHandler((th, e) -> probe.errors.add(e));
            FleetInvariants oracle = FleetInvariants.baseline(probe);
            ScriptRunner runner = ScriptRunner.attach(fleet.network, fleet.clock, Duration.ofMillis(RefFleet.PERIOD_MILLIS),
                    PeerNode.ChannelAuth.SESSION, Script.of(SPOILER).seed(seed)
                            .rule("hold-late-version")
                            .when(Trigger.frame(Envelope.Kind.PIPE_DATA).to(JOINER).item(ItemKind.NODE).id(x.root()))
                            .act(Action.hold()).always()
                            .until(Trigger.counter("the joiner's swap returned", swapped::get, 1)).ceiling(Ceiling.periods(800))
                            .build());
            fleet.afterTick.add(runner::tick);
            RefFleet.Peer joiner = fleet.newPeer(JOINER, seed + 201, linear(), SPOILER);
            Ref<PMap<String, String>> onJoiner = joiner.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            boolean comparable = fleet.runFor(bound, () -> RefFleet.headEntries(joiner, NAME).size() == 5
                    && List.of(z.root(), a.root(), t, f).stream().allMatch(root -> joiner.store().complete(root))
                    && runner.report().fired("hold-late-version") > 0);
            assertThat(comparable).as("the joiner holds the five live entries, Z, A, T and F whole, and X's answer is held")
                    .isTrue();
            assertThat(joiner.store().complete(x.root())).as("X is still loading at the joiner").isFalse();

            PMap<String, String> written = onJoiner.swap(m -> m.assoc("kl", "l"));
            swapped.incrementAndGet();
            Space.Entry<PRef> writtenEntry = RefFleet.headEntries(joiner, NAME).stream()
                    .filter(e -> e.value().root().equals(written.root())).findFirst().orElseThrow();
            List<String> writtenBase = writtenEntry.value().base();
            System.out.printf("SC-I4 LINEAR_TAKE (seed %d): the joiner took %s; its swap wrote a version over %s with keys %s%n", seed,
                    writtenBase.contains(t) ? "T" : "another head", writtenBase.stream().map(r -> r.equals(t) ? "T" : r.equals(f) ? "F"
                            : r.equals(a.root()) ? "A" : r.equals(z.root()) ? "Z" : r.equals(x.root()) ? "X" : r).toList(),
                    new TreeSet<>(written.toMap().keySet()));
            assertThat(writtenBase).as("the swap took T and merged F").contains(t, f);
            assertThat(writtenBase).as("A, which T supersedes through X, is no merge head of the swap").doesNotContain(a.root());
            assertThat(written.toMap()).as("the written version brings back no key a later version removed")
                    .doesNotContainKey("ka").containsEntry("kt", "t").containsEntry("kf", "f").containsEntry("kl", "l");

            // the fleet reads the joiner's value, with ka gone everywhere (the re-offered entries below T stay live
            // beside it, so the reader may still merge them in; they add nothing T did not supersede)
            assertThat(fleet.runFor(bound, () -> onReader.deref().root().equals(written.root())
                    && onJoiner.deref().root().equals(written.root())))
                    .as("the reader and the joiner deref the joiner's version").isTrue();
            assertThat(onReader.deref().toMap()).as("the reader's value after the swap").doesNotContainKey("ka");
            ScriptRunner.Report report = runner.finish();
            System.out.printf("SC-I4 %s firing sequence: %s%n", "LINEAR_TAKE", report.signatures());
            assertThat(report.fired("hold-late-version")).as("the answer with X's root node was held").isPositive();
            oracle.assertHolds(report.expectedDrops(), true);
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(previous);
            fleet.close();
        }
    }

    private static long seqOf(RefFleet.Peer peer, String root) {
        return RefFleet.headEntries(peer, NAME).stream().filter(e -> e.value().root().equals(root))
                .mapToLong(e -> e.value().seq()).max().orElseThrow();
    }
}
