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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SC-I4 of srad/spoiler-peers.md, the acceptance of phase 1 (SP-S-01, §2.6 scenario 1, §7.3): the fleet of
 * {@code ConcurrentHeadsTest} (five writers, eight readers, one ref, {@code ChannelAuth.SESSION}) with its writers
 * stepped on a barrier (SP-T-01), then a late joiner, and one writer, {@code w2}, a spoiler.
 *
 * <p>On the barrier alone the fork at the end is shallow (every round's tick spreads the round's heads, so the next
 * round merges them), where the busy-waiting writers of the original test left heads up to sixteen versions apart.
 * The scenario therefore shapes the fork with a script first: for the last {@link #ISOLATED} rounds (six) the spoiler's
 * frames on the ref stream are held both ways, so its line runs that many versions without the others and theirs
 * without it, and every version on both lines stays live (PD-R-06). Then each hypothesis for the late joiner's
 * conflict count (33 against the readers' 29 in R2's {@code T-F3}) runs once, with no load:
 * <ul>
 *   <li>the negative control: no rule after the fork (SP-C-02);</li>
 *   <li>hypothesis 2: every frame carrying the state of the common base's entry is held on every link into the joiner,
 *       until the joiner has read the ref once.</li>
 * </ul>
 * Hypothesis 1 (every content answer that carries a {@code vg} chain node held on every link into the joiner) ran on
 * the barrier fleet too, and there the joiner needed chain nodes in some runs and none in others (the live entries
 * reached the base), so its rule fired or not by the run. It runs on a fork built so the joiner needs them
 * ({@link #chainFork}), with its own negative control. Hypothesis 3, which the barrier runs and {@code Refs.heads}
 * pointed to, runs in its smallest fork ({@link #hypothesisThreeALateVersionBetweenTwoLoadedOnesMakesNoAncestorAHead}):
 * one version between a loaded ancestor and a loaded head loads late at the joiner. Its write path, a linear swap, is
 * {@code SpoilerLinearTakeTest}.
 *
 * <p>What the scenarios settle and what they do not. Hypothesis 3 was a product defect in {@code Refs} (a read and a
 * linear swap both), fixed, each with a red-first test. Hypothesis 1 reproduces the second mechanism R2 recorded (a
 * joiner whose base walk lacks chain nodes takes the latest head whole, {@code NO_COMMON_BASE}), which is the designed
 * fallback of a walk that timed out, and repairs. Hypothesis 2 cannot be posed at the link level. No trace ties either
 * mechanism to the counts of R2's run (33 against 29, 9 heads against 6), and why a chain or base fetch failed there with
 * nothing held is still not traced.
 *
 * <p>The barrier fleet is not deterministic from its seed: entry ids are random UUIDs, which order the space's take and
 * scan, and the writers of one round swap on their own threads, so the fork, the readers' conflict counts and hypothesis
 * 2's firing sequence differ between runs of one seed. The barrier tests therefore assert relations inside their own
 * run, never a count. Of the constructed forks, hypothesis 3 and the linear take replayed with equal firing sequences
 * (SP-Q-REL-04); hypothesis 1 replayed with the same rule, count and verdict, but the held answer came from {@code w1}
 * in one run and {@code w2} in the other, since the content swarm picks the holder it asks at random.
 *
 * <p>Each run records what the joiner's first read saw against the readers (its heads, its conflicts, their reasons, its
 * root), then asserts the repair: once the holds end, the joiner reads the readers' root with the readers' conflicts
 * within the bound the original test derives (cancel grace plus repair window plus the longest retry, on the fleet
 * clock), and the minimal fleet oracle holds with no member struck (class A).
 *
 * <p>The joiner's load timeout is five seconds rather than the fleet's thirty: a deref waits on the wall clock for a
 * held answer (srad/spoiler-peers.md §3.4.4), so a hold costs one load timeout of wall time per read, and the repair
 * bound below is computed from the joiner's own timeout.
 */
class SpoilerConcurrentHeadsTest {

    private static final String NAME = "ledger";
    private static final String JOINER = "l";
    private static final int WRITERS = 5;
    private static final int READERS = 8;
    private static final int SWAPS = Integer.getInteger("spoiler.swaps", 100);
    private static final int KEYS = 40;
    private static final Duration JOINER_LOAD = Duration.ofSeconds(5);
    private static final String SPOILER = "w2";
    private static final String REF_STREAM = "space:" + RefFleet.REF_SPACE;
    /**
     * The rounds the spoiler's line runs apart from the others' at the end of the writes. It must release the spoiler's
     * line before the version it forked from can be cancelled, or the lines share no live base (PD-R-06 keeps a fork's
     * versions live only once a writer holds both lines; a base cancelled before the lagging line reached its writer is
     * the documented exception that {@code Refs} fetches by root). That version is superseded, at the earliest, by the
     * first isolated round, so its writer cancels it {@link Refs#CANCEL_GRACE_MILLIS} of fleet clock (8 ticks) after
     * that round's tick; the release lands in the tick after the last isolated round. Eight rounds released in the same
     * tick that cancelled the base whenever one writer saw another's version inside the first isolated round (the
     * phase-2 gate of srad/agentspaces-event-loop.md, §10.15), so six leave two ticks to spare. {@code spoiler.isolated}
     * overrides it for an exploratory run.
     */
    private static final int ISOLATED = Integer.getInteger("spoiler.isolated",
            (int) (Refs.CANCEL_GRACE_MILLIS / RefFleet.PERIOD_MILLIS) - 2);

    enum Hypothesis { CONTROL, BASE_ENTRY }

    /**
     * What one run saw.
     *
     * @param perReader       the conflicts each reader counted on its final read
     * @param settledHeads    the heads the readers settled on
     * @param firstHeads      the heads of the joiner's first read
     * @param firstConflicts  the conflicts of the joiner's first read
     * @param firstReasons    their reasons, counted
     * @param firstRootAgrees whether the first read's root was the readers'
     * @param chainMissed     chain nodes the joiner left out by its first read
     * @param repairMillis    fleet time from the release to the joiner's steady agreement
     * @param finalConflicts  the conflicts of the joiner's final read
     */
    record Outcome(Hypothesis hypothesis, String spoiler, long perReader, int settledHeads, boolean comparable, int firstHeads,
                   long firstConflicts, Map<String, Integer> firstReasons, boolean firstRootAgrees, long chainMissed,
                   long repairMillis, long finalConflicts, ScriptRunner.Report report, long wallMillis) {

        /** The joiner held what the readers hold when it first read, and still read otherwise. */
        boolean reproduced() {
            return comparable && (firstHeads != settledHeads || firstConflicts != perReader || !firstRootAgrees);
        }
    }

    @Test
    void negativeControlTheHonestSpoilerFiresNothingAndTheJoinerReadsTheReaders() throws Exception {
        Outcome o = run(Hypothesis.CONTROL, 9100);
        assertThat(o.report().firings()).as("an honest script fires nothing").isEmpty();
        assertThat(o.comparable()).as("the joiner held every live entry and the settled heads before its first read").isTrue();
        assertThat(o.reproduced()).as("with nothing held the joiner's first read is the readers'").isFalse();
    }

    /**
     * The negative control of hypothesis 1's fork ({@link #chainFork}): nothing held, the joiner's first read is the
     * readers' root, with their conflicts, and it walks the chain nodes it needs within its load timeout.
     */
    @Test
    void hypothesisOneControlTheChainForkWithNothingHeldReadsTheReaders() throws Exception {
        ChainOutcome o = chainFork(false, 9210);
        assertThat(o.report().firings()).as("an honest script fires nothing").isEmpty();
        assertThat(o.chainMissed()).as("the joiner's walk found every chain node it needed").isZero();
        assertThat(o.firstReasons()).as("the joiner's first read merges over the readers' base").isEqualTo(o.readerReasons());
        assertThat(o.firstRoot()).as("the joiner's first read derefs the readers' root").isEqualTo(o.readersRoot());
    }

    /**
     * Hypothesis 1 in the fork where the joiner needs chain nodes ({@link #chainFork}): every content answer that
     * carries a {@code vg} node toward the joiner is held until it has read once. On the barrier fleet the joiner needed
     * no chain node in some runs (the live entries reached the base), so the rule there fired in some runs and not in
     * others; this fork needs them by construction.
     *
     * <p>What it pins is the designed fallback, and it reproduces the second mechanism R2 saw in {@code T-F3}: the base
     * walk waits one load timeout for the chain nodes, then the read takes the latest head whole and counts
     * {@code NO_COMMON_BASE} conflicts, where the readers merged over the base; once the nodes arrive the joiner reads
     * the readers' root.
     */
    @Test
    void hypothesisOneChainNodesHeldTowardTheJoinerMakeItsFirstReadTakeTheLatestHeadWhole() throws Exception {
        ChainOutcome o = chainFork(true, 9220);
        assertThat(o.report().fired("hold-chain-nodes")).as("vg answers to the joiner were held").isPositive();
        assertThat(o.chainMissed()).as("the joiner's walk left chain nodes out").isPositive();
        assertThat(o.firstReasons()).as("the joiner's first read found no common base").containsOnly("NO_COMMON_BASE");
        assertThat(o.readerReasons()).as("the readers merged over their base").doesNotContain("NO_COMMON_BASE");
        assertThat(o.firstRoot()).as("so the first read is not the readers' root").isNotEqualTo(o.readersRoot());
    }

    /**
     * Hypothesis 2 cannot be posed at the link level, and the test pins that rather than passing it as a verdict: every
     * pull answer to a fresh joiner carries the whole ref space, so holding the frames that carry the base entry
     * withholds every entry, and the joiner reads no head at all. Withholding one entry needs the shadow replica of
     * srad/spoiler-peers.md (after 0.7.0).
     */
    @Test
    void hypothesisTwoTheBaseEntryCannotBeHeldAloneAtTheLinkLevel() throws Exception {
        Outcome o = run(Hypothesis.BASE_ENTRY, 9300);
        assertThat(o.report().fired("hold-base-entry")).as("the base entry was held").isPositive();
        assertThat(o.comparable()).as("NOT COMPARABLE: holding the base entry held the whole ref space").isFalse();
        assertThat(o.firstHeads()).as("the joiner's first read saw no head").isZero();
    }

    /**
     * Hypothesis 3, which the runs above and {@code Refs.heads} pointed to, in its smallest fork (the barrier fleet
     * cannot isolate one version's fetch at the link level: a joiner asks for subtree CIDs, and every small swap travels
     * as an inline delta). Writer {@code w1} writes Z, then, while the spoiler {@code w2} is held apart on the ref stream,
     * A (adds {@code ka}), X (too large to travel inline) and a head H over X that drops {@code ka} and carries all its
     * nodes inline; {@code w2} writes F over Z. The readers' heads are H and F, and {@code ka} is gone. The late joiner's
     * fetch of X is held until it has read once: Z, A, H and F are loadable, X is not.
     *
     * <p>The product rule pinned (annex H heads, PD-R-06): a version that a loaded head supersedes through live entries
     * is no head, whether or not the versions between them have loaded. Read as a head, A brings {@code ka} back.
     */
    @Test
    void hypothesisThreeALateVersionBetweenTwoLoadedOnesMakesNoAncestorAHead() throws Exception {
        long seed = 9400;
        RefFleet fleet = new RefFleet();
        Thread.UncaughtExceptionHandler previous = Thread.getDefaultUncaughtExceptionHandler();
        try {
            RefFleet.Peer w1 = fleet.newPeer("w1", seed);
            RefFleet.Peer w2 = fleet.newPeer(SPOILER, seed + 1, "w1");
            List<RefFleet.Peer> readers = List.of(fleet.newPeer("r0", seed + 2, "w1"), fleet.newPeer("r1", seed + 3, "w1"));
            fleet.settle(12);
            Ref<PMap<String, String>> on1 = w1.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            Ref<PMap<String, String>> on2 = w2.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            List<Ref<PMap<String, String>>> onReaders = readers.stream()
                    .map(r -> r.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR)).toList();
            String z = on1.swap(m -> m.assoc("base", "z")).root();
            assertThat(fleet.runFor(Duration.ofSeconds(10), () -> on2.heads().roots().equals(List.of(z))
                    && w2.store().complete(z))).as("w2 holds Z").isTrue();

            // the fork: the spoiler's frames on the ref stream are held both ways while the two lines are written
            AtomicLong forked = new AtomicLong();
            Trigger forkEnds = Trigger.counter("the fork is written", forked::get, 1);
            ScriptRunner fork = ScriptRunner.attach(fleet.network, fleet.clock, Duration.ofMillis(RefFleet.PERIOD_MILLIS),
                    PeerNode.ChannelAuth.SESSION, Script.of(SPOILER).seed(seed)
                            .rule("isolate-out").when(Trigger.frame().from(SPOILER).stream(REF_STREAM))
                            .act(Action.hold()).until(forkEnds).ceiling(Ceiling.periods(8)).always()
                            .rule("isolate-in").when(Trigger.frame().to(SPOILER).stream(REF_STREAM))
                            .act(Action.hold()).until(forkEnds).ceiling(Ceiling.periods(8)).always()
                            .build());
            fleet.afterTick.add(fork::tick);
            on1.swap(m -> m.assoc("ka", "a"));
            Random random = new Random(seed);
            PMap<String, String> x = on1.swap(m -> {
                PMap<String, String> out = m;
                for (int i = 0; i * 120 < 3 * PRef.INLINE_DELTA_BYTES; i++) {
                    out = out.assoc("big-" + i, RefFleet.value(100, random));
                }
                return out;
            });
            String f = on2.swap(m -> m.assoc("kf", "f")).root();
            long xSeq = RefFleet.headEntries(w1, NAME).stream().filter(e -> e.value().root().equals(x.root()))
                    .mapToLong(e -> e.value().seq()).max().orElseThrow();
            assertThat(RefFleet.headEntries(w1, NAME).stream().filter(e -> e.value().root().equals(x.root()))
                    .allMatch(e -> e.value().nodes() == null)).as("X is too large to travel inline").isTrue();
            PMap<String, String> hValue = PMap.empty(w1.store(), RefFleet.STR, RefFleet.STR).assoc("base", "z").assoc("kh", "h");
            String h = writeWholeHead(w1, hValue, x.root(), xSeq + 1);
            forked.set(1);
            fleet.tickAll();
            fork.finish();
            fleet.afterTick.clear();

            long quiet = Refs.CANCEL_GRACE_MILLIS
                    + fleet.peers.stream().mapToLong(p -> p.runtime().gossip().repairWindowMillis()).max().orElseThrow();
            Duration bound = Duration.ofMillis(quiet + Refs.UNLOADABLE_BACKOFF * RefFleet.DEADLINE.toMillis() * 3 / 2);
            assertThat(steady(fleet, bound, quiet, () -> {
                Set<String> roots = new HashSet<>();
                onReaders.forEach(ref -> roots.add(ref.deref().root()));
                return roots.size() == 1 && onReaders.get(0).heads().roots().size() == 2 ? roots.iterator().next() : null;
            })).as("the readers settle on one root over two heads").isTrue();
            List<String> settledRoots = readers.get(0).pdt().refs().heads(NAME).roots();
            assertThat(settledRoots).as("the readers' heads").containsExactlyInAnyOrder(h, f);
            List<PConflict> readerConflicts = new CopyOnWriteArrayList<>();
            PMap<String, String> readersValue;
            try (var counted = onReaders.get(0).onConflict(readerConflicts::add)) {
                readersValue = onReaders.get(0).deref();
            }
            assertThat(readersValue.toMap()).as("the readers' value").containsEntry("kh", "h").containsEntry("kf", "f")
                    .doesNotContainKey("ka");
            int liveEntries = RefFleet.headEntries(readers.get(0), NAME).size();

            // the late joiner, whose fetch of X waits until it has read the ref once
            AtomicLong observed = new AtomicLong();
            RefFleetProbe probe = new RefFleetProbe(fleet, NAME, Set.of(SPOILER));
            Thread.setDefaultUncaughtExceptionHandler((t, e) -> probe.errors.add(e));
            FleetInvariants oracle = FleetInvariants.baseline(probe);
            ScriptRunner runner = ScriptRunner.attach(fleet.network, fleet.clock, Duration.ofMillis(RefFleet.PERIOD_MILLIS),
                    PeerNode.ChannelAuth.SESSION, Script.of(SPOILER).seed(seed)
                            // the answer that carries X's root node, from whichever holder the joiner asked (it asks by a
                            // NODE_WANT batch, then for the subtrees by TREE_WANT); without the root node X cannot load
                            .rule("hold-late-version")
                            .when(Trigger.frame(Envelope.Kind.PIPE_DATA).to(JOINER).item(ItemKind.NODE).id(x.root()))
                            .act(Action.hold()).always()
                            .until(Trigger.counter("the joiner read the ref once", observed::get, 1)).ceiling(Ceiling.periods(800))
                            .build());
            fleet.afterTick.add(runner::tick);
            RefFleet.Peer joiner = fleet.newPeer(JOINER, seed + 201, RefFleet.Spec.defaults().pdt(b -> b.loadTimeout(JOINER_LOAD)), "w1");
            Ref<PMap<String, String>> onJoiner = joiner.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            // comparable once the joiner holds every live entry and the readers' heads, and X's answer is held
            boolean comparable = fleet.runFor(bound, () -> RefFleet.headEntries(joiner, NAME).size() == liveEntries
                    && settledRoots.stream().allMatch(root -> joiner.store().complete(root))
                    && runner.report().fired("hold-late-version") > 0);
            List<String> firstHeads = onJoiner.heads().roots();
            List<PConflict> firstConflicts = new CopyOnWriteArrayList<>();
            PMap<String, String> first;
            try (var counted = onJoiner.onConflict(firstConflicts::add)) {
                first = onJoiner.deref();
            }
            boolean xLate = !joiner.store().complete(x.root());
            observed.incrementAndGet();
            List<String> readerReasons = readerConflicts.stream().map(c -> reasonName(c.reason())).sorted().toList();
            List<String> firstReasons = firstConflicts.stream().map(c -> reasonName(c.reason())).sorted().toList();
            System.out.printf("SC-I4 LATE_VERSION (seed %d): readers' heads %s, conflicts %s; the joiner, holding every live entry and"
                            + " the heads (%s), X %s, first read heads %s, conflicts %s, keys %s, root %s the readers'%n", seed,
                    short6(settledRoots), readerReasons, comparable, xLate ? "still loading" : "loaded", short6(firstHeads), firstReasons,
                    new java.util.TreeSet<>(first.toMap().keySet()), first.root().equals(readersValue.root()) ? "equal to" : "DIFFERENT from");

            boolean repaired = steady(fleet, bound, quiet, () -> {
                String root = onJoiner.deref().root();
                return root.equals(readersValue.root()) && onJoiner.heads().roots().equals(settledRoots) ? root : null;
            });
            ScriptRunner.Report report = runner.finish();
            System.out.printf("SC-I4 %s firing sequence: %s%n", "LATE_VERSION", report.signatures());
            assertThat(report.fired("hold-late-version")).as("the answer with X's root node was held").isPositive();
            assertThat(comparable).as("the joiner held every live entry and the readers' heads").isTrue();
            assertThat(xLate).as("X was still loading at the first read").isTrue();
            assertThat(firstHeads).as("the joiner's first read: A is superseded through X, which is late, by H, which is loaded")
                    .containsExactlyInAnyOrderElementsOf(settledRoots);
            assertThat(first.toMap()).as("the joiner's first read brings back no key a later version removed")
                    .doesNotContainKey("ka");
            assertThat(firstReasons).as("the joiner's first read merges against the readers' base, with their conflicts")
                    .doesNotContain("NO_COMMON_BASE").isEqualTo(readerReasons);
            assertThat(first.root()).as("the joiner's first read derefs the readers' root").isEqualTo(readersValue.root());
            assertThat(repaired).as("the joiner keeps the readers' root once X loads").isTrue();
            oracle.assertHolds(report.expectedDrops(), true);
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(previous);
            fleet.close();
        }
    }

    /**
     * What one run of the chain fork saw.
     *
     * @param readersRoot   the root the readers derefed
     * @param readerReasons the reasons of the readers' conflicts, sorted
     * @param firstRoot     the root of the joiner's first read
     * @param firstReasons  the reasons of its conflicts, sorted
     * @param chainMissed   chain nodes its first read left out
     */
    record ChainOutcome(String readersRoot, List<String> readerReasons, String firstRoot, List<String> firstReasons,
                        long chainMissed, ScriptRunner.Report report) {
    }

    /**
     * Hypothesis 1's fork, where a late joiner needs chain nodes to find the base. {@code w1} writes A1 (with
     * {@code kr}), and every member holds it; then, while the spoiler {@code w2} is held apart on the ref stream,
     * {@code w2} writes B over A1 (adds {@code kb}) and {@code w1} writes A2 (drops {@code kr}) to A5. {@code w1} sees
     * no fork, so it cancels A1 to A4 after the cancel grace, and only A5 and B stay live. The readers merge them over A1
     * from the version memory of the run; a late joiner knows no entry below A5 and walks A4 to A2 by their chain nodes.
     */
    private ChainOutcome chainFork(boolean holdChainNodes, long seed) throws Exception {
        RefFleet fleet = new RefFleet();
        Thread.UncaughtExceptionHandler previous = Thread.getDefaultUncaughtExceptionHandler();
        try {
            RefFleet.Peer w1 = fleet.newPeer("w1", seed);
            RefFleet.Peer w2 = fleet.newPeer(SPOILER, seed + 1, "w1");
            List<RefFleet.Peer> readers = List.of(fleet.newPeer("r0", seed + 2, "w1"), fleet.newPeer("r1", seed + 3, "w1"));
            fleet.settle(12);
            Ref<PMap<String, String>> on1 = w1.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            Ref<PMap<String, String>> on2 = w2.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            List<Ref<PMap<String, String>>> onReaders = readers.stream()
                    .map(r -> r.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR)).toList();
            String a1 = on1.swap(m -> m.assoc("base", "1").assoc("kr", "r")).root();
            long quiet = Refs.CANCEL_GRACE_MILLIS
                    + fleet.peers.stream().mapToLong(p -> p.runtime().gossip().repairWindowMillis()).max().orElseThrow();
            Duration bound = Duration.ofMillis(quiet + Refs.UNLOADABLE_BACKOFF * RefFleet.DEADLINE.toMillis() * 3 / 2);
            assertThat(fleet.runFor(bound, () -> on2.heads().roots().equals(List.of(a1)) && w2.store().complete(a1)
                    && onReaders.stream().allMatch(r -> r.heads().roots().equals(List.of(a1)))))
                    .as("every member holds A1").isTrue();

            AtomicLong forked = new AtomicLong();
            Trigger forkEnds = Trigger.counter("the fork is written", forked::get, 1);
            ScriptRunner fork = ScriptRunner.attach(fleet.network, fleet.clock, Duration.ofMillis(RefFleet.PERIOD_MILLIS),
                    PeerNode.ChannelAuth.SESSION, Script.of(SPOILER).seed(seed)
                            .rule("isolate-out").when(Trigger.frame().from(SPOILER).stream(REF_STREAM))
                            .act(Action.hold()).until(forkEnds).ceiling(Ceiling.periods(800)).always()
                            .rule("isolate-in").when(Trigger.frame().to(SPOILER).stream(REF_STREAM))
                            .act(Action.hold()).until(forkEnds).ceiling(Ceiling.periods(800)).always()
                            .build());
            fleet.afterTick.add(fork::tick);
            String b = on2.swap(m -> m.assoc("kb", "b")).root();
            on1.swap(m -> m.dissoc("kr"));
            fleet.tickAll();
            for (int i = 3; i <= 5; i++) {
                int step = i;
                on1.swap(m -> m.assoc("k" + step, "a" + step));
                fleet.tickAll();
            }
            String a5 = on1.heads().roots().get(0);
            assertThat(fleet.runFor(bound, () -> RefFleet.headEntries(w1, NAME).size() == 1
                    && readers.stream().allMatch(r -> RefFleet.headEntries(r, NAME).size() == 1)))
                    .as("w1 cancelled A1 to A4, and the readers hold A5 alone").isTrue();
            forked.set(1);
            fleet.tickAll();
            fork.finish();
            fleet.afterTick.clear();

            List<String> settledRoots = List.of(a5, b);
            assertThat(steady(fleet, bound, quiet, () -> {
                Set<String> roots = new HashSet<>();
                onReaders.forEach(ref -> roots.add(ref.deref().root()));
                return roots.size() == 1 && new HashSet<>(onReaders.get(0).heads().roots()).equals(new HashSet<>(settledRoots))
                        ? roots.iterator().next() : null;
            })).as("the readers settle on one root over A5 and B").isTrue();
            List<PConflict> readerConflicts = new CopyOnWriteArrayList<>();
            PMap<String, String> readersValue;
            try (var counted = onReaders.get(0).onConflict(readerConflicts::add)) {
                readersValue = onReaders.get(0).deref();
            }
            assertThat(readersValue.toMap()).as("the readers' value, merged over A1").containsEntry("kb", "b")
                    .containsEntry("k5", "a5").doesNotContainKey("kr");

            AtomicLong observed = new AtomicLong();
            RefFleetProbe probe = new RefFleetProbe(fleet, NAME, Set.of(SPOILER));
            Thread.setDefaultUncaughtExceptionHandler((t, e) -> probe.errors.add(e));
            FleetInvariants oracle = FleetInvariants.baseline(probe);
            Script script = holdChainNodes ? Script.of(SPOILER).seed(seed)
                    .rule("hold-chain-nodes")
                    .when(Trigger.frame(Envelope.Kind.PIPE_DATA).to(JOINER).node("vg"))
                    .act(Action.hold()).until(Trigger.counter("the joiner read the ref once", observed::get, 1))
                    .ceiling(Ceiling.periods(800)).always()
                    .build() : Script.honest(SPOILER);
            ScriptRunner runner = ScriptRunner.attach(fleet.network, fleet.clock, Duration.ofMillis(RefFleet.PERIOD_MILLIS),
                    PeerNode.ChannelAuth.SESSION, script);
            fleet.afterTick.add(runner::tick);
            RefFleet.Peer joiner = fleet.newPeer(JOINER, seed + 201, RefFleet.Spec.defaults().pdt(p -> p.loadTimeout(JOINER_LOAD)), "w1");
            Ref<PMap<String, String>> onJoiner = joiner.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            assertThat(fleet.runFor(bound, () -> RefFleet.headEntries(joiner, NAME).size() == 2
                    && settledRoots.stream().allMatch(root -> joiner.store().complete(root))))
                    .as("the joiner holds the two live entries and both heads whole").isTrue();
            long missedBefore = joiner.pdt().refs().chainNodesMissed();
            List<PConflict> firstConflicts = new CopyOnWriteArrayList<>();
            PMap<String, String> first;
            try (var counted = onJoiner.onConflict(firstConflicts::add)) {
                first = onJoiner.deref();
            }
            long chainMissed = joiner.pdt().refs().chainNodesMissed() - missedBefore;
            observed.incrementAndGet();
            List<String> readerReasons = readerConflicts.stream().map(c -> reasonName(c.reason())).sorted().toList();
            List<String> firstReasons = firstConflicts.stream().map(c -> reasonName(c.reason())).sorted().toList();

            java.util.concurrent.atomic.AtomicBoolean withoutBase = new java.util.concurrent.atomic.AtomicBoolean();
            boolean repaired;
            try (var noBase = onJoiner.onConflict(c -> withoutBase.compareAndSet(false, PConflict.NO_COMMON_BASE.equals(c.reason())))) {
                repaired = steady(fleet, Duration.ofMillis(quiet + Refs.UNLOADABLE_BACKOFF * JOINER_LOAD.toMillis() * 3 / 2 + quiet),
                        quiet, () -> {
                            String root = onJoiner.deref().root();
                            return !withoutBase.getAndSet(false) && root.equals(readersValue.root()) ? root : null;
                        });
            }
            ScriptRunner.Report report = runner.finish();
            System.out.printf("SC-I4 CHAIN_FORK %s (seed %d): readers' heads %s, conflicts %s, keys %s; the joiner's first read left"
                            + " %d chain node(s) out, conflicts %s, keys %s, root %s the readers'; repaired %s%n",
                    holdChainNodes ? "HELD" : "CONTROL", seed, short6(settledRoots), readerReasons,
                    new java.util.TreeSet<>(readersValue.toMap().keySet()), chainMissed, firstReasons,
                    new java.util.TreeSet<>(first.toMap().keySet()), first.root().equals(readersValue.root()) ? "equal to" : "DIFFERENT from",
                    repaired);
            System.out.printf("SC-I4 CHAIN_FORK %s firing sequence: %s%n", holdChainNodes ? "HELD" : "CONTROL", report.signatures());
            assertThat(repaired).as("the joiner reads the readers' root steadily, with its base, once the holds end").isTrue();
            oracle.assertHolds(report.expectedDrops(), true);
            return new ChainOutcome(readersValue.root(), readerReasons, first.root(), firstReasons, chainMissed, report);
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(previous);
            fleet.close();
        }
    }

    /** Writes a head over {@code base} that carries every node of its value inline, so it loads with nothing else. */
    private static String writeWholeHead(RefFleet.Peer peer, PMap<String, String> value, String base, long seq) {
        return writeWholeHead(peer, value, List.of(base), seq, PRef.MODE_MULTI);
    }

    /**
     * As above over any base and in any mode: the entry a spoiler re-offers or writes beside the versions it lists, with
     * no take, so the versions below stay live.
     */
    static String writeWholeHead(RefFleet.Peer peer, PMap<String, String> value, List<String> base, long seq, String mode) {
        String root = value.root();
        List<byte[]> nodes = new ArrayList<>();
        for (String cid : peer.store().reachable(root)) {
            nodes.add(peer.store().get(cid).orElseThrow());
        }
        PRef head = new PRef(PRef.versionFor(PRef.KIND_MAP), NAME, PRef.KIND_MAP, root, base, seq, value.count(), nodes,
                mode, PRef.SEALING_NONE, null, null);
        assertThat(head.violation()).as("a valid head").isEmpty();
        peer.refSpace().write(head, ai.badmonkey.agentspaces.api.space.Lease.of(Duration.ofMinutes(10)), head.tags());
        return root;
    }

    private static List<String> short6(List<String> roots) {
        return roots.stream().map(r -> r.substring(7, 13)).toList();
    }

    private Outcome run(Hypothesis hypothesis, long seed) throws Exception {
        long wallStart = System.nanoTime();
        RefFleet fleet = new RefFleet();
        Thread.UncaughtExceptionHandler previous = Thread.getDefaultUncaughtExceptionHandler();
        try {
            List<RefFleet.Peer> writers = new ArrayList<>();
            List<RefFleet.Peer> readers = new ArrayList<>();
            writers.add(fleet.newPeer("w0", seed));
            for (int i = 1; i < WRITERS; i++) {
                writers.add(fleet.newPeer("w" + i, seed + i, "w0"));
            }
            for (int i = 0; i < READERS; i++) {
                readers.add(fleet.newPeer("r" + i, seed + 100 + i, "w0"));
            }
            fleet.settle(12);
            List<Ref<PMap<String, String>>> writerRefs = new ArrayList<>();
            for (RefFleet.Peer w : writers) {
                writerRefs.add(w.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR));
            }
            List<Ref<PMap<String, String>>> readerRefs = new ArrayList<>();
            List<List<PConflict>> seen = new ArrayList<>();
            for (RefFleet.Peer r : readers) {
                Ref<PMap<String, String>> ref = r.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
                List<PConflict> mine = new CopyOnWriteArrayList<>();
                ref.onConflict(mine::add);
                readerRefs.add(ref);
                seen.add(mine);
            }

            // SP-T-01: every writer swaps exactly once per round on its own thread, then the fleet ticks once
            List<RefFleet.WriterStep> steps = new ArrayList<>();
            for (int w = 0; w < WRITERS; w++) {
                int writer = w;
                Ref<PMap<String, String>> ref = writerRefs.get(w);
                Random random = new Random(seed * 31 + w);
                steps.add(round -> {
                    String key = "k" + random.nextInt(KEYS);
                    String value = "w" + writer + ":" + round + ":" + RefFleet.value(8, random);
                    ref.swap(m -> m.assoc(key, value));
                });
            }
            assertThat(fleet.stepOnBarrier(steps, SWAPS - ISOLATED)).as("the writers' swaps").isEmpty();
            // the fork: the spoiler's frames on the ref stream are held both ways for the last rounds, then released
            AtomicLong writesDone = new AtomicLong();
            Trigger writesEnd = Trigger.counter("the writes ended", writesDone::get, 1);
            ScriptRunner fork = ScriptRunner.attach(fleet.network, fleet.clock, Duration.ofMillis(RefFleet.PERIOD_MILLIS),
                    PeerNode.ChannelAuth.SESSION, Script.of(SPOILER).seed(seed)
                            .rule("isolate-out").when(Trigger.frame().from(SPOILER).stream(REF_STREAM))
                            .act(Action.hold()).until(writesEnd).ceiling(Ceiling.periods(ISOLATED + 8)).always()
                            .rule("isolate-in").when(Trigger.frame().to(SPOILER).stream(REF_STREAM))
                            .act(Action.hold()).until(writesEnd).ceiling(Ceiling.periods(ISOLATED + 8)).always()
                            .build());
            fleet.afterTick.add(fork::tick);
            assertThat(fleet.stepOnBarrier(steps, ISOLATED)).as("the writers' swaps, the spoiler apart").isEmpty();
            writesDone.set(1);
            fleet.tickAll();
            ScriptRunner.Report forked = fork.finish();
            fleet.afterTick.clear();

            // the fleet settles by the original test's rule: one root everywhere, kept for a cancel grace and a repair window
            long quiet = Refs.CANCEL_GRACE_MILLIS
                    + fleet.peers.stream().mapToLong(p -> p.runtime().gossip().repairWindowMillis()).max().orElseThrow();
            Duration bound = Duration.ofMillis(quiet + Refs.UNLOADABLE_BACKOFF * RefFleet.DEADLINE.toMillis() * 3 / 2);
            List<Ref<PMap<String, String>>> everyRef = new ArrayList<>(writerRefs);
            everyRef.addAll(readerRefs);
            assertThat(steady(fleet, bound, quiet, () -> {
                Set<String> roots = new HashSet<>();
                everyRef.forEach(ref -> roots.add(ref.deref().root()));
                return roots.size() == 1 ? roots.iterator().next() : null;
            })).as("every peer derefs one root and keeps it").isTrue();
            Heads settled = readers.get(0).pdt().refs().heads(NAME);
            List<String> settledRoots = settled.roots();
            Set<Long> counts = new HashSet<>();
            String reference = null;
            for (int i = 0; i < READERS; i++) {
                int before = seen.get(i).size();
                String root = readerRefs.get(i).deref().root();
                counts.add((long) (seen.get(i).size() - before));
                assertThat(reference == null || reference.equals(root)).as("one root on every reader").isTrue();
                reference = root;
            }
            assertThat(counts).as("every reader counted the same conflicts on its final read").hasSize(1);
            long perReader = counts.iterator().next();
            String referenceRoot = reference;

            String spoiler = SPOILER;
            AtomicLong observed = new AtomicLong();
            Trigger observedOnce = Trigger.counter("the joiner read the ref once", observed::get, 1);
            Set<String> lateRoots = lateVersions(readers.get(0), settledRoots);
            Script script = switch (hypothesis) {
                case CONTROL -> Script.honest(spoiler);
                case BASE_ENTRY -> Script.of(spoiler).seed(seed)
                        .rule("hold-base-entry")
                        .when(Trigger.frame(Envelope.Kind.RUMOR, Envelope.Kind.PULL_RESP, Envelope.Kind.BATCH).to(JOINER)
                                .item(ItemKind.STATE).id(baseEntry(readers.get(0), settled)))
                        .act(Action.hold()).until(observedOnce).ceiling(Ceiling.periods(800)).always()
                        .build();
            };
            RefFleetProbe probe = new RefFleetProbe(fleet, NAME, Set.of(spoiler));
            Thread.setDefaultUncaughtExceptionHandler((t, e) -> probe.errors.add(e));
            FleetInvariants oracle = FleetInvariants.baseline(probe);
            ScriptRunner runner = ScriptRunner.attach(fleet.network, fleet.clock, Duration.ofMillis(RefFleet.PERIOD_MILLIS),
                    PeerNode.ChannelAuth.SESSION, script);
            fleet.afterTick.add(runner::tick);

            // the late joiner: no version memory, the fleet's seed as its only contact
            RefFleet.Peer joiner = fleet.newPeer(JOINER, seed + 201, RefFleet.Spec.defaults().pdt(b -> b.loadTimeout(JOINER_LOAD)), "w0");
            Ref<PMap<String, String>> onJoiner = joiner.pdt().mapRef(NAME, RefFleet.STR, RefFleet.STR);
            List<PConflict> joinerConflicts = new CopyOnWriteArrayList<>();
            onJoiner.onConflict(joinerConflicts::add);
            // who serves the joiner's chain nodes, measured on the fabric (no rule: the negative control fires none)
            Map<String, Long> chainServers = new java.util.concurrent.ConcurrentHashMap<>();
            Map<String, Long> treeWants = new java.util.concurrent.ConcurrentHashMap<>();
            fleet.network.filter(null, JOINER, frame -> {
                frame.items().stream().filter(i -> "vg".equals(i.attribute("kind")))
                        .forEach(i -> chainServers.merge(frame.from(), 1L, Long::sum));
                return ai.badmonkey.agentspaces.test.LinkFilter.Verdict.pass();
            });
            fleet.network.filter(JOINER, null, frame -> {
                frame.items().stream().filter(i -> i.type().equals(ItemKind.TREE_WANT.name())).forEach(i -> {
                    String kind = settledRoots.contains(i.id()) ? "head" : lateRoots.contains(i.id()) ? "below" : "other";
                    treeWants.merge(kind + ">" + frame.to(), 1L, Long::sum);
                    if (kind.equals("other") && treeWants.getOrDefault("examples", 0L) < 3) {
                        treeWants.merge("examples", 1L, Long::sum);
                        System.out.printf("SC-I4 %s: the joiner fetched %s (haves %s), no live version%n", hypothesis, i.id(),
                                i.attribute("haves"));
                    }
                });
                return ai.badmonkey.agentspaces.test.LinkFilter.Verdict.pass();
            });
            int liveEntries = RefFleet.headEntries(readers.get(0), NAME).size();
            // the joiner is comparable with the readers once it holds every live entry and the settled heads complete
            boolean sawHeads = fleet.runFor(bound, () -> RefFleet.headEntries(joiner, NAME).size() == liveEntries
                    && settledRoots.stream().allMatch(root -> joiner.store().complete(root)));

            // the observation: the joiner's first read, while the script still holds what it holds
            long missedBefore = joiner.pdt().refs().chainNodesMissed();
            int firstHeads = onJoiner.heads().roots().size();
            joinerConflicts.clear();
            String firstRoot = onJoiner.deref().root();
            List<PConflict> first = List.copyOf(joinerConflicts);
            long chainMissed = joiner.pdt().refs().chainNodesMissed() - missedBefore;
            observed.incrementAndGet();
            long releasedAt = fleet.clock.millis();

            // the repair: the joiner holds the settled heads and derefs the readers' root with no merge short of its
            // base, steadily for the quiet window, within the original bound counted from the joiner's own timeout
            Duration repairBound = Duration.ofMillis(quiet + Refs.UNLOADABLE_BACKOFF * JOINER_LOAD.toMillis() * 3 / 2 + quiet);
            java.util.concurrent.atomic.AtomicBoolean withoutBase = new java.util.concurrent.atomic.AtomicBoolean();
            long[] steadyFrom = {-1};
            boolean repaired;
            try (var noBase = onJoiner.onConflict(c -> withoutBase.compareAndSet(false, PConflict.NO_COMMON_BASE.equals(c.reason())))) {
                repaired = fleet.runFor(repairBound, () -> {
                    boolean ok = onJoiner.heads().roots().equals(settledRoots)
                            && settledRoots.stream().allMatch(root -> joiner.store().complete(root))
                            && onJoiner.deref().root().equals(referenceRoot);
                    ok &= !withoutBase.getAndSet(false);
                    if (!ok) {
                        steadyFrom[0] = -1;
                        return false;
                    }
                    if (steadyFrom[0] < 0) {
                        steadyFrom[0] = fleet.clock.millis();
                    }
                    return fleet.clock.millis() - steadyFrom[0] >= quiet;
                });
            }
            joinerConflicts.clear();
            String finalRoot = onJoiner.deref().root();
            List<PConflict> last = List.copyOf(joinerConflicts);
            ScriptRunner.Report report = runner.finish();

            Map<String, Integer> reasons = new java.util.TreeMap<>();
            first.forEach(c -> reasons.merge(reasonName(c.reason()), 1, Integer::sum));
            Outcome o = new Outcome(hypothesis, spoiler, perReader, settledRoots.size(), sawHeads, firstHeads, first.size(), reasons,
                    referenceRoot.equals(firstRoot), chainMissed, repaired ? steadyFrom[0] - releasedAt : -1, last.size(), report,
                    (System.nanoTime() - wallStart) / 1_000_000);
            System.out.printf("SC-I4 %s (seed %d, spoiler %s, %d swaps on the barrier): readers settled on %d head(s), %d conflict(s)"
                            + " each; the joiner %s every live entry and the settled heads before its first read, which saw %d head(s), %d conflict(s) %s, root %s the"
                            + " readers', %d chain node(s) left out; %s; the holds released %d frame(s) %s; repaired after %d ms of fleet"
                            + " time, final read %d conflict(s), root %s; %d ms of wall%n",
                    hypothesis, seed, spoiler, SWAPS, o.settledHeads(), perReader, sawHeads ? "held" : "did NOT hold", firstHeads,
                    first.size(), reasons, o.firstRootAgrees() ? "equal to" : "DIFFERENT from", chainMissed,
                    !sawHeads ? "NOT COMPARABLE" : o.reproduced() ? "REPRODUCED a joiner/reader mismatch" : "no mismatch", report.releases().stream()
                            .mapToInt(ScriptRunner.Release::frames).sum(), report.releases(), o.repairMillis(), last.size(),
                    referenceRoot.equals(finalRoot) ? "equal" : "DIFFERENT", o.wallMillis());
            System.out.printf("SC-I4 %s firing sequence: %s%n", hypothesis, report.signatures());
            System.out.printf("SC-I4 %s fork: %d live entries, %d of them the spoiler's late intermediate versions; the isolation"
                            + " held %d frame(s); chain nodes reached the joiner from %s; the joiner's tree fetches %s; its late heads %d%n",
                    hypothesis, liveEntries, lateRoots.size(), forked.releases().stream().mapToInt(ScriptRunner.Release::frames).sum(),
                    chainServers, new java.util.TreeMap<>(treeWants), joiner.pdt().stats().headsLate());

            assertThat(repaired).as("the joiner reads the readers' root steadily within %s of fleet time after the release",
                    repairBound).isTrue();
            assertThat(finalRoot).as("the joiner's final root").isEqualTo(referenceRoot);
            assertThat(last).as("the joiner's final read found its base").extracting(PConflict::reason)
                    .doesNotContain(PConflict.NO_COMMON_BASE);
            assertThat((long) last.size()).as("the joiner's final read counts the readers' conflicts").isEqualTo(perReader);
            oracle.assertHolds(report.expectedDrops(), true);
            return o;
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(previous);
            fleet.close();
        }
    }

    /** Ticks until {@code root} names one value that stays for {@code quiet} of fleet time. */
    private static boolean steady(RefFleet fleet, Duration bound, long quiet, java.util.function.Supplier<String> root)
            throws InterruptedException {
        String[] agreed = {null};
        long[] since = {0};
        return fleet.runFor(bound, () -> {
            String now = root.get();
            if (now == null || !now.equals(agreed[0])) {
                agreed[0] = now;
                since[0] = fleet.clock.millis();
                return false;
            }
            return fleet.clock.millis() - since[0] >= quiet;
        });
    }

    /**
     * The entry of the heads' common base among a reader's live entries: the first root of the latest head's
     * breadth-first base walk that every head's walk reaches, as {@code Refs.commonBase} picks it, over the live
     * entries alone (every version from a head down to the base stays live while the fork lasts, PD-R-06).
     */
    private static String baseEntry(RefFleet.Peer reader, Heads settled) {
        Map<String, Space.Entry<PRef>> byRoot = new HashMap<>();
        for (Space.Entry<PRef> e : RefFleet.headEntries(reader, NAME)) {
            byRoot.merge(e.value().root(), e, (a, b) -> a.value().seq() >= b.value().seq() ? a : b);
        }
        List<Set<String>> walks = new ArrayList<>();
        for (Heads.Head head : settled.heads()) {
            Set<String> walk = new LinkedHashSet<>();
            Deque<String> frontier = new ArrayDeque<>(List.of(head.root()));
            while (!frontier.isEmpty()) {
                String root = frontier.poll();
                if (!walk.add(root)) {
                    continue;
                }
                Space.Entry<PRef> e = byRoot.get(root);
                if (e != null) {
                    frontier.addAll(e.value().base());
                }
            }
            walks.add(walk);
        }
        Set<String> latest = walks.get(walks.size() - 1);
        for (String candidate : latest) {
            if (walks.stream().allMatch(w -> w.contains(candidate)) && byRoot.containsKey(candidate)) {
                return byRoot.get(candidate).entryId().toString();
            }
        }
        throw new AssertionError("the heads " + settled.roots() + " share no live base entry on " + reader.name());
    }

    /** The roots of every live version that is no settled head: the versions a late joiner loads below the heads. */
    private static Set<String> lateVersions(RefFleet.Peer reader, List<String> settledRoots) {
        Set<String> late = java.util.concurrent.ConcurrentHashMap.newKeySet();
        for (Space.Entry<PRef> e : RefFleet.headEntries(reader, NAME)) {
            if (!settledRoots.contains(e.value().root())) {
                late.add(e.value().root());
            }
        }
        return late;
    }

    private static String reasonName(String reason) {
        return reason.equals(PConflict.LATER_HLC) ? "LATER_HLC" : reason.equals(PConflict.NO_COMMON_BASE) ? "NO_COMMON_BASE"
                : reason.equals(PConflict.WHOLE_HEAD) ? "WHOLE_HEAD" : reason;
    }
}
