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
package ai.badmonkey.agentspaces.space.replicated;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.hlc.HybridLogicalClock;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.crdt.Dot;
import ai.badmonkey.agentspaces.space.crdt.EntryState;
import ai.badmonkey.agentspaces.space.crdt.LwwRegister;
import ai.badmonkey.agentspaces.space.crdt.SpaceStateCrdt;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * RE-H-05 and RE-Q-REL-04 of srad/replication-by-entry.md: a randomized simulation of the per-entry horizon of §3.5
 * over the real {@link SpaceStateCrdt} and {@link ReplicatedSpace}. Three replicas of one space sit on a
 * {@link SimNetwork}, each in the others' views; replica a writes, renews and cancels, replicas b and c take and
 * complete. The views form over the network first, then the network is cut for good, so that copies travel only by
 * the run's anti-entropy pulls, whose answers are held in flight for a random number of steps, lost, reordered, or
 * cut by a partition. No node ticks after that, so no member leaves a view. Each replica reads its own clock, the shared fake time plus a skew that moves within plus or minus five
 * minutes, so any two clocks stay within {@code MAX_DRIFT} (ten minutes, SPEC §9) and none ever runs backwards. After
 * every step the test reads every copy of every entry, held or in flight, and checks the invariants of
 * {@code docs/proofs/HorizonPerEntry.tla}, NoLateLive included: since the drift floor of SPEC §7.3 (v0.1.35, owner
 * decision 22), once any replica has collected a withdrawal, no replica turns that entry live, not even one that never
 * heard of the withdrawal and whose clock trails by the whole drift ceiling.
 *
 * <p><b>Two runs.</b> {@link PerEntryRule} restates the rule of §3.5 independently, as step 5a modelled it, and is the
 * oracle the real {@code SpaceStateCrdt.collectable(state, now, maxLease, Horizon.PER_ENTRY)} is checked against at
 * every delivery of both runs.
 * <ul>
 *   <li><i>Engaged</i> (step 5b): every node advertises {@code wire.horizon=1}, no {@code maxLease} is fixed, and each
 *   replica reads its mode from its own view and clock: it engages after three fully hinted rounds and comes into
 *   force one maximum lease plus five minutes later, so each run holds a stretch under the legacy rule before the
 *   per-entry one. The run asserts every replica reaches {@code PER_ENTRY}, and checks the invariants against the
 *   rule each collection was made under.</li>
 *   <li><i>Shadow</i>: every node keeps {@code agentspaces.space.horizon=legacy}, so the real classes keep the unhinted
 *   rule, and at every step where {@link PerEntryRule} would collect a replica's withdrawn tombstone, the test checks
 *   that every copy that tombstone subsumes, anywhere, would be refused at that replica's door by the same rule.</li>
 * </ul>
 */
class HorizonSimulationTest {

    private static final long SECOND = 1_000L;
    private static final long MINUTE = 60 * SECOND;
    /** Each clock stays within this of the shared time, so two clocks stay within MAX_DRIFT (ten minutes). */
    private static final long MAX_SKEW = 5 * MINUTE;
    private static final int STEPS = 400;
    private static final int SEEDS = 200;
    private static final int ENTRIES = 6;
    private static final CborCodec CODEC = CborCodec.defaultCodec();

    /**
     * The per-entry horizon rule of §3.5, restated apart from the real classes as the oracle: a withdrawn entry that is
     * not completed is collectable the later of five minutes and the drift ceiling (ten minutes) past its own lease
     * expiry L; a completed entry keeps 2 x maxLease, and one withdrawn as well at least the drift ceiling; a lapsed
     * one keeps the lapse horizon of SPEC §7.3. The door refuses a copy exactly when this says collectable.
     */
    static final class PerEntryRule {
        private PerEntryRule() {
        }

        static long keep(Copy c, long maxLease) {
            if (c.completed()) {
                long keep = 2 * Math.max(0, maxLease);
                // Withdrawn as well: the drift floor too, so it never goes before a copy withdrawn alone.
                return c.removes().containsAll(c.adds()) ? Math.max(keep, HybridLogicalClock.MAX_DRIFT_MILLIS) : keep;
            }
            if (c.withdrawn()) {
                return Math.max(5 * MINUTE, HybridLogicalClock.MAX_DRIFT_MILLIS);
            }
            return SpaceStateCrdt.lapsedKeepMillis(maxLease);
        }

        static long horizon(Copy c, long maxLease) {
            long expiry = c.expiry();
            long horizon = expiry + keep(c, maxLease);
            return horizon >= expiry ? horizon : Long.MAX_VALUE;
        }

        static boolean collectable(Copy c, long now, long maxLease) {
            if (c.present() && c.expiry() > now) {
                return false;
            }
            return horizon(c, maxLease) <= now;
        }
    }

    /** One copy of an entry's state as it travels: the signed state's fields, read without its signature. */
    record Copy(EntryId id, Set<Dot> adds, Set<Dot> removes, boolean completed, HlcTimestamp stamp, LeaseInfo lease) {

        static Copy of(SpaceWire.EntryStateDto dto) {
            EntryId id = dto.record() != null ? dto.record().entryId() : dto.entryId();
            return new Copy(id, Set.copyOf(dto.adds()), Set.copyOf(dto.removes()), dto.completed(),
                    dto.leaseStamp(), dto.leaseValue());
        }

        long expiry() {
            return lease.expiresAtMillis();
        }

        boolean present() {
            return !completed && !removes.containsAll(adds);
        }

        boolean withdrawn() {
            return !completed && !adds.isEmpty() && removes.containsAll(adds);
        }

        /** Merging this copy into {@code k} changes nothing: it says nothing {@code k} does not. */
        boolean subsumedBy(Copy k) {
            return k.adds.containsAll(adds) && k.removes.containsAll(removes) && (!completed || k.completed)
                    && stamp.compareTo(k.stamp) <= 0;
        }

        EntryState state(EntryRecord record) {
            return new EntryState(record, adds, removes, new LwwRegister<>(stamp, lease), completed);
        }
    }

    /** A replica's clock: the shared fake time plus its own skew, which moves but never takes the clock backwards. */
    static final class SkewClock implements InstantSource {
        private final TestClock real;
        private volatile long offset;

        SkewClock(TestClock real, long offset) {
            this.real = real;
            this.offset = offset;
        }

        @Override
        public Instant instant() {
            return real.instant().plusMillis(offset);
        }
    }

    private record Msg(int from, int to, byte[] bytes, List<Copy> copies, int due) {
    }

    /** What one run saw, summed over seeds for the report and the vacuity checks. */
    static final class Stats {
        long steps;
        long deliveries;
        long lost;
        long withdrawals;
        long completions;
        long collectedWithdrawals;
        long refusedPresentAfterCollect;
        long perEntryCollectedWithdrawals;
        long perEntryCollectedElsewhere;
        long refusedPresentAfterPerEntryCollect;
        long perEntrySteps;
        long oracleChecks;
        long shadowDue;
        long shadowPresentCopies;
        long completionReturned;
        long legacyBounces;
        long lateCopiesDelivered;
        long lateLiveChecks;
        long maxSkewSeen;

        void add(Stats o) {
            steps += o.steps;
            deliveries += o.deliveries;
            lost += o.lost;
            withdrawals += o.withdrawals;
            completions += o.completions;
            collectedWithdrawals += o.collectedWithdrawals;
            refusedPresentAfterCollect += o.refusedPresentAfterCollect;
            perEntryCollectedWithdrawals += o.perEntryCollectedWithdrawals;
            perEntryCollectedElsewhere += o.perEntryCollectedElsewhere;
            refusedPresentAfterPerEntryCollect += o.refusedPresentAfterPerEntryCollect;
            perEntrySteps += o.perEntrySteps;
            oracleChecks += o.oracleChecks;
            shadowDue += o.shadowDue;
            shadowPresentCopies += o.shadowPresentCopies;
            completionReturned += o.completionReturned;
            legacyBounces += o.legacyBounces;
            lateCopiesDelivered += o.lateCopiesDelivered;
            lateLiveChecks += o.lateLiveChecks;
            maxSkewSeen = Math.max(maxSkewSeen, o.maxSkewSeen);
        }

        @Override
        public String toString() {
            return "steps " + steps + ", deliveries " + deliveries + ", lost " + lost + ", withdrawals " + withdrawals
                    + ", completions " + completions + ", withdrawn tombstones collected " + collectedWithdrawals
                    + ", present copies refused after a collected withdrawal " + refusedPresentAfterCollect
                    + ", of them withdrawn tombstones collected under the per-entry rule " + perEntryCollectedWithdrawals
                    + " (" + perEntryCollectedElsewhere + " on a replica other than the writer's)"
                    + " and present copies refused after one " + refusedPresentAfterPerEntryCollect
                    + ", replica-steps in force " + perEntrySteps + ", oracle checks " + oracleChecks
                    + ", shadow horizons due " + shadowDue + ", present copies checked at them "
                    + shadowPresentCopies + ", completions taken back present (a delivery assumption, see the proof"
                    + " note) " + completionReturned + ", collected tombstones taken back after the observed maxLease"
                    + " grew (never a withdrawal collected under the per-entry rule) " + legacyBounces
                    + ", present copies delivered to a replica that never held the entry after another collected its"
                    + " withdrawal " + lateCopiesDelivered + ", replica-entry checks of NoLateLive " + lateLiveChecks
                    + ", largest skew between two clocks " + maxSkewSeen / SECOND + " s";
        }
    }

    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<GroupRuntime> runtimes = new ArrayList<>();

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
        nodes.clear();
        runtimes.clear();
    }

    private ReplicatedSpace replica(SimNetwork network, GroupAdvertisement ad, String name, PeerIdentity identity,
                                    InstantSource clock, boolean engaged, long seed) throws Exception {
        return replica(network, ad, name, identity, clock, engaged, seed, List.of());
    }

    /**
     * A replica on its own node. The engaged run's nodes advertise wire.horizon=1 (the default); the shadow run's
     * keep the legacy rule. With no seeds the view stays empty, and the replica judges by its own setting and its own
     * clock, and its own writes alone by the per-entry rule.
     */
    private ReplicatedSpace replica(SimNetwork network, GroupAdvertisement ad, String name, PeerIdentity identity,
                                    InstantSource clock, boolean engaged, long seed, List<String> seeds)
            throws Exception {
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).horizon(engaged).build();
        nodes.add(node);
        node.listen(network.register(name), name);
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String s : seeds) {
            endpoints.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(ad, GroupMembership.Config.defaults(), endpoints);
        runtimes.add(runtime);
        return ReplicatedSpace.builder(runtime, "tasks", identity, "worker")
                .clock(clock).settleWindow(Duration.ZERO).build();
    }

    private static GroupAdvertisement group(String id) {
        return new GroupAdvertisement("aspace://" + id, PeerIdentity.generate().peerId(), GroupId.of(id),
                Instant.EPOCH, Duration.ofDays(1), "horizon", GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
    }

    /** Every copy a replica would offer a partner that has nothing: its held states, tombstones included. */
    private static Map<EntryId, Copy> observe(ReplicatedSpace replica) {
        return decode(replica.deltaFor(new byte[0]));
    }

    private static Map<EntryId, Copy> decode(byte[] delta) {
        Map<EntryId, Copy> out = new HashMap<>();
        if (delta.length == 0) {
            return out;
        }
        SpaceWire.SyncDelta sync = CODEC.fromWire(delta, SpaceWire.SyncDelta.class);
        if (sync.states() != null) {
            for (SpaceWire.EntryStateDto dto : sync.states()) {
                Copy c = Copy.of(dto);
                out.put(c.id(), c);
            }
        }
        return out;
    }

    /** Every replica holds the other two in its view, and, in the engaged run, reads the hint on their cards. */
    private static boolean seeAllHinted(List<GroupRuntime> runtimes, boolean engaged) {
        for (GroupRuntime runtime : runtimes) {
            if (runtime.membership().allMembers().size() != 2) {
                return false;
            }
            if (engaged) {
                for (GroupMembership.Member m : runtime.membership().allMembers()) {
                    if (!runtime.advertisesHorizon(m.id())) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    // ---- the runs ----

    @Test
    void theEngagedHorizonNeverLetsAWithdrawnEntryBack() throws Exception {
        Stats total = new Stats();
        for (long seed = 1; seed <= SEEDS; seed++) {
            total.add(run(seed, true));
            tearDown();
        }
        System.out.println("HorizonSimulation engaged, " + SEEDS + " seeds x " + STEPS + " steps: " + total);
        // Not vacuous: the mode came into force, withdrawn tombstones were collected by the per-entry rule, and present
        // copies older than the withdrawal reached a replica after it collected it, and its door turned them away.
        assertThat(total.perEntrySteps).isPositive();
        assertThat(total.perEntryCollectedWithdrawals).isPositive();
        // The writer's hint reaches the other replicas through their views: they collect its withdrawals by it too.
        assertThat(total.perEntryCollectedElsewhere).isPositive();
        assertThat(total.refusedPresentAfterPerEntryCollect).isPositive();
        // NoLateLive was judged, entry by entry, on every replica after a withdrawal was collected somewhere.
        assertThat(total.lateLiveChecks).isPositive();
    }

    @Test
    void theShadowHorizonWouldRefuseEveryCopyItCollects() throws Exception {
        Stats total = new Stats();
        for (long seed = 1_001; seed <= 1_000 + SEEDS; seed++) {
            total.add(run(seed, false));
            tearDown();
        }
        System.out.println("HorizonSimulation shadow, " + SEEDS + " seeds x " + STEPS + " steps: " + total);
        assertThat(total.shadowDue).isPositive();
        assertThat(total.shadowPresentCopies).isPositive();
        assertThat(total.perEntrySteps).as("legacy nodes never engage").isZero();
    }

    private Stats run(long seed, boolean engaged) throws Exception {
        Random rnd = new Random(seed);
        Stats stats = new Stats();
        SimNetwork network = new SimNetwork();
        GroupAdvertisement ad = group("zHorizon" + seed);
        TestClock real = TestClock.create();
        SkewClock[] clocks = new SkewClock[3];
        ReplicatedSpace[] r = new ReplicatedSpace[3];
        String[] names = {"a", "b", "c"};
        long[] skew = new long[3];
        for (int i = 0; i < 3; i++) {
            skew[i] = rnd.nextLong(-MAX_SKEW, MAX_SKEW + 1);
            clocks[i] = new SkewClock(real, 0); // the views form on one clock; frames older than 5 min are dropped
            r[i] = replica(network, ad, names[i], PeerIdentity.generate(), clocks[i], engaged, seed * 10 + i,
                    i == 0 ? List.of() : List.of(names[0]));
        }
        // Each replica holds the other two in its view, with their cards, before anything is written: b and c then
        // judge a's withdrawals by the per-entry rule once a has been in view a maximum lease and the grace (RE-H-04).
        int[] ticks = {0};
        while (!seeAllHinted(runtimes, engaged)) {
            assertThat(ticks[0]++).as("the views form, seed %d", seed).isLessThan(800);
            nodes.forEach(PeerNode::tick);
            real.advance(Duration.ofMillis(250));
        }
        // Then the network is cut for good: copies travel only by the run's own pulls, and no node ticks again, so
        // the views never change. Ten minutes on, every member has been in view longer than any settle of the run
        // (a lease of at most four minutes plus the grace), and then each clock takes its skew without stepping back.
        network.partition(names[0], names[1]);
        network.partition(names[0], names[2]);
        network.partition(names[1], names[2]);
        real.advance(Duration.ofMinutes(10).plusMillis(MAX_SKEW));
        for (int i = 0; i < 3; i++) {
            clocks[i].offset = skew[i];
        }
        if (!engaged) {
            // A long lease seen keeps the legacy horizon of a withdrawal (2 x 30 min) well past the per-entry one (10
            // min), so the shadow run has tombstones the per-entry rule would collect while the legacy rule holds them.
            // Since the drift floor, the leases of the run alone (at most four minutes) give both rules ten minutes.
            r[0].write(new TaskEntry("companion-" + seed, -1), Lease.of(Duration.ofMinutes(30)));
        }
        List<EntryHandle> handles = new ArrayList<>();
        Map<EntryId, EntryRecord> records = new HashMap<>();
        Map<EntryId, Long> withdrawnAt = new HashMap<>(); // L of each withdrawn entry
        List<List<TakenEntry<TaskEntry>>> taken = List.of(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        List<Msg> inFlight = new ArrayList<>();
        int isolated = -1; // the replica a partition cuts off, or -1
        List<Map<EntryId, Copy>> seen = List.of(new HashMap<>(), new HashMap<>(), new HashMap<>());
        List<Set<EntryId>> knew = List.of(new HashSet<>(), new HashSet<>(), new HashSet<>());
        List<Map<EntryId, List<Copy>>> gone = List.of(new HashMap<>(), new HashMap<>(), new HashMap<>());
        // The maxLease each replica reckoned with at the previous check, and the one in force when each state left.
        long[] lastMaxLease = new long[3];
        List<Map<Copy, Long>> goneUnder = List.of(new HashMap<>(), new HashMap<>(), new HashMap<>());
        // The copies each replica collected while the per-entry rule was in force there at the previous check too.
        List<Set<Copy>> goneByRule = List.of(new HashSet<>(), new HashSet<>(), new HashSet<>());
        boolean[] wasPerEntry = new boolean[3];
        // NoLateLive: the entries each replica held live at the previous check, and the entries whose withdrawal some
        // replica had collected by then (the TLA+ property reads its premise in the state before the step).
        List<Set<EntryId>> wasLive = List.of(new HashSet<>(), new HashSet<>(), new HashSet<>());
        Set<EntryId> withdrawalCollected = new HashSet<>();
        int written = 0;
        for (int step = 0; step < STEPS; step++) {
            String at = (engaged ? "engaged" : "shadow") + " seed " + seed + " step " + step;
            stats.steps++;
            int op = rnd.nextInt(14);
            switch (op) {
                case 0 -> {
                    if (written < ENTRIES) {
                        EntryHandle h = r[0].write(new TaskEntry("t-" + seed + "-" + written, written),
                                Lease.of(Duration.ofSeconds(30 + rnd.nextInt(211))));
                        handles.add(h);
                        records.put(h.entryId(), r[0].recordOf(h.entryId()).orElseThrow());
                        written++;
                    }
                }
                case 1 -> {
                    if (!handles.isEmpty()) {
                        EntryHandle h = handles.get(rnd.nextInt(handles.size()));
                        try {
                            h.renew(Duration.ofSeconds(30 + rnd.nextInt(211)));
                        } catch (RuntimeException lapsedOrGone) {
                            // a renewal needs the entry present and unexpired at the writer
                        }
                    }
                }
                case 2 -> {
                    if (!handles.isEmpty() && rnd.nextInt(3) == 0) {
                        EntryHandle h = handles.get(rnd.nextInt(handles.size()));
                        Optional<EntryRecord> before = r[0].recordOf(h.entryId());
                        h.cancel();
                        Copy after = observe(r[0]).get(h.entryId());
                        if (before.isPresent() && after != null && after.withdrawn()
                                && !withdrawnAt.containsKey(h.entryId())) {
                            withdrawnAt.put(h.entryId(), before.get().lease().expiresAtMillis());
                            stats.withdrawals++;
                        }
                    }
                }
                case 3 -> {
                    int i = 1 + rnd.nextInt(2);
                    r[i].take(Template.of(TaskEntry.class), Lease.of(Duration.ofSeconds(60 + rnd.nextInt(121))),
                            Duration.ZERO).ifPresent(taken.get(i)::add);
                }
                case 4 -> {
                    int i = 1 + rnd.nextInt(2);
                    if (!taken.get(i).isEmpty()) {
                        TakenEntry<TaskEntry> t = taken.get(i).remove(rnd.nextInt(taken.get(i).size()));
                        try {
                            r[i].complete(t);
                            stats.completions++;
                        } catch (RuntimeException lost) {
                            // the take lapsed, lost its race, or the entry left first
                        }
                    }
                }
                case 5, 6, 7, 8 -> {
                    // One anti-entropy pull: j sends its digest, i answers with what j lacks, and the answer travels.
                    int i = rnd.nextInt(3);
                    int j = (i + 1 + rnd.nextInt(2)) % 3;
                    if (isolated == i || isolated == j) {
                        break;
                    }
                    byte[] answer = r[i].deltaFor(r[j].digest());
                    if (answer.length == 0) {
                        break;
                    }
                    if (rnd.nextInt(7) == 0) {
                        stats.lost++;
                        break;
                    }
                    // Most answers arrive within a few steps, about one in four within 40, and one in eight within
                    // 120: since the drift floor, a present copy reaches a replica that collected its withdrawal only
                    // after a long wait in flight, and those late arrivals are what the door is checked on.
                    int delay = rnd.nextInt(8) == 0 ? rnd.nextInt(120)
                            : rnd.nextInt(4) == 0 ? rnd.nextInt(40) : rnd.nextInt(3);
                    inFlight.add(new Msg(i, j, answer, List.copyOf(decode(answer).values()), step + delay));
                }
                case 9, 10 -> {
                    long advance = rnd.nextInt(90_001);
                    long[] before = new long[3];
                    for (int i = 0; i < 3; i++) {
                        before[i] = clocks[i].millis();
                    }
                    real.advance(Duration.ofMillis(advance));
                    for (int i = 0; i < 3; i++) {
                        SkewClock clock = clocks[i];
                        // The skew drifts, but a clock never steps back: its skew falls by at most the time that
                        // passed, and the bounds only ever raise it or bring it down to MAX_SKEW from above.
                        long next = clock.offset + rnd.nextLong(-30 * SECOND, 30 * SECOND + 1);
                        next = Math.max(next, clock.offset - advance);
                        clock.offset = Math.max(-MAX_SKEW, Math.min(MAX_SKEW, next));
                        assertThat(clock.millis()).as("a clock never steps back").isGreaterThanOrEqualTo(before[i]);
                    }
                }
                case 11 -> r[rnd.nextInt(3)].sweepNow();
                case 12 -> isolated = rnd.nextInt(4) == 0 ? -1 : rnd.nextInt(3);
                default -> isolated = -1;
            }
            // Deliver what is due, in a random order; a partition loses what crosses it.
            List<Msg> due = new ArrayList<>();
            for (Iterator<Msg> it = inFlight.iterator(); it.hasNext(); ) {
                Msg m = it.next();
                if (m.due() <= step) {
                    due.add(m);
                    it.remove();
                }
            }
            java.util.Collections.shuffle(due, rnd);
            for (Msg m : due) {
                if (isolated == m.from() || isolated == m.to()) {
                    stats.lost++;
                    continue;
                }
                stats.deliveries++;
                ReplicatedSpace to = r[m.to()];
                long now = clocks[m.to()].millis();
                for (Copy c : m.copies()) {
                    // The real per-entry rule and the oracle agree on every copy the run can judge by both.
                    EntryRecord record = records.get(c.id());
                    if (record != null) {
                        stats.oracleChecks++;
                        assertThat(SpaceStateCrdt.collectable(c.state(record), now, to.maxLeaseMillis(),
                                SpaceStateCrdt.Horizon.PER_ENTRY))
                                .as("the real per-entry rule and PerEntryRule agree on %s at %s", c, at)
                                .isEqualTo(PerEntryRule.collectable(c, now, to.maxLeaseMillis()));
                    }
                }
                for (Copy c : m.copies()) {
                    // The case the drift floor is for: a present copy reaching a replica that never held the entry,
                    // after another replica collected its withdrawal.
                    if (c.present() && withdrawalCollected.contains(c.id()) && !seen.get(m.to()).containsKey(c.id())
                            && gone.get(m.to()).getOrDefault(c.id(), List.of()).isEmpty()) {
                        stats.lateCopiesDelivered++;
                    }
                }
                to.applySyncDelta(m.bytes());
                for (Copy c : m.copies()) {
                    List<Copy> collected = gone.get(m.to()).getOrDefault(c.id(), List.of());
                    boolean collectedWithdrawal = collected.stream().anyMatch(Copy::withdrawn);
                    if (c.present() && collectedWithdrawal && to.recordOf(c.id(), true).isEmpty()
                            && !seen.get(m.to()).containsKey(c.id())) {
                        stats.refusedPresentAfterCollect++;
                        if (collected.stream().anyMatch(k -> k.withdrawn() && goneByRule.get(m.to()).contains(k))) {
                            stats.refusedPresentAfterPerEntryCollect++;
                        }
                    }
                }
            }
            check(r, clocks, inFlight, records, withdrawnAt, seen, knew, gone, lastMaxLease, goneUnder, goneByRule,
                    wasPerEntry, wasLive, withdrawalCollected, engaged, stats, at);
        }
        return stats;
    }

    private static void check(ReplicatedSpace[] r, SkewClock[] clocks, List<Msg> inFlight,
                              Map<EntryId, EntryRecord> records, Map<EntryId, Long> withdrawnAt,
                              List<Map<EntryId, Copy>> seen, List<Set<EntryId>> knew,
                              List<Map<EntryId, List<Copy>>> gone, long[] lastMaxLease,
                              List<Map<Copy, Long>> goneUnder, List<Set<Copy>> goneByRule, boolean[] wasPerEntry,
                              List<Set<EntryId>> wasLive, Set<EntryId> withdrawalCollected,
                              boolean engaged, Stats stats, String at) {
        List<Map<EntryId, Copy>> now = new ArrayList<>();
        for (ReplicatedSpace replica : r) {
            now.add(observe(replica));
        }
        long[] t = new long[3];
        for (int i = 0; i < 3; i++) {
            t[i] = clocks[i].millis();
        }
        stats.maxSkewSeen = Math.max(stats.maxSkewSeen,
                Math.max(t[0], Math.max(t[1], t[2])) - Math.min(t[0], Math.min(t[1], t[2])));
        // Every copy of each entry, held or in flight.
        Map<EntryId, List<Copy>> copies = new HashMap<>();
        for (Map<EntryId, Copy> held : now) {
            held.values().forEach(c -> copies.computeIfAbsent(c.id(), k -> new ArrayList<>()).add(c));
        }
        for (Msg m : inFlight) {
            m.copies().forEach(c -> copies.computeIfAbsent(c.id(), k -> new ArrayList<>()).add(c));
        }
        for (int i = 0; i < 3; i++) {
            Map<EntryId, Copy> before = seen.get(i);
            Map<EntryId, Copy> after = now.get(i);
            boolean perEntry = r[i].horizonMode() == ReplicatedSpace.HorizonMode.PER_ENTRY;
            assertThat(engaged || !perEntry).as("a legacy node never engages, at %s", at).isTrue();
            if (perEntry) {
                stats.perEntrySteps++;
            }
            // A state that left the replica was collected: remember it, and every copy it subsumes stays refused.
            for (Map.Entry<EntryId, Copy> e : before.entrySet()) {
                if (!after.containsKey(e.getKey())) {
                    gone.get(i).computeIfAbsent(e.getKey(), k -> new ArrayList<>()).add(e.getValue());
                    goneUnder.get(i).putIfAbsent(e.getValue(), lastMaxLease[i]);
                    if (e.getValue().withdrawn()) {
                        stats.collectedWithdrawals++;
                    }
                    // In force at the previous check and now, so in force when the sweep collected it: the mode only
                    // leaves PER_ENTRY when an unhinted member appears, and none does here. The writer's half of the
                    // rule is read from the real class: its writer is this replica, or a hinted member long in view.
                    EntryRecord written = records.get(e.getKey());
                    if (wasPerEntry[i] && perEntry && written != null
                            && r[i].ruleFor(e.getValue().state(written)) == SpaceStateCrdt.Horizon.PER_ENTRY) {
                        goneByRule.get(i).add(e.getValue());
                        if (e.getValue().withdrawn()) {
                            stats.perEntryCollectedWithdrawals++;
                            if (i != 0) {
                                stats.perEntryCollectedElsewhere++;
                            }
                        }
                    }
                }
            }
            for (Map.Entry<EntryId, Copy> e : after.entrySet()) {
                Copy held = e.getValue();
                if (held.withdrawn()) {
                    knew.get(i).add(e.getKey());
                }
                if (!before.containsKey(e.getKey())) {
                    for (Copy k : gone.get(i).getOrDefault(e.getKey(), List.of())) {
                        if (!held.subsumedBy(k)) {
                            continue;
                        }
                        // A withdrawal collected under the per-entry rule never comes back: its keep, the later of
                        // five minutes and the drift ceiling, does not move. Every other horizon reads the replica's
                        // observed maxLease, which grows when a longer lease is seen, so there a collected tombstone
                        // is taken back after such growth, and only then. It comes back as the tombstone it was, never present: NoResurrection still holds.
                        assertThat(k.withdrawn() && goneByRule.get(i).contains(k))
                                .as("replica %d took back %s, a withdrawal it collected under the per-entry rule, at %s",
                                        i, held, at)
                                .isFalse();
                        assertThat(r[i].maxLeaseMillis())
                                .as("replica %d took back %s with no growth of its maxLease, at %s", i, k, at)
                                .isGreaterThan(goneUnder.get(i).get(k));
                        stats.legacyBounces++;
                    }
                    if (gone.get(i).getOrDefault(e.getKey(), List.of()).stream().anyMatch(Copy::completed)
                            && held.present()) {
                        stats.completionReturned++;
                    }
                }
            }
            // NoResurrection: a replica that held the entry withdrawn never holds it present and unexpired again.
            for (EntryId id : knew.get(i)) {
                Optional<EntryRecord> live = r[i].recordOf(id);
                assertThat(live.isPresent() && live.get().lease().expiresAtMillis() > t[i])
                        .as("replica %d holds %s present and unexpired after its withdrawal, at %s", i, id, at)
                        .isFalse();
            }
            // RefusedAtEveryDoor: each copy a withdrawal collected under the per-entry rule subsumes is refused at this
            // replica's door now, under the rule the replica applies now.
            if (engaged && perEntry) {
                for (Map.Entry<EntryId, List<Copy>> g : gone.get(i).entrySet()) {
                    EntryRecord record = records.get(g.getKey());
                    for (Copy k : g.getValue()) {
                        if (!k.withdrawn() || !goneByRule.get(i).contains(k)) {
                            continue;
                        }
                        for (Copy c : copies.getOrDefault(g.getKey(), List.of())) {
                            if (c.subsumedBy(k) && record != null) {
                                assertThat(r[i].ruleFor(c.state(record))).as("one writer, one rule, at %s", at)
                                        .isEqualTo(SpaceStateCrdt.Horizon.PER_ENTRY);
                                assertThat(SpaceStateCrdt.collectable(c.state(record), t[i], r[i].maxLeaseMillis(),
                                        SpaceStateCrdt.Horizon.PER_ENTRY))
                                        .as("replica %d's door would take back %s after collecting %s, at %s",
                                                i, c, k, at)
                                        .isTrue();
                            }
                        }
                    }
                }
            }
            // Shadow: where the per-entry rule would collect a withdrawn tombstone now, every copy it subsumes
            // anywhere must be one the same rule refuses at this replica's door.
            if (!engaged) {
                for (Copy held : after.values()) {
                    if (held.withdrawn() && PerEntryRule.collectable(held, t[i], r[i].maxLeaseMillis())) {
                        stats.shadowDue++;
                        for (Copy c : copies.getOrDefault(held.id(), List.of())) {
                            if (c.subsumedBy(held)) {
                                if (c.present()) {
                                    stats.shadowPresentCopies++;
                                }
                                assertThat(PerEntryRule.collectable(c, t[i], r[i].maxLeaseMillis()))
                                        .as("the per-entry rule would collect %s at replica %d while %s could"
                                                + " still come back, at %s", held, i, c, at)
                                        .isTrue();
                            }
                        }
                    }
                }
            }
        }
        // NoLateLive: once some replica had collected an entry's withdrawal, no replica that did not hold the entry
        // live then holds it live now, whichever rule collected it. Every clock is within MAX_DRIFT of the collector's, so
        // at L + MAX_DRIFT there it reads L or later, and no copy carries an expiry past L.
        List<Set<EntryId>> liveNow = List.of(new HashSet<>(), new HashSet<>(), new HashSet<>());
        for (int i = 0; i < 3; i++) {
            for (EntryId id : records.keySet()) {
                Optional<EntryRecord> live = r[i].recordOf(id);
                if (live.isPresent() && live.get().lease().expiresAtMillis() > t[i]) {
                    liveNow.get(i).add(id);
                }
            }
            for (EntryId id : withdrawalCollected) {
                stats.lateLiveChecks++;
                assertThat(liveNow.get(i).contains(id) && !wasLive.get(i).contains(id))
                        .as("replica %d turned %s live after a replica collected its withdrawal, at %s", i, id, at)
                        .isFalse();
            }
        }
        // LeaseBound, the lemma of §3.5: once withdrawn, no copy anywhere carries an expiry past the withdrawal's L.
        for (Map.Entry<EntryId, Long> w : withdrawnAt.entrySet()) {
            for (Copy c : copies.getOrDefault(w.getKey(), List.of())) {
                assertThat(c.expiry()).as("a copy of %s outlives the withdrawal's lease, at %s", w.getKey(), at)
                        .isLessThanOrEqualTo(w.getValue());
            }
        }
        for (int i = 0; i < 3; i++) {
            seen.get(i).clear();
            seen.get(i).putAll(now.get(i));
            lastMaxLease[i] = r[i].maxLeaseMillis();
            wasPerEntry[i] = r[i].horizonMode() == ReplicatedSpace.HorizonMode.PER_ENTRY;
            wasLive.get(i).clear();
            wasLive.get(i).addAll(liveNow.get(i));
            for (Map.Entry<EntryId, List<Copy>> g : gone.get(i).entrySet()) {
                if (g.getValue().stream().anyMatch(Copy::withdrawn)) {
                    withdrawalCollected.add(g.getKey());
                }
            }
        }
    }

    /**
     * Brings replicas whose views are empty into the per-entry horizon on a shared fake clock: four judgements a
     * gossip period apart engage it (three counted rounds), and one maximum lease plus five minutes later it is in
     * force. Each sweep judges the mode.
     */
    static void bringIntoForce(TestClock clock, ReplicatedSpace... replicas) {
        for (int round = 0; round <= ReplicatedSpace.DEFAULT_HORIZON_ENGAGE_ROUNDS; round++) {
            for (ReplicatedSpace r : replicas) {
                r.sweepNow();
            }
            clock.advance(Duration.ofSeconds(1));
        }
        for (ReplicatedSpace r : replicas) {
            r.sweepNow();
            assertThat(r.horizonMode()).isIn(ReplicatedSpace.HorizonMode.SETTLING, ReplicatedSpace.HorizonMode.PER_ENTRY);
        }
        long settle = 0;
        for (ReplicatedSpace r : replicas) {
            settle = Math.max(settle, r.maxLeaseMillis());
        }
        clock.advance(Duration.ofMillis(settle + SpaceStateCrdt.LAPSED_GRACE_MILLIS));
        for (ReplicatedSpace r : replicas) {
            r.sweepNow();
            assertThat(r.horizonMode()).isEqualTo(ReplicatedSpace.HorizonMode.PER_ENTRY);
        }
    }

    /**
     * P40 of reviews/RBE-REGRESSION-MATRIX.md: the break condition of §3.5 on the real classes, with the mode read from
     * the view (step 5b). One identity renews from two nodes. Node a writes and later cancels; node b, holding the
     * same identity, renews the entry while it cannot hear a. The renewal outlives the withdrawal's L, so once a
     * collected its tombstone at L + 10 min (the drift floor of SPEC §7.3), b's copy passes a's door as present and
     * unexpired: the entry is back on the replica that withdrew it.
     *
     * <p>What this shows, and what guards it: nothing in the protocol can tell two nodes renewing one identity apart
     * from one, so the guard is the hint's stated precondition. {@code PeerNode.HORIZON_HINT}, the builder's
     * {@code horizon} switch, the {@code agentspaces.space.horizon} property and SPEC §7.3 each say a node that renews
     * for an identity another node renews for at the same time keeps {@code legacy}.
     *
     * <p>The comparison with the legacy rule, honestly: the legacy rule is safe against this break whenever the judge's
     * observed maximum lease covers the other node's renewal, and the per-entry rule is not. With a 30-minute lease
     * seen, the legacy judge keeps the tombstone an hour and the 20-minute copy merges into it; the per-entry judge
     * takes the copy back. Only when the judge never saw a lease as long as the renewal (here, 2 minutes) does the
     * legacy rule let it back too. So the shorter horizon widens the exposure to this break, and the precondition is
     * what the per-entry rule rests on.
     */
    @Test
    void aSecondNodeRenewingTheSameIdentityBreaksTheHorizon() throws Exception {
        assertThat(secondNodeRenewalComesBack(true, false)).as("under the per-entry horizon").isTrue();
        assertThat(secondNodeRenewalComesBack(true, true)).as("under the per-entry horizon, a 30-minute lease seen")
                .isTrue();
        assertThat(secondNodeRenewalComesBack(false, false))
                .as("under the legacy rule, a never having seen a lease as long as the 20 minutes").isTrue();
        assertThat(secondNodeRenewalComesBack(false, true))
                .as("under the legacy rule, a having seen a 30-minute lease: the tombstone outlives the copy").isFalse();
    }

    private boolean secondNodeRenewalComesBack(boolean perEntry, boolean longLeaseSeen) throws Exception {
        SimNetwork network = new SimNetwork();
        GroupAdvertisement ad = group("zHorizonP40" + perEntry + longLeaseSeen);
        TestClock clock = TestClock.create();
        PeerIdentity shared = PeerIdentity.generate();
        ReplicatedSpace a = replica(network, ad, "a", shared, clock, perEntry, 1);
        ReplicatedSpace b = replica(network, ad, "b", shared, clock, perEntry, 2);
        if (longLeaseSeen) {
            a.write(new TaskEntry("long", 0), Lease.of(Duration.ofMinutes(30)));
        }
        if (perEntry) {
            bringIntoForce(clock, a, b);
        }
        EntryHandle handle = a.write(new TaskEntry("p40", 1), Lease.of(Duration.ofMinutes(2)));
        EntryId id = handle.entryId();
        b.applySyncDelta(a.deltaFor(b.digest()));
        assertThat(b.recordOf(id)).isPresent();

        // b renews as the same identity, and a never hears it.
        b.handleAs(id, shared.agentIdentity("worker")).renew(Duration.ofMinutes(20));
        long renewed = b.recordOf(id).orElseThrow().lease().expiresAtMillis();
        long lease = a.recordOf(id).orElseThrow().lease().expiresAtMillis();
        handle.cancel();
        assertThat(renewed).as("the second node's renewal outlives the withdrawal's L").isGreaterThan(lease);

        clock.advance(Duration.ofMinutes(13)); // past L + max(5 min, MAX_DRIFT), and past L + 2 x 2 min, on a
        a.sweepNow();
        assertThat(a.horizonMode()).isEqualTo(perEntry ? ReplicatedSpace.HorizonMode.PER_ENTRY
                : ReplicatedSpace.HorizonMode.LEGACY);
        boolean legacyKeeps = !perEntry && longLeaseSeen; // L + 2 x 30 min is far off
        assertThat(observe(a)).as("a collected its tombstone, unless the legacy rule keeps it")
                .matches(held -> held.containsKey(id) == legacyKeeps);

        a.applySyncDelta(b.deltaFor(a.digest()));
        Optional<EntryRecord> back = a.recordOf(id);
        return back.isPresent() && back.get().lease().expiresAtMillis() > clock.instant().toEpochMilli();
    }
}
