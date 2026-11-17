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
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.SpaceId;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * RE-Q-PRF-01 and RE-Q-PRF-06 (srad/replication-by-entry.md): what one changed state, a digest, a delta and a join cost
 * in one replicated space of n entries, and what the map costs in memory per entry. Phase 0 took the before figures
 * while the map was a hash map copied on every change (§10.1); phase 1 runs the same steps over the fingerprinted trie.
 * A JMH-free timing in {@link DigestScalingTest}'s style: it prints its numbers and asserts only that each step did its
 * work, so a loaded machine cannot fail it.
 *
 * <p>The suite runs it at 10,000 entries. The full baseline runs with
 * {@code -Dagentspaces.bench.sizes=10000,100000,1000000}; the join, which is quadratic in n today, runs only for the
 * sizes up to {@code agentspaces.bench.join-max} (100,000 by default).
 *
 * <ul>
 *   <li>(a) one new signed state merged by anti-entropy ({@code applySyncDelta} of a one-state delta), the part of it
 *       spent under the space's lock (the merge, without the signature checks made before the lock), the same delta
 *       again (held already, so no copy), and the bare CRDT merge of one state into the n-entry map;</li>
 *   <li>(b) the digest rebuilt on a cache miss;</li>
 *   <li>(c) the digest hash on a miss: the rebuild plus SHA-256 over it, and the hash alone;</li>
 *   <li>(d) {@code deltaFor} against a partner digest that lacks 1 and 100 entries;</li>
 *   <li>(e) a join: one PULL_RESP of all n signed states applied to an empty replica;</li>
 *   <li>(f) the memory of the map per entry beyond the state objects: the trie and a live index of the shape the
 *       replicated space keeps (RE-Q-PRF-06), beside a hash map of the same states for comparison.</li>
 * </ul>
 */
class ReplicationBaselineTest {

    private static final CborCodec CODEC = CborCodec.defaultCodec();

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final GroupId groupId = GroupId.of("zBaseline");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zBaseline", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "baseline",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private PeerIdentity identity;
    private PeerNode node;
    private GroupRuntime runtime;

    @BeforeEach
    void setUp() throws Exception {
        identity = PeerIdentity.generate();
        node = PeerNode.builder(identity).clock(clock).randomSeed(1).build();
        node.listen(network.register("baseline"), "baseline");
        runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
    }

    @AfterEach
    void tearDown() {
        node.close();
    }

    @Test
    void theBaselineOfOneChangeADigestADeltaAndAJoin() {
        int[] sizes = Arrays.stream(System.getProperty("agentspaces.bench.sizes", "10000").split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).mapToInt(Integer::parseInt).toArray();
        int joinMax = Integer.getInteger("agentspaces.bench.join-max", 100_000);
        StringBuilder report = new StringBuilder(String.format(
                "replication baseline (RE-Q-PRF-01 and -06; medians; %s):", mapKind()));
        measure(1_000, false, new StringBuilder()); // warm the JIT on a small space, unreported
        List<double[]> bareBySize = new ArrayList<>();
        for (int n : sizes) {
            bareBySize.add(new double[] {n, measure(n, n <= joinMax, report)});
        }
        double low = bareBySize.stream().mapToDouble(b -> b[1]).min().orElse(0);
        double high = bareBySize.stream().mapToDouble(b -> b[1]).max().orElse(0);
        report.append(String.format("%n  RE-Q-PRF-01: bare merge per state %.2f to %.2f us across the sizes, a ratio of "
                + "%.2f (target: within 2x and under 10 us)", low, high, low == 0 ? 0 : high / low));
        System.out.println(report);
    }

    /** Measures one size and returns the bare merge's median, in microseconds. */
    private double measure(int n, boolean join, StringBuilder report) {
        long started = System.nanoTime();
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "bench-" + n + "-" + System.nanoTime(), identity,
                        "worker").clock(clock).settleWindow(Duration.ZERO).build();
        Seeded seeded = seed(n, space);
        space.mergeForTest(seeded.state(), seeded.signed());
        double seedSeconds = (System.nanoTime() - started) / 1e9;
        assertThat(space.knownEntries()).isEqualTo(n);
        int reps = n >= 1_000_000 ? 5 : 9;

        // (a) one new changed state, then the same state again, then the bare CRDT merge
        List<byte[]> fresh = new ArrayList<>();
        List<SpaceWire.EntryStateDto> freshStates = new ArrayList<>();
        for (int i = 0; i < reps; i++) {
            SpaceWire.EntryStateDto dto = signedWrite(space.id(), "new-" + i);
            freshStates.add(dto);
            fresh.add(CODEC.toBytes(new SpaceWire.SyncDelta(List.of(dto), Map.of())));
        }
        long[] miss = new long[reps];
        long[] locked = new long[reps];
        long[] hit = new long[reps];
        for (int i = 0; i < reps; i++) {
            byte[] delta = fresh.get(i);
            miss[i] = time(() -> space.applySyncDelta(delta) ? 1 : 0);
            locked[i] = space.lastBatchLockedNanos();
        }
        for (int i = 0; i < reps; i++) {
            byte[] delta = fresh.get(i);
            hit[i] = time(() -> space.applySyncDelta(delta) ? 1 : 0);
        }
        assertThat(space.knownEntries()).as("each new state merged once").isEqualTo(n + reps);
        long[] bare = new long[reps];
        for (int i = 0; i < reps; i++) {
            SpaceWire.EntryStateDto dto = freshStates.get(i);
            SpaceStateCrdt one = SpaceStateCrdt.of(dto.record().entryId(), dto.toState());
            bare[i] = time(() -> seeded.state().merge(one).knownEntries());
        }

        // (b) the digest on a cache miss, and (c) the hash on a miss, whole and alone. A full GC before each timed
        // step keeps one step's garbage out of the next one's time: with three reps and no GC at a million entries,
        // (c), which is (b) plus a SHA-256, read lower than (b) in two runs, so the figures were inside their noise.
        int digestReps = n >= 1_000_000 ? 7 : 5;
        long[] digest = new long[digestReps];
        long[] hashWhole = new long[digestReps];
        long[] hashAlone = new long[digestReps];
        byte[] text = space.digest();
        for (int i = 0; i < digestReps; i++) {
            space.invalidateDigestForTest();
            settle();
            digest[i] = time(() -> space.digest().length);
            space.invalidateDigestForTest();
            settle();
            hashWhole[i] = time(() -> space.digestHash().length);
            byte[] built = space.digest();
            hashAlone[i] = time(() -> Digests.sha256(built).length);
        }
        text = space.digest();

        // (d) deltaFor against a partner digest that lacks 1 and 100 entries
        String[] lines = new String(text, StandardCharsets.UTF_8).split("\n");
        long[] lack1 = new long[digestReps];
        long[] lack100 = new long[digestReps];
        int[] lack1Bytes = new int[1];
        int[] lack100Bytes = new int[1];
        byte[] remote1 = without(lines, 1);
        byte[] remote100 = without(lines, 100);
        for (int i = 0; i < digestReps; i++) {
            settle();
            lack1[i] = time(() -> lack1Bytes[0] = space.deltaFor(remote1).length);
            settle();
            lack100[i] = time(() -> lack100Bytes[0] = space.deltaFor(remote100).length);
        }
        assertThat(CODEC.fromWire(space.deltaFor(remote1), SpaceWire.SyncDelta.class).states()).hasSize(1);
        assertThat(CODEC.fromWire(space.deltaFor(remote100), SpaceWire.SyncDelta.class).states()).hasSize(100);
        space.close();

        report.append(String.format("%n  n=%,9d  seeded in %.1f s; digest %,d bytes", n, seedSeconds, text.length));
        report.append(String.format("%n    (a) one new state by applySyncDelta %10.1f us (under the lock %6.1f us; the "
                + "rest is decoding and the two signature checks)   held again %8.1f us   bare SpaceStateCrdt.merge "
                + "%8.2f us %s", micros(miss), micros(locked), micros(hit), micros(bare), spread(bare)));
        report.append(String.format("%n    (b) digest on a miss %12.1f us %s", micros(digest), spread(digest)));
        report.append(String.format("%n    (c) digestHash on a miss %8.1f us %s   SHA-256 alone %8.1f us",
                micros(hashWhole), spread(hashWhole), micros(hashAlone)));
        report.append(String.format("%n    (d) deltaFor lacking 1 %10.1f us %s (%,d bytes)   lacking 100 %10.1f us %s"
                        + " (%,d bytes)", micros(lack1), spread(lack1), lack1Bytes[0], micros(lack100),
                spread(lack100), lack100Bytes[0]));

        if (join) {
            // (e) the join: every state signed, as a PULL_RESP carries them, into an empty replica of the same space
            ReplicatedSpace replica = ReplicatedSpace.builder(runtime, "join-" + n + "-" + System.nanoTime(),
                    identity, "worker").clock(clock).settleWindow(Duration.ZERO).build();
            long signing = System.nanoTime();
            List<SpaceWire.EntryStateDto> all = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                all.add(signedWrite(replica.id(), "join-" + i));
            }
            byte[] pullResp = CODEC.toBytes(new SpaceWire.SyncDelta(all, Map.of()));
            double signSeconds = (System.nanoTime() - signing) / 1e9;
            all = null;
            settle();
            long joined = System.nanoTime();
            assertThat(replica.applySyncDelta(pullResp)).isTrue();
            double joinSeconds = (System.nanoTime() - joined) / 1e9;
            double lockedSeconds = replica.lastBatchLockedNanos() / 1e9;
            assertThat(replica.knownEntries()).isEqualTo(n);
            replica.close();
            report.append(String.format("%n    (e) join of %,d states (%,d bytes) %10.2f s  (%.1f us per state; the merge "
                    + "under the lock %.2f s, %.1f us per state; signing them took %.1f s, not counted)", n,
                    pullResp.length, joinSeconds, joinSeconds * 1e6 / n, lockedSeconds, lockedSeconds * 1e6 / n,
                    signSeconds));
        }

        // (f) the map's memory per entry beyond the state objects (RE-Q-PRF-06)
        report.append(footprint(seeded.state(), n));
        return micros(bare);
    }

    /**
     * The heap the map takes per entry beyond the state objects it holds: the trie built over the states, and the
     * live index {@code ReplicatedSpace} keeps beside it, against the states alone. A hash map of the same states, the map of phase 0, is measured the same way for comparison. Each figure is
     * the used heap after full collections, so it is good to a few percent at a million entries and rough at ten
     * thousand.
     */
    private static String footprint(SpaceStateCrdt seeded, int n) {
        List<EntryState> states = new ArrayList<>(n);
        for (EntryId entryId : seeded.entryIds()) {
            states.add(seeded.state(entryId).orElseThrow());
        }
        long base = usedHeap();
        SpaceStateCrdt map = SpaceStateCrdt.empty();
        for (EntryState state : states) {
            map = map.merge(state.record().entryId(), state);
        }
        LiveIndex live = new LiveIndex();
        for (EntryState state : states) {
            if (state.present()) {
                live.put(SpaceStateCrdt.liveRecord(state));
            }
        }
        long withTrie = usedHeap();
        java.util.Map<EntryId, EntryState> hashMap = new java.util.HashMap<>();
        for (EntryState state : states) {
            hashMap.put(state.record().entryId(), state);
        }
        long withHashMap = usedHeap();
        assertThat(map.knownEntries()).isEqualTo(n);
        assertThat(live.size()).isEqualTo(n);
        assertThat(hashMap).hasSize(n);
        return String.format("%n    (f) memory per entry beyond the states: trie plus live index %6.1f B   (a hash map of "
                + "the states alone, phase 0's map, %6.1f B)", (withTrie - base) / (double) n,
                (withHashMap - withTrie) / (double) n);
    }

    private static long usedHeap() {
        Runtime runtime = Runtime.getRuntime();
        for (int i = 0; i < 4; i++) {
            System.gc();
        }
        return runtime.totalMemory() - runtime.freeMemory();
    }

    /**
     * Which map the CRDT keeps, read off its fields, so a run against a scratch build of an older class says so in its
     * report: the trie of phase 1, or the hash map before it.
     */
    private static String mapKind() {
        for (java.lang.reflect.Field field : SpaceStateCrdt.class.getDeclaredFields()) {
            if (!java.lang.reflect.Modifier.isStatic(field.getModifiers())
                    && (field.getType().getSimpleName().equals("HashTrie") || field.getType() == Map.class)) {
                return "the map is a " + field.getType().getSimpleName();
            }
        }
        return "the map is of an unknown kind";
    }

    /** A digest text with {@code lacking} of its {@code e:} lines left out, as a partner that lacks those entries sends. */
    private static byte[] without(String[] lines, int lacking) {
        StringBuilder sb = new StringBuilder();
        int dropped = 0;
        for (int i = 0; i < lines.length; i++) {
            if (dropped < lacking && lines[i].startsWith("e:") && i % 7 == 3) {
                dropped++;
                continue;
            }
            sb.append(lines[i]).append('\n');
        }
        assertThat(dropped).isEqualTo(lacking);
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private SpaceWire.EntryStateDto signedWrite(SpaceId spaceId, String topic) {
        return ForgeSupport.authoredWrite(identity, "worker", spaceId, new TaskEntry(topic, 1),
                new HlcTimestamp(clock.millis(), 0, "baseline"), clock.millis() + Duration.ofHours(1).toMillis());
    }

    private record Seeded(SpaceStateCrdt state, List<SpaceWire.EntryStateDto> signed) {
    }

    /**
     * n present entries and the state each would carry, merged pairwise so seeding costs n log n, not n squared. The
     * states are not signed: {@code deltaFor} forwards them as held and nothing verifies them here.
     */
    private Seeded seed(int n, ReplicatedSpace space) {
        AgentId issuer = identity.agentIdentity("worker").id();
        byte[] issuerKey = identity.rawPublicKey();
        long now = clock.millis();
        List<SpaceStateCrdt> parts = new ArrayList<>(n);
        List<SpaceWire.EntryStateDto> signed = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            EntryId entryId = EntryId.newId();
            LeaseInfo lease = new LeaseInfo(issuer, now + Duration.ofHours(1).toMillis(), LeaseKind.WRITE);
            HlcTimestamp stamp = new HlcTimestamp(now, 0, issuer.encoded());
            EntryRecord record = new EntryRecord(entryId, space.id(), "bench#1", new byte[] {1}, null, issuer, stamp,
                    lease, Map.of(), new byte[0]);
            EntryState state = EntryState.added(record, new Dot("bench", i + 1), new LwwRegister<>(stamp, lease));
            parts.add(SpaceStateCrdt.of(entryId, state));
            signed.add(SpaceWire.EntryStateDto.from(state, issuerKey));
        }
        while (parts.size() > 1) {
            List<SpaceStateCrdt> next = new ArrayList<>((parts.size() + 1) / 2);
            for (int i = 0; i < parts.size(); i += 2) {
                next.add(i + 1 < parts.size() ? parts.get(i).merge(parts.get(i + 1)) : parts.get(i));
            }
            parts = next;
        }
        return new Seeded(parts.get(0), signed);
    }

    private static long time(LongSupplier work) {
        long started = System.nanoTime();
        long sink = work.getAsLong();
        long elapsed = System.nanoTime() - started;
        if (sink == Long.MIN_VALUE) {
            System.out.print(""); // keeps the result live
        }
        return elapsed;
    }

    /** Two full GCs, untimed, so the step after starts on a quiet heap. */
    private static void settle() {
        System.gc();
        System.gc();
    }

    /** The reps' range and count beside a median, so a reader sees how much of a difference is noise. */
    private static String spread(long[] nanos) {
        long min = Arrays.stream(nanos).min().orElse(0);
        long max = Arrays.stream(nanos).max().orElse(0);
        return String.format("[%.1f to %.1f, %d reps]", min / 1_000.0, max / 1_000.0, nanos.length);
    }

    private static double micros(long[] nanos) {
        long[] sorted = nanos.clone();
        Arrays.sort(sorted);
        return sorted[sorted.length / 2] / 1_000.0;
    }
}
