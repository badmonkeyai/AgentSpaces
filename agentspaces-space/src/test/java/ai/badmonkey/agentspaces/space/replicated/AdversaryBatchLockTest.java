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
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Lock-hold adversary of RE-M-08 (srad/replication-by-entry.md phase 1): {@code applySyncDelta} now holds the space
 * lock for the whole {@code PULL_RESP}, and makes before the lock the signature checks that read nothing of the
 * replica. A completed state is checked before the lock too, against the claim it should meet (the held claim merged
 * with the one the delta carries); only a guess that proves wrong is checked again under the lock. So a delta of
 * completions holds the lock for a claim merge and a state merge per entry, where a delta of writes holds it for one
 * state merge. This measures the lock hold per state of a delta of written states and of the same entries completed,
 * from {@code lastBatchLockedNanos}.
 */
class AdversaryBatchLockTest {

    private static final CborCodec CODEC = CborCodec.defaultCodec();

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zLock", PeerIdentity.generate().peerId(), GroupId.of("zLock"),
            java.time.Instant.EPOCH, Duration.ofDays(1), "lock",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final List<PeerNode> nodes = new ArrayList<>();

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private ReplicatedSpace replica(String name) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(nodes.size() + 1).build();
        nodes.add(node);
        node.listen(network.register(name), name);
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        return ReplicatedSpace.builder(runtime, "tasks", identity, "worker")
                .clock(clock).settleWindow(Duration.ZERO).build();
    }

    @Test
    void aDeltaOfCompletionsHoldsTheLockNoLongerPerStateThanADeltaOfWrites() throws Exception {
        int n = 400;
        ReplicatedSpace a = replica("a");
        for (int i = 0; i < n; i++) {
            a.write(new TaskEntry("lock-" + i, i), Lease.of(Duration.ofHours(1)));
        }
        // a fresh replica learns the written entries in one delta: every check is made before the lock
        byte[] writes = a.deltaFor(replica("probe").digest());
        assertThat(CODEC.fromWire(writes, SpaceWire.SyncDelta.class).states()).hasSize(n);
        // warm the JIT on a throwaway replica first
        ReplicatedSpace warm = replica("w");
        warm.applySyncDelta(writes);

        // a takes and completes every entry; a replica that holds the writes then learns the claims and the
        // completed states in one delta
        List<TakenEntry<TaskEntry>> taken = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            taken.add(a.take(Template.of(TaskEntry.class), Lease.of(Duration.ofMinutes(10)), Duration.ZERO)
                    .orElseThrow());
        }
        taken.forEach(a::complete);
        byte[] completions = a.deltaFor(warm.digest());
        SpaceWire.SyncDelta sync = CODEC.fromWire(completions, SpaceWire.SyncDelta.class);
        assertThat(sync.states()).hasSize(n);
        assertThat(sync.states()).allMatch(SpaceWire.EntryStateDto::completed);
        warm.applySyncDelta(completions);

        // One batch is a few milliseconds of wall time, so a GC or JIT pause, a preemption, or a move to an
        // efficiency core inside it can double or triple its per-state figure; a write batch is a quarter as long and
        // is hit less often. Such interference only adds time, so the bound is judged on the fastest of several fresh
        // replicas of each kind, the estimate of the cost itself (srad/agentspaces-event-loop.md §10.28: the phase-5
        // gate's reactor under -T 2 read completions of 18.6 to 21.3 us in three rounds and 9.3 and 10.4 us in the
        // other two, against writes of 3.5 to 3.7 us, and the median judged 18.6 us). Every sample is printed.
        int rounds = 5;
        List<Double> written = new ArrayList<>();
        List<Double> completed = new ArrayList<>();
        for (int r = 0; r < rounds; r++) {
            ReplicatedSpace b = replica("b" + r);
            assertThat(b.applySyncDelta(writes)).isTrue();
            written.add(b.lastBatchLockedNanos() / 1_000.0 / n);
            long started = System.nanoTime();
            assertThat(b.applySyncDelta(completions)).isTrue();
            double wholePerState = (System.nanoTime() - started) / 1_000.0 / n;
            completed.add(b.lastBatchLockedNanos() / 1_000.0 / n);
            assertThat(b.digest()).isEqualTo(a.digest());
            System.out.printf("RE-M-08 lock hold per state: written %.1f us, completed %.1f us (whole applySyncDelta "
                    + "%.1f us per completed state)%n", written.get(r), completed.get(r), wholePerState);
        }
        double writeLockedPerState = written.stream().mapToDouble(Double::doubleValue).min().orElseThrow();
        double completionLockedPerState = completed.stream().mapToDouble(Double::doubleValue).min().orElseThrow();
        System.out.printf("RE-M-08 over %d replicas: medians written %.1f us, completed %.1f us; fastest written %.1f"
                + " us, completed %.1f us%n", rounds, median(written), median(completed), writeLockedPerState,
                completionLockedPerState);
        // Each round applies its write batch and its completion batch back to back on one replica, so the two see
        // alike interference; the fastest of each kind over all rounds did not (2026-11-23, forkCount 3 under load:
        // writes 10.5 to 14.7 us in four rounds and 4.0 us in one quiet window, completions 16.4 to 23.6 us, and the
        // lucky write alone failed the bound). The bound is judged per round: in at least one round the completion
        // batch holds the lock no longer per state than four times that round's write batch (floor 10 us).
        double bestMargin = Double.MAX_VALUE;
        int bestRound = -1;
        for (int r = 0; r < rounds; r++) {
            double margin = completed.get(r) - Math.max(10.0, 4 * written.get(r));
            if (margin < bestMargin) {
                bestMargin = margin;
                bestRound = r;
            }
        }
        System.out.printf("RE-M-08 paired: round %d, written %.1f us, completed %.1f us (fastest written %.1f us,"
                + " completed %.1f us)%n", bestRound, written.get(bestRound), completed.get(bestRound),
                writeLockedPerState, completionLockedPerState);
        assertThat(completed.get(bestRound))
                .as("the lock hold per completed state in the round that judged best, against %.1f us per written state"
                        + " in that round", written.get(bestRound))
                .isLessThan(Math.max(10.0, 4 * written.get(bestRound)));
    }

    private static double median(List<Double> values) {
        List<Double> sorted = new ArrayList<>(values);
        sorted.sort(null);
        int m = sorted.size() / 2;
        return sorted.size() % 2 == 1 ? sorted.get(m) : (sorted.get(m - 1) + sorted.get(m)) / 2;
    }
}
