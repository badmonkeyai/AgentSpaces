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
package ai.badmonkey.agentspaces.space.crdt;

import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.SpaceId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the CRDT laws the spec depends on (§7.3): merge commutativity,
 * associativity, idempotence, order-independent convergence, and monotone
 * completion. Randomized cases are seeded, so every failure reproduces exactly.
 */
class SpaceStateCrdtTest {

    private static final AgentId AGENT = AgentId.parse("zP/writer");
    private static final SpaceId SPACE = SpaceId.local("tasks");

    @Test
    void addedEntryIsLiveAndCarriesItsLease() {
        EntryRecord record = record("e1");
        SpaceStateCrdt state = SpaceStateCrdt.empty()
                .add(record, new Dot("r1", 1), lease(10, "r1", 5_000));

        assertThat(state.liveRecords()).hasSize(1);
        assertThat(state.liveRecords().get(0).lease().expiresAtMillis()).isEqualTo(5_000);
        assertThat(state.state(record.entryId())).isPresent();
        assertThat(state.knownEntries()).isEqualTo(1);
    }

    @Test
    void removeWithdrawsObservedAdds() {
        EntryRecord record = record("e1");
        SpaceStateCrdt state = SpaceStateCrdt.empty()
                .add(record, new Dot("r1", 1), lease(10, "r1", 5_000))
                .remove(record.entryId());

        assertThat(state.liveRecords()).isEmpty();
    }

    @Test
    void concurrentReAddSurvivesAnObservedRemove() {
        // r1 adds, r2 observes and removes; meanwhile r1 re-adds with a new dot.
        // Add-wins: the unobserved add survives the merge.
        EntryRecord record = record("e1");
        SpaceStateCrdt base = SpaceStateCrdt.empty()
                .add(record, new Dot("r1", 1), lease(10, "r1", 5_000));

        SpaceStateCrdt removed = base.remove(record.entryId());
        SpaceStateCrdt readded = base.add(record, new Dot("r1", 2), lease(11, "r1", 6_000));

        assertThat(removed.merge(readded).liveRecords()).hasSize(1);
        assertThat(readded.merge(removed).liveRecords()).hasSize(1);
    }

    @Test
    void completionIsMonotoneAcrossMerges() {
        EntryRecord record = record("e1");
        SpaceStateCrdt base = SpaceStateCrdt.empty()
                .add(record, new Dot("r1", 1), lease(10, "r1", 5_000));

        SpaceStateCrdt completed = base.complete(record.entryId());
        SpaceStateCrdt renewed = base.setLease(record.entryId(), lease(99, "r2", 99_000));

        SpaceStateCrdt merged = completed.merge(renewed);
        assertThat(merged.liveRecords()).isEmpty();
        assertThat(merged.merge(base).liveRecords()).isEmpty();
    }

    @Test
    void leaseMergesByHlcTotalOrder() {
        EntryRecord record = record("e1");
        SpaceStateCrdt a = SpaceStateCrdt.empty()
                .add(record, new Dot("r1", 1), lease(10, "r1", 5_000))
                .setLease(record.entryId(), lease(20, "r1", 7_000));
        SpaceStateCrdt b = SpaceStateCrdt.empty()
                .add(record, new Dot("r1", 1), lease(10, "r1", 5_000))
                .setLease(record.entryId(), lease(15, "r2", 6_000));

        long expiryAb = a.merge(b).liveRecords().get(0).lease().expiresAtMillis();
        long expiryBa = b.merge(a).liveRecords().get(0).lease().expiresAtMillis();

        assertThat(expiryAb).isEqualTo(7_000);
        assertThat(expiryBa).isEqualTo(7_000);
    }

    /** Spec §7.3: tombstones persist for twice the maximum lease after their lease lapses, then garbage collection drops them. */
    @Test
    void completedEntriesAreCollectedAfterTwiceTheMaximumLease() {
        long maxLease = 3_000;
        EntryRecord old = record("old");       // completed; lease lapsed at 5_000
        EntryRecord young = record("young");   // completed; lease lapses at 9_000
        EntryRecord live = record("live");     // present, lease far in the future
        EntryRecord lapsed = record("lapsed"); // present, but its write lease lapsed at 1_000
        SpaceStateCrdt state = SpaceStateCrdt.empty()
                .add(old, new Dot("r1", 1), lease(10, "r1", 5_000)).complete(old.entryId())
                .add(young, new Dot("r1", 2), lease(11, "r1", 9_000)).complete(young.entryId())
                .add(live, new Dot("r1", 3), lease(12, "r1", 50_000))
                .add(lapsed, new Dot("r1", 4), lease(13, "r1", 1_000));

        // One millisecond before the horizon (5_000 + 2 * 3_000) nothing of the old tombstone goes.
        assertThat(state.collectable(10_999, maxLease)).containsExactly(lapsed.entryId());
        assertThat(state.gc(10_999, maxLease).knownEntries()).isEqualTo(3);

        // At the horizon the old tombstone is collected; the younger one and the live entry stay.
        SpaceStateCrdt collected = state.gc(11_000, maxLease);
        assertThat(collected.state(old.entryId())).isEmpty();
        assertThat(collected.state(lapsed.entryId())).isEmpty();
        assertThat(collected.state(young.entryId())).isPresent();
        assertThat(collected.state(live.entryId())).isPresent();
        assertThat(collected.liveRecords()).extracting(EntryRecord::entryId)
                .containsExactly(live.entryId());
        assertThat(SpaceStateCrdt.collectable(state.state(young.entryId()).orElseThrow(),
                11_000, maxLease)).isFalse();
        assertThat(SpaceStateCrdt.collectable(state.state(young.entryId()).orElseThrow(),
                15_000, maxLease)).isTrue();

        // Nothing to collect returns the same instance; merge laws are untouched.
        assertThat(collected.gc(11_000, maxLease)).isSameAs(collected);
        assertThat(collected.merge(collected)).isEqualTo(collected);
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 7, 42, 20260826, 987654321})
    void randomOperationHistoriesConvergeRegardlessOfDeliveryOrder(long seed) {
        Random random = new Random(seed);
        List<UnaryOperator<SpaceStateCrdt>> ops = randomOps(random, 60);

        // One replica applies ops in order; another applies deltas in a shuffled
        // order via merge of singleton applications on the shared base.
        SpaceStateCrdt sequential = apply(SpaceStateCrdt.empty(), ops);

        List<UnaryOperator<SpaceStateCrdt>> shuffled = new ArrayList<>(ops);
        Collections.shuffle(shuffled, new Random(seed ^ 0xBEEF));
        SpaceStateCrdt mergedFromShuffle = SpaceStateCrdt.empty();
        for (UnaryOperator<SpaceStateCrdt> op : shuffled) {
            mergedFromShuffle = mergedFromShuffle.merge(op.apply(SpaceStateCrdt.empty()));
        }
        SpaceStateCrdt mergedInOrder = SpaceStateCrdt.empty();
        for (UnaryOperator<SpaceStateCrdt> op : ops) {
            mergedInOrder = mergedInOrder.merge(op.apply(SpaceStateCrdt.empty()));
        }

        assertThat(mergedFromShuffle).isEqualTo(mergedInOrder);
        // The sequential replica saw each op applied to evolving state, so its
        // remove/complete ops observed more dots; live sets still agree on adds
        // that neither history removed. At minimum the merged replicas converge:
        assertThat(mergedFromShuffle.merge(sequential))
                .isEqualTo(sequential.merge(mergedFromShuffle));
    }

    @ParameterizedTest
    @ValueSource(longs = {3, 99, 1234})
    void mergeIsCommutativeAssociativeAndIdempotent(long seed) {
        Random random = new Random(seed);
        SpaceStateCrdt a = apply(SpaceStateCrdt.empty(), randomOps(random, 25));
        SpaceStateCrdt b = apply(SpaceStateCrdt.empty(), randomOps(random, 25));
        SpaceStateCrdt c = apply(SpaceStateCrdt.empty(), randomOps(random, 25));

        assertThat(a.merge(b)).isEqualTo(b.merge(a));
        assertThat(a.merge(b).merge(c)).isEqualTo(a.merge(b.merge(c)));
        assertThat(a.merge(a)).isEqualTo(a);
        assertThat(a.merge(SpaceStateCrdt.empty())).isEqualTo(a);
    }

    // ------------------------------------------------------------------ helpers

    /** A pool of records shared across generated histories, so ids collide on purpose. */
    private static final List<EntryRecord> POOL = List.of(
            record("p0"), record("p1"), record("p2"), record("p3"), record("p4"));

    private static List<UnaryOperator<SpaceStateCrdt>> randomOps(Random random, int count) {
        List<UnaryOperator<SpaceStateCrdt>> ops = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            EntryRecord record = POOL.get(random.nextInt(POOL.size()));
            EntryId entryId = record.entryId();
            int kind = random.nextInt(10);
            long stamp = random.nextInt(1_000);
            String replica = "r" + random.nextInt(3);
            long counter = i + 1;
            if (kind < 5) {
                ops.add(s -> s.add(record, new Dot(replica, counter),
                        lease(stamp, replica, 1_000 + stamp)));
            } else if (kind < 7) {
                ops.add(s -> s.remove(entryId));
            } else if (kind < 9) {
                ops.add(s -> s.setLease(entryId, lease(stamp, replica, 2_000 + stamp)));
            } else {
                ops.add(s -> s.complete(entryId));
            }
        }
        return ops;
    }

    private static SpaceStateCrdt apply(SpaceStateCrdt state, List<UnaryOperator<SpaceStateCrdt>> ops) {
        for (UnaryOperator<SpaceStateCrdt> op : ops) {
            state = op.apply(state);
        }
        return state;
    }

    private static LwwRegister<LeaseInfo> lease(long physical, String node, long expiry) {
        return new LwwRegister<>(new HlcTimestamp(physical, 0, node),
                new LeaseInfo(AGENT, expiry, LeaseKind.WRITE));
    }

    private static EntryRecord record(String seed) {
        return new EntryRecord(EntryId.of("entry-" + seed), SPACE, "T#v1",
                new byte[]{1, 2}, null, AGENT, new HlcTimestamp(1L, 0, "seed"),
                new LeaseInfo(AGENT, 1_000, LeaseKind.WRITE), Map.of(), null);
    }
}
