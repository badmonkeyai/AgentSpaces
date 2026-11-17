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
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.crdt.Dot;
import ai.badmonkey.agentspaces.space.crdt.EntryState;
import ai.badmonkey.agentspaces.space.crdt.LwwRegister;
import ai.badmonkey.agentspaces.space.crdt.SpaceStateCrdt;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a probe and an anti-entropy round cost on a space that has not changed (reviews/IDLE-WORK-2026-11-13.md
 * items 3 and 4). A JMH-free timing: each size seeds a space in one merge, builds its digest once, and then times the
 * repeated calls a quiet group makes, the digest and the hash a probe carries. Before the expiry index every call swept
 * the whole state first, so the cost grew with the entry count; with it an unchanged space answers from its caches.
 * The timings are printed for the record, and the assertions are structural (no rebuild, no sweep visit) with only a
 * loose bound on time, so a loaded build machine cannot fail it.
 */
class DigestScalingTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final GroupId groupId = GroupId.of("zBench");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zBench", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "bench",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private PeerIdentity identity;
    private PeerNode node;
    private GroupRuntime runtime;

    @BeforeEach
    void setUp() throws Exception {
        identity = PeerIdentity.generate();
        node = PeerNode.builder(identity).clock(clock).randomSeed(1).build();
        node.listen(network.register("bench"), "bench");
        runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
    }

    @AfterEach
    void tearDown() {
        node.close();
    }

    @Test
    void anUnchangedSpaceAnswersAProbeWithoutScanningItsEntries() {
        Map<Integer, double[]> micros = new LinkedHashMap<>();
        for (int n : new int[] {1_000, 10_000, 100_000}) {
            ReplicatedSpace space = ReplicatedSpace.builder(runtime, "bench-" + n, identity, "worker")
                    .clock(clock)
                    .settleWindow(Duration.ZERO)
                    .build();
            space.mergeForTest(seed(n, space));
            space.digest(); // the one rebuild
            long rebuilds = space.digestComputations();
            long visits = space.sweepVisits();
            for (int i = 0; i < 50; i++) {
                space.digest(); // warm up
                probeHash(space);
            }
            int rounds = 200;
            long started = System.nanoTime();
            for (int i = 0; i < rounds; i++) {
                space.digest();
            }
            double digestMicros = (System.nanoTime() - started) / 1_000.0 / rounds;
            started = System.nanoTime();
            for (int i = 0; i < rounds; i++) {
                probeHash(space);
            }
            double probeMicros = (System.nanoTime() - started) / 1_000.0 / rounds;
            micros.put(n, new double[] {digestMicros, probeMicros});
            assertThat(space.digestComputations()).as("an unchanged space never rebuilds its digest").isEqualTo(rebuilds);
            assertThat(space.sweepVisits()).as("nothing is due, so no digest visits an entry").isEqualTo(visits);
            space.close();
        }
        StringBuilder report = new StringBuilder("digest scaling (microseconds per call on an unchanged space):");
        micros.forEach((n, m) -> report.append(String.format("%n  n=%,7d  digest() %10.2f  probe hash %10.2f", n, m[0], m[1])));
        System.out.println(report);
        // Flat in n: the hundred-thousand-entry space costs about what the thousand-entry one does. Loose, for a busy
        // machine: the full sweep this replaces cost milliseconds per call at that size.
        assertThat(micros.get(100_000)[1]).isLessThan(Math.max(20 * micros.get(1_000)[1], 500.0));
    }

    /** The hash a probe carries for one stream, as the gossip bus asks for it. */
    private static byte[] probeHash(ReplicatedSpace space) {
        return space.digestHash();
    }

    /** n entries, half of them withdrawn, merged pairwise so seeding costs n log n, not n squared. */
    private SpaceStateCrdt seed(int n, ReplicatedSpace space) {
        AgentId issuer = identity.agentIdentity("worker").id();
        long now = clock.instant().toEpochMilli();
        List<SpaceStateCrdt> parts = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            EntryId entryId = EntryId.newId();
            LeaseInfo lease = new LeaseInfo(issuer, now + Duration.ofHours(1).toMillis(), LeaseKind.WRITE);
            HlcTimestamp stamp = new HlcTimestamp(now, 0, issuer.encoded());
            EntryRecord record = new EntryRecord(entryId, space.id(), "bench#1", new byte[] {1}, null, issuer, stamp,
                    lease, Map.of(), new byte[0]);
            EntryState state = EntryState.added(record, new Dot("bench", i + 1), new LwwRegister<>(stamp, lease));
            parts.add(SpaceStateCrdt.of(entryId, i % 2 == 0 ? state : state.withObservedRemoved()));
        }
        while (parts.size() > 1) {
            List<SpaceStateCrdt> next = new ArrayList<>((parts.size() + 1) / 2);
            for (int i = 0; i < parts.size(); i += 2) {
                next.add(i + 1 < parts.size() ? parts.get(i).merge(parts.get(i + 1)) : parts.get(i));
            }
            parts = next;
        }
        return parts.get(0);
    }
}
