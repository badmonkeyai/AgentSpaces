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
 * lock for the whole {@code PULL_RESP}, and makes before the lock only the signature checks that read nothing of the
 * replica. A completed state's signature is still verified under the lock (verifyState reads the held claim), so a
 * delta of completions holds the lock for one Ed25519 verification per completion, where before phase 1 each was
 * verified outside the lock and a local read, write or take could run between two states. This measures the lock hold
 * per state of a delta of written states and of the same entries completed, from {@code lastBatchLockedNanos}.
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
        // b learns the written entries in one delta: every check is made before the lock.
        ReplicatedSpace b = replica("b");
        byte[] writes = a.deltaFor(b.digest());
        assertThat(CODEC.fromWire(writes, SpaceWire.SyncDelta.class).states()).hasSize(n);
        // warm the JIT on a throwaway replica first, then time b
        ReplicatedSpace warm = replica("w");
        warm.applySyncDelta(writes);
        assertThat(b.applySyncDelta(writes)).isTrue();
        double writeLockedPerState = b.lastBatchLockedNanos() / 1_000.0 / n;

        // a takes and completes every entry; b then learns the claims and the completed states in one delta.
        List<TakenEntry<TaskEntry>> taken = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            taken.add(a.take(Template.of(TaskEntry.class), Lease.of(Duration.ofMinutes(10)), Duration.ZERO)
                    .orElseThrow());
        }
        taken.forEach(a::complete);
        byte[] completions = a.deltaFor(b.digest());
        SpaceWire.SyncDelta sync = CODEC.fromWire(completions, SpaceWire.SyncDelta.class);
        assertThat(sync.states()).hasSize(n);
        assertThat(sync.states()).allMatch(SpaceWire.EntryStateDto::completed);
        warm.applySyncDelta(completions);
        long started = System.nanoTime();
        assertThat(b.applySyncDelta(completions)).isTrue();
        double wholePerState = (System.nanoTime() - started) / 1_000.0 / n;
        double completionLockedPerState = b.lastBatchLockedNanos() / 1_000.0 / n;
        assertThat(b.digest()).isEqualTo(a.digest());

        System.out.printf("RE-M-08 lock hold per state: written %.1f us, completed %.1f us (whole applySyncDelta "
                + "%.1f us per completed state)%n", writeLockedPerState, completionLockedPerState, wholePerState);
        assertThat(completionLockedPerState)
                .as("the lock hold per completed state, against %.1f us per written state", writeLockedPerState)
                .isLessThan(Math.max(10.0, 4 * writeLockedPerState));
    }
}
