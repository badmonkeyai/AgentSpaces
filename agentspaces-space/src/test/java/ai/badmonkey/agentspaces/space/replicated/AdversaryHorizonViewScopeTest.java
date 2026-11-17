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
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Adversary of phase 5 of srad/replication-by-entry.md (the per-entry horizon), kept as a regression test. The mode is
 * judged from this replica's own view, but the copies it judges come from writers that need not be in that view: the
 * far side of a partition, or members that left before the heal. The documented remedy for one identity on two nodes
 * is to keep those nodes on the legacy rule, which only helps a replica that knows the writer's setting.
 *
 * <p>Here the two writers keep {@code agentspaces.space.horizon=legacy}, as the hint's javadoc tells them to, and the
 * replica that judges sees neither of them in its view (an empty view, the stand-in for a partition). It has seen a
 * 30-minute lease, so under the legacy rule it keeps the withdrawal for an hour and the longer copy merges into it.
 * Before the fix, the same replica on the per-entry rule collected the withdrawal at L + 5 min (L + 10 min since the
 * drift floor) and took the longer copy back, present and unexpired. The per-entry horizon now applies only to a
 * withdrawal whose writing node is in this replica's view, hinted, and has been there a maximum lease plus the grace,
 * so a writer out of sight keeps the legacy horizon and the withdrawal holds under both settings of the judge.
 */
class AdversaryHorizonViewScopeTest {

    private final List<PeerNode> nodes = new ArrayList<>();

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
        nodes.clear();
    }

    private ReplicatedSpace replica(SimNetwork network, GroupAdvertisement ad, String name, PeerIdentity identity,
                                    TestClock clock, boolean hinted, long seed) throws Exception {
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).horizon(hinted).build();
        nodes.add(node);
        node.listen(network.register(name), name);
        // No seeds: every view is empty, so no replica here has the others in view.
        GroupRuntime runtime = node.joinGroup(ad, GroupMembership.Config.defaults(), List.of());
        return ReplicatedSpace.builder(runtime, "tasks", identity, "worker")
                .clock(clock).settleWindow(Duration.ZERO).build();
    }

    private static GroupAdvertisement group(String id) {
        return new GroupAdvertisement("aspace://" + id, PeerIdentity.generate().peerId(), GroupId.of(id),
                Instant.EPOCH, Duration.ofDays(1), "horizon", GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
    }

    @Test
    void legacyWritersOutsideTheViewNeverBringAWithdrawalBack() throws Exception {
        assertThat(comesBack(false)).as("judging replica on the legacy rule keeps the tombstone an hour").isFalse();
        assertThat(comesBack(true)).as("judging replica on the per-entry rule, view fully hinted (empty), writer out of"
                + " sight").isFalse();
    }

    /**
     * Writers w1 and w2 share one identity and keep the legacy setting. Replica j is hinted or not. j has seen a
     * 30-minute lease of its own, so its legacy horizon is L + 60 min.
     */
    private boolean comesBack(boolean judgeHinted) throws Exception {
        SimNetwork network = new SimNetwork();
        GroupAdvertisement ad = group("zAdvHorizonScope" + judgeHinted);
        TestClock clock = TestClock.create();
        PeerIdentity shared = PeerIdentity.generate();
        ReplicatedSpace w1 = replica(network, ad, "w1", shared, clock, false, 1);
        ReplicatedSpace w2 = replica(network, ad, "w2", shared, clock, false, 2);
        ReplicatedSpace j = replica(network, ad, "j", PeerIdentity.generate(), clock, judgeHinted, 3);

        j.write(new TaskEntry("long", 0), Lease.of(Duration.ofMinutes(30)));
        assertThat(j.maxLeaseMillis()).isEqualTo(Duration.ofMinutes(30).toMillis());
        if (judgeHinted) {
            HorizonSimulationTest.bringIntoForce(clock, j);
            assertThat(j.horizonMode()).isEqualTo(ReplicatedSpace.HorizonMode.PER_ENTRY);
        }

        EntryHandle handle = w1.write(new TaskEntry("x", 1), Lease.of(Duration.ofMinutes(2)));
        EntryId id = handle.entryId();
        w2.applySyncDelta(w1.deltaFor(w2.digest()));
        j.applySyncDelta(w1.deltaFor(j.digest()));
        assertThat(j.recordOf(id)).isPresent();

        // w2 renews for the shared identity, a deployment the hint tells to stay on legacy, and w1 never hears it.
        w2.handleAs(id, shared.agentIdentity("worker")).renew(Duration.ofMinutes(20));
        handle.cancel();
        j.applySyncDelta(w1.deltaFor(j.digest())); // j now holds the withdrawal, L = t + 2 min
        assertThat(j.recordOf(id)).as("withdrawn on j").isEmpty();

        // Past the per-entry horizon, L + 10 min with the drift floor, and far short of L + 60 min: only the writer's
        // scope keeps the withdrawal here, while w2's copy, renewed to 20 minutes, is still live.
        clock.advance(Duration.ofMinutes(13));
        j.sweepNow();
        assertThat(j.signedState(id)).as("the writer is out of j's view, so j keeps the withdrawal").isPresent();

        // The lagging copy arrives, here straight from w2 (in a group, from any member that holds it).
        j.applySyncDelta(w2.deltaFor(j.digest()));
        Optional<EntryRecord> back = j.recordOf(id);
        return back.isPresent() && back.get().lease().expiresAtMillis() > clock.instant().toEpochMilli();
    }
}
