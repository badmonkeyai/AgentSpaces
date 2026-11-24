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
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The §7.4 re-offer and the winner's targeted rounds keep their back-off (at once, then 1, 2, 4 ... s) on the space's
 * monotonic base (srad/agentspaces-event-loop.md §10.36, item g of the core round of 2026-11-23). On the wall clock a
 * backward step stalled both for as long as the step, and a forward one spent an offer at once.
 */
@Timeout(60)
class ReofferMonotonicTest {

    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-23T00:00:00Z"));
    private PeerNode node;

    @AfterEach
    void tearDown() {
        if (node != null) {
            node.close();
        }
    }

    private ReplicatedSpace space() throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        GroupId groupId = GroupId.of("zReofferMono");
        GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zReofferMono", identity.peerId(), groupId,
                Instant.EPOCH, Duration.ofDays(1), "reoffer-mono", GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
        node = PeerNode.builder(identity).clock(time).time(time).randomSeed(7).build();
        node.listen(new SimNetwork().register("a"), "a");
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "tasks", identity, "worker").clock(time)
                .settleWindow(Duration.ZERO).build();
        node.startTicking(Duration.ofMillis(250));
        return space;
    }

    private EntryId heldEntry(ReplicatedSpace space) {
        space.write(new TaskEntry("contested", 1), Lease.of(Duration.ofHours(4)));
        Optional<TakenEntry<TaskEntry>> taken = space.take(Template.of(TaskEntry.class), Lease.of(Duration.ofHours(2)),
                Duration.ZERO);
        assertThat(taken).isPresent();
        return taken.get().entryId();
    }

    @Test
    void theTargetedRoundsKeepTheirBackoffAcrossWallSteps() throws Exception {
        ReplicatedSpace space = space();
        EntryId entry = heldEntry(space);
        space.noteDisplacedHolderForTest(entry, PeerIdentity.generate().peerId());
        time.runDue();
        assertThat(space.displacedPullRounds()).as("the first round at once").isEqualTo(1);
        time.stepWall(Duration.ofHours(-1)); // NTP steps the clock back an hour
        time.advance(Duration.ofMillis(1_010));
        assertThat(space.displacedPullRounds()).as("1 s later, the step notwithstanding").isEqualTo(2);
        time.stepWall(Duration.ofMinutes(30)); // and forward half an hour: no round falls due early
        time.advance(Duration.ofMillis(1_900));
        assertThat(space.displacedPullRounds()).as("the 2 s wait is not cut short").isEqualTo(2);
        time.advance(Duration.ofMillis(110));
        assertThat(space.displacedPullRounds()).isEqualTo(3);
        time.advance(Duration.ofSeconds(4));
        assertThat(space.displacedPullRounds()).isEqualTo(4);
    }

    @Test
    void theReofferKeepsItsBackoffAcrossWallSteps() throws Exception {
        ReplicatedSpace space = space();
        EntryId entry = heldEntry(space);
        space.noteDisplacedOwnForTest(entry);
        assertThat(space.displacedReoffers()).as("the first offer at once").isEqualTo(1);
        time.stepWall(Duration.ofHours(-1));
        time.advance(Duration.ofMillis(1_010));
        space.staleOfferForTest(entry);
        assertThat(space.displacedReoffers()).as("1 s later, the step notwithstanding").isEqualTo(2);
        time.stepWall(Duration.ofMinutes(30));
        space.staleOfferForTest(entry);
        assertThat(space.displacedReoffers()).as("a forward step spends no offer early").isEqualTo(2);
        time.advance(Duration.ofMillis(2_010));
        space.staleOfferForTest(entry);
        assertThat(space.displacedReoffers()).isEqualTo(3);
    }
}
