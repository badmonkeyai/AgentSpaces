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
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.crypto.GroupKey;
import ai.badmonkey.agentspaces.common.crypto.GroupKeyRing;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.time.Waits;
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

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The space's waits on its time source (srad/agentspaces-event-loop.md EL-C-10, EL-W-01, EL-W-07, §10.17): the settle
 * window, the take's timeout and its looks are reckoned and parked on one base, so on virtual time each bound is exact.
 * Each node runs its own loop on the one {@link VirtualTime}; the takes run as tasks of it ({@link VirtualTime#run}).
 */
@Timeout(120)
class SpaceWaitsTest {

    private static final Duration TICK = Duration.ofMillis(250);
    private static final Lease HOURS_2 = Lease.of(Duration.ofHours(2));

    private final SimNetwork network = new SimNetwork();
    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-22T00:00:00Z"));
    private final GroupId groupId = GroupId.of("zSpaceWaits");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zSpaceWaits",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "space-waits",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final List<PeerNode> nodes = new ArrayList<>();

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private ReplicatedSpace space(String name, Duration settle, GroupKeyRing ring) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).time(time).randomSeed(name.hashCode()).build();
        node.listen(network.register(name), name);
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.<PeerAdvertisement.Endpoint>of());
        ReplicatedSpace.Builder builder = ReplicatedSpace.builder(runtime, "tasks", identity, "worker")
                .settleWindow(settle);
        if (ring != null) {
            builder.keyRing(ring);
        }
        node.startTicking(TICK);
        nodes.add(node);
        return builder.build();
    }

    @Test
    void aTakeSettlesItsWindowOnTheSpacesTimeSource() throws Exception {
        ReplicatedSpace space = space("settle", Duration.ofMillis(200), null);
        space.write(new TaskEntry("job", 1), HOURS_2);
        VirtualTime.Outcome<Optional<TakenEntry<TaskEntry>>> taken = time.run(Duration.ofMillis(10),
                Duration.ofSeconds(5), () -> space.take(Template.of(TaskEntry.class), HOURS_2, Duration.ZERO));
        assertThat(taken.value()).isPresent();
        // decision 12: the settle window is a hard bound in virtual time, not a wall-clock duration to measure
        assertThat(taken.took()).isEqualTo(Duration.ofMillis(200));
    }

    @Test
    void aTakesTimeoutAndItsLooksAreOnOneBaseAndItLooksOnlyAtTheBackstop() throws Exception {
        ReplicatedSpace space = space("timeout", Duration.ZERO, null);
        long polls = space.waitingPolls();
        VirtualTime.Outcome<Optional<TakenEntry<TaskEntry>>> none = time.run(Duration.ofSeconds(1),
                Duration.ofMinutes(5), () -> space.take(Template.of(TaskEntry.class), HOURS_2, Duration.ofMinutes(3)));
        assertThat(none.value()).isEmpty();
        assertThat(none.took()).as("the timeout, exactly, on the space's time source (EL-C-10)")
                .isEqualTo(Duration.ofMinutes(3));
        // the one take-wait rule (EL-W-07): a look at the start, one per 30 s backstop, and the deadline's
        assertThat(space.waitingPolls() - polls).isEqualTo(1 + Duration.ofMinutes(3).dividedBy(Waits.BACKSTOP));
    }

    @Test
    void aWriteWakesAWaitingTakeInTheInstantItLands() throws Exception {
        ReplicatedSpace space = space("wake", Duration.ZERO, null);
        time.schedule(Duration.ofMillis(1_234), () -> space.write(new TaskEntry("late", 2), HOURS_2));
        VirtualTime.Outcome<Optional<TakenEntry<TaskEntry>>> taken = time.run(Duration.ofMillis(1),
                Duration.ofSeconds(10), () -> space.take(Template.of(TaskEntry.class), HOURS_2, Duration.ofMinutes(1)));
        assertThat(taken.value()).isPresent();
        assertThat(taken.took()).isEqualTo(Duration.ofMillis(1_234));
    }

    @Test
    void aClaimLapsingByTheClockEndsTheWaitAtItsExpiry() throws Exception {
        ReplicatedSpace space = space("lapse", Duration.ZERO, null);
        space.write(new TaskEntry("held", 3), HOURS_2);
        Optional<TakenEntry<TaskEntry>> first = space.take(Template.of(TaskEntry.class), Lease.of(Duration.ofSeconds(10)),
                Duration.ZERO);
        assertThat(first).isPresent();
        VirtualTime.Outcome<Optional<TakenEntry<TaskEntry>>> second = time.run(Duration.ofMillis(1),
                Duration.ofSeconds(40), () -> space.take(Template.of(TaskEntry.class), HOURS_2, Duration.ofMinutes(1)));
        assertThat(second.value()).as("the lapsed claim's entry is takeable again").isPresent();
        assertThat(second.took()).as("woken by the expiry index's next due, not the 30 s backstop")
                .isBetween(Duration.ofSeconds(10), Duration.ofSeconds(10).plusMillis(2));
    }

    @Test
    void aKeyInstalledForASealedPayloadSignalsTheSpace() throws Exception {
        GroupKeyRing ring = GroupKeyRing.of(GroupKey.generate());
        ReplicatedSpace space = space("sealed", Duration.ZERO, ring);
        long seen = space.changeSignals();
        ring.install(1, "rotator", GroupKey.generate(), Instant.parse("2026-11-22T00:00:00Z"));
        // EL-W-07: what lets a sealed take succeed signals, so the take waits on changes and the 30 s backstop alone
        assertThat(space.changeSignals()).isGreaterThan(seen);
        space.close();
        long closed = space.changeSignals();
        ring.install(2, "rotator", GroupKey.generate(), Instant.parse("2026-11-22T00:00:00Z"));
        assertThat(space.changeSignals()).as("a closed space stopped listening").isEqualTo(closed);
    }
}
