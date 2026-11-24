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
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Subscription;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import ai.badmonkey.agentspaces.test.Fixtures;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A subscription's lease is local, so it is judged on the monotonic base its renewer runs on (time adversary of phase 3,
 * srad/agentspaces-event-loop.md §10.18, EL-C-04 and EL-W-02). The binder renews a 1-hour subscription every 30 minutes
 * as a task on the monotonic base; before the fix the lease was judged on wall time, so a 2-hour laptop sleep (§2.5
 * scenario 3) or a forward NTP step past half the lease lapsed the subscription at the next event, the renewal threw,
 * and the agent stopped reacting for good.
 */
@Timeout(60)
class SubscriptionTimeBaseTest {

    private static final Duration TICK = Duration.ofMillis(250);
    private static final Lease HOUR = Lease.of(Duration.ofHours(1));
    private static final Lease HOURS_4 = Lease.of(Duration.ofHours(4));

    private final SimNetwork network = new SimNetwork();
    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-22T00:00:00Z"));
    private final GroupId groupId = GroupId.of("zSubscriptionBase");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zSubscriptionBase",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "subscription-base",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final List<PeerNode> nodes = new ArrayList<>();

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private ReplicatedSpace space(VirtualTime.NodeTime view) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).time(view).randomSeed(7).build();
        node.listen(network.register("solo"), "solo");
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.<PeerAdvertisement.Endpoint>of());
        node.startTicking(TICK);
        nodes.add(node);
        return ReplicatedSpace.builder(runtime, "tasks", identity, "worker").settleWindow(Duration.ZERO).build();
    }

    private static Subscription watch(Space space, AtomicInteger seen) {
        return space.notify(Template.of(TaskEntry.class), event -> seen.incrementAndGet(), HOUR);
    }

    @Test
    void aTwoHourSuspendLapsesNoSubscription() throws Exception {
        VirtualTime.NodeTime view = time.unwatchedNode();
        ReplicatedSpace space = space(view);
        AtomicInteger seen = new AtomicInteger();
        Subscription sub = watch(space, seen);
        time.advance(Duration.ofMinutes(10));

        view.suspend();
        time.advance(Duration.ofHours(2));
        view.resume();
        time.advance(TICK);

        space.write(new TaskEntry("after-sleep", 1), HOURS_4);
        time.advance(TICK);
        assertThat(seen.get()).as("the subscription still delivers after the sleep").isEqualTo(1);
        assertThatCode(() -> sub.renew(HOUR.duration())).as("its renewer can still renew it").doesNotThrowAnyException();
    }

    @Test
    void aForwardWallStepLapsesNoSubscription() throws Exception {
        VirtualTime.NodeTime view = time.unwatchedNode();
        ReplicatedSpace space = space(view);
        AtomicInteger seen = new AtomicInteger();
        Subscription sub = watch(space, seen);

        view.stepWall(Duration.ofMinutes(90));
        time.advance(TICK);

        space.write(new TaskEntry("after-step", 2), HOURS_4);
        time.advance(TICK);
        assertThat(seen.get()).isEqualTo(1);
        assertThatCode(() -> sub.renew(HOUR.duration())).doesNotThrowAnyException();
    }

    @Test
    void anUnrenewedSubscriptionStillLapsesAtItsLease() throws Exception {
        VirtualTime.NodeTime view = time.unwatchedNode();
        ReplicatedSpace space = space(view);
        AtomicInteger seen = new AtomicInteger();
        Subscription sub = watch(space, seen);

        time.advance(Duration.ofMinutes(61));
        space.write(new TaskEntry("late", 3), HOURS_4);
        time.advance(TICK);
        assertThat(seen.get()).as("the lease still bounds an unrenewed subscription").isZero();
        assertThatThrownBy(() -> sub.renew(HOUR.duration())).isInstanceOf(
                ai.badmonkey.agentspaces.api.error.LeaseExpiredException.class);
    }

    @Test
    void aLocalSpacesSubscriptionSurvivesAForwardWallStep() {
        VirtualTime.NodeTime view = time.node();
        try (LocalSpace space = LocalSpace.builder("tasks", Fixtures.agent("watcher")).clock(view).build()) {
            AtomicInteger seen = new AtomicInteger();
            Subscription sub = watch(space, seen);
            view.stepWall(Duration.ofMinutes(90));
            space.write(new TaskEntry("after-step", 4), HOURS_4);
            assertThat(seen.get()).isEqualTo(1);
            assertThatCode(() -> sub.renew(HOUR.duration())).doesNotThrowAnyException();
        }
    }
}
