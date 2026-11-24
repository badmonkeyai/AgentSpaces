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
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
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
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The space's participant fires the sweep's events outside the space's lock (2026-11-23). A listener may take a lock of
 * its own that another thread holds while it reads the space, as a reduce binding's drain does; fired under the space's
 * lock, the two waited on each other for good. The listener here reads the space from a second thread and waits for it
 * a bounded time: under the lock that read cannot finish.
 */
@Timeout(60)
class SweepEventLockTest {

    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-23T00:00:00Z"));
    private PeerNode node;

    @AfterEach
    void tearDown() {
        if (node != null) {
            node.close();
        }
    }

    @Test
    void anExpiryFiredByTheSpacesRunLetsAnotherThreadReadTheSpace() throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        GroupId groupId = GroupId.of("zSweepEventLock");
        GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zSweepEventLock", identity.peerId(), groupId,
                Instant.EPOCH, Duration.ofDays(1), "sweep-lock", GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
        node = PeerNode.builder(identity).clock(time).time(time).randomSeed(3).build();
        node.listen(new SimNetwork().register("a"), "a");
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "tasks", identity, "worker").clock(time)
                .settleWindow(Duration.ZERO).build();
        node.startTicking(Duration.ofMillis(250));
        List<Boolean> readFinished = new CopyOnWriteArrayList<>();
        space.notify(Template.of(TaskEntry.class), (SpaceEvent<TaskEntry> event) -> {
            if (event.kind() != SpaceEvent.Kind.EXPIRED) {
                return;
            }
            Thread reader = Thread.ofPlatform().start(() -> space.readAll(Template.of(TaskEntry.class), 10));
            try {
                readFinished.add(reader.join(Duration.ofSeconds(5)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, Lease.of(Duration.ofHours(1)));
        space.write(new TaskEntry("brief", 1), Lease.of(Duration.ofSeconds(10)));
        time.advance(Duration.ofSeconds(12)); // the lapse falls due: the space's own run sweeps and fires EXPIRED
        System.out.printf("[final] EXPIRED fired by the run: a second thread's read finished %s%n", readFinished);
        assertThat(readFinished).as("the event fired once, outside the space's lock").containsExactly(true);
    }
}
