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
import ai.badmonkey.agentspaces.api.ad.SpaceAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
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
 * srad/agentspaces-event-loop.md §10.33, found by the Babashka spirit review and confirmed in the core: a replicated
 * space published its advertisement (15 min TTL) once, when built, and nothing renewed it, so 15 minutes later the
 * space left every discovery cache (semantic discovery, the console view) on every host, the Spring starter's
 * included. Principle 8 of §3.11: what a peer judges by a TTL is renewed inside it. REPAIRED: the space's own
 * participant re-publishes at two thirds of the TTL, so the advertisement in force never lapses, and a forward wall
 * step (a laptop's wake) re-publishes at once.
 */
@Timeout(120)
class SpaceAdRenewalTest {

    private final SimNetwork network = new SimNetwork();
    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-23T00:00:00Z"));
    private final GroupId groupId = GroupId.of("zSpaceAdRenewal");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zSpaceAdRenewal",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "space-ad",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private PeerNode node;

    @AfterEach
    void tearDown() {
        if (node != null) {
            node.close();
        }
    }

    @Test
    void anAdvertisedSpaceRenewsItsAdvertisementInsideItsTtlAndAtAForwardWallStep() throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        node = PeerNode.builder(identity).clock(time).time(time).randomSeed(33).build();
        node.listen(network.register("a"), "a");
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        List<SpaceAdvertisement> published = new CopyOnWriteArrayList<>();
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "tasks", identity, "worker").clock(time)
                .settleWindow(Duration.ZERO)
                .advertise(signed -> published.add((SpaceAdvertisement) signed.advertisement()))
                .build();
        node.startTicking(Duration.ofMillis(250));
        time.runDue();
        assertThat(published).as("published once when built").hasSize(1);
        int lapsedMinutes = 0;
        for (int minute = 1; minute <= 40; minute++) {
            time.advance(Duration.ofMinutes(1));
            SpaceAdvertisement last = published.get(published.size() - 1);
            if (!last.issued().plus(last.ttl()).isAfter(time.instant())) {
                lapsedMinutes++;
            }
        }
        System.out.printf("[space-ad] 40 idle minutes: %d advertisements, %d minutes lapsed, %d renewals%n",
                published.size(), lapsedMinutes, space.adRenewals());
        assertThat(lapsedMinutes).as("the advertisement in force never lapsed in 40 minutes").isZero();
        assertThat(published.size()).as("renewed at 10, 20, 30 and 40 minutes").isEqualTo(5);
        int before = published.size();
        time.stepWall(Duration.ofHours(2)); // a laptop wakes two hours on
        time.advance(Duration.ofMillis(5));
        assertThat(published.size()).as("a forward wall step re-publishes at once").isEqualTo(before + 1);
        SpaceAdvertisement last = published.get(published.size() - 1);
        assertThat(last.issued().plus(last.ttl())).isAfter(time.instant());
        space.close();
    }
}
