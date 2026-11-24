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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The self-advertisement in the same-millisecond audit (srad/agentspaces-event-loop.md §10.16): a self ad whose content
 * changed in the millisecond of the last one is issued a millisecond later, so it reaches the group at once. Its gossip
 * item id is the issue millisecond, which every member had seen, and a member keeps the card it holds on an equal issue
 * instant; before the fix the change waited for the 30 s refresh. On virtual time the clock does not move between the
 * two changes. The class timeout is a hang guard only.
 */
@Timeout(60)
class SelfAdIssueTest {

    private final SimNetwork network = new SimNetwork();
    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-22T00:00:00Z"));
    private final GroupId groupId = GroupId.of("zSelfAdIssue");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zSelfAdIssue",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "self-ad-issue",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private PeerNode a;
    private PeerNode b;

    @AfterEach
    void tearDown() {
        if (a != null) {
            a.close();
        }
        if (b != null) {
            b.close();
        }
    }

    @Test
    void aSelfAdChangedInTheMillisecondOfTheLastOneReachesTheGroupAtOnce() throws Exception {
        Map<PeerId, Map<String, String>> hints = new ConcurrentHashMap<>();
        a = PeerNode.builder(PeerIdentity.generate()).time(time).randomSeed(1).build();
        b = PeerNode.builder(PeerIdentity.generate()).time(time).randomSeed(2)
                .onCredentialHints((peer, map) -> hints.put(peer, Map.copyOf(map))).build();
        a.listen(network.register("a"), "a");
        b.listen(network.register("b"), "b");
        GroupRuntime ra = a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime rb = b.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        a.startTicking(Duration.ofMillis(250));
        b.startTicking(Duration.ofMillis(250));
        for (int i = 0; i < 400 && (ra.membership().allMembers().isEmpty() || rb.membership().allMembers().isEmpty());
             i++) {
            time.advance(Duration.ofMillis(250));
        }
        time.advance(Duration.ofSeconds(5));

        a.credentialHint("tier", "gold");
        time.runDue();
        assertThat(hints.get(a.peerId())).containsEntry("tier", "gold");

        a.credentialHint("tier", "platinum"); // the clock has not moved: the millisecond of the last self ad
        time.runDue();
        time.advance(Duration.ofSeconds(1)); // well inside the 30 s refresh
        assertThat(hints.get(a.peerId())).as("the second change reached the member at once")
                .containsEntry("tier", "platinum");
    }
}
