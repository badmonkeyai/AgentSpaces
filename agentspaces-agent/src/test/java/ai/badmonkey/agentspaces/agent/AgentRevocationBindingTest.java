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
package ai.badmonkey.agentspaces.agent;

import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.api.ad.CredentialRevocation;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.Fixtures.FindingEntry;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.InstantSource;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SPEC §5.6 v0.1.13 (TODO-9-10-11 C4): revoking a local agent unbinds it. Its
 * take loop stops, so it handles nothing further, while a sibling agent on
 * the same peer keeps working.
 */
class AgentRevocationBindingTest {

    private final InstantSource clock = InstantSource.system();
    private final PeerIdentity identity = PeerIdentity.generate();
    private final GroupId groupId = GroupId.of("zRevokeBinding");
    private final PeerNode node = PeerNode.builder(identity).clock(clock).build();
    private final AgentSpaces spaces = new AgentSpaces(identity, clock,
            name -> identity.renewingSubordinate(name, Duration.ofDays(1), clock));

    @AfterEach
    void tearDown() {
        spaces.close();
        node.close();
    }

    @AgentSpec(name = "researcher", description = "Researches topics", goals = {"research"})
    public static class Researcher {
        @SpaceTake(space = "tasks", lease = "PT1M", pollTimeout = "PT0.1S", resultSpace = "findings")
        public FindingEntry research(TaskEntry task) {
            return new FindingEntry(task.topic(), "researched");
        }
    }

    @AgentSpec(name = "reviewer", description = "Reviews topics", goals = {"review"})
    public static class Reviewer {
        @SpaceTake(space = "reviews", lease = "PT1M", pollTimeout = "PT0.1S", resultSpace = "findings")
        public FindingEntry review(TaskEntry task) {
            return new FindingEntry(task.topic(), "reviewed");
        }
    }

    @Test
    void aRevokedLocalAgentIsUnboundAndItsSiblingKeepsWorking() {
        GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zRevokeBinding", identity.peerId(),
                groupId, java.time.Instant.EPOCH, Duration.ofDays(1), "fleet",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        CborCodec codec = CborCodec.defaultCodec();
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, clock), codec,
                identity.peerId());
        AgentSpaces.GroupContext group = spaces.register("fleet", groupId, runtime, discovery);
        for (String name : List.of("tasks", "reviews", "findings")) {
            group.space(name, ReplicatedSpace.builder(runtime, name, identity, "app")
                    .clock(clock).settleWindow(Duration.ZERO).build());
        }
        AgentBinder.Bound researcher = group.bind(new Researcher());
        AgentBinder.Bound reviewer = group.bind(new Reviewer());
        Lease lease = Lease.of(Duration.ofMinutes(5));
        group.space("tasks").write(new TaskEntry("first", 1), lease);
        assertThat(group.space("findings").read(Template.of(FindingEntry.class), Duration.ofSeconds(5)))
                .isPresent();

        assertThat(group.revokeAgent(identity.agent("researcher"), CredentialRevocation.RETIRED)).isPresent();
        assertThat(researcher.isRunning()).as("unbound as the revocation landed").isFalse();
        assertThat(reviewer.isRunning()).isTrue();
        group.space("tasks").write(new TaskEntry("second", 2), lease);
        group.space("reviews").write(new TaskEntry("third", 3), lease);
        assertThat(group.space("findings").read(Template.of(FindingEntry.class)
                        .where("topic", ai.badmonkey.agentspaces.api.space.Matchers.eq("third")),
                Duration.ofSeconds(5))).as("the sibling still works").isPresent();
        assertThat(group.space("findings").readAll(Template.of(FindingEntry.class), 10))
                .extracting(FindingEntry::topic).as("the revoked agent handled nothing more")
                .containsExactlyInAnyOrder("first", "third");
        assertThat(group.space("tasks").read(Template.of(TaskEntry.class))).as("still waiting")
                .contains(new TaskEntry("second", 2));

        // The facade's general revoke (any target, back-dated) and its peer revoke.
        assertThat(group.revoke(CredentialRevocation.Target.agent(identity.agent("reviewer")),
                CredentialRevocation.RETIRED, clock.instant().minusSeconds(1))).isPresent();
        assertThat(reviewer.isRunning()).as("unbound through the general revoke").isFalse();
        ai.badmonkey.agentspaces.common.id.PeerId stranger = PeerIdentity.generate().peerId();
        assertThat(group.revokePeer(stranger, "compromised")).as("the founder is the trust root").isPresent();
        assertThat(runtime.revocationView().revoked(stranger)).isTrue();
    }
}
