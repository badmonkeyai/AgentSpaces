/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
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
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * ISSUE-AppCardOriginFormats FR-6 (SPEC §6.1a.2, §9; AppCard origin and formats, 2026-11-09): a node advertises
 * {@code wire.appcard=2}, reads every member's level from its advertisement as canonical decimal text, and reports
 * whether its whole view is at level 2; a member whose advertisement has not arrived counts as level 1.
 */
class AppCardHintTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<GroupRuntime> runtimes = new ArrayList<>();
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zHint", PeerIdentity.generate().peerId(),
            GroupId.of("zHint"), Instant.EPOCH, Duration.ofDays(1), "hint", GroupAdvertisement.MembershipPolicy.OPEN,
            ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private GroupRuntime node(String address, int level) throws IOException {
        return node(address, level, PeerIdentity.generate());
    }

    private GroupRuntime node(String address, int level, PeerIdentity identity) throws IOException {
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(nodes.size() + 7L).appCardLevel(level).build();
        node.listen(network.register(address), address);
        nodes.add(node);
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                nodes.size() == 1 ? List.of() : List.of(new PeerAdvertisement.Endpoint("mem", "h0", 0)));
        runtimes.add(runtime);
        return runtime;
    }

    private boolean until(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(8).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10);
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofMillis(500));
        }
        return condition.getAsBoolean();
    }

    @Test
    void theLevelTravelsInTheAdvertisementAndGatesTheView() throws Exception {
        GroupRuntime a = node("h0", 2);
        assertThat(nodes.get(0).wireHints()).containsEntry(PeerNode.APPCARD_HINT, "2");
        assertThat(a.viewAtAppCardLevel(2)).as("an empty view at level 2 reads the fields").isTrue();
        GroupRuntime b = node("h1", 2);
        assertThat(until(() -> a.membership().allMembers().size() == 1 && b.membership().allMembers().size() == 1)).isTrue();
        assertThat(until(() -> a.viewAtAppCardLevel(2) && b.viewAtAppCardLevel(2))).isTrue();
        assertThat(nodes.get(0).appCardLevel(nodes.get(1).peerId())).isEqualTo(2);

        node("h2", 1);
        assertThat(nodes.get(2).wireHints()).doesNotContainKey(PeerNode.APPCARD_HINT);
        assertThat(until(() -> a.membership().allMembers().size() == 2)).isTrue();
        assertThat(a.viewAtAppCardLevel(2)).as("a level-1 member closes the gate").isFalse();
        assertThat(runtimes.get(2).viewAtAppCardLevel(2)).as("a level-1 node writes no gated field").isFalse();
        assertThat(a.viewAtAppCardLevel(1)).isTrue();
    }

    @Test
    void aMemberThatDropsTheHintOrIsForgottenOnLeaveCountsAsLevelOne() throws Exception {
        // FR-6 (AppCard origin and formats, 2026-11-09): only the member's current advertisement gives it level 2
        GroupRuntime a = node("h0", 2);
        PeerIdentity restarting = PeerIdentity.generate();
        node("h1", 2, restarting);
        PeerId b = restarting.peerId();
        assertThat(until(() -> nodes.get(0).appCardLevel(b) == 2)).isTrue();
        nodes.remove(1).close();
        node("h1b", 1, restarting);
        assertThat(until(() -> nodes.get(0).appCardLevel(b) == 1)).as("the restarted member's advertisement drops the hint").isTrue();

        node("h2", 2);
        PeerId c = nodes.get(nodes.size() - 1).peerId();
        assertThat(until(() -> nodes.get(0).appCardLevel(c) == 2)).isTrue();
        assertThat(nodes.get(0).leaveGroup(a.id())).isTrue();
        assertThat(nodes.get(0).appCardLevel(c)).as("leaving forgets the member's level with its other hints").isEqualTo(1);
    }

    @Test
    void theHintIsCanonicalDecimalReadAsLevelNOrAbove() {
        assertThat(PeerNode.appCardLevelOf(null)).isEqualTo(1);
        assertThat(PeerNode.appCardLevelOf("")).isEqualTo(1);
        assertThat(PeerNode.appCardLevelOf("2")).isEqualTo(2);
        assertThat(PeerNode.appCardLevelOf("3")).isEqualTo(3);
        assertThat(PeerNode.appCardLevelOf("10")).isEqualTo(10);
        assertThat(PeerNode.appCardLevelOf("99999999999999999999")).isEqualTo(Integer.MAX_VALUE);
        assertThat(PeerNode.appCardLevelOf("02")).as("no leading zero").isEqualTo(1);
        assertThat(PeerNode.appCardLevelOf("+2")).isEqualTo(1);
        assertThat(PeerNode.appCardLevelOf("2.0")).isEqualTo(1);
        assertThat(PeerNode.appCardLevelOf(" 2")).isEqualTo(1);
        assertThat(PeerNode.appCardLevelOf("0")).isEqualTo(1);
        assertThat(PeerNode.appCardLevelOf("true")).isEqualTo(1);
        assertThatThrownBy(() -> PeerNode.builder(PeerIdentity.generate()).appCardLevel(3)).hasMessageContaining("levels 1 to 2");
        assertThatThrownBy(() -> PeerNode.builder(PeerIdentity.generate()).appCardLevel(0)).hasMessageContaining("levels 1 to 2");
    }
}
