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
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Lost members back off and retire; the retirement list is the opt-in last resort
 * (reviews/LOST-MEMBER-RETIREMENT-2026-11-18.md). Every scenario runs on the test clock, one tick a second.
 */
class RetirementTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zRetire");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zRetire",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "retire",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode node(String address, long seed, UnaryOperator<PeerNode.Builder> options) throws IOException {
        return node(address, seed, PeerIdentity.generate(), options);
    }

    private PeerNode node(String address, long seed, PeerIdentity identity, UnaryOperator<PeerNode.Builder> options)
            throws IOException {
        PeerNode node = options.apply(PeerNode.builder(identity).clock(clock).randomSeed(seed)).build();
        node.listen(network.register(address), address);
        nodes.add(node);
        return node;
    }

    private GroupRuntime join(PeerNode node, String... seeds) {
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String s : seeds) {
            endpoints.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        return node.joinGroup(groupAd, GroupMembership.Config.defaults(), endpoints);
    }

    private void seconds(int n) {
        for (int i = 0; i < n; i++) {
            for (PeerNode node : List.copyOf(nodes)) {
                node.tick();
            }
            clock.advance(Duration.ofSeconds(1));
        }
    }

    private static int members(GroupRuntime runtime) {
        return runtime.membership().allMembers().size();
    }

    @Test
    void aLostMemberIsRetriedOnItsOwnBackOffAndRetires() throws Exception {
        PeerNode a = node("a", 1, b -> b.retireAfter(Duration.ofMinutes(20)));
        PeerNode b = node("b", 2, UnaryOperator.identity());
        GroupRuntime ra = join(a);
        join(b, "a");
        seconds(10);
        assertThat(members(ra)).isEqualTo(1);
        PeerId lostPeer = b.peerId();
        network.partition("a", "b"); // b is gone, as far as a can tell

        seconds(15 * 60);
        long in15 = a.lostIntroductions();
        // 5, 10, 20, 40 s, then every minute for the first hour (each spread a quarter either way): about eighteen in
        // 15 minutes; the group-wide 5-to-30 s schedule it replaces made about thirty in its ten-minute window
        assertThat(in15).as("introductions to the lost member in 15 min").isBetween(13L, 23L);
        assertThat(a.retired(groupId)).isEmpty();

        seconds(6 * 60);
        assertThat(a.retired(groupId)).as("retired 20 min after its loss").containsExactly(lostPeer);
        assertThat(a.retirements()).isEqualTo(1);
        long atRetirement = a.lostIntroductions();
        seconds(10 * 60);
        assertThat(a.lostIntroductions()).as("a retired member is not retried").isEqualTo(atRetirement);
        assertThat(a.lastResortIntroductions()).as("post-retirement is off by default").isZero();
        assertThat(a.isolated(groupId)).as("alone, with nothing left to try").isTrue();
    }

    @Test
    void withRetirementOffALostMemberIsForgottenAfterTenMinutesAsBefore() throws Exception {
        PeerNode a = node("a", 1, b -> b.retirement(false));
        PeerNode b = node("b", 2, UnaryOperator.identity());
        GroupRuntime ra = join(a);
        join(b, "a");
        seconds(10);
        assertThat(members(ra)).isEqualTo(1);
        network.partition("a", "b");
        seconds(11 * 60);
        long after = a.lostIntroductions();
        seconds(10 * 60);
        assertThat(a.lostIntroductions()).isEqualTo(after);
        assertThat(a.retired(groupId)).isEmpty();
    }

    @Test
    void aLargeLossIsReDialedOverRoundsNotAllAtOnce() throws Exception {
        PeerNode a = node("a", 1, UnaryOperator.identity());
        join(a);
        long now = clock.millis();
        for (int i = 0; i < 30; i++) {
            a.noteLostForTest(groupId, PeerIdentity.generate().peerId(),
                    List.of(new PeerAdvertisement.Endpoint("mem", "gone-" + i, 0)), now);
        }
        clock.advance(Duration.ofSeconds(5));
        a.tick();
        assertThat(a.lostIntroductions()).as("one round's budget").isEqualTo(PeerNode.LOST_RETRY_BUDGET);
        for (int i = 0; i < 4; i++) {
            a.tick();
        }
        assertThat(a.lostIntroductions()).as("every lost member tried within a few rounds").isEqualTo(30);
    }

    /** Three members joined through C; C dies for good and A and B are cut apart until both retire each other. */
    private PeerNode[] splitUntilRetired(boolean postRetirementAtA) throws Exception {
        PeerNode c = node("c", 3, UnaryOperator.identity());
        PeerNode a = node("a", 1, o -> o.retireAfter(Duration.ofMinutes(5)).postRetirement(postRetirementAtA));
        PeerNode b = node("b", 2, o -> o.retireAfter(Duration.ofMinutes(5)));
        join(c);
        GroupRuntime ra = join(a, "c");
        GroupRuntime rb = join(b, "c");
        seconds(20);
        assertThat(members(ra)).isEqualTo(2);
        assertThat(members(rb)).isEqualTo(2);
        c.close();
        nodes.remove(c);
        network.partition("a", "c");
        network.partition("b", "c");
        network.partition("a", "b");
        seconds(7 * 60);
        assertThat(members(ra)).isZero();
        assertThat(a.retired(groupId)).contains(b.peerId());
        return new PeerNode[]{a, b};
    }

    @Test
    void postRetirementBringsAnIsolatedNodeBackThroughARetiredMember() throws Exception {
        PeerNode[] fleet = splitUntilRetired(true);
        PeerNode a = fleet[0];
        PeerNode b = fleet[1];
        network.heal();
        network.partition("a", "c");
        network.partition("b", "c");
        int waited = 0;
        while (members(a.group(groupId).orElseThrow()) == 0 && waited < 15 * 60) {
            seconds(10);
            waited += 10;
        }
        assertThat(a.lastResortIntroductions()).as("the retirement list was tried").isPositive();
        assertThat(a.group(groupId).orElseThrow().membership().member(b.peerId())).as("back together").isPresent();
        assertThat(a.retired(groupId)).as("b is back from retirement").doesNotContain(b.peerId());
        seconds(5);
        assertThat(a.isolated(groupId)).isFalse();
    }

    @Test
    void withPostRetirementOffAnIsolatedNodeStaysIsolatedAndSaysSo() throws Exception {
        PeerNode[] fleet = splitUntilRetired(false);
        PeerNode a = fleet[0];
        network.heal();
        network.partition("a", "c");
        network.partition("b", "c");
        seconds(15 * 60);
        assertThat(a.lastResortIntroductions()).isZero();
        assertThat(members(a.group(groupId).orElseThrow())).isZero();
        assertThat(a.isolated(groupId)).isTrue();
    }

    @Test
    void theRetirementListSurvivesARestart(@TempDir Path state) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode a = node("a", 1, identity, o -> o.stateDirectory(state).retireAfter(Duration.ofMinutes(5)));
        PeerNode b = node("b", 2, UnaryOperator.identity());
        join(a);
        join(b, "a");
        seconds(10);
        network.partition("a", "b");
        seconds(7 * 60);
        assertThat(a.retired(groupId)).containsExactly(b.peerId());
        a.close();
        nodes.remove(a);
        assertThat(Files.list(state.resolve(RetirementList.DIRECTORY))).hasSize(1);

        PeerNode again = node("a2", 4, identity, o -> o.stateDirectory(state).postRetirement(true));
        join(again);
        assertThat(again.retired(groupId)).as("loaded on join").containsExactly(b.peerId());
    }

    private PeerNode.MembershipState state(PeerNode node) {
        return node.membershipStatus(groupId).orElseThrow().state();
    }

    @Test
    void aSoleNodeWithNothingToReachIsAloneNotIsolated() throws Exception {
        PeerNode a = node("a", 1, UnaryOperator.identity());
        join(a);
        seconds(5 * 60);
        assertThat(state(a)).isEqualTo(PeerNode.MembershipState.ALONE);
        assertThat(a.isolated(groupId)).isFalse();
    }

    @Test
    void theStatusFollowsTheFleetAndTellsEveryChange() throws Exception {
        PeerNode a = node("a", 1, UnaryOperator.identity());
        PeerNode b = node("b", 2, UnaryOperator.identity());
        PeerNode c = node("c", 3, UnaryOperator.identity());
        List<PeerNode.MembershipState> seen = new java.util.concurrent.CopyOnWriteArrayList<>();
        a.onMembershipStatus(status -> seen.add(status.state()));
        join(a);
        join(b, "a");
        join(c, "a");
        seconds(20);
        assertThat(state(a)).isEqualTo(PeerNode.MembershipState.CONNECTED);

        network.partition("a", "c");
        network.partition("b", "c"); // c is gone for both: an indirect probe through b would still reach it otherwise
        seconds(30);
        PeerNode.MembershipStatus degraded = a.membershipStatus(groupId).orElseThrow();
        assertThat(degraded.state()).as("c lost, b still here").isEqualTo(PeerNode.MembershipState.DEGRADED);
        assertThat(degraded.lost()).extracting(PeerNode.LostMember::peer).containsExactly(c.peerId());
        assertThat(degraded.lost().get(0).attempts()).isPositive();

        network.partition("a", "b");
        seconds(30);
        assertThat(state(a)).isEqualTo(PeerNode.MembershipState.RECONNECTING);
        seconds(3 * 60);
        assertThat(state(a)).isEqualTo(PeerNode.MembershipState.ISOLATED);
        assertThat(a.isolated(groupId)).isTrue();

        network.heal();
        for (int i = 0; i < 20 && state(a) != PeerNode.MembershipState.CONNECTED; i++) {
            seconds(10);
        }
        assertThat(state(a)).isEqualTo(PeerNode.MembershipState.CONNECTED);
        seconds(5); // listeners hear a change from the tick that sees it, a tick after the status reads it
        assertThat(seen).as("every change told, in order").containsSubsequence(PeerNode.MembershipState.CONNECTED,
                PeerNode.MembershipState.DEGRADED, PeerNode.MembershipState.RECONNECTING,
                PeerNode.MembershipState.ISOLATED, PeerNode.MembershipState.CONNECTED);
    }
}
