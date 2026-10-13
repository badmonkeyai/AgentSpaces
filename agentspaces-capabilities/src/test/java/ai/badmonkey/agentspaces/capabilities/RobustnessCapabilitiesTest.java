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
package ai.badmonkey.agentspaces.capabilities;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.capabilities.orderedlog.RaftLog;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.vote.VoteCapability;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ROBUSTNESS.md §4, the capability protocols: the ordered log under frames held, reordered,
 * doubled and replayed, and under a one-way loss to a follower; a vote whose ballots are held,
 * doubled and replayed. Each command applies once in order everywhere, each ballot counts
 * once, one leader stands, and nobody is struck.
 */
class RobustnessCapabilitiesTest {

    private static final Lease HOUR = Lease.of(Duration.ofHours(1));

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zRobustCap");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zRobustCap", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "robust-cap",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Member(String address, PeerNode node, RaftLog raft, List<String> applied) {
    }

    private record Voter(String address, PeerNode node, PeerIdentity identity, VoteCapability vote) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    private List<Member> raftCluster(int size) throws IOException {
        List<PeerIdentity> identities = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            identities.add(PeerIdentity.generate());
        }
        List<PeerId> memberIds = identities.stream().map(PeerIdentity::peerId).toList();
        List<Member> members = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            String address = "n" + i;
            PeerNode node = PeerNode.builder(identities.get(i)).clock(clock).randomSeed(100 + i).build();
            node.listen(network.register(address), address);
            GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                    i == 0 ? List.of() : List.of(new PeerAdvertisement.Endpoint("mem", "n0", 0)));
            List<String> applied = new CopyOnWriteArrayList<>();
            RaftLog raft = new RaftLog(new CapabilityPipes(runtime, codec), codec, identities.get(i).peerId(),
                    memberIds, clock, 1000 + i,
                    (command, index) -> applied.add(index + ":" + new String(command, StandardCharsets.UTF_8)));
            nodes.add(node);
            members.add(new Member(address, node, raft, applied));
        }
        for (int i = 0; i < 50; i++) {
            tickAll(1);
            if (members.stream().allMatch(m -> m.node().group(groupId).orElseThrow().membership().allMembers().size() == size - 1)) {
                return members;
            }
        }
        throw new AssertionError("membership did not converge");
    }

    private static void raftTicks(List<Member> members, int rounds) {
        for (int i = 0; i < rounds; i++) {
            members.forEach(m -> m.raft().tick());
        }
    }

    private static Member awaitLeader(List<Member> members) {
        for (int i = 0; i < 80; i++) {
            raftTicks(members, 1);
            for (Member member : members) {
                if (member.raft().isLeader()) {
                    return member;
                }
            }
        }
        throw new AssertionError("no leader elected within 80 rounds");
    }

    private static Member currentLeader(List<Member> members) {
        return members.stream().filter(m -> m.raft().isLeader()).findFirst().orElse(null);
    }

    @Test
    @Timeout(60)
    void theOrderedLogAppliesEachCommandOnceInOrderUnderHeldReorderedDoubledAndReplayedFrames() throws Exception {
        List<Member> members = raftCluster(3);
        Member leader = awaitLeader(members);
        Member lagging = members.stream().filter(m -> m != leader).findFirst().orElseThrow();
        List<byte[]> toLagging = new CopyOnWriteArrayList<>();
        network.tap(leader.address(), lagging.address(), toLagging::add);

        for (SimNetwork.Order order : SimNetwork.Order.values()) {
            network.hold(leader.address(), lagging.address());
            for (int i = 0; i < 4; i++) {
                assertThat(leader.raft().submit((order + "-" + i).getBytes(StandardCharsets.UTF_8))).isTrue();
                raftTicks(members, 2);
            }
            raftTicks(members, 6);
            network.release(leader.address(), lagging.address(), order, 5);
            raftTicks(members, 20);
            List<String> reference = members.stream().filter(m -> m != lagging).findFirst().orElseThrow().applied();
            assertThat(lagging.applied()).as(order + ": the lagging follower applied the same log")
                    .containsExactlyElementsOf(reference);
            assertThat(reference).doesNotHaveDuplicates();
            assertThat(members.stream().filter(m -> m.raft().isLeader()).count()).as("one leader").isEqualTo(1);
        }
        // Replaying every frame the leader ever sent the follower changes nothing.
        List<String> before = List.copyOf(lagging.applied());
        for (byte[] frame : List.copyOf(toLagging)) {
            network.inject(leader.address(), lagging.address(), frame);
        }
        raftTicks(members, 10);
        assertThat(lagging.applied()).containsExactlyElementsOf(before);
        for (Member m : members) {
            assertThat(m.applied()).containsExactlyElementsOf(before);
        }
    }

    @Test
    @Timeout(60)
    void theOrderedLogSurvivesAOneWayLossToAFollowerAndConvergesAfterItHeals() throws Exception {
        List<Member> members = raftCluster(3);
        Member leader = awaitLeader(members);
        Member cut = members.stream().filter(m -> m != leader).findFirst().orElseThrow();
        network.oneWay(leader.address(), cut.address()); // the follower hears nothing from the leader
        for (int i = 0; i < 5; i++) {
            Member current = currentLeader(members);
            if (current != null) {
                current.raft().submit(("cmd-" + i).getBytes(StandardCharsets.UTF_8));
            }
            raftTicks(members, 4);
        }
        raftTicks(members, 30); // the cut follower may call elections it cannot win; the rest carry on
        network.twoWay(leader.address(), cut.address());
        raftTicks(members, 60);
        assertThat(members.stream().filter(m -> m.raft().isLeader()).count()).as("one leader after the heal").isEqualTo(1);
        List<String> reference = members.get(0).applied();
        for (Member m : members) {
            assertThat(m.applied()).as(m.address() + " converged").containsExactlyElementsOf(reference);
        }
        assertThat(reference).isNotEmpty().doesNotHaveDuplicates();
    }

    private Voter newVoter(String address, long seed, String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        ReplicatedSpace voteSpace = ReplicatedSpace.builder(runtime, "votes", identity, "voter")
                .clock(clock).settleWindow(Duration.ZERO).build();
        VoteCapability vote = new VoteCapability(voteSpace, identity.agent("voter"), identity.peerId(), clock);
        nodes.add(node);
        return new Voter(address, node, identity, vote);
    }

    @Test
    @Timeout(60)
    void ballotsHeldDoubledAndReplayedCountOnceAndDecideOnce() throws Exception {
        Voter a = newVoter("a", 1);
        Voter b = newVoter("b", 2, "a");
        Voter c = newVoter("c", 3, "a");
        tickAll(8);
        List<byte[]> bToA = new CopyOnWriteArrayList<>();
        network.tap("b", "a", bToA::add);
        a.vote().propose("p", "Ship?", List.of("approve", "reject"), 3, HOUR);
        tickAll(2);
        network.hold("b", "a");
        network.hold("c", "a");
        b.vote().castBallot("p", "approve", HOUR);
        c.vote().castBallot("p", "reject", HOUR);
        a.vote().castBallot("p", "approve", HOUR);
        tickAll(2);
        assertThat(a.vote().decision("p")).as("two ballots held: no decision at A yet").isEmpty();
        network.release("b", "a", SimNetwork.Order.DUPLICATED, 0);
        network.release("c", "a", SimNetwork.Order.REVERSED, 0);
        tickAll(4);
        assertThat(a.vote().tally("p")).containsEntry("approve", 2).containsEntry("reject", 1);
        assertThat(a.vote().decision("p")).hasValueSatisfying(d -> assertThat(d.winner()).isEqualTo("approve"));
        for (byte[] frame : List.copyOf(bToA)) {
            network.inject("b", "a", frame);
        }
        tickAll(2);
        assertThat(a.vote().tally("p")).as("a replayed ballot is not a second ballot")
                .containsEntry("approve", 2).containsEntry("reject", 1);
        for (Voter v : List.of(a, b, c)) {
            assertThat(v.vote().decision("p")).as(v.address() + " agrees").hasValueSatisfying(d -> assertThat(d.winner()).isEqualTo("approve"));
        }
    }
}
