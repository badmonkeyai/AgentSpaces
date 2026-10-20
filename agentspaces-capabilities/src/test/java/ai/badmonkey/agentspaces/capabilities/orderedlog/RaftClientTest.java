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
package ai.badmonkey.agentspaces.capabilities.orderedlog;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Raft client gate (srad/agentspaces-js.md JS-V-05; ASF-005 kept): an
 * admitted group peer outside the fixed member set may submit a command
 * under {@code RAFT_CLIENT}, within a per-sender rate, and the log commits it
 * in order; the same peer's votes and appends are still dropped, and an
 * authorizer that refuses {@code RAFT_CLIENT} refuses the submit.
 */
class RaftClientTest {

    private final CborCodec codec = CborCodec.defaultCodec();
    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.fromFounding("raft-client".getBytes(StandardCharsets.UTF_8));
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://" + groupId.value(), PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "raft-client",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Member(PeerNode node, RaftLog raft, List<String> applied) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private List<Member> cluster(Authorizer authorizer) throws IOException {
        List<PeerIdentity> identities = List.of(PeerIdentity.generate(), PeerIdentity.generate(),
                PeerIdentity.generate());
        List<PeerId> memberIds = identities.stream().map(PeerIdentity::peerId).toList();
        List<Member> members = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            PeerNode node = PeerNode.builder(identities.get(i)).clock(clock).randomSeed(300 + i).build();
            node.listen(network.register("n" + i), "n" + i);
            GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                    i == 0 ? List.of() : List.of(new PeerAdvertisement.Endpoint("mem", "n0", 0)));
            List<String> applied = new CopyOnWriteArrayList<>();
            RaftLog raft = new RaftLog(new CapabilityPipes(runtime, codec), codec, identities.get(i).peerId(),
                    memberIds, clock, RaftLog.DEFAULT_TICK_PERIOD, 1000 + i,
                    (command, index) -> applied.add(index + ":" + new String(command, StandardCharsets.UTF_8)),
                    authorizer, groupId);
            nodes.add(node);
            members.add(new Member(node, raft, applied));
        }
        return members;
    }

    private GroupRuntime joinClient(PeerIdentity client) throws IOException {
        PeerNode node = PeerNode.builder(client).clock(clock).randomSeed(777).build();
        node.listen(network.register("client"), "client");
        nodes.add(node);
        return node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "n0", 0)));
    }

    private void converge(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    private static Member awaitLeader(List<Member> members) {
        for (int i = 0; i < 60; i++) {
            members.forEach(m -> m.raft().tick());
            for (Member member : members) {
                if (member.raft().isLeader()) {
                    return member;
                }
            }
        }
        throw new AssertionError("no leader elected");
    }

    private static void raftTicks(List<Member> members, int rounds) {
        for (int i = 0; i < rounds; i++) {
            members.forEach(m -> m.raft().tick());
        }
    }

    @Test
    void anAdmittedNonMemberSubmitsAsAClientAndTheLogCommitsInOrder() throws Exception {
        List<Member> members = cluster((peer, operation, scope) -> groupId.value().equals(scope));
        PeerIdentity client = PeerIdentity.generate();
        GroupRuntime clientRuntime = joinClient(client);
        converge(8);
        awaitLeader(members);
        CapabilityPipes pipes = new CapabilityPipes(clientRuntime, codec);
        byte[] command = "from-client".getBytes(StandardCharsets.UTF_8);
        // Sent to a follower as well as the leader: a follower forwards under its leader hint.
        for (Member member : members) {
            pipes.send(member.node().peerId(), RaftLog.TYPE,
                    codec.toBytes(RaftMessages.Frame.of(new RaftMessages.ClientSubmit(command))));
        }
        raftTicks(members, 8);
        for (Member member : members) {
            assertThat(member.applied()).as("applied at " + member.node().peerId())
                    .containsExactly("1:from-client", "2:from-client", "3:from-client");
        }
        assertThat(members.stream().mapToLong(m -> m.raft().clientSubmits()).sum()).isEqualTo(3);

        // The client still cannot drive the log: a forged append from it never applies.
        RaftMessages.AppendEntries forged = new RaftMessages.AppendEntries(99, client.peerId(), 0, 0,
                List.of(new RaftMessages.LogEntry(99, "evil".getBytes(StandardCharsets.UTF_8))), 1);
        for (Member member : members) {
            pipes.send(member.node().peerId(), RaftLog.TYPE, codec.toBytes(RaftMessages.Frame.of(forged)));
        }
        raftTicks(members, 6);
        for (Member member : members) {
            assertThat(member.applied()).hasSize(3);
        }
        assertThat(members.stream().mapToLong(m -> m.raft().clientSubmitsRefused()).sum()).isEqualTo(3);
    }

    @Test
    void theClientRateIsPerSenderPerSecondAndARefusedClientIsDropped() throws Exception {
        List<Member> members = cluster((peer, operation, scope) -> groupId.value().equals(scope));
        PeerIdentity client = PeerIdentity.generate();
        GroupRuntime clientRuntime = joinClient(client);
        converge(8);
        Member leader = awaitLeader(members);
        CapabilityPipes pipes = new CapabilityPipes(clientRuntime, codec);
        for (int i = 0; i < RaftLog.CLIENT_SUBMITS_PER_SECOND + 10; i++) {
            pipes.send(leader.node().peerId(), RaftLog.TYPE, codec.toBytes(RaftMessages.Frame.of(
                    new RaftMessages.ClientSubmit(("c" + i).getBytes(StandardCharsets.UTF_8)))));
        }
        assertThat(leader.raft().clientSubmits()).isEqualTo(RaftLog.CLIENT_SUBMITS_PER_SECOND);
        assertThat(leader.raft().clientSubmitsRefused()).isEqualTo(10);
        clock.advance(Duration.ofSeconds(1)); // the next second refills the sender's budget
        pipes.send(leader.node().peerId(), RaftLog.TYPE, codec.toBytes(RaftMessages.Frame.of(
                new RaftMessages.ClientSubmit("again".getBytes(StandardCharsets.UTF_8)))));
        assertThat(leader.raft().clientSubmits()).isEqualTo(RaftLog.CLIENT_SUBMITS_PER_SECOND + 1);

        // An authorizer that grants RAFT_CLIENT to nobody refuses the submit outright.
        nodes.forEach(PeerNode::close);
        nodes.clear();
        List<Member> strict = cluster((peer, operation, scope) ->
                operation == Authorizer.Operation.RAFT_VOTER && groupId.value().equals(scope));
        PeerIdentity other = PeerIdentity.generate();
        GroupRuntime otherRuntime = joinClient(other);
        converge(8);
        Member strictLeader = awaitLeader(strict);
        new CapabilityPipes(otherRuntime, codec).send(strictLeader.node().peerId(), RaftLog.TYPE,
                codec.toBytes(RaftMessages.Frame.of(new RaftMessages.ClientSubmit("no".getBytes(StandardCharsets.UTF_8)))));
        raftTicks(strict, 4);
        assertThat(strictLeader.raft().clientSubmits()).isZero();
        assertThat(strictLeader.raft().clientSubmitsRefused()).isEqualTo(1);
        assertThat(strictLeader.applied()).isEmpty();
    }
}
