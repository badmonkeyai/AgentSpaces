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
import ai.badmonkey.agentspaces.capabilities.orderedlog.RaftMessages.AppendEntries;
import ai.badmonkey.agentspaces.capabilities.orderedlog.RaftMessages.Frame;
import ai.badmonkey.agentspaces.capabilities.orderedlog.RaftMessages.LogEntry;
import ai.badmonkey.agentspaces.capabilities.orderedlog.RaftMessages.RequestVote;
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
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Raft voter's durable state (srad/agentspaces-js.md JS-N-06, the port of
 * {@code packages/ordered/test/raft.test.ts}): three voters over
 * {@link FileLogStore}s commit in order and every store holds the committed
 * entries; a voter restarted from its file store keeps its term, its vote,
 * and its entries, and refuses a second candidate in the term it already
 * voted in (where a store-less restart would grant, which is the amnesia the
 * SRAD names); and a store whose write fails leaves the vote reply, the
 * append reply, and the candidacy unsent, counted, with the in-memory state
 * as the store had it. Deterministic: seeded timeouts and manual ticks over
 * the {@link SimNetwork}, which delivers synchronously.
 */
class RaftLogStoreTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zRaftStore");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zRaftStore", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "raft-store",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @TempDir
    Path stores;

    private record Member(PeerIdentity identity, CapabilityPipes pipes, RaftLog raft,
                          List<String> applied, Path storeDirectory) {
        PeerId peerId() {
            return identity.peerId();
        }
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static List<String> commands(List<LogEntry> entries) {
        return entries.stream().map(e -> new String(e.command(), StandardCharsets.UTF_8)).toList();
    }

    /** A converged cluster whose member {@code i} runs over {@code storeFor.apply(i)}. */
    private List<Member> cluster(int size, IntFunction<LogStore> storeFor) throws IOException {
        List<PeerIdentity> identities = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            identities.add(PeerIdentity.generate());
        }
        List<PeerId> memberIds = identities.stream().map(PeerIdentity::peerId).toList();
        List<Member> members = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            String address = "n" + i;
            PeerNode node = PeerNode.builder(identities.get(i))
                    .clock(clock).randomSeed(300 + i).build();
            node.listen(network.register(address), address);
            GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                    i == 0 ? List.of()
                            : List.of(new PeerAdvertisement.Endpoint("mem", "n0", 0)));
            List<String> applied = new CopyOnWriteArrayList<>();
            CapabilityPipes pipes = new CapabilityPipes(runtime, codec);
            RaftLog raft = new RaftLog(pipes, codec, identities.get(i).peerId(), memberIds, clock,
                    3000 + i, (command, index) ->
                            applied.add(index + ":" + new String(command, StandardCharsets.UTF_8)),
                    storeFor.apply(i));
            nodes.add(node);
            members.add(new Member(identities.get(i), pipes, raft, applied, stores.resolve("m" + i)));
        }
        // Membership converges before Raft runs: the data plane serves admitted
        // members only (ASF-003), so every member must hold every other.
        for (int i = 0; i < 50; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
            boolean converged = nodes.stream().allMatch(n ->
                    n.group(groupId).orElseThrow().membership().allMembers().size() == size - 1);
            if (converged) {
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
        for (int i = 0; i < 50; i++) {
            raftTicks(members, 1);
            for (Member member : members) {
                if (member.raft().isLeader()) {
                    return member;
                }
            }
        }
        throw new AssertionError("no leader elected within 50 rounds");
    }

    /** Replaces a member's Raft handler with a tap, retiring its log; returns the captured frames. */
    private List<Frame> tap(Member member) {
        List<Frame> captured = new CopyOnWriteArrayList<>();
        member.pipes().onCapability(RaftLog.TYPE, (from, payload) ->
                captured.add(codec.fromBytes(payload, Frame.class)));
        return captured;
    }

    private void sendFrom(Member member, PeerId to, Frame frame) {
        member.pipes().send(to, RaftLog.TYPE, codec.toBytes(frame));
    }

    /** JS-N-06: three voters over file stores elect, commit in order, and every store holds the committed log. */
    @Test
    void threeVotersWithFileStoresCommitInOrder() throws Exception {
        List<Member> members = cluster(3, i -> new FileLogStore(stores.resolve("m" + i)));
        Member leader = awaitLeader(members);
        for (int i = 0; i < 3; i++) {
            assertThat(leader.raft().submit(utf8("cmd-" + i))).isTrue();
            raftTicks(members, 2);
        }
        raftTicks(members, 3);

        List<String> expected = List.of("1:cmd-0", "2:cmd-1", "3:cmd-2");
        for (Member member : members) {
            assertThat(member.applied()).isEqualTo(expected);
            assertThat(member.raft().storeFailures()).isZero();
            LogStore.State durable = new FileLogStore(member.storeDirectory()).load();
            assertThat(durable.term()).as("the term is durable").isEqualTo(leader.raft().term());
            assertThat(commands(durable.entries())).as("the committed log is durable")
                    .containsExactly("cmd-0", "cmd-1", "cmd-2");
        }
        assertThat(new FileLogStore(leader.storeDirectory()).load().votedFor())
                .as("the leader's vote for itself is durable").isEqualTo(leader.peerId());
    }

    /** JS-N-06 (JS-Q-REL-03 in the TypeScript twin): a voter restarted from its file store keeps term, vote, and entries, and does not vote twice in a term. */
    @Test
    void aVoterRestartedFromItsFileStoreKeepsItsVoteAndNeverVotesTwice() throws Exception {
        List<Member> members = cluster(3, i -> new FileLogStore(stores.resolve("m" + i)));
        Member leader = awaitLeader(members);
        assertThat(leader.raft().submit(utf8("durable"))).isTrue();
        raftTicks(members, 4);
        long term = leader.raft().term();

        // A follower that granted the leader its term: the leader's majority
        // needed at least one, and its grant was written before it was sent.
        List<Member> followers = members.stream().filter(m -> m != leader).toList();
        Member voter = followers.stream().filter(m -> {
            try {
                return leader.peerId().equals(new FileLogStore(m.storeDirectory()).load().votedFor());
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }).findFirst().orElseThrow(() -> new AssertionError("no follower holds a durable vote for the leader"));
        Member probe = followers.stream().filter(m -> m != voter).findFirst().orElseThrow();
        assertThat(voter.applied()).containsExactly("1:durable");

        // Restart the voter: a new log on the same pipes, state only from the store.
        List<String> appliedAgain = new CopyOnWriteArrayList<>();
        RaftLog restarted = new RaftLog(voter.pipes(), codec, voter.peerId(),
                members.stream().map(Member::peerId).toList(), clock, 4242,
                (command, index) -> appliedAgain.add(index + ":" + new String(command, StandardCharsets.UTF_8)),
                new FileLogStore(voter.storeDirectory()));
        assertThat(restarted.term()).as("the term survived").isEqualTo(term);
        assertThat(restarted.lastLogIndex()).as("the entry survived").isEqualTo(1);
        assertThat(restarted.commitIndex()).as("commitment is relearned from the leader").isZero();

        // The probe asks for the same term as a different candidate with an
        // up-to-date log: refused, the restarted voter already voted this term.
        List<Frame> replies = tap(probe);
        sendFrom(probe, voter.peerId(), Frame.of(new RequestVote(term, probe.peerId(), 1, term)));
        assertThat(replies).hasSize(1);
        assertThat(replies.get(0).voteReply()).isNotNull();
        assertThat(replies.get(0).voteReply().term()).isEqualTo(term);
        assertThat(replies.get(0).voteReply().granted()).as("no second vote in a term").isFalse();

        // The candidate it did vote for may ask again and is granted again (Raft's rule).
        replies.clear();
        sendFrom(probe, voter.peerId(), Frame.of(new RequestVote(term, leader.peerId(), 1, term)));
        assertThat(replies).hasSize(1);
        assertThat(replies.get(0).voteReply().granted()).as("the same candidate is re-granted").isTrue();

        // The leader's next heartbeat re-commits the durable entry on the
        // restarted voter and the applied listener sees it from index 1 again.
        leader.raft().tick();
        assertThat(appliedAgain).containsExactly("1:durable");

        // The contrast the SRAD names: the same restart without a store forgets
        // the vote and grants the term a second time.
        RaftLog amnesiac = new RaftLog(voter.pipes(), codec, voter.peerId(),
                members.stream().map(Member::peerId).toList(), clock, 4343, (command, index) -> {
                });
        assertThat(amnesiac.term()).isZero();
        replies.clear();
        sendFrom(probe, voter.peerId(), Frame.of(new RequestVote(term, probe.peerId(), 1, term)));
        assertThat(replies).hasSize(1);
        assertThat(replies.get(0).voteReply().granted()).as("amnesia votes twice").isTrue();
    }

    /** JS-N-06: a store write that fails leaves the reply (or the candidacy) unsent, counted, and the memory state as the store had it. */
    @Test
    void aStoreWriteThatFailsLeavesTheReplyUnsent() throws Exception {
        MemoryLogStore store = new MemoryLogStore();
        List<Member> members = cluster(2, i -> i == 0 ? store : new MemoryLogStore());
        Member voter = members.get(0);
        Member peer = members.get(1);
        List<Frame> replies = tap(peer);

        // A vote request whose term bump cannot be written: no reply, term unchanged.
        store.failNextWrite(new IOException("disk full"));
        sendFrom(peer, voter.peerId(), Frame.of(new RequestVote(1, peer.peerId(), 0, 0)));
        assertThat(replies).as("no reply on a failed write").isEmpty();
        assertThat(voter.raft().term()).isZero();
        assertThat(voter.raft().storeFailures()).isEqualTo(1);
        assertThat(store.load().term()).isZero();

        // The same request once the store writes: the vote is durable, then answered.
        sendFrom(peer, voter.peerId(), Frame.of(new RequestVote(1, peer.peerId(), 0, 0)));
        assertThat(replies).hasSize(1);
        assertThat(replies.get(0).voteReply().granted()).isTrue();
        assertThat(replies.get(0).voteReply().term()).isEqualTo(1);
        assertThat(voter.raft().term()).isEqualTo(1);
        assertThat(store.load().term()).isEqualTo(1);
        assertThat(store.load().votedFor()).isEqualTo(peer.peerId());
        long writesSoFar = store.writes();
        assertThat(writesSoFar).isGreaterThanOrEqualTo(2);

        // An append whose entries cannot be written: no reply, log unchanged.
        AppendEntries append = new AppendEntries(1, peer.peerId(), 0, 0,
                List.of(new LogEntry(1, utf8("cmd"))), 0);
        replies.clear();
        store.failNextWrite(new IOException("disk full"));
        sendFrom(peer, voter.peerId(), Frame.of(append));
        assertThat(replies).as("no acknowledgement on a failed write").isEmpty();
        assertThat(voter.raft().lastLogIndex()).isZero();
        assertThat(voter.raft().storeFailures()).isEqualTo(2);
        assertThat(store.load().entries()).isEmpty();

        // Resent, it is written and then acknowledged.
        sendFrom(peer, voter.peerId(), Frame.of(append));
        assertThat(replies).hasSize(1);
        assertThat(replies.get(0).appendReply().success()).isTrue();
        assertThat(replies.get(0).appendReply().matchIndex()).isEqualTo(1);
        assertThat(voter.raft().lastLogIndex()).isEqualTo(1);
        assertThat(commands(store.load().entries())).containsExactly("cmd");
        assertThat(store.writes()).isEqualTo(writesSoFar + 1);

        // A candidacy whose term bump cannot be written: no request leaves, the
        // timer runs again, and the next timeout's candidacy does leave.
        replies.clear();
        store.failNextWrite(new IOException("disk full"));
        for (int i = 0; i < 10; i++) {
            voter.raft().tick();
        }
        assertThat(voter.raft().storeFailures()).isEqualTo(3);
        assertThat(replies).as("no vote request after the failed candidacy").isEmpty();
        assertThat(voter.raft().term()).as("the failed candidacy's term is not kept").isEqualTo(1);
        assertThat(voter.raft().isLeader()).isFalse();
        for (int i = 0; i < 10 && replies.isEmpty(); i++) {
            voter.raft().tick();
        }
        assertThat(replies).hasSize(1);
        assertThat(replies.get(0).requestVote()).isNotNull();
        assertThat(replies.get(0).requestVote().term()).isEqualTo(2);
        assertThat(store.load().term()).isEqualTo(2);
        assertThat(store.load().votedFor()).isEqualTo(voter.peerId());
        assertThat(voter.raft().storeFailures()).isEqualTo(3);
    }
}
