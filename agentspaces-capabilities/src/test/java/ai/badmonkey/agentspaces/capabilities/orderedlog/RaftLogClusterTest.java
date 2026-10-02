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

import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
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
 * Raft over the SimNetwork: election, ordered application on every member,
 * forwarded submits, and leader failover. Deterministic: seeded timeouts and
 * manual ticks.
 */
class RaftLogClusterTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zRaft");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zRaft", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "raft",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Member(PeerNode node, RaftLog raft, List<String> applied,
                          CapabilityRuntime capabilities) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private List<Member> cluster(int size) throws IOException {
        return cluster(size, -1);
    }

    /**
     * A cluster whose members share one {@link Authorizer}; the member at
     * {@code refusedIndex} (when non-negative) is refused {@code RAFT_VOTER}
     * by every member, itself included.
     */
    private List<Member> cluster(int size, int refusedIndex) throws IOException {
        List<PeerIdentity> identities = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            identities.add(PeerIdentity.generate());
        }
        List<PeerId> memberIds = identities.stream().map(PeerIdentity::peerId).toList();
        List<Member> members = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            String address = "n" + i;
            PeerNode node = PeerNode.builder(identities.get(i))
                    .clock(clock).randomSeed(100 + i).build();
            node.listen(network.register(address), address);
            GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                    i == 0 ? List.of()
                            : List.of(new PeerAdvertisement.Endpoint("mem", "n0", 0)));
            List<String> applied = new CopyOnWriteArrayList<>();
            RaftLog raft;
            if (refusedIndex < 0) {
                raft = new RaftLog(new CapabilityPipes(runtime, codec), codec,
                        identities.get(i).peerId(), memberIds, clock, 1000 + i,
                        (command, index) ->
                                applied.add(index + ":" + new String(command, StandardCharsets.UTF_8)));
            } else {
                PeerId refused = memberIds.get(refusedIndex);
                Authorizer authorizer = (peer, operation, scope) ->
                        operation == Authorizer.Operation.RAFT_VOTER
                                && groupId.value().equals(scope) && !peer.equals(refused);
                raft = new RaftLog(new CapabilityPipes(runtime, codec), codec,
                        identities.get(i).peerId(), memberIds, clock,
                        RaftLog.DEFAULT_TICK_PERIOD, 1000 + i,
                        (command, index) ->
                                applied.add(index + ":" + new String(command, StandardCharsets.UTF_8)),
                        authorizer, groupId);
            }
            DiscoveryService discovery = new DiscoveryService(
                    runtime, new AdCache(codec, clock), codec, identities.get(i).peerId());
            nodes.add(node);
            members.add(new Member(node, raft, applied,
                    new CapabilityRuntime(runtime, discovery, identities.get(i))));
        }
        // Let membership fully converge before Raft runs: the data plane serves
        // admitted members only (ASF-003), so every member must hold every other
        // member in its view or frames drop.
        for (int i = 0; i < 50; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // anti-entropy runs once per gossip period on the node clock
            boolean converged = members.stream().allMatch(m ->
                    m.node().group(groupId).orElseThrow().membership().allMembers().size()
                            == size - 1);
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

    @Test
    void aGroupPeerOutsideTheRaftMemberSetCannotDriveTheLog() throws Exception {
        List<Member> members = cluster(3);
        awaitLeader(members);

        // A fourth peer joins the (OPEN) group but is not in the Raft member
        // set. It forges leadership: AppendEntries at a huge term carrying a
        // command, with leaderCommit claiming it committed (ASF-005).
        PeerIdentity outsider = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(outsider).clock(clock).randomSeed(777).build();
        node.listen(network.register("outsider"), "outsider");
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "n0", 0)));
        nodes.add(node);
        for (int i = 0; i < 4; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // anti-entropy runs once per gossip period on the node clock
        }
        CapabilityPipes evil = new CapabilityPipes(runtime, codec);
        RaftMessages.AppendEntries forged = new RaftMessages.AppendEntries(99,
                outsider.peerId(), 0, 0,
                List.of(new RaftMessages.LogEntry(99,
                        "evil".getBytes(StandardCharsets.UTF_8))), 1);
        for (Member member : members) {
            evil.send(member.node().peerId(), RaftLog.TYPE,
                    codec.toBytes(RaftMessages.Frame.of(forged)));
        }
        raftTicks(members, 6);
        for (Member member : members) {
            assertThat(member.applied()).as("forged command never applies").isEmpty();
        }

        // The authorized cluster is unharmed and still commits in order.
        Member leader = awaitLeader(members);
        leader.raft().submit("legit".getBytes(StandardCharsets.UTF_8));
        raftTicks(members, 6);
        for (Member member : members) {
            assertThat(member.applied()).containsExactly("1:legit");
        }
    }

    /** TODO-EFG §4 / TODO item 6 (ASF-005): a fixed-set member the profile's Authorizer refuses RAFT_VOTER is a replica, never a voter — it cannot be elected and its submits are dropped, while the permitted quorum keeps committing. */
    @Test
    void aMemberTheAuthorizerRefusesCannotVoteOrSubmit() throws Exception {
        List<Member> members = cluster(3, 2);
        Member refused = members.get(2);
        List<Member> permitted = List.of(members.get(0), members.get(1));

        // Only a permitted member can gather a majority: the refused member's
        // vote requests are dropped by everyone else.
        Member leader = awaitLeader(members);
        assertThat(leader).isNotSameAs(refused);
        for (int i = 0; i < 60; i++) {
            raftTicks(members, 1);
            assertThat(refused.raft().isLeader()).as("round " + i).isFalse();
        }

        // The refused member's submit either knows no leader or forwards to
        // one that drops the frame; nothing it submits ever commits.
        refused.raft().submit("rogue".getBytes(StandardCharsets.UTF_8));
        raftTicks(members, 8);
        for (Member member : members) {
            assertThat(member.applied()).as("rogue command never applies").isEmpty();
        }

        // The permitted quorum is unharmed and still commits in order.
        Member current = awaitLeader(members);
        current.raft().submit("legit".getBytes(StandardCharsets.UTF_8));
        raftTicks(members, 8);
        for (Member member : permitted) {
            assertThat(member.applied()).containsExactly("1:legit");
        }
    }

    @Test
    void duplicateVoteGrantsFromOnePeerCountOnce() throws Exception {
        // Five declared members, three of them ghosts that never answer, so a
        // real majority needs three distinct grants (the candidate plus two).
        PeerIdentity cid = PeerIdentity.generate();
        PeerIdentity hid = PeerIdentity.generate();
        PeerIdentity h2id = PeerIdentity.generate();
        List<PeerId> memberIds = List.of(cid.peerId(), hid.peerId(), h2id.peerId(),
                PeerIdentity.generate().peerId(), PeerIdentity.generate().peerId());

        PeerNode cNode = PeerNode.builder(cid).clock(clock).randomSeed(50).build();
        cNode.listen(network.register("c"), "c");
        GroupRuntime cRt = cNode.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        PeerNode hNode = PeerNode.builder(hid).clock(clock).randomSeed(51).build();
        hNode.listen(network.register("h"), "h");
        GroupRuntime hRt = hNode.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "c", 0)));
        PeerNode h2Node = PeerNode.builder(h2id).clock(clock).randomSeed(52).build();
        h2Node.listen(network.register("h2"), "h2");
        GroupRuntime h2Rt = h2Node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "c", 0)));
        nodes.add(cNode);
        nodes.add(hNode);
        nodes.add(h2Node);
        for (int i = 0; i < 4; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // anti-entropy runs once per gossip period on the node clock
        }

        RaftLog candidate = new RaftLog(new CapabilityPipes(cRt, codec), codec,
                cid.peerId(), memberIds, clock, 42, (command, index) -> {
                });
        java.util.concurrent.atomic.AtomicReference<RaftMessages.RequestVote> request =
                new java.util.concurrent.atomic.AtomicReference<>();
        CapabilityPipes hPipes = new CapabilityPipes(hRt, codec);
        hPipes.onCapability(RaftLog.TYPE, (from, payload) -> {
            RaftMessages.Frame frame = codec.fromBytes(payload, RaftMessages.Frame.class);
            if (frame != null && frame.requestVote() != null) {
                request.set(frame.requestVote());
            }
        });
        CapabilityPipes h2Pipes = new CapabilityPipes(h2Rt, codec);

        for (int i = 0; i < 30 && request.get() == null; i++) {
            candidate.tick();
        }
        assertThat(request.get()).as("candidate started an election").isNotNull();

        // One member grants three times (a replayed grant); still one vote.
        byte[] grant = codec.toBytes(RaftMessages.Frame.of(
                new RaftMessages.VoteReply(request.get().term(), true)));
        hPipes.send(cid.peerId(), RaftLog.TYPE, grant);
        hPipes.send(cid.peerId(), RaftLog.TYPE, grant);
        hPipes.send(cid.peerId(), RaftLog.TYPE, grant);
        assertThat(candidate.isLeader())
                .as("two distinct grants of five members are no majority").isFalse();

        // A grant from a second distinct member is the real majority.
        h2Pipes.send(cid.peerId(), RaftLog.TYPE, grant);
        assertThat(candidate.isLeader())
                .as("three distinct grants elect").isTrue();
    }

    @Test
    void electsExactlyOneLeader() throws Exception {
        List<Member> members = cluster(3);
        Member leader = awaitLeader(members);
        raftTicks(members, 3);

        assertThat(members.stream().filter(m -> m.raft().isLeader())).hasSize(1);
        for (Member member : members) {
            assertThat(member.raft().leader()).contains(leader.node().peerId());
        }
    }

    @Test
    void commandsApplyInTheSameOrderEverywhere() throws Exception {
        List<Member> members = cluster(3);
        Member leader = awaitLeader(members);

        for (int i = 0; i < 5; i++) {
            assertThat(leader.raft().submit(("cmd-" + i).getBytes(StandardCharsets.UTF_8)))
                    .isTrue();
            raftTicks(members, 2);
        }
        raftTicks(members, 3);

        List<String> expected = List.of("1:cmd-0", "2:cmd-1", "3:cmd-2", "4:cmd-3", "5:cmd-4");
        for (Member member : members) {
            assertThat(member.applied()).isEqualTo(expected);
        }
    }

    @Test
    void followerSubmitsForwardToTheLeader() throws Exception {
        List<Member> members = cluster(3);
        Member leader = awaitLeader(members);
        Member follower = members.stream().filter(m -> !m.raft().isLeader()).findFirst()
                .orElseThrow();

        assertThat(follower.raft().submit("via-follower".getBytes(StandardCharsets.UTF_8)))
                .isTrue();
        raftTicks(members, 4);

        for (Member member : members) {
            assertThat(member.applied()).containsExactly("1:via-follower");
        }
        assertThat(leader.raft().commitIndex()).isEqualTo(1);
    }

    @Test
    void survivesLeaderFailureAndKeepsOrder() throws Exception {
        List<Member> members = cluster(3);
        Member firstLeader = awaitLeader(members);
        firstLeader.raft().submit("before".getBytes(StandardCharsets.UTF_8));
        raftTicks(members, 4);

        // The leader dies: partition it away and stop ticking it.
        List<Member> survivors = members.stream()
                .filter(m -> m != firstLeader).toList();
        for (Member survivor : survivors) {
            String leaderAddr = "n" + members.indexOf(firstLeader);
            network.partition("n" + members.indexOf(survivor), leaderAddr);
        }

        Member newLeader = null;
        for (int i = 0; i < 80 && newLeader == null; i++) {
            survivors.forEach(m -> m.raft().tick());
            newLeader = survivors.stream().filter(m -> m.raft().isLeader())
                    .findFirst().orElse(null);
        }
        assertThat(newLeader).isNotNull();

        newLeader.raft().submit("after".getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < 6; i++) {
            survivors.forEach(m -> m.raft().tick());
        }

        for (Member survivor : survivors) {
            assertThat(survivor.applied()).containsExactly("1:before", "2:after");
        }
    }

    private static void partitionFrom(SimNetwork network, List<Member> members, Member isolated) {
        String isolatedAddr = "n" + members.indexOf(isolated);
        for (Member other : members) {
            if (other != isolated) {
                network.partition("n" + members.indexOf(other), isolatedAddr);
            }
        }
    }

    private static Member electAmong(List<Member> group) {
        for (int i = 0; i < 80; i++) {
            group.forEach(m -> m.raft().tick());
            for (Member member : group) {
                if (member.raft().isLeader()) {
                    return member;
                }
            }
        }
        throw new AssertionError("no leader elected among " + group.size() + " members");
    }

    /** SPEC §8 ordered-log: a deposed leader's uncommitted tail is truncated on rejoin; only the majority's order is ever applied. */
    @Test
    void aDeposedLeaderTruncatesItsDivergentTailOnRejoin() throws Exception {
        List<Member> members = cluster(3);
        Member firstLeader = awaitLeader(members);
        firstLeader.raft().submit("committed".getBytes(StandardCharsets.UTF_8));
        raftTicks(members, 4);
        for (Member member : members) {
            assertThat(member.applied()).containsExactly("1:committed");
        }

        // Cut the leader off, then let it accept a command it can never commit.
        partitionFrom(network, members, firstLeader);
        assertThat(firstLeader.raft().submit("orphan".getBytes(StandardCharsets.UTF_8)))
                .as("a leader accepts locally even without a quorum").isTrue();
        for (int i = 0; i < 3; i++) {
            firstLeader.raft().tick();
        }
        assertThat(firstLeader.raft().commitIndex())
                .as("no majority, no commit of the orphan").isEqualTo(1);
        assertThat(firstLeader.applied()).containsExactly("1:committed");

        List<Member> survivors = members.stream().filter(m -> m != firstLeader).toList();
        Member newLeader = electAmong(survivors);
        assertThat(newLeader).isNotSameAs(firstLeader);
        newLeader.raft().submit("after".getBytes(StandardCharsets.UTF_8));
        raftTicks(survivors, 6);
        for (Member survivor : survivors) {
            assertThat(survivor.applied()).containsExactly("1:committed", "2:after");
        }

        // The old leader rejoins: the higher term demotes it, the conflicting
        // index-2 entry is overwritten, and the majority's history is applied.
        network.heal();
        raftTicks(members, 30);
        for (Member member : members) {
            assertThat(member.applied()).as("orphan never applies anywhere")
                    .containsExactly("1:committed", "2:after");
        }
        assertThat(firstLeader.raft().isLeader()).isFalse();
        assertThat(members.stream().filter(m -> m.raft().isLeader())).hasSize(1);
        assertThat(firstLeader.raft().leader()).contains(newLeader.node().peerId());
        assertThat(firstLeader.raft().commitIndex()).isEqualTo(2);
    }

    /** SPEC §8 ordered-log: a stale leader that keeps ticking through a partition steps down on hearing the higher term; exactly one leader remains. */
    @Test
    void aStaleLeaderStepsDownWhenItHearsAHigherTerm() throws Exception {
        List<Member> members = cluster(3);
        Member firstLeader = awaitLeader(members);
        firstLeader.raft().submit("before".getBytes(StandardCharsets.UTF_8));
        raftTicks(members, 4);

        partitionFrom(network, members, firstLeader);
        List<Member> survivors = members.stream().filter(m -> m != firstLeader).toList();

        // Everyone keeps ticking, the stale leader included: it goes on believing
        // it leads (its heartbeats fall into the partition) while the majority
        // elects a successor.
        Member newLeader = null;
        for (int i = 0; i < 80 && newLeader == null; i++) {
            members.forEach(m -> m.raft().tick());
            newLeader = survivors.stream().filter(m -> m.raft().isLeader())
                    .findFirst().orElse(null);
        }
        assertThat(newLeader).isNotNull();
        assertThat(firstLeader.raft().isLeader())
                .as("the stale leader still believes it leads while cut off").isTrue();
        assertThat(members.stream().filter(m -> m.raft().isLeader()))
                .as("a split view: two self-declared leaders").hasSize(2);

        newLeader.raft().submit("after".getBytes(StandardCharsets.UTF_8));
        raftTicks(survivors, 6);

        network.heal();
        raftTicks(members, 30);

        assertThat(members.stream().filter(m -> m.raft().isLeader()))
                .as("one leader once the partition heals").hasSize(1);
        assertThat(firstLeader.raft().isLeader()).isFalse();
        for (Member member : members) {
            assertThat(member.raft().leader()).contains(newLeader.node().peerId());
            assertThat(member.applied()).containsExactly("1:before", "2:after");
        }
    }
    /**
     * QA3 A3-4: the advertised leader lease is derived from the cadence the log
     * is really driven at, not from the one assumed at construction. A log
     * registered on a runtime whose host declares its cadence re-expresses the
     * same election timeout (5..9 ticks) in the host's wall time.
     */
    @Test
    void theAdvertisedLeaseFollowsTheCadenceTheLogIsActuallyDrivenAt() throws Exception {
        List<Member> members = cluster(3);
        // The host drives Raft itself at 300 ms, as the partybus clerks do, and
        // says so. Every member's advertisement must now speak in 300 ms ticks.
        members.forEach(m -> m.capabilities()
                .detachFromPeerTick()
                .cadence(Duration.ofMillis(300))
                .register(m.raft()));

        Member leader = awaitLeader(members);
        raftTicks(members, 3);
        clock.advance(Duration.ofMinutes(1));
        members.forEach(m -> m.capabilities().refreshTick());
        nodes.forEach(PeerNode::tick);
        clock.advance(Duration.ofSeconds(1));

        List<CapabilityAdvertisement> leaderAds = leaderAdsSeenBy(leader);
        assertThat(leaderAds).hasSize(1);
        CapabilityAdvertisement ad = leaderAds.get(0);
        java.time.Instant leaseUntil = java.time.Instant.parse(ad.parameters().get("leaseUntil"));
        // 5..9 ticks at 300 ms is 1.5..2.7 s, not the 5..9 s the 1 s default gave.
        assertThat(leaseUntil)
                .as("the lease describes the real election timeout")
                .isAfterOrEqualTo(ad.issued().plusMillis(1500))
                .isBeforeOrEqualTo(ad.issued().plusMillis(2700));
    }

    private static List<CapabilityAdvertisement> leaderAdsSeenBy(Member member) {
        return member.capabilities().providersOf(RaftLog.TYPE).stream()
                .filter(ad -> "leader".equals(ad.parameters().get("role"))).toList();
    }

    /** SPEC §8 ordered-log: the leader lease is an advertisement — exactly one refreshed ad carries role=leader with a leaseUntil, it names the elected peer, and after a failover the leader ad moves to the successor. */
    @Test
    void theLeaderLeaseIsAnAdvertisement() throws Exception {
        List<Member> members = cluster(3);
        members.forEach(m -> m.capabilities().register(m.raft()));
        for (Member member : members) {
            assertThat(member.capabilities().providersOf(RaftLog.TYPE))
                    .as("before the election every member advertises as a follower")
                    .hasSize(3)
                    .allSatisfy(ad -> {
                        assertThat(ad.parameters()).containsEntry("role", "follower");
                        assertThat(ad.parameters()).containsEntry("members", "3");
                        assertThat(ad.parameters()).doesNotContainKey("leaseUntil");
                    });
        }

        Member leader = awaitLeader(members);
        raftTicks(members, 3);
        clock.advance(Duration.ofMinutes(1)); // a newer issue time wins in the caches
        members.forEach(m -> m.capabilities().refreshTick());
        nodes.forEach(PeerNode::tick);
        clock.advance(Duration.ofSeconds(1)); // anti-entropy runs once per gossip period on the node clock

        for (Member member : members) {
            List<CapabilityAdvertisement> leaderAds = leaderAdsSeenBy(member);
            assertThat(leaderAds).as("exactly one leader ad").hasSize(1);
            CapabilityAdvertisement ad = leaderAds.get(0);
            assertThat(ad.issuer()).isEqualTo(leader.node().peerId());
            assertThat(ad.parameters()).containsEntry("term", String.valueOf(leader.raft().term()));
            java.time.Instant leaseUntil = java.time.Instant.parse(ad.parameters().get("leaseUntil"));
            // issued + electionTimeout ticks at the default 1 s tick period (5..9 ticks)
            assertThat(leaseUntil).isAfterOrEqualTo(ad.issued().plusSeconds(5))
                    .isBeforeOrEqualTo(ad.issued().plusSeconds(9));
            assertThat(member.capabilities().providersOf(RaftLog.TYPE)).hasSize(3)
                    .filteredOn(a -> !a.issuer().equals(leader.node().peerId()))
                    .allSatisfy(a -> {
                        assertThat(a.parameters()).containsEntry("role", "follower");
                        assertThat(a.parameters()).doesNotContainKey("leaseUntil");
                    });
        }

        // Failover: cut the leader off, elect a successor, then heal so the old
        // leader hears the higher term and steps down. On refresh the leader ad moves.
        partitionFrom(network, members, leader);
        List<Member> survivors = members.stream().filter(m -> m != leader).toList();
        Member newLeader = electAmong(survivors);
        assertThat(newLeader).isNotSameAs(leader);
        network.heal();
        raftTicks(members, 30);
        assertThat(leader.raft().isLeader()).isFalse();

        clock.advance(Duration.ofMinutes(1));
        members.forEach(m -> m.capabilities().refreshTick());
        nodes.forEach(PeerNode::tick);
        clock.advance(Duration.ofSeconds(1)); // anti-entropy runs once per gossip period on the node clock

        for (Member member : members) {
            List<CapabilityAdvertisement> leaderAds = leaderAdsSeenBy(member);
            assertThat(leaderAds).as("the leader ad moved").hasSize(1);
            assertThat(leaderAds.get(0).issuer()).isEqualTo(newLeader.node().peerId());
            assertThat(leaderAds.get(0).parameters())
                    .containsEntry("term", String.valueOf(newLeader.raft().term()));
        }
    }
}
