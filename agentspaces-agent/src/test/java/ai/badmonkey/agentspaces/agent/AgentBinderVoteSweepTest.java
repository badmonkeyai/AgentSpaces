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

import static org.assertj.core.api.Assertions.assertThat;

import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.Ballot;
import ai.badmonkey.agentspaces.agent.annotation.OnDecision;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.capabilities.vote.VoteCapability;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Owner decision 23 (srad/replication-by-entry.md, 2026-11-16): a {@code @Ballot} or {@code @OnDecision} binding hears
 * the proposals and decisions this replica already holds when it binds, as well as later ones, each once (SPEC §10,
 * "held and future"). R2 found the gap in the clients: a council worker's proposal came by rumor a moment before its
 * agent bound, a pull with the replica's own digest never sends it again, and a binding that only watched later writes
 * never voted. Here a member binds after the votes folded, and again after a restart.
 */
class AgentBinderVoteSweepTest {

    private static final Lease HOUR = Lease.of(Duration.ofHours(1));
    private static final List<String> OPTIONS = List.of("approve", "deny");

    public record Verdict(String proposalId, String winner) {
    }

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zSweep");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zSweep", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "votes",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Member(PeerIdentity identity, PeerNode node, ReplicatedSpace votes, VoteCapability vote,
                          AgentSpaces spaces, AgentSpaces.GroupContext group) {
    }

    @AfterEach
    void tearDown() {
        for (AutoCloseable c : closeables) {
            try {
                c.close();
            } catch (Exception ignored) {
                // best effort
            }
        }
        nodes.forEach(PeerNode::close);
    }

    private Member member(PeerIdentity identity, String address, long seed, String... seeds) throws IOException {
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String s : seeds) {
            endpoints.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), endpoints);
        ReplicatedSpace votes = ReplicatedSpace.builder(runtime, "votes", identity, "member")
                .clock(clock).settleWindow(Duration.ZERO).build();
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, clock), codec,
                identity.peerId());
        CapabilityRuntime capabilities = new CapabilityRuntime(runtime, discovery, identity);
        VoteCapability vote = new VoteCapability(votes, identity.agent("member"), identity.peerId(), clock);
        AgentSpaces spaces = new AgentSpaces(identity, clock);
        AgentSpaces.GroupContext group = spaces.register("votes", groupId, runtime, discovery)
                .capabilities(capabilities);
        group.space("votes", votes);
        group.provide(vote);
        nodes.add(node);
        closeables.add(spaces);
        return new Member(identity, node, votes, vote, spaces, group);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofMillis(300));
        }
    }

    @AgentSpec(name = "panelist", description = "Approves everything", goals = {"vote"})
    public static class Panelist {
        final List<String> seen = new CopyOnWriteArrayList<>();

        @Ballot(space = "votes", prefix = "case:")
        public String judge(VoteCapability.Proposal proposal) {
            seen.add(proposal.proposalId());
            return "approve";
        }
    }

    @AgentSpec(name = "recorder", description = "Records decisions", goals = {"record"})
    public static class Recorder {
        final List<String> decided = new CopyOnWriteArrayList<>();

        @OnDecision(space = "votes", prefix = "case:")
        public Verdict record(VoteCapability.Decision decision) {
            decided.add(decision.proposalId());
            return new Verdict(decision.proposalId(), decision.winner());
        }
    }

    /** The ballots a member's agents wrote for a proposal, as a replica reads them. */
    private static long ballotsBy(Space space, Member voter, String proposalId) {
        return space.readAllIssued(Template.of(VoteCapability.Ballot.class), 1_000).stream()
                .filter(i -> i.entry().proposalId().equals(proposalId))
                .filter(i -> i.issuer().peer().equals(voter.identity().peerId()))
                .count();
    }

    @Test
    void aBindingMadeAfterTheVotesFoldedVotesAndHearsEachOnce() throws Exception {
        Member a = member(PeerIdentity.generate(), "a", 1);
        Member b = member(PeerIdentity.generate(), "b", 2, "a");
        tickAll(4);
        a.vote().propose("case:open", "ship 1.0?", OPTIONS, 2, HOUR);
        a.vote().propose("case:closed", "ship 0.9?", OPTIONS, 1, HOUR);
        a.vote().castBallot("case:closed", "deny", HOUR);
        a.vote().propose("ops:1", "restart?", OPTIONS, 1, HOUR);
        await(() -> b.votes().readAll(Template.of(VoteCapability.Proposal.class), 10).size() == 3
                && b.vote().decision("case:closed").isPresent(), Duration.ofSeconds(30));

        // The proposals and the closed vote folded at b before anything there was bound.
        Panelist panelist = new Panelist();
        Recorder recorder = new Recorder();
        long started = System.nanoTime();
        closeables.add(b.group().bind(panelist));
        closeables.add(b.group().bind(recorder));
        await(() -> ballotsBy(a.votes(), b, "case:open") == 1 && recorder.decided.contains("case:closed"),
                Duration.ofSeconds(30));
        System.out.printf("late bind: ballot at a and held decision heard within %.3f s%n",
                (System.nanoTime() - started) / 1e9);

        // A later decision is heard too: a's ballot closes the open vote.
        a.vote().castBallot("case:open", "approve", HOUR);
        await(() -> recorder.decided.contains("case:open"), Duration.ofSeconds(30));
        tickAll(10);
        assertThat(panelist.seen).as("each held proposal under the prefix judged once")
                .containsExactlyInAnyOrder("case:open", "case:closed");
        assertThat(recorder.decided).as("the held decision and the later one, each once")
                .containsExactlyInAnyOrder("case:closed", "case:open");
        assertThat(ballotsBy(b.votes(), b, "case:open")).isEqualTo(1);
        assertThat(ballotsBy(b.votes(), b, "case:closed")).isEqualTo(1);
        assertThat(ballotsBy(b.votes(), b, "ops:1")).as("the prefix still filters what the sweep reads").isZero();
    }

    @Test
    void aBindingMadeAgainAfterARestartVotesOnceAndHearsHeldDecisions() throws Exception {
        Member a = member(PeerIdentity.generate(), "a", 1);
        PeerIdentity bIdentity = PeerIdentity.generate();
        Member b = member(bIdentity, "b", 2, "a");
        tickAll(4);
        Panelist before = new Panelist();
        b.group().bind(before);
        a.vote().propose("case:1", "ship 1.0?", OPTIONS, 2, HOUR);
        await(() -> ballotsBy(a.votes(), b, "case:1") == 1, Duration.ofSeconds(30));
        assertThat(before.seen).containsExactly("case:1");

        // b goes down; its binding and its replica go with it. A vote opens while it is away.
        b.spaces().close();
        b.node().close();
        nodes.remove(b.node());
        a.vote().propose("case:2", "ship 1.1?", OPTIONS, 2, HOUR);
        a.vote().castBallot("case:1", "approve", HOUR);
        assertThat(a.vote().decision("case:1")).as("b's ballot from before the restart counted").isPresent();

        // b comes back under the same identity and catches up before its agents bind.
        Member back = member(bIdentity, "b2", 3, "a");
        await(() -> back.votes().readAll(Template.of(VoteCapability.Proposal.class), 10).size() == 2
                && back.vote().decision("case:1").isPresent(), Duration.ofSeconds(30));
        Panelist after = new Panelist();
        Recorder recorder = new Recorder();
        closeables.add(back.group().bind(after));
        closeables.add(back.group().bind(recorder));
        await(() -> ballotsBy(a.votes(), back, "case:2") == 1 && recorder.decided.contains("case:1"),
                Duration.ofSeconds(30));
        a.vote().castBallot("case:2", "approve", HOUR);
        await(() -> recorder.decided.contains("case:2"), Duration.ofSeconds(30));
        tickAll(10);

        assertThat(after.seen).as("the proposal it already voted on is not judged again")
                .containsExactly("case:2");
        assertThat(ballotsBy(a.votes(), back, "case:1")).as("one ballot of b's for the first vote").isEqualTo(1);
        assertThat(ballotsBy(a.votes(), back, "case:2")).as("and one for the vote opened while it was down")
                .isEqualTo(1);
        assertThat(recorder.decided).as("the held decision and the later one, each once")
                .containsExactlyInAnyOrder("case:1", "case:2");
    }

    private void await(BooleanSupplier condition, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within " + timeout);
            }
            tickAll(1);
            Thread.sleep(20);
        }
    }
}
