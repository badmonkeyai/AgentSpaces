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
import ai.badmonkey.agentspaces.agent.annotation.Propose;
import ai.badmonkey.agentspaces.agent.annotation.SpaceNotify;
import ai.badmonkey.agentspaces.agent.capability.Motion;
import ai.badmonkey.agentspaces.agent.capability.VoteClient;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
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
 * A vote opened on one peer, cast on two others, decided once (ISSUE-Motion
 * and ISSUE-Propose, end to end): a {@code Motion} lead and a {@code @Propose}
 * lead on peer A, {@code @Ballot} panelists on B and C, an {@code @OnDecision}
 * recorder on A; and {@code VoteClient.propose(Motion)} idempotent.
 */
class AgentBinderVoteClusterTest {

    public record Cue(String caseId, String text) {
    }

    public record Verdict(String proposalId, String winner) {
    }

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zVotes");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zVotes", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "votes",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Member(PeerIdentity identity, PeerNode node, ReplicatedSpace cues,
                          ReplicatedSpace votes, VoteCapability vote, AgentSpaces.GroupContext group) {
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

    private Member member(String address, long seed, String... seeds) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String s : seeds) {
            endpoints.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), endpoints);
        ReplicatedSpace cues = ReplicatedSpace.builder(runtime, "cues", identity, "member")
                .clock(clock).settleWindow(Duration.ZERO).build();
        ReplicatedSpace votes = ReplicatedSpace.builder(runtime, "votes", identity, "member")
                .clock(clock).settleWindow(Duration.ZERO).build();
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, clock), codec,
                identity.peerId());
        CapabilityRuntime capabilities = new CapabilityRuntime(runtime, discovery, identity);
        VoteCapability vote = new VoteCapability(votes, identity.agent("member"), identity.peerId(), clock);
        AgentSpaces spaces = new AgentSpaces(identity, clock);
        AgentSpaces.GroupContext group = spaces.register("votes", groupId, runtime, discovery)
                .capabilities(capabilities);
        group.space("cues", cues);
        group.space("votes", votes);
        group.provide(vote);
        nodes.add(node);
        closeables.add(spaces);
        return new Member(identity, node, cues, votes, vote, group);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofMillis(300));
        }
    }

    @AgentSpec(name = "motion-lead", description = "Opens by motion", goals = {"open"})
    public static class MotionLead {
        @SpaceNotify(space = "cues")
        public Motion open(Cue cue) {
            return cue.caseId().startsWith("m") ? Motion.of("case:" + cue.caseId(), cue.text(),
                    List.of("approve", "deny"), 2, Lease.of(Duration.ofHours(1))) : null;
        }
    }

    @AgentSpec(name = "propose-lead", description = "Opens by annotation", goals = {"open"})
    public static class ProposeLead {
        @Propose(space = "cues", vote = "votes", prefix = "case:", key = "caseId", tags = "kind=p",
                options = {"approve", "deny"}, quorum = 2, lease = "1h")
        public String open(Cue cue) {
            return cue.text();
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

        @OnDecision(space = "votes", prefix = "case:", resultSpace = "cues")
        public Verdict record(VoteCapability.Decision decision) {
            decided.add(decision.proposalId());
            return new Verdict(decision.proposalId(), decision.winner());
        }
    }

    @Test
    void aVoteOpenedOnOnePeerIsCastOnOthersAndDecidedOnce() throws Exception {
        Member a = member("a", 1);
        Member b = member("b", 2, "a");
        Member c = member("c", 3, "a");
        tickAll(4);
        Recorder recorder = new Recorder();
        Panelist pb = new Panelist();
        Panelist pc = new Panelist();
        closeables.add(a.group().bind(new MotionLead()));
        closeables.add(a.group().bind(new ProposeLead()));
        closeables.add(a.group().bind(recorder));
        closeables.add(b.group().bind(pb));
        closeables.add(c.group().bind(pc));
        tickAll(2);

        a.cues().write(new Cue("m1", "by motion?"), Lease.of(Duration.ofMinutes(10)));
        a.cues().write(new Cue("p1", "by annotation?"), Lease.of(Duration.ofMinutes(10)),
                java.util.Map.of("kind", "p"));
        a.cues().write(new Cue("p2", "unfiltered"), Lease.of(Duration.ofMinutes(10)));   // no tag: no vote
        await(() -> a.cues().readAll(Template.of(Verdict.class), 10).size() >= 2, Duration.ofSeconds(30));
        tickAll(4);
        List<Verdict> verdicts = a.cues().readAll(Template.of(Verdict.class), 10);
        assertThat(verdicts).extracting(Verdict::proposalId).containsExactlyInAnyOrder("case:m1", "case:p1");
        assertThat(verdicts).extracting(Verdict::winner).containsOnly("approve");
        assertThat(recorder.decided).as("decided once each").hasSize(2);
        assertThat(pb.seen).containsExactlyInAnyOrder("case:m1", "case:p1");
        assertThat(pc.seen).containsExactlyInAnyOrder("case:m1", "case:p1");
        assertThat(a.vote().proposal("case:p2")).as("the filter kept the untagged cue out").isEmpty();
        assertThat(b.votes().readAll(Template.of(VoteCapability.Proposal.class), 10)).hasSize(2);

        // VoteClient.propose(Motion) is idempotent (ISSUE-Motion FR-8).
        VoteClient client = a.group().capability(VoteClient.class);
        Motion motion = Motion.of("case:x", "client?", List.of("approve", "deny"), 2, Lease.of(Duration.ofHours(1)));
        client.propose(motion).propose(motion);
        assertThat(a.votes().readAll(Template.of(VoteCapability.Proposal.class), 10))
                .filteredOn(p -> p.proposalId().equals("case:x")).hasSize(1);
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
