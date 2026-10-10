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

import ai.badmonkey.agentspaces.agent.capability.AggregateClient;
import ai.badmonkey.agentspaces.agent.capability.Motion;
import ai.badmonkey.agentspaces.agent.capability.VoteClient;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.capabilities.aggregate.PushSumAggregate;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * The typed capability clients behave, beyond resolving (QA-SPEC-COVERAGE §7
 * claimed them on a type-presence test): {@link VoteClient} proposes once
 * per motion, casts, tallies, and decides across two members; {@link
 * AggregateClient} starts an epoch, answers empty before any exchange, awaits
 * an estimate and a settled estimate, and returns empty on a timeout.
 */
class CapabilityClientsClusterTest {

    private static final Lease HOUR = Lease.of(Duration.ofHours(1));

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zClients");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zClients", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "clients",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Member(PeerIdentity identity, PeerNode node, ReplicatedSpace votes,
                          VoteCapability vote, PushSumAggregate aggregate, AgentSpaces.GroupContext group) {
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
        ReplicatedSpace votes = ReplicatedSpace.builder(runtime, "votes", identity, "voter")
                .clock(clock).settleWindow(Duration.ZERO).build();
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, clock), codec, identity.peerId());
        CapabilityRuntime capabilities = new CapabilityRuntime(runtime, discovery, identity);
        VoteCapability vote = new VoteCapability(votes, identity.agent("voter"), identity.peerId(), clock);
        PushSumAggregate aggregate = new PushSumAggregate(new CapabilityPipes(runtime, codec),
                runtime.sampler(), identity.peerId(), codec, clock);
        AgentSpaces spaces = new AgentSpaces(identity, clock);
        AgentSpaces.GroupContext group = spaces.register("clients", groupId, runtime, discovery)
                .capabilities(capabilities);
        group.space("votes", votes, identity.agent("voter"));
        group.provide(vote);
        group.provide(aggregate);
        nodes.add(node);
        closeables.add(spaces);
        return new Member(identity, node, votes, vote, aggregate, group);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofMillis(300));
        }
    }

    @Test
    @Timeout(60)
    void theVoteClientProposesOncePerMotionCastsTalliesAndDecidesAcrossMembers() throws Exception {
        Member a = member("a", 1);
        Member b = member("b", 2, "a");
        tickAll(4);
        VoteClient va = a.group().capability(VoteClient.class);
        VoteClient vb = b.group().capability(VoteClient.class);
        assertThat(va.capability()).isSameAs(a.vote());
        assertThat(va.providers()).as("both members advertise the vote").hasSizeGreaterThanOrEqualTo(1);

        Motion motion = Motion.of("release", "Ship 1.0?", List.of("ship", "hold"), 2, HOUR);
        va.propose(motion);
        va.propose(motion);                       // idempotent: the proposal is not reopened
        assertThat(va.proposal("release")).hasValueSatisfying(p -> assertThat(p.quorum()).isEqualTo(2));
        tickAll(6);
        assertThat(vb.proposal("release")).as("the proposal replicated").isPresent();

        va.castBallot("release", "ship", HOUR);
        tickAll(6);
        assertThat(vb.tally("release")).containsEntry("ship", 1);
        assertThat(vb.decision("release")).as("one of two voters").isEmpty();
        vb.castBallot("release", "hold", HOUR);
        tickAll(6);
        assertThat(va.tally("release")).containsEntry("ship", 1).containsEntry("hold", 1);
        assertThat(va.decision("release")).hasValueSatisfying(d -> {
            assertThat(d.winner()).as("a tie breaks to the lexicographically first option").isEqualTo("hold");
            assertThat(d.tally()).containsEntry("ship", 1).containsEntry("hold", 1);
        });
        assertThat(vb.decision("release").map(VoteCapability.Decision::winner)).hasValue("hold");
    }

    @Test
    @Timeout(60)
    void theAggregateClientStartsAwaitsSettlesAndTimesOutHonestly() throws Exception {
        Member a = member("a", 1);
        Member b = member("b", 2, "a");
        tickAll(4);
        AggregateClient ca = a.group().capability(AggregateClient.class);
        AggregateClient cb = b.group().capability(AggregateClient.class);
        assertThat(ca.capability()).isSameAs(a.aggregate());
        assertThat(ca.estimate("load")).as("nothing before any exchange").isEmpty();
        assertThat(ca.awaitEstimate("load", Duration.ofMillis(200))).as("a timeout is an empty answer").isEmpty();
        assertThat(ca.awaitSettled("load", PushSumAggregate.Settle.FIRST, Duration.ofMillis(200))).isEmpty();

        ca.start("load", 10.0);
        cb.start("load", 30.0);
        Thread waiter = Thread.ofVirtual().start(() -> {
            OptionalDouble settled = cb.awaitSettled("load", PushSumAggregate.Settle.after(3, 0.01), Duration.ofSeconds(20));
            assertThat(settled).isPresent();
            assertThat(settled.getAsDouble()).isCloseTo(20.0, within(0.5));
        });
        for (int i = 0; i < 60 && waiter.isAlive(); i++) {
            tickAll(1);
            a.aggregate().tick();
            b.aggregate().tick();
            Thread.sleep(20);
        }
        assertThat(waiter.isAlive()).as("awaitSettled returned").isFalse();
        assertThat(ca.awaitEstimate("load", Duration.ofSeconds(1))).isPresent();
        assertThat(ca.estimate("load").getAsDouble()).isCloseTo(20.0, within(0.5));
    }
}
