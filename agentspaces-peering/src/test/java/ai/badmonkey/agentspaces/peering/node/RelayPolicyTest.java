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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The relay's membership check and policy seam (SPEC §5.4, ISSUE-PeerRobustness-2,
 * R2-3; ISSUE-BootstrapCore FR-2, SR-2, PR-2): a relay forwards only to a member of
 * the group the frame names, asks the {@link RelayPolicy} before each forward, drops
 * on a refusal or a failure, and counts everything per group and per verified
 * target. The NAT peer is partitioned from Alice so that every frame between them,
 * probes included, must cross the relay; the assertions are therefore deltas and
 * invariants around the one data frame the test sends.
 */
class RelayPolicyTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupAdvertisement groupA = group("zRelayPolicyA");
    private final GroupAdvertisement groupB = group("zRelayPolicyB");

    private static GroupAdvertisement group(String id) {
        return new GroupAdvertisement("aspace://" + id, PeerIdentity.generate().peerId(), GroupId.of(id),
                Instant.EPOCH, Duration.ofDays(1), id, GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
    }

    /** One policy call, as the relay made it. */
    private record Asked(GroupId group, PeerId from, PeerId target, int bytes) {
    }

    /** The §5.4 fleet once joined and settled. */
    private record Fleet(PeerNode relay, PeerNode alice, GroupRuntime aliceRuntime, PeerNode nat,
                         GroupRuntime natRuntime, List<String> natSaw) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode relay(RelayPolicy policy) throws IOException {
        PeerNode.Builder builder = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(1)
                .roles(Set.of(PeerAdvertisement.PeerRole.RELAY));
        if (policy != null) {
            builder.relayPolicy(policy);
        }
        PeerNode node = builder.build();
        node.listen(network.register("relay"), "relay");
        nodes.add(node);
        return node;
    }

    private PeerNode listening(String address, long seed) throws IOException {
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        nodes.add(node);
        return node;
    }

    /** A dial-only node: it can reach others, nobody can dial it. */
    private PeerNode natNode(String address, long seed) {
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(seed).build();
        node.transport(network.register(address));
        nodes.add(node);
        return node;
    }

    private static List<PeerAdvertisement.Endpoint> seed(String address) {
        return List.of(new PeerAdvertisement.Endpoint("mem", address, 0));
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    /** A relay, Alice, and a NAT peer Alice can reach only through the relay (the pair is partitioned). */
    private Fleet fleet(RelayPolicy policy) throws IOException {
        PeerNode relay = relay(policy);
        PeerNode alice = listening("alice", 2);
        PeerNode nat = natNode("nat", 3);
        network.partition("nat", "alice");
        relay.joinGroup(groupA, GroupMembership.Config.defaults(), List.of());
        GroupRuntime aliceRuntime = alice.joinGroup(groupA, GroupMembership.Config.defaults(), seed("relay"));
        GroupRuntime natRuntime = nat.joinGroup(groupA, GroupMembership.Config.defaults(), seed("relay"));
        tickAll(4);
        assertThat(aliceRuntime.membership().member(nat.peerId())).isPresent();
        List<String> natSaw = new CopyOnWriteArrayList<>();
        natRuntime.onKind(Envelope.Kind.PIPE_DATA, (from, body) ->
                natSaw.add(new String(body, StandardCharsets.UTF_8)));
        return new Fleet(relay, alice, aliceRuntime, nat, natRuntime, natSaw);
    }

    private static PeerNode.RelayStats stats(PeerNode relay, GroupId group) {
        return relay.relayStats().getOrDefault(group, new PeerNode.RelayStats(0, 0, 0, 0));
    }

    /** R2-3.2: the default forwards every frame the membership check allows, and counts it per group and per target. */
    @Test
    void defaultForwards() throws Exception {
        Fleet fleet = fleet(null);
        PeerNode.RelayStats before = stats(fleet.relay(), groupA.group());
        assertThat(before.droppedNotMember()).isZero();
        assertThat(before.droppedByPolicy()).isZero();
        Map<PeerId, PeerNode.RelayStats> perTargetBefore = fleet.relay().relayStats(groupA.group());
        long natBefore = perTargetBefore.getOrDefault(fleet.nat().peerId(), new PeerNode.RelayStats(0, 0, 0, 0))
                .framesForwarded();

        fleet.aliceRuntime().send(fleet.nat().peerId(), Envelope.Kind.PIPE_DATA, "hello".getBytes(StandardCharsets.UTF_8));

        assertThat(fleet.natSaw()).containsExactly("hello");
        PeerNode.RelayStats after = stats(fleet.relay(), groupA.group());
        assertThat(after.framesForwarded()).isEqualTo(before.framesForwarded() + 1);
        assertThat(after.bytesForwarded()).isGreaterThan(before.bytesForwarded());
        assertThat(after.droppedByPolicy()).isZero();
        assertThat(fleet.relay().relayStats(groupA.group()).get(fleet.nat().peerId()).framesForwarded())
                .isEqualTo(natBefore + 1);
    }

    /** R2-3.2: a refusing policy drops the frame and counts the drop; it saw the group, the sender, the target, and the bytes. */
    @Test
    void policyDenies() throws Exception {
        List<Asked> asked = new CopyOnWriteArrayList<>();
        Fleet fleet = fleet((group, from, target, bytes) -> {
            asked.add(new Asked(group, from, target, bytes));
            return false;
        });
        PeerNode.RelayStats before = stats(fleet.relay(), groupA.group());
        asked.clear();

        fleet.aliceRuntime().send(fleet.nat().peerId(), Envelope.Kind.PIPE_DATA, "hello".getBytes(StandardCharsets.UTF_8));

        assertThat(fleet.natSaw()).isEmpty();
        PeerNode.RelayStats after = stats(fleet.relay(), groupA.group());
        assertThat(after.droppedByPolicy()).isEqualTo(before.droppedByPolicy() + 1);
        assertThat(after.framesForwarded()).as("nothing was forwarded, before or after").isZero();
        assertThat(asked).hasSize(1);
        assertThat(asked.get(0).group()).isEqualTo(groupA.group());
        assertThat(asked.get(0).from()).isEqualTo(fleet.alice().peerId());
        assertThat(asked.get(0).target()).isEqualTo(fleet.nat().peerId());
        assertThat(asked.get(0).bytes()).isGreaterThan(0);
        assertThat(fleet.relay().relayStats(groupA.group()).get(fleet.nat().peerId()).droppedByPolicy())
                .isGreaterThanOrEqualTo(1);
    }

    /** R2-S3: a policy that throws fails closed; the frame is dropped and counted, and the relay goes on. */
    @Test
    void policyThrows() throws Exception {
        Fleet fleet = fleet((group, from, target, bytes) -> {
            throw new IllegalStateException("quota store down");
        });
        PeerNode.RelayStats before = stats(fleet.relay(), groupA.group());

        fleet.aliceRuntime().send(fleet.nat().peerId(), Envelope.Kind.PIPE_DATA, "hello".getBytes(StandardCharsets.UTF_8));

        assertThat(fleet.natSaw()).isEmpty();
        PeerNode.RelayStats after = stats(fleet.relay(), groupA.group());
        assertThat(after.droppedByPolicy()).isEqualTo(before.droppedByPolicy() + 1);
        assertThat(after.framesForwarded()).isZero();
        // The relay still serves the group: Alice and the NAT peer stay in its view.
        tickAll(4);
        assertThat(fleet.relay().group(groupA.group()).orElseThrow().membership().allMembers()).hasSize(2);
    }

    /** ISSUE-BootstrapCore FR-2, SR-2: counts per target, keyed by the verified member's PeerId, sum to the group total. */
    @Test
    void perTargetCounts() throws Exception {
        Fleet fleet = fleet(null);
        PeerNode nat2 = natNode("nat2", 4);
        network.partition("nat2", "alice");
        nat2.joinGroup(groupA, GroupMembership.Config.defaults(), seed("relay"));
        tickAll(4);
        PeerId nat = fleet.nat().peerId();
        Map<PeerId, PeerNode.RelayStats> before = fleet.relay().relayStats(groupA.group());
        long natBefore = before.getOrDefault(nat, new PeerNode.RelayStats(0, 0, 0, 0)).framesForwarded();
        long nat2Before = before.getOrDefault(nat2.peerId(), new PeerNode.RelayStats(0, 0, 0, 0)).framesForwarded();

        fleet.aliceRuntime().send(nat, Envelope.Kind.PIPE_DATA, "one".getBytes(StandardCharsets.UTF_8));
        fleet.aliceRuntime().send(nat, Envelope.Kind.PIPE_DATA, "two".getBytes(StandardCharsets.UTF_8));
        fleet.aliceRuntime().send(nat2.peerId(), Envelope.Kind.PIPE_DATA, "three".getBytes(StandardCharsets.UTF_8));

        Map<PeerId, PeerNode.RelayStats> perTarget = fleet.relay().relayStats(groupA.group());
        assertThat(perTarget.get(nat).framesForwarded()).isEqualTo(natBefore + 2);
        assertThat(perTarget.get(nat2.peerId()).framesForwarded()).isEqualTo(nat2Before + 1);
        PeerNode.RelayStats total = stats(fleet.relay(), groupA.group());
        assertThat(perTarget.values().stream().mapToLong(PeerNode.RelayStats::framesForwarded).sum())
                .as("the per-target counts sum to the group total").isEqualTo(total.framesForwarded());
        assertThat(perTarget.values().stream().mapToLong(PeerNode.RelayStats::bytesForwarded).sum())
                .isEqualTo(total.bytesForwarded());
        assertThat(perTarget.keySet()).as("every key is a member the relay verified")
                .allSatisfy(peer -> assertThat(fleet.relay().group(groupA.group()).orElseThrow()
                        .membership().member(peer)).isPresent());
        assertThat(fleet.relay().relayStats(groupB.group())).as("a group never relayed in").isEmpty();
    }

    /** ISSUE-BootstrapCore FR-2: the daily budget drops past the group's bytes for the day and opens again the next UTC day. */
    @Test
    void dailyBudgetDropsAndResets() throws Exception {
        RelayPolicy budget = RelayPolicy.dailyBudget(100, clock);
        GroupId group = groupA.group();
        PeerId from = PeerIdentity.generate().peerId();
        PeerId target = PeerIdentity.generate().peerId();
        assertThat(budget.allow(group, from, target, 60)).isTrue();
        assertThat(budget.allow(group, from, target, 40)).isTrue();
        assertThat(budget.allow(group, from, target, 1)).as("past the budget").isFalse();
        assertThat(budget.allow(groupB.group(), from, target, 100)).as("another group has its own budget").isTrue();
        clock.advance(Duration.ofDays(1));
        assertThat(budget.allow(group, from, target, 100)).as("the next UTC day starts afresh").isTrue();
        assertThat(budget.allow(group, from, target, 1)).isFalse();

        // Through the relay: twenty data frames against a budget a few of them exhaust. The probes
        // of the settling fleet cross the relay too, so the day is turned over first and no tick runs
        // between the sends.
        Fleet fleet = fleet(RelayPolicy.dailyBudget(2_000, clock));
        clock.advance(Duration.ofDays(1));
        PeerNode.RelayStats before = stats(fleet.relay(), group);
        byte[] body = new byte[200];
        for (int i = 0; i < 20; i++) {
            fleet.aliceRuntime().send(fleet.nat().peerId(), Envelope.Kind.PIPE_DATA, body);
        }
        assertThat(fleet.natSaw()).as("some got through, then the budget shut").hasSizeBetween(1, 19);
        PeerNode.RelayStats stats = stats(fleet.relay(), group);
        assertThat(stats.bytesForwarded() - before.bytesForwarded()).isLessThanOrEqualTo(2_000);
        assertThat(stats.framesForwarded() - before.framesForwarded()).isEqualTo(fleet.natSaw().size());
        assertThat(stats.droppedByPolicy()).isGreaterThanOrEqualTo(1);
        assertThat(fleet.relay().relayStats(group).get(fleet.nat().peerId()).droppedByPolicy())
                .isGreaterThanOrEqualTo(1);
    }

    /**
     * R2-3.1: a frame whose target the relay holds a link to through another group, and who is no
     * member of the frame's group, is dropped before the policy is asked, and counted.
     */
    @Test
    void notMemberDropped() throws Exception {
        List<Asked> asked = new CopyOnWriteArrayList<>();
        PeerNode relay = relay((group, from, target, bytes) -> {
            asked.add(new Asked(group, from, target, bytes));
            return true;
        });
        PeerNode alice = listening("alice", 2);
        PeerNode other = listening("other", 3);
        relay.joinGroup(groupA, GroupMembership.Config.defaults(), List.of());
        relay.joinGroup(groupB, GroupMembership.Config.defaults(), List.of());
        alice.joinGroup(groupA, GroupMembership.Config.defaults(), seed("relay"));
        GroupRuntime otherRuntime = other.joinGroup(groupB, GroupMembership.Config.defaults(), seed("relay"));
        tickAll(4);
        assertThat(relay.connectedTo(other.peerId())).as("the relay holds a link to other, through group B").isTrue();
        List<String> otherSaw = new CopyOnWriteArrayList<>();
        otherRuntime.onKind(Envelope.Kind.PIPE_DATA, (from, body) ->
                otherSaw.add(new String(body, StandardCharsets.UTF_8)));
        asked.clear();

        // Alice, a member of A, asks the relay to carry a frame of A to other, who is in B alone.
        clock.advance(Duration.ofSeconds(1));
        byte[] inner = TestFrames.signed(alice.identity(), groupA.group(), Envelope.Kind.PIPE_DATA,
                other.peerId(), clock, 0, "leak".getBytes(StandardCharsets.UTF_8));
        TransportConnection injector = network.register("injector").dial("relay");
        injector.send(TestFrames.signed(alice.identity(), groupA.group(), Envelope.Kind.RELAY_FRAME,
                relay.peerId(), clock, 1, new Bodies.RelayFrame(other.peerId(), inner)));

        assertThat(otherSaw).isEmpty();
        PeerNode.RelayStats stats = stats(relay, groupA.group());
        assertThat(stats.droppedNotMember()).isEqualTo(1);
        assertThat(stats.framesForwarded()).isZero();
        assertThat(asked).as("the membership check runs before the policy").isEmpty();
        assertThat(relay.relayStats(groupA.group())).as("an unverified target gets no per-target entry (SR-2)")
                .doesNotContainKey(other.peerId());
        assertThat(relay.relayStats()).as("nothing was relayed in B").doesNotContainKey(groupB.group());
    }
}
