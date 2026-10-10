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
package ai.badmonkey.agentspaces.capabilities.aggregate;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.spi.PeerSampler;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Two hazards in push-sum settling, found on the wire on 2026-10-09 and
 * pinned here. (1) Halving leaves a node's value/weight ratio untouched, so a
 * node nobody pushes to has a perfectly stable estimate; a settle rule that
 * counted ticks fired at that node's own value while its peers were still
 * mixing (the council's 18.8, the Spring AI flake). A tick now counts only
 * when the node heard a frame for the epoch, unless it has nobody to exchange
 * with. (2) A share pushed to a member that runs no aggregate is mass lost
 * and every estimate drifts by it (the Spring AI run's 22.0); a participant
 * rule, {@code advertisedIn(discovery)}, keeps shares among members that
 * advertise the capability, and the capability runtime applies it.
 */
class PushSumHardeningTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zHarden");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zHarden", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "harden",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    record Member(PeerIdentity identity, PeerNode node, GroupRuntime runtime, DiscoveryService discovery,
                  CapabilityRuntime capabilities) {
    }

    @AfterEach
    void tearDown() {
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
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, clock), codec,
                identity.peerId());
        nodes.add(node);
        return new Member(identity, node, runtime, discovery, new CapabilityRuntime(runtime, discovery, identity));
    }

    /** An aggregate over a sampler of the test's choosing (the member's own when {@code sampler} is null). */
    private PushSumAggregate aggregate(Member m, Function<Member, PeerSampler> sampler) {
        return new PushSumAggregate(new CapabilityPipes(m.runtime(), codec),
                sampler == null ? m.runtime().sampler() : sampler.apply(m), m.identity().peerId(), codec, clock);
    }

    private static PeerSampler always(Member target) {
        return n -> List.of(target.identity().peerId());
    }

    private void tick(List<PushSumAggregate> aggregates, int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            aggregates.forEach(PushSumAggregate::tick);
            clock.advance(Duration.ofMillis(300));
        }
    }

    private void settleMembership(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofMillis(300));
        }
    }

    @Test
    void aNodeNobodyPushesToDoesNotSettleAtItsOwnValue() throws Exception {
        Member a = member("a", 1);
        Member b = member("b", 2, "a");
        Member c = member("c", 3, "a");
        settleMembership(6);
        // A pushes to B, B and C push to each other: nobody ever pushes to A.
        PushSumAggregate pa = aggregate(a, m -> always(b));
        PushSumAggregate pb = aggregate(b, m -> always(c));
        PushSumAggregate pc = aggregate(c, m -> always(b));
        List<PushSumAggregate> all = List.of(pa, pb, pc);
        pa.start("load", 10.0);
        pb.start("load", 30.0);
        pc.start("load", 20.0);
        List<Double> firedAtA = new CopyOnWriteArrayList<>();
        List<Double> firedAtB = new CopyOnWriteArrayList<>();
        pa.onEstimate("load"::equals, PushSumAggregate.Settle.after(2, 0.01), e -> firedAtA.add(e.value()));
        pb.onEstimate("load"::equals, PushSumAggregate.Settle.after(2, 0.01), e -> firedAtB.add(e.value()));
        tick(all, 30);
        // A's ratio never moved (halving keeps it), which is the hazard: it is
        // 10.0, far from any fleet value, and the rule must not call it settled.
        assertThat(pa.estimate("load")).hasValue(10.0);
        assertThat(firedAtA).as("a silent node has no evidence to settle on").isEmpty();
        // B heard C every tick and settled on what the two of them agree on.
        Thread.sleep(200);
        assertThat(firedAtB).hasSize(1);
        assertThat(firedAtB.get(0)).isCloseTo(pc.estimate("load").orElseThrow(), within(0.5));
    }

    @Test
    void aNodeWithNobodyToExchangeWithSettlesOnItsOwnTicks() throws Exception {
        Member a = member("a", 1);
        settleMembership(2);
        PushSumAggregate pa = aggregate(a, null);
        pa.start("load", 12.5);
        List<Double> fired = new CopyOnWriteArrayList<>();
        pa.onEstimate("load"::equals, PushSumAggregate.Settle.after(2, 0.01), e -> fired.add(e.value()));
        tick(List.of(pa), 5);
        Thread.sleep(200);
        assertThat(fired).containsExactly(12.5);
    }

    @Test
    void aSharePushedToAMemberWithoutTheAggregateIsMassLost() throws Exception {
        Member a = member("a", 1);
        Member b = member("b", 2, "a");
        Member bystander = member("d", 4, "a"); // a member of the group with no aggregate
        settleMembership(6);
        // A pushes its first three shares to the bystander, then to B; B pushes to A.
        int[] ticks = {0};
        PushSumAggregate pa = aggregate(a, m -> n -> List.of(
                (ticks[0]++ < 3 ? bystander : b).identity().peerId()));
        PushSumAggregate pb = aggregate(b, m -> always(a));
        pa.start("load", 10.0);
        pb.start("load", 30.0);
        tick(List.of(pa, pb), 40);
        // Seven eighths of A's mass went to a member that never mixed it back:
        // both nodes now agree on a value far above the true mean of 20.
        assertThat(pa.estimate("load").orElseThrow()).isCloseTo(pb.estimate("load").orElseThrow(), within(0.5));
        assertThat(pa.estimate("load").orElseThrow()).as("the hazard without a participant rule").isGreaterThan(25.0);
    }

    @Test
    void theAdvertisedParticipantRuleKeepsTheMassAmongAggregates() throws Exception {
        Member a = member("a", 1);
        Member b = member("b", 2, "a");
        Member c = member("c", 3, "a");
        Member bystander = member("d", 4, "a");
        settleMembership(6);
        // Registering through the capability runtime publishes the advertisement
        // and applies the rule, so shares go only to members that advertise.
        PushSumAggregate pa = aggregate(a, null);
        PushSumAggregate pb = aggregate(b, null);
        PushSumAggregate pc = aggregate(c, null);
        a.capabilities().register(pa);
        b.capabilities().register(pb);
        c.capabilities().register(pc);
        assertThat(pa.hasParticipantRule()).isTrue();
        settleMembership(6); // the advertisements reach every cache
        pa.start("load", 10.0);
        pb.start("load", 30.0);
        pc.start("load", 20.0);
        List<PushSumAggregate> all = List.of(pa, pb, pc);
        tick(all, 80);
        for (PushSumAggregate aggregate : all) {
            assertThat(aggregate.estimate("load").orElseThrow()).as("no mass reached the bystander")
                    .isCloseTo(20.0, within(0.2));
        }
        assertThat(bystander.discovery().find(ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement.class,
                ad -> PushSumAggregate.TYPE.equals(ad.capabilityType()))).hasSize(3);
    }
}
