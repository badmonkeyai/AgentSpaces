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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.OnEstimate;
import ai.badmonkey.agentspaces.agent.annotation.SpaceNotify;
import ai.badmonkey.agentspaces.agent.capability.AggregateClient;
import ai.badmonkey.agentspaces.agent.capability.Contribution;
import ai.badmonkey.agentspaces.api.ad.CardAction;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.aggregate.PushSumAggregate;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * {@code @OnEstimate} (ISSUE-OnEstimate) over two members on the peer tick:
 * sensors contribute, the supervisor reacts once when the epoch settles,
 * {@code awaitSettled} is the procedural form, and the annotation refuses
 * to bind without an aggregate.
 */
class AgentBinderEstimateClusterTest {

    public record Reading(String topic, double value) {
    }

    public record Report(String epoch, double value, int ticks) {
    }

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zEstimate");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zEstimate", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "estimate",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Member(PeerIdentity identity, PeerNode node, ReplicatedSpace space,
                          PushSumAggregate aggregate, AgentSpaces.GroupContext group) {
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
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "readings", identity, "sensor")
                .clock(clock).settleWindow(Duration.ZERO).build();
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, clock), codec,
                identity.peerId());
        CapabilityRuntime capabilities = new CapabilityRuntime(runtime, discovery, identity);
        PushSumAggregate aggregate = new PushSumAggregate(new CapabilityPipes(runtime, codec),
                runtime.sampler(), identity.peerId(), codec, clock);
        AgentSpaces spaces = new AgentSpaces(identity, clock);
        AgentSpaces.GroupContext group = spaces.register("estimate", groupId, runtime, discovery)
                .capabilities(capabilities);
        group.space("readings", space);
        group.provide(aggregate);
        nodes.add(node);
        closeables.add(spaces);
        return new Member(identity, node, space, aggregate, group);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofMillis(300));
        }
    }

    @AgentSpec(name = "sensor", description = "Contributes readings", goals = {"sense"})
    public static class Sensor {
        @SpaceNotify(space = "readings")
        public Contribution feel(Reading reading) {
            return Contribution.to(reading.topic(), reading.value());
        }
    }

    @AgentSpec(name = "supervisor", description = "Reports settled estimates", goals = {"report"})
    public static class Supervisor {
        final List<PushSumAggregate.Estimate> seen = new CopyOnWriteArrayList<>();

        @OnEstimate(prefix = "backlog", settleTicks = 3, tolerance = 0.01, resultSpace = "readings")
        public Report report(PushSumAggregate.Estimate estimate) {
            seen.add(estimate);
            return new Report(estimate.epochId(), estimate.value(), estimate.ticks());
        }
    }

    @Test
    void theSupervisorReactsOnceWhenTheEpochSettlesAndTheClientCanAwaitIt() throws Exception {
        Member a = member("a", 1);
        Member b = member("b", 2, "a");
        tickAll(4);
        Supervisor supervisor = new Supervisor();
        closeables.add(a.group().bind(new Sensor()));
        closeables.add(b.group().bind(new Sensor()));
        AgentBinder.Bound bound = a.group().bind(supervisor);
        closeables.add(bound);
        assertThat(bound.card().actions().get(0).kind()).isEqualTo(CardAction.ON_ESTIMATE);
        assertThat(bound.card().actions().get(0).produces()).containsExactly(Report.class.getName() + "#v1");

        a.space().write(new Reading("backlog", 40.0), Lease.of(Duration.ofMinutes(5)));
        b.space().write(new Reading("backlog", 20.0), Lease.of(Duration.ofMinutes(5)));
        b.space().write(new Reading("other", 99.0), Lease.of(Duration.ofMinutes(5)));   // outside the prefix
        await(() -> !a.space().readAll(Template.of(Report.class), 10).isEmpty(), Duration.ofSeconds(30));
        Report report = a.space().readAll(Template.of(Report.class), 10).get(0);
        assertThat(report.epoch()).isEqualTo("backlog");
        assertThat(report.value()).isCloseTo(30.0, within(0.5));
        assertThat(report.ticks()).as("settled after at least three unchanged ticks").isGreaterThanOrEqualTo(3);
        tickAll(10);
        assertThat(supervisor.seen).as("once per epoch").hasSize(1);
        assertThat(a.space().readAll(Template.of(Report.class), 10)).hasSize(1);

        AggregateClient client = b.group().capability(AggregateClient.class);
        Thread waiter = Thread.ofVirtual().start(() -> {
            OptionalDouble settled = client.awaitSettled("backlog", PushSumAggregate.Settle.after(2, 0.01),
                    Duration.ofSeconds(20));
            assertThat(settled).isPresent();
            assertThat(settled.getAsDouble()).isCloseTo(30.0, within(0.5));
        });
        for (int i = 0; i < 40 && waiter.isAlive(); i++) {
            tickAll(1);
            Thread.sleep(20);
        }
        assertThat(waiter.isAlive()).as("awaitSettled returned").isFalse();
    }

    public static class Aggregateless {
        @OnEstimate(prefix = "backlog")
        public void report(PushSumAggregate.Estimate estimate) {
        }
    }

    @Test
    void anOnEstimateWithoutAnAggregateIsRefusedAtBindTime() {
        PeerIdentity identity = PeerIdentity.generate();
        try (LocalSpace space = LocalSpace.builder("readings", identity.agent("host")).build()) {
            AgentBinder binder = new AgentBinder(identity, GroupId.of("zEstimate"), null, clock)
                    .space("readings", space);
            try {
                assertThatThrownBy(() -> binder.bind(new Aggregateless()))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("group.provide(aggregate)");
            } finally {
                binder.close();
            }
        }
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
