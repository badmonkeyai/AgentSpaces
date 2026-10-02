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

import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.OrderedTake;
import ai.badmonkey.agentspaces.agent.annotation.SpaceNotify;
import ai.badmonkey.agentspaces.agent.capability.Contribution;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.aggregate.PushSumAggregate;
import ai.badmonkey.agentspaces.capabilities.orderedlog.OrderedTakes;
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
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The Layer 4 annotations that need a fleet: {@code @OrderedTake} over a
 * three-member log, and a {@link Contribution} returned from {@code @SpaceNotify}
 * (LAYER4-ANNOTATIONS.md §2.3, §2.5).
 */
class AgentBinderLayer4ClusterTest {

    public record Order(String id, long cents) {
    }

    public record Receipt(String id, String clerk) {
    }

    public record Reading(String topic, double value) {
    }

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zLayer4");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zLayer4", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "layer4",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Member(PeerIdentity identity, PeerNode node, GroupRuntime runtime,
                          ReplicatedSpace space, CapabilityRuntime capabilities,
                          AgentSpaces.GroupContext group) {
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

    private Member member(String address, long seed, String spaceName, String... seeds) throws IOException {
        return member(PeerIdentity.generate(), null, address, seed, spaceName, seeds);
    }

    private Member member(PeerIdentity identity,
                          java.util.function.Function<String, ai.badmonkey.agentspaces.api.spi.AgentIdentity> identities,
                          String address, long seed, String spaceName, String... seeds) throws IOException {
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String s : seeds) {
            endpoints.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), endpoints);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, spaceName, identity, "clerk")
                .clock(clock).settleWindow(Duration.ZERO).build();
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, clock), codec,
                identity.peerId());
        CapabilityRuntime capabilities = new CapabilityRuntime(runtime, discovery, identity);
        AgentSpaces spaces = new AgentSpaces(identity, clock, identities);
        AgentSpaces.GroupContext group = spaces.register("layer4", groupId, runtime, discovery)
                .capabilities(capabilities);
        group.space(spaceName, space);
        nodes.add(node);
        closeables.add(spaces);
        return new Member(identity, node, runtime, space, capabilities, group);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofMillis(300));
        }
    }

    /** A clerk that confirms every order it is handed, exactly once. */
    @AgentSpec(name = "clerk", description = "Confirms orders", goals = {"confirm"})
    public static class Clerk {
        final List<String> confirmed = new CopyOnWriteArrayList<>();
        final String label;

        Clerk(String label) {
            this.label = label;
        }

        @OrderedTake(space = "orders", lease = "30s", pollTimeout = "200ms")
        public Receipt confirm(Order order) {
            confirmed.add(order.id());
            return new Receipt(order.id(), label);
        }
    }

    /**
     * {@code @OrderedTake} (written before the annotation existed): three clerks
     * on a three-member log each bind one method; every order is confirmed by
     * exactly one clerk and drains from every replica, the desk's included.
     */
    @Test
    void anOrderedTakeMethodCompletesEachEntryExactlyOnceThroughTheLog() throws Exception {
        Member a = member("a", 1, "orders");
        Member b = member("b", 2, "orders", "a");
        Member c = member("c", 3, "orders", "a");
        List<Member> members = List.of(a, b, c);
        List<PeerId> memberIds = members.stream().map(m -> m.identity().peerId()).toList();
        tickAll(4);
        List<Clerk> clerks = new ArrayList<>();
        int seed = 100;
        for (Member m : members) {
            OrderedTakes ordered = OrderedTakes.over(new CapabilityPipes(m.runtime(), codec), codec,
                    m.identity(), "clerk", memberIds, clock, seed++, m.space());
            m.group().ordered("orders", ordered);   // registers the log on the peer tick too
            Clerk clerk = new Clerk(m.identity().peerId().display());
            closeables.add(m.group().bind(clerk));
            clerks.add(clerk);
        }
        // Elect a leader on the peer tick.
        for (int i = 0; i < 60 && members.stream().noneMatch(m -> leads(m)); i++) {
            tickAll(1);
        }
        assertThat(members.stream().filter(this::leads).count()).as("exactly one leader").isEqualTo(1);

        for (int i = 1; i <= 4; i++) {
            a.space().write(new Order("ord-" + i, 100L * i), Lease.of(Duration.ofMinutes(10)));
        }
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline
                && a.space().readAll(Template.of(Receipt.class), 10).size() < 4) {
            tickAll(1);
            Thread.sleep(20);
        }
        List<Receipt> receipts = a.space().readAll(Template.of(Receipt.class), 10);
        assertThat(receipts).extracting(Receipt::id).containsExactlyInAnyOrder("ord-1", "ord-2", "ord-3", "ord-4");
        assertThat(clerks.stream().mapToInt(cl -> cl.confirmed.size()).sum())
                .as("every order confirmed by exactly one clerk").isEqualTo(4);
        tickAll(6);
        for (Member m : members) {
            assertThat(m.space().readAll(Template.of(Order.class), 10)).as("drained at " + m.identity().peerId().display()).isEmpty();
        }
    }

    private boolean leads(Member m) {
        return m.group().binder() != null && m.capabilities().providersOf(
                ai.badmonkey.agentspaces.capabilities.orderedlog.RaftLog.TYPE).stream()
                .anyMatch(ad -> "leader".equals(ad.parameters().get("role"))
                        && ad.issuer().equals(m.identity().peerId()));
    }

    /** An {@code @OrderedTake} on a space with no coordinator is refused at bind time, naming the remedy. */
    @Test
    void anOrderedTakeWithoutACoordinatorIsRefusedAtBindTime() throws Exception {
        Member a = member("a", 1, "orders");
        assertThatThrownBy(() -> a.group().bind(new Clerk("x")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no ordered-log coordinator")
                .hasMessageContaining("OrderedTakes.over(");
    }

    /**
     * M-11: an attested clerk takes and completes as itself. A coordinator that
     * claims under the peer key (even naming the same agent id) is refused at
     * bind time; one built over the agent's identity completes with an
     * agent-attested receipt.
     */
    @Test
    void anAttestedClerkTakesAndCompletesThroughItsOwnIdentity() throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        Map<String, ai.badmonkey.agentspaces.api.spi.AgentIdentity> agents = new ConcurrentHashMap<>();
        java.util.function.Function<String, ai.badmonkey.agentspaces.api.spi.AgentIdentity> identities =
                name -> agents.computeIfAbsent(name,
                        n -> identity.renewingSubordinate(n, Duration.ofDays(1), clock));
        Member a = member(identity, identities, "a", 1, "orders");
        Member c = member("c", 3, "orders", "a");   // the log's second voter; binds no clerk
        List<PeerId> memberIds = List.of(identity.peerId(), c.identity().peerId());
        tickAll(4);

        PeerIdentity other = PeerIdentity.generate();
        Member b = member(other, name -> other.renewingSubordinate(name, Duration.ofDays(1), clock),
                "b", 2, "orders");
        OrderedTakes peerKeyed = OrderedTakes.over(new CapabilityPipes(b.runtime(), codec), codec,
                other, "clerk", List.of(other.peerId()), clock, 7, b.space());
        b.group().ordered("orders", peerKeyed);
        assertThatThrownBy(() -> b.group().bind(new Clerk("x")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("attested agent")
                .hasMessageContaining("under the peer key");

        OrderedTakes ordered = OrderedTakes.over(new CapabilityPipes(a.runtime(), codec), codec,
                identity, identities.apply("clerk"), memberIds, clock, 8, a.space());
        a.group().ordered("orders", ordered);
        c.group().ordered("orders", OrderedTakes.over(new CapabilityPipes(c.runtime(), codec), codec,
                c.identity(), "clerk", memberIds, clock, 9, c.space()));
        closeables.add(a.group().bind(new Clerk("attested")));
        for (int i = 0; i < 60 && !leads(a) && !leads(c); i++) {
            tickAll(1);
        }
        assertThat(leads(a) || leads(c)).as("the log elects a leader").isTrue();

        a.space().write(new Order("ord-1", 100L), Lease.of(Duration.ofMinutes(10)));
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline
                && a.space().readAll(Template.of(Receipt.class), 10).isEmpty()) {
            tickAll(1);
            Thread.sleep(20);
        }
        List<ai.badmonkey.agentspaces.api.space.Space.Issued<Receipt>> receipts =
                a.space().readAllIssued(Template.of(Receipt.class), 10);
        assertThat(receipts).singleElement().satisfies(r -> {
            assertThat(r.issuer()).isEqualTo(identity.agent("clerk"));
            assertThat(r.attestation()).isEqualTo(ai.badmonkey.agentspaces.api.space.Space.Attestation.AGENT_ATTESTED);
        });
    }

    /** A sensor that contributes each reading to the aggregate epoch named by its topic. */
    @AgentSpec(name = "sensor", description = "Contributes readings", goals = {"sense"})
    public static class Sensor {
        @SpaceNotify(space = "readings")
        public Contribution feel(Reading reading) {
            return Contribution.to(reading.topic(), reading.value());
        }
    }

    /**
     * A {@link Contribution} returned from {@code @SpaceNotify} starts the peer's
     * aggregate epoch instead of writing an entry; with a second participant the
     * fleet estimate converges on the mean. Without a registered aggregate, binding
     * such a method is refused.
     */
    @Test
    void aContributionReturnStartsTheAggregateEpoch() throws Exception {
        Member a = member("a", 1, "readings");
        Member b = member("b", 2, "readings", "a");
        tickAll(4);
        PushSumAggregate aggA = new PushSumAggregate(new CapabilityPipes(a.runtime(), codec),
                a.runtime().sampler(), a.identity().peerId(), codec, clock);
        PushSumAggregate aggB = new PushSumAggregate(new CapabilityPipes(b.runtime(), codec),
                b.runtime().sampler(), b.identity().peerId(), codec, clock);
        a.group().provide(aggA);
        b.group().provide(aggB);
        closeables.add(a.group().bind(new Sensor()));

        b.space().write(new Reading("backlog", 40.0), Lease.of(Duration.ofMinutes(5)));
        aggB.start("backlog", 20.0);
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline && aggA.estimate("backlog").isEmpty()) {
            tickAll(1);
            Thread.sleep(10);
        }
        for (int i = 0; i < 40; i++) {
            tickAll(1);
        }
        assertThat(aggA.estimate("backlog")).isPresent();
        assertThat(aggA.estimate("backlog").getAsDouble()).isCloseTo(30.0, org.assertj.core.data.Offset.offset(1.0));
        assertThat(a.space().readAll(Template.of(Contribution.class), 10))
                .as("a contribution is not written as an entry").isEmpty();
    }
}
