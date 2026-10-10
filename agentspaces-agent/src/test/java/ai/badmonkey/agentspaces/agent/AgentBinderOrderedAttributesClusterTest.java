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

import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.OnEstimate;
import ai.badmonkey.agentspaces.agent.annotation.OrderedTake;
import ai.badmonkey.agentspaces.agent.annotation.ProvidesCapability;
import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.ad.CardAction;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.CapabilityProvider;
import ai.badmonkey.agentspaces.capabilities.aggregate.PushSumAggregate;
import ai.badmonkey.agentspaces.capabilities.orderedlog.OrderedTakes;
import ai.badmonkey.agentspaces.capabilities.orderedlog.RaftLog;
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
import java.io.IOException;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The attributes that need a fleet (the 2026-10-09 audit): an
 * {@code @OrderedTake}'s tag and field filters, result space, result lease,
 * declared products, description, and null and fork returns over a
 * three-member log; the refusal of a malformed field filter on one; an
 * {@code @OnEstimate}'s result lease and description; and a
 * {@code @ProvidesCapability} bean registered on the group it names. Every
 * Layer 4 annotation here names its group by the registered name.
 */
class AgentBinderOrderedAttributesClusterTest {

    public record Order(String id, long cents) {
    }

    public record Receipt(String id, String clerk) {
    }

    public record Audit(String id) {
    }

    public record Report(String epoch, double value) {
    }

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zOrdAttrs");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zOrdAttrs", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "ordered",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Member(PeerIdentity identity, PeerNode node, GroupRuntime runtime,
                          ReplicatedSpace orders, ReplicatedSpace receipts, CapabilityRuntime capabilities,
                          PushSumAggregate aggregate, AgentSpaces spaces, AgentSpaces.GroupContext group) {
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
        ReplicatedSpace orders = ReplicatedSpace.builder(runtime, "orders", identity, "clerk")
                .clock(clock).settleWindow(Duration.ZERO).build();
        ReplicatedSpace receipts = ReplicatedSpace.builder(runtime, "receipts", identity, "clerk")
                .clock(clock).settleWindow(Duration.ZERO).build();
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, clock), codec,
                identity.peerId());
        CapabilityRuntime capabilities = new CapabilityRuntime(runtime, discovery, identity);
        PushSumAggregate aggregate = new PushSumAggregate(new CapabilityPipes(runtime, codec),
                runtime.sampler(), identity.peerId(), codec, clock);
        AgentSpaces spaces = new AgentSpaces(identity, clock);
        AgentSpaces.GroupContext group = spaces.register("ordered", groupId, runtime, discovery)
                .capabilities(capabilities);
        group.space("orders", orders);
        group.space("receipts", receipts);
        group.provide(aggregate);
        nodes.add(node);
        closeables.add(spaces);
        return new Member(identity, node, runtime, orders, receipts, capabilities, aggregate, spaces, group);
    }

    private OrderedTakes coordinator(Member m, List<PeerId> memberIds, long seed) {
        return OrderedTakes.over(new CapabilityPipes(m.runtime(), codec), codec, m.identity(), "clerk",
                memberIds, clock, seed, m.orders());
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofMillis(300));
        }
    }

    private boolean leads(Member m) {
        return m.capabilities().providersOf(RaftLog.TYPE).stream()
                .anyMatch(ad -> "leader".equals(ad.parameters().get("role"))
                        && ad.issuer().equals(m.identity().peerId()));
    }

    /** Asserts a write lease of about {@code declared}, allowing for the clock the ticks advanced since the write. */
    private void assertLeaseAbout(Space.Entry<?> entry, Duration declared) {
        long now = clock.instant().toEpochMilli();
        assertThat(entry.lease().expiresAtMillis())
                .isGreaterThan(now + declared.minus(Duration.ofMinutes(15)).toMillis())
                .isLessThanOrEqualTo(now + declared.toMillis());
    }

    // ------------------------------------------------------------------ @OrderedTake

    @AgentSpec(name = "clerk", description = "Confirms EU orders", goals = {"confirm"})
    public static class Clerk {
        final List<String> confirmed = new CopyOnWriteArrayList<>();
        final String label;

        Clerk(String label) {
            this.label = label;
        }

        @OrderedTake(space = "orders", group = "ordered", lease = "30s", pollTimeout = "200ms",
                tags = "region=eu", where = "cents!=0", resultSpace = "receipts", resultLease = "3h",
                produces = {Receipt.class, Audit.class}, description = "Confirms an EU order")
        public Entries confirm(Order order) {
            confirmed.add(order.id());
            if (order.id().startsWith("quiet")) {
                return null;
            }
            return Entries.of(new Receipt(order.id(), label), new Audit(order.id()));
        }
    }

    @Test
    void anOrderedTakeFiltersByTagAndFieldAndWritesItsForkWhereAndForHowLongItSays() throws Exception {
        Member a = member("a", 1);
        Member b = member("b", 2, "a");
        Member c = member("c", 3, "a");
        List<Member> members = List.of(a, b, c);
        List<PeerId> memberIds = members.stream().map(m -> m.identity().peerId()).toList();
        tickAll(4);
        List<Clerk> clerks = new ArrayList<>();
        int seed = 100;
        AgentBinder.Bound bound = null;
        for (Member m : members) {
            m.group().ordered("orders", coordinator(m, memberIds, seed++));
            Clerk clerk = new Clerk(m.identity().peerId().display());
            bound = m.group().bind(clerk);
            closeables.add(bound);
            clerks.add(clerk);
        }
        CardAction action = bound.card().actions().get(0);
        assertThat(action.kind()).isEqualTo(CardAction.ORDERED_TAKE);
        assertThat(action.description()).isEqualTo("Confirms an EU order");
        assertThat(action.space()).isEqualTo("orders");
        assertThat(action.produces()).containsExactly(Receipt.class.getName() + "#v1", Audit.class.getName() + "#v1");
        for (int i = 0; i < 60 && members.stream().noneMatch(this::leads); i++) {
            tickAll(1);
        }
        assertThat(members.stream().filter(this::leads).count()).as("exactly one leader").isEqualTo(1);

        Lease tenMinutes = Lease.of(Duration.ofMinutes(10));
        a.orders().write(new Order("eu-1", 100), tenMinutes, Map.of("region", "eu"));
        a.orders().write(new Order("eu-2", 200), tenMinutes, Map.of("region", "eu"));
        a.orders().write(new Order("quiet-1", 300), tenMinutes, Map.of("region", "eu"));
        a.orders().write(new Order("us-1", 400), tenMinutes, Map.of("region", "us"));   // tags: region=eu
        a.orders().write(new Order("zero-1", 0), tenMinutes, Map.of("region", "eu"));   // where: cents!=0
        await(() -> a.receipts().readAll(Template.of(Audit.class), 10).size() == 2
                && clerks.stream().mapToInt(cl -> cl.confirmed.size()).sum() == 3, Duration.ofSeconds(30));
        tickAll(6);
        List<Space.Entry<Receipt>> receipts = a.receipts().readAllEntries(Template.of(Receipt.class), 10);
        assertThat(receipts).extracting(r -> r.value().id()).containsExactlyInAnyOrder("eu-1", "eu-2");
        receipts.forEach(r -> assertLeaseAbout(r, Duration.ofHours(3)));
        assertThat(a.orders().readAll(Template.of(Receipt.class), 10)).as("results go to the result space").isEmpty();
        assertThat(clerks.stream().flatMap(cl -> cl.confirmed.stream()).toList())
                .as("the null return completed its take and wrote nothing")
                .containsExactlyInAnyOrder("eu-1", "eu-2", "quiet-1");
        for (Member m : members) {
            assertThat(m.orders().readAll(Template.of(Order.class), 10)).extracting(Order::id)
                    .as("the filtered-out orders were never claimed at " + m.identity().peerId().display())
                    .containsExactlyInAnyOrder("us-1", "zero-1");
        }
    }

    public static class BadFilter {
        @OrderedTake(space = "orders", where = "=x")
        public void confirm(Order order) {
        }
    }

    @Test
    void aMalformedFieldFilterOnAnOrderedTakeIsRefusedAtBindTime() throws Exception {
        Member a = member("a", 1);
        a.group().ordered("orders", coordinator(a, List.of(a.identity().peerId()), 7));
        assertThatThrownBy(() -> a.group().bind(new BadFilter()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("field filter '=x'");
    }

    // ------------------------------------------------------------------ @OnEstimate

    @AgentSpec(name = "supervisor", description = "Reports the backlog", goals = {"report"})
    public static class Supervisor {
        @OnEstimate(prefix = "backlog", settleTicks = 0, group = "ordered", resultSpace = "receipts",
                resultLease = "2h", description = "Reports the settled backlog")
        public Report report(PushSumAggregate.Estimate estimate) {
            return new Report(estimate.epochId(), estimate.value());
        }
    }

    @Test
    void anOnEstimateWritesItsReportUnderTheDeclaredResultLeaseAndDescribesItself() throws Exception {
        Member a = member("a", 1);
        AgentBinder.Bound bound = a.group().bind(new Supervisor());
        closeables.add(bound);
        CardAction action = bound.card().actions().get(0);
        assertThat(action.kind()).isEqualTo(CardAction.ON_ESTIMATE);
        assertThat(action.description()).isEqualTo("Reports the settled backlog");
        assertThat(action.space()).isEqualTo("receipts");

        a.aggregate().start("backlog", 40.0);
        await(() -> !a.receipts().readAll(Template.of(Report.class), 10).isEmpty(), Duration.ofSeconds(20));
        List<Space.Entry<Report>> reports = a.receipts().readAllEntries(Template.of(Report.class), 10);
        assertThat(reports).singleElement().satisfies(r -> {
            assertThat(r.value().epoch()).isEqualTo("backlog");
            assertLeaseAbout(r, Duration.ofHours(2));
        });
    }

    // ------------------------------------------------------------------ @ProvidesCapability

    @ProvidesCapability(value = "aspace:cap/probe", group = "ordered")
    public static class Probe implements CapabilityProvider {
        private final PeerId self;
        private final InstantSource clock;

        Probe(PeerId self, InstantSource clock) {
            this.self = self;
            this.clock = clock;
        }

        @Override
        public String capabilityType() {
            return "aspace:cap/probe";
        }

        @Override
        public CapabilityAdvertisement describe(GroupId group) {
            return new CapabilityAdvertisement("aspace://" + group.value() + "/cap/probe/" + self.value(),
                    self, group, clock.instant(), Duration.ofMinutes(15), "aspace:cap/probe", "0.1", "pipe",
                    Map.of(), Map.of());
        }
    }

    @Test
    void aProvidesCapabilityBeanIsRegisteredOnTheGroupItNames() throws Exception {
        Member a = member("a", 1);
        Probe probe = new Probe(a.identity().peerId(), clock);
        assertThat(a.spaces().bind(probe)).as("a pure provider binds no agent").isEmpty();
        assertThat(a.group().provider("aspace:cap/probe")).containsSame(probe);
        tickAll(2);
        assertThat(a.capabilities().providersOf("aspace:cap/probe"))
                .as("the group advertises it").anySatisfy(ad -> assertThat(ad.issuer()).isEqualTo(a.identity().peerId()));
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
