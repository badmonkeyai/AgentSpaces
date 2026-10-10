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
import ai.badmonkey.agentspaces.agent.annotation.SpaceReduce;
import ai.badmonkey.agentspaces.agent.join.JoinTicket;
import ai.badmonkey.agentspaces.agent.reduce.Reductions;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * ISSUE-SpaceReduce §14: two reducers in {@code ORDERED} mode over a real
 * quorum fold elements written at different members, every element exactly
 * once, with one accumulator per key at every member once settled.
 */
class AgentBinderReduceClusterTest {

    public record Payment(String account, long cents) {
    }

    public record Balance(String account, long cents, int payments) {
    }

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zReduceLog");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zReduceLog", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "reducelog",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    static {
        // The reduce's and the join's decisions at DEBUG on the console, so a failure reads as a timeline.
        for (String name : List.of("ai.badmonkey.agentspaces.agent.reduce", "ai.badmonkey.agentspaces.agent.join")) {
            java.util.logging.Logger logger = java.util.logging.Logger.getLogger(name);
            logger.setLevel(java.util.logging.Level.FINE);
            java.util.logging.ConsoleHandler console = new java.util.logging.ConsoleHandler();
            console.setLevel(java.util.logging.Level.FINE);
            logger.addHandler(console);
        }
    }

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

    private Member member(String address, long seed, String... seeds) throws IOException {
        return member(address, seed, clock, seeds);
    }

    private Member member(String address, long seed, java.time.InstantSource clock, String... seeds)
            throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String s : seeds) {
            endpoints.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), endpoints);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "ledger", identity, "teller")
                .clock(clock).settleWindow(Duration.ZERO).build();
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, clock), codec,
                identity.peerId());
        CapabilityRuntime capabilities = new CapabilityRuntime(runtime, discovery, identity);
        AgentSpaces spaces = new AgentSpaces(identity, clock);
        AgentSpaces.GroupContext group = spaces.register("reducelog", groupId, runtime, discovery)
                .capabilities(capabilities);
        group.space("ledger", space);
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

    private boolean leads(Member m) {
        return m.capabilities().providersOf(RaftLog.TYPE).stream()
                .anyMatch(ad -> "leader".equals(ad.parameters().get("role"))
                        && ad.issuer().equals(m.identity().peerId()));
    }

    @AgentSpec(name = "teller", description = "Keeps a running balance per account, once fleet-wide", goals = {"balance"})
    public static class Teller {
        final AtomicInteger steps = new AtomicInteger();

        @SpaceReduce(space = "ledger", key = "account", mode = SpaceReduce.Mode.ORDERED, name = "teller",
                lease = "30s", pollTimeout = "200ms", accumulatorLease = "1h")
        public Balance apply(Balance balance, Payment payment) {
            steps.incrementAndGet();
            return new Balance(payment.account(), (balance == null ? 0 : balance.cents()) + payment.cents(),
                    (balance == null ? 0 : balance.payments()) + 1);
        }
    }

    @Test
    void anOrderedReduceFoldsEveryElementOnceAcrossTwoReducers() throws Exception {
        Member a = member("a", 1);
        Member b = member("b", 2, "a");
        Member c = member("c", 3, "a");
        List<Member> members = List.of(a, b, c);
        List<PeerId> memberIds = members.stream().map(m -> m.identity().peerId()).toList();
        tickAll(4);
        List<Teller> tellers = new ArrayList<>();
        int seed = 100;
        for (Member m : List.of(a, b)) {
            OrderedTakes ordered = OrderedTakes.over(new CapabilityPipes(m.runtime(), codec), codec,
                    m.identity(), "teller", memberIds, clock, seed++, m.space());
            m.group().ordered("ledger", ordered);
            Teller teller = new Teller();
            closeables.add(m.group().bind(teller));
            tellers.add(teller);
        }
        OrderedTakes third = OrderedTakes.over(new CapabilityPipes(c.runtime(), codec), codec,
                c.identity(), "teller", memberIds, clock, seed, c.space());
        c.group().ordered("ledger", third);
        for (int i = 0; i < 60 && members.stream().noneMatch(this::leads); i++) {
            tickAll(1);
        }
        assertThat(members.stream().filter(this::leads).count()).as("exactly one leader").isEqualTo(1);

        Member[] writers = {a, b, c};
        long[] expected = new long[3];
        for (int i = 1; i <= 12; i++) {
            int account = i % 3;
            writers[(i + 1) % 3].space().write(new Payment("acct-" + account, i), Lease.of(Duration.ofMinutes(10)));
            expected[account] += i;
        }
        // Until every member holds the sums and no ticket is left anywhere: a ticket
        // whose take lease lapsed under the test clock reappears and is completed on a
        // later drain, so the drain is part of what settles.
        long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (System.nanoTime() < deadline && !(settled(members, expected) && ticketsDrained(members))) {
            tickAll(1);
            Thread.sleep(20);
        }
        if (!settled(members, expected) || members.stream().anyMatch(m -> !m.space().readAll(Template.of(JoinTicket.class), 20).isEmpty())) {
            for (Member m : members) {
                System.err.println("=== STATE at " + m.identity().peerId().display()
                        + " payments=" + m.space().readAll(Template.of(Payment.class), 20).size()
                        + " balances=" + m.space().readAllEntries(Template.of(Balance.class), 20).stream()
                                .map(e -> e.value() + e.tags().toString()).toList()
                        + " tickets=" + m.space().readAllEntries(Template.of(JoinTicket.class), 20).stream()
                                .map(e -> e.entryId() + e.tags().toString() + "@" + e.issued()).toList());
            }
        }
        assertThat(settled(members, expected)).as("every member holds one accumulator per key with the full sum").isTrue();
        assertThat(tellers.stream().mapToInt(t -> t.steps.get()).sum())
                .as("every element folded exactly once across the fleet").isEqualTo(12);
        for (Member m : members) {
            assertThat(m.space().readAll(Template.of(JoinTicket.class), 20))
                    .as("tickets drained at " + m.identity().peerId().display()).isEmpty();
        }
    }

    /** An accumulator the space cannot write: its payload is over the inline limit and the space has no block store. */
    public record Unwritable(String account, String blob) {
    }

    @AgentSpec(name = "unwritable", description = "Folds into something the codec refuses", goals = {"x"})
    public static class UnwritableTeller {
        final AtomicInteger calls = new AtomicInteger();

        @SpaceReduce(space = "ledger", key = "account", name = "unwritable", lease = "600ms", pollTimeout = "100ms",
                accumulatorLease = "1h")
        public Unwritable apply(Unwritable acc, Payment payment) {
            calls.incrementAndGet();
            return new Unwritable(payment.account(), "x".repeat(100_000));
        }
    }

    /**
     * ISSUE-SpaceReduce §8.2, mutation M1: the element's completion carries the
     * accumulator, so a result the space cannot write never consumes the element.
     * A replicated space prepares the result before the completion commits (a
     * local space stores the object as it is, so this is pinned here), and this
     * member runs on the system clock so its leases lapse in real time, not
     * under the reducer's feet.
     */
    @Test
    void aResultTheSpaceCannotWriteLeavesTheElementInTheSpace() throws Exception {
        Member a = member("a", 1, java.time.InstantSource.system());
        tickAll(2);
        UnwritableTeller teller = new UnwritableTeller();
        closeables.add(a.group().bind(teller));
        a.space().write(new Payment("acct-u", 9), Lease.of(Duration.ofMinutes(10)));
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline && teller.calls.get() < 2) {
            tickAll(1);          // the lease lapses on the sweep; the element reappears and is tried again
            Thread.sleep(20);
        }
        assertThat(teller.calls.get()).as("the step was tried, failed to write, and tried again").isGreaterThanOrEqualTo(2);
        deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline && a.space().readAll(Template.of(Payment.class), 10).isEmpty()) {
            tickAll(1);
            Thread.sleep(20);
        }
        assertThat(a.space().readAll(Template.of(Payment.class), 10))
                .as("a result that cannot be written never consumes the element").containsExactly(new Payment("acct-u", 9));
        assertThat(Reductions.current(a.space(), Unwritable.class, "unwritable", "acct-u")).isEmpty();
    }

    private boolean ticketsDrained(List<Member> members) {
        return members.stream().allMatch(m -> m.space().readAll(Template.of(JoinTicket.class), 20).isEmpty());
    }

    private boolean settled(List<Member> members, long[] expected) {
        for (Member m : members) {
            if (!m.space().readAll(Template.of(Payment.class), 20).isEmpty()) {
                return false;
            }
            List<Balance> balances = m.space().readAll(Template.of(Balance.class), 20);
            if (balances.size() != 3) {
                return false;
            }
            for (int account = 0; account < 3; account++) {
                var current = Reductions.current(m.space(), Balance.class, "teller", "acct-" + account);
                if (current.isEmpty() || current.get().value().cents() != expected[account]
                        || current.get().value().payments() != 4) {
                    return false;
                }
            }
        }
        return true;
    }
}
