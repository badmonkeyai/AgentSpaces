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

    /**
     * Held for the class's life: the log manager keeps loggers only weakly, so a logger configured and dropped is
     * collected and comes back at the default level, and the DEBUG timeline silently disappears.
     */
    private static final List<java.util.logging.Logger> DEBUG_LOGGERS = new ArrayList<>();

    static {
        // The reduce's and the join's decisions at DEBUG on the console, so a failure reads as a timeline.
        for (String name : List.of("ai.badmonkey.agentspaces.agent.reduce", "ai.badmonkey.agentspaces.agent.join")) {
            java.util.logging.Logger logger = java.util.logging.Logger.getLogger(name);
            DEBUG_LOGGERS.add(logger);
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
        return member(address, seed, clock, PeerIdentity.generate(), seeds);
    }

    private Member member(String address, long seed, PeerIdentity identity, String... seeds) throws IOException {
        return member(address, seed, clock, identity, seeds);
    }

    private Member member(String address, long seed, java.time.InstantSource clock, PeerIdentity identity,
                          String... seeds) throws IOException {
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

    /** What a late member saw at the first event of its first batch: per account, the accumulator's count and the payments left. */
    private record FirstView(int known, java.util.Map<String, Integer> folded, java.util.Map<String, Integer> visible) {
    }

    /**
     * RBE regression matrix probe P8 (srad/replication-by-entry.md phase 1, RE-M-08): a reducer on a member that
     * joins late learns, in one {@code PULL_RESP}, the elements already folded (completed, each with the accumulator it
     * produced), the accumulators they replaced (cancelled), and the elements not yet folded. The batch's events fire
     * after all of it is in, so the reducer's first look sees one atomic view: the newest accumulator counting exactly the
     * folded elements, and only the unfolded ones still readable. It folds those on top, each once, and every member
     * ends with one accumulator per key holding every element once.
     */
    @Test
    void aLateReducerSeesTheCompletionsAndTheNewAccumulatorAsOneViewAndFoldsEachElementOnce() throws Exception {
        Member a = member("a", 1);
        Member b = member("b", 2, "a");
        // The third voter starts after the folding: never admitted before, it hears no lazy-push announcement of the
        // older rumors, so anti-entropy is its only source and one PULL_RESP carries the whole ledger.
        PeerIdentity lateIdentity = PeerIdentity.generate();
        List<PeerId> memberIds = List.of(a.identity().peerId(), b.identity().peerId(), lateIdentity.peerId());
        tickAll(4);
        int seed = 100;
        for (Member m : List.of(a, b)) {
            m.group().ordered("ledger", OrderedTakes.over(new CapabilityPipes(m.runtime(), codec), codec,
                    m.identity(), "teller", memberIds, clock, seed++, m.space()));
        }
        for (int i = 0; i < 60 && List.of(a, b).stream().noneMatch(this::leads); i++) {
            tickAll(1);
        }
        assertThat(List.of(a, b).stream().filter(this::leads).count()).as("a leader of two of three").isEqualTo(1);

        Teller early = new Teller();
        AgentBinder.Bound earlyBinding = a.group().bind(early);
        long[] expected = new long[2];
        for (int i = 1; i <= 6; i++) {
            b.space().write(new Payment("acct-" + (i % 2), i), Lease.of(Duration.ofMinutes(10)));
            expected[i % 2] += i;
        }
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline && !(foldedAt(a, 3) && ticketsDrained(List.of(a, b)))) {
            tickAll(1);
            Thread.sleep(20);
        }
        assertThat(foldedAt(a, 3)).as("the early reducer folded the first six").isTrue();
        // Close returns once the early reducer can fold nothing more, so the rest can be written at once.
        earlyBinding.close();
        assertThat(early.steps.get()).isEqualTo(6);
        for (int i = 7; i <= 10; i++) {
            b.space().write(new Payment("acct-" + (i % 2), i), Lease.of(Duration.ofMinutes(10)));
            expected[i % 2] += i;
        }
        tickAll(2);
        assertThat(b.space().readAll(Template.of(Payment.class), 20)).as("four left unfolded").hasSize(4);
        int ledger = b.space().knownEntries(); // payments, accumulators, and the tickets the early reducer held
        assertThat(a.space().knownEntries()).as("both replicas hold the same ledger").isEqualTo(ledger);

        Member c = member("c", 3, lateIdentity, "a");
        OrderedTakes third = OrderedTakes.over(new CapabilityPipes(c.runtime(), codec), codec,
                c.identity(), "teller", memberIds, clock, seed, c.space());
        c.group().ordered("ledger", third);
        java.util.concurrent.atomic.AtomicReference<FirstView> first = new java.util.concurrent.atomic.AtomicReference<>();
        Lease hour = Lease.of(Duration.ofHours(1));
        for (Class<?> type : List.of(Payment.class, Balance.class)) {
            c.space().notify(Template.of(type), e -> first.compareAndSet(null, viewAt(c)), hour);
        }
        Teller late = new Teller();
        closeables.add(c.group().bind(late));
        assertThat(c.space().knownEntries()).as("bound before anything arrived").isZero();

        List<Member> members = List.of(a, b, c);
        deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (System.nanoTime() < deadline && !(settledAt(members, expected, 5) && ticketsDrained(members))) {
            tickAll(1);
            Thread.sleep(20);
        }

        FirstView view = first.get();
        assertThat(view).as("the late member heard the ledger").isNotNull();
        assertThat(view.known()).as("one batch: the whole ledger was in at the first event").isEqualTo(ledger);
        assertThat(view.folded()).as("the newest accumulator counts exactly the folded elements")
                .containsEntry("acct-0", 3).containsEntry("acct-1", 3);
        assertThat(view.visible()).as("only the unfolded elements are readable beside it")
                .containsEntry("acct-0", 2).containsEntry("acct-1", 2);
        assertThat(settledAt(members, expected, 5))
                .as("every member holds one accumulator per key with every element once").isTrue();
        assertThat(early.steps.get()).isEqualTo(6);
        assertThat(late.steps.get()).as("the late reducer folded only the four left, each once").isEqualTo(4);
        assertThat(ticketsDrained(members)).isTrue();
    }

    /** An ORDERED teller whose drain waits long for the next element, so a close finds its take in flight. */
    @AgentSpec(name = "teller", description = "Keeps a running balance per account, once fleet-wide", goals = {"balance"})
    public static class PatientOrderedTeller {
        final AtomicInteger steps = new AtomicInteger();

        @SpaceReduce(space = "ledger", key = "account", mode = SpaceReduce.Mode.ORDERED, name = "teller",
                lease = "30s", pollTimeout = "20s", accumulatorLease = "1h")
        public Balance apply(Balance balance, Payment payment) {
            steps.incrementAndGet();
            return new Balance(payment.account(), (balance == null ? 0 : balance.cents()) + payment.cents(),
                    (balance == null ? 0 : balance.payments()) + 1);
        }
    }

    /** The LEASED form of {@link PatientOrderedTeller}. */
    @AgentSpec(name = "teller", description = "Keeps a running balance per account", goals = {"balance"})
    public static class PatientLeasedTeller {
        final AtomicInteger steps = new AtomicInteger();

        @SpaceReduce(space = "ledger", key = "account", mode = SpaceReduce.Mode.LEASED, name = "teller",
                lease = "30s", pollTimeout = "20s", accumulatorLease = "1h")
        public Balance apply(Balance balance, Payment payment) {
            steps.incrementAndGet();
            return new Balance(payment.account(), (balance == null ? 0 : balance.cents()) + payment.cents(),
                    (balance == null ? 0 : balance.payments()) + 1);
        }
    }

    /**
     * Closing an ORDERED reduce stops the take it has in flight. The reducer folds two payments and then waits in
     * its next element take (a 20 s poll). Before the fix, close returned at once but the take swallowed the
     * interrupt, kept scanning, and adopted and folded a payment written after close; the core and PetClinic tests
     * waited out the poll and the log's attempt window for it. Now close returns once nothing more can fold, and the
     * payment is left to the next reducer, which folds it once.
     */
    @Test
    void closingAnOrderedReduceStopsItsTakeInFlightAndNothingFoldsAfterClose() throws Exception {
        Member a = member("a", 1);
        Member b = member("b", 2, "a");
        Member c = member("c", 3, "a");
        List<Member> members = List.of(a, b, c);
        List<PeerId> memberIds = members.stream().map(m -> m.identity().peerId()).toList();
        tickAll(4);
        int seed = 100;
        for (Member m : members) {
            m.group().ordered("ledger", OrderedTakes.over(new CapabilityPipes(m.runtime(), codec), codec,
                    m.identity(), "teller", memberIds, clock, seed++, m.space()));
        }
        for (int i = 0; i < 60 && members.stream().noneMatch(this::leads); i++) {
            tickAll(1);
        }
        assertThat(members.stream().filter(this::leads).count()).as("exactly one leader").isEqualTo(1);

        PatientOrderedTeller closing = new PatientOrderedTeller();
        AgentBinder.Bound binding = a.group().bind(closing);
        assertClosePrompt(members, a, b, binding, closing.steps, () -> {
            PatientOrderedTeller next = new PatientOrderedTeller();
            closeables.add(b.group().bind(next));
            return next.steps;
        });
    }

    /** As {@link #closingAnOrderedReduceStopsItsTakeInFlightAndNothingFoldsAfterClose}, for a LEASED reduce. */
    @Test
    void closingALeasedReduceStopsItsTakeInFlightAndNothingFoldsAfterClose() throws Exception {
        Member a = member("a", 1);
        Member b = member("b", 2, "a");
        List<Member> members = List.of(a, b);
        tickAll(4);
        PatientLeasedTeller closing = new PatientLeasedTeller();
        AgentBinder.Bound binding = a.group().bind(closing);
        assertClosePrompt(members, a, b, binding, closing.steps, () -> {
            PatientLeasedTeller next = new PatientLeasedTeller();
            closeables.add(b.group().bind(next));
            return next.steps;
        });
    }

    private void assertClosePrompt(List<Member> members, Member a, Member b, AgentBinder.Bound binding,
                                   AtomicInteger closingSteps,
                                   java.util.concurrent.Callable<AtomicInteger> bindNext) throws Exception {
        for (int i = 1; i <= 2; i++) {
            b.space().write(new Payment("acct-0", i), Lease.of(Duration.ofMinutes(10)));
        }
        java.util.function.BooleanSupplier twoFolded = () -> closingSteps.get() == 2
                && Reductions.current(a.space(), Balance.class, "teller", "acct-0")
                        .map(e -> e.value().payments() == 2).orElse(false);
        assertThat(awaitTicking("two folded", twoFolded, Duration.ofSeconds(60))).as("two folded").isTrue();
        // The drain now waits in its next element take, which a 20 s poll keeps in flight.
        Thread.sleep(200);

        long start = System.nanoTime();
        binding.close();
        long closeMillis = (System.nanoTime() - start) / 1_000_000;
        System.out.println("REDUCE-CLOSE-MEASURE close returned in " + closeMillis + " ms");
        assertThat(closeMillis).as("close stops the take in flight rather than waiting out its 20 s poll")
                .isLessThan(20_000);

        b.space().write(new Payment("acct-0", 3), Lease.of(Duration.ofMinutes(10)));
        // As long as the take used to stay in flight after close (the old poll and attempt window), with the log
        // and gossip running: a take still in flight would claim the payment here.
        for (int i = 0; i < 20; i++) {
            tickAll(1);
            Thread.sleep(200);
        }
        assertThat(closingSteps.get()).as("nothing folds after close returns").isEqualTo(2);
        for (Member m : members) {
            assertThat(m.space().readAll(Template.of(Payment.class), 20)).as("the payment waits, unfolded")
                    .containsExactly(new Payment("acct-0", 3));
        }

        AtomicInteger nextSteps = bindNext.call();
        java.util.function.BooleanSupplier allFolded = () -> members.stream().allMatch(m ->
                m.space().readAll(Template.of(Payment.class), 20).isEmpty()
                        && Reductions.current(m.space(), Balance.class, "teller", "acct-0")
                                .map(e -> e.value().cents() == 6 && e.value().payments() == 3).orElse(false));
        assertThat(awaitTicking("next reducer folds the third", allFolded, Duration.ofSeconds(60)))
                .as("the next reducer folds the payment the closed one left").isTrue();
        assertThat(nextSteps.get()).as("folded once, by the next reducer").isEqualTo(1);
        assertThat(closingSteps.get()).isEqualTo(2);
    }

    /** Ticks until the condition holds or the ceiling passes, and prints how long it took. */
    private boolean awaitTicking(String what, java.util.function.BooleanSupplier condition, Duration ceiling)
            throws InterruptedException {
        long start = System.nanoTime();
        long deadline = start + ceiling.toNanos();
        boolean held = condition.getAsBoolean();
        while (!held && System.nanoTime() < deadline) {
            tickAll(1);
            Thread.sleep(20);
            held = condition.getAsBoolean();
        }
        System.out.println("REDUCE-CLOSE-MEASURE " + what + " " + (System.nanoTime() - start) / 1_000_000 + " ms"
                + (held ? "" : " (ceiling reached)"));
        return held;
    }

    private static FirstView viewAt(Member m) {
        java.util.Map<String, Integer> folded = new java.util.TreeMap<>();
        java.util.Map<String, Integer> visible = new java.util.TreeMap<>();
        for (String account : List.of("acct-0", "acct-1")) {
            folded.put(account, Reductions.current(m.space(), Balance.class, "teller", account)
                    .map(e -> e.value().payments()).orElse(0));
            visible.put(account, (int) m.space().readAll(Template.of(Payment.class), 20).stream()
                    .filter(p -> p.account().equals(account)).count());
        }
        return new FirstView(m.space().knownEntries(), folded, visible);
    }

    private static boolean foldedAt(Member m, int each) {
        return m.space().readAll(Template.of(Payment.class), 20).isEmpty()
                && List.of("acct-0", "acct-1").stream().allMatch(account -> Reductions.current(m.space(),
                        Balance.class, "teller", account).map(e -> e.value().payments() == each).orElse(false));
    }

    private static boolean settledAt(List<Member> members, long[] expected, int each) {
        for (Member m : members) {
            if (!m.space().readAll(Template.of(Payment.class), 20).isEmpty()
                    || m.space().readAll(Template.of(Balance.class), 20).size() != 2) {
                return false;
            }
            for (int account = 0; account < 2; account++) {
                var current = Reductions.current(m.space(), Balance.class, "teller", "acct-" + account);
                if (current.isEmpty() || current.get().value().cents() != expected[account]
                        || current.get().value().payments() != each) {
                    return false;
                }
            }
        }
        return true;
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
