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
import ai.badmonkey.agentspaces.agent.annotation.BidFunction;
import ai.badmonkey.agentspaces.agent.annotation.SpaceNotify;
import ai.badmonkey.agentspaces.agent.annotation.SpaceRef;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.SpaceListener;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import ai.badmonkey.agentspaces.test.Fixtures.FindingEntry;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.capabilities.vote.VoteCapability;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AgentBinderTest {

    private final PeerIdentity identity = PeerIdentity.generate();
    private final LocalSpace tasks = LocalSpace.builder("tasks", identity.agent("host"))
            .sweepEvery(Duration.ofMillis(50))
            .build();
    private final AgentBinder binder = new AgentBinder(identity, GroupId.of("zBind"),
            null, InstantSource.system());

    @AfterEach
    void tearDown() {
        binder.close();
        tasks.close();
    }

    // ------------------------------------------------------- Layer 4 annotations (LAYER4-ANNOTATIONS.md)

    /** A panelist that approves proposals mentioning "ship" and abstains on the rest. */
    @AgentSpec(name = "panelist", description = "Votes", goals = {"decide"})
    public static class Panelist {
        final java.util.List<String> seen = new java.util.concurrent.CopyOnWriteArrayList<>();

        @ai.badmonkey.agentspaces.agent.annotation.Ballot(space = "votes", prefix = "rel:")
        public String judge(VoteCapability.Proposal proposal) {
            seen.add(proposal.proposalId());
            return proposal.question().contains("ship") ? "yes" : null;
        }
    }

    /** A lead that records each closed vote as a Verdict entry in the "verdicts" space. */
    @AgentSpec(name = "lead", description = "Records decisions", goals = {"record"})
    public static class Lead {
        final java.util.List<VoteCapability.Decision> decisions = new java.util.concurrent.CopyOnWriteArrayList<>();

        @ai.badmonkey.agentspaces.agent.annotation.OnDecision(space = "votes", resultSpace = "verdicts")
        public Verdict record(VoteCapability.Decision decision) {
            decisions.add(decision);
            return new Verdict(decision.proposalId(), decision.winner());
        }
    }

    public record Verdict(String proposalId, String winner) {
    }

    private VoteCapability voteOver(LocalSpace votes) {
        return new VoteCapability(votes, identity.agent("host"), identity.peerId(), InstantSource.system());
    }

    /**
     * {@code @Ballot} (written before the annotation existed): one ballot per
     * proposal, cast as the agent through the registered vote; a redelivered
     * proposal does not cast twice; a null return abstains; the prefix filters.
     */
    @Test
    void aBallotMethodCastsOncePerProposalAndAbstainsOnNull() throws Exception {
        LocalSpace votes = LocalSpace.builder("votes", identity.agent("host")).build();
        try {
            VoteCapability vote = voteOver(votes);
            AgentBinder voting = new AgentBinder(identity, GroupId.of("zBind"), null, InstantSource.system())
                    .space("votes", votes).vote("votes", vote);
            Panelist panelist = new Panelist();
            AgentBinder.Bound bound = voting.bind(panelist);
            try {
                vote.propose("rel:1", "ship it?", List.of("yes", "no"), 1, Lease.of(Duration.ofMinutes(5)));
                vote.propose("rel:2", "hold it?", List.of("yes", "no"), 1, Lease.of(Duration.ofMinutes(5)));
                vote.propose("other:3", "ship this too?", List.of("yes", "no"), 1, Lease.of(Duration.ofMinutes(5)));
                await(() -> panelist.seen.size() >= 2, Duration.ofSeconds(5));
                await(() -> vote.tally("rel:1").getOrDefault("yes", 0) == 1, Duration.ofSeconds(5));
                assertThat(vote.tally("rel:1")).containsEntry("yes", 1);
                assertThat(vote.tally("rel:2")).as("abstained").containsEntry("yes", 0).containsEntry("no", 0);
                assertThat(panelist.seen).as("the prefix keeps other votes out").doesNotContain("other:3");
                assertThat(vote.decision("rel:1")).hasValueSatisfying(d -> assertThat(d.winner()).isEqualTo("yes"));
            } finally {
                bound.close();
                voting.close();
            }
        } finally {
            votes.close();
        }
    }

    /** {@code @OnDecision}: fires once when the quorum closes, writes the return, and not again for late ballots. */
    @Test
    void anOnDecisionMethodFiresOnceWhenTheQuorumCloses() throws Exception {
        LocalSpace votes = LocalSpace.builder("votes", identity.agent("host")).build();
        LocalSpace verdicts = LocalSpace.builder("verdicts", identity.agent("host")).build();
        try {
            VoteCapability vote = voteOver(votes);
            AgentBinder deciding = new AgentBinder(identity, GroupId.of("zBind"), null, InstantSource.system())
                    .space("votes", votes).space("verdicts", verdicts).vote("votes", vote);
            Lead lead = new Lead();
            AgentBinder.Bound bound = deciding.bind(lead);
            try {
                vote.propose("p", "approve?", List.of("yes", "no"), 1, Lease.of(Duration.ofMinutes(5)));
                assertThat(lead.decisions).as("no ballots yet").isEmpty();
                vote.castBallot("p", "yes", Lease.of(Duration.ofMinutes(5)));
                await(() -> lead.decisions.size() == 1, Duration.ofSeconds(5));
                assertThat(lead.decisions).singleElement().satisfies(d -> {
                    assertThat(d.winner()).isEqualTo("yes");
                    assertThat(d.granularity()).isEqualTo(ai.badmonkey.agentspaces.api.spi.Authorizer.Granularity.PEER);
                });
                await(() -> !verdicts.readAll(Template.of(Verdict.class), 10).isEmpty(), Duration.ofSeconds(5));
                assertThat(verdicts.readAll(Template.of(Verdict.class), 10))
                        .containsExactly(new Verdict("p", "yes"));
                vote.castBallot("p", "yes", Lease.of(Duration.ofMinutes(5))); // the same voter again
                Thread.sleep(300);
                assertThat(lead.decisions).as("one decision per proposal").hasSize(1);
            } finally {
                bound.close();
                deciding.close();
            }
        } finally {
            verdicts.close();
            votes.close();
        }
    }

    /** A vote annotation on a space with no vote capability is refused at bind time, naming the remedy. */
    @Test
    void aBallotOnASpaceWithoutAVoteIsRefusedAtBindTime() {
        AgentBinder noVote = new AgentBinder(identity, GroupId.of("zBind"), null, InstantSource.system())
                .space("votes", tasks);
        assertThatThrownBy(() -> noVote.bind(new Panelist()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no vote capability")
                .hasMessageContaining("binder.vote(");
    }

    /** Something an agent wants injected by type. */
    public static final class Thermometer {
        double reading() {
            return 21.5;
        }
    }

    @AgentSpec(name = "sensor", description = "Reads a thermometer", goals = {"sense"})
    public static class Sensor {
        @ai.badmonkey.agentspaces.agent.annotation.CapabilityRef
        Thermometer thermometer;
    }

    /** {@code @CapabilityRef}: a registered client is injected by type; an unknown type is refused at bind time. */
    @Test
    void aCapabilityRefFieldIsInjectedByTypeOrRefused() {
        Thermometer thermometer = new Thermometer();
        binder.space("tasks", tasks).client(Thermometer.class, thermometer);
        Sensor sensor = new Sensor();
        AgentBinder.Bound bound = binder.bind(sensor);
        try {
            assertThat(sensor.thermometer).isSameAs(thermometer);
            assertThat(sensor.thermometer.reading()).isEqualTo(21.5);
        } finally {
            bound.close();
        }
        AgentBinder bare = new AgentBinder(identity, GroupId.of("zBind"), null, InstantSource.system())
                .space("tasks", tasks);
        assertThatThrownBy(() -> bare.bind(new Sensor()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Thermometer")
                .hasMessageContaining("binder.client(");
    }

    /** Several instances of one class become several agents when bound under explicit names. */
    @Test
    void bindingUnderAnExplicitNameOverridesTheDeclaredName() throws Exception {
        try (ReplicatedFixture fx = new ReplicatedFixture("slots")) {
            AgentBinder keyed = new AgentBinder(identity, GroupId.of("zBind"), null,
                    InstantSource.system(), null, identity::subordinate).space("slots", fx.space);
            Auditor finance = new Auditor();
            Auditor ops = new Auditor();
            AgentBinder.Bound f = keyed.bind(finance, "finance");
            AgentBinder.Bound o = keyed.bind(ops, "ops");
            try {
                assertThat(f.agentId()).isEqualTo(identity.agent("finance"));
                assertThat(o.agentId()).isEqualTo(identity.agent("ops"));
                assertThat(finance.slots.writer()).contains(identity.agent("finance"));
                assertThat(ops.slots.writer()).contains(identity.agent("ops"));
                assertThat(f.identity().publicKey()).isNotEqualTo(o.identity().publicKey());
                assertThat(f.card().agent().localName()).isEqualTo("finance");
            } finally {
                keyed.close();
            }
        }
    }

    private static void await(BooleanSupplier condition, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(25);
        }
    }

    // ------------------------------------------------------- QA4 A4-7 phase 3: agents with keys of their own

    @AgentSpec(name = "auditor", description = "Audits", goals = {"audit"})
    public static class Auditor {
        @SpaceRef("slots")
        Space slots;
    }

    @AgentSpec(name = "clerk", description = "Files", goals = {"file"})
    public static class Clerk {
        @SpaceRef("slots")
        Space slots;
    }

    /**
     * QA4 A4-7 phase 3, written before the identity factory existed: with
     * {@code identity::subordinate} each bound agent gets a certified key of its
     * own, its {@code @SpaceRef} is a view writing as that agent, and its card
     * carries the key. With the default factory nothing changes: the handle is the
     * registered space and the card has no key, exactly as before.
     */
    @Test
    void aSubordinateIdentityFactoryHandsAgentsViewsAndKeyedCards() throws Exception {
        try (ReplicatedFixture fx = new ReplicatedFixture("slots")) {
            AgentBinder keyed = new AgentBinder(identity, GroupId.of("zBind"), null,
                    InstantSource.system(), null, identity::subordinate).space("slots", fx.space);
            Auditor auditor = new Auditor();
            Clerk clerk = new Clerk();
            AgentBinder.Bound a = keyed.bind(auditor);
            AgentBinder.Bound c = keyed.bind(clerk);
            try {
                assertThat(a.identity().isSubordinate()).isTrue();
                assertThat(auditor.slots.writer()).contains(identity.agent("auditor"));
                assertThat(clerk.slots.writer()).contains(identity.agent("clerk"));
                assertThat(auditor.slots.id()).isEqualTo(fx.space.id());
                assertThat(a.card().agentPublicKey()).isEqualTo(a.identity().publicKey());
                assertThat(a.card().attested()).isTrue();
                assertThat(c.card().agentPublicKey()).isNotEqualTo(a.card().agentPublicKey());

                auditor.slots.write(new TaskEntry("audited", 1), Lease.of(Duration.ofMinutes(5)));
                clerk.slots.write(new TaskEntry("filed", 2), Lease.of(Duration.ofMinutes(5)));
                java.util.Map<String, Space.Issued<TaskEntry>> byTopic =
                        fx.space.readAllIssued(ai.badmonkey.agentspaces.api.space.Template.of(TaskEntry.class), 10)
                                .stream().collect(java.util.stream.Collectors.toMap(i -> i.entry().topic(), i -> i));
                assertThat(byTopic.get("audited").issuer()).isEqualTo(identity.agent("auditor"));
                assertThat(byTopic.get("audited").attestation()).isEqualTo(Space.Attestation.AGENT_ATTESTED);
                assertThat(byTopic.get("filed").issuer()).isEqualTo(identity.agent("clerk"));
            } finally {
                keyed.close();
            }

            AgentBinder plain = new AgentBinder(identity, GroupId.of("zBind"), null,
                    InstantSource.system()).space("slots", fx.space);
            Auditor asBefore = new Auditor();
            AgentBinder.Bound p = plain.bind(asBefore);
            try {
                assertThat(p.identity().isSubordinate()).isFalse();
                assertThat(asBefore.slots).isSameAs(fx.space);
                assertThat(asBefore.slots.writer()).contains(identity.agent("host"));
                assertThat(p.card().agentPublicKey()).isNull();
                assertThat(p.card().attested()).isFalse();
            } finally {
                plain.close();
            }
        }
    }

    /** QA4 A4-7 phase 3: a factory that names another agent (or peer) is refused at bind time. */
    @Test
    void anIdentityFactoryThatMisnamesTheAgentIsRefused() {
        AgentBinder wrong = new AgentBinder(identity, GroupId.of("zBind"), null,
                InstantSource.system(), null, name -> identity.agentIdentity("somebody-else"))
                .space("tasks", tasks);
        assertThatThrownBy(() -> wrong.bind(new Researcher()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("somebody-else")
                .hasMessageContaining("researcher");
    }

    // ------------------------------------------------------- QA4 A4-8: one bid per space per peer

    /** A bidding planner; two of these on one peer must not silently share one bid function. */
    @AgentSpec(name = "dining-planner", description = "Bids on days", goals = {"plan"})
    public static class DiningPlanner {
        @BidFunction(space = "slots")
        public double bid(TaskEntry slot) {
            return 10;
        }
    }

    @AgentSpec(name = "sights-planner", description = "Bids on days", goals = {"plan"})
    public static class SightsPlanner {
        @BidFunction(space = "slots")
        public double bid(TaskEntry slot) {
            return 20;
        }
    }

    @AgentSpec(name = "rest-planner", description = "Bids elsewhere", goals = {"plan"})
    public static class RestPlanner {
        @BidFunction(space = "naps")
        public double bid(TaskEntry slot) {
            return 5;
        }
    }

    /**
     * QA4 A4-8, written before the fix: {@code ReplicatedSpace.bidFunction} is a
     * single field and the binder called its setter for every bean, so the last
     * bidder bound silently won and the first never priced anything. The second
     * binding must refuse, and say which two agents collided and on what.
     */
    @Test
    void aSecondBidFunctionOnOneSpaceIsRefusedAndNamesBoth() throws Exception {
        try (ReplicatedFixture fx = new ReplicatedFixture("slots")) {
            AgentBinder auction = new AgentBinder(identity, GroupId.of("zBind"), null,
                    InstantSource.system()).space("slots", fx.space);
            auction.bind(new DiningPlanner());
            assertThatThrownBy(() -> auction.bind(new SightsPlanner()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("dining-planner")
                    .hasMessageContaining("sights-planner")
                    .hasMessageContaining("slots")
                    .hasMessageContaining("peer");
            auction.close();
        }
    }

    /** Bid functions on different spaces on one peer are independent and both bind. */
    @Test
    void bidFunctionsOnDifferentSpacesOnOnePeerBothBind() throws Exception {
        try (ReplicatedFixture slots = new ReplicatedFixture("slots");
             ReplicatedFixture naps = new ReplicatedFixture("naps", slots)) {
            AgentBinder auction = new AgentBinder(identity, GroupId.of("zBind"), null,
                    InstantSource.system()).space("slots", slots.space).space("naps", naps.space);
            auction.bind(new DiningPlanner());
            auction.bind(new RestPlanner());
            auction.close();
        }
    }

    /** One in-JVM node and one replicated space named {@code name}; a second fixture shares the node. */
    private final class ReplicatedFixture implements AutoCloseable {
        final ai.badmonkey.agentspaces.peering.node.PeerNode node;
        final ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace space;
        private final boolean ownsNode;

        ReplicatedFixture(String name) throws java.io.IOException {
            ai.badmonkey.agentspaces.test.SimNetwork network = new ai.badmonkey.agentspaces.test.SimNetwork();
            node = ai.badmonkey.agentspaces.peering.node.PeerNode.builder(identity).build();
            node.listen(network.register("bind-" + name), "bind-" + name);
            ai.badmonkey.agentspaces.common.id.GroupId groupId = GroupId.of("zBindAuction");
            ai.badmonkey.agentspaces.api.ad.GroupAdvertisement ad =
                    new ai.badmonkey.agentspaces.api.ad.GroupAdvertisement("aspace://zBindAuction",
                            identity.peerId(), groupId, java.time.Instant.EPOCH, Duration.ofDays(1),
                            "auction", ai.badmonkey.agentspaces.api.ad.GroupAdvertisement.MembershipPolicy.OPEN,
                            ai.badmonkey.agentspaces.api.space.ConflictStrategyType.LEASE_RACE,
                            ai.badmonkey.agentspaces.api.ad.GroupAdvertisement.GossipParameters.defaults());
            ai.badmonkey.agentspaces.peering.node.GroupRuntime runtime = node.joinGroup(ad,
                    ai.badmonkey.agentspaces.peering.membership.GroupMembership.Config.defaults(),
                    java.util.List.of());
            space = ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace
                    .builder(runtime, name, identity, "host").build();
            ownsNode = true;
        }

        ReplicatedFixture(String name, ReplicatedFixture sharing) {
            node = sharing.node;
            space = ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace
                    .builder(node.group(GroupId.of("zBindAuction")).orElseThrow(), name, identity, "host")
                    .build();
            ownsNode = false;
        }

        @Override
        public void close() {
            space.close();
            if (ownsNode) {
                node.close();
            }
        }
    }

    @AgentSpec(name = "researcher", description = "Researches topics", goals = {"research"})
    public static class Researcher {
        final CopyOnWriteArrayList<String> seen = new CopyOnWriteArrayList<>();

        @SpaceTake(space = "tasks", lease = "PT10M", pollTimeout = "PT0.2S")
        public FindingEntry research(TaskEntry task) {
            seen.add(task.topic());
            return new FindingEntry(task.topic(), "done: " + task.topic());
        }
    }

    public static class FlakyWorker {
        final ConcurrentHashMap<String, AtomicInteger> attempts = new ConcurrentHashMap<>();

        @SpaceTake(space = "tasks", lease = "PT0.3S", pollTimeout = "PT0.2S")
        public FindingEntry research(TaskEntry task) {
            int attempt = attempts.computeIfAbsent(task.topic(), k -> new AtomicInteger())
                    .incrementAndGet();
            if (attempt == 1) {
                throw new IllegalStateException("simulated crash on first attempt");
            }
            return new FindingEntry(task.topic(), "recovered on attempt " + attempt);
        }
    }

    public static class Watcher {
        final CopyOnWriteArrayList<String> written = new CopyOnWriteArrayList<>();

        @SpaceNotify(space = "tasks")
        public void onTask(TaskEntry task) {
            written.add(task.topic());
        }
    }

    public static class Broken {
        @SpaceTake(space = "tasks")
        public void twoParameters(TaskEntry task, String extra) {
        }
    }

    @Test
    @Timeout(30)
    void takeLoopDrainsTasksAndWritesResults() throws Exception {
        binder.space("tasks", tasks);
        Researcher researcher = new Researcher();
        binder.bind(researcher);

        for (int i = 0; i < 3; i++) {
            tasks.write(new TaskEntry("topic-" + i, i), Lease.of(Duration.ofMinutes(10)));
        }

        List<FindingEntry> findings = List.of();
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (findings.size() < 3 && System.nanoTime() < deadline) {
            findings = tasks.readAll(Template.of(FindingEntry.class), 10);
            Thread.sleep(50);
        }

        assertThat(findings).hasSize(3);
        assertThat(researcher.seen).containsExactlyInAnyOrder("topic-0", "topic-1", "topic-2");
        assertThat(tasks.readAll(Template.of(TaskEntry.class), 10)).isEmpty();
    }

    @Test
    @Timeout(30)
    void crashedActionLetsTheLeaseLapseAndTheTaskReappears() throws Exception {
        binder.space("tasks", tasks);
        FlakyWorker worker = new FlakyWorker();
        binder.bind(worker);

        tasks.write(new TaskEntry("fragile", 1), Lease.of(Duration.ofMinutes(10)));

        List<FindingEntry> findings = List.of();
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (findings.isEmpty() && System.nanoTime() < deadline) {
            findings = tasks.readAll(Template.of(FindingEntry.class), 10);
            Thread.sleep(50);
        }

        assertThat(findings).hasSize(1);
        assertThat(findings.get(0).summary()).isEqualTo("recovered on attempt 2");
        assertThat(worker.attempts.get("fragile").get()).isEqualTo(2);
    }

    @Test
    @Timeout(30)
    void notifyMethodsSeeWritesWithoutConsuming() throws Exception {
        binder.space("tasks", tasks);
        Watcher watcher = new Watcher();
        binder.bind(watcher);

        tasks.write(new TaskEntry("observed", 1), Lease.of(Duration.ofMinutes(10)));

        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (watcher.written.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }

        assertThat(watcher.written).containsExactly("observed");
        assertThat(tasks.read(Template.of(TaskEntry.class))).isPresent();
    }

    @Test
    void cardsAreGeneratedFromAnnotationsAndSignatures() {
        binder.space("tasks", tasks);
        AgentBinder.Bound bound = binder.bind(new Researcher());

        assertThat(bound.agentId().localName()).isEqualTo("researcher");
        assertThat(bound.card().description()).isEqualTo("Researches topics");
        assertThat(bound.card().goals()).containsExactly("research");
        assertThat(bound.card().consumes())
                .containsExactly(TaskEntry.class.getName() + "#v1");
        assertThat(bound.card().produces())
                .containsExactly(FindingEntry.class.getName() + "#v1");
    }

    /** Takes tasks from one space and watches findings in another. */
    @AgentSpec(name = "twoSpaces", description = "Binds two spaces")
    public static class TwoSpaces {
        @SpaceTake(space = "tasks", pollTimeout = "PT0.2S")
        public void work(TaskEntry task) {
        }

        @SpaceNotify(space = "findings")
        public void watch(FindingEntry finding) {
        }
    }

    /** SPEC §6.1/§10.4: a card's space bindings name, per consumed schema, the space the agent takes or watches it in. */
    @Test
    void cardsDeclareTheSpaceEachConsumedTypeIsTakenFrom() {
        binder.space("tasks", tasks);
        LocalSpace findings = LocalSpace.builder("findings", identity.agent("host")).build();
        try {
            binder.space("findings", findings);
            AgentBinder.Bound bound = binder.bind(new TwoSpaces());

            String taskSchema = TaskEntry.class.getName() + "#v1";
            String findingSchema = FindingEntry.class.getName() + "#v1";
            assertThat(bound.card().spaceBindings())
                    .containsExactlyInAnyOrderEntriesOf(java.util.Map.of(
                            taskSchema, "tasks", findingSchema, "findings"));
            assertThat(bound.card().spaceFor(taskSchema)).contains("tasks");
            assertThat(bound.card().spaceFor(findingSchema)).contains("findings");
            assertThat(bound.card().spaceFor("com.nope.Unknown#v1")).isEmpty();

            // The bindings survive a refresh: the re-issued card is the same card.
            binder.refreshCards();
            assertThat(bound.card().spaceBindings()).containsEntry(taskSchema, "tasks");
        } finally {
            findings.close();
        }
    }

    @Test
    void invalidSignaturesFailFastAtBindTime() {
        binder.space("tasks", tasks);

        assertThatThrownBy(() -> binder.bind(new Broken()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("entry parameter");
        assertThatThrownBy(() -> new AgentBinder(identity, GroupId.of("z"),
                null, InstantSource.system()).bind(new Researcher()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no registered space");
    }

    // ------------------------------------------------- the zero-config surface

    /** A composed stereotype: one custom annotation meaning "agent", Spring-style. */
    @java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
    @java.lang.annotation.Target(java.lang.annotation.ElementType.TYPE)
    @AgentSpec
    public @interface TestFleetAgent {

        /** The agent name. */
        String name() default "";

        /** The description. */
        String description() default "";

        /** The goals. */
        String[] goals() default {};
    }

    /**
     * The drop-annotations-on-a-POJO agent: a composed stereotype, no space
     * names anywhere (the sole registered space is inferred), simple-style
     * durations, and a {@code @SpaceRef}-injected handle for mid-method writes.
     */
    @TestFleetAgent(name = "minimal", description = "Zero-config worker", goals = {"work"})
    public static class MinimalAgent {

        @ai.badmonkey.agentspaces.agent.annotation.SpaceRef
        ai.badmonkey.agentspaces.api.space.Space space;

        @SpaceTake(lease = "10m", pollTimeout = "200ms")
        public FindingEntry work(TaskEntry task) {
            // The injected handle writes a progress marker mid-method (a
            // FindingEntry, not a TaskEntry, so the worker never re-takes it).
            space.write(new FindingEntry("progress:" + task.topic(), "in flight"),
                    Lease.of(Duration.ofMinutes(1)));
            return new FindingEntry(task.topic(), "worked: " + task.topic());
        }
    }

    /** Choreography: react to a task, produce a finding, never consume the task. */
    public static class Triager {

        final CopyOnWriteArrayList<String> threads = new CopyOnWriteArrayList<>();

        @SpaceNotify
        public FindingEntry onTask(TaskEntry task) {
            threads.add(Thread.currentThread().getName());
            return new FindingEntry(task.topic(), "triaged: " + task.topic());
        }
    }

    @Test
    @Timeout(30)
    void aComposedStereotypeWithInferredSpacesAndInjectionJustWorks() throws Exception {
        binder.space("tasks", tasks);
        AgentBinder.Bound bound = binder.bind(new MinimalAgent());

        // The composed annotation supplied the whole identity.
        assertThat(bound.agentId().localName()).isEqualTo("minimal");
        assertThat(bound.card().description()).isEqualTo("Zero-config worker");
        assertThat(bound.card().goals()).containsExactly("work");

        tasks.write(new TaskEntry("zero-config", 1), Lease.of(Duration.ofMinutes(10)));

        List<FindingEntry> findings = List.of();
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (findings.stream().noneMatch(f -> f.summary().startsWith("worked:"))
                && System.nanoTime() < deadline) {
            findings = tasks.readAll(Template.of(FindingEntry.class), 10);
            Thread.sleep(50);
        }
        assertThat(findings).extracting(FindingEntry::summary)
                .contains("worked: zero-config");
        // The @SpaceRef handle wrote the mid-method progress marker too.
        assertThat(findings).extracting(FindingEntry::topic)
                .contains("progress:zero-config");
    }

    @Test
    @Timeout(30)
    void notifyReturnsBecomeEntriesOffTheDeliveryThread() throws Exception {
        binder.space("tasks", tasks);
        Triager triager = new Triager();
        binder.bind(triager);

        tasks.write(new TaskEntry("choreographed", 1), Lease.of(Duration.ofMinutes(10)));

        List<FindingEntry> findings = List.of();
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (findings.isEmpty() && System.nanoTime() < deadline) {
            findings = tasks.readAll(Template.of(FindingEntry.class), 10);
            Thread.sleep(20);
        }

        // React-and-write in one method: the return value became the next entry,
        // the reacted-to entry was not consumed, and the reaction ran on the
        // binder's own virtual thread, not the fabric's delivery thread.
        assertThat(findings).extracting(FindingEntry::summary)
                .containsExactly("triaged: choreographed");
        assertThat(tasks.read(Template.of(TaskEntry.class))).isPresent();
        assertThat(triager.threads).hasSize(1);
        assertThat(triager.threads.get(0)).startsWith("space-notify-");
        // And the produced type is on the card, so discovery sees the flow.
        assertThat(binder.bind(new Triager()).card().produces())
                .containsExactly(FindingEntry.class.getName() + "#v1");
    }

    @Test
    void severalSpacesWithoutANameFailFastAndNameTheCandidates() {
        binder.space("tasks", tasks);
        LocalSpace other = LocalSpace.builder("other", identity.agent("host")).build();
        try {
            binder.space("other", other);
            assertThatThrownBy(() -> binder.bind(new Triager()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("2 spaces are registered")
                    .hasMessageContaining("tasks")
                    .hasMessageContaining("other");
        } finally {
            other.close();
        }
    }

    // ------------------------------------------------ crash hand-off (spec §10.3)

    /** Never completes: every attempt throws, so its take lease always lapses. */
    public static class AlwaysCrashes {
        final AtomicInteger attempts = new AtomicInteger();

        @SpaceTake(space = "tasks", lease = "PT0.3S", pollTimeout = "PT0.1S")
        public FindingEntry research(TaskEntry task) {
            attempts.incrementAndGet();
            throw new IllegalStateException("simulated crash");
        }
    }

    /** Always completes. */
    public static class Reliable {
        @SpaceTake(space = "tasks", lease = "PT10M", pollTimeout = "PT0.1S")
        public FindingEntry research(TaskEntry task) {
            return new FindingEntry(task.topic(), "reliable: " + task.topic());
        }
    }

    /** Throws an Error (not an exception) on the first attempt, then succeeds. */
    public static class ErrorThrower {
        final AtomicInteger attempts = new AtomicInteger();

        @SpaceTake(space = "tasks", lease = "PT0.3S", pollTimeout = "PT0.1S")
        public FindingEntry research(TaskEntry task) {
            if (attempts.incrementAndGet() == 1) {
                throw new AssertionError("simulated Error on first attempt");
            }
            return new FindingEntry(task.topic(), "recovered on attempt " + attempts.get());
        }
    }

    /** SPEC §10.3: a crashed worker's lapsed lease lets another bound worker finish the task. */
    @Test
    @Timeout(30)
    void crashedWorkersTaskIsFinishedByAnotherBoundWorker() throws Exception {
        binder.space("tasks", tasks);
        AlwaysCrashes crasher = new AlwaysCrashes();
        binder.bind(crasher);
        tasks.write(new TaskEntry("handoff", 1), Lease.of(Duration.ofMinutes(10)));
        awaitTrue(() -> crasher.attempts.get() >= 1, Duration.ofSeconds(10));

        binder.bind(new Reliable());

        awaitTrue(() -> !tasks.readAll(Template.of(FindingEntry.class), 10).isEmpty(),
                Duration.ofSeconds(20));
        List<FindingEntry> findings = tasks.readAll(Template.of(FindingEntry.class), 10);
        assertThat(findings).extracting(FindingEntry::summary)
                .containsExactly("reliable: handoff");
        assertThat(crasher.attempts.get()).isGreaterThanOrEqualTo(1);
        assertThat(tasks.readAll(Template.of(TaskEntry.class), 10)).isEmpty();
    }

    /** SPEC §10.3: an Error thrown by the action lapses the lease but must not kill the worker loop. */
    @Test
    @Timeout(30)
    void anErrorInTheActionDoesNotKillTheWorkerLoop() throws Exception {
        binder.space("tasks", tasks);
        ErrorThrower worker = new ErrorThrower();
        binder.bind(worker);

        tasks.write(new TaskEntry("fragile", 1), Lease.of(Duration.ofMinutes(10)));

        awaitTrue(() -> !tasks.readAll(Template.of(FindingEntry.class), 10).isEmpty(),
                Duration.ofSeconds(20));
        assertThat(tasks.readAll(Template.of(FindingEntry.class), 10))
                .extracting(FindingEntry::summary)
                .containsExactly("recovered on attempt 2");
    }

    // ------------------------------------------- notify discipline (spec §10.3)

    /** Reacts but returns null: nothing must be written back. */
    public static class Silent {
        final CopyOnWriteArrayList<String> seen = new CopyOnWriteArrayList<>();

        @SpaceNotify(space = "tasks")
        public FindingEntry onTask(TaskEntry task) {
            seen.add(task.topic());
            return null;
        }
    }

    /** Counts every invocation. */
    public static class Counter {
        final AtomicInteger invocations = new AtomicInteger();

        @SpaceNotify(space = "tasks")
        public void onTask(TaskEntry task) {
            invocations.incrementAndGet();
        }
    }

    /** Blocks every reaction on a gate until the test releases it. */
    public static class SlowReactor {
        final CountDownLatch gate = new CountDownLatch(1);
        final AtomicInteger started = new AtomicInteger();

        @SpaceNotify(space = "tasks")
        public void onTask(TaskEntry task) throws InterruptedException {
            started.incrementAndGet();
            gate.await(10, TimeUnit.SECONDS);
        }
    }

    /** A short-leased reactor. */
    public static class ShortLeased {
        final CopyOnWriteArrayList<String> seen = new CopyOnWriteArrayList<>();

        @SpaceNotify(space = "tasks", lease = "300ms")
        public void onTask(TaskEntry task) {
            seen.add(task.topic());
        }
    }

    /** Routes its returned entry into another space. */
    public static class Router {
        @SpaceNotify(space = "tasks", resultSpace = "findings")
        public FindingEntry onTask(TaskEntry task) {
            return new FindingEntry(task.topic(), "routed: " + task.topic());
        }
    }

    /** SPEC §10.3: a null return from a @SpaceNotify method writes nothing. */
    @Test
    @Timeout(30)
    void notifyReturningNullWritesNothing() throws Exception {
        binder.space("tasks", tasks);
        Silent silent = new Silent();
        binder.bind(silent);

        tasks.write(new TaskEntry("quiet", 1), Lease.of(Duration.ofMinutes(10)));
        awaitTrue(() -> silent.seen.size() == 1, Duration.ofSeconds(10));
        Thread.sleep(300);

        assertThat(tasks.readAll(Template.of(FindingEntry.class), 10)).isEmpty();
        assertThat(tasks.read(Template.of(TaskEntry.class))).isPresent();
    }

    /**
     * SPEC §10.3 / TECH-SPEC §9.2: delivery is deduplicated per entry id (at-least-once
     * becomes effectively-once) against a bounded set of 4,096 remembered ids.
     */
    @Test
    @Timeout(60)
    void notifyIsDeliveredOnceEvenWhenTheSpaceRedelivers() throws Exception {
        // A Space that hands the binder's listener every event twice and lets the
        // test replay any recorded event later: the redelivery the fabric may do.
        AtomicReference<SpaceListener<Object>> captured = new AtomicReference<>();
        CopyOnWriteArrayList<SpaceEvent<Object>> recorded = new CopyOnWriteArrayList<>();
        Space redelivering = redeliveringProxy(tasks, captured, recorded);
        binder.space("tasks", redelivering);
        Counter counter = new Counter();
        binder.bind(counter);

        tasks.write(new TaskEntry("once", 1), Lease.of(Duration.ofMinutes(10)));
        awaitTrue(() -> recorded.size() >= 1, Duration.ofSeconds(10));
        Thread.sleep(200);
        assertThat(counter.invocations.get()).isEqualTo(1);

        // Fill the dedup window with 4,096 further ids, then replay the first
        // event: its id has been evicted, so it is handled once more, while the
        // most recent id is still remembered and stays deduplicated.
        for (int i = 0; i < 4096; i++) {
            tasks.write(new TaskEntry("fill-" + i, i), Lease.of(Duration.ofMinutes(10)));
        }
        awaitTrue(() -> counter.invocations.get() == 4097, Duration.ofSeconds(30));
        SpaceEvent<Object> first = recorded.get(0);
        SpaceEvent<Object> last = recorded.get(recorded.size() - 1);
        captured.get().onEvent(last);
        Thread.sleep(200);
        assertThat(counter.invocations.get()).isEqualTo(4097);
        captured.get().onEvent(first);
        awaitTrue(() -> counter.invocations.get() == 4098, Duration.ofSeconds(10));
    }

    /** SPEC §10.3: a slow reaction runs on its own virtual thread and never blocks later deliveries. */
    @Test
    @Timeout(30)
    void aSlowReactionDoesNotBlockDelivery() throws Exception {
        binder.space("tasks", tasks);
        SlowReactor reactor = new SlowReactor();
        binder.bind(reactor);
        try {
            for (int i = 0; i < 3; i++) {
                tasks.write(new TaskEntry("slow-" + i, i), Lease.of(Duration.ofMinutes(10)));
            }
            // All three reactions start while the first is still blocked.
            awaitTrue(() -> reactor.started.get() == 3, Duration.ofSeconds(5));
            assertThat(reactor.gate.getCount()).isEqualTo(1);
        } finally {
            reactor.gate.countDown();
        }
    }

    /** SPEC §7.2 + §10.3: the binder renews the notify subscription, so reactions outlive the lease. */
    @Test
    @Timeout(30)
    void notifySubscriptionOutlivesItsLeaseWhileBound() throws Exception {
        binder.space("tasks", tasks);
        ShortLeased reactor = new ShortLeased();
        binder.bind(reactor);

        tasks.write(new TaskEntry("first", 1), Lease.of(Duration.ofMinutes(10)));
        awaitTrue(() -> reactor.seen.contains("first"), Duration.ofSeconds(10));

        Thread.sleep(800); // well past the 300ms lease
        tasks.write(new TaskEntry("second", 2), Lease.of(Duration.ofMinutes(10)));
        awaitTrue(() -> reactor.seen.contains("second"), Duration.ofSeconds(10));
        assertThat(reactor.seen).containsExactly("first", "second");
    }

    /** SPEC §10.3: a @SpaceNotify resultSpace routes the returned entry away from the watched space. */
    @Test
    @Timeout(30)
    void notifyResultSpaceRoutesTheReturnedEntry() throws Exception {
        LocalSpace findings = LocalSpace.builder("findings", identity.agent("host")).build();
        try {
            binder.space("tasks", tasks).space("findings", findings);
            binder.bind(new Router());

            tasks.write(new TaskEntry("route me", 1), Lease.of(Duration.ofMinutes(10)));

            assertThat(findings.read(Template.of(FindingEntry.class), Duration.ofSeconds(10)))
                    .hasValueSatisfying(f -> assertThat(f.summary()).isEqualTo("routed: route me"));
            assertThat(tasks.readAll(Template.of(FindingEntry.class), 10)).isEmpty();
            assertThat(tasks.read(Template.of(TaskEntry.class))).isPresent();
        } finally {
            findings.close();
        }
    }

    // ---------------------------------------- sole-space inference (spec §10.3)

    /** SPEC §10.3: an empty space name with no registered space fails fast. */
    @Test
    void emptySpaceNameWithNoRegisteredSpacesFailsFast() {
        assertThatThrownBy(() -> binder.bind(new Triager()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("0 spaces are registered")
                .hasMessageContaining("name one");
    }

    public static class InferredTake {
        @SpaceTake
        public void work(TaskEntry task) {
        }
    }

    public static class InferredRef {
        @SpaceRef
        Space space;
    }

    public static class InferredBid {
        @BidFunction
        public double bid(TaskEntry task) {
            return 1;
        }
    }

    /** SPEC §10.3: the several-spaces failure names the candidates for take, ref and bid too. */
    @Test
    void severalSpacesWithoutANameFailFastForTakeRefAndBid() {
        binder.space("tasks", tasks);
        LocalSpace other = LocalSpace.builder("other", identity.agent("host")).build();
        try {
            binder.space("other", other);
            for (Object agent : List.of(new InferredTake(), new InferredRef(), new InferredBid())) {
                assertThatThrownBy(() -> binder.bind(agent))
                        .as(agent.getClass().getSimpleName())
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("2 spaces are registered")
                        .hasMessageContaining("tasks")
                        .hasMessageContaining("other");
            }
        } finally {
            other.close();
        }
    }

    // --------------------------------------------------- @SpaceRef (spec §10.3)

    public abstract static class Base {
        @SpaceRef("other")
        protected Space aux;
    }

    public static class Child extends Base {
        @SpaceRef("tasks")
        Space main;
        @SpaceRef("tasks")
        Object loose;
    }

    public static class BadRef {
        @SpaceRef("tasks")
        String bad;
    }

    /** SPEC §10.3: named @SpaceRef fields are injected, including those declared on a superclass. */
    @Test
    void spaceRefByNameAndInheritedFieldsAreInjected() {
        binder.space("tasks", tasks);
        LocalSpace other = LocalSpace.builder("other", identity.agent("host")).build();
        try {
            binder.space("other", other);
            Child child = new Child();
            binder.bind(child);
            assertThat(child.main).isSameAs(tasks);
            assertThat(child.aux).isSameAs(other);
            // Current behaviour, pinned: any field a Space is assignable to is accepted.
            assertThat(child.loose).isSameAs(tasks);
        } finally {
            other.close();
        }
    }

    /** SPEC §10.3: a @SpaceRef on a field a Space cannot be assigned to fails fast at bind time. */
    @Test
    void spaceRefOnANonSpaceFieldFailsFast() {
        binder.space("tasks", tasks);
        assertThatThrownBy(() -> binder.bind(new BadRef()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must have type Space");
    }

    // ------------------------------------------ meta-annotation (spec §10.3)

    @AgentSpec(name = "direct", description = "direct wins")
    @TestFleetAgent(name = "composed", description = "composed loses")
    public static class BothAnnotated {
    }

    @java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
    @java.lang.annotation.Target(java.lang.annotation.ElementType.TYPE)
    @AgentSpec
    public @interface Bare {
    }

    @Bare
    public static class BarelyAnnotated {
    }

    @java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
    @java.lang.annotation.Target(java.lang.annotation.ElementType.TYPE)
    @TestFleetAgent(name = "second-level")
    public @interface SecondLevel {
    }

    @SecondLevel
    public static class TwoLevelsDeep {
    }

    /** SPEC §10.3: a direct @AgentSpec takes precedence over a composed stereotype. */
    @Test
    void directAgentSpecWinsOverAComposedStereotype() {
        binder.space("tasks", tasks);
        AgentBinder.Bound bound = binder.bind(new BothAnnotated());
        assertThat(bound.agentId().localName()).isEqualTo("direct");
        assertThat(bound.card().description()).isEqualTo("direct wins");
    }

    /** SPEC §10.3: a stereotype without the named attributes yields the default identity. */
    @Test
    void aStereotypeWithoutAttributesYieldsEmptyIdentity() {
        binder.space("tasks", tasks);
        assertThat(AgentBinder.isAgentType(BarelyAnnotated.class)).isTrue();
        AgentBinder.Bound bound = binder.bind(new BarelyAnnotated());
        assertThat(bound.agentId().localName()).isEqualTo("barelyAnnotated");
        assertThat(bound.card().description()).isEmpty();
        assertThat(bound.card().goals()).isEmpty();
    }

    /** SPEC §10.3: resolution is exactly one level of meta-annotation deep. */
    @Test
    void aTwoLevelStereotypeIsNotResolved() {
        assertThat(AgentBinder.isAgentType(TwoLevelsDeep.class)).isFalse();
        binder.space("tasks", tasks);
        assertThat(binder.bind(new TwoLevelsDeep()).agentId().localName())
                .isEqualTo("twoLevelsDeep");
    }

    // ------------------------------------------------ @BidFunction (spec §10.3)

    public static class LocalBidder {
        @BidFunction(space = "tasks")
        public double bid(TaskEntry task) {
            return 1;
        }
    }

    public static class IntBidder {
        @BidFunction(space = "tasks")
        public int bid(TaskEntry task) {
            return 1;
        }
    }

    /** SPEC §10.3: a @BidFunction needs an AUCTION-capable ReplicatedSpace; a LocalSpace fails fast. */
    @Test
    void bindingABidFunctionOnALocalSpaceFailsFast() {
        binder.space("tasks", tasks);
        assertThatThrownBy(() -> binder.bind(new LocalBidder()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires a ReplicatedSpace");
    }

    /** SPEC §10.3: a bid is a double-valued function; other return types fail fast. */
    @Test
    void nonDoubleBidFunctionFailsFast() {
        binder.space("tasks", tasks);
        assertThatThrownBy(() -> binder.bind(new IntBidder()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must return double");
    }

    // ------------------------------------------------------------------ helpers

    private static void awaitTrue(BooleanSupplier condition, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within " + timeout);
            }
            Thread.sleep(20);
        }
    }

    /**
     * Wraps a space so every notify listener receives each event twice (the
     * fabric's at-least-once redelivery), capturing the listener and the
     * events so the test can replay one later.
     */
    @SuppressWarnings("unchecked")
    private static Space redeliveringProxy(Space target,
                                           AtomicReference<SpaceListener<Object>> captured,
                                           List<SpaceEvent<Object>> recorded) {
        return (Space) Proxy.newProxyInstance(Space.class.getClassLoader(),
                new Class<?>[] {Space.class}, (proxy, method, args) -> {
                    try {
                        if (method.getName().equals("notify")) {
                            SpaceListener<Object> original = (SpaceListener<Object>) args[1];
                            captured.set(original);
                            SpaceListener<Object> doubled = event -> {
                                recorded.add(event);
                                original.onEvent(event);
                                original.onEvent(event);
                            };
                            return method.invoke(target, args[0], doubled, args[2]);
                        }
                        return method.invoke(target, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }
}
