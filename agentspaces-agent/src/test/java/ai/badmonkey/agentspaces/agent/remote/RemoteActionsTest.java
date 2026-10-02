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
package ai.badmonkey.agentspaces.agent.remote;

import ai.badmonkey.agentspaces.agent.AgentBinder;
import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.AdvertisementSigner;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Remote cards as invocable actions, over a real two-peer TCP fleet: the worker
 * peer binds an annotated agent (which publishes its card), the caller peer
 * discovers the card as a {@link RemoteAction} and invokes it as a space
 * round-trip.
 */
class RemoteActionsTest {

    /** A research request. */
    public record Task(String topic, int priority) {
    }

    /** The result, correlated to its task by the shared {@code topic} field. */
    public record Finding(String topic, String summary) {
    }

    /** The remote worker: takes tasks, produces findings. */
    @AgentSpec(name = "researcher", description = "Researches topics",
            goals = {"answer research questions"})
    public static class Researcher {

        /**
         * Works one task.
         *
         * @param task the task
         * @return the finding
         */
        @SpaceTake(space = "work", pollTimeout = "PT0.2S")
        public Finding research(Task task) {
            return new Finding(task.topic(), "researched: " + task.topic());
        }
    }

    private record Peer(PeerNode node, PeerIdentity identity, GroupRuntime runtime,
                        DiscoveryService discovery, ReplicatedSpace work, AgentBinder binder) {
    }

    private final List<Peer> peers = new ArrayList<>();

    /** A peer on the in-memory network under the shared test clock. */
    private record SimPeer(PeerNode node, PeerIdentity identity, DiscoveryService discovery,
                           LocalSpace work, AgentBinder binder) {
    }

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<SimPeer> simPeers = new ArrayList<>();

    /** A peer on the in-memory network with two replicated spaces, on the system clock. */
    private record FleetPeer(PeerNode node, PeerIdentity identity, GroupRuntime runtime,
                             DiscoveryService discovery, ReplicatedSpace work,
                             ReplicatedSpace other, AgentBinder binder) {
    }

    private final SimNetwork fleetNetwork = new SimNetwork();
    private final List<FleetPeer> fleetPeers = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (Peer peer : peers) {
            peer.binder().close();
            peer.work().close();
            peer.node().close();
        }
        for (SimPeer peer : simPeers) {
            peer.binder().close();
            peer.work().close();
            peer.node().close();
        }
        for (FleetPeer peer : fleetPeers) {
            peer.binder().close();
            peer.work().close();
            peer.other().close();
            peer.node().close();
        }
    }

    @Test
    @Timeout(90)
    void aForeignCardBecomesATypedActionWhoseInvocationRoundTripsTheFleet() throws Exception {
        int seedPort = freePort();
        Peer worker = startPeer(seedPort, 0);
        Peer caller = startPeer(freePort(), seedPort);
        worker.binder().bind(new Researcher());

        RemoteActions remote = new RemoteActions(caller.discovery(),
                caller.identity().peerId(), Map.of("work", caller.work()));

        // The worker's card gossips over; the caller sees exactly one foreign
        // action, typed end to end, and never its own reflection.
        List<RemoteAction> actions = awaitActions(remote, 1, Duration.ofSeconds(30));
        assertThat(actions).hasSize(1);
        RemoteAction research = actions.get(0);
        assertThat(research.name()).isEqualTo("researcher_Task");
        assertThat(research.declared()).as("a binder card declares the action")
                .hasValueSatisfying(declared -> assertThat(declared.name()).isEqualTo("research"));
        assertThat(research.inputType()).isEqualTo(Task.class);
        assertThat(research.outputType()).isEqualTo(Finding.class);
        assertThat(research.description()).isEqualTo("Researches topics");
        assertThat(research.goals()).containsExactly("answer research questions");

        // Invoking the action is a space round-trip: write the task, the remote
        // worker takes and completes it, the correlated finding comes back.
        Optional<Finding> finding = research.invoke(new Task("agentic memory", 3),
                Finding.class, Duration.ofSeconds(30));
        assertThat(finding).isPresent();
        assertThat(finding.get().topic()).isEqualTo("agentic memory");
        assertThat(finding.get().summary()).isEqualTo("researched: agentic memory");

        // Correlation picks the right answer even with a decoy result present.
        caller.work().write(new Finding("something else", "unrelated"),
                ai.badmonkey.agentspaces.api.space.Lease.of(Duration.ofMinutes(5)));
        Optional<Finding> second = research.invoke(new Task("space robotics", 1),
                Finding.class, Duration.ofSeconds(30));
        assertThat(second).isPresent();
        assertThat(second.get().topic()).isEqualTo("space robotics");

        // The registry filters by production type, and an absent capability
        // yields no action rather than a wrong one.
        assertThat(remote.producing(Finding.class)).hasSize(1);
        assertThat(remote.producing(Task.class)).isEmpty();
    }

    @Test
    @Timeout(60)
    void anUnservedInvocationTimesOutEmptyInsteadOfMisdelivering() throws Exception {
        int seedPort = freePort();
        Peer worker = startPeer(seedPort, 0);
        Peer caller = startPeer(freePort(), seedPort);
        worker.binder().bind(new Researcher());

        RemoteActions remote = new RemoteActions(caller.discovery(),
                caller.identity().peerId(), Map.of("work", caller.work()));
        RemoteAction research = awaitActions(remote, 1, Duration.ofSeconds(30)).get(0);

        // Close the worker's loops and wait until it has actually quiesced: a
        // worker mid-take is allowed to finish that task (graceful drain), so
        // prove it stopped by writing probe tasks until one survives untaken.
        worker.binder().close();
        awaitWorkerQuiescence(caller, Duration.ofSeconds(30));

        // The unserved invocation comes back empty rather than misdelivering,
        // and the task entry keeps waiting for a future worker: that is the
        // lease model, not a lost request.
        Optional<Object> result = research.invoke(new Task("orphaned", 1),
                Duration.ofSeconds(3));
        assertThat(result).isEmpty();
        assertThat(caller.work().readAll(Template.of(Task.class), 100))
                .extracting(Task::topic).contains("orphaned");
    }

    /** Writes probe tasks until one survives untaken, proving no worker runs. */
    private static void awaitWorkerQuiescence(Peer caller, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        int probe = 0;
        while (System.nanoTime() < deadline) {
            String topic = "quiescence-probe-" + probe++;
            caller.work().write(new Task(topic, 0),
                    ai.badmonkey.agentspaces.api.space.Lease.of(Duration.ofMinutes(5)));
            Thread.sleep(3_000);
            boolean survived = caller.work().readAll(Template.of(Task.class), 100).stream()
                    .anyMatch(task -> task.topic().equals(topic));
            if (survived) {
                return;
            }
        }
        throw new AssertionError("the worker never quiesced within " + timeout);
    }


    // --------------------------------------------------- the card view (spec §10.6)

    /** SPEC §10.6: the registry watches foreign cards only — never the peer's own. */
    @Test
    @Timeout(90)
    void theRegistryNeverExposesThePeersOwnCards() throws Exception {
        int seedPort = freePort();
        Peer worker = startPeer(seedPort, 0);
        Peer caller = startPeer(freePort(), seedPort);
        worker.binder().bind(new Researcher());
        caller.binder().bind(new Researcher()); // the caller advertises the same skill

        RemoteActions callerView = new RemoteActions(caller.discovery(),
                caller.identity().peerId(), Map.of("work", caller.work()));
        List<RemoteAction> actions = awaitActions(callerView, 1, Duration.ofSeconds(30));
        // Give a second (own) card every chance to appear; it must not.
        Thread.sleep(1_000);
        actions = callerView.available();
        assertThat(actions).hasSize(1);
        assertThat(actions.get(0).card().issuer()).isEqualTo(worker.identity().peerId());

        RemoteActions workerView = new RemoteActions(worker.discovery(),
                worker.identity().peerId(), Map.of("work", worker.work()));
        assertThat(awaitActions(workerView, 1, Duration.ofSeconds(30)))
                .extracting(action -> action.card().issuer())
                .containsExactly(caller.identity().peerId());
    }

    /** SPEC §10.6: cards are TTL-leased, so an agent that stops refreshing ages out of the action space. */
    @Test
    @Timeout(60)
    void anExpiredCardRemovesItsAction() throws Exception {
        SimPeer worker = simPeer("w");
        SimPeer caller = simPeer("c", "w");
        tick(4);
        worker.binder().bind(new Researcher());
        RemoteActions remote = new RemoteActions(caller.discovery(),
                caller.identity().peerId(), Map.of("work", caller.work()));
        for (int i = 0; i < 20 && remote.available().isEmpty(); i++) {
            tick(1);
        }
        assertThat(remote.available()).hasSize(1);

        clock.advance(Duration.ofMinutes(16)); // past the 15-minute card TTL, no refresh
        assertThat(remote.available()).isEmpty();

        // A refresh brings it back: the action space is live in both directions.
        worker.binder().refreshCards();
        for (int i = 0; i < 20 && remote.available().isEmpty(); i++) {
            tick(1);
        }
        assertThat(remote.available()).hasSize(1);
    }

    /** SPEC §10.6: multi-type cards fan out per resolvable pair; unresolvable and platform schemas are skipped. */
    @Test
    void multiPairCardsFanOutAndUnresolvableSchemasAreSkipped() throws Exception {
        SimPeer local = simPeer("solo");
        PeerIdentity foreign = PeerIdentity.generate();
        AdvertisementSigner signer = new AdvertisementSigner();
        GroupId groupId = GroupId.of("zRemote"); // the simPeer fixture's group
        AgentCard first = new AgentCard("aspace://" + groupId.value() + "/agent/multi",
                foreign.peerId(), groupId, clock.instant(), Duration.ofMinutes(15),
                foreign.agent("multi"), "Does several things", List.of("multi-task"),
                List.of(Task.class.getName() + "#v1", "com.nope.Missing#v1"),
                List.of(Finding.class.getName() + "#v1", "java.lang.Runtime#v1"),
                Map.of());
        local.discovery().publish(signer.sign(first, foreign));

        RemoteActions remote = new RemoteActions(local.discovery(),
                local.identity().peerId(), Map.of("work", local.work()));
        List<RemoteAction> actions = remote.available();
        assertThat(actions).hasSize(1);
        assertThat(actions.get(0).inputType()).isEqualTo(Task.class);
        assertThat(actions.get(0).outputType()).isEqualTo(Finding.class);
        assertThat(actions.get(0).name()).isEqualTo("multi_Task");

        // A refreshed copy of the same capability is one action, the newest card.
        clock.advance(Duration.ofMinutes(1));
        AgentCard refreshed = new AgentCard(first.id(), first.issuer(), first.group(),
                clock.instant(), first.ttl(), first.agent(), first.description(), first.goals(),
                first.consumes(), first.produces(), first.costHints());
        local.discovery().publish(signer.sign(refreshed, foreign));
        actions = remote.available();
        assertThat(actions).hasSize(1);
        assertThat(actions.get(0).card().issued()).isEqualTo(refreshed.issued());
    }

    /** A second request type, for a two-action agent. */
    public record Question(String topic, String text) {
    }

    /** Its answer, correlated by {@code topic}. */
    public record Answer(String topic, String answer) {
    }

    /** SPEC §6.1/§10.6 v0.1.13 (TODO item 6): a card that declares two actions yields exactly those two remote actions, not the four of the cross product, each with its own name and description; a non-invocable ballot action yields none. */
    @Test
    void aTwoActionCardYieldsExactlyItsDeclaredActions() throws Exception {
        SimPeer local = simPeer("declared");
        PeerIdentity foreign = PeerIdentity.generate();
        GroupId groupId = GroupId.of("zRemote");
        String task = Task.class.getName() + "#v1";
        String finding = Finding.class.getName() + "#v1";
        String question = Question.class.getName() + "#v1";
        String answer = Answer.class.getName() + "#v1";
        AgentCard card = new AgentCard("aspace://" + groupId.value() + "/agent/desk",
                foreign.peerId(), groupId, clock.instant(), Duration.ofMinutes(15),
                foreign.agent("desk"), "A research desk", List.of("help"),
                List.of(task, question), List.of(finding, answer), Map.of(), Map.of(), null,
                List.of(new ai.badmonkey.agentspaces.api.ad.CardAction("research",
                                "Researches a topic in depth", List.of(task), List.of(finding),
                                "work", ai.badmonkey.agentspaces.api.ad.CardAction.TAKE),
                        new ai.badmonkey.agentspaces.api.ad.CardAction("answer",
                                "", List.of(question), List.of(answer),
                                "work", ai.badmonkey.agentspaces.api.ad.CardAction.NOTIFY),
                        new ai.badmonkey.agentspaces.api.ad.CardAction("vote",
                                "", List.of(task), List.of(answer),
                                "work", ai.badmonkey.agentspaces.api.ad.CardAction.BALLOT)));
        local.discovery().publish(new AdvertisementSigner().sign(card, foreign));

        List<RemoteAction> actions = new RemoteActions(local.discovery(),
                local.identity().peerId(), Map.of("work", local.work())).available();
        assertThat(actions).extracting(a -> a.declared().orElseThrow().name())
                .containsExactlyInAnyOrder("research", "answer");
        assertThat(actions).extracting(RemoteAction::name)
                .as("planner names stay type-derived").containsExactlyInAnyOrder("desk_Task", "desk_Question");
        RemoteAction research = actions.stream()
                .filter(a -> a.name().equals("desk_Task")).findFirst().orElseThrow();
        assertThat(research.inputType()).isEqualTo(Task.class);
        assertThat(research.outputType()).isEqualTo(Finding.class);
        assertThat(research.description()).isEqualTo("Researches a topic in depth");
        RemoteAction reply = actions.stream()
                .filter(a -> a.name().equals("desk_Question")).findFirst().orElseThrow();
        assertThat(reply.inputType()).isEqualTo(Question.class);
        assertThat(reply.outputType()).isEqualTo(Answer.class);
        assertThat(reply.description()).as("an undescribed action falls back to the card")
                .isEqualTo("A research desk");
    }

    /**
     * SPEC §10.6 v0.1.13: a take and a notify over the same types are two actions,
     * each routed to its own declared space, and a refreshed card still yields two.
     */
    @Test
    void twoDeclaredActionsOverTheSameTypesStayTwoAndRouteToTheirOwnSpaces() throws Exception {
        SimPeer local = simPeer("same-types");
        LocalSpace other = LocalSpace.builder("other", local.identity().agent("host")).build();
        try {
            PeerIdentity foreign = PeerIdentity.generate();
            AdvertisementSigner signer = new AdvertisementSigner();
            GroupId groupId = GroupId.of("zRemote");
            String task = Task.class.getName() + "#v1";
            String finding = Finding.class.getName() + "#v1";
            List<ai.badmonkey.agentspaces.api.ad.CardAction> declared = List.of(
                    new ai.badmonkey.agentspaces.api.ad.CardAction("research", "Researches",
                            List.of(task), List.of(finding), "work",
                            ai.badmonkey.agentspaces.api.ad.CardAction.TAKE),
                    new ai.badmonkey.agentspaces.api.ad.CardAction("watch", "Watches",
                            List.of(task), List.of(finding), "other",
                            ai.badmonkey.agentspaces.api.ad.CardAction.NOTIFY));
            AgentCard card = new AgentCard("aspace://" + groupId.value() + "/agent/desk",
                    foreign.peerId(), groupId, clock.instant(), Duration.ofMinutes(15),
                    foreign.agent("desk"), "A desk", List.of("help"),
                    List.of(task), List.of(finding), Map.of(), Map.of(), null, declared);
            local.discovery().publish(signer.sign(card, foreign));

            RemoteActions remote = new RemoteActions(local.discovery(), local.identity().peerId(),
                    Map.of("work", local.work(), "other", other));
            List<RemoteAction> actions = remote.available();
            assertThat(actions).extracting(a -> a.declared().orElseThrow().name())
                    .containsExactlyInAnyOrder("research", "watch");
            for (RemoteAction action : actions) {
                Space expected = action.declared().orElseThrow().name().equals("research")
                        ? local.work() : other;
                assertThat(action.taskSpace()).as(action.declared().orElseThrow().name())
                        .isSameAs(expected);
            }

            clock.advance(Duration.ofMinutes(1));
            AgentCard refreshed = new AgentCard(card.id(), card.issuer(), card.group(),
                    clock.instant(), card.ttl(), card.agent(), card.description(), card.goals(),
                    card.consumes(), card.produces(), Map.of(), Map.of(), null, declared);
            local.discovery().publish(signer.sign(refreshed, foreign));
            assertThat(remote.available()).as("a refresh is not a third action").hasSize(2)
                    .allSatisfy(a -> assertThat(a.card().issued()).isEqualTo(refreshed.issued()));
        } finally {
            other.close();
        }
    }

    /** SPEC §10.6: a card declaring no actions yields the full cross product of its flat lists. */
    @Test
    void aCardWithoutDeclaredActionsYieldsTheCrossProduct() throws Exception {
        SimPeer local = simPeer("cross");
        PeerIdentity foreign = PeerIdentity.generate();
        GroupId groupId = GroupId.of("zRemote");
        AgentCard card = new AgentCard("aspace://" + groupId.value() + "/agent/legacy",
                foreign.peerId(), groupId, clock.instant(), Duration.ofMinutes(15),
                foreign.agent("legacy"), "An older card", List.of("help"),
                List.of(Task.class.getName() + "#v1", Question.class.getName() + "#v1"),
                List.of(Finding.class.getName() + "#v1", Answer.class.getName() + "#v1"),
                Map.of());
        local.discovery().publish(new AdvertisementSigner().sign(card, foreign));

        List<RemoteAction> actions = new RemoteActions(local.discovery(),
                local.identity().peerId(), Map.of("work", local.work())).available();
        assertThat(actions).hasSize(4).allSatisfy(a -> assertThat(a.declared()).isEmpty());
        assertThat(actions).extracting(a -> a.inputType().getSimpleName() + "->"
                        + a.outputType().getSimpleName())
                .containsExactlyInAnyOrder("Task->Finding", "Task->Answer",
                        "Question->Finding", "Question->Answer");
    }

    /** SPEC §10.6: routes must name a registered space; unknown names fail fast listing the spaces. */
    @Test
    void routesToUnknownSpacesFailFast() throws Exception {
        SimPeer local = simPeer("routes");
        RemoteActions remote = new RemoteActions(local.discovery(),
                local.identity().peerId(), Map.of("work", local.work()));
        assertThatThrownBy(() -> remote.route(Task.class, "nope"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nope")
                .hasMessageContaining("work");
        assertThatThrownBy(() -> remote.resultsIn(Finding.class, "nope"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("work");
        assertThatThrownBy(() -> new RemoteActions(local.discovery(),
                local.identity().peerId(), Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one space");
    }

    // ------------------------------------------- routing from cards (spec §10.6)

    /** SPEC §10.6: with several spaces and no routes, the card's own space binding routes the input, end to end. */
    @Test
    @Timeout(90)
    void cardSpaceBindingsRouteInputsWithoutExplicitRoutes() throws Exception {
        FleetPeer worker = fleetPeer("w", null);
        FleetPeer caller = fleetPeer("c", "w");
        worker.binder().bind(new Researcher()); // @SpaceTake(space = "work") binds Task -> work

        // Two spaces, no routes: before v0.1.10 this yielded no action at all.
        RemoteActions remote = new RemoteActions(caller.discovery(),
                caller.identity().peerId(),
                Map.of("work", caller.work(), "other", caller.other()));
        List<RemoteAction> actions = awaitActions(remote, 1, Duration.ofSeconds(30));
        assertThat(actions).hasSize(1);
        RemoteAction research = actions.get(0);
        assertThat(research.card().spaceFor(Task.class.getName() + "#v1")).contains("work");
        assertThat(research.taskSpace()).isSameAs(caller.work());
        assertThat(research.resultSpace()).as("results default to the take space")
                .isSameAs(caller.work());

        Optional<Finding> finding = research.invoke(new Task("bound routing", 2),
                Finding.class, Duration.ofSeconds(30));
        assertThat(finding).isPresent();
        assertThat(finding.get().summary()).isEqualTo("researched: bound routing");
        assertThat(caller.other().readAll(Template.of(Task.class), 10)).isEmpty();
    }

    /** SPEC §10.6: an output type with no resultsIn route is awaited in the input's space instead of dropping the action. */
    @Test
    @Timeout(60)
    void resultsDefaultToTheTaskSpaceWhenOnlyTheInputIsRouted() throws Exception {
        SimPeer local = simPeer("default-results");
        LocalSpace other = LocalSpace.builder("other", local.identity().agent("host")).build();
        try {
            PeerIdentity foreign = PeerIdentity.generate();
            GroupId groupId = GroupId.of("zRemote");
            AgentCard unbound = new AgentCard("aspace://" + groupId.value() + "/agent/plain",
                    foreign.peerId(), groupId, clock.instant(), Duration.ofMinutes(15),
                    foreign.agent("plain"), "Card with no space bindings", List.of(),
                    List.of(Task.class.getName() + "#v1"), List.of(Finding.class.getName() + "#v1"),
                    Map.of());
            local.discovery().publish(new AdvertisementSigner().sign(unbound, foreign));

            RemoteActions remote = new RemoteActions(local.discovery(),
                    local.identity().peerId(), Map.of("work", local.work(), "other", other));
            assertThat(remote.available()).as("two spaces, no route, no binding").isEmpty();

            remote.route(Task.class, "work"); // the input only; no resultsIn
            List<RemoteAction> actions = remote.available();
            assertThat(actions).hasSize(1);
            assertThat(actions.get(0).taskSpace()).isSameAs(local.work());
            assertThat(actions.get(0).resultSpace()).isSameAs(local.work());

            // A worker completing into the take space is found there.
            local.binder().bind(new Researcher());
            Optional<Finding> finding = actions.get(0).invoke(new Task("defaulted", 1),
                    Finding.class, Duration.ofSeconds(20));
            assertThat(finding).isPresent();
            assertThat(finding.get().summary()).isEqualTo("researched: defaulted");
        } finally {
            other.close();
        }
    }

    private FleetPeer fleetPeer(String address, String seedAddress) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).randomSeed(fleetPeers.size() + 1).build();
        node.listen(fleetNetwork.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = seedAddress == null ? List.of()
                : List.of(new PeerAdvertisement.Endpoint("mem", seedAddress, 0));
        GroupId groupId = GroupId.fromFounding(
                "remote-actions-fleet-v1".getBytes(StandardCharsets.UTF_8));
        GroupAdvertisement groupAd = new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(365), "remote-actions-fleet",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
        GroupRuntime runtime = node.joinGroup(groupAd,
                new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2),
                seeds);
        CborCodec codec = CborCodec.defaultCodec();
        DiscoveryService discovery = new DiscoveryService(runtime,
                new AdCache(codec, InstantSource.system()), codec, identity.peerId());
        ReplicatedSpace work = ReplicatedSpace.builder(runtime, "work", identity, "host")
                .settleWindow(Duration.ofMillis(150)).build();
        ReplicatedSpace other = ReplicatedSpace.builder(runtime, "other", identity, "host")
                .settleWindow(Duration.ofMillis(150)).build();
        AgentBinder binder = new AgentBinder(identity, groupId, discovery, InstantSource.system());
        binder.space("work", work).space("other", other);
        node.startTicking(Duration.ofMillis(250));
        FleetPeer peer = new FleetPeer(node, identity, runtime, discovery, work, other, binder);
        fleetPeers.add(peer);
        return peer;
    }

    // ------------------------------------------------------- in-memory fixture

    private SimPeer simPeer(String address, String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock)
                .randomSeed(simPeers.size() + 1).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String seed : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", seed, 0));
        }
        GroupId groupId = GroupId.of("zRemote");
        GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zRemote",
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(1), "remote", GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        CborCodec codec = CborCodec.defaultCodec();
        DiscoveryService discovery = new DiscoveryService(runtime,
                new AdCache(codec, clock), codec, identity.peerId());
        LocalSpace work = LocalSpace.builder("work", identity.agent("host")).build();
        AgentBinder binder = new AgentBinder(identity, groupId, discovery, clock);
        binder.space("work", work);
        SimPeer peer = new SimPeer(node, identity, discovery, work, binder);
        simPeers.add(peer);
        return peer;
    }

    private void tick(int rounds) {
        for (int i = 0; i < rounds; i++) {
            simPeers.forEach(peer -> peer.node().tick());
        }
    }

    // ------------------------------------------------------------------ fixture

    private static GroupAdvertisement group() {
        GroupId groupId = GroupId.fromFounding(
                "remote-actions-test-v1".getBytes(StandardCharsets.UTF_8));
        return new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(365), "remote-actions-test",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
    }

    private Peer startPeer(int port, int seedPort) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).build();
        node.listen(new TcpTransport(), "127.0.0.1:" + port);
        List<PeerAdvertisement.Endpoint> seeds = seedPort == 0 ? List.of()
                : List.of(new PeerAdvertisement.Endpoint("tcp", "127.0.0.1:" + seedPort, 0));
        GroupRuntime runtime = node.joinGroup(group(),
                new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2),
                seeds);
        CborCodec codec = CborCodec.defaultCodec();
        DiscoveryService discovery = new DiscoveryService(runtime,
                new AdCache(codec, InstantSource.system()), codec, identity.peerId());
        ReplicatedSpace work = ReplicatedSpace.builder(runtime, "work", identity, "host")
                .settleWindow(Duration.ofMillis(150))
                .build();
        AgentBinder binder = new AgentBinder(identity, runtime.id(), discovery,
                InstantSource.system());
        binder.space("work", work);
        node.startTicking(Duration.ofMillis(250));
        Peer peer = new Peer(node, identity, runtime, discovery, work, binder);
        peers.add(peer);
        return peer;
    }

    private static List<RemoteAction> awaitActions(RemoteActions remote, int atLeast,
            Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        List<RemoteAction> actions = List.of();
        while (System.nanoTime() < deadline) {
            actions = remote.available();
            if (actions.size() >= atLeast) {
                return actions;
            }
            Thread.sleep(200);
        }
        return actions;
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
