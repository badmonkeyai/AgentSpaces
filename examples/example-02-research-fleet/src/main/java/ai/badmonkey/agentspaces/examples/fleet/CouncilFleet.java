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
package ai.badmonkey.agentspaces.examples.fleet;

import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.agent.remote.RemoteAction;
import ai.badmonkey.agentspaces.agent.remote.RemoteActions;
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.aggregate.PushSumAggregate;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.capabilities.semantic.HashingEmbedder;
import ai.badmonkey.agentspaces.capabilities.semantic.SemanticDiscovery;
import ai.badmonkey.agentspaces.capabilities.vote.VoteCapability;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.examples.fleet.ResearchFleet.Finding;
import ai.badmonkey.agentspaces.examples.fleet.ResearchFleet.ResearchTask;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;

import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * The council act of the polyglot demo (L3L4-COVERAGE.md §6.6): a Java
 * coordinator in the research-fleet group with the layer-4 capabilities, and
 * workers in other languages (Python, TypeScript) and runtimes that take its
 * tasks, sit on its council vote, contribute to its push-sum epoch, answer
 * its semantic queries, and serve the remote actions their cards declare.
 *
 * <pre>
 *   mvn -q -pl examples/example-02-research-fleet exec:java \
 *       -Dexec.mainClass=ai.badmonkey.agentspaces.examples.fleet.CouncilFleet \
 *       -Dexec.args="7470 2 20"      # port, expected workers, this peer's load
 * </pre>
 *
 * <p>The group, the task space, and the record types are example 02's, so the
 * same Python and TypeScript workers that drain the research fleet join this
 * council unchanged, and the wire is the one the golden vectors pin.
 */
public final class CouncilFleet {

    /** The proposal the council decides. */
    public static final String PROPOSAL = "council:ship";
    /** The push-sum epoch every member contributes to. */
    public static final String EPOCH = "load";

    private static final String HOST = "127.0.0.1";

    private CouncilFleet() {
    }

    /** The coordinator's assembled stack. */
    public record Council(PeerIdentity identity, PeerNode node, GroupRuntime runtime,
                          ReplicatedSpace tasks, ReplicatedSpace votes, DiscoveryService discovery,
                          PushSumAggregate aggregate, VoteCapability vote, SemanticDiscovery semantic,
                          AgentSpaces spaces, AgentSpaces.GroupContext group) implements AutoCloseable {

        /** The actions the other members' cards declare, routed through the task space. */
        public RemoteActions remoteActions() {
            return new RemoteActions(discovery, identity.peerId(), Map.of("tasks", tasks));
        }

        @Override
        public void close() {
            try {
                spaces.close();
            } catch (Exception ignored) {
                // closing
            }
            tasks.close();
            votes.close();
            node.close();
        }
    }

    /** What one council act produced. */
    public record Outcome(List<Finding> findings, Optional<VoteCapability.Decision> decision,
                          OptionalDouble estimate, List<AgentCard> cards, Optional<Finding> remote) {
    }

    /** The coordinator on TCP: the research group, the task and vote spaces, discovery, and the capabilities. */
    public static Council start(int port) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).build();
        node.listen(new TcpTransport(), HOST + ":" + port);
        GroupRuntime runtime = node.joinGroup(ResearchFleet.fleetGroup(),
                new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2),
                List.<PeerAdvertisement.Endpoint>of());
        CborCodec codec = CborCodec.defaultCodec();
        InstantSource clock = InstantSource.system();
        ReplicatedSpace tasks = ReplicatedSpace.builder(runtime, "tasks", identity, "coordinator")
                .settleWindow(Duration.ofMillis(150)).build();
        ReplicatedSpace votes = ReplicatedSpace.builder(runtime, "votes", identity, "coordinator")
                .settleWindow(Duration.ofMillis(150)).build();
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, clock), codec,
                identity.peerId());
        CapabilityPipes pipes = new CapabilityPipes(runtime, codec);
        PushSumAggregate aggregate = new PushSumAggregate(pipes, runtime.sampler(), identity.peerId(),
                codec, clock);
        VoteCapability vote = new VoteCapability(votes, identity.agent("coordinator"), identity.peerId(), clock);
        SemanticDiscovery semantic = new SemanticDiscovery(pipes, runtime, discovery, identity.peerId(),
                codec, clock, new HashingEmbedder());
        CapabilityRuntime capabilities = new CapabilityRuntime(runtime, discovery, identity);
        AgentSpaces spaces = new AgentSpaces(identity, clock);
        AgentSpaces.GroupContext group = spaces.register("council", runtime.id(), runtime, discovery)
                .capabilities(capabilities);
        group.space("tasks", tasks);
        group.space("votes", votes);
        group.provide(aggregate);
        group.provide(vote);
        group.provide(semantic);
        node.startTicking(Duration.ofMillis(250));
        return new Council(identity, node, runtime, tasks, votes, discovery, aggregate, vote, semantic,
                spaces, group);
    }

    /**
     * One act: tasks for the workers, the council vote at a quorum of every
     * member, the push-sum epoch, a semantic query over the pipe, and one
     * remote action invoked on a worker's declared action.
     *
     * @param council    the coordinator
     * @param workers    how many workers are expected (the quorum is workers plus this peer)
     * @param localValue this peer's contribution to the epoch
     * @param timeout    how long each wait may take
     */
    public static Outcome act(Council council, int workers, double localValue, Duration timeout)
            throws InterruptedException {
        int taskCount = 2 * workers;
        for (int i = 1; i <= taskCount; i++) {
            council.tasks().write(new ResearchTask("council-topic-" + i, i), Lease.of(Duration.ofMinutes(30)));
        }
        council.vote().propose(PROPOSAL, "ship 1.0?", List.of("yes", "no"), workers + 1,
                Lease.of(Duration.ofHours(1)));
        council.vote().castBallot(PROPOSAL, "yes", Lease.of(Duration.ofHours(1)));
        council.aggregate().start(EPOCH, localValue);
        System.out.println("coordinator: " + taskCount + " tasks, proposal " + PROPOSAL
                + " (quorum " + (workers + 1) + "), epoch " + EPOCH + " = " + localValue);

        await(timeout, () -> findings(council).size() >= taskCount);
        List<Finding> findings = findings(council);
        findings.forEach(f -> System.out.println("finding: " + f.topic() + " by " + f.worker()));

        await(timeout, () -> council.vote().decision(PROPOSAL).isPresent());
        Optional<VoteCapability.Decision> decision = council.vote().decision(PROPOSAL);
        decision.ifPresent(d -> System.out.println("decision: " + d.winner() + " " + d.tally()));

        CountDownLatch settled = new CountDownLatch(1);
        double[] box = new double[1];
        // Over real sockets some mass is always in flight, so the rule is stricter
        // than the in-memory cluster tests' (a plateau of a few ticks is not settled).
        try (AutoCloseable watch = council.aggregate().onEstimate(EPOCH::equals,
                PushSumAggregate.Settle.after(8, 0.002), e -> {
                    box[0] = e.value();
                    settled.countDown();
                })) {
            settled.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        OptionalDouble estimate = settled.getCount() == 0 ? OptionalDouble.of(box[0])
                : council.aggregate().estimate(EPOCH);
        estimate.ifPresent(v -> System.out.println("estimate: " + EPOCH + " settled at " + v));

        List<AgentCard> cards = new ArrayList<>();
        for (SemanticDiscovery.Match match : council.semantic().remoteQuery("research topics", 8,
                Duration.ofSeconds(2))) {
            if (match.advertisement() instanceof AgentCard card && !card.issuer().equals(council.identity().peerId())) {
                cards.add(card);
                System.out.println("semantic: " + card.agent().localName() + " scored " + match.score());
            }
        }

        Optional<Finding> remote = Optional.empty();
        List<RemoteAction> actions = council.remoteActions().consuming(ResearchTask.class);
        if (!actions.isEmpty()) {
            RemoteAction action = actions.get(0);
            remote = action.invoke(new ResearchTask("council-remote", 9), Finding.class, timeout);
            remote.ifPresent(f -> System.out.println("remote action " + action.name() + " answered: "
                    + f.summary() + " by " + f.worker()));
        }
        return new Outcome(findings, decision, estimate, cards, remote);
    }

    private static List<Finding> findings(Council council) {
        return council.tasks().readAll(Template.of(Finding.class), 100);
    }

    private static void await(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline && !condition.getAsBoolean()) {
            Thread.sleep(200);
        }
    }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 7470;
        int workers = args.length > 1 ? Integer.parseInt(args[1]) : 2;
        double value = args.length > 2 ? Double.parseDouble(args[2]) : 20.0;
        try (Council council = start(port)) {
            System.out.println("council coordinator listening on " + port + " as " + council.identity().peerId().value());
            Outcome outcome = act(council, workers, value, Duration.ofSeconds(90));
            boolean ok = outcome.findings().size() >= 2 * workers && outcome.decision().isPresent()
                    && outcome.estimate().isPresent();
            System.out.println(ok ? "council act complete" : "council act INCOMPLETE");
            System.exit(ok ? 0 : 1);
        }
    }
}
