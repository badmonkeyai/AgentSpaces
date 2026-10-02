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
package ai.badmonkey.agentspaces.embabel;

import ai.badmonkey.agentspaces.agent.AgentBinder;
import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.agent.remote.RemoteActions;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import com.embabel.agent.api.annotation.AchievesGoal;
import com.embabel.agent.api.annotation.Action;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The point of the planner bridge, proven end to end: a single agent whose only
 * local skill is drafting a research task CANNOT plan its way from a brief to a
 * reviewed report — no chain of its own actions produces one. The moment the
 * fleet's AgentCards are bridged in, the same type-chaining planning that
 * Embabel's GOAP performs over action signatures finds the three-step plan
 * (draft locally, research on one remote peer, review on another), and
 * executing that plan runs each remote step as a real leased space round-trip
 * across a three-peer TCP fleet.
 *
 * <p>The in-test planner is deliberately minimal — breadth-first chaining over
 * {@code @Action} method signatures — because that is exactly the information
 * Embabel's planner conditions on. It stands in for GOAP here; the real GOAP
 * planner runs over the same fleet agent in integration-tests/embabel-it.
 */
class FleetPlannerEnablementTest {

    /** What the lone agent starts from. */
    public record Brief(String topic, String requestedBy) {
    }

    /** A drafted research task (the lone agent's only local skill produces it). */
    public record Task(String topic, int priority) {
    }

    /** A finding: only the remote researcher can produce one. */
    public record Finding(String topic, String summary) {
    }

    /** The goal: a reviewed report; only the remote reviewer can produce one. */
    public record Report(String topic, String body, String reviewedBy) {
    }

    /** The remote researcher fleet member. */
    @AgentSpec(name = "researcher", description = "Researches topics",
            goals = {"answer research questions"})
    public static class Researcher {

        /**
         * Works one task.
         *
         * @param task the task
         * @return the finding
         */
        @SpaceTake(space = "tasks", pollTimeout = "PT0.2S")
        public Finding research(Task task) {
            return new Finding(task.topic(), "researched: " + task.topic());
        }
    }

    /** The remote reviewer fleet member. */
    @AgentSpec(name = "reviewer", description = "Reviews findings into reports",
            goals = {"write reviewed reports"})
    public static class Reviewer {

        /**
         * Reviews one finding.
         *
         * @param finding the finding
         * @return the reviewed report
         */
        @SpaceTake(space = "reviews", pollTimeout = "PT0.2S")
        public Report review(Finding finding) {
            return new Report(finding.topic(),
                    "report on " + finding.topic() + " [" + finding.summary() + "]",
                    "reviewer");
        }
    }

    /** One planning step: an action signature and how to run it. */
    private record Step(String name, Class<?> in, Class<?> out, Function<Object, Object> run) {
    }

    private record Peer(PeerNode node, PeerIdentity identity, GroupRuntime runtime,
                        DiscoveryService discovery, ReplicatedSpace tasks,
                        ReplicatedSpace reviews, AgentBinder binder) {
    }

    private final List<Peer> peers = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (Peer peer : peers) {
            peer.binder().close();
            peer.reviews().close();
            peer.tasks().close();
            peer.node().close();
        }
    }

    @Test
    @Timeout(120)
    void theFleetLetsALonePlannerReachAGoalItProvablyCannotReachAlone() throws Exception {
        int seedPort = freePort();
        Peer researcher = startPeer(seedPort, 0);
        Peer reviewer = startPeer(freePort(), seedPort);
        Peer planner = startPeer(freePort(), seedPort);
        researcher.binder().bind(new Researcher());
        reviewer.binder().bind(new Reviewer());

        // The lone agent's whole local repertoire: draft a task from a brief.
        List<Step> localActions = List.of(new Step("draft", Brief.class, Task.class,
                input -> new Task(((Brief) input).topic(), 3)));

        // Provably out of reach alone: no chain of local actions ends in a Report.
        assertThat(plan(Brief.class, Report.class, localActions)).isEmpty();

        // Bridge the fleet in: both cards gossip over, and the generated agent
        // carries one typed @Action per remote capability.
        RemoteActions remote = new RemoteActions(planner.discovery(),
                planner.identity().peerId(),
                Map.of("tasks", planner.tasks(), "reviews", planner.reviews()))
                .route(Task.class, "tasks").resultsIn(Finding.class, "tasks")
                .route(Finding.class, "reviews").resultsIn(Report.class, "reviews");
        awaitAvailable(remote, 2, Duration.ofSeconds(30));
        EmbabelRemoteActions bridge = new EmbabelRemoteActions(remote,
                Duration.ofSeconds(30), "remoteFleet", "The fleet's advertised skills");
        Object fleetAgent = bridge.agentInstance().orElseThrow();

        // The reviewer's goal annotation reached the generated method, so the
        // planner's goal condition ("write reviewed reports") is visible too.
        Method review = fleetAgent.getClass().getMethod("reviewer_Finding", Finding.class);
        assertThat(review.getAnnotation(AchievesGoal.class).description())
                .isEqualTo("write reviewed reports");

        // With the fleet's actions in the set, the plan exists: exactly the
        // three-step chain through two different remote peers.
        List<Step> withFleet = new ArrayList<>(localActions);
        withFleet.addAll(fleetSteps(fleetAgent));
        List<Step> found = plan(Brief.class, Report.class, withFleet).orElseThrow();
        assertThat(found).extracting(Step::name)
                .containsExactly("draft", "researcher_Task", "reviewer_Finding");

        // Executing the plan runs the two remote steps as real space
        // round-trips; the result threads the whole chain.
        Object current = new Brief("agentic memory", "al");
        for (Step step : found) {
            current = step.run().apply(current);
        }
        Report report = (Report) current;
        assertThat(report.topic()).isEqualTo("agentic memory");
        assertThat(report.body())
                .isEqualTo("report on agentic memory [researched: agentic memory]");
        assertThat(report.reviewedBy()).isEqualTo("reviewer");
    }

    // ------------------------------------------------------------- the planner

    /**
     * Breadth-first forward chaining over action signatures — the same
     * type-level information Embabel's GOAP planner conditions on.
     */
    private static Optional<List<Step>> plan(Class<?> start, Class<?> goal,
                                             List<Step> actions) {
        record Node(Class<?> type, List<Step> path) {
        }
        Deque<Node> frontier = new ArrayDeque<>();
        frontier.add(new Node(start, List.of()));
        Set<Class<?>> visited = new HashSet<>();
        visited.add(start);
        while (!frontier.isEmpty()) {
            Node node = frontier.poll();
            if (goal.isAssignableFrom(node.type())) {
                return Optional.of(node.path());
            }
            for (Step step : actions) {
                if (step.in().isAssignableFrom(node.type()) && visited.add(step.out())) {
                    List<Step> path = new ArrayList<>(node.path());
                    path.add(step);
                    frontier.add(new Node(step.out(), path));
                }
            }
        }
        return Optional.empty();
    }

    /** Reads the generated agent's {@code @Action} methods as planning steps. */
    private static List<Step> fleetSteps(Object fleetAgent) {
        List<Step> steps = new ArrayList<>();
        for (Method method : fleetAgent.getClass().getDeclaredMethods()) {
            if (method.getAnnotation(Action.class) == null) {
                continue;
            }
            steps.add(new Step(method.getName(), method.getParameterTypes()[0],
                    method.getReturnType(), input -> {
                try {
                    return method.invoke(fleetAgent, input);
                } catch (ReflectiveOperationException e) {
                    throw new IllegalStateException(method.getName() + " failed", e);
                }
            }));
        }
        return steps;
    }

    // ------------------------------------------------------------------ fixture

    private static GroupAdvertisement group() {
        GroupId groupId = GroupId.fromFounding(
                "planner-enablement-test-v1".getBytes(StandardCharsets.UTF_8));
        return new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(365), "planner-enablement-test",
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
        ReplicatedSpace tasks = ReplicatedSpace.builder(runtime, "tasks", identity, "host")
                .settleWindow(Duration.ofMillis(150))
                .build();
        ReplicatedSpace reviews = ReplicatedSpace.builder(runtime, "reviews", identity, "host")
                .settleWindow(Duration.ofMillis(150))
                .build();
        AgentBinder binder = new AgentBinder(identity, runtime.id(), discovery,
                InstantSource.system());
        binder.space("tasks", tasks).space("reviews", reviews);
        node.startTicking(Duration.ofMillis(250));
        Peer peer = new Peer(node, identity, runtime, discovery, tasks, reviews, binder);
        peers.add(peer);
        return peer;
    }

    private static void awaitAvailable(RemoteActions remote, int atLeast, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline && remote.available().size() < atLeast) {
            Thread.sleep(200);
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
