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
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
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
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import com.embabel.agent.api.annotation.Action;
import com.embabel.agent.api.annotation.Agent;
import com.embabel.agent.core.AgentPlatform;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Remote AgentCards as Embabel planner actions: over a real two-peer TCP
 * fleet, the generated {@code @Agent} class carries one typed {@code @Action}
 * method per discovered card, its invocation is the real space round-trip, and
 * deployment reaches a platform's {@code deploy}. (The stub annotations and
 * platform under {@code src/test/java/com/embabel} mirror Embabel 1.5's API by
 * fully qualified name; integration-tests/embabel-it runs against the real
 * artifact.)
 */
class EmbabelRemoteActionsTest {

    /** A request the remote fleet serves. */
    public record Task(String topic, int priority) {
    }

    /** The correlated result. */
    public record Finding(String topic, String summary) {
    }

    /** The remote worker whose card becomes the planner action. */
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

    @AfterEach
    void tearDown() {
        for (Peer peer : peers) {
            peer.binder().close();
            peer.work().close();
            peer.node().close();
        }
    }

    @Test
    @Timeout(120)
    void discoveredCardsBecomeTypedPlannerActionsBackedByTheFleet() throws Exception {
        int seedPort = freePort();
        Peer worker = startPeer(seedPort, 0);
        Peer caller = startPeer(freePort(), seedPort);
        worker.binder().bind(new Researcher());

        RemoteActions remote = new RemoteActions(caller.discovery(),
                caller.identity().peerId(), Map.of("work", caller.work()));
        awaitAvailable(remote, Duration.ofSeconds(30));

        EmbabelRemoteActions bridge = new EmbabelRemoteActions(remote,
                Duration.ofSeconds(30), "remoteFleet", "The fleet's advertised skills");
        Object agent = bridge.agentInstance().orElseThrow();

        // The generated class is an @Agent whose one @Action method carries the
        // exact remote signature, so GOAP condition matching sees Task -> Finding.
        Agent agentAnnotation = agent.getClass().getAnnotation(Agent.class);
        assertThat(agentAnnotation).isNotNull();
        assertThat(agentAnnotation.name()).isEqualTo("remoteFleet");
        assertThat(agentAnnotation.description()).isEqualTo("The fleet's advertised skills");
        Method action = agent.getClass().getMethod("researcher_Task", Task.class);
        assertThat(action.getAnnotation(Action.class)).isNotNull();
        assertThat(action.getReturnType()).isEqualTo(Finding.class);

        // Invoking the generated action runs the real round-trip: the remote
        // worker takes the task and the correlated finding returns as the value.
        Object result = action.invoke(agent, new Task("agentic memory", 3));
        assertThat(result).isInstanceOf(Finding.class);
        assertThat(((Finding) result).summary()).isEqualTo("researched: agentic memory");

        // Deployment reaches a platform's deploy(Object) reflectively.
        List<Object> deployed = new ArrayList<>();
        AgentPlatform platform = deployed::add;
        assertThat(bridge.deployTo(platform)).isTrue();
        assertThat(deployed).hasSize(1);
        assertThat(deployed.get(0).getClass().getAnnotation(Agent.class)).isNotNull();
    }

    @Test
    void withoutAdvertisedActionsNoAgentIsGeneratedAndDeployReportsFalse() throws Exception {
        int seedPort = freePort();
        Peer lonely = startPeer(seedPort, 0);
        RemoteActions remote = new RemoteActions(lonely.discovery(),
                lonely.identity().peerId(), Map.of("work", lonely.work()));
        EmbabelRemoteActions bridge = new EmbabelRemoteActions(remote,
                Duration.ofSeconds(5), "remoteFleet", "empty");
        assertThat(bridge.agentInstance()).isEmpty();
        assertThat(bridge.deployTo((AgentPlatform) agent -> {
        })).isFalse();
    }


    /** SPEC §10.6: a result that never arrives within the timeout throws, which the planner treats as failure. */
    @Test
    @Timeout(60)
    void aResultThatNeverArrivesThrowsFromTheGeneratedAction() throws Exception {
        Peer lonely = startPeer(freePort(), 0);
        PeerIdentity foreign = PeerIdentity.generate();
        publishForeignCard(lonely, foreign, "researcher");
        RemoteActions remote = new RemoteActions(lonely.discovery(),
                lonely.identity().peerId(), Map.of("work", lonely.work()));
        awaitAvailable(remote, Duration.ofSeconds(10));
        EmbabelRemoteActions bridge = new EmbabelRemoteActions(remote,
                Duration.ofMillis(500), "remoteFleet", "nobody serves this");
        Object agent = bridge.agentInstance().orElseThrow();
        Method action = agent.getClass().getMethod("researcher_Task", Task.class);

        long started = System.nanoTime();
        assertThatThrownBy(() -> action.invoke(agent, new Task("nobody home", 1)))
                .isInstanceOf(InvocationTargetException.class)
                .cause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("produced no Finding")
                .hasMessageContaining("researcher_Task")
                .hasMessageContaining("PT0.5S");
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(10));
        // The request entry stays leased in the space: a work item, not a lost call.
        assertThat(lonely.work().readAll(ai.badmonkey.agentspaces.api.space.Template.of(Task.class), 10))
                .extracting(Task::topic).contains("nobody home");
    }

    /** SPEC §10.6: two remote agents with the same name yield distinct, collision-free @Action methods. */
    @Test
    @Timeout(60)
    void generatedMethodNamesDoNotCollide() throws Exception {
        Peer lonely = startPeer(freePort(), 0);
        publishForeignCard(lonely, PeerIdentity.generate(), "researcher");
        publishForeignCard(lonely, PeerIdentity.generate(), "researcher");
        RemoteActions remote = new RemoteActions(lonely.discovery(),
                lonely.identity().peerId(), Map.of("work", lonely.work()));
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline && remote.available().size() < 2) {
            Thread.sleep(100);
        }
        assertThat(remote.available()).hasSize(2);

        Object agent = new EmbabelRemoteActions(remote, Duration.ofSeconds(1), "remoteFleet", "d")
                .agentInstance().orElseThrow();
        Method first = agent.getClass().getMethod("researcher_Task", Task.class);
        Method second = agent.getClass().getMethod("researcher_Task_2", Task.class);
        assertThat(first.getAnnotation(Action.class)).isNotNull();
        assertThat(second.getAnnotation(Action.class)).isNotNull();
        assertThat(first.getReturnType()).isEqualTo(Finding.class);
        assertThat(second.getReturnType()).isEqualTo(Finding.class);
    }

    /** SPEC §10.6: the bridge deploys itself onto the platform when remote actions appear, and again when the action set changes. */
    @Test
    @Timeout(60)
    void theBridgeDeploysItselfWhenActionsAppear() throws Exception {
        Peer lonely = startPeer(freePort(), 0);
        RemoteActions remote = new RemoteActions(lonely.discovery(),
                lonely.identity().peerId(), Map.of("work", lonely.work()));
        EmbabelRemoteActions bridge = new EmbabelRemoteActions(remote,
                Duration.ofSeconds(1), "remoteFleet", "auto-deployed");
        List<Object> deployed = new java.util.concurrent.CopyOnWriteArrayList<>();
        AgentPlatform platform = deployed::add;

        try (EmbabelRemoteActionsDeployer deployer =
                     new EmbabelRemoteActionsDeployer(bridge, Duration.ofMillis(100))) {
            deployer.deployWhenReady(platform);
            deployer.start();
            assertThat(deployer.isRunning()).isTrue();
            Thread.sleep(300);
            assertThat(deployed).as("nothing advertised, nothing deployed").isEmpty();

            // A card arrives: one generation with its one action is deployed.
            publishForeignCard(lonely, PeerIdentity.generate(), "researcher");
            awaitDeployments(deployer, 1, Duration.ofSeconds(10));
            assertThat(deployed).hasSize(1);
            assertThat(deployed.get(0).getClass().getAnnotation(Agent.class)).isNotNull();
            assertThat(deployed.get(0).getClass().getMethod("researcher_Task", Task.class)
                    .getAnnotation(Action.class)).isNotNull();

            // A second capability arrives: a new generation carries both actions.
            publishForeignCard(lonely, PeerIdentity.generate(), "summarizer");
            awaitDeployments(deployer, 2, Duration.ofSeconds(10));
            assertThat(deployed).hasSize(2);
            assertThat(deployed.get(1).getClass().getMethod("summarizer_Task", Task.class))
                    .isNotNull();

            // The same two cards, refreshed: no change, so no redeploy.
            Thread.sleep(400);
            assertThat(deployer.deployments()).isEqualTo(2);
        }
    }

    /** SPEC §10.6: a platform handed to a running deployer is deployed to at once, and stop is idempotent. */
    @Test
    @Timeout(60)
    void aPlatformHandedToARunningDeployerIsDeployedToImmediately() throws Exception {
        Peer lonely = startPeer(freePort(), 0);
        publishForeignCard(lonely, PeerIdentity.generate(), "researcher");
        RemoteActions remote = new RemoteActions(lonely.discovery(),
                lonely.identity().peerId(), Map.of("work", lonely.work()));
        awaitAvailable(remote, Duration.ofSeconds(10));
        EmbabelRemoteActions bridge = new EmbabelRemoteActions(remote,
                Duration.ofSeconds(1), "remoteFleet", "late platform");
        List<Object> deployed = new java.util.concurrent.CopyOnWriteArrayList<>();
        EmbabelRemoteActionsDeployer deployer =
                new EmbabelRemoteActionsDeployer(bridge, Duration.ofSeconds(30));
        deployer.start();
        Thread.sleep(200);
        assertThat(deployed).as("no platform yet").isEmpty();

        deployer.deployWhenReady((AgentPlatform) deployed::add);
        assertThat(deployed).as("deployed synchronously on hand-over").hasSize(1);
        assertThat(deployer.checkNow()).as("unchanged set does not redeploy").isFalse();

        deployer.stop();
        deployer.stop();
        assertThat(deployer.isRunning()).isFalse();
        assertThatThrownBy(() -> new EmbabelRemoteActionsDeployer(bridge, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static void awaitDeployments(EmbabelRemoteActionsDeployer deployer, int atLeast,
                                         Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline && deployer.deployments() < atLeast) {
            Thread.sleep(50);
        }
        assertThat(deployer.deployments()).isGreaterThanOrEqualTo(atLeast);
    }

    /** Publishes a Task-to-Finding card signed by a foreign identity into the peer's discovery. */
    private static void publishForeignCard(Peer peer, PeerIdentity foreign, String agentName) {
        GroupId groupId = peer.runtime().id();
        AgentCard card = new AgentCard(
                "aspace://" + groupId.value() + "/agent/" + foreign.peerId().value() + "/" + agentName,
                foreign.peerId(), groupId, Instant.now(), Duration.ofMinutes(15),
                foreign.agent(agentName), "Researches topics", List.of("answer research questions"),
                List.of(Task.class.getName() + "#v1"), List.of(Finding.class.getName() + "#v1"),
                Map.of());
        peer.discovery().publish(new AdvertisementSigner().sign(card, foreign));
    }

    // ------------------------------------------------------------------ fixture

    private static GroupAdvertisement group() {
        GroupId groupId = GroupId.fromFounding(
                "embabel-remote-test-v1".getBytes(StandardCharsets.UTF_8));
        return new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(365), "embabel-remote-test",
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

    private static void awaitAvailable(RemoteActions remote, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline && remote.available().isEmpty()) {
            Thread.sleep(200);
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
