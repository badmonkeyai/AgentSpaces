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
package ai.badmonkey.agentspaces.examples.a2a;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ai.badmonkey.agentspaces.a2a.A2aGateway;
import ai.badmonkey.agentspaces.a2a.A2aMessages.A2aTaskEntry;
import ai.badmonkey.agentspaces.a2a.A2aMessages.A2aTaskResult;
import ai.badmonkey.agentspaces.a2a.A2aTaskBinding;
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
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
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;

/**
 * Example 07: the fleet behind one A2A door. Worker agents join a replicated
 * space over TCP and publish AgentCards; the gateway peer serves them as A2A
 * agent cards and speaks the A2A task surface (JSON-RPC message/send,
 * tasks/get). A plain HTTP client, standing in for any A2A-speaking framework,
 * sends a message and polls the task to completion while the actual work rides
 * the space's take/complete semantics, crash tolerance included.
 *
 * <p>Run: {@code mvn -q -pl examples/example-07-a2a-fleet exec:java}
 */
public final class A2aFleet {

    private static final String HOST = "127.0.0.1";

    private A2aFleet() {
    }

    /** One assembled peer. */
    public record Peer(String name, PeerNode node, GroupRuntime runtime,
                       DiscoveryService discovery, ReplicatedSpace tasks) {

        /** Shuts the peer down. */
        public void close() {
            tasks.close();
            node.close();
        }
    }

    /** The example's well-known group. */
    public static GroupAdvertisement group() {
        GroupId groupId = GroupId.fromFounding(
                "a2a-fleet-demo-v1".getBytes(StandardCharsets.UTF_8));
        return new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(365), "a2a-fleet",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
    }

    /**
     * Assembles one peer on the shared task space.
     *
     * @param name     the agent name
     * @param port     the TCP port
     * @param seedPort an existing member's port, or 0 for the first
     * @return the running peer
     * @throws Exception if the port cannot be bound
     */
    public static Peer startPeer(String name, int port, int seedPort) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).build();
        node.listen(new TcpTransport(), HOST + ":" + port);
        List<PeerAdvertisement.Endpoint> seeds = seedPort == 0 ? List.of()
                : List.of(new PeerAdvertisement.Endpoint("tcp", HOST + ":" + seedPort, 0));
        GroupRuntime runtime = node.joinGroup(group(),
                new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2),
                seeds);
        CborCodec codec = CborCodec.defaultCodec();
        DiscoveryService discovery = new DiscoveryService(runtime,
                new AdCache(codec, InstantSource.system()), codec, identity.peerId());
        ReplicatedSpace tasks = ReplicatedSpace.builder(runtime, "a2a-tasks", identity, name)
                .settleWindow(Duration.ofMillis(150))
                .build();
        node.startTicking(Duration.ofMillis(250));
        return new Peer(name, node, runtime, discovery, tasks);
    }

    /**
     * Publishes a worker's AgentCard and starts its take-complete loop.
     *
     * @param worker the worker peer
     * @return the worker thread
     */
    public static Thread runWorker(Peer worker) {
        AdvertisementSigner signer = new AdvertisementSigner();
        AgentCard card = new AgentCard(
                "aspace://" + worker.runtime().id().value() + "/agent/" + worker.name(),
                worker.node().peerId(), worker.runtime().id(), Instant.now(),
                Duration.ofMinutes(15), worker.node().identity().agent(worker.name()),
                "Answers fleet questions posed through the A2A gateway",
                List.of("answer"),
                List.of(A2aTaskEntry.class.getName() + "#v1"),
                List.of(A2aTaskResult.class.getName() + "#v1"), Map.of());
        worker.discovery().publish(signer.sign(card, worker.node().identity()));
        return Thread.ofVirtual().start(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                var taken = worker.tasks().take(
                        Template.of(A2aTaskEntry.class).where("agent", eq(worker.name())),
                        Lease.of(Duration.ofMinutes(1)), Duration.ofSeconds(2));
                taken.ifPresent(t -> worker.tasks().complete(t,
                        new A2aTaskResult(t.entry().taskId(), worker.name(),
                                worker.name() + " says: worked '" + t.entry().text() + "'"),
                        Lease.of(Duration.ofHours(1))));
            }
        });
    }

    /**
     * Runs the demo.
     *
     * @param args unused
     * @throws Exception on startup failure
     */
    public static void main(String[] args) throws Exception {
        System.out.println("a2a-fleet: one A2A door, a whole fleet behind it\n");
        Peer gatewayPeer = startPeer("gateway", 7501, 0);
        Peer analyst = startPeer("analyst", 7502, 7501);
        List<Peer> fleet = List.of(gatewayPeer, analyst);
        Thread workerThread = null;
        try (A2aGateway gateway = new A2aGateway("a2a-fleet", "AgentSpaces demo",
                () -> gatewayPeer.discovery().find(AgentCard.class, c -> true))
                .taskBinding(new A2aTaskBinding(gatewayPeer.tasks()))) {
            workerThread = runWorker(analyst);
            Thread.sleep(1500); // membership and the analyst's card settle
            int port = gateway.start(0);

            ObjectMapper json = new ObjectMapper();
            HttpClient http = HttpClient.newHttpClient();
            String base = "http://localhost:" + port;

            JsonNode fleetCard = json.readTree(http.send(HttpRequest.newBuilder(
                            URI.create(base + "/.well-known/agent-card.json")).build(),
                    HttpResponse.BodyHandlers.ofString()).body());
            System.out.println("fleet card '" + fleetCard.path("name").asText()
                    + "' advertises " + fleetCard.path("skills").size() + " skill(s)");

            JsonNode sent = json.readTree(http.send(HttpRequest.newBuilder(
                            URI.create(base + "/agents/analyst"))
                    .POST(HttpRequest.BodyPublishers.ofString("""
                            {"jsonrpc":"2.0","id":1,"method":"message/send","params":
                            {"message":{"role":"user","parts":[{"kind":"text",
                            "text":"reconcile the ledgers"}],"messageId":"m1"}}}
                            """))
                    .build(), HttpResponse.BodyHandlers.ofString()).body());
            String taskId = sent.path("result").path("id").asText();
            System.out.println("message/send -> task " + taskId + " ["
                    + sent.path("result").path("status").path("state").asText() + "]");

            JsonNode task = sent;
            for (int i = 0; i < 100; i++) {
                task = json.readTree(http.send(HttpRequest.newBuilder(
                                URI.create(base + "/agents/analyst"))
                        .POST(HttpRequest.BodyPublishers.ofString("""
                                {"jsonrpc":"2.0","id":2,"method":"tasks/get",
                                "params":{"id":"%s"}}""".formatted(taskId)))
                        .build(), HttpResponse.BodyHandlers.ofString()).body());
                if ("completed".equals(task.path("result").path("status")
                        .path("state").asText())) {
                    break;
                }
                Thread.sleep(200);
            }
            System.out.println("tasks/get   -> ["
                    + task.path("result").path("status").path("state").asText() + "] "
                    + task.path("result").path("artifacts").path(0)
                    .path("parts").path(0).path("text").asText());
            System.out.println("\nthe A2A client never saw the space; "
                    + "the fleet never saw HTTP.");
        } finally {
            if (workerThread != null) {
                workerThread.interrupt();
            }
            fleet.forEach(Peer::close);
        }
    }
}
