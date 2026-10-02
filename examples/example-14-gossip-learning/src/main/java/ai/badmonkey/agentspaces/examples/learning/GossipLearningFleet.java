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
package ai.badmonkey.agentspaces.examples.learning;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.capabilities.learn.GossipLearning;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Example 14: gossip learning. Three peers each hold a local model (a weight
 * vector fitted to their own data) and average it with random partners over
 * the fabric, round after round, until every peer holds the fleet's mean — no
 * parameter server, no coordinator, no one who sees all the data
 * ({@code aspace:cap/gossip-learn}, SPEC §8). The learner is registered on the
 * peer's {@link CapabilityRuntime}, so the peer tick drives the exchange and
 * the capability is advertised; nothing here ticks anything by hand.
 *
 * <p>This is the first consumer of the learning capability outside the
 * starter, written so that the shape of a learning agent is visible before an
 * annotation for it is designed (LAYER4-ANNOTATIONS.md §2.6): a peer starts a
 * model, and reads the merged model back as rounds complete.
 *
 * <p>Run: {@code mvn -q -pl examples/example-14-gossip-learning exec:java}
 */
public final class GossipLearningFleet {

    /** The model every peer trains and averages: a small weight vector. */
    public static final String MODEL = "demand-forecast";

    private static final String HOST = "127.0.0.1";

    /** One assembled learner. */
    public record Peer(String name, PeerNode node, GroupRuntime runtime,
                       CapabilityRuntime capabilities, GossipLearning learning) {

        /** Shuts the peer down. */
        public void close() {
            capabilities.close();
            node.close();
        }

        /** The merged model this peer currently holds, once at least one round has run. */
        public Optional<double[]> model() {
            return learning.model(MODEL);
        }

        /** How many exchange rounds this peer has completed. */
        public long rounds() {
            return learning.round(MODEL);
        }
    }

    private GossipLearningFleet() {
    }

    /** The example's well-known group. */
    public static GroupAdvertisement group() {
        GroupId groupId = GroupId.fromFounding("gossip-learning-v1".getBytes(StandardCharsets.UTF_8));
        return new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(365),
                "gossip-learning", GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
    }

    /**
     * Assembles a learner: joins the group and registers a weight-averaging
     * learner on the peer's capability runtime.
     *
     * @param name     the peer's name
     * @param port     the TCP port to listen on
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
                new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2), seeds);
        CborCodec codec = CborCodec.defaultCodec();
        DiscoveryService discovery = new DiscoveryService(runtime,
                new AdCache(codec, InstantSource.system()), codec, identity.peerId());
        CapabilityRuntime capabilities = new CapabilityRuntime(runtime, discovery, identity);
        GossipLearning learning = new GossipLearning(new CapabilityPipes(runtime, codec),
                runtime.sampler(), identity.peerId(), codec, InstantSource.system());
        // Registering is the whole of the wiring: the peer tick drives the
        // offer/accept exchange and the learner is advertised to the fleet.
        capabilities.register(learning);
        node.startTicking(Duration.ofMillis(250));
        return new Peer(name, node, runtime, capabilities, learning);
    }

    /**
     * Starts this peer's local model: what it fitted to the data only it can see.
     *
     * @param peer    the learner
     * @param weights the local weights
     */
    public static void train(Peer peer, double... weights) {
        peer.learning().start(MODEL, weights);
    }

    /** The element-wise mean of several weight vectors: what every peer should converge to. */
    public static double[] mean(List<double[]> models) {
        double[] out = new double[models.get(0).length];
        for (double[] model : models) {
            for (int i = 0; i < out.length; i++) {
                out[i] += model[i] / models.size();
            }
        }
        return out;
    }

    /**
     * Runs the demo.
     *
     * @param args unused
     * @throws Exception on startup failure
     */
    public static void main(String[] args) throws Exception {
        System.out.println("gossip learning: three local models, one fleet model, no server\n");
        Peer north = startPeer("north", 7801, 0);
        Peer south = startPeer("south", 7802, 7801);
        Peer east = startPeer("east", 7803, 7801);
        List<Peer> fleet = List.of(north, south, east);
        try {
            Thread.sleep(1500); // membership settles
            train(north, 1.0, 2.0, 3.0);
            train(south, 3.0, 2.0, 1.0);
            train(east, 2.0, 5.0, 2.0);
            System.out.println("fleet mean should be " + Arrays.toString(mean(List.of(
                    new double[] {1, 2, 3}, new double[] {3, 2, 1}, new double[] {2, 5, 2}))));
            for (int i = 0; i < 40; i++) {
                Thread.sleep(250);
                if (fleet.stream().allMatch(p -> p.rounds() >= 8)) {
                    break;
                }
            }
            for (Peer peer : fleet) {
                System.out.printf("  %-6s after %2d rounds: %s%n", peer.name(), peer.rounds(),
                        peer.model().map(Arrays::toString).orElse("(no round yet)"));
            }
        } finally {
            fleet.forEach(Peer::close);
        }
    }
}
