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
package ai.badmonkey.agentspaces.examples.data;

import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.semantic.HashingEmbedder;
import ai.badmonkey.agentspaces.capabilities.semantic.SemanticDiscovery;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.connect.ConnectorRuntime;
import ai.badmonkey.agentspaces.connect.DataQueryClient;
import ai.badmonkey.agentspaces.connect.TableAssetProvider;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;

/**
 * Example 10: the data fleet, the enterprise economics in one demo
 * (ENTERPRISE §3). A connector peer holds an orders table and advertises it as
 * a leased AssetCard; analyst agents discover it by meaning through semantic
 * discovery, then query it through the space. The first identical query per
 * freshness window executes against the source; every later one reads the
 * leased result at replica speed, and the counters print the money: executions
 * against the source versus answers the space already held.
 *
 * <p>Run: {@code mvn -q -pl examples/example-10-data-fleet exec:java}
 */
public final class DataFleet {

    /** One assembled peer. */
    public record Peer(String name, PeerNode node, GroupRuntime runtime,
                       DiscoveryService discovery, SemanticDiscovery semantic,
                       ReplicatedSpace data) {

        /** Shuts the peer down. */
        public void close() {
            data.close();
            node.close();
        }
    }

    private static final String HOST = "127.0.0.1";

    private DataFleet() {
    }

    /** The example's well-known group. */
    public static GroupAdvertisement group() {
        GroupId groupId = GroupId.fromFounding(
                "data-fleet-demo-v1".getBytes(StandardCharsets.UTF_8));
        return new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(365), "data-fleet",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
    }

    /**
     * Assembles one peer on the shared data space.
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
        SemanticDiscovery semantic = new SemanticDiscovery(
                new CapabilityPipes(runtime, codec), runtime, discovery,
                identity.peerId(), codec, InstantSource.system(), new HashingEmbedder());
        ReplicatedSpace data = ReplicatedSpace.builder(runtime, "data", identity, name)
                .settleWindow(Duration.ofMillis(150))
                .build();
        node.startTicking(Duration.ofMillis(250));
        return new Peer(name, node, runtime, discovery, semantic, data);
    }

    /** The demo's orders table. */
    public static TableAssetProvider ordersTable() {
        return new TableAssetProvider("orders", "postgres://ops/public.orders",
                "Customer order history with line items and settlement status",
                "com.example.OrderRow#v1", Duration.ofMinutes(5),
                List.of(Map.of("id", "1", "region", "eu", "amount", "120.00"),
                        Map.of("id", "2", "region", "us", "amount", "80.00"),
                        Map.of("id", "3", "region", "eu", "amount", "45.50"),
                        Map.of("id", "4", "region", "eu", "amount", "300.00")));
    }

    /**
     * Runs the demo.
     *
     * @param args unused
     * @throws Exception on startup failure
     */
    public static void main(String[] args) throws Exception {
        System.out.println("data-fleet: pull once, share under policy\n");
        Peer connectorPeer = startPeer("pg-connector", 7531, 0);
        Peer analystA = startPeer("analyst-a", 7532, 7531);
        Peer analystB = startPeer("analyst-b", 7533, 7531);
        List<Peer> fleet = List.of(connectorPeer, analystA, analystB);
        TableAssetProvider provider = ordersTable();
        try (ConnectorRuntime connector = new ConnectorRuntime(connectorPeer.data(),
                connectorPeer.discovery(), connectorPeer.node().identity(),
                "pg-connector", provider, connectorPeer.runtime().id(),
                InstantSource.system(), Duration.ofMinutes(1))) {
            connector.start();
            Thread.sleep(1500); // membership and the AssetCard settle

            // Analyst A finds the data by meaning, then queries it.
            var matches = analystA.semantic()
                    .remoteQuery("who holds customer order history?", 3,
                            Duration.ofSeconds(2));
            AssetCard card = (AssetCard) matches.get(0).advertisement();
            System.out.println("analyst-a asked by meaning and found: " + card.asset()
                    + " (" + card.uri() + "), freshness " + card.freshness());

            DataQueryClient clientA = new DataQueryClient(analystA.data(), "analyst-a");
            DataQueryClient clientB = new DataQueryClient(analystB.data(), "analyst-b");

            var first = clientA.fetch(card.asset(), Map.of("region", "eu"),
                    Duration.ofSeconds(10)).orElseThrow();
            System.out.println("analyst-a: eu orders -> " + first.result().rows().size()
                    + " rows (fromCache=" + first.fromCache() + ")");
            Thread.sleep(1000); // the result replicates

            var second = clientB.fetch(card.asset(), Map.of("region", "eu"),
                    Duration.ofSeconds(10)).orElseThrow();
            System.out.println("analyst-b: same query          -> "
                    + second.result().rows().size() + " rows (fromCache="
                    + second.fromCache() + ")");

            var third = clientB.fetch(card.asset(), Map.of("region", "us"),
                    Duration.ofSeconds(10)).orElseThrow();
            System.out.println("analyst-b: us orders  -> " + third.result().rows().size()
                    + " row (fromCache=" + third.fromCache() + ")");

            System.out.println("\nsource executions: " + provider.executions()
                    + "  (three fetches, two hit the source)");
            System.out.println("every avoided execution is warehouse credits the "
                    + "abstraction returned.");
        } finally {
            fleet.forEach(Peer::close);
        }
    }
}
