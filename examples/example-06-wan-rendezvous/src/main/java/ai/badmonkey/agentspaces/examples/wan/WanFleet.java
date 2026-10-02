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
package ai.badmonkey.agentspaces.examples.wan;

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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Example 06: two sites, one fleet. A rendezvous peer at headquarters is the only
 * address either site was configured with; peers from both sites join through
 * it, discovery flows through its larger cache, and the shared task space spans
 * everyone. Relay for NAT-restricted peers and WAN payload encryption follow in
 * later milestones (plan M3); the topology role machinery is what this example
 * shows.
 *
 * <p>Run: {@code mvn -q -pl examples/example-06-wan-rendezvous exec:java}
 */
public final class WanFleet {

    /** A task passed between sites. */
    public record SiteTask(String description, String origin) {
    }

    /** A result produced by whichever site had capacity. */
    public record SiteResult(String description, String origin, String completedBy) {
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

    private static final String HOST = "127.0.0.1";

    private WanFleet() {
    }

    /** The example's well-known group. */
    public static GroupAdvertisement group() {
        GroupId groupId = GroupId.fromFounding(
                "wan-rendezvous-demo-v1".getBytes(StandardCharsets.UTF_8));
        return new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(365), "wan-fleet",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
    }

    /**
     * Assembles one peer.
     *
     * @param name       the agent name
     * @param port       the TCP port
     * @param seedPort   the rendezvous port, or 0 for the rendezvous itself
     * @param rendezvous whether this peer serves the RENDEZVOUS role
     * @return the running peer
     * @throws Exception if the port cannot be bound
     */
    public static Peer startPeer(String name, int port, int seedPort, boolean rendezvous)
            throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity)
                .roles(rendezvous ? Set.of(PeerAdvertisement.PeerRole.RENDEZVOUS) : Set.of())
                .build();
        node.listen(new TcpTransport(), HOST + ":" + port);
        List<PeerAdvertisement.Endpoint> seeds = seedPort == 0 ? List.of()
                : List.of(new PeerAdvertisement.Endpoint("tcp", HOST + ":" + seedPort, 0));
        GroupRuntime runtime = node.joinGroup(group(),
                new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2),
                seeds);
        CborCodec codec = CborCodec.defaultCodec();
        DiscoveryService discovery = new DiscoveryService(runtime,
                new AdCache(codec, InstantSource.system()), codec, identity.peerId());
        ReplicatedSpace tasks = ReplicatedSpace.builder(runtime, "site-tasks", identity, name)
                .settleWindow(Duration.ofMillis(150))
                .build();
        node.startTicking(Duration.ofMillis(250));
        return new Peer(name, node, runtime, discovery, tasks);
    }

    /**
     * Runs the demo.
     *
     * @param args unused
     * @throws Exception on startup failure
     */
    public static void main(String[] args) throws Exception {
        System.out.println("wan-rendezvous: two sites, one fleet, one well-known address\n");
        Peer hq = startPeer("hq-rendezvous", 7491, 0, true);
        Peer siteA1 = startPeer("siteA-analyst", 7492, 7491, false);
        Peer siteA2 = startPeer("siteA-worker", 7493, 7491, false);
        Peer siteB1 = startPeer("siteB-analyst", 7494, 7491, false);
        Peer siteB2 = startPeer("siteB-worker", 7495, 7491, false);
        List<Peer> fleet = List.of(hq, siteA1, siteA2, siteB1, siteB2);
        try {
            Thread.sleep(2000); // both sites learn the fleet through the rendezvous

            System.out.println("membership at siteB-worker: "
                    + siteB2.runtime().membership().allMembers().size()
                    + " peers (all learned through the rendezvous)");
            System.out.println("rendezvous peers known: " + siteB2.runtime().membership()
                    .withRole(PeerAdvertisement.PeerRole.RENDEZVOUS).size() + "\n");

            // Site A publishes its analyst's card; site B discovers it.
            AdvertisementSigner signer = new AdvertisementSigner();
            AgentCard card = new AgentCard(
                    "aspace://" + hq.runtime().id().value() + "/agent/siteA-analyst",
                    siteA1.node().peerId(), hq.runtime().id(), Instant.now(),
                    Duration.ofMinutes(15), siteA1.node().identity().agent("analyst"),
                    "Analyzes cross-site workloads", List.of("analyze"),
                    List.of(), List.of(SiteResult.class.getName() + "#v1"), Map.of());
            siteA1.discovery().publish(signer.sign(card, siteA1.node().identity()));
            Thread.sleep(800);
            System.out.println("siteB found " + siteB1.discovery().find(AgentCard.class,
                    c -> c.description().contains("cross-site")).size()
                    + " analyst card(s) from the other site\n");

            // Work crosses sites through the shared space.
            siteB1.tasks().write(new SiteTask("reconcile ledgers", "site-B"),
                    Lease.of(Duration.ofMinutes(10)));
            var taken = siteA2.tasks().take(Template.of(SiteTask.class),
                    Lease.of(Duration.ofMinutes(5)), Duration.ofSeconds(15)).orElseThrow();
            siteA2.tasks().complete(taken, new SiteResult(taken.entry().description(),
                    taken.entry().origin(), "site-A"), Lease.of(Duration.ofHours(1)));

            SiteResult result = siteB1.tasks()
                    .read(Template.of(SiteResult.class), Duration.ofSeconds(15)).orElseThrow();
            System.out.println("task from " + result.origin() + " completed by "
                    + result.completedBy() + ": " + result.description());
            System.out.println("\nno site knew any address except the rendezvous.");
        } finally {
            fleet.forEach(Peer::close);
        }
    }
}
