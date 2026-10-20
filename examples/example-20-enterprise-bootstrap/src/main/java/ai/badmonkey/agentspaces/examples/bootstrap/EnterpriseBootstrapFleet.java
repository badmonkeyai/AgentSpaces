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
package ai.badmonkey.agentspaces.examples.bootstrap;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.spi.BeaconLocator;
import ai.badmonkey.agentspaces.api.spi.LocatePolicy;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.bootstrap.BootstrapChain;
import ai.badmonkey.agentspaces.peering.bootstrap.SeedsLocator;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Example 20: finding a beacon in the enterprise (SRAD
 * discovery-bootstrap-customization, DB-O-05).
 *
 * <p>Three seeds sit behind one DNS name, as they would behind
 * {@code seeds.corp.example} or a Kubernetes headless service. A worker
 * configures that one name and nothing else. Its bootstrap chain expands
 * the name to every address and dials them all, not the first; when its
 * view of the group is empty and the hint has aged, the chain is asked
 * again, so a seed that moved is found without a restart; and the trace of
 * what each locator answered is on the node for the console to show.
 *
 * <p>The test runs it with an in-memory resolver mapping the name to the
 * seeds' loopback addresses. {@link #main} runs it against the platform's
 * resolver: give it a real name whose records are the seeds' addresses
 * (see the README for the records a beacon prints).
 */
public final class EnterpriseBootstrapFleet {

    /** The group every peer founds from the same string, so any member is a seed. */
    public static final String FOUNDING = "enterprise-bootstrap-v1";

    private EnterpriseBootstrapFleet() {
    }

    /** The literal-id group of this example. */
    public static GroupAdvertisement group() {
        GroupId groupId = GroupId.fromFounding(FOUNDING.getBytes(StandardCharsets.UTF_8));
        return new GroupAdvertisement("aspace://" + groupId.value(), PeerIdentity.generate().peerId(), groupId,
                Instant.EPOCH, Duration.ofDays(365), "enterprise-fleet", GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
    }

    /** One peer: listening on {@code host:port}, playing the given roles. */
    public record Peer(PeerNode node, GroupRuntime runtime) implements AutoCloseable {
        @Override
        public void close() {
            node.close();
        }
    }

    /**
     * Starts a seed: a listening member, a rendezvous when asked.
     *
     * @param host       the bind host
     * @param port       the port
     * @param rendezvous whether this seed plays the {@code RENDEZVOUS} role
     * @param seeds      the other seeds to introduce itself to ({@code host:port}), none for the first
     * @return the running peer
     * @throws Exception if the port cannot be bound
     */
    public static Peer startSeed(String host, int port, boolean rendezvous, List<String> seeds) throws Exception {
        PeerNode node = PeerNode.builder(PeerIdentity.generate())
                .roles(rendezvous ? Set.of(PeerAdvertisement.PeerRole.RENDEZVOUS) : Set.of())
                .build();
        node.listen(new TcpTransport(), host + ":" + port);
        GroupRuntime runtime = node.joinGroup(group(), GroupMembership.Config.defaults(),
                SeedsLocator.parse(seeds, "tcp"));
        node.startTicking(Duration.ofMillis(250));
        return new Peer(node, runtime);
    }

    /**
     * Starts a worker that knows one name.
     *
     * @param seedName the seeds' DNS name with its port ({@code seeds.corp.example:7500})
     * @param resolver how names become addresses: the platform's, or the test's
     * @param policy   how the node paces and requires the chain
     * @return the running peer, dial-only
     */
    public static Peer startWorker(String seedName, Function<String, List<InetAddress>> resolver, LocatePolicy policy) {
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).build();
        node.transport(new TcpTransport());
        BeaconLocator chain = BootstrapChain.builder()
                .add(SeedsLocator.of(List.of(seedName)))
                .expandNames(resolver)
                .locatePolicy(policy)
                .build();
        GroupRuntime runtime = node.joinGroup(group(), GroupMembership.Config.defaults(), chain, java.util.Map.of(),
                null, "enterprise-fleet");
        node.startTicking(Duration.ofMillis(250));
        return new Peer(node, runtime);
    }

    /**
     * Runs the example against real DNS: {@code <name:port> <port-a> <port-b>}
     * starts two seeds on the loopback ports and a worker that resolves the
     * name through the platform; the name's records must point at this host.
     *
     * @param args the seeds' name with port, then two loopback ports
     * @throws Exception when a port cannot be bound
     */
    public static void main(String[] args) throws Exception {
        String name = args.length > 0 ? args[0] : "localhost:7500";
        int portA = args.length > 1 ? Integer.parseInt(args[1]) : 7500;
        int portB = args.length > 2 ? Integer.parseInt(args[2]) : 7501;
        try (Peer a = startSeed("0.0.0.0", portA, true, List.of());
             Peer b = startSeed("0.0.0.0", portB, false, List.of("127.0.0.1:" + portA));
             Peer worker = startWorker(name, ai.badmonkey.agentspaces.peering.bootstrap.DnsExpansion.PLATFORM,
                     LocatePolicy.DEFAULT)) {
            for (int i = 0; i < 40 && worker.runtime().membership().allMembers().size() < 2; i++) {
                Thread.sleep(250);
            }
            System.out.println("worker sees " + worker.runtime().membership().allMembers().size() + " member(s);"
                    + " hint: " + worker.node().lastBeaconHint(worker.runtime().id()).map(h -> h.evidence()).orElse("none"));
            worker.node().lastBootstrapTrace(worker.runtime().id())
                    .forEach(row -> System.out.println("  " + row.locator() + " " + row.millis() + " ms " + row.endpoints()
                            + " endpoint(s) " + row.evidence()));
        }
    }
}
