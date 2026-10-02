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
package ai.badmonkey.agentspaces.examples.quickstart;

import ai.badmonkey.agentspaces.agent.AgentBinder;
import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.SpaceNotify;
import ai.badmonkey.agentspaces.agent.annotation.SpaceRef;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
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

/**
 * Example 11: the gentlest possible start. Two plain classes become a two-peer
 * fleet: a worker that takes orders and ships them, and an auditor that reacts
 * to every shipment with a receipt — choreography in one returning method. No
 * space names appear anywhere (the group's sole space is inferred), durations
 * read like Spring properties ({@code "10m"}), a {@code @SpaceRef} field gives
 * the worker a handle for a mid-work progress entry, and the whole fleet is
 * this one file.
 *
 * <p>Run: {@code mvn -q -pl examples/example-11-quickstart exec:java}
 */
public final class Quickstart {

    /** An order somebody wants fulfilled. */
    public record Order(String orderId, String item) {
    }

    /** A progress marker the worker drops mid-work. */
    public record Progress(String orderId, String note) {
    }

    /** The worker's output. */
    public record Shipment(String orderId, String item, String by) {
    }

    /** The auditor's receipt, produced by choreography. */
    public record Receipt(String orderId, String summary) {
    }

    /** The worker: takes orders, ships them; crashes just lapse the lease. */
    @AgentSpec(name = "fulfiller", description = "Ships orders from the shared space",
            goals = {"fulfill orders"})
    public static class Fulfiller {

        /** Injected at bind time; the group's sole space, no name needed. */
        @SpaceRef
        private Space space;

        /**
         * Ships one order.
         *
         * @param order the order
         * @return the shipment
         */
        @SpaceTake(lease = "10m", pollTimeout = "300ms")
        public Shipment ship(Order order) {
            space.write(new Progress(order.orderId(), "picking " + order.item()),
                    Lease.of(Duration.ofMinutes(5)));
            return new Shipment(order.orderId(), order.item(), "fulfiller");
        }
    }

    /** The auditor: reacts to every shipment with a receipt; consumes nothing. */
    @AgentSpec(name = "auditor", description = "Writes a receipt for every shipment",
            goals = {"account for shipments"})
    public static class Auditor {

        /**
         * Accounts for one shipment. The binder dedupes redeliveries, runs
         * this off the delivery thread, and writes the returned receipt.
         *
         * @param shipment the shipment
         * @return the receipt
         */
        @SpaceNotify
        public Receipt account(Shipment shipment) {
            return new Receipt(shipment.orderId(),
                    shipment.item() + " shipped by " + shipment.by());
        }
    }

    private Quickstart() {
    }

    /** One assembled peer. */
    private record Peer(PeerNode node, GroupRuntime runtime, ReplicatedSpace space,
                        AgentBinder binder) {

        void close() {
            binder.close();
            space.close();
            node.close();
        }
    }

    /**
     * Runs the fleet: two peers, one space, two annotated POJOs.
     *
     * @param args unused
     * @throws Exception on startup failure
     */
    public static void main(String[] args) throws Exception {
        System.out.println("quickstart: two POJOs, two peers, one leased space\n");
        Peer workerPeer = startPeer(7591, 0);
        Peer auditorPeer = startPeer(7592, 7591);
        try {
            workerPeer.binder().bind(new Fulfiller());
            auditorPeer.binder().bind(new Auditor());
            Thread.sleep(1500);

            workerPeer.space().write(new Order("ord-1", "kite"),
                    Lease.of(Duration.ofMinutes(10)));
            workerPeer.space().write(new Order("ord-2", "compass"),
                    Lease.of(Duration.ofMinutes(10)));

            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            List<Receipt> receipts = List.of();
            while (receipts.size() < 2 && System.nanoTime() < deadline) {
                receipts = workerPeer.space().readAll(Template.of(Receipt.class), 10);
                Thread.sleep(200);
            }

            System.out.println("progress markers (worker's @SpaceRef writes):");
            workerPeer.space().readAll(Template.of(Progress.class), 10)
                    .forEach(p -> System.out.println("  " + p.orderId() + ": " + p.note()));
            System.out.println("receipts (auditor's @SpaceNotify returns):");
            receipts.forEach(r -> System.out.println("  " + r.orderId() + ": " + r.summary()));
            System.out.println("\nNo space names, no listeners, no dispatcher: the space"
                    + "\ncarried orders to the worker and shipments to the auditor.");
        } finally {
            auditorPeer.close();
            workerPeer.close();
        }
    }

    private static GroupAdvertisement group() {
        GroupId groupId = GroupId.fromFounding(
                "quickstart-v1".getBytes(StandardCharsets.UTF_8));
        return new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(365), "quickstart",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
    }

    private static Peer startPeer(int port, int seedPort) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).build();
        node.listen(new TcpTransport(), "127.0.0.1:" + port);
        List<PeerAdvertisement.Endpoint> seeds = seedPort == 0 ? List.of()
                : List.of(new PeerAdvertisement.Endpoint("tcp", "127.0.0.1:" + seedPort, 0));
        GroupRuntime runtime = node.joinGroup(group(),
                new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2),
                seeds);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "work", identity, "host")
                .settleWindow(Duration.ofMillis(150))
                .build();
        AgentBinder binder = new AgentBinder(identity, runtime.id(), null,
                InstantSource.system());
        binder.space("work", space);
        node.startTicking(Duration.ofMillis(250));
        return new Peer(node, runtime, space, binder);
    }
}
