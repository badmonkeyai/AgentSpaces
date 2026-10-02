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
package ai.badmonkey.agentspaces.examples.desk;

import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.OrderedTake;
import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.orderedlog.OrderedTakes;
import ai.badmonkey.agentspaces.capabilities.orderedlog.RaftLog;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
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
import java.util.Optional;

/**
 * Example 12: the exactly-once desk. Three clerks share a Raft-backed ordered
 * log over the {@code payments} space; a fourth peer, the desk, writes payment
 * orders and never joins the log. Every order is confirmed by exactly one
 * clerk, the confirmation replicates as a signed entry, and the order drains
 * from <em>every</em> replica — the desk's included, although it holds no
 * claim of its own and learns of the completion only by gossip.
 *
 * <p>Three things this example is careful to show, because each was got wrong
 * in a flagship before it was got right here (QA3 A3-1, QA4 A4-3, QA4 A4-5):
 * the {@link RaftLog} is <em>registered</em> on each clerk's
 * {@link CapabilityRuntime}, so the peer's own tick drives elections and the
 * leader lease is advertised and discoverable; ORDERED is not a space strategy
 * but a coordinator, so the space is built with the default strategy and taken
 * through {@link OrderedTakes}; and the claim the log commits carries its
 * holder's own signature, which is what lets the desk authenticate a
 * completion it did not witness.
 *
 * <p>The clerk is one annotated method: {@code @OrderedTake} is {@code @SpaceTake}
 * with the take routed through the space's ordered-log coordinator, so the
 * binder owns the loop (and the resubmit after a leader election) that used to
 * be written by hand here. The coordinator itself comes from
 * {@link OrderedTakes#over}, which wires the log's apply callback so the
 * {@code AtomicReference} cycle is gone too.
 *
 * <p>Run: {@code mvn -q -pl examples/example-12-exactly-once-desk exec:java}
 */
public final class ExactlyOnceDesk {

    /** Money someone wants moved. Non-idempotent: charging it twice is the bug. */
    public record PaymentOrder(String orderId, String payee, long cents) {
    }

    /** One clerk's confirmation, written atomically with the completion. */
    public record PaymentReceipt(String orderId, String payee, long cents, String clerk,
                                 long logIndex) {
    }

    /** The replicated space every peer shares. */
    public static final String PAYMENTS = "payments";

    private static final String HOST = "127.0.0.1";
    private static final Lease ORDER_LEASE = Lease.of(Duration.ofMinutes(10));
    private static final Lease RECEIPT_LEASE = Lease.of(Duration.ofHours(1));
    private static final Lease TAKE_LEASE = Lease.of(Duration.ofSeconds(30));

    /** One assembled peer. Clerks carry a log and a coordinator; the desk carries neither. */
    public record Peer(String name, PeerIdentity identity, PeerNode node, GroupRuntime runtime,
                       DiscoveryService discovery, CapabilityRuntime capabilities,
                       ReplicatedSpace payments, RaftLog raft, OrderedTakes ordered,
                       AgentSpaces.GroupContext group) {

        /** Shuts the peer down. */
        public void close() {
            group.binder().close();
            capabilities.close();
            payments.close();
            node.close();
        }

        /** Whether this peer's log member currently leads the quorum. */
        public boolean leads() {
            return raft != null && raft.isLeader();
        }
    }

    private ExactlyOnceDesk() {
    }

    /** The example's well-known group. */
    public static GroupAdvertisement group() {
        GroupId groupId = GroupId.fromFounding("exactly-once-desk-v1".getBytes(StandardCharsets.UTF_8));
        return new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(365),
                "payments-desk", GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
    }

    /**
     * Seats a clerk: joins the group, opens the payments space, and forms its
     * member of the Raft log — registered as a capability so the peer tick
     * drives it and its leader lease is advertised to the fleet.
     *
     * @param identity this clerk's identity (the member set is fixed up front)
     * @param name     the clerk's agent name
     * @param port     the TCP port to listen on
     * @param seedPort an existing member's port, or 0 for the first
     * @param members  every clerk's PeerId, the same list on every clerk
     * @param seed     a per-clerk election-timer seed
     * @return the running clerk
     * @throws Exception if the port cannot be bound
     */
    public static Peer startClerk(PeerIdentity identity, String name, int port, int seedPort,
                                  List<PeerId> members, long seed) throws Exception {
        Peer base = join(identity, name, port, seedPort);
        CborCodec codec = CborCodec.defaultCodec();
        // The log and its coordinator, wired together: the coordinator installs
        // each committed claim on the space with its holder's own signature intact.
        OrderedTakes ordered = OrderedTakes.over(new CapabilityPipes(base.runtime(), codec), codec,
                identity, name, members, InstantSource.system(), seed, base.payments());
        // Registering the coordinator is the whole of the wiring: its log rides the
        // peer tick (elections, heartbeats, the truthful leader lease, role=leader
        // discoverable), and @OrderedTake methods on this peer can now be bound.
        base.group().ordered(PAYMENTS, ordered);
        return new Peer(name, identity, base.node(), base.runtime(), base.discovery(),
                base.capabilities(), base.payments(), ordered.raft(), ordered, base.group());
    }

    /**
     * Seats the desk: a member of the group and a replica of the payments
     * space, but not of the log. It writes orders and reads receipts.
     */
    public static Peer startDesk(String name, int port, int seedPort) throws Exception {
        return join(PeerIdentity.generate(), name, port, seedPort);
    }

    private static Peer join(PeerIdentity identity, String name, int port, int seedPort)
            throws Exception {
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
        CapabilityRuntime capabilities = new CapabilityRuntime(runtime, discovery, identity);
        // ORDERED is a coordinator over the log, not a strategy the space knows:
        // the space is built plainly and taken through OrderedTakes.
        ReplicatedSpace payments = ReplicatedSpace.builder(runtime, PAYMENTS, identity, name)
                .settleWindow(Duration.ofMillis(150))
                .build();
        AgentSpaces spaces = new AgentSpaces(identity, InstantSource.system());
        AgentSpaces.GroupContext group = spaces.register("payments-desk", runtime.id(), runtime, discovery)
                .capabilities(capabilities);
        group.space(PAYMENTS, payments);
        node.startTicking(Duration.ofMillis(250));
        return new Peer(name, identity, node, runtime, discovery, capabilities, payments,
                null, null, group);
    }

    /**
     * A clerk: takes payment orders through the log and confirms each exactly
     * once. The returned receipt completes the take; a crash lets the lease
     * lapse and the log's next committed claim reassigns the order.
     */
    @AgentSpec(name = "clerk", description = "Confirms payments exactly once", goals = {"confirm"})
    public static final class Clerk {
        private final Peer peer;

        public Clerk(Peer peer) {
            this.peer = peer;
        }

        @OrderedTake(space = PAYMENTS, lease = "30s", pollTimeout = "2s", resultLease = "1h")
        public PaymentReceipt confirm(PaymentOrder order) {
            return new PaymentReceipt(order.orderId(), order.payee(), order.cents(), peer.name(),
                    peer.raft().commitIndex());
        }
    }

    /**
     * Binds a clerk on its peer.
     *
     * @param clerk the clerk peer
     * @return the binding; close it to stop the clerk
     */
    public static AutoCloseable runClerk(Peer clerk) {
        return clerk.group().bind(new Clerk(clerk));
    }

    /** The current leader's advertisement as the desk discovers it, if any. */
    public static Optional<CapabilityAdvertisement> leaderSeenBy(Peer peer) {
        return peer.discovery().find(CapabilityAdvertisement.class,
                        ad -> RaftLog.TYPE.equals(ad.capabilityType())
                                && "leader".equals(ad.parameters().get("role")))
                .stream().findFirst();
    }

    /**
     * Runs the demo.
     *
     * @param args unused
     * @throws Exception on startup failure
     */
    public static void main(String[] args) throws Exception {
        System.out.println("exactly-once desk: three clerks, one log, every payment confirmed once\n");
        List<PeerIdentity> ids = List.of(PeerIdentity.generate(), PeerIdentity.generate(),
                PeerIdentity.generate());
        List<PeerId> members = ids.stream().map(PeerIdentity::peerId).toList();
        Peer a = startClerk(ids.get(0), "clerk-a", 7601, 0, members, 1);
        Peer b = startClerk(ids.get(1), "clerk-b", 7602, 7601, members, 2);
        Peer c = startClerk(ids.get(2), "clerk-c", 7603, 7601, members, 3);
        Peer desk = startDesk("desk", 7604, 7601);
        List<Peer> fleet = List.of(a, b, c, desk);
        List<AutoCloseable> clerks = List.of(runClerk(a), runClerk(b), runClerk(c));
        try {
            Thread.sleep(2500); // membership settles and a leader is elected
            System.out.println("leader, as the desk discovers it: "
                    + leaderSeenBy(desk).map(ad -> ad.issuer().display()).orElse("(not yet)"));
            for (int i = 1; i <= 3; i++) {
                desk.payments().write(new PaymentOrder("ord-" + i, "payee-" + i, 1000L * i),
                        ORDER_LEASE);
            }
            Thread.sleep(4000);
            List<PaymentReceipt> receipts = desk.payments()
                    .readAll(Template.of(PaymentReceipt.class), 10);
            receipts.forEach(r -> System.out.printf("  %s  %6d cents  confirmed once by %s (log #%d)%n",
                    r.orderId(), r.cents(), r.clerk(), r.logIndex()));
            System.out.println("orders still open at the desk: "
                    + desk.payments().readAll(Template.of(PaymentOrder.class), 10).size());
        } finally {
            for (AutoCloseable clerk : clerks) {
                clerk.close();
            }
            fleet.forEach(Peer::close);
        }
    }
}
