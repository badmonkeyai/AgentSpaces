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
package ai.badmonkey.agentspaces.examples.quorum;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.capabilities.aggregate.PushSumAggregate;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.agent.AgentBinder;
import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.Ballot;
import ai.badmonkey.agentspaces.agent.annotation.OnDecision;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.spi.AgentIdentity;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.capabilities.vote.VoteCapability;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.peering.membership.MembershipAuthorizer;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
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
import java.util.Map;
import java.util.Set;
import java.util.OptionalDouble;
import java.util.Optional;

/**
 * Example 05: capability choreography. Push-sum aggregation senses the fleet's
 * average backlog; when it crosses a threshold, a supervisor opens a scale-up
 * proposal; every member casts a signed ballot; the quorum decision closes
 * identically on every replica, auditable from the vote space.
 *
 * <p>Run: {@code mvn -q -pl examples/example-05-quorum exec:java}
 */
public final class QuorumFleet {

    /** One assembled peer: node, capabilities, and the vote machinery. */
    public record Peer(PeerNode node, GroupRuntime runtime, ReplicatedSpace votes,
                       PushSumAggregate aggregate, VoteCapability vote,
                       AgentSpaces.GroupContext group) {

        /** Shuts the peer down. */
        public void close() {
            group.binder().close();
            votes.close();
            node.close();
        }
    }

    private static final String HOST = "127.0.0.1";
    /** The backlog level at which the fleet should consider scaling up. */
    public static final double BACKLOG_THRESHOLD = 20.0;

    private QuorumFleet() {
    }

    /** The example's well-known group. */
    public static GroupAdvertisement group() {
        GroupId groupId = GroupId.fromFounding(
                "quorum-demo-v1".getBytes(StandardCharsets.UTF_8));
        return new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(365), "quorum-fleet",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
    }

    /**
     * Assembles one peer with aggregate and vote capabilities.
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
        ReplicatedSpace votes = ReplicatedSpace.builder(runtime, "votes", identity, name)
                .settleWindow(Duration.ofMillis(150))
                .build();
        PushSumAggregate aggregate = new PushSumAggregate(
                new CapabilityPipes(runtime, codec), runtime.sampler(),
                identity.peerId(), codec, InstantSource.system());
        VoteCapability vote = new VoteCapability(votes, identity.agent(name),
                identity.peerId(), InstantSource.system());
        // Registering is the whole of it: the capability is advertised to the
        // fleet and driven by this peer's tick. No scheduler here, and no
        // aggregate.tick() anywhere in this example (QA3 A3-1, QA4 A4-3).
        DiscoveryService discovery = new DiscoveryService(runtime,
                new AdCache(codec, InstantSource.system()), codec, identity.peerId());
        CapabilityRuntime capabilities = new CapabilityRuntime(runtime, discovery, identity);
        // Providing through the facade registers, advertises, and drives both, and
        // tells the binder which vote backs @Ballot/@OnDecision on "votes".
        AgentSpaces spaces = new AgentSpaces(identity, InstantSource.system());
        AgentSpaces.GroupContext group = spaces.register("quorum", runtime.id(), runtime, discovery)
                .capabilities(capabilities);
        group.space("votes", votes);
        group.provide(aggregate);
        group.provide(vote);
        node.startTicking(Duration.ofMillis(250));
        return new Peer(node, runtime, votes, aggregate, vote, group);
    }

    // ------------------------------------------------ the agents, as annotations

    /** A member votes to scale up when its own backlog says it needs help. */
    @AgentSpec(name = "member", description = "Votes on scaling by its own backlog", goals = {"vote"})
    public static final class Member {
        private final double backlog;

        public Member(double backlog) {
            this.backlog = backlog;
        }

        @Ballot(space = "votes", prefix = "scale", lease = "10m")
        public String vote(VoteCapability.Proposal proposal) {
            return backlog > BACKLOG_THRESHOLD / 2 ? "approve" : "reject";
        }
    }

    /** The decision, on the record: written once when the quorum closes. */
    public record ScalingDecision(String proposalId, String winner, String tally, String countedPer) {
    }

    /** The supervisor records each closed scaling vote. */
    @AgentSpec(name = "supervisor", description = "Records scaling decisions", goals = {"record"})
    public static final class Supervisor {
        @OnDecision(space = "votes", prefix = "scale", resultLease = "10m")
        public ScalingDecision record(VoteCapability.Decision decision) {
            return new ScalingDecision(decision.proposalId(), decision.winner(),
                    decision.tally().toString(), decision.granularity().name());
        }
    }

    /** Binds a voting member on its peer; close the binding to stop it. */
    public static AutoCloseable runMember(Peer peer, double backlog) {
        return peer.group().bind(new Member(backlog));
    }

    /** Binds the supervisor on its peer; close the binding to stop it. */
    public static AutoCloseable runSupervisor(Peer peer) {
        return peer.group().bind(new Supervisor());
    }

    // ------------------------------------------------ act two: the electorate made visible

    /**
     * A council seat: one named agent on the shared council peer, voting through
     * its own view of the votes space with its own certified key (QA4 A4-7 phase 3).
     */
    public record Seat(String name, AgentIdentity identity, Space votes, VoteCapability vote) {
    }

    /** A council seat as an annotated agent: it votes "approve" on every budget proposal. */
    @AgentSpec(name = "seat", description = "A council seat", goals = {"vote"})
    public static final class CouncilSeat {
        @Ballot(space = "votes", prefix = "budget", lease = "10m")
        public String vote(VoteCapability.Proposal proposal) {
            return "approve";
        }
    }

    /**
     * Act two through the annotations: the council's seats are {@code @Ballot}
     * agents bound on one peer with a subordinate identity factory, so each
     * ballot is its agent's own attested record and the granted seats count per
     * agent while the ungranted one is refused before it writes. Under the
     * starter this is {@code agentspaces.identity.agent-keys=subordinate} plus the
     * grant; here the binder is built by hand to carry the council's authorizer.
     *
     * @param host    the peer the council sits on
     * @param names   the seats (each bound as agent {@code <peer>/<name>})
     * @param granted the seats the authorizer names for VOTE
     * @return the bindings, one per seat, in order
     */
    public static List<AutoCloseable> bindCouncil(Peer host, List<String> names, Set<String> granted) {
        Set<AgentId> grantedIds = new java.util.LinkedHashSet<>();
        for (String name : granted) {
            grantedIds.add(host.node().identity().agent(name));
        }
        Authorizer authorizer = new MembershipAuthorizer(host.runtime().membership(),
                host.node().peerId(), Map.of(Authorizer.Operation.VOTE, Set.of()),
                Map.of(Authorizer.Operation.VOTE, grantedIds));
        // One binder with a subordinate identity factory; each seat is the same
        // class bound under its own name, so each gets its own certified key.
        AgentBinder binder = new AgentBinder(host.node().identity(), host.runtime().id(), null,
                InstantSource.system(), "quorum", name -> host.node().identity().renewingSubordinate(name, java.time.Duration.ofHours(24), java.time.InstantSource.system()))
                .space("votes", host.votes())
                .vote("votes", new VoteCapability(host.votes(), host.votes().writer().orElseThrow(),
                        host.node().peerId(), InstantSource.system(),
                        VoteCapability.ANY_AUTHENTICATED_ISSUER, authorizer));
        List<AutoCloseable> bindings = new java.util.ArrayList<>();
        for (String name : names) {
            bindings.add(binder.bind(new CouncilSeat(), name));
        }
        return bindings;
    }

    /**
     * One peer, several voters (act two). Seats {@code names} on {@code host} as
     * subordinate identities, each with a per-agent view of the votes space, and
     * an authorizer that grants {@code VOTE} to exactly {@code granted}. With an
     * AgentId granted the authorizer speaks at AGENT granularity, so every
     * granted, attested seat counts once and an ungranted seat on the same peer
     * counts for nothing — which is the rule that makes one-member-one-vote hold
     * however a peer is seated (SPEC §8, §11).
     *
     * @param host    the peer the council sits on
     * @param names   the seats
     * @param granted the seats the authorizer names for VOTE
     * @return the seats, in order
     */
    public static List<Seat> seatCouncil(Peer host, List<String> names, Set<String> granted) {
        Set<AgentId> grantedIds = new java.util.LinkedHashSet<>();
        for (String name : granted) {
            grantedIds.add(host.node().identity().agent(name));
        }
        Authorizer authorizer = new MembershipAuthorizer(host.runtime().membership(),
                host.node().peerId(), Map.of(Authorizer.Operation.VOTE, Set.of()),
                Map.of(Authorizer.Operation.VOTE, grantedIds));
        List<Seat> seats = new java.util.ArrayList<>();
        for (String name : names) {
            // The seat's key, certified by the peer; its view of the one replica.
            AgentIdentity identity = host.node().identity().renewingSubordinate(name, java.time.Duration.ofHours(24), java.time.InstantSource.system());
            Space votes = host.votes().as(identity);
            VoteCapability vote = new VoteCapability(votes, identity.id(), host.node().peerId(),
                    InstantSource.system(), VoteCapability.ANY_AUTHENTICATED_ISSUER, authorizer);
            seats.add(new Seat(name, identity, votes, vote));
        }
        return seats;
    }

    /**
     * Runs the demo.
     *
     * @param args unused
     * @throws Exception on startup failure
     */
    public static void main(String[] args) throws Exception {
        System.out.println("quorum: push-sum senses the fleet, a vote decides\n");
        Peer supervisor = startPeer("supervisor", 7481, 0);
        Peer w1 = startPeer("worker-1", 7482, 7481);
        Peer w2 = startPeer("worker-2", 7483, 7481);
        Peer w3 = startPeer("worker-3", 7484, 7481);
        List<Peer> fleet = List.of(supervisor, w1, w2, w3);
        try {
            Thread.sleep(1500);

            // Each member contributes its local backlog: average = 26.5 > 20.
            double[] backlogs = {4, 30, 41, 31};
            for (int i = 0; i < fleet.size(); i++) {
                fleet.get(i).aggregate().start("backlog", backlogs[i]);
            }
            // The peer tick drives the exchange; we only wait for it to settle.
            double sensed = 0;
            for (int round = 0; round < 60; round++) {
                Thread.sleep(250);
                OptionalDouble estimate = supervisor.aggregate().estimate("backlog");
                if (estimate.isPresent()) {
                    sensed = estimate.getAsDouble();
                    if (round > 20) {
                        break;   // converged
                    }
                }
            }
            System.out.printf("sensed fleet average backlog: %.2f (threshold %.1f)%n%n",
                    sensed, BACKLOG_THRESHOLD);

            if (sensed > BACKLOG_THRESHOLD) {
                supervisor.vote().propose("scale-up-1",
                        "Average backlog " + String.format("%.1f", sensed)
                                + " exceeds threshold; add two workers?",
                        List.of("approve", "reject"), 4, Lease.of(Duration.ofMinutes(10)));
                Thread.sleep(800); // the proposal replicates

                String[] names = {"supervisor", "worker-1", "worker-2", "worker-3"};
                for (int i = 0; i < fleet.size(); i++) {
                    // Members approve when their own backlog says they need help.
                    String ballot = backlogs[i] > BACKLOG_THRESHOLD / 2 ? "approve" : "reject";
                    fleet.get(i).vote().castBallot("scale-up-1", ballot,
                            Lease.of(Duration.ofMinutes(10)));
                    System.out.println("  " + names[i] + " ballot: " + ballot
                            + " (backlog " + backlogs[i] + ")");
                }

                Optional<VoteCapability.Decision> decision = Optional.empty();
                long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
                while (decision.isEmpty() && System.nanoTime() < deadline) {
                    decision = supervisor.vote().decision("scale-up-1");
                    Thread.sleep(200);
                }
                System.out.println("\ndecision: " + decision.orElseThrow().winner()
                        + " " + decision.orElseThrow().tally()
                        + "  (signed ballots, recomputable by any member, counted per "
                        + decision.orElseThrow().granularity() + ")");
            }

            // Act two: a council of three on one peer, two of them granted.
            List<Seat> council = seatCouncil(supervisor, List.of("finance", "ops", "intern"),
                    Set.of("finance", "ops"));
            council.get(0).vote().propose("budget-1", "Approve the budget?",
                    List.of("approve", "reject"), 2, Lease.of(Duration.ofMinutes(10)));
            Thread.sleep(500);
            for (Seat seat : council) {
                try {
                    seat.vote().castBallot("budget-1", "approve", Lease.of(Duration.ofMinutes(10)));
                    System.out.println("  " + seat.name() + " cast a ballot");
                } catch (IllegalStateException e) {
                    System.out.println("  " + seat.name() + " refused: " + e.getMessage());
                }
            }
            Thread.sleep(1000);
            System.out.println("council tally: " + council.get(0).vote().tally("budget-1")
                    + " at " + council.get(0).vote().granularity() + " granularity;"
                    + " a worker's per-peer view counts " + w1.vote().tally("budget-1"));
        } finally {
            fleet.forEach(Peer::close);
        }
    }
}
