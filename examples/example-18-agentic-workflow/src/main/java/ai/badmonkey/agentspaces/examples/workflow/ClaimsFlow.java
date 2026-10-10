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
package ai.badmonkey.agentspaces.examples.workflow;

import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.agent.capability.AggregateClient;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.aggregate.PushSumAggregate;
import ai.badmonkey.agentspaces.capabilities.orderedlog.OrderedTakes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.capabilities.vote.VoteCapability;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.examples.workflow.Claims.Claim;
import ai.badmonkey.agentspaces.examples.workflow.Claims.ClaimAssessment;
import ai.badmonkey.agentspaces.examples.workflow.Claims.ClaimClosed;
import ai.badmonkey.agentspaces.examples.workflow.Claims.Payment;
import ai.badmonkey.agentspaces.examples.workflow.Claims.PricedClaim;
import ai.badmonkey.agentspaces.examples.workflow.Claims.RepairEstimate;
import ai.badmonkey.agentspaces.examples.workflow.Claims.SeniorReview;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Example 18: a workflow built a stage at a time, every stage an annotated
 * POJO method, and the entries between them the whole of the protocol. Three
 * peers share five spaces: {@code intake} (LEASE_RACE), {@code assessment}
 * (AUCTION), {@code decisions} (the vote), {@code payments} (taken through the
 * ordered log) and {@code ledger} (long leases). A claim is registered, three
 * specialists examine it in parallel, their findings are joined, the join is
 * priced at auction, a panel votes, an approved claim is paid exactly once, and
 * every case closes in the ledger while the fleet converges on its average
 * reserve.
 *
 * <p>Run: {@code mvn -q -pl examples/example-18-agentic-workflow exec:java}
 */
public final class ClaimsFlow {

    public static final String GROUP = "claims-fleet";
    private static final String HOST = "127.0.0.1";

    /** The claims the demo and the test put in. */
    public static final List<Claim> CLAIMS = List.of(
            new Claim("CLM-1", "P-100", "A. Okafor", "rear-end collision at a stop light", 8_000),
            new Claim("CLM-2", "P-200-LAPSED", "B. Lindqvist", "hail damage to the roof", 3_000),
            new Claim("CLM-3", "P-300", "C. Marsh", "staged accident, two vehicles", 12_000),
            new Claim("CLM-4", "P-400", "D. Varga", "kitchen fire, structural damage", 40_000),
            new Claim("CLM-5", "P-500", "E. Nakamura", "windscreen chip, amount missing", 0));

    private ClaimsFlow() {
    }

    /** One assembled peer: node, group, the five spaces, and the three capabilities. */
    public record Peer(String name, PeerIdentity identity, PeerNode node, GroupRuntime runtime,
                       DiscoveryService discovery, CapabilityRuntime capabilities,
                       ReplicatedSpace intake, ReplicatedSpace assessment, ReplicatedSpace decisions,
                       ReplicatedSpace payments, ReplicatedSpace ledger,
                       OrderedTakes ordered, VoteCapability vote, PushSumAggregate aggregate,
                       AgentSpaces.GroupContext group) {

        public void close() {
            group.binder().close();
            capabilities.close();
            for (ReplicatedSpace space : List.of(ledger, payments, decisions, assessment, intake)) {
                space.close();
            }
            node.close();
        }

        public AggregateClient reserves() {
            return group.capability(AggregateClient.class);
        }
    }

    public static GroupAdvertisement group() {
        GroupId groupId = GroupId.fromFounding("claims-fleet-v1".getBytes(StandardCharsets.UTF_8));
        return new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(365),
                GROUP, GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
    }

    /**
     * Joins the group, builds the five spaces, and registers the three
     * capabilities the Layer 4 annotations need: the vote on {@code decisions},
     * the aggregate for {@code Contribution} returns, and the ordered-log
     * coordinator over {@code payments}. Registering is the whole of the wiring;
     * the peer tick drives all three.
     */
    public static Peer startPeer(PeerIdentity identity, String name, int port, int seedPort,
                                 List<PeerId> members, long seed) throws Exception {
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

        // One space per lane: the strategy, the retention and the admission are
        // decided here, once, and every stage inherits them by naming the space.
        ReplicatedSpace intake = space(runtime, identity, name, Intake.SPACE);
        ReplicatedSpace assessment = ReplicatedSpace.builder(runtime, Pricing.SPACE, identity, name)
                .settleWindow(Duration.ofMillis(150))
                .strategy(ConflictStrategyType.AUCTION)      // the bound @BidFunction prices takes
                .build();
        ReplicatedSpace decisions = space(runtime, identity, name, Decisions.SPACE);
        ReplicatedSpace payments = space(runtime, identity, name, Settlement.PAYMENTS);
        ReplicatedSpace ledger = space(runtime, identity, name, Settlement.LEDGER);

        CapabilityPipes pipes = new CapabilityPipes(runtime, codec);
        OrderedTakes ordered = OrderedTakes.over(pipes, codec, identity, name, members,
                InstantSource.system(), seed, payments);
        VoteCapability vote = new VoteCapability(decisions, identity.agent(name), identity.peerId(),
                InstantSource.system());
        PushSumAggregate aggregate = new PushSumAggregate(pipes, runtime.sampler(), identity.peerId(),
                codec, InstantSource.system());

        AgentSpaces spaces = new AgentSpaces(identity, InstantSource.system());
        AgentSpaces.GroupContext group = spaces.register(GROUP, runtime.id(), runtime, discovery)
                .capabilities(capabilities);
        group.space(Intake.SPACE, intake);
        group.space(Pricing.SPACE, assessment);
        group.space(Decisions.SPACE, decisions);
        group.space(Settlement.PAYMENTS, payments);
        group.space(Settlement.LEDGER, ledger);
        group.provide(vote);                          // backs @Ballot and @OnDecision on "decisions"
        group.provide(aggregate);                     // the target of Contribution returns
        group.ordered(Settlement.PAYMENTS, ordered);  // backs @OrderedTake on "payments"
        node.startTicking(Duration.ofMillis(250));
        return new Peer(name, identity, node, runtime, discovery, capabilities, intake, assessment,
                decisions, payments, ledger, ordered, vote, aggregate, group);
    }

    private static ReplicatedSpace space(GroupRuntime runtime, PeerIdentity identity, String name,
                                         String spaceName) {
        return ReplicatedSpace.builder(runtime, spaceName, identity, name)
                .settleWindow(Duration.ofMillis(150))
                .build();
    }

    /**
     * Seats the agents. Roles that must be singletons in the flow (the assembler,
     * the underwriter, the approver, the ledger, the supervisor) are bound on one
     * peer; roles that are one per peer (panelist, payer, sensor) are bound on
     * all three; the two registrars and the two adjusters show the lease race
     * and the auction.
     */
    public static List<AutoCloseable> staff(Peer a, Peer b, Peer c, Intake.Hook hook) {
        return staff(a, b, c, hook, true, true, Duration.ofSeconds(10));
    }

    /** The fleet with no panelists at all and a short reminder, so the deadline shows. */
    public static List<AutoCloseable> staffWithoutPanel(Peer a, Peer b, Peer c, Intake.Hook hook,
                                                        Duration reminderLease) {
        return staff(a, b, c, hook, true, false, reminderLease);
    }

    private static List<AutoCloseable> staff(Peer a, Peer b, Peer c, Intake.Hook hook,
                                             boolean localJoiner, boolean panel, Duration reminderLease) {
        List<AutoCloseable> bound = new ArrayList<>();
        bound.add(a.group().bind(new Intake.Registrar("registrar-a", hook)));
        bound.add(b.group().bind(new Intake.Registrar("registrar-b", hook)));
        bound.add(a.group().bind(new Intake.FraudScreen()));
        bound.add(b.group().bind(new Intake.CoverageCheck()));
        bound.add(c.group().bind(new Intake.DamageEstimator()));
        if (localJoiner) {
            bound.add(a.group().bind(new Intake.Assembler("assembler")));
        }
        bound.add(a.group().bind(new Pricing.SeniorAdjuster()));
        bound.add(b.group().bind(new Pricing.JuniorAdjuster()));
        // Stage 3a and 3b: the fork, one shop per peer taking its own tasks, the
        // gather, and the severity route.
        bound.add(a.group().bind(new Quotes.QuoteDesk()));
        bound.add(a.group().bind(new Quotes.NorthShop()));
        bound.add(b.group().bind(new Quotes.EastShop()));
        bound.add(c.group().bind(new Quotes.SouthShop()));
        bound.add(c.group().bind(new Quotes.QuoteGatherer()));
        bound.add(b.group().bind(new Quotes.SeniorReviewer()));
        bound.add(a.group().bind(new Decisions.Underwriter(reminderLease)));
        bound.add(a.group().bind(new Decisions.Escalator()));
        if (panel) {
            bound.add(a.group().bind(new Decisions.Panelist(50_000), "panelist-a"));
            bound.add(b.group().bind(new Decisions.Panelist(50_000), "panelist-b"));
            bound.add(c.group().bind(new Decisions.Panelist(15_000), "panelist-c"));
        }
        bound.add(a.group().bind(new Decisions.Approver()));
        for (Peer peer : List.of(a, b, c)) {
            bound.add(peer.group().bind(new Settlement.Payer(peer.name(), peer.ordered())));
            bound.add(peer.group().bind(new Settlement.ReserveSensor()));
        }
        bound.add(c.group().bind(new Settlement.Ledger()));
        bound.add(c.group().bind(new Settlement.Exposure()));
        bound.add(c.group().bind(new Settlement.Supervisor("supervisor")));
        return bound;
    }

    /**
     * The same fleet with the join in {@code ORDERED} mode on two peers instead
     * of one {@code LOCAL} joiner; the joiners' tickets ride the payments log.
     */
    public static List<AutoCloseable> staffWithOrderedJoin(Peer a, Peer b, Peer c, Intake.Hook hook) {
        List<AutoCloseable> bound = new ArrayList<>(staff(a, b, c, hook, false, true, Duration.ofSeconds(10)));
        bound.add(a.group().bind(new Intake.OrderedAssembler("assembler-a")));
        bound.add(b.group().bind(new Intake.OrderedAssembler("assembler-b")));
        return bound;
    }

    /** A hook that throws once, on the first claim any registrar takes. */
    public static Intake.Hook crashOnce(AtomicBoolean armed) {
        return (worker, claim) -> {
            if (armed.compareAndSet(true, false)) {
                System.out.println("  " + worker + " crashes while registering " + claim.claimId()
                        + " (the lease will lapse)");
                throw new IllegalStateException("simulated crash in " + worker);
            }
        };
    }

    public static void main(String[] args) throws Exception {
        System.out.println("claims flow: nine stages, three peers, five spaces, no orchestrator\n");
        List<PeerIdentity> ids = List.of(PeerIdentity.generate(), PeerIdentity.generate(),
                PeerIdentity.generate());
        List<PeerId> members = ids.stream().map(PeerIdentity::peerId).toList();
        Peer a = startPeer(ids.get(0), "peer-a", 7711, 0, members, 1);
        Peer b = startPeer(ids.get(1), "peer-b", 7712, 7711, members, 2);
        Peer c = startPeer(ids.get(2), "peer-c", 7713, 7711, members, 3);
        List<Peer> fleet = List.of(a, b, c);
        List<AutoCloseable> staff = staff(a, b, c, crashOnce(new AtomicBoolean(true)));
        try {
            Thread.sleep(2500);                       // membership settles, a log leader is elected
            for (Claim claim : CLAIMS) {
                a.intake().write(claim, Lease.of(Duration.ofHours(1)));
                System.out.printf("  in: %s %-12s %-40s %,10.0f%n", claim.claimId(), claim.policyId(),
                        claim.description(), claim.claimedAmount());
            }
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            List<ClaimClosed> closed = List.of();
            while (closed.size() < CLAIMS.size() && System.nanoTime() < deadline) {
                Thread.sleep(250);
                closed = c.ledger().readAll(Template.of(ClaimClosed.class), 10);
            }
            System.out.println("\nassessments left unpriced: "
                    + a.assessment().readAll(Template.of(ClaimAssessment.class), 10).size()
                    + " (the auction take consumes each one)");
            System.out.println("priced at auction:");
            a.assessment().readAll(Template.of(PricedClaim.class), 10).stream()
                    .sorted(Comparator.comparing(PricedClaim::claimId))
                    .forEach(p -> System.out.printf("  %s reserve %,8.0f %-7s by %s%n", p.claimId(),
                            p.reserve(), p.recommendation(), p.pricedBy()));
            System.out.println("payments (exactly once, through the log):");
            c.payments().readAll(Template.of(Payment.class), 10).stream()
                    .sorted(Comparator.comparing(Payment::claimId))
                    .forEach(p -> System.out.printf("  %s %,10d cents by %s (log #%d)%n", p.claimId(),
                            p.cents(), p.paidBy(), p.logIndex()));
            System.out.println("repair estimates (the lowest of three quotes, gathered per claim):");
            c.ledger().readAll(Template.of(RepairEstimate.class), 10).stream()
                    .sorted(Comparator.comparing(RepairEstimate::claimId))
                    .forEach(e -> System.out.printf("  %s %,8.0f from %s (of %d)%n", e.claimId(), e.amount(),
                            e.shop(), e.considered()));
            System.out.println("senior reviews (severe claims only):");
            c.ledger().readAll(Template.of(SeniorReview.class), 10)
                    .forEach(r -> System.out.printf("  %s %s%n", r.claimId(), r.note()));
            System.out.println("ledger:");
            closed.stream().sorted(Comparator.comparing(ClaimClosed::claimId))
                    .forEach(x -> System.out.printf("  %s %-8s %s%n", x.claimId(), x.outcome(), x.summary()));
            System.out.printf("%nfleet average reserve: %s%n",
                    c.reserves().awaitEstimate(Settlement.RESERVE_EPOCH, Duration.ofSeconds(10))
                            .stream().mapToObj(v -> String.format("%,.0f", v)).findFirst()
                            .orElse("(not yet)"));
        } finally {
            for (AutoCloseable s : staff) {
                s.close();
            }
            fleet.forEach(Peer::close);
        }
    }
}
