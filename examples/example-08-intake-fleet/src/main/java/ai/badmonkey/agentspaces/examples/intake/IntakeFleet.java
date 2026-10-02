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
package ai.badmonkey.agentspaces.examples.intake;

import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.Ballot;
import ai.badmonkey.agentspaces.agent.annotation.OnDecision;
import ai.badmonkey.agentspaces.agent.annotation.SpaceNotify;
import ai.badmonkey.agentspaces.agent.annotation.SpaceRef;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.vote.VoteCapability;
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
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;

/**
 * Example 08: the document intake fleet from USE-CASES.md. Hard-to-read scans
 * enter the space; two extraction agents each produce a candidate reading with
 * a confidence score; the fleet closes a signed quorum vote on which reading
 * to trust; and an auditor files the winning candidate, carrying the whole
 * provenance chain, scan, both candidates, ballots, and filing, as signed
 * entries a compliance reviewer can replay. Service choreography end to end:
 * each stage consumes the entries the previous stage produced, and no
 * orchestrator process exists.
 *
 * <p>Run: {@code mvn -q -pl examples/example-08-intake-fleet exec:java}
 */
public final class IntakeFleet {

    /** A scan needing extraction; the text stands in for image bytes. */
    public record ScanEntry(String scanId, String smudgedText) {
    }

    /** One agent's candidate reading of a scan. */
    public record ExtractionCandidate(String scanId, String extractor,
                                      String taxpayerId, String amount,
                                      double confidence) {
    }

    /** The filed, quorum-approved reading. */
    public record FilingEntry(String scanId, String extractor, String taxpayerId,
                              String amount, String approvedBy) {
    }

    /** One assembled peer. */
    public record Peer(String name, PeerNode node, GroupRuntime runtime,
                       ReplicatedSpace intake, VoteCapability vote, AgentSpaces.GroupContext group) {

        /** Shuts the peer down. */
        public void close() {
            group.binder().close();
            intake.close();
            node.close();
        }
    }

    private static final String HOST = "127.0.0.1";

    private IntakeFleet() {
    }

    /** The example's well-known group. */
    public static GroupAdvertisement group() {
        GroupId groupId = GroupId.fromFounding(
                "intake-fleet-demo-v1".getBytes(StandardCharsets.UTF_8));
        return new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH,
                Duration.ofDays(365), "intake-fleet",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
    }

    /**
     * Assembles one peer on the shared intake space.
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
        ReplicatedSpace intake = ReplicatedSpace.builder(runtime, "intake", identity, name)
                .settleWindow(Duration.ofMillis(150))
                .build();
        VoteCapability vote = new VoteCapability(intake, identity.agent(name),
                identity.peerId(), InstantSource.system());
        // The agents are annotated POJOs bound through the facade; the vote is
        // registered so @Ballot and @OnDecision on "intake" know what to cast with.
        AgentSpaces spaces = new AgentSpaces(identity, InstantSource.system());
        AgentSpaces.GroupContext group = spaces.register("intake", runtime.id(), runtime, null);
        group.space("intake", intake);
        group.vote("intake", vote);
        node.startTicking(Duration.ofMillis(250));
        return new Peer(name, node, runtime, intake, vote, group);
    }

    /**
     * An extractor: its cue is a scan; its output is its reading, written back as
     * an {@link ExtractionCandidate}. A sloppy extractor misreads digits.
     */
    @AgentSpec(name = "extractor", description = "Reads a scan", goals = {"extract"})
    public static final class Extractor {
        private final String label;
        private final double skill;

        public Extractor(String label, double skill) {
            this.label = label;
            this.skill = skill;
        }

        @SpaceNotify(space = "intake", lease = "1h", resultLease = "10m")
        public ExtractionCandidate extract(ScanEntry scan) {
            String taxpayerId = skill > 0.8 ? "TIN-88-1234567" : "TIN-88-1284567";
            String amount = skill > 0.8 ? "12,400.00" : "12,4O0.OO";
            return new ExtractionCandidate(scan.scanId(), label, taxpayerId, amount, skill);
        }
    }

    /**
     * A voter: its cue is the auditor's proposal ("which reading do we trust?");
     * it backs the candidate with the highest confidence. Because the cue is the
     * proposal itself the ballot is never refused as unknown; the only wait is for
     * the candidates the proposal names to have replicated here.
     */
    @AgentSpec(name = "voter", description = "Backs the most confident reading", goals = {"vote"})
    public static final class Voter {
        @SpaceRef("intake")
        Space intake;

        @Ballot(space = "intake", prefix = "scan-", lease = "10m")
        public String back(VoteCapability.Proposal proposal) {
            String scanId = proposal.proposalId().substring("scan-".length());
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (System.nanoTime() < deadline) {
                List<ExtractionCandidate> candidates = intake.readAll(
                        Template.of(ExtractionCandidate.class).where("scanId", eq(scanId)), 10);
                if (candidates.size() >= proposal.options().size()) {
                    return candidates.stream()
                            .max(java.util.Comparator.comparingDouble(ExtractionCandidate::confidence))
                            .orElseThrow().extractor();
                }
                sleep(100);
            }
            return null; // the candidates never arrived: abstain
        }
    }

    /**
     * The auditor: its cue is a closed vote; it files the winning reading with
     * the tally as provenance and takes the consumed candidates off the space.
     */
    @AgentSpec(name = "auditor", description = "Files the reading the quorum trusts", goals = {"file"})
    public static final class Auditor {
        private final int quorum;
        @SpaceRef("intake")
        Space intake;

        public Auditor(int quorum) {
            this.quorum = quorum;
        }

        @OnDecision(space = "intake", prefix = "scan-", resultLease = "1d")
        public FilingEntry file(VoteCapability.Decision decision) {
            String scanId = decision.proposalId().substring("scan-".length());
            String winner = decision.winner();
            List<ExtractionCandidate> candidates = intake.readAll(
                    Template.of(ExtractionCandidate.class).where("scanId", eq(scanId)), 10);
            ExtractionCandidate chosen = candidates.stream()
                    .filter(candidate -> candidate.extractor().equals(winner))
                    .findFirst().orElseThrow();
            // Take the candidates off the space: the pipeline consumed them.
            for (int i = 0; i < candidates.size(); i++) {
                intake.take(Template.of(ExtractionCandidate.class).where("scanId", eq(scanId)),
                                Lease.of(Duration.ofMinutes(1)), Duration.ofSeconds(5))
                        .ifPresent(intake::complete);
            }
            return new FilingEntry(scanId, chosen.extractor(), chosen.taxpayerId(), chosen.amount(),
                    String.format(Locale.ROOT, "%d-vote quorum %s", quorum, decision.tally()));
        }
    }

    /**
     * Binds an extractor and a voter on a peer.
     *
     * @param peer  the extractor's peer
     * @param skill how carefully it reads (above 0.8 reads correctly)
     * @return the bindings; close to stop both
     */
    public static AutoCloseable runExtractor(Peer peer, double skill) {
        AutoCloseable extractor = peer.group().bind(new Extractor(peer.name(), skill));
        AutoCloseable voter = peer.group().bind(new Voter());
        return () -> {
            voter.close();
            extractor.close();
        };
    }

    /**
     * Audits a scan: once the candidates are on the record, puts "which reading
     * do we trust?" to a quorum vote; the auditor's own {@code @OnDecision} agent
     * files the winner when the vote closes, and this call awaits that filing.
     *
     * @param auditor the auditing peer
     * @param scanId  the scan
     * @param quorum  the ballots needed
     * @return the filing, or empty when no quorum formed in time
     */
    public static Optional<FilingEntry> auditScan(Peer auditor, String scanId, int quorum) {
        List<ExtractionCandidate> candidates = waitForCandidates(auditor, scanId);
        if (candidates.size() < 2) {
            return Optional.empty();
        }
        AutoCloseable filing = auditor.group().bind(new Auditor(quorum));
        AutoCloseable voter = auditor.group().bind(new Voter());
        try {
            auditor.vote().propose("scan-" + scanId,
                    "Which reading of scan " + scanId + " do we trust?",
                    candidates.stream().map(ExtractionCandidate::extractor).sorted().toList(),
                    quorum, Lease.of(Duration.ofMinutes(10)));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline) {
                Optional<FilingEntry> filed = auditor.intake().read(
                        Template.of(FilingEntry.class).where("scanId", eq(scanId)));
                if (filed.isPresent()) {
                    return filed;
                }
                sleep(150);
            }
            return Optional.empty();
        } finally {
            try {
                voter.close();
                filing.close();
            } catch (Exception ignored) {
                // best effort
            }
        }
    }


    private static List<ExtractionCandidate> waitForCandidates(Peer peer, String scanId) {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            List<ExtractionCandidate> candidates = peer.intake().readAll(
                    Template.of(ExtractionCandidate.class).where("scanId", eq(scanId)), 10);
            if (candidates.size() >= 2) {
                return candidates;
            }
            sleep(100);
        }
        return List.of();
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Runs the demo.
     *
     * @param args unused
     * @throws Exception on startup failure
     */
    public static void main(String[] args) throws Exception {
        System.out.println("intake-fleet: extract, vote on quality, file with provenance\n");
        Peer auditor = startPeer("auditor", 7511, 0);
        Peer careful = startPeer("extract-careful", 7512, 7511);
        Peer fast = startPeer("extract-fast", 7513, 7511);
        List<Peer> fleet = List.of(auditor, careful, fast);
        try (AutoCloseable a = runExtractor(careful, 0.95);
             AutoCloseable b = runExtractor(fast, 0.6)) {
            Thread.sleep(1500);

            auditor.intake().write(new ScanEntry("scan-001",
                    "smudged 1099: T1N-88-12?4567, amount 12,4??.??"),
                    Lease.of(Duration.ofMinutes(30)));
            System.out.println("scan-001 entered the intake space");

            FilingEntry filing = auditScan(auditor, "scan-001", 3).orElseThrow();
            System.out.println("\ncandidates were:");
            System.out.println("  extract-careful (0.95): TIN-88-1234567 / 12,400.00");
            System.out.println("  extract-fast    (0.60): TIN-88-1284567 / 12,4O0.OO");
            System.out.println("\nquorum filed: " + filing.taxpayerId() + " / "
                    + filing.amount() + " from " + filing.extractor()
                    + " [" + filing.approvedBy() + "]");
            System.out.println("\nevery stage is a signed entry: scan, candidates, "
                    + "ballots, filing. The provenance chain IS the space.");
        } finally {
            fleet.forEach(Peer::close);
        }
    }
}
