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
package ai.badmonkey.agentspaces.examples.signed;

import ai.badmonkey.agentspaces.agent.AgentBinder;
import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.SpaceRef;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.security.AgentCertificate;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.AgentIdentity;
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
import java.util.Optional;

/**
 * Example 13: signed agents. Two agents, an {@code auditor} and a {@code clerk},
 * each hold a <em>subordinate</em> identity: an Ed25519 key of their own that
 * their peer certifies (SPEC §4.2). Every finding they write is signed by the
 * agent's key and travels with the peer's certificate, so a reader anywhere in
 * the fleet can attribute the entry to the agent, not merely to the peer that
 * hosts it: {@link Space.Attestation#AGENT_ATTESTED}. A third agent, the
 * {@code desk}, writes the way every space always has, under the peer key, and
 * its findings read as {@link Space.Attestation#PEER_ASSERTED}. Nothing else
 * about the space changes; a peer that has never heard of certificates
 * replicates all three.
 *
 * <p>The first half seats each agent on a peer of its own and wires the
 * identity by hand ({@link #startAgent}). The second half is the one to copy:
 * {@link #startAnnotatedPeer} puts <em>both</em> agents on <em>one</em> peer as
 * plain {@code @AgentSpec} POJOs bound through the {@link AgentSpaces} facade
 * with renewing subordinate keys as its identity factory
 * ({@code identity.renewingSubordinate(name, ttl, clock)}, which re-certifies at
 * half-life, exactly as the starter's {@code agentspaces.identity.agent-keys=subordinate}). No {@code AgentIdentity}
 * appears in the agent classes: each {@code @SpaceRef} arrives as a per-agent
 * view of the shared replica, so each bean's findings are signed by its own key
 * and attributed to it everywhere (QA4 A4-7 phase 3). The certificate has a
 * lifetime: {@link #startAgent(String, int, int, Instant, Duration)} lets the
 * flow test issue one that has already lapsed and show that the agent can no
 * longer sign (SPEC v0.1.13 judges a certificate at the signing time).
 *
 * <p>Run: {@code mvn -q -pl examples/example-13-signed-agents exec:java}
 */
public final class SignedAgents {

    /** What an agent concluded about something. */
    public record Finding(String subject, String verdict) {
    }

    /** The replicated space every agent writes into. */
    public static final String FINDINGS = "findings";

    private static final String HOST = "127.0.0.1";
    private static final Lease FINDING_LEASE = Lease.of(Duration.ofHours(1));
    private static final Duration CERTIFICATE_TTL = Duration.ofHours(1);

    /** One assembled peer hosting one agent. {@code writer} is empty for a peer-signed agent. */
    public record Peer(String agentName, PeerIdentity identity, Optional<AgentIdentity> writer,
                       PeerNode node, GroupRuntime runtime, ReplicatedSpace findings) {

        /** Shuts the peer down. */
        public void close() {
            findings.close();
            node.close();
        }

        /** The certificate the peer issued for its agent, when the agent has a key of its own. */
        public Optional<AgentCertificate> certificate() {
            return writer.flatMap(AgentIdentity::certificate);
        }
    }

    private SignedAgents() {
    }

    /** The example's well-known group. */
    public static GroupAdvertisement group() {
        GroupId groupId = GroupId.fromFounding("signed-agents-v1".getBytes(StandardCharsets.UTF_8));
        return new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(365),
                "signed-agents", GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
    }

    /**
     * Seats an agent with a key of its own, certified from now for an hour.
     *
     * @param name     the agent's local name
     * @param port     the TCP port to listen on
     * @param seedPort an existing member's port, or 0 for the first
     * @return the running peer
     * @throws Exception if the port cannot be bound
     */
    public static Peer startAgent(String name, int port, int seedPort) throws Exception {
        return startAgent(name, port, seedPort, Instant.now(), CERTIFICATE_TTL);
    }

    /**
     * Seats an agent with a key of its own under a certificate of the given
     * validity. The peer signs the certificate; the agent key signs the records.
     *
     * @param name     the agent's local name
     * @param port     the TCP port to listen on
     * @param seedPort an existing member's port, or 0 for the first
     * @param issued   when the certificate is issued
     * @param ttl      how long it certifies the key
     * @return the running peer
     * @throws Exception if the port cannot be bound
     */
    public static Peer startAgent(String name, int port, int seedPort, Instant issued, Duration ttl)
            throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        Joined joined = join(identity, port, seedPort);
        // The agent's own key, vouched for by the peer: the one line that opts in.
        AgentIdentity writer = identity.subordinate(name, issued, ttl);
        ReplicatedSpace findings = ReplicatedSpace.builder(joined.runtime(), FINDINGS, identity, name)
                .writer(writer)
                .settleWindow(Duration.ofMillis(150))
                .build();
        joined.node().startTicking(Duration.ofMillis(250));
        return new Peer(name, identity, Optional.of(writer), joined.node(), joined.runtime(), findings);
    }

    /**
     * Seats an agent the way every space always has: records signed by the peer
     * key, attributed to {@code <peer>/<name>} on the peer's word.
     *
     * @param name     the agent's local name
     * @param port     the TCP port to listen on
     * @param seedPort an existing member's port, or 0 for the first
     * @return the running peer
     * @throws Exception if the port cannot be bound
     */
    public static Peer startPlainAgent(String name, int port, int seedPort) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        Joined joined = join(identity, port, seedPort);
        ReplicatedSpace findings = ReplicatedSpace.builder(joined.runtime(), FINDINGS, identity, name)
                .settleWindow(Duration.ofMillis(150))
                .build();
        joined.node().startTicking(Duration.ofMillis(250));
        return new Peer(name, identity, Optional.empty(), joined.node(), joined.runtime(), findings);
    }

    private record Joined(PeerNode node, GroupRuntime runtime) {
    }

    private static Joined join(PeerIdentity identity, int port, int seedPort) throws Exception {
        PeerNode node = PeerNode.builder(identity).build();
        node.listen(new TcpTransport(), HOST + ":" + port);
        List<PeerAdvertisement.Endpoint> seeds = seedPort == 0 ? List.of()
                : List.of(new PeerAdvertisement.Endpoint("tcp", HOST + ":" + seedPort, 0));
        return new Joined(node, node.joinGroup(group(),
                new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2), seeds));
    }

    // ------------------------------------------------ second half: annotated agents, one peer

    /** An auditing agent: an ordinary POJO. Its {@code findings} handle writes as "auditor". */
    @AgentSpec(name = "auditor", description = "Audits releases", goals = {"audit"})
    public static final class Auditor {
        @SpaceRef(FINDINGS)
        Space findings;

        /** Records a verdict as the auditor. */
        public void conclude(String subject, String verdict) {
            findings.write(new Finding(subject, verdict), FINDING_LEASE);
        }
    }

    /** A filing clerk: another POJO on the same peer, another identity. */
    @AgentSpec(name = "clerk", description = "Files invoices", goals = {"file"})
    public static final class Clerk {
        @SpaceRef(FINDINGS)
        Space findings;

        /** Records a verdict as the clerk. */
        public void conclude(String subject, String verdict) {
            findings.write(new Finding(subject, verdict), FINDING_LEASE);
        }
    }

    /** One peer hosting both annotated agents. */
    public record AnnotatedPeer(PeerIdentity identity, PeerNode node, AgentSpaces spaces,
                                ReplicatedSpace findings, Auditor auditor, Clerk clerk,
                                AgentBinder.Bound auditorBound, AgentBinder.Bound clerkBound) {

        /** Shuts the peer down. */
        public void close() {
            auditorBound.close();
            clerkBound.close();
            findings.close();
            node.close();
        }
    }

    /**
     * Seats the auditor and the clerk on one peer through the facade. The single
     * opt-in is the facade's identity factory; the agents are annotated POJOs
     * that know nothing about keys.
     *
     * @param port     the TCP port to listen on
     * @param seedPort an existing member's port, or 0 for the first
     * @return the running peer
     * @throws Exception if the port cannot be bound
     */
    public static AnnotatedPeer startAnnotatedPeer(int port, int seedPort) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        Joined joined = join(identity, port, seedPort);
        ReplicatedSpace findings = ReplicatedSpace.builder(joined.runtime(), FINDINGS, identity, "host")
                .settleWindow(Duration.ofMillis(150))
                .build();
        // The starter does exactly this from agentspaces.identity.agent-keys=subordinate.
        AgentSpaces spaces = new AgentSpaces(identity, InstantSource.system(),
                name -> identity.renewingSubordinate(name, java.time.Duration.ofHours(24), java.time.InstantSource.system()));
        AgentSpaces.GroupContext group = spaces.register("signed-agents", joined.runtime().id(),
                joined.runtime(), null);
        group.space(FINDINGS, findings);
        Auditor auditor = new Auditor();
        Clerk clerk = new Clerk();
        AgentBinder.Bound auditorBound = group.bind(auditor);
        AgentBinder.Bound clerkBound = group.bind(clerk);
        joined.node().startTicking(Duration.ofMillis(250));
        return new AnnotatedPeer(identity, joined.node(), spaces, findings, auditor, clerk,
                auditorBound, clerkBound);
    }

    /** Writes a finding as this peer's agent. */
    public static void conclude(Peer peer, String subject, String verdict) {
        peer.findings().write(new Finding(subject, verdict), FINDING_LEASE);
    }

    /**
     * The ledger as one replica sees it: every finding with who issued it and
     * how strongly the fleet can hold them to it.
     */
    public static List<Space.Issued<Finding>> ledger(Peer reader) {
        return reader.findings().readAllIssued(Template.of(Finding.class), 100);
    }

    /** One ledger line: the finding, the issuing agent, and its attestation. */
    public static String describe(Space.Issued<Finding> issued) {
        return issued.entry().subject() + ": " + issued.entry().verdict()
                + "  -- " + issued.issuer().encoded() + " (" + issued.attestation() + ")";
    }

    /**
     * Runs the demo.
     *
     * @param args unused
     * @throws Exception on startup failure
     */
    public static void main(String[] args) throws Exception {
        System.out.println("signed agents: two certified agent keys, one peer-signed desk\n");
        Peer auditor = startAgent("auditor", 7701, 0);
        Peer clerk = startAgent("clerk", 7702, 7701);
        Peer desk = startPlainAgent("desk", 7703, 7701);
        List<Peer> fleet = List.of(auditor, clerk, desk);
        try {
            Thread.sleep(2000); // membership settles
            conclude(auditor, "release-7.4.0", "no hardcoded credentials");
            conclude(clerk, "invoice-118", "paid in full");
            conclude(desk, "office", "closes at six");
            Thread.sleep(2000);
            System.out.println("the ledger, read at the desk:");
            ledger(desk).forEach(issued -> System.out.println("  " + describe(issued)));
            System.out.println("\ncertificates the peers issued:");
            fleet.forEach(peer -> peer.certificate().ifPresent(certificate ->
                    System.out.println("  " + certificate)));

            // Second half: both agents on one peer, annotated, no identity code.
            AnnotatedPeer both = startAnnotatedPeer(7704, 7701);
            try {
                Thread.sleep(2000);
                both.auditor().conclude("release-7.5.0", "signed off");
                both.clerk().conclude("invoice-119", "disputed");
                Thread.sleep(2000);
                System.out.println("\none peer, two annotated agents, as the desk reads them:");
                ledger(desk).stream().filter(i -> i.issuer().peer().equals(both.identity().peerId()))
                        .forEach(issued -> System.out.println("  " + describe(issued)));
                System.out.println("  cards: auditor key present = "
                        + both.auditorBound().card().attested() + ", clerk key present = "
                        + both.clerkBound().card().attested());
            } finally {
                both.close();
            }
        } finally {
            fleet.forEach(Peer::close);
        }
    }
}
