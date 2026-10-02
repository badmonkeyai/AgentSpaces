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
package ai.badmonkey.agentspaces.capabilities.vote;

import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.api.security.AgentCertificate;
import ai.badmonkey.agentspaces.api.spi.AgentIdentity;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.SpaceId;
import ai.badmonkey.agentspaces.space.crdt.Dot;
import ai.badmonkey.agentspaces.space.replicated.SpaceWire;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.capabilities.aggregate.PushSumAggregate;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.AdvertisementSigner;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.membership.MembershipAuthorizer;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

class VoteClusterTest {

    private static final Lease HOUR = Lease.of(Duration.ofHours(1));
    private static final List<String> OPTIONS = List.of("approve", "reject");

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zVote");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zVote", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "vote",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Voter(PeerNode node, PeerIdentity identity, ReplicatedSpace space,
                         VoteCapability vote, PushSumAggregate aggregate,
                         DiscoveryService discovery) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    /**
     * QA4 A4-4: a ballot is only countable when the voter it declares is the
     * agent the space attributes the write to. Constructing a capability with a
     * {@code self} the space can never write as produced a capability that
     * silently rejected its own ballots as forged: the tally stayed empty, the
     * quorum never closed, and nothing anywhere said why. It must refuse instead.
     */
    @Test
    void aVoterIdentityTheSpaceCannotWriteAsIsRefused() throws Exception {
        Voter a = newVoter("a", 1);

        assertThatThrownBy(() -> new VoteCapability(
                a.space(), a.identity().agent("somebody-else"), a.identity().peerId(), clock))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("somebody-else")
                .hasMessageContaining("voter");
    }

    /** QA4 A4-4: the matching identity is accepted, and its ballots count. */
    @Test
    void aVoterIdentityMatchingTheSpacesWriterIsAccepted() throws Exception {
        Voter a = newVoter("a", 1);
        VoteCapability sameAgent = new VoteCapability(
                a.space(), a.identity().agent("voter"), a.identity().peerId(), clock);

        sameAgent.propose("p", "Ship?", List.of("yes", "no"), 1, HOUR);
        sameAgent.castBallot("p", "yes", HOUR);

        assertThat(sameAgent.tally("p")).containsEntry("yes", 1);
        assertThat(sameAgent.decision("p")).hasValueSatisfying(
                d -> assertThat(d.winner()).isEqualTo("yes"));
    }

    /**
     * One peer offering votes over two spaces publishes two advertisements. The
     * capability runtime once keyed providers by capability type and the vote
     * advertisement id named only the peer, so the second registration silently
     * displaced the first and the fleet saw one vote space where there were two.
     */
    @Test
    void onePeerVotingInTwoSpacesAdvertisesBoth() throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(1).build();
        nodes.add(node);
        node.listen(network.register("a"), "a");
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        DiscoveryService discovery = new DiscoveryService(
                runtime, new AdCache(codec, clock), codec, identity.peerId());
        CapabilityRuntime capabilities = new CapabilityRuntime(runtime, discovery, identity);
        for (String spaceName : List.of("advisories", "decisions")) {
            ReplicatedSpace space = ReplicatedSpace.builder(runtime, spaceName, identity, "voter")
                    .clock(clock).settleWindow(Duration.ZERO).build();
            capabilities.register(new VoteCapability(
                    space, identity.agent("voter"), identity.peerId(), clock));
        }

        assertThat(capabilities.providersOf(VoteCapability.TYPE))
                .extracting(ad -> ad.binding())
                .containsExactlyInAnyOrder("space:advisories", "space:decisions");
        capabilities.close();
    }

    private Voter newVoter(String address, long seed, String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        ReplicatedSpace voteSpace = ReplicatedSpace.builder(runtime, "votes", identity, "voter")
                .clock(clock)
                .settleWindow(Duration.ZERO)
                .build();
        VoteCapability vote = new VoteCapability(
                voteSpace, identity.agent("voter"), identity.peerId(), clock);
        PushSumAggregate aggregate = new PushSumAggregate(
                new CapabilityPipes(runtime, codec), runtime.sampler(),
                identity.peerId(), codec, clock);
        DiscoveryService discovery = new DiscoveryService(
                runtime, new AdCache(codec, clock), codec, identity.peerId());
        nodes.add(node);
        return new Voter(node, identity, voteSpace, vote, aggregate, discovery);
    }

    /** Publishes a signed AgentCard for the voter's agent with the given TTL. */
    private void publishCard(Voter voter, Duration ttl) {
        AgentCard card = new AgentCard(
                "aspace://" + groupId.value() + "/agent/" + voter.identity().peerId().value(),
                voter.identity().peerId(), groupId, clock.instant(), ttl,
                voter.identity().agent("voter"), "a voter", List.of(), List.of(), List.of(),
                Map.of());
        voter.discovery().publish(new AdvertisementSigner().sign(card, voter.identity()));
    }

    /** SPEC §8 vote QUORUM: only members with a fresh AgentCard are the electorate; a third voter without a card does not count, and when the cards lapse the decision recomputes to open. */
    @Test
    void votersWithoutAFreshAgentCardDoNotCount() throws Exception {
        Voter a = newVoter("a", 1);
        Voter b = newVoter("b", 2, "a");
        Voter c = newVoter("c", 3, "a");
        tickAll(4);
        Duration cardTtl = Duration.ofMinutes(10);
        publishCard(a, cardTtl);
        publishCard(b, cardTtl);
        tickAll(3);
        List<Voter> voters = List.of(a, b, c);
        for (Voter voter : voters) {
            assertThat(voter.discovery().find(AgentCard.class, card -> true))
                    .as("both cards reached " + voter.node().peerId()).hasSize(2);
        }

        // Electorate-aware capabilities over the same vote space.
        List<VoteCapability> carded = voters.stream().map(v -> new VoteCapability(
                v.space(), v.identity().agent("voter"), v.identity().peerId(), clock,
                v.discovery())).toList();

        carded.get(0).propose("e1", "Rotate the group key?", OPTIONS, 2, HOUR);
        tickAll(2);
        a.vote().castBallot("e1", "approve", HOUR);
        b.vote().castBallot("e1", "approve", HOUR);
        c.vote().castBallot("e1", "reject", HOUR); // c holds no card
        tickAll(2);

        for (int i = 0; i < voters.size(); i++) {
            // The default electorate (the plain capability) still counts all three.
            assertThat(voters.get(i).vote().tally("e1"))
                    .containsEntry("approve", 2).containsEntry("reject", 1);
            // The fresh-card electorate counts two: c's ballot is not in the electorate.
            assertThat(carded.get(i).tally("e1"))
                    .containsEntry("approve", 2).containsEntry("reject", 0);
            assertThat(carded.get(i).decision("e1")).isPresent()
                    .get().satisfies(d -> assertThat(d.winner()).isEqualTo("approve"));
        }

        // The cards lapse (the ballots, leased for an hour, are still there):
        // nobody is in the electorate any more, so the decision recomputes to open.
        clock.advance(cardTtl.plusMinutes(1));
        for (int i = 0; i < voters.size(); i++) {
            assertThat(voters.get(i).discovery().find(AgentCard.class, card -> true)).isEmpty();
            assertThat(carded.get(i).tally("e1"))
                    .containsEntry("approve", 0).containsEntry("reject", 0);
            assertThat(carded.get(i).decision("e1")).isEmpty();
            assertThat(voters.get(i).vote().decision("e1"))
                    .as("the default electorate is unaffected").isPresent();
        }
    }

    // ------------------------------------------------ QA4 A4-7: the sybil case

    /** Mirrors ReplicatedSpace's private SignView (TECH-SPEC §7.2). */
    record SignView(EntryId entryId, SpaceId spaceId, String type, byte[] payload,
                    String payloadRef, AgentId issuer, HlcTimestamp issued,
                    Map<String, String> tags) {
    }

    /** Mirrors ReplicatedSpace's private StateSignView (SPEC §11a.4). */
    record StateSignView(SpaceId spaceId, EntryId entryId, List<Dot> adds, List<Dot> removes,
                         HlcTimestamp leaseStamp, LeaseInfo leaseValue, boolean completed,
                         // v0.1.13 mirror of the agent-signer fields, omitted when null
                         @com.fasterxml.jackson.annotation.JsonInclude(
                                 com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                         String signer,
                         @com.fasterxml.jackson.annotation.JsonInclude(
                                 com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                         HlcTimestamp signedAt) {
    }

    private int rawCounter = 100;

    /**
     * Writes a ballot the way a raw client can: a record whose issuer is any
     * name under {@code from}'s PeerId, signed by {@code writer} (the peer key
     * itself, or an agent key with its certificate), gossiped straight onto the
     * vote stream. This bypasses the one-writer-per-handle rule a Java
     * {@code ReplicatedSpace} imposes and is exactly what QA3 §8 A4-7 describes.
     */
    private void gossipBallot(Voter from, AgentIdentity writer, String proposalId, String option) {
        AgentId issuer = writer.id();
        EntryId entryId = EntryId.newId();
        SpaceId spaceId = from.space().id();
        String type = VoteCapability.Ballot.class.getName() + "#v1";
        byte[] payload = codec.toBytes(new VoteCapability.Ballot(proposalId, option, issuer.encoded()));
        HlcTimestamp stamp = new HlcTimestamp(clock.millis(), 0, from.identity().peerId().value());
        LeaseInfo lease = new LeaseInfo(issuer, clock.millis() + Duration.ofHours(1).toMillis(),
                LeaseKind.WRITE);
        byte[] recordSig = writer.sign(codec.toBytes(new SignView(entryId, spaceId, type, payload,
                null, issuer, stamp, Map.of())));
        EntryRecord record = new EntryRecord(entryId, spaceId, type, payload, null, issuer, stamp,
                lease, Map.of(), recordSig);
        List<Dot> adds = List.of(new Dot(from.identity().peerId().value(), rawCounter++));
        // SPEC §11a.4 v0.1.13: an attested writer signs its own state transitions.
        String signer = writer.isSubordinate() ? issuer.encoded() : null;
        HlcTimestamp signedAt = writer.isSubordinate() ? stamp : null;
        byte[] signView = codec.toBytes(new StateSignView(spaceId, entryId,
                adds, List.of(), stamp, lease, false, signer, signedAt));
        byte[] stateSig = writer.isSubordinate() ? writer.sign(signView) : from.identity().sign(signView);
        SpaceWire.EntryStateDto dto = new SpaceWire.EntryStateDto(record,
                from.identity().rawPublicKey(), adds, List.of(), stamp, lease, false, stateSig,
                writer.certificate().orElse(null), writer.certificate().orElse(null), signer, signedAt);
        from.node().group(groupId).orElseThrow().gossip().publish("space:votes",
                "ballot-" + entryId.value(), codec.toBytes(new SpaceWire.Delta(dto, null, null)));
    }

    /**
     * QA3 §8 A4-7, written before phase 2: one authorized peer writes three
     * ballots under three self-asserted names. Authorization is per PeerId and
     * counting was per AgentId, so the three counted as three voters and a
     * quorum of three closed on one member's say-so. Under the granularity rule
     * a peer-level authorizer yields one counted ballot per peer, however the
     * peer is seated, and the quorum stays open.
     */
    @Test
    void onePeerWritingThreeNamesCastsOneVote() throws Exception {
        Voter a = newVoter("a", 1);
        Voter sybil = newVoter("sybil", 2, "a");
        tickAll(4);
        a.vote().propose("p-sybil", "Approve?", OPTIONS, 3, HOUR);
        tickAll(2);

        for (String name : List.of("one", "two", "three")) {
            gossipBallot(sybil, sybil.identity().agentIdentity(name), "p-sybil", "approve");
        }
        tickAll(4);

        for (Voter replica : List.of(a, sybil)) {
            assertThat(replica.vote().tally("p-sybil"))
                    .as("one peer, one counted ballot, at " + replica.identity().peerId().display())
                    .containsEntry("approve", 1);
            assertThat(replica.vote().decision("p-sybil"))
                    .as("a quorum of three is not one peer three times").isEmpty();
        }
    }

    /** A vote capability at {@code a} whose authorizer grants exactly the given agents for VOTE. */
    private VoteCapability countingAgents(Voter a, Set<AgentId> granted) {
        GroupRuntime runtime = a.node().group(groupId).orElseThrow();
        MembershipAuthorizer authorizer = new MembershipAuthorizer(runtime.membership(),
                a.identity().peerId(), Map.of(Authorizer.Operation.VOTE, Set.of()),
                Map.of(Authorizer.Operation.VOTE, granted));
        return new VoteCapability(a.space(), a.identity().agent("voter"), a.identity().peerId(),
                clock, VoteCapability.ANY_AUTHENTICATED_ISSUER, authorizer);
    }

    /**
     * QA4 A4-7 phase 2, the feature: with agent-level grants the authorizer
     * speaks at AGENT granularity, so three granted agents on one peer, each
     * signing with its own certified key, are three counted voters and a
     * quorum of three closes on one node. A fourth agent on that peer, attested
     * but ungranted, counts for nothing; so does a granted name whose ballot is
     * merely peer-asserted. The decision says which rule produced it.
     */
    @Test
    void grantedAttestedAgentsOnOnePeerEachCountOnce() throws Exception {
        Voter a = newVoter("a", 1);
        Voter host = newVoter("host", 2, "a");
        tickAll(4);
        AgentIdentity one = host.identity().subordinate("one", clock.instant(), Duration.ofHours(1));
        AgentIdentity two = host.identity().subordinate("two", clock.instant(), Duration.ofHours(1));
        AgentIdentity three = host.identity().subordinate("three", clock.instant(), Duration.ofHours(1));
        AgentIdentity ungranted = host.identity().subordinate("four", clock.instant(), Duration.ofHours(1));
        VoteCapability counting = countingAgents(a, Set.of(one.id(), two.id(), three.id(),
                host.identity().agent("asserted")));
        assertThat(counting.granularity()).isEqualTo(Authorizer.Granularity.AGENT);

        a.vote().propose("p-agents", "Approve?", OPTIONS, 3, HOUR);
        tickAll(2);
        gossipBallot(host, one, "p-agents", "approve");
        gossipBallot(host, two, "p-agents", "approve");
        gossipBallot(host, ungranted, "p-agents", "reject");                       // attested, not granted
        gossipBallot(host, host.identity().agentIdentity("asserted"), "p-agents", "reject"); // granted, not attested
        tickAll(4);

        assertThat(counting.tally("p-agents")).containsEntry("approve", 2).containsEntry("reject", 0);
        assertThat(counting.decision("p-agents")).as("two of three").isEmpty();

        gossipBallot(host, three, "p-agents", "approve");
        tickAll(4);
        assertThat(counting.tally("p-agents")).containsEntry("approve", 3);
        assertThat(counting.decision("p-agents")).hasValueSatisfying(d -> {
            assertThat(d.winner()).isEqualTo("approve");
            assertThat(d.granularity()).isEqualTo(Authorizer.Granularity.AGENT);
        });
        // The same ballots at PEER granularity: one peer, one counted ballot,
        // whichever of the five was read first; a quorum of three stays open.
        Map<String, Integer> perPeer = host.vote().tally("p-agents");
        assertThat(perPeer.values().stream().mapToInt(Integer::intValue).sum()).isEqualTo(1);
        assertThat(host.vote().decision("p-agents")).isEmpty();
    }

    /**
     * QA4 A4-7 phase 2: under AGENT granularity the electorate predicate is a
     * liveness filter, not an eligibility check — a card holder the authorizer
     * does not name is not counted.
     */
    @Test
    void aFreshCardAloneDoesNotAdmitAVoterUnderAgentGranularity() throws Exception {
        Voter a = newVoter("a", 1);
        Voter b = newVoter("b", 2, "a");
        tickAll(4);
        publishCard(a, Duration.ofHours(1));
        publishCard(b, Duration.ofHours(1));
        tickAll(3);
        AgentIdentity granted = a.identity().subordinate("named", clock.instant(), Duration.ofHours(1));
        GroupRuntime runtime = a.node().group(groupId).orElseThrow();
        MembershipAuthorizer authorizer = new MembershipAuthorizer(runtime.membership(),
                a.identity().peerId(), Map.of(Authorizer.Operation.VOTE, Set.of()),
                Map.of(Authorizer.Operation.VOTE, Set.of(granted.id())));
        VoteCapability counting = new VoteCapability(a.space(), a.identity().agent("voter"),
                a.identity().peerId(), clock, VoteCapability.freshAgentCards(a.discovery()), authorizer);

        a.vote().propose("p-card", "Approve?", OPTIONS, 1, HOUR);
        tickAll(2);
        b.vote().castBallot("p-card", "reject", HOUR);   // carded, peer-asserted, ungranted
        tickAll(4);
        assertThat(counting.tally("p-card")).containsEntry("reject", 0);
        assertThat(counting.decision("p-card")).isEmpty();
    }

    /** QA4 A4-7 phase 2: a voter an agent-level authorizer does not name refuses to cast. */
    @Test
    void anUngrantedAgentRefusesToCastUnderAgentGranularity() throws Exception {
        Voter a = newVoter("a", 1);
        tickAll(2);
        VoteCapability counting = countingAgents(a,
                Set.of(a.identity().agent("somebody-granted")));
        a.vote().propose("p-refuse", "Approve?", OPTIONS, 1, HOUR);
        assertThatThrownBy(() -> counting.castBallot("p-refuse", "approve", HOUR))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("voter")
                .hasMessageContaining("not permitted VOTE");
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // anti-entropy runs once per gossip period on the node clock
        }
    }

    @Test
    void quorumVoteDecidesIdenticallyOnEveryReplica() throws Exception {
        Voter a = newVoter("a", 1);
        Voter b = newVoter("b", 2, "a");
        Voter c = newVoter("c", 3, "a");
        tickAll(4);

        a.vote().propose("p1", "Adopt the auction strategy for tasks?", OPTIONS, 3, HOUR);
        assertThat(b.vote().proposal("p1")).isPresent();

        a.vote().castBallot("p1", "approve", HOUR);
        assertThat(a.vote().decision("p1")).isEmpty(); // quorum of 3 not reached
        b.vote().castBallot("p1", "approve", HOUR);
        c.vote().castBallot("p1", "reject", HOUR);

        for (Voter voter : List.of(a, b, c)) {
            var decision = voter.vote().decision("p1");
            assertThat(decision).isPresent();
            assertThat(decision.get().winner()).isEqualTo("approve");
            assertThat(decision.get().tally())
                    .containsEntry("approve", 2)
                    .containsEntry("reject", 1);
        }
    }

    /**
     * SPEC §5.6 v0.1.13 (review M-13): revocation applies at recount. A voter
     * whose agent is revoked after the decision stops counting, so the decision
     * reopens on every replica; its ballot entry itself stays held.
     */
    @Test
    void aRevokedVotersBallotStopsCountingAndTheDecisionReopens() throws Exception {
        Voter a = newVoter("a", 1);
        Voter b = newVoter("b", 2, "a");
        Voter c = newVoter("c", 3, "a");
        tickAll(4);
        a.vote().propose("p2", "Revocable?", OPTIONS, 3, HOUR);
        a.vote().castBallot("p2", "approve", HOUR);
        b.vote().castBallot("p2", "approve", HOUR);
        c.vote().castBallot("p2", "reject", HOUR);
        tickAll(4);
        assertThat(a.vote().decision("p2")).isPresent();

        var bVoter = b.space().writer().orElseThrow();
        assertThat(b.node().group(groupId).orElseThrow().revokeAgent(bVoter,
                ai.badmonkey.agentspaces.api.ad.CredentialRevocation.PRIVILEGE_WITHDRAWN)).isPresent();
        tickAll(4);
        for (Voter voter : List.of(a, b, c)) {
            assertThat(voter.vote().tally("p2")).containsEntry("approve", 1).containsEntry("reject", 1);
            assertThat(voter.vote().decision("p2")).as("below quorum again").isEmpty();
        }
    }

    /**
     * gate2-review G2-3: an {@link Authorizer} narrows the electorate, asked
     * for {@code VOTE} in the vote space's scope, so an identity provider can
     * name who counts in a QUORUM decision.
     */
    @Test
    void anAuthorizerNarrowsTheElectorate() throws Exception {
        Voter a = newVoter("a", 1);
        Voter b = newVoter("b", 2, "a");
        Voter c = newVoter("c", 3, "a");
        tickAll(4);
        a.vote().propose("p5", "Ship the release?", OPTIONS, 3, HOUR);
        assertThat(c.vote().proposal("p5")).isPresent();

        a.vote().castBallot("p5", "approve", HOUR);
        b.vote().castBallot("p5", "approve", HOUR);
        c.vote().castBallot("p5", "reject", HOUR);
        tickAll(4);

        // Every ballot is authentic, so the unnarrowed tally counts all three.
        assertThat(a.vote().tally("p5"))
                .containsEntry("approve", 2).containsEntry("reject", 1);
        assertThat(a.vote().decision("p5")).isPresent();

        // A replica whose authorizer refuses c discards c's ballot, and the
        // quorum of three is no longer met. The authorizer is asked for VOTE
        // in the vote space's scope.
        VoteCapability narrowed = new VoteCapability(a.space(), a.identity().agent("voter"),
                a.identity().peerId(), clock, VoteCapability.ANY_AUTHENTICATED_ISSUER,
                (peer, operation, scope) -> {
                    assertThat(operation).isEqualTo(Authorizer.Operation.VOTE);
                    assertThat(scope).isEqualTo(a.space().name());
                    return !peer.equals(c.identity().peerId());
                });
        assertThat(narrowed.tally("p5"))
                .containsEntry("approve", 2).containsEntry("reject", 0);
        assertThat(narrowed.decision("p5"))
                .as("two of a three-voter quorum is short").isEmpty();
    }

    /** gate2-review G2-3: a peer the authorizer refuses will not cast a ballot
     * every tally would discard. */
    @Test
    void aPeerThatMayNotVoteRefusesToCastABallot() throws Exception {
        Voter a = newVoter("a", 1);
        tickAll(2);
        a.vote().propose("p6", "Ship the release?", OPTIONS, 2, HOUR);

        VoteCapability refused = new VoteCapability(a.space(), a.identity().agent("voter"),
                a.identity().peerId(), clock, VoteCapability.ANY_AUTHENTICATED_ISSUER,
                (peer, operation, scope) -> false);

        assertThatThrownBy(() -> refused.castBallot("p6", "approve", HOUR))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("VOTE");
        assertThat(a.vote().tally("p6")).containsEntry("approve", 0);
    }

    @Test
    void forgedAndImpersonatedBallotsAreDiscardedAtTally() throws Exception {
        Voter a = newVoter("a", 1);
        Voter b = newVoter("b", 2, "a");
        Voter evil = newVoter("evil", 3, "a");
        tickAll(4);
        a.vote().propose("p3", "Approve the deploy?", OPTIONS, 3, HOUR);
        assertThat(evil.vote().proposal("p3")).isPresent();

        // Ballot stuffing under invented voter ids, plus a ballot impersonating
        // b — written straight to the space, bypassing castBallot's honest
        // self-stamping. Every one of them is signed by evil's space machinery,
        // so the tally sees a voter field that mismatches the authenticated
        // issuer and discards it (ASF-006).
        String victim = b.identity().agent("voter").encoded();
        for (int i = 0; i < 5; i++) {
            evil.space().write(new VoteCapability.Ballot("p3", "reject", "ghost-" + i), HOUR);
        }
        evil.space().write(new VoteCapability.Ballot("p3", "reject", victim), HOUR);
        tickAll(4);
        assertThat(a.vote().tally("p3")).containsEntry("approve", 0).containsEntry("reject", 0);
        assertThat(a.vote().decision("p3")).as("stuffed quorum is not reached").isEmpty();

        // The victim's real ballot still counts as cast, not as the forgery
        // pre-wrote it; evil's own honest ballot counts once like anyone's.
        b.vote().castBallot("p3", "approve", HOUR);
        a.vote().castBallot("p3", "approve", HOUR);
        evil.vote().castBallot("p3", "reject", HOUR);
        tickAll(4);
        for (Voter voter : List.of(a, b, evil)) {
            assertThat(voter.vote().tally("p3"))
                    .containsEntry("approve", 2)
                    .containsEntry("reject", 1);
        }
    }

    @Test
    void repeatBallotsBySameVoterCountOnce() throws Exception {
        Voter a = newVoter("a", 1);
        Voter b = newVoter("b", 2, "a");
        tickAll(3);
        a.vote().propose("p2", "Ship it?", OPTIONS, 2, HOUR);

        a.vote().castBallot("p2", "approve", HOUR);
        a.vote().castBallot("p2", "approve", HOUR); // re-cast: harmless
        assertThat(a.vote().decision("p2")).isEmpty(); // still one distinct voter

        b.vote().castBallot("p2", "approve", HOUR);
        assertThat(b.vote().decision("p2")).isPresent();
        assertThat(a.vote().tally("p2")).containsEntry("approve", 2);
    }

    @Test
    void invalidBallotsAreRejectedLocally() throws Exception {
        Voter a = newVoter("a", 1);
        a.vote().propose("p3", "Pick one", OPTIONS, 1, HOUR);

        assertThatThrownBy(() -> a.vote().castBallot("p3", "abstain-hard", HOUR))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> a.vote().castBallot("unknown", "approve", HOUR))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> a.vote().propose("p4", "?", List.of("only"), 1, HOUR))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void tieBreaksLexicographicallyEverywhere() throws Exception {
        Voter a = newVoter("a", 1);
        Voter b = newVoter("b", 2, "a");
        tickAll(3);
        a.vote().propose("p5", "Split decision", OPTIONS, 2, HOUR);
        a.vote().castBallot("p5", "reject", HOUR);
        b.vote().castBallot("p5", "approve", HOUR);

        assertThat(a.vote().decision("p5").orElseThrow().winner()).isEqualTo("approve");
        assertThat(b.vote().decision("p5").orElseThrow().winner()).isEqualTo("approve");
    }

    /** SPEC §8 vote QUORUM: the decision closes exactly when distinct voters reach the quorum, on every replica. */
    @Test
    void decisionClosesExactlyAtTheQuorumBoundary() throws Exception {
        Voter a = newVoter("a", 1);
        Voter b = newVoter("b", 2, "a");
        Voter c = newVoter("c", 3, "a");
        Voter d = newVoter("d", 4, "a");
        List<Voter> fleet = List.of(a, b, c, d);
        tickAll(4);

        a.vote().propose("q", "Quorum of three among four?", OPTIONS, 3, HOUR);
        tickAll(2);

        a.vote().castBallot("q", "approve", HOUR);
        tickAll(2);
        for (Voter voter : fleet) {
            assertThat(voter.vote().decision("q")).as("1 of 3").isEmpty();
        }

        b.vote().castBallot("q", "reject", HOUR);
        tickAll(2);
        for (Voter voter : fleet) {
            assertThat(voter.vote().decision("q")).as("2 of 3: one short").isEmpty();
        }

        c.vote().castBallot("q", "approve", HOUR);
        tickAll(2);
        for (Voter voter : fleet) {
            var decision = voter.vote().decision("q");
            assertThat(decision).as("3 of 3 closes").isPresent();
            assertThat(decision.get().winner()).isEqualTo("approve");
            assertThat(decision.get().tally()).containsEntry("approve", 2).containsEntry("reject", 1);
        }

        // A late ballot after closure is still counted by the recomputable tally,
        // and the decision stays closed everywhere.
        d.vote().castBallot("q", "approve", HOUR);
        tickAll(2);
        for (Voter voter : fleet) {
            var decision = voter.vote().decision("q");
            assertThat(decision).isPresent();
            assertThat(decision.get().winner()).isEqualTo("approve");
            assertThat(decision.get().tally().values().stream().mapToInt(Integer::intValue).sum())
                    .isEqualTo(4);
        }
    }

    /** SPEC §8 vote QUORUM: a quorum larger than the electorate never closes, but the tally stays auditable. */
    @Test
    void quorumLargerThanTheElectorateNeverCloses() throws Exception {
        Voter a = newVoter("a", 1);
        Voter b = newVoter("b", 2, "a");
        tickAll(3);

        a.vote().propose("big", "Needs five of two?", OPTIONS, 5, HOUR);
        a.vote().castBallot("big", "approve", HOUR);
        b.vote().castBallot("big", "approve", HOUR);
        tickAll(4);

        for (Voter voter : List.of(a, b)) {
            assertThat(voter.vote().decision("big")).isEmpty();
            assertThat(voter.vote().tally("big")).containsEntry("approve", 2).containsEntry("reject", 0);
        }
    }

    /** SPEC §8 vote QUORUM: the quorum is a positive count of distinct voters; zero or negative is rejected. */
    @Test
    void proposeRejectsNonPositiveQuorum() throws Exception {
        Voter a = newVoter("a", 1);

        assertThatThrownBy(() -> a.vote().propose("z0", "?", OPTIONS, 0, HOUR))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> a.vote().propose("z1", "?", OPTIONS, -1, HOUR))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(a.vote().proposal("z0")).isEmpty();
        assertThat(a.vote().proposal("z1")).isEmpty();
    }

    /** SPEC §8 vote QUORUM: an authentic ballot for an option not on the proposal is spoiled — it neither counts toward the quorum nor invents an option. */
    @Test
    void offBallotOptionsWrittenDirectlyDoNotCount() throws Exception {
        Voter a = newVoter("a", 1);
        Voter b = newVoter("b", 2, "a");
        tickAll(3);
        a.vote().propose("p6", "Spoiled ballots?", OPTIONS, 2, HOUR);
        assertThat(b.vote().proposal("p6")).isPresent();

        // b bypasses castBallot's local option check and writes an honest-identity
        // ballot for an option the proposal never offered.
        b.space().write(new VoteCapability.Ballot("p6", "bogus",
                b.identity().agent("voter").encoded()), HOUR);
        a.vote().castBallot("p6", "approve", HOUR);
        tickAll(4);

        for (Voter voter : List.of(a, b)) {
            assertThat(voter.vote().tally("p6")).doesNotContainKey("bogus")
                    .containsEntry("approve", 1).containsEntry("reject", 0);
            assertThat(voter.vote().decision("p6")).as("spoiled ballot is not a voter").isEmpty();
        }

        // b's genuine ballot counts once, like anyone's.
        b.vote().castBallot("p6", "reject", HOUR);
        tickAll(4);
        for (Voter voter : List.of(a, b)) {
            var decision = voter.vote().decision("p6");
            assertThat(decision).isPresent();
            assertThat(decision.get().tally()).containsEntry("approve", 1).containsEntry("reject", 1)
                    .doesNotContainKey("bogus");
        }
    }

    /** SPEC §8 vote MAJORITY_GOSSIP: the push-sum estimates are the preference shares themselves, not just a leader. */
    @Test
    void majorityGossipEstimatesTheSharesNotJustTheLeader() throws Exception {
        Voter a = newVoter("a", 1);
        Voter b = newVoter("b", 2, "a");
        Voter c = newVoter("c", 3, "a");
        Voter d = newVoter("d", 4, "a");
        tickAll(5);
        List<Voter> fleet = List.of(a, b, c, d);

        // 3:1 — every member's estimate approximates the 0.75 / 0.25 split.
        VoteCapability.castPreference(a.aggregate(), "split", OPTIONS, "approve");
        VoteCapability.castPreference(b.aggregate(), "split", OPTIONS, "approve");
        VoteCapability.castPreference(c.aggregate(), "split", OPTIONS, "approve");
        VoteCapability.castPreference(d.aggregate(), "split", OPTIONS, "reject");
        // 2:2 — a dead heat: shares are 0.5 each and the leader is whichever the
        // approximation favours; the mode is honest that it cannot decide.
        VoteCapability.castPreference(a.aggregate(), "tie", OPTIONS, "approve");
        VoteCapability.castPreference(b.aggregate(), "tie", OPTIONS, "approve");
        VoteCapability.castPreference(c.aggregate(), "tie", OPTIONS, "reject");
        VoteCapability.castPreference(d.aggregate(), "tie", OPTIONS, "reject");

        for (int round = 0; round < 40; round++) {
            fleet.forEach(v -> v.aggregate().tick());
        }

        for (Voter voter : fleet) {
            assertThat(voter.aggregate().estimate("vote:split:approve").orElseThrow())
                    .isCloseTo(0.75, within(0.02));
            assertThat(voter.aggregate().estimate("vote:split:reject").orElseThrow())
                    .isCloseTo(0.25, within(0.02));
            assertThat(VoteCapability.leader(voter.aggregate(), "split", OPTIONS)).contains("approve");

            assertThat(voter.aggregate().estimate("vote:tie:approve").orElseThrow())
                    .isCloseTo(0.5, within(0.02));
            assertThat(voter.aggregate().estimate("vote:tie:reject").orElseThrow())
                    .isCloseTo(0.5, within(0.02));
            assertThat(VoteCapability.leader(voter.aggregate(), "tie", OPTIONS)).isPresent();
        }
    }

    @Test
    void majorityGossipConvergesOnTheLeader() throws Exception {
        Voter a = newVoter("a", 1);
        Voter b = newVoter("b", 2, "a");
        Voter c = newVoter("c", 3, "a");
        Voter d = newVoter("d", 4, "a");
        tickAll(5);

        VoteCapability.castPreference(a.aggregate(), "mood", OPTIONS, "approve");
        VoteCapability.castPreference(b.aggregate(), "mood", OPTIONS, "approve");
        VoteCapability.castPreference(c.aggregate(), "mood", OPTIONS, "approve");
        VoteCapability.castPreference(d.aggregate(), "mood", OPTIONS, "reject");

        List<Voter> fleet = List.of(a, b, c, d);
        for (int round = 0; round < 40; round++) {
            fleet.forEach(v -> v.aggregate().tick());
        }

        for (Voter voter : fleet) {
            assertThat(VoteCapability.leader(voter.aggregate(), "mood", OPTIONS))
                    .contains("approve");
        }
    }
}
