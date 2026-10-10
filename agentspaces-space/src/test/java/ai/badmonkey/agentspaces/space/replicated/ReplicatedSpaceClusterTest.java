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
package ai.badmonkey.agentspaces.space.replicated;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.ad.SignedAdvertisement;
import ai.badmonkey.agentspaces.api.ad.SpaceAdvertisement;
import ai.badmonkey.agentspaces.api.error.LeaseExpiredException;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.space.SpaceAdmissionException;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.AdvertisementSigner;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.gossip.ReconcilableState;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.ConsistencyHint;
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.security.AgentCertificate;
import ai.badmonkey.agentspaces.api.spi.AgentIdentity;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.test.Fixtures.FindingEntry;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;
import static ai.badmonkey.agentspaces.api.space.Matchers.gte;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The full Layer 0-3 stack on a SimNetwork: replicated writes, LEASE_RACE
 * arbitration across a partition, kill tolerance, and convergence. Settle windows
 * are zero and ticks are manual, so every run is deterministic.
 */
class ReplicatedSpaceClusterTest {

    private static final Lease MINUTES_30 = Lease.of(Duration.ofMinutes(30));
    private static final Lease MINUTES_10 = Lease.of(Duration.ofMinutes(10));

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zFleet");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zFleet", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "fleet",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(PeerNode node, ReplicatedSpace space) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Peer newPeer(String address, long seed, String... seedAddresses) throws IOException {
        return newPeer(address, seed, Duration.ZERO, seedAddresses);
    }

    private Peer newPeer(String address, long seed, Duration settleWindow,
                         String... seedAddresses) throws IOException {
        return newPeer(address, seed, settleWindow, builder -> { }, seedAddresses);
    }

    private Peer newPeer(String address, long seed, Duration settleWindow,
                         Consumer<ReplicatedSpace.Builder> customize,
                         String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        ReplicatedSpace.Builder builder = ReplicatedSpace.builder(runtime, "tasks", identity, "worker")
                .clock(clock)
                .settleWindow(settleWindow);
        customize.accept(builder);
        ReplicatedSpace space = builder.build();
        nodes.add(node);
        return new Peer(node, space);
    }

    /** A peer whose space writes as the identity {@code writerOf} derives from the peer's own. */
    private Peer newPeer(String address, long seed,
                         java.util.function.Function<PeerIdentity, AgentIdentity> writerOf,
                         String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "tasks", identity, "worker")
                .clock(clock)
                .settleWindow(Duration.ZERO)
                .writer(writerOf.apply(identity))
                .build();
        nodes.add(node);
        return new Peer(node, space);
    }

    // ------------------------------------------------ QA4 A4-7 phase 3: the per-agent write view

    /**
     * QA4 A4-7 phase 3, written before {@code as(...)} existed: two views of one
     * replica are two writers everywhere. Each view's records are issued and
     * signed by its own agent, attested, and every replica attributes them so.
     */
    @Test
    void twoViewsOnOneNodeAreTwoWritersEverywhere() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        tickAll(4);
        AgentIdentity auditor = a.node().identity().subordinate("auditor", clock.instant(), Duration.ofHours(1));
        AgentIdentity clerk = a.node().identity().subordinate("clerk", clock.instant(), Duration.ofHours(1));
        Space asAuditor = a.space().as(auditor);
        Space asClerk = a.space().as(clerk);
        assertThat(asAuditor.writer()).contains(auditor.id());
        assertThat(asClerk.writer()).contains(clerk.id());
        assertThat(asAuditor.id()).isEqualTo(a.space().id());

        asAuditor.write(new TaskEntry("audited", 1), MINUTES_30);
        asClerk.write(new TaskEntry("filed", 2), MINUTES_30);
        a.space().write(new TaskEntry("plain", 3), MINUTES_30);
        tickAll(4);

        for (Space replica : List.of(a.space(), asAuditor, b.space())) {
            Map<String, Space.Issued<TaskEntry>> byTopic = replica
                    .readAllIssued(Template.of(TaskEntry.class), 10).stream()
                    .collect(java.util.stream.Collectors.toMap(i -> i.entry().topic(), i -> i));
            assertThat(byTopic).as("all three at " + replica).hasSize(3);
            assertThat(byTopic.get("audited").issuer()).isEqualTo(auditor.id());
            assertThat(byTopic.get("audited").attestation()).isEqualTo(Space.Attestation.AGENT_ATTESTED);
            assertThat(byTopic.get("filed").issuer()).isEqualTo(clerk.id());
            assertThat(byTopic.get("filed").attestation()).isEqualTo(Space.Attestation.AGENT_ATTESTED);
            assertThat(byTopic.get("plain").issuer()).isEqualTo(a.node().identity().agent("worker"));
            assertThat(byTopic.get("plain").attestation()).isEqualTo(Space.Attestation.PEER_ASSERTED);
        }
        // One replica, one clock: the views share the node's HLC and its digest.
        assertThat(a.space().knownEntries()).isEqualTo(3);
        assertThat(a.space().as(a.node().identity().agentIdentity("someone")).id())
                .isEqualTo(a.space().id());
    }

    /** QA4 A4-7 phase 3: a view acts as another peer's agent for nobody. */
    @Test
    void aViewMustBelongToThisPeer() throws Exception {
        Peer a = newPeer("a", 1);
        PeerIdentity stranger = PeerIdentity.generate();
        assertThatThrownBy(() -> a.space().as(stranger.agentIdentity("intruder")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("intruder");
    }

    /** QA4 A4-7 phase 3: admission under ALLOWLIST is judged per view identity on one node. */
    @Test
    void admissionIsJudgedPerViewOnOneNode() throws Exception {
        PeerIdentity host = PeerIdentity.generate();
        AgentIdentity admitted = host.subordinate("admitted", clock.instant(), Duration.ofHours(1));
        AgentIdentity refused = host.subordinate("refused", clock.instant(), Duration.ofHours(1));
        PeerNode node = PeerNode.builder(host).clock(clock).randomSeed(7).build();
        node.listen(network.register("host"), "host");
        nodes.add(node);
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "tasks", host, "worker")
                .clock(clock)
                .admission(ai.badmonkey.agentspaces.api.ad.SpaceAdvertisement.Admission.ALLOWLIST,
                        java.util.Set.of(admitted.id()))
                .build();

        space.as(admitted).write(new TaskEntry("in", 1), MINUTES_30);
        assertThatThrownBy(() -> space.as(refused).write(new TaskEntry("out", 2), MINUTES_30))
                .isInstanceOf(SpaceAdmissionException.class)
                .hasMessageContaining("refused");
        assertThat(space.readAll(Template.of(TaskEntry.class), 10)).hasSize(1);
    }

    /** QA4 A4-7 phase 3: a take through a view names the view's agent as holder, and its sibling cannot complete it. */
    @Test
    void aViewTakesAndCompletesAsItsOwnAgent() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        tickAll(4);
        AgentIdentity taker = a.node().identity().subordinate("taker", clock.instant(), Duration.ofHours(1));
        AgentIdentity other = a.node().identity().subordinate("other", clock.instant(), Duration.ofHours(1));
        Space asTaker = a.space().as(taker);
        Space asOther = a.space().as(other);
        b.space().write(new TaskEntry("job", 1), MINUTES_30);
        tickAll(4);

        TakenEntry<TaskEntry> taken = asTaker.take(Template.of(TaskEntry.class), MINUTES_10,
                Duration.ofSeconds(1)).orElseThrow();
        tickAll(2);
        assertThat(a.space().currentClaim(taken.entryId())).hasValueSatisfying(claim ->
                assertThat(claim.holder()).isEqualTo(taker.id()));
        assertThat(b.space().currentClaim(taken.entryId())).hasValueSatisfying(claim ->
                assertThat(claim.holder()).isEqualTo(taker.id()));
        assertThatThrownBy(() -> asOther.complete(taken))
                .as("a sibling view does not hold this claim")
                .isInstanceOf(LeaseExpiredException.class);

        asTaker.complete(taken, new TaskEntry("done", 9), MINUTES_30);
        tickAll(4);
        for (Peer replica : List.of(a, b)) {
            List<Space.Issued<TaskEntry>> issued = replica.space().readAllIssued(Template.of(TaskEntry.class), 10);
            assertThat(issued).extracting(i -> i.entry().topic()).containsExactly("done");
            assertThat(issued.get(0).issuer()).isEqualTo(taker.id());
            assertThat(issued.get(0).attestation()).isEqualTo(Space.Attestation.AGENT_ATTESTED);
        }
    }

    /**
     * QA4 A4-7 phase 3, completing the plan: a claim taken through a view is
     * signed by the view's agent key and travels with the peer's certificate,
     * while the peer key still rides as holderKey, so every replica stores an
     * agent-attested proof and the completion authenticates as before.
     */
    @Test
    void aViewsClaimIsSignedByItsAgentAndCarriesItsCertificateEverywhere() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        tickAll(4);
        AgentIdentity taker = a.node().identity().subordinate("taker", clock.instant(), Duration.ofHours(1));
        b.space().write(new TaskEntry("job", 1), MINUTES_30);
        tickAll(4);

        TakenEntry<TaskEntry> taken = a.space().as(taker).take(Template.of(TaskEntry.class), MINUTES_10,
                Duration.ofSeconds(1)).orElseThrow();
        tickAll(2);
        for (Peer replica : List.of(a, b)) {
            SpaceWire.SignedClaim proof = replica.space().claimProof(taken.entryId()).orElseThrow();
            assertThat(proof.claim().holder()).isEqualTo(taker.id());
            assertThat(proof.agentAttested()).as("certificate rides with the proof at " + replica).isTrue();
            assertThat(proof.holderCertificate()).isEqualTo(taker.certificate().orElseThrow());
            assertThat(proof.holderAttested()).as("holderKey is still the peer key").isTrue();
            assertThat(ai.badmonkey.agentspaces.common.crypto.Ed25519.verifyRaw(taker.publicKey(),
                    CODEC.toBytes(proof.claim()), proof.signature())).as("signed by the agent key").isTrue();
            assertThat(ai.badmonkey.agentspaces.common.crypto.Ed25519.verifyRaw(
                    a.node().identity().rawPublicKey(), CODEC.toBytes(proof.claim()), proof.signature()))
                    .as("not by the peer key").isFalse();
        }
        // A peer-signed take carries no certificate: byte-for-byte the old proof.
        b.space().write(new TaskEntry("plain", 2), MINUTES_30);
        tickAll(4);
        TakenEntry<TaskEntry> plain = b.space().take(Template.of(TaskEntry.class).where("topic",
                ai.badmonkey.agentspaces.api.space.Matchers.eq("plain")), MINUTES_10, Duration.ofSeconds(1)).orElseThrow();
        assertThat(b.space().claimProof(plain.entryId()).orElseThrow().agentAttested()).isFalse();

        a.space().as(taker).complete(taken);
        tickAll(4);
        // The job is completed everywhere (authenticated against the agent-signed
        // proof); "plain" is still held by b's take, so nothing is available to read.
        assertThat(b.space().readAll(Template.of(TaskEntry.class), 10)).isEmpty();
        assertThat(b.space().claimProof(plain.entryId())).isPresent();
        assertThat(b.space().knownEntries()).isEqualTo(2);
    }

    /** QA4 A4-11 (found en route): a signed record from another space is not folded into this one. */
    @Test
    void aRecordFromAnotherSpaceIsNotFoldedHere() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        tickAll(4);
        GroupRuntime runtime = b.node().group(groupId).orElseThrow();
        ReplicatedSpace other = ReplicatedSpace.builder(runtime, "other", b.node().identity(), "worker")
                .clock(clock).build();
        try {
            other.write(new TaskEntry("elsewhere", 1), MINUTES_30);
            // Replay the other space's validly signed state onto the tasks stream, as a
            // relay or a hostile member could: every signature verifies, only the spaceId differs.
            SpaceWire.EntryStateDto foreign = other.signedState(
                    other.entryIdOf(new TaskEntry("elsewhere", 1)).orElseThrow()).orElseThrow();
            runtime.gossip().publish("space:tasks", "replay",
                    CODEC.toBytes(new SpaceWire.Delta(foreign, null, null)));
            tickAll(4);
            assertThat(a.space().readAll(Template.of(TaskEntry.class), 10))
                    .as("a record whose spaceId is another space's never lands here").isEmpty();
            assertThat(a.space().knownEntries()).isZero();
        } finally {
            other.close();
        }
    }

    // ------------------------------------------------ QA4 A4-7 phase 1: agent-attested records

    /**
     * QA4 A4-7 phase 1, written before {@code .writer(...)} existed: a record
     * written under a subordinate identity carries the agent's signature and the
     * peer's certificate, verifies at every replica, and is attributed to the
     * agent as {@code AGENT_ATTESTED}; a peer-signed write on the same space is
     * {@code PEER_ASSERTED}. Nothing about the fleet changes for anyone who does
     * not opt in.
     */
    @Test
    void aSubordinateWriteIsAgentAttestedAtEveryReplica() throws Exception {
        Peer a = newPeer("a", 1, id -> id.subordinate("auditor", clock.instant(), Duration.ofHours(1)));
        Peer b = newPeer("b", 2, "a");
        Peer c = newPeer("c", 3, "a");
        tickAll(4);

        a.space().write(new TaskEntry("attested", 1), MINUTES_30);
        b.space().write(new TaskEntry("asserted", 2), MINUTES_30);
        tickAll(4);

        for (Peer replica : List.of(a, b, c)) {
            List<Space.Issued<TaskEntry>> issued = replica.space().readAllIssued(
                    Template.of(TaskEntry.class), 10);
            assertThat(issued).as("both writes at " + replica.node().peerId()).hasSize(2);
            Space.Issued<TaskEntry> attested = issued.stream()
                    .filter(i -> i.entry().topic().equals("attested")).findFirst().orElseThrow();
            Space.Issued<TaskEntry> asserted = issued.stream()
                    .filter(i -> i.entry().topic().equals("asserted")).findFirst().orElseThrow();
            assertThat(attested.issuer()).isEqualTo(a.node().identity().agent("auditor"));
            assertThat(attested.attestation()).isEqualTo(Space.Attestation.AGENT_ATTESTED);
            assertThat(asserted.issuer()).isEqualTo(b.node().identity().agent("worker"));
            assertThat(asserted.attestation()).isEqualTo(Space.Attestation.PEER_ASSERTED);
        }
    }

    /** SPEC §4.2 v0.1.13: a writer holding no certificate that covers its record's stamp refuses to sign, since every receiver would refuse the record. */
    @Test
    void aWriterWhoseCertificateHasLapsedRefusesToSign() throws Exception {
        Peer a = newPeer("a", 1, id -> id.subordinate("auditor",
                clock.instant().minus(Duration.ofHours(2)), Duration.ofHours(1)));
        assertThatThrownBy(() -> a.space().write(new TaskEntry("stale-cert", 1), MINUTES_30))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("holds no certificate covering");
    }

    /** SPEC §4.2 v0.1.13, receiver side: a record whose certificate does not cover its issue stamp is refused at every other replica, even when a hostile writer attaches it anyway. */
    @Test
    void aRecordStampedOutsideItsCertificateWindowIsRefusedElsewhere() throws Exception {
        Peer a = newPeer("a", 1, id -> {
            AgentIdentity stale = id.subordinate("auditor",
                    clock.instant().minus(Duration.ofHours(2)), Duration.ofHours(1));
            return new AgentIdentity() {   // attaches its lapsed certificate regardless
                @Override public ai.badmonkey.agentspaces.common.id.AgentId id() { return stale.id(); }
                @Override public byte[] publicKey() { return stale.publicKey(); }
                @Override public byte[] sign(byte[] bytes) { return stale.sign(bytes); }
                @Override public java.util.Optional<AgentCertificate> certificate() {
                    return stale.certificate();
                }
                @Override public java.util.Optional<AgentCertificate> certificateCovering(Instant t) {
                    return stale.certificate();
                }
            };
        });
        Peer b = newPeer("b", 2, "a");
        tickAll(4);

        a.space().write(new TaskEntry("stale-cert", 1), MINUTES_30);
        tickAll(6);

        assertThat(a.space().read(Template.of(TaskEntry.class))).as("local write lands").isPresent();
        assertThat(b.space().read(Template.of(TaskEntry.class)))
                .as("a certificate that did not cover the record's stamp certifies nothing")
                .isEmpty();
    }

    /** SPEC §4.2 v0.1.13 (review finding §1.2 of TODO-9-10-11): an agent's entries stay mergeable after its certificate lapses, because the certificate is judged at the record's stamp, so a late joiner still receives them. */
    @Test
    void anAgentsEntriesStayMergeableAfterItsCertificateExpires() throws Exception {
        Peer a = newPeer("a", 1, id -> id.subordinate("auditor", clock.instant(),
                Duration.ofMinutes(1)));
        Peer b = newPeer("b", 2, "a");
        tickAll(4);
        a.space().write(new TaskEntry("outlives its certificate", 1), MINUTES_30);
        tickAll(4);
        assertThat(b.space().knownEntries()).isEqualTo(1);

        clock.advance(Duration.ofMinutes(2)); // the one-minute certificate has lapsed
        Peer late = newPeer("c", 3, "a");
        tickAll(8);
        assertThat(late.space().read(Template.of(TaskEntry.class)))
                .as("the late joiner merges history signed while the certificate was valid")
                .contains(new TaskEntry("outlives its certificate", 1));
        assertThat(late.space().readAllIssued(Template.of(TaskEntry.class), 10))
                .extracting(Space.Issued::attestation)
                .containsExactly(Space.Attestation.AGENT_ATTESTED);
    }

    /** SPEC §11a.4 v0.1.13 (TODO-9-10-11 B4, R3): an agent's write, renewal, and cancellation are signed by the agent's own key, name it as signer, and verify at every replica; the stored state that anti-entropy forwards is the agent-signed one. */
    @Test
    void anAgentsStateTransitionsAreAgentSignedAndVerifyEverywhere() throws Exception {
        Peer a = newPeer("a", 1, id -> id.renewingSubordinate("worker", Duration.ofHours(1), clock));
        Peer b = newPeer("b", 2, "a");
        tickAll(4);

        ai.badmonkey.agentspaces.api.space.EntryHandle handle =
                a.space().write(new TaskEntry("signed transitions", 1), MINUTES_10);
        tickAll(4);
        SpaceWire.EntryStateDto written = b.space().signedState(handle.entryId()).orElseThrow();
        assertThat(written.agentSigned()).isTrue();
        assertThat(written.signer()).isEqualTo(a.space().writer().orElseThrow().encoded());
        assertThat(written.stateCertificate()).isNotNull();

        handle.renew(Duration.ofMinutes(30));
        tickAll(4);
        SpaceWire.EntryStateDto renewed = b.space().signedState(handle.entryId()).orElseThrow();
        assertThat(renewed.agentSigned()).isTrue();
        assertThat(renewed.leaseValue().expiresAtMillis())
                .as("the agent-signed renewal landed").isGreaterThan(written.leaseValue().expiresAtMillis());

        handle.cancel();
        tickAll(4);
        assertThat(b.space().read(Template.of(TaskEntry.class)))
                .as("the agent-signed cancellation landed").isEmpty();

        Peer late = newPeer("c", 3, "a");
        tickAll(8);
        assertThat(late.space().signedState(handle.entryId()))
                .as("a late joiner receives the agent-signed tombstone")
                .hasValueSatisfying(dto -> assertThat(dto.agentSigned()).isTrue());
    }

    /** SPEC §11a.4 v0.1.13: a completion by an agent holder is signed by the holder's key and attributes to it; a peer-signed holder's completion of an agent's entry still lands (the holder, not the writer, is the acting party). */
    @Test
    void completionsAreSignedByTheHoldingAgent() throws Exception {
        Peer a = newPeer("a", 1, id -> id.renewingSubordinate("writer", Duration.ofHours(1), clock));
        Peer b = newPeer("b", 2, id -> id.renewingSubordinate("taker", Duration.ofHours(1), clock), "a");
        Peer plain = newPeer("p", 3, "a");
        tickAll(4);
        List<SpaceEvent<TaskEntry>> seenAtA = new CopyOnWriteArrayList<>();
        a.space().notify(Template.of(TaskEntry.class), seenAtA::add, MINUTES_30);
        ai.badmonkey.agentspaces.api.space.EntryHandle first =
                a.space().write(new TaskEntry("agent takes", 1), MINUTES_30);
        tickAll(4);
        TakenEntry<TaskEntry> taken = b.space().take(Template.of(TaskEntry.class),
                MINUTES_10, Duration.ofSeconds(1)).orElseThrow();
        b.space().complete(taken);
        tickAll(4);
        SpaceWire.EntryStateDto completed = a.space().signedState(first.entryId()).orElseThrow();
        assertThat(completed.completed()).isTrue();
        assertThat(completed.signer()).as("the holder signed the completion")
                .isEqualTo(b.space().writer().orElseThrow().encoded());
        assertThat(seenAtA).filteredOn(e -> e.kind() == SpaceEvent.Kind.COMPLETED)
                .singleElement().satisfies(e -> {
                    assertThat(e.issuer()).isEqualTo(a.space().writer().orElseThrow());
                    assertThat(e.actor()).as("the completion event names the holder")
                            .isEqualTo(b.space().writer().orElseThrow());
                });

        a.space().write(new TaskEntry("peer takes", 2), MINUTES_30);
        tickAll(4);
        TakenEntry<TaskEntry> byPeer = plain.space().take(Template.of(TaskEntry.class),
                MINUTES_10, Duration.ofSeconds(1)).orElseThrow();
        plain.space().complete(byPeer);
        tickAll(4);
        assertThat(a.space().read(Template.of(TaskEntry.class)))
                .as("a peer-signed holder's completion of an agent's entry lands").isEmpty();
    }

    /** SPEC §4.2 v0.1.13 (TODO-9-10-11 B3, R1): a renewing agent runs across several certificate lifetimes: it writes, takes, renews its take, and completes with a result, and a late joiner merges all of it. */
    @Test
    void aRenewingAgentRunsAcrossSeveralCertificateLifetimes() throws Exception {
        Peer a = newPeer("a", 1, id -> id.renewingSubordinate("worker", Duration.ofMinutes(10), clock));
        Peer b = newPeer("b", 2, "a");
        tickAll(4);

        a.space().write(new TaskEntry("first", 1), Lease.of(Duration.ofHours(2)));
        clock.advance(Duration.ofMinutes(12));     // past the first certificate's lifetime
        tickAll(3);
        a.space().write(new TaskEntry("second", 2), Lease.of(Duration.ofHours(2)));
        TakenEntry<TaskEntry> taken = a.space().take(
                Template.of(TaskEntry.class).where("topic", ai.badmonkey.agentspaces.api.space.Matchers.eq("first")),
                Lease.of(Duration.ofMinutes(30)), Duration.ofSeconds(1)).orElseThrow();
        clock.advance(Duration.ofMinutes(11));     // past the second certificate's lifetime
        tickAll(3);
        taken.renew(Duration.ofMinutes(30));        // the claim keeps its first stamp
        clock.advance(Duration.ofMinutes(11));
        tickAll(3);
        a.space().complete(taken, new FindingEntry("first", "done across lifetimes"),
                Lease.of(Duration.ofHours(2)));
        tickAll(6);

        Peer late = newPeer("c", 3, "a");
        tickAll(10);
        for (Peer replica : List.of(b, late)) {
            assertThat(replica.space().readAll(Template.of(TaskEntry.class), 10))
                    .as("only the untaken entry remains").containsExactly(new TaskEntry("second", 2));
            assertThat(replica.space().read(Template.of(FindingEntry.class)))
                    .contains(new FindingEntry("first", "done across lifetimes"));
        }
    }

    /** QA4 A4-7 phase 1: a certificate for one agent does not let a record claim to be another. */
    @Test
    void aCertificateNamingAnotherAgentDoesNotBindTheIssuer() throws Exception {
        Peer a = newPeer("a", 1, id -> {
            AgentIdentity worker = id.subordinate("worker", clock.instant(), Duration.ofHours(1));
            return new AgentIdentity() {          // says "auditor", proves "worker"
                @Override public ai.badmonkey.agentspaces.common.id.AgentId id() {
                    return id.agent("auditor");
                }
                @Override public byte[] publicKey() { return worker.publicKey(); }
                @Override public byte[] sign(byte[] bytes) { return worker.sign(bytes); }
                @Override public java.util.Optional<AgentCertificate> certificate() {
                    return worker.certificate();
                }
            };
        });
        Peer b = newPeer("b", 2, "a");
        tickAll(4);

        a.space().write(new TaskEntry("impostor", 1), MINUTES_30);
        tickAll(6);

        assertThat(b.space().read(Template.of(TaskEntry.class)))
                .as("the certificate names 'worker'; a record issued by 'auditor' is not covered")
                .isEmpty();
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // anti-entropy paces itself by the gossip period
        }
    }

    // ------------------------------------------------ QA4 A4-8 / A4-9: no silent replacement

    /** QA4 A4-8, written before the fix: a second bid function must not silently replace the first. */
    @Test
    void aSecondBidFunctionOnOneSpaceIsRefused() throws Exception {
        Peer a = newPeer("a", 1);
        a.space().bidFunction(entry -> 1.0);
        assertThatThrownBy(() -> a.space().bidFunction(entry -> 2.0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("tasks")
                .hasMessageContaining("bid function");
    }

    /** QA4 A4-8: the builder's bid function is the first assignment; a runtime second one is refused. */
    @Test
    void aBuilderBidFunctionCountsAsTheFirstAssignment() throws Exception {
        Peer a = newPeer("a", 1, Duration.ZERO, b -> b.bidFunction(entry -> 1.0));
        assertThatThrownBy(() -> a.space().bidFunction(entry -> 2.0))
                .isInstanceOf(IllegalStateException.class);
    }

    /**
     * QA4 A4-9, written before the fix: a second handle for one space name on one
     * node used to replace the first's gossip registrations, so the first handle
     * silently stopped converging. The second must be refused, naming the stream.
     */
    @Test
    void aSecondHandleForOneSpaceNameIsRefused() throws Exception {
        Peer a = newPeer("a", 1);
        GroupRuntime runtime = a.node().group(groupId).orElseThrow();
        assertThatThrownBy(() -> ReplicatedSpace.builder(runtime, "tasks", a.node().identity(), "again")
                .clock(clock).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("space:tasks");
    }

    /** QA4 A4-9: closing a space deregisters it, so the same name can be opened again and converges. */
    @Test
    void aClosedSpaceCanBeReopenedByNameAndConverges() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        tickAll(3);
        GroupRuntime runtime = a.node().group(groupId).orElseThrow();
        assertThat(runtime.gossip().streams()).contains("space:tasks");

        a.space().close();
        assertThat(runtime.gossip().streams()).as("closing deregisters").doesNotContain("space:tasks");

        ReplicatedSpace reopened = ReplicatedSpace.builder(runtime, "tasks", a.node().identity(), "worker")
                .clock(clock).build();
        b.space().write(new TaskEntry("after-reopen", 1), MINUTES_30);
        tickAll(4);
        assertThat(reopened.read(Template.of(TaskEntry.class)))
                .as("the reopened handle converges").isPresent();
        reopened.close();
    }

    // ------------------------------------------------ QA4 A4-5: log-decided claims

    private static final ai.badmonkey.agentspaces.common.codec.CborCodec CODEC =
            ai.badmonkey.agentspaces.common.codec.CborCodec.defaultCodec();

    /** A claim on {@code entryId} held by {@code holder}'s worker agent, as the log would commit it. */
    private TakeClaim claimBy(Peer holder, EntryId entryId, long epoch) {
        long now = clock.instant().toEpochMilli();
        return new TakeClaim(entryId, holder.space().id(), epoch,
                new ai.badmonkey.agentspaces.common.hlc.HlcTimestamp(now, 0,
                        holder.node().peerId().value()),
                holder.node().identity().agent("worker"), 0.0, now + MINUTES_10.duration().toMillis());
    }

    private static byte[] signedBy(Peer signer, TakeClaim claim) {
        return signer.node().identity().sign(CODEC.toBytes(claim));
    }

    private static List<SpaceEvent.Kind> eventsAt(Peer peer) {
        List<SpaceEvent.Kind> seen = new java.util.concurrent.CopyOnWriteArrayList<>();
        peer.space().notify(Template.of(TaskEntry.class), e -> seen.add(e.kind()), MINUTES_30);
        return seen;
    }

    /**
     * QA4 A4-5: a claim installed with the holder's own attestation lets every
     * replica authenticate the holder's completion — by anti-entropy alone, with
     * the completion rumor cut — and the completed entry never reappears.
     */
    @Test
    void anAttestedLogClaimAuthenticatesTheHoldersCompletionEverywhere() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        Peer c = newPeer("c", 3, "a");
        tickAll(4);
        a.space().write(new TaskEntry("ordered", 1), MINUTES_30);
        tickAll(2);
        // A non-writer resolves an entry id from a value it has decoded, so read first,
        // exactly as OrderedTakes.take does before it asks entryIdOf.
        TaskEntry seenAtB = b.space().read(Template.of(TaskEntry.class)).orElseThrow();
        EntryId entryId = b.space().entryIdOf(seenAtB).orElseThrow();
        TakeClaim claim = claimBy(b, entryId, 1);
        byte[] key = b.node().identity().rawPublicKey();
        byte[] sig = signedBy(b, claim);
        for (Peer p : List.of(a, b, c)) {
            p.space().applyAuthorizedClaim(entryId, claim, key, sig);   // what the log does
        }
        List<SpaceEvent.Kind> atA = eventsAt(a);
        List<SpaceEvent.Kind> atC = eventsAt(c);

        TakenEntry<TaskEntry> taken = b.space().adoptClaim(Template.of(TaskEntry.class), entryId)
                .orElseThrow();
        network.partition("b", "a");
        network.partition("b", "c");
        b.space().complete(taken);
        network.heal();
        tickAll(40);

        assertThat(atA).as("a authenticated the completion by anti-entropy")
                .contains(SpaceEvent.Kind.COMPLETED);
        assertThat(atC).as("c authenticated the completion by anti-entropy")
                .contains(SpaceEvent.Kind.COMPLETED);
        clock.advance(Duration.ofMinutes(11));
        tickAll(3);
        for (Peer p : List.of(a, b, c)) {
            assertThat(p.space().read(Template.of(TaskEntry.class)))
                    .as("completed work does not reappear at " + p.node().peerId()).isEmpty();
        }
    }

    /**
     * QA4 A4-5, the negative that keeps the check honest: a claim installed
     * without the holder's attestation is signed by the installing node, so the
     * holder's completion cannot be authenticated against it anywhere else. This
     * is the old failure, pinned: no completion is seen, and once the TAKE lease
     * lapses the finished work comes back as takeable.
     */
    @Test
    void aBareLogClaimCannotAuthenticateACompletionAtOtherReplicas() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        Peer c = newPeer("c", 3, "a");
        tickAll(4);
        a.space().write(new TaskEntry("stuck", 1), MINUTES_30);
        tickAll(2);
        // A non-writer resolves an entry id from a value it has decoded, so read first,
        // exactly as OrderedTakes.take does before it asks entryIdOf.
        TaskEntry seenAtB = b.space().read(Template.of(TaskEntry.class)).orElseThrow();
        EntryId entryId = b.space().entryIdOf(seenAtB).orElseThrow();
        TakeClaim claim = claimBy(b, entryId, 1);
        for (Peer p : List.of(a, b, c)) {
            p.space().applyAuthorizedClaim(entryId, claim);              // bare: no attestation
        }
        List<SpaceEvent.Kind> atA = eventsAt(a);

        TakenEntry<TaskEntry> taken = b.space().adoptClaim(Template.of(TaskEntry.class), entryId)
                .orElseThrow();
        network.partition("b", "a");
        network.partition("b", "c");
        b.space().complete(taken);
        network.heal();
        tickAll(40);

        assertThat(atA).as("a cannot authenticate a completion against a claim it signed itself")
                .doesNotContain(SpaceEvent.Kind.COMPLETED);
        clock.advance(Duration.ofMinutes(11));
        tickAll(3);
        assertThat(a.space().read(Template.of(TaskEntry.class)))
                .as("the finished order reappears at a — the Party Bus symptom").isPresent();
    }

    /** QA4 A4-5: proof that does not prove what it claims is refused, naming both parties. */
    @Test
    void anAttestationWhoseKeyIsNotTheHoldersIsRefused() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        tickAll(3);
        a.space().write(new TaskEntry("forged", 1), MINUTES_30);
        tickAll(2);
        EntryId entryId = a.space().entryIdOf(new TaskEntry("forged", 1)).orElseThrow();
        TakeClaim heldByB = claimBy(b, entryId, 1);

        assertThatThrownBy(() -> a.space().applyAuthorizedClaim(entryId, heldByB,
                a.node().identity().rawPublicKey(), signedBy(a, heldByB)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(b.node().peerId().display())
                .hasMessageContaining(a.node().peerId().display())
                .hasMessageContaining("completion");
        assertThat(a.space().currentClaim(entryId)).as("nothing was installed").isEmpty();
    }

    /**
     * QA4 A4-5: the log's decision is authoritative for its epoch. A gossip
     * claim at the same epoch from another holder does not displace it — the
     * priority the log-index re-stamp used to buy by accident — while the
     * lattice's own rule still lets a higher epoch win.
     */
    @Test
    void aLogDecidedEpochIsNotDisplacedBySameEpochGossip() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        Peer c = newPeer("c", 3, "a");
        tickAll(4);
        a.space().write(new TaskEntry("contested", 1), MINUTES_30);
        tickAll(2);
        EntryId entryId = a.space().entryIdOf(new TaskEntry("contested", 1)).orElseThrow();
        TakeClaim decided = claimBy(b, entryId, 1);
        a.space().applyAuthorizedClaim(entryId, decided,
                b.node().identity().rawPublicKey(), signedBy(b, decided));

        TakeClaim rival = claimBy(c, entryId, 1);                        // same epoch, other holder
        gossipClaimFrom(c, entryId, rival);
        tickAll(3);
        assertThat(a.space().currentClaim(entryId)).hasValueSatisfying(cl ->
                assertThat(cl.holder()).as("the log's decision stands").isEqualTo(decided.holder()));

        TakeClaim later = claimBy(c, entryId, 2);                        // a higher epoch may win
        gossipClaimFrom(c, entryId, later);
        tickAll(3);
        assertThat(a.space().currentClaim(entryId)).hasValueSatisfying(cl ->
                assertThat(cl.epoch()).as("the lattice still advances by epoch").isEqualTo(2L));
    }

    /**
     * QA4 A4-5: an identical claim may still merge, so a replica holding an
     * unattested copy is upgraded by the attested one a completion delta carries.
     */
    @Test
    void anIdenticalAttestedClaimUpgradesAnUnattestedCopy() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        tickAll(3);
        a.space().write(new TaskEntry("upgrade", 1), MINUTES_30);
        tickAll(2);
        // A non-writer resolves an entry id from a value it has decoded, so read first,
        // exactly as OrderedTakes.take does before it asks entryIdOf.
        TaskEntry seenAtB = b.space().read(Template.of(TaskEntry.class)).orElseThrow();
        EntryId entryId = b.space().entryIdOf(seenAtB).orElseThrow();
        TakeClaim claim = claimBy(b, entryId, 1);
        a.space().applyAuthorizedClaim(entryId, claim);                  // unattested copy at a
        b.space().applyAuthorizedClaim(entryId, claim,
                b.node().identity().rawPublicKey(), signedBy(b, claim));
        List<SpaceEvent.Kind> atA = eventsAt(a);

        TakenEntry<TaskEntry> taken = b.space().adoptClaim(Template.of(TaskEntry.class), entryId)
                .orElseThrow();
        b.space().complete(taken);                                       // rumor carries the proof
        tickAll(2);

        assertThat(atA).as("the rumor's attested proof upgraded a's copy")
                .contains(SpaceEvent.Kind.COMPLETED);
    }

    private void gossipClaimFrom(Peer sender, EntryId entryId, TakeClaim claim) {
        SpaceWire.SignedClaim signed = new SpaceWire.SignedClaim(claim,
                sender.node().identity().rawPublicKey(), signedBy(sender, claim));
        sender.node().group(groupId).orElseThrow().gossip().publish("space:tasks",
                "claim-" + claim.epoch() + "-" + sender.node().peerId().value(),
                CODEC.toBytes(new SpaceWire.Delta(null, entryId, signed)));
    }

    @Test
    void writesReplicateToEveryPeer() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        Peer c = newPeer("c", 3, "a");
        tickAll(4);

        a.space().write(new TaskEntry("replicate me", 5), MINUTES_30);

        assertThat(b.space().read(Template.of(TaskEntry.class)))
                .contains(new TaskEntry("replicate me", 5));
        assertThat(c.space().read(Template.of(TaskEntry.class).where("priority", gte(5))))
                .isPresent();
    }

    /** Issue #16 §9.2: tags travel on the record, so a tag condition selects the same entries at every replica, before decode, on reads and on subscriptions. */
    @Test
    void tagConditionsSelectReplicatedEntriesAtEveryPeer() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        tickAll(4);
        List<SpaceEvent<TaskEntry>> euAtB = new CopyOnWriteArrayList<>();
        List<SpaceEvent<TaskEntry>> allAtB = new CopyOnWriteArrayList<>();
        b.space().notify(Template.of(TaskEntry.class).whereTag("region", eq("eu")), euAtB::add,
                MINUTES_30);
        b.space().notify(Template.of(TaskEntry.class), allAtB::add, MINUTES_30);

        a.space().write(new TaskEntry("eu-task", 5), MINUTES_30, Map.of("region", "eu"));
        a.space().write(new TaskEntry("us-task", 5), MINUTES_30, Map.of("region", "us"));
        tickAll(4);

        assertThat(b.space().read(Template.of(TaskEntry.class).whereTag("region", eq("eu"))))
                .contains(new TaskEntry("eu-task", 5));
        assertThat(b.space().read(Template.of(TaskEntry.class).whereTag("region", eq("apac"))))
                .isEmpty();
        assertThat(b.space().readAll(Template.of(TaskEntry.class).hasTag("region"), 10)).hasSize(2);
        assertThat(b.space().readAllEntries(Template.of(TaskEntry.class)
                .whereTag("region", eq("us")), 10)).singleElement().satisfies(entry -> {
                    assertThat(entry.value()).isEqualTo(new TaskEntry("us-task", 5));
                    assertThat(entry.tags()).containsExactly(Map.entry("region", "us"));
                    assertThat(entry.issuer()).isEqualTo(a.node().identity().agent("worker"));
                });
        assertThat(allAtB).hasSize(2);
        assertThat(euAtB).singleElement().satisfies(event -> {
            assertThat(event.entry()).isEqualTo(new TaskEntry("eu-task", 5));
            assertThat(event.tags()).containsExactly(Map.entry("region", "eu"));
        });

        TakenEntry<TaskEntry> taken = b.space().take(
                Template.of(TaskEntry.class).whereTag("region", eq("us")), MINUTES_10,
                Duration.ZERO).orElseThrow();
        assertThat(taken.entry().topic()).isEqualTo("us-task");
        assertThat(b.space().take(Template.of(TaskEntry.class).whereTag("region", eq("apac")),
                MINUTES_10, Duration.ZERO)).isEmpty();
        b.space().complete(taken, new FindingEntry("us-task", "done"), MINUTES_30,
                Map.of("region", "us"));
        tickAll(4);
        assertThat(a.space().read(Template.of(FindingEntry.class).whereTag("region", eq("us"))))
                .contains(new FindingEntry("us-task", "done"));
        assertThat(a.space().read(Template.of(FindingEntry.class).whereTag("region", eq("eu"))))
                .isEmpty();
    }

    /** A result that overruns the inline limit on a space with no block store. */
    public record BigResult(String blob) {
    }

    @Test
    void aResultThatCannotBeWrittenDoesNotConsumeTheTake() throws Exception {
        Peer a = newPeer("a", 1);
        tickAll(2);
        a.space().write(new TaskEntry("job", 1), MINUTES_30);
        Optional<TakenEntry<TaskEntry>> taken =
                a.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(taken).isPresent();

        // Completing with an oversize result on this blocks-less space must fail
        // BEFORE the completion commits, or the task would be lost fleet-wide.
        BigResult tooBig = new BigResult("x".repeat(70_000));
        assertThatThrownBy(() -> a.space().complete(taken.get(), tooBig, MINUTES_30))
                .isInstanceOf(IllegalStateException.class);

        // The take was not consumed: the holder can still complete it normally.
        a.space().complete(taken.get(), new BigResult("small"), MINUTES_30);
        assertThat(a.space().readAll(Template.of(BigResult.class), 10))
                .containsExactly(new BigResult("small"));
        assertThat(a.space().read(Template.of(TaskEntry.class))).isEmpty();
    }

    @Test
    void takeIsExclusiveAcrossTheFleet() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        Peer c = newPeer("c", 3, "a");
        tickAll(4);
        a.space().write(new TaskEntry("one task", 1), MINUTES_30);

        Optional<TakenEntry<TaskEntry>> first =
                b.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        Optional<TakenEntry<TaskEntry>> second =
                c.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);

        assertThat(first).isPresent();
        assertThat(second).isEmpty(); // B's claim propagated; C sees the entry held
        b.space().complete(first.get());

        // Completion replicates: nobody can read or take it anywhere.
        assertThat(a.space().read(Template.of(TaskEntry.class))).isEmpty();
        assertThat(c.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO))
                .isEmpty();
    }

    @Test
    void partitionedClaimsResolveToOneWinnerOnHeal() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        Peer c = newPeer("c", 3, "a");
        tickAll(4);
        a.space().write(new TaskEntry("contested", 1), MINUTES_30);
        assertThat(b.space().read(Template.of(TaskEntry.class))).isPresent();
        assertThat(c.space().read(Template.of(TaskEntry.class))).isPresent();

        // Fully isolate C (gossip relays claims through any connected peer, so a
        // single cut is healed by A); each side claims knowing nothing of the other.
        network.partition("b", "c");
        network.partition("a", "c");
        Optional<TakenEntry<TaskEntry>> byB =
                b.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        Optional<TakenEntry<TaskEntry>> byC =
                c.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(byB).isPresent();
        assertThat(byC).isPresent(); // transient duplicate work: the documented trade-off

        network.heal();
        tickAll(6); // claims merge deterministically everywhere

        // Exactly one holder survives the merge; the other's complete fails.
        List<Boolean> outcomes = new ArrayList<>();
        try {
            b.space().complete(byB.get());
            outcomes.add(true);
        } catch (LeaseExpiredException e) {
            outcomes.add(false);
        }
        try {
            c.space().complete(byC.get());
            outcomes.add(true);
        } catch (LeaseExpiredException e) {
            outcomes.add(false);
        }
        assertThat(outcomes).containsExactlyInAnyOrder(true, false);
        assertThat(a.space().read(Template.of(TaskEntry.class))).isEmpty();
    }

    @Test
    void killedWorkersTaskReappearsForAnotherPeer() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        Peer c = newPeer("c", 3, "a");
        tickAll(4);
        a.space().write(new TaskEntry("survivable", 1), MINUTES_30);

        Optional<TakenEntry<TaskEntry>> doomed = b.space().take(
                Template.of(TaskEntry.class), Lease.of(Duration.ofMinutes(1)), Duration.ZERO);
        assertThat(doomed).isPresent();
        b.node().close(); // the worker dies mid-task
        network.partition("a", "b");
        network.partition("c", "b");

        // While the claim is live, nobody else can take it.
        assertThat(c.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO))
                .isEmpty();

        // The claim lapses; the entry reappears and C completes it with a result.
        clock.advance(Duration.ofMinutes(2));
        Optional<TakenEntry<TaskEntry>> retry =
                c.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(retry).isPresent();
        c.space().complete(retry.get(),
                new FindingEntry("survivable", "finished elsewhere"), MINUTES_30);

        assertThat(a.space().read(Template.of(FindingEntry.class)))
                .contains(new FindingEntry("survivable", "finished elsewhere"));
    }

    @Test
    void lateJoinerConvergesThroughAntiEntropy() throws Exception {
        Peer a = newPeer("a", 1);
        tickAll(1);
        a.space().write(new TaskEntry("history", 3), MINUTES_30);

        Peer d = newPeer("d", 4, "a");
        tickAll(6);

        assertThat(d.space().read(Template.of(TaskEntry.class)))
                .contains(new TaskEntry("history", 3));
    }

    @Test
    void expiredWriteLeasesHideEntriesEverywhere() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        tickAll(3);
        a.space().write(new TaskEntry("ephemeral", 1), Lease.of(Duration.ofMinutes(1)));
        assertThat(b.space().read(Template.of(TaskEntry.class))).isPresent();

        clock.advance(Duration.ofMinutes(2));

        assertThat(a.space().read(Template.of(TaskEntry.class))).isEmpty();
        assertThat(b.space().read(Template.of(TaskEntry.class))).isEmpty();
    }

    @Test
    void fleetDrainsTasksExactlyOnce() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        Peer c = newPeer("c", 3, "a");
        tickAll(4);
        int tasks = 30;
        for (int i = 0; i < tasks; i++) {
            a.space().write(new TaskEntry("task-" + i, i % 5), MINUTES_30);
        }

        int completed = 0;
        List<Peer> workers = List.of(a, b, c);
        int idle = 0;
        int turn = 0;
        while (idle < workers.size()) {
            Peer worker = workers.get(turn % workers.size());
            turn++;
            Optional<TakenEntry<TaskEntry>> taken = worker.space().take(
                    Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
            if (taken.isEmpty()) {
                idle++;
                continue;
            }
            idle = 0;
            worker.space().complete(taken.get());
            completed++;
        }

        assertThat(completed).isEqualTo(tasks);
        assertThat(a.space().read(Template.of(TaskEntry.class))).isEmpty();
        assertThat(b.space().read(Template.of(TaskEntry.class))).isEmpty();
        assertThat(c.space().read(Template.of(TaskEntry.class))).isEmpty();
    }

    /** Spec §11a.4: anti-entropy forwards the holder-signed completion verbatim, so a late joiner reachable only via a relay still accepts it. */
    @Test
    void aLateJoinerAcceptsACompletionForwardedByARelay() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        Peer c = newPeer("c", 3, "a");
        tickAll(4);
        a.space().write(new TaskEntry("finished", 1), MINUTES_30);
        Optional<TakenEntry<TaskEntry>> taken =
                b.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(taken).isPresent();
        b.space().complete(taken.get());
        tickAll(4);
        assertThat(c.space().knownEntries()).isEqualTo(1);

        // D joins late and can only ever talk to C, which authored neither the
        // entry nor the completion; C must forward the stored signatures, not re-sign.
        network.partition("d", "a");
        network.partition("d", "b");
        Peer d = newPeer("d", 4, "c");
        tickAll(12);

        assertThat(d.space().knownEntries()).as("tombstone reached D through the relay").isEqualTo(1);
        assertThat(d.space().read(Template.of(TaskEntry.class))).isEmpty();
        assertThat(d.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO))
                .isEmpty();
    }

    /** Spec §7.4 LEASE_RACE: two takers racing under a real (non-zero) settle window yield exactly one winner. */
    @Test
    void concurrentTakesUnderARealSettleWindowYieldExactlyOneWinner() throws Exception {
        Duration settle = Duration.ofMillis(50);
        Peer a = newPeer("a", 1, settle);
        Peer b = newPeer("b", 2, settle, "a");
        Peer c = newPeer("c", 3, settle, "a");
        tickAll(4);
        a.space().write(new TaskEntry("raced", 1), MINUTES_30);
        assertThat(b.space().read(Template.of(TaskEntry.class))).isPresent();
        assertThat(c.space().read(Template.of(TaskEntry.class))).isPresent();

        CountDownLatch go = new CountDownLatch(1);
        Map<Peer, Optional<TakenEntry<TaskEntry>>> outcomes = new ConcurrentHashMap<>();
        List<Thread> racers = new ArrayList<>();
        for (Peer racer : List.of(b, c)) {
            racers.add(Thread.ofVirtual().start(() -> {
                try {
                    go.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                outcomes.put(racer, racer.space().take(Template.of(TaskEntry.class),
                        MINUTES_10, Duration.ZERO));
            }));
        }
        go.countDown();
        for (Thread racer : racers) {
            racer.join(Duration.ofSeconds(10).toMillis());
        }

        assertThat(outcomes).hasSize(2);
        assertThat(outcomes.values().stream().filter(Optional::isPresent).count())
                .as("exactly one taker survives the settle window").isEqualTo(1);
        Peer winner = outcomes.get(b).isPresent() ? b : c;
        winner.space().complete(outcomes.get(winner).get());
        assertThat(a.space().read(Template.of(TaskEntry.class))).isEmpty();
    }

    /** Spec §7.3: a completed entry's tombstone is collected twice the maximum lease after its lease lapses, on every replica, and a late joiner is never offered it. */
    @Test
    void tombstonesAreGarbageCollectedAndNotReofferedToLateJoiners() throws Exception {
        Lease oneMinute = Lease.of(Duration.ofMinutes(1));
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        tickAll(4);

        a.space().write(new TaskEntry("done-soon", 1), oneMinute);
        TakenEntry<TaskEntry> taken = b.space().take(Template.of(TaskEntry.class), oneMinute,
                Duration.ZERO).orElseThrow();
        b.space().complete(taken);
        tickAll(2);
        assertThat(a.space().knownEntries()).isEqualTo(1);
        assertThat(b.space().knownEntries()).isEqualTo(1);
        assertThat(a.space().maxLeaseMillis()).isEqualTo(Duration.ofMinutes(1).toMillis());

        // Lease lapses at +1m; the tombstone lives until +1m + 2 * maxLease = +3m.
        clock.advance(Duration.ofMinutes(2));
        tickAll(2);
        assertThat(a.space().knownEntries()).as("still inside the tombstone window").isEqualTo(1);
        clock.advance(Duration.ofMinutes(1));
        tickAll(2); // the anti-entropy tick runs the sweep
        assertThat(a.space().knownEntries()).isZero();
        assertThat(b.space().knownEntries()).isZero();

        // A late joiner pulls from replicas that collected it and never sees it.
        Peer d = newPeer("d", 4, "a");
        tickAll(6);
        assertThat(d.space().knownEntries()).isZero();

        // A tombstone younger than the threshold survives everywhere, joiner included.
        a.space().write(new TaskEntry("done-later", 2), oneMinute);
        TakenEntry<TaskEntry> second = b.space().take(Template.of(TaskEntry.class), oneMinute,
                Duration.ZERO).orElseThrow();
        b.space().complete(second);
        clock.advance(Duration.ofMinutes(2)); // lapsed at +1m, horizon at +3m
        tickAll(4);
        assertThat(a.space().knownEntries()).isEqualTo(1);
        assertThat(b.space().knownEntries()).isEqualTo(1);
        assertThat(d.space().knownEntries()).isEqualTo(1);
        assertThat(d.space().read(Template.of(TaskEntry.class))).isEmpty();
    }

    /** Spec §7.2: a FRESH read performs exactly one anti-entropy pull toward one random member before matching locally; LOCAL reads the replica as it stands. */
    @Test
    void aFreshReadPullsOnceBeforeMatching() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        tickAll(4);
        a.space().write(new TaskEntry("current", 1), MINUTES_30);

        // A shared reconcilable state counts every anti-entropy digest a member
        // answers; registering it on the joiner too puts it in the joiner's digest.
        AtomicInteger pullsAnswered = new AtomicInteger();
        ReconcilableState counter = new ReconcilableState() {
            @Override
            public byte[] digest() {
                return new byte[0];
            }

            @Override
            public byte[] deltaFor(byte[] remoteDigest) {
                pullsAnswered.incrementAndGet();
                return new byte[0];
            }

            @Override
            public void applyDelta(byte[] delta) {
            }
        };
        Peer d = newPeer("d", 4, "a");
        for (Peer peer : List.of(a, b, d)) {
            peer.node().group(groupId).orElseThrow().gossip().reconcile("pull-counter", counter);
        }
        // Only the seed ticks, so the joiner learns of it without pulling itself.
        a.node().tick();
        int before = pullsAnswered.get();

        assertThat(d.space().readAll(Template.of(TaskEntry.class), ConsistencyHint.LOCAL, 10))
                .as("zero rounds after joining, the local replica is empty").isEmpty();
        assertThat(pullsAnswered.get()).isEqualTo(before);

        assertThat(d.space().read(Template.of(TaskEntry.class), ConsistencyHint.FRESH, Duration.ZERO))
                .contains(new TaskEntry("current", 1));
        assertThat(pullsAnswered.get() - before).as("exactly one pull").isEqualTo(1);

        assertThat(d.space().readAll(Template.of(TaskEntry.class), ConsistencyHint.FRESH, 10))
                .containsExactly(new TaskEntry("current", 1));
        assertThat(pullsAnswered.get() - before).isEqualTo(2);
    }

    /** Spec §7.5: a member creates a space by publishing a signed SpaceAdvertisement; refresh republishes it with the schema hints seen so far. */
    @Test
    void aSpaceAdvertisesItselfOnCreation() throws Exception {
        List<SignedAdvertisement<?>> published = new CopyOnWriteArrayList<>();
        Instant createdAt = clock.instant();
        Peer a = newPeer("a", 1, Duration.ZERO, builder -> builder
                .advertise(published::add)
                .advertisementTtl(Duration.ofMinutes(5)));
        tickAll(2);

        assertThat(published).hasSize(1);
        SignedAdvertisement<?> signed = published.get(0);
        assertThat(signed.advertisement()).isInstanceOf(SpaceAdvertisement.class);
        SpaceAdvertisement ad = (SpaceAdvertisement) signed.advertisement();
        assertThat(ad.spaceName()).isEqualTo("tasks");
        assertThat(ad.group()).isEqualTo(groupId);
        assertThat(ad.issuer()).isEqualTo(a.node().peerId());
        assertThat(ad.strategy()).isEqualTo(ConflictStrategyType.LEASE_RACE);
        assertThat(ad.admission()).isEqualTo(SpaceAdvertisement.Admission.GROUP);
        assertThat(ad.replication()).isEqualTo(SpaceAdvertisement.Replication.FULL);
        assertThat(ad.ttl()).isEqualTo(Duration.ofMinutes(5));
        assertThat(ad.issued()).isEqualTo(createdAt);
        assertThat(new AdvertisementSigner().verify(signed)).as("signed by the founder").isTrue();
        assertThat(a.space().advertisement()).map(SignedAdvertisement::signature)
                .contains(signed.signature());

        // The refresh loop republishes with a fresh issue instant and the types seen.
        a.space().write(new TaskEntry("hinted", 1), MINUTES_30);
        clock.advance(Duration.ofMinutes(1));
        assertThat(a.space().refreshAdvertisement()).isPresent();
        assertThat(published).hasSize(2);
        SpaceAdvertisement refreshed = (SpaceAdvertisement) published.get(1).advertisement();
        assertThat(refreshed.issued()).isEqualTo(clock.instant());
        assertThat(refreshed.schemaHints()).contains(ForgeSupport.schemaNameOf(TaskEntry.class));

        // A space built without a publisher advertises nothing and refresh is a no-op.
        Peer quiet = newPeer("q", 5, "a");
        assertThat(quiet.space().advertisement()).isEmpty();
        assertThat(quiet.space().refreshAdvertisement()).isEmpty();
    }

    /** Spec §7.4: ORDERED is not a space strategy; the ordered-log coordinator drives those takes. */
    @Test
    void orderedIsNotASpaceStrategy() throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(9).build();
        node.listen(network.register("z"), "z");
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        nodes.add(node);

        assertThatThrownBy(() -> ReplicatedSpace.builder(runtime, "commits", identity, "taker")
                .strategy(ConflictStrategyType.ORDERED))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("OrderedTakes");
    }
}
