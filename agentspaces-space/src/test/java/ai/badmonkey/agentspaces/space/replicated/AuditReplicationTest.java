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
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.gossip.GossipBus;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;
import ai.badmonkey.agentspaces.space.crdt.Dot;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The trie audit on the wire (srad/replication-by-entry.md phase 4, step 4a; SPEC annex I.6): two and three members
 * repair a space by descending their tries to the entries that differ and fetching those by id, never sending the
 * space's text digest; a member off {@code wire.ae=2} keeps the forms of v0.1.17; a stale card falls back within a
 * round; sessions keep their bounds; a late copy is applied; a lapsed entry and an id holding a control character stay
 * out of the audited set; and an acknowledged collected tombstone keeps a converged pair's roots equal.
 *
 * <p>The fabric delivers on the sender's thread, so a whole audit runs inside the call that opens its round. The
 * tests that drive many exchanges run on a thread with a deep stack for that reason; a real transport delivers each
 * frame on its own reader thread and nests nothing.
 */
class AuditReplicationTest {

    private static final CborCodec CODEC = CborCodec.defaultCodec();
    private static final WireCodec WIRE = new WireCodec(CODEC);
    private static final Lease HOUR = Lease.of(Duration.ofHours(1));
    private static final String STREAM = "space:tasks";

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zAudit", PeerIdentity.generate().peerId(), GroupId.of("zAudit"),
            java.time.Instant.EPOCH, Duration.ofDays(1), "audit",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(String address, PeerIdentity identity, PeerNode node, GroupRuntime runtime) {
        ReplicatedSpace space(String name, TestClock clock) {
            return ReplicatedSpace.builder(runtime, name, identity, "worker").clock(clock)
                    .settleWindow(Duration.ZERO).build();
        }

        PeerId id() {
            return identity.peerId();
        }

        GossipBus gossip() {
            return runtime.gossip();
        }
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Peer peer(String address, long seed, boolean audit, String... seeds) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).audit(audit).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String s : seeds) {
            endpoints.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), endpoints);
        nodes.add(node);
        return new Peer(address, identity, node, runtime);
    }

    private void tick(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    /** Runs work on a thread with a deep stack: the fabric nests every exchange of an audit in the call that opens it. */
    private static void deep(ThrowingRunnable work) throws Exception {
        AtomicReference<Throwable> failed = new AtomicReference<>();
        Thread thread = new Thread(null, () -> {
            try {
                work.run();
            } catch (Throwable t) {
                failed.set(t);
            }
        }, "audit-deep", 512L << 20);
        thread.start();
        thread.join();
        if (failed.get() instanceof Exception e) {
            throw e;
        }
        if (failed.get() instanceof Error e) {
            throw e;
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /** Every DIGEST body sent on a direction, decoded, for the checks on what went on the wire. */
    private List<Bodies.Digest> digestsSent(String from, String to) {
        List<Bodies.Digest> seen = new CopyOnWriteArrayList<>();
        network.tap(from, to, frame -> WIRE.decode(frame).ifPresent(envelope -> {
            if (envelope.kind() == Envelope.Kind.DIGEST) {
                seen.add(CODEC.fromWire(envelope.body(), Bodies.Digest.class));
            } else if (envelope.kind() == Envelope.Kind.BATCH) {
                throw new IllegalStateException("the tests run without coalescing");
            }
        }));
        return seen;
    }

    private static boolean sentSpaceText(List<Bodies.Digest> bodies) {
        for (Bodies.Digest body : bodies) {
            if (body.digests() != null && body.digests().containsKey(STREAM)) {
                return true;
            }
        }
        return false;
    }

    private static boolean sentAuditFields(List<Bodies.Digest> bodies) {
        return bodies.stream().anyMatch(Bodies.Digest::carriesAudit);
    }

    private static Set<String> topics(ReplicatedSpace space) {
        Set<String> out = new HashSet<>();
        for (TaskEntry entry : space.readAll(Template.of(TaskEntry.class), 100_000)) {
            out.add(entry.topic());
        }
        return out;
    }

    private static void writeMany(ReplicatedSpace space, String prefix, int count) {
        for (int i = 0; i < count; i++) {
            space.write(new TaskEntry(prefix + i, i), HOUR);
        }
    }

    private static String hex(byte[] bytes) {
        return java.util.HexFormat.of().formatHex(bytes);
    }

    /**
     * RE-T-03, RE-T-04: one entry differing in a space of three hundred. The partner answers the opening with its root,
     * the asker descends to the one leaf that differs and fetches that entry by id, and neither sends the space's text
     * digest.
     */
    @Test
    void aSingleDifferingEntryIsFoundByDescentAndFetchedById() throws Exception {
        Peer a = peer("a", 1, true);
        Peer b = peer("b", 2, true, "a");
        tick(6);
        ReplicatedSpace sa = a.space("tasks", clock);
        ReplicatedSpace sb = b.space("tasks", clock);
        writeMany(sa, "shared-", 300);
        tick(4);
        assertThat(sb.knownEntries()).isEqualTo(300);
        assertThat(hex(sb.streamHash())).isEqualTo(hex(sa.streamHash()));

        network.partition("a", "b");
        sa.write(new TaskEntry("missed", 1), HOUR);
        network.heal();
        List<Bodies.Digest> fromB = digestsSent("b", "a");
        List<Bodies.Digest> fromA = digestsSent("a", "b");
        GossipBus.AuditSignals before = b.gossip().auditSignals();
        GossipBus.AuditSignals partnerBefore = a.gossip().auditSignals();
        b.gossip().antiEntropyWith(a.id());

        assertThat(topics(sb)).contains("missed");
        assertThat(hex(sb.streamHash())).isEqualTo(hex(sa.streamHash()));
        GossipBus.AuditSignals after = b.gossip().auditSignals();
        GossipBus.AuditSignals partner = a.gossip().auditSignals();
        assertThat(after.idsFetched() - before.idsFetched()).as("exactly the one entry, by id").isEqualTo(1);
        assertThat(partner.rootsAnswered() - partnerBefore.rootsAnswered()).isEqualTo(1);
        assertThat(partner.answerBytesSent() - partnerBefore.answerBytesSent())
                .as("a root and a leaf or two of preimage, not the space").isLessThan(8_000);
        assertThat(sentSpaceText(fromA) || sentSpaceText(fromB)).as("the space's text digest never travels").isFalse();
        assertThat(after.refusals()).doesNotContainKeys("fingerprint-mismatch", "unsolicited-answer");
    }

    /**
     * Many entries differing both ways, with deletions: each side writes what the other lacks and one cancels entries
     * both held. A round each way leaves the two with equal stream hashes and the same live entries.
     */
    @Test
    void manyEntriesAndDeletionsBothWaysConvergeByAudit() throws Exception {
        Peer a = peer("a", 1, true);
        Peer b = peer("b", 2, true, "a");
        tick(6);
        ReplicatedSpace sa = a.space("tasks", clock);
        ReplicatedSpace sb = b.space("tasks", clock);
        List<EntryHandle> shared = new ArrayList<>();
        for (int i = 0; i < 120; i++) {
            shared.add(sa.write(new TaskEntry("shared-" + i, i), HOUR));
        }
        tick(4);
        assertThat(sb.knownEntries()).isEqualTo(120);

        network.partition("a", "b");
        writeMany(sa, "a-only-", 150);
        writeMany(sb, "b-only-", 80);
        for (int i = 0; i < 20; i++) {
            shared.get(i).cancel();
        }
        network.heal();
        List<Bodies.Digest> fromA = digestsSent("a", "b");
        List<Bodies.Digest> fromB = digestsSent("b", "a");
        deep(() -> {
            b.gossip().antiEntropyWith(a.id());
            a.gossip().antiEntropyWith(b.id());
        });

        assertThat(topics(sb)).isEqualTo(topics(sa));
        assertThat(topics(sa)).hasSize(120 - 20 + 150 + 80);
        assertThat(sb.knownEntries()).isEqualTo(sa.knownEntries()).isEqualTo(350);
        assertThat(hex(sb.streamHash())).isEqualTo(hex(sa.streamHash()));
        assertThat(sentSpaceText(fromA) || sentSpaceText(fromB)).isFalse();
        assertThat(b.gossip().auditSignals().idsFetched()).isGreaterThanOrEqualTo(150 + 20);
        assertThat(a.gossip().auditSignals().idsFetched()).isGreaterThanOrEqualTo(80);
    }

    /** Three members, each with entries the others lack after a partition, converge on the periodic rounds. */
    @Test
    void threeMembersConvergeOnThePeriodicRounds() throws Exception {
        Peer a = peer("a", 1, true);
        Peer b = peer("b", 2, true, "a");
        Peer c = peer("c", 3, true, "a");
        tick(8);
        ReplicatedSpace sa = a.space("tasks", clock);
        ReplicatedSpace sb = b.space("tasks", clock);
        ReplicatedSpace sc = c.space("tasks", clock);
        writeMany(sa, "base-", 60);
        tick(4);
        network.partition("a", "c");
        network.partition("b", "c");
        writeMany(sa, "a-", 40);
        writeMany(sb, "b-", 40);
        writeMany(sc, "c-", 40);
        network.heal();
        deep(() -> {
            for (int i = 0; i < 40 && !(java.util.Arrays.equals(sa.streamHash(), sb.streamHash())
                    && java.util.Arrays.equals(sa.streamHash(), sc.streamHash()) && sa.knownEntries() == 180); i++) {
                tick(1);
            }
        });
        assertThat(sa.knownEntries()).isEqualTo(180);
        assertThat(hex(sb.streamHash())).isEqualTo(hex(sa.streamHash()));
        assertThat(hex(sc.streamHash())).isEqualTo(hex(sa.streamHash()));
        assertThat(topics(sc)).isEqualTo(topics(sa));
    }

    /**
     * RE-T-03, RE-Q-SEC-04, RE-Q-REL-06: a member off the hint keeps the hashed form of v0.1.17, and a member on it
     * never sends it a field of annex I.6, while it audits with the third member that is on it.
     */
    @Test
    void aPartnerOnEachFormGetsItsOwnForm() throws Exception {
        Peer a = peer("a", 1, true);
        Peer b = peer("b", 2, false, "a");
        Peer c = peer("c", 3, true, "a");
        tick(8);
        assertThat(a.gossip().audits(b.id())).isFalse();
        assertThat(a.gossip().audits(c.id())).isTrue();
        ReplicatedSpace sa = a.space("tasks", clock);
        ReplicatedSpace sb = b.space("tasks", clock);
        ReplicatedSpace sc = c.space("tasks", clock);
        writeMany(sa, "base-", 50);
        tick(4);
        network.partition("a", "b");
        network.partition("a", "c");
        writeMany(sa, "a-", 30);
        network.heal();
        List<Bodies.Digest> toB = digestsSent("a", "b");
        List<Bodies.Digest> fromB = digestsSent("b", "a");
        deep(() -> {
            b.gossip().antiEntropyWith(a.id());
            c.gossip().antiEntropyWith(a.id());
        });
        assertThat(topics(sb)).isEqualTo(topics(sa));
        assertThat(topics(sc)).isEqualTo(topics(sa));
        assertThat(sentAuditFields(toB) || sentAuditFields(fromB)).as("no field of annex I.6 toward or from b")
                .isFalse();
        assertThat(sentSpaceText(fromB)).as("b pulled by the text form, as since v0.1.17").isTrue();
        assertThat(b.gossip().auditSignals().refusals()).doesNotContainKey("lacks-hint");
        assertThat(c.gossip().auditSignals().idsFetched()).as("c audited with a").isGreaterThanOrEqualTo(30);
    }

    /**
     * RE-T-08, RE-Q-REL-07, RE-Q-REL-09: a member whose view of a partner's card is stale (it still reads
     * {@code wire.ae=2} on a member that dropped the hint) sends it a root the partner drops and counts; the round
     * after the card is read again repairs the space by the hashed form.
     */
    @Test
    void aStaleCardIsDroppedAndCountedAndTheNextRoundFallsBack() throws Exception {
        Peer a = peer("a", 1, true);
        Peer b = peer("b", 2, false, "a");
        tick(6);
        ReplicatedSpace sa = a.space("tasks", clock);
        ReplicatedSpace sb = b.space("tasks", clock);
        writeMany(sa, "base-", 50);
        tick(4);
        network.partition("a", "b");
        sa.write(new TaskEntry("missed", 1), HOUR);
        network.heal();

        a.gossip().audit(peer -> true); // a's view of b's card is stale: it still reads the hint
        b.gossip().antiEntropyWith(a.id());
        assertThat(b.gossip().auditSignals().refusals()).as("b dropped the root it cannot read")
                .containsEntry("lacks-hint", 1L);
        assertThat(topics(sb)).as("nothing came this round").doesNotContain("missed");
        assertThat(b.node().framesDropped()).isZero();

        a.gossip().audit(peer -> !peer.equals(b.id())); // the card is read again
        b.gossip().antiEntropyWith(a.id());
        assertThat(topics(sb)).as("the next round repaired it by the hashed form").contains("missed");
        assertThat(b.gossip().auditSignals().refusals()).containsEntry("lacks-hint", 1L);
        assertThat(hex(sb.digestHash())).isEqualTo(hex(sa.digestHash()));
    }

    /**
     * RE-T-05: a partner keeps at most its bound of sessions per asker, so streams past it wait a round; the starting
     * stream moves each round, and every stream converges.
     */
    @Test
    void streamsPastTheSessionBoundWaitARoundAndAllConverge() throws Exception {
        Peer a = peer("a", 1, true);
        Peer b = peer("b", 2, true, "a");
        tick(6);
        b.gossip().auditSessions(1);
        List<ReplicatedSpace> at = new ArrayList<>();
        List<ReplicatedSpace> bt = new ArrayList<>();
        for (int s = 0; s < 3; s++) {
            at.add(a.space("s" + s, clock));
            bt.add(b.space("s" + s, clock));
        }
        network.partition("a", "b");
        for (int s = 0; s < 3; s++) {
            writeMany(bt.get(s), "s" + s + "-", 60); // past 32: each root is an inner node, which takes a session
        }
        network.heal();
        deep(() -> {
            for (int round = 0; round < 6; round++) {
                a.gossip().antiEntropyWith(b.id());
            }
        });
        for (int s = 0; s < 3; s++) {
            assertThat(at.get(s).knownEntries()).as("stream s" + s).isEqualTo(60);
            assertThat(hex(at.get(s).streamHash())).isEqualTo(hex(bt.get(s).streamHash()));
        }
        assertThat(b.gossip().auditSignals().refusals().getOrDefault("session-busy", 0L))
                .as("two streams waited in the first round").isGreaterThanOrEqualTo(2);
        assertThat(b.gossip().auditSignals().openSessions()).isLessThanOrEqualTo(1);
    }

    /**
     * RE-T-05, RE-Q-REL-09: a session closes at 64 exchanges, its stream waits a full round, the next round reopens
     * it, and the space still converges. A small budget makes the descent long: each answer carries a node or two.
     */
    @Test
    void aSessionAtItsExchangeBoundClosesWaitsARoundAndTheSpaceStillConverges() throws Exception {
        Peer a = peer("a", 1, true);
        Peer b = peer("b", 2, true, "a");
        tick(6);
        a.gossip().auditBudget(4096);
        ReplicatedSpace sa = a.space("tasks", clock);
        ReplicatedSpace sb = b.space("tasks", clock);
        network.partition("a", "b");
        writeMany(sb, "bulk-", 5_000);
        network.heal();
        deep(() -> {
            for (int round = 0; round < 12 && sa.knownEntries() < 5_000; round++) {
                a.gossip().antiEntropyWith(b.id());
                clock.advance(Duration.ofSeconds(1));
            }
        });
        Map<String, Long> refusals = b.gossip().auditSignals().refusals();
        assertThat(refusals.getOrDefault("session-capped", 0L)).as("the bound closed a session").isPositive();
        assertThat(refusals.getOrDefault("session-cooling", 0L)).as("and its stream waited a round").isPositive();
        assertThat(sa.knownEntries()).isEqualTo(5_000);
        assertThat(hex(sa.streamHash())).isEqualTo(hex(sb.streamHash()));
    }

    /**
     * RE-Q-REL-02: the answer to a fetch that arrives after its session idled is still applied, and the late copy
     * leaves the next round's audit to run as usual.
     */
    @Test
    void aLateCopyIsAppliedAfterItsSessionIdled() throws Exception {
        Peer a = peer("a", 1, true);
        Peer b = peer("b", 2, true, "a");
        tick(6);
        ReplicatedSpace sa = a.space("tasks", clock);
        ReplicatedSpace sb = b.space("tasks", clock);
        writeMany(sa, "shared-", 100);
        tick(4);
        network.partition("a", "b");
        sa.write(new TaskEntry("late", 1), HOUR);
        network.heal();

        AtomicReference<byte[]> held = new AtomicReference<>();
        network.corrupt("a", "b", frame -> {
            var envelope = WIRE.decode(frame);
            if (held.get() == null && envelope.isPresent() && envelope.get().kind() == Envelope.Kind.PULL_RESP
                    && CODEC.fromWire(envelope.get().body(), Bodies.PullResp.class).deltas().containsKey(STREAM)) {
                held.set(frame);
                return new byte[] {0}; // lost on the way: the receiver drops what does not decode
            }
            return frame;
        });
        b.gossip().antiEntropyWith(a.id());
        network.corrupt("a", "b", null);
        assertThat(held.get()).as("the fetch was answered").isNotNull();
        assertThat(topics(sb)).doesNotContain("late");

        clock.advance(Duration.ofSeconds(31)); // the session idled out
        assertThat(network.inject("a", "b", held.get())).isTrue();
        assertThat(topics(sb)).as("the late copy is applied").contains("late");
        assertThat(hex(sb.streamHash())).isEqualTo(hex(sa.streamHash()));

        network.partition("a", "b");
        sa.write(new TaskEntry("later", 2), HOUR);
        network.heal();
        b.gossip().antiEntropyWith(a.id());
        assertThat(topics(sb)).as("the next round audits as usual").contains("later");
    }

    /**
     * RE-T-06: an entry whose lease lapsed by the replica's clock leaves the audited set while it is still held and
     * still in the text digest; and a state whose id holds a control character is dropped uncounted.
     */
    @Test
    void aLapsedEntryLeavesTheAuditedSetAndAControlCharacterIdNeverEntersIt() throws Exception {
        Peer a = peer("a", 1, true);
        ReplicatedSpace sa = a.space("tasks", clock);
        byte[] empty = Digests.sha256(new byte[0]);
        assertThat(hex(sa.streamHash())).isEqualTo(hex(empty));
        EntryHandle handle = sa.write(new TaskEntry("brief", 1), Lease.of(Duration.ofMinutes(1)));
        assertThat(hex(sa.streamHash())).isNotEqualTo(hex(empty));
        clock.advance(Duration.ofSeconds(61));
        assertThat(hex(sa.streamHash())).as("lapsed: out of the audited set").isEqualTo(hex(empty));
        assertThat(new String(sa.digest(), StandardCharsets.UTF_8)).as("still in the text digest")
                .contains("e:" + handle.entryId().value() + "=");

        int known = sa.knownEntries();
        SpaceWire.EntryStateDto bad = writeWithId(a.identity(), sa, "bad\nid");
        assertThat(sa.applySyncDelta(CODEC.toBytes(new SpaceWire.SyncDelta(List.of(bad), Map.of())))).isFalse();
        assertThat(sa.knownEntries()).isEqualTo(known);
        assertThat(new String(sa.digest(), StandardCharsets.UTF_8)).doesNotContain("bad");
        assertThat(hex(sa.streamHash())).isEqualTo(hex(empty));
    }

    private SpaceWire.EntryStateDto writeWithId(PeerIdentity author, ReplicatedSpace space, String id) {
        AgentId issuer = author.agent("worker");
        HlcTimestamp issued = new HlcTimestamp(clock.millis(), 0, author.peerId().value());
        LeaseInfo lease = new LeaseInfo(issuer, clock.millis() + Duration.ofHours(1).toMillis(), LeaseKind.WRITE);
        EntryRecord record = ForgeSupport.signRecord(author, new EntryRecord(EntryId.of(id), space.id(),
                ForgeSupport.schemaNameOf(TaskEntry.class), CODEC.toBytes(new TaskEntry("bad", 1)), null, issuer,
                issued, lease, Map.of(), null));
        SpaceWire.EntryStateDto unsigned = new SpaceWire.EntryStateDto(record, author.rawPublicKey(),
                List.of(new Dot(author.peerId().value(), 1)), List.of(), issued, lease, false, null);
        return ForgeSupport.signState(author, unsigned);
    }

    /**
     * Phase 2's held acknowledgement under the audit (RE-H-03, annex I.6): a member that collected a tombstone refuses
     * the re-offer and acknowledges it, and the acknowledged line stands as the line itself, so the converged pair's
     * stream hashes are equal while the other member still holds the tombstone. Once the hold passes they differ until
     * the other collects it too, and then agree again.
     */
    @Test
    void anAcknowledgedCollectedTombstoneKeepsAConvergedPairsRootsEqual() throws Exception {
        Peer pa = peer("a", 1, true);
        Peer pb = peer("b", 2, true);
        ReplicatedSpace old = ReplicatedSpace.builder(pa.runtime(), "jobs", pa.identity(), "worker").clock(clock)
                .settleWindow(Duration.ZERO).maxLease(Duration.ofHours(1)).build();
        ReplicatedSpace fresh = pb.space("jobs", clock);
        EntryHandle handle = old.write(new TaskEntry("gone", 1), Lease.of(Duration.ofMinutes(1)));
        handle.cancel();
        assertThat(fresh.applySyncDelta(old.deltaFor(new byte[0]))).isTrue();
        assertThat(hex(fresh.streamHash())).isEqualTo(hex(old.streamHash()));

        // Past the fresh member's horizon: its longest lease is a minute, so the withdrawal keeps the drift floor of
        // SPEC §7.3, ten minutes past its lease. The old member keeps it two hours.
        clock.advance(Duration.ofMinutes(12));
        fresh.sweepIfDue();
        assertThat(fresh.knownEntries()).as("collected here").isZero();
        assertThat(old.knownEntries()).as("kept there").isEqualTo(1);
        assertThat(hex(fresh.streamHash())).isNotEqualTo(hex(old.streamHash()));

        // The audit names the entry, and the fetch by id re-offers it: refused at the door and acknowledged.
        byte[] reoffer = old.statesFor(List.of(handle.entryId().value()));
        assertThat(fresh.applySyncDelta(reoffer)).isFalse();
        assertThat(fresh.collectedReoffersAcknowledged()).isEqualTo(1);
        assertThat(hex(fresh.streamHash())).as("the acknowledged line stands as the line: roots equal")
                .isEqualTo(hex(old.streamHash()));
        assertThat(new String(fresh.auditLines(new byte[0]), StandardCharsets.UTF_8))
                .isEqualTo(new String(old.auditLines(new byte[0]), StandardCharsets.UTF_8));

        clock.advance(Duration.ofMillis(ReplicatedSpace.COLLECTED_ACK_HOLD_MILLIS + 1_000));
        fresh.sweepIfDue();
        assertThat(hex(fresh.streamHash())).as("the hold passed").isNotEqualTo(hex(old.streamHash()));
        clock.advance(Duration.ofHours(2));
        old.sweepIfDue();
        assertThat(hex(fresh.streamHash())).as("both collected: equal again").isEqualTo(hex(old.streamHash()));
    }
}
