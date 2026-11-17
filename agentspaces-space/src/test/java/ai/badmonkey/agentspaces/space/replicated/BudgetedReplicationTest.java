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
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.gossip.GossipBus;
import ai.badmonkey.agentspaces.peering.gossip.ReconcilableState;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The fetch and the budget on spaces (srad/replication-by-entry.md phase 4, step 4b; SPEC annex I.6; the matrix's
 * probes P24, P26, P27, P29 and P35): a budgeted answer's order, newest first with present states before tombstones and
 * a claim beside its state; a get-space under a budget as a run of bounded pulls; a replacement pair split by a budget
 * that never leaves a reader's view empty; a join under a small budget in many bounded answers; a revoked writer's
 * entries never adopted by a late joiner whose pulls are budgeted; and the door on the fetch path.
 *
 * <p>The fabric delivers on the sender's thread, so a whole audit and its fetches run inside the call that opens the
 * round; the tests that drive many exchanges run on a thread with a deep stack for that reason.
 */
class BudgetedReplicationTest {

    private static final CborCodec CODEC = CborCodec.defaultCodec();
    private static final WireCodec WIRE = new WireCodec(CODEC);
    private static final Lease HOUR = Lease.of(Duration.ofHours(1));
    private static final String STREAM = "space:tasks";
    private static final long SMALL = 4096;

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final PeerIdentity founder = PeerIdentity.generate();
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zBudget", founder.peerId(), GroupId.of("zBudget"),
            java.time.Instant.EPOCH, Duration.ofDays(1), "budget",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(String address, PeerIdentity identity, PeerNode node, GroupRuntime runtime) {
        ReplicatedSpace space(TestClock clock) {
            return ReplicatedSpace.builder(runtime, "tasks", identity, "worker").clock(clock)
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

    private Peer peer(String address, long seed, long budget, String... seeds) throws IOException {
        return peer(PeerIdentity.generate(), address, seed, budget, seeds);
    }

    private Peer peer(PeerIdentity identity, String address, long seed, long budget, String... seeds)
            throws IOException {
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).auditBudget(budget).build();
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
        }, "budget-deep", 1L << 30);
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

    /** A PULL_RESP seen on the wire: its body's bytes and its decoded body. */
    private record Seen(int bytes, Bodies.PullResp resp) {
        SpaceWire.SyncDelta delta() {
            byte[] bytes = resp.deltas().get(STREAM);
            return bytes == null ? null : CODEC.fromWire(bytes, SpaceWire.SyncDelta.class);
        }
    }

    private List<Seen> pullRespsSent(String from, String to) {
        List<Seen> seen = new CopyOnWriteArrayList<>();
        network.tap(from, to, frame -> WIRE.decode(frame).ifPresent(envelope -> {
            if (envelope.kind() == Envelope.Kind.PULL_RESP) {
                seen.add(new Seen(envelope.body().length, CODEC.fromWire(envelope.body(), Bodies.PullResp.class)));
            }
        }));
        return seen;
    }

    private static SpaceWire.SyncDelta decode(byte[] delta) {
        return CODEC.fromWire(delta, SpaceWire.SyncDelta.class);
    }

    private static boolean present(SpaceWire.EntryStateDto dto) {
        return dto.toState().present();
    }

    private static List<String> idsOf(SpaceWire.SyncDelta delta) {
        return delta.states().stream().map(dto -> dto.entryIdOf().value()).toList();
    }

    private static String text(int length, char fill) {
        return String.valueOf(fill).repeat(length);
    }

    private static int live(ReplicatedSpace space) {
        return space.readAll(Template.of(TaskEntry.class), 100_000).size();
    }

    /**
     * RE-I-01, RE-I-06 and the core of P24: an answer to a fetch puts every present state before every tombstone, each
     * group newest first by its lease stamp, and a claim beside its entry's state; a budget cuts it to a prefix of that
     * order, every claim still beside its state, and counts what it left out.
     */
    @Test
    void aBudgetedAnswerPutsPresentStatesFirstNewestFirstAndEveryClaimBesideItsState() throws Exception {
        Peer a = peer("a", 1, SMALL);
        ReplicatedSpace sa = a.space(clock);
        List<EntryHandle> handles = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            handles.add(sa.write(new TaskEntry("t" + i + text(300, 'x'), i), HOUR));
            clock.advance(Duration.ofSeconds(1));
        }
        handles.get(2).cancel();
        clock.advance(Duration.ofSeconds(1));
        TakenEntry<TaskEntry> done = sa.take(Template.of(TaskEntry.class), Lease.of(Duration.ofMinutes(5)),
                Duration.ZERO).orElseThrow();
        sa.complete(done);
        clock.advance(Duration.ofSeconds(1));
        TakenEntry<TaskEntry> held = sa.take(Template.of(TaskEntry.class), Lease.of(Duration.ofMinutes(5)),
                Duration.ZERO).orElseThrow();
        List<String> ids = handles.stream().map(h -> h.entryId().value()).toList();

        ReconcilableState.Bounded whole = sa.statesFor(ids, Long.MAX_VALUE, true);
        assertThat(whole.left()).isZero();
        SpaceWire.SyncDelta all = decode(whole.delta());
        assertThat(all.states()).hasSize(8);
        int tombstones = 0;
        for (int i = 0; i < all.states().size(); i++) {
            SpaceWire.EntryStateDto dto = all.states().get(i);
            if (!present(dto)) {
                tombstones++;
            } else {
                assertThat(tombstones).as("a present state never follows a tombstone").isZero();
            }
            if (i > 0 && present(all.states().get(i - 1)) == present(dto)) {
                assertThat(all.states().get(i - 1).leaseStamp().compareTo(dto.leaseStamp()))
                        .as("newest first by the lease stamp").isGreaterThanOrEqualTo(0);
            }
        }
        assertThat(tombstones).as("the cancelled entry and the completed one").isEqualTo(2);
        assertThat(all.claims()).containsKeys(done.entryId().value(), held.entryId().value());

        List<String> order = idsOf(all);
        int sizes = 0;
        for (long budget = 600; budget <= whole.delta().length + 600; budget += 450) {
            ReconcilableState.Bounded cut = sa.statesFor(ids, budget, true);
            SpaceWire.SyncDelta part = decode(cut.delta());
            List<String> got = idsOf(part);
            assertThat(order.subList(0, got.size())).as("a prefix of the order").isEqualTo(got);
            assertThat(cut.left()).isEqualTo(8 - got.size());
            if (got.size() > 1) {
                assertThat(cut.delta().length).as("within the budget").isLessThanOrEqualTo((int) budget);
            }
            for (String claimed : part.claims().keySet()) {
                assertThat(got).as("a claim travels with its entry's state").contains(claimed);
            }
            for (String id : got) {
                if (all.claims().containsKey(id)) {
                    assertThat(part.claims()).as("and the state with its claim").containsKey(id);
                }
            }
            sizes = Math.max(sizes, got.size());
        }
        assertThat(sizes).isEqualTo(8);
    }

    /**
     * P29: an answer always carries its first item, so a state near the inline payload ceiling travels alone under a
     * 4 KiB budget, and one that is not first is left out and counted.
     */
    @Test
    void aFirstStateLargerThanTheBudgetTravelsAlone() throws Exception {
        Peer a = peer("a", 1, SMALL);
        ReplicatedSpace sa = a.space(clock);
        EntryHandle small = sa.write(new TaskEntry("small", 1), HOUR);
        clock.advance(Duration.ofSeconds(1));
        EntryHandle big = sa.write(new TaskEntry(text(60_000, 'b'), 2), HOUR);
        List<String> ids = List.of(small.entryId().value(), big.entryId().value());

        ReconcilableState.Bounded first = sa.statesFor(ids, SMALL, true);
        assertThat(idsOf(decode(first.delta()))).containsExactly(big.entryId().value());
        assertThat(first.delta().length).isGreaterThan(60_000);
        assertThat(first.left()).isEqualTo(1);
        ReconcilableState.Bounded notFirst = sa.statesFor(ids, SMALL, false);
        assertThat(notFirst.delta()).isEmpty();
        assertThat(notFirst.left()).isEqualTo(2);

        // Over the wire: a member with a 4 KiB budget fetches it in an answer of its own and holds both.
        Peer b = peer("b", 2, SMALL, "a");
        tick(6);
        ReplicatedSpace sb = b.space(clock);
        List<Seen> answers = pullRespsSent("a", "b");
        deep(() -> b.gossip().antiEntropyWith(a.id()));
        assertThat(sb.knownEntries()).isEqualTo(2);
        long withBig = answers.stream().filter(s -> s.delta() != null
                && idsOf(s.delta()).contains(big.entryId().value())).count();
        assertThat(withBig).as("sent once, alone").isEqualTo(1);
        assertThat(answers.stream().filter(s -> s.delta() != null
                && idsOf(s.delta()).contains(big.entryId().value())).findFirst().orElseThrow().delta().states())
                .hasSize(1);
    }

    /**
     * RE-I-04: a get-space under a budget is a run of bounded pulls, each within the budget and newest first, which a
     * replica continues with its own grown digest until nothing is left; and a budgeted body of the text form, as a
     * client that never reads a DIGEST would send it, is answered with a remainder.
     */
    @Test
    void aBudgetedGetSpaceIsARunOfBoundedPullsNewestFirst() throws Exception {
        Peer a = peer("a", 1, SMALL);
        Peer b = peer("b", 2, SMALL, "a");
        tick(6);
        ReplicatedSpace sa = a.space(clock);
        ReplicatedSpace sb = b.space(clock);
        network.partition("a", "b");
        for (int i = 0; i < 150; i++) {
            sa.write(new TaskEntry("g" + i + text(200, 'g'), i), HOUR);
            clock.advance(Duration.ofMillis(10));
        }

        int pulls = 0;
        ReconcilableState.Bounded pull;
        do {
            pull = sa.deltaFor(sb.digest(), 8192, true);
            if (pull.delta().length == 0) {
                break;
            }
            SpaceWire.SyncDelta delta = decode(pull.delta());
            assertThat(pull.delta().length).isLessThanOrEqualTo(8192);
            if (pulls == 0) {
                assertThat(delta.states()).allSatisfy(dto -> assertThat(present(dto)).isTrue());
                assertThat(((TaskEntry) CODEC.fromBytes(delta.states().get(0).record().payload(), TaskEntry.class))
                        .priority()).as("the newest first").isEqualTo(149);
            }
            sb.applySyncDelta(pull.delta());
            pulls++;
        } while (pull.left() > 0);
        assertThat(pulls).isGreaterThanOrEqualTo(5);
        assertThat(sb.knownEntries()).isEqualTo(150);
        assertThat(sa.deltaFor(sb.digest(), 8192, true).left()).isZero();

        network.heal();
        List<Seen> answers = pullRespsSent("a", "b");
        a.gossip().onDigest(b.id(), new Bodies.Digest(Map.of(STREAM, new byte[0]), null, null, null, null, 8192L,
                null));
        assertThat(answers).hasSize(1);
        assertThat(answers.get(0).bytes()).isLessThanOrEqualTo(8192);
        assertThat(answers.get(0).resp().remainder()).containsKey(STREAM);
        assertThat(answers.get(0).resp().remainder().get(STREAM) + answers.get(0).delta().states().size())
                .isEqualTo(150);
    }

    /**
     * P27, RE-I-06: a replacement (a new revision written, the old one cancelled) reaches a member that held the old
     * one under a budget that fits one state. The new revision comes in the first answer and the cancel in the next,
     * so the member's reader always sees at least one of the two. DesiredPublisher and Reporters, which replace this
     * way, are platform callers and checked at R2.
     */
    @Test
    void aReplacementSplitByTheBudgetNeverLeavesTheReaderEmpty() throws Exception {
        Peer a = peer("a", 1, SMALL);
        Peer b = peer("b", 2, SMALL, "a");
        tick(6);
        ReplicatedSpace sa = a.space(clock);
        ReplicatedSpace sb = b.space(clock);
        EntryHandle old = sa.write(new TaskEntry("old" + text(2_000, 'o'), 1), HOUR);
        tick(3);
        assertThat(live(sb)).isEqualTo(1);

        network.partition("a", "b");
        clock.advance(Duration.ofSeconds(1));
        EntryHandle fresh = sa.write(new TaskEntry("new" + text(2_000, 'n'), 2), HOUR);
        old.cancel();
        network.heal();

        List<Integer> seenLive = new CopyOnWriteArrayList<>();
        List<SpaceEvent.Kind> kinds = new CopyOnWriteArrayList<>();
        sb.notify(Template.of(TaskEntry.class), event -> {
            kinds.add(event.kind());
            seenLive.add(live(sb));
        }, HOUR);
        List<Seen> answers = pullRespsSent("a", "b");
        deep(() -> b.gossip().antiEntropyWith(a.id()));

        List<Seen> fetched = answers.stream().filter(s -> s.delta() != null).toList();
        assertThat(fetched).as("the budget split the pair").hasSizeGreaterThanOrEqualTo(2);
        assertThat(idsOf(fetched.get(0).delta())).containsExactly(fresh.entryId().value());
        assertThat(fetched.get(0).resp().remainder()).containsEntry(STREAM, 1L);
        assertThat(seenLive).as("the reader's view is never empty").isNotEmpty().allSatisfy(n ->
                assertThat(n).isPositive());
        assertThat(sb.readAll(Template.of(TaskEntry.class), 10)).extracting(TaskEntry::priority).containsExactly(2);
        assertThat(b.gossip().auditSignals().fetchContinued()).isPositive();
    }

    /**
     * RE-I-05 in the fabric, P24 and P25's core: a member joins a space of 400 entries, some taken and completed and
     * some taken and held, under a 4 KiB budget. Every answer stays within the budget, the join runs in many answers
     * continued at once, every taken entry's state carries its claim beside it and no claim travels ahead of its
     * record, and the joiner ends with the holder's set.
     */
    @Test
    void aJoinUnderASmallBudgetRunsInManyBoundedAnswersAndKeepsEachClaimBesideItsCompletion() throws Exception {
        Peer a = peer("a", 1, SMALL);
        tick(2);
        ReplicatedSpace sa = a.space(clock);
        for (int i = 0; i < 400; i++) {
            sa.write(new TaskEntry("j" + i + text(150, 'j'), i), HOUR);
        }
        Set<String> completed = new HashSet<>();
        for (int i = 0; i < 6; i++) {
            TakenEntry<TaskEntry> taken = sa.take(Template.of(TaskEntry.class), Lease.of(Duration.ofMinutes(5)),
                    Duration.ZERO).orElseThrow();
            sa.complete(taken);
            completed.add(taken.entryId().value());
        }
        // P25's core: entries taken and not completed, whose committed claim must never arrive ahead of the record.
        Set<String> held = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            held.add(sa.take(Template.of(TaskEntry.class), Lease.of(Duration.ofMinutes(30)), Duration.ZERO)
                    .orElseThrow().entryId().value());
        }

        Peer b = peer("b", 2, SMALL, "a");
        List<Seen> answers = pullRespsSent("a", "b");
        AtomicReference<ReplicatedSpace> sb = new AtomicReference<>();
        deep(() -> {
            tick(4);
            sb.set(b.space(clock));
            for (int i = 0; i < 20 && sb.get().knownEntries() < 400; i++) {
                tick(1);
            }
        });

        assertThat(sb.get().knownEntries()).isEqualTo(400);
        assertThat(live(sb.get())).isEqualTo(live(sa));
        assertThat(sb.get().streamHash()).isEqualTo(sa.streamHash());
        List<Seen> fetched = answers.stream().filter(s -> s.delta() != null).toList();
        assertThat(fetched.size()).as("many bounded answers").isGreaterThanOrEqualTo(10);
        for (Seen seen : fetched) {
            if (seen.delta().states().size() > 1) {
                assertThat(seen.bytes()).as("each within the budget").isLessThanOrEqualTo((int) SMALL);
            }
            for (SpaceWire.EntryStateDto dto : seen.delta().states()) {
                if (completed.contains(dto.entryIdOf().value()) || held.contains(dto.entryIdOf().value())) {
                    assertThat(seen.delta().claims()).as("a taken entry's state travels with its claim")
                            .containsKey(dto.entryIdOf().value());
                }
            }
            assertThat(idsOf(seen.delta())).as("and a claim never ahead of its record")
                    .containsAll(seen.delta().claims().keySet());
        }
        assertThat(b.gossip().auditSignals().remainderReceived()).isPositive();
    }

    /**
     * P26: a late joiner whose pulls are budgeted gets a revoked writer's entries from a holder that kept them, and
     * the revocation with them. The revocation registry answers before the space's fetch, so the joiner never adopts
     * one of those entries, not even for a moment, and acknowledges each in its own trie so the two converge.
     */
    @Test
    void aLateJoinerUnderABudgetNeverAdoptsARevokedWritersEntries() throws Exception {
        Peer f = peer(founder, "f", 1, SMALL);
        Peer h = peer("h", 2, SMALL, "f");
        Peer v = peer("v", 3, SMALL, "f");
        tick(6);
        ReplicatedSpace sf = f.space(clock);
        ReplicatedSpace sh = h.space(clock);
        ReplicatedSpace sv = v.space(clock);
        for (int i = 0; i < 30; i++) {
            sv.write(new TaskEntry("revoked" + i + text(300, 'r'), i), HOUR);
        }
        sh.write(new TaskEntry("honest", 100), HOUR);
        tick(6);
        assertThat(sh.knownEntries()).isEqualTo(31);
        assertThat(f.runtime().revoke(v.id(), "compromised")).isPresent();
        tick(4);
        assertThat(h.runtime().revocations().revoked(v.id())).isTrue();

        Peer late = peer("l", 4, SMALL, "h");
        AtomicInteger adopted = new AtomicInteger();
        AtomicReference<ReplicatedSpace> sl = new AtomicReference<>();
        deep(() -> {
            tick(2);
            sl.set(late.space(clock));
            sl.get().notify(Template.of(TaskEntry.class), event -> {
                if (event.entry() != null && event.entry().topic().startsWith("revoked")) {
                    adopted.incrementAndGet();
                }
            }, HOUR);
            tick(12);
        });

        assertThat(adopted.get()).as("no event for a revoked writer's entry").isZero();
        assertThat(sl.get().readAll(Template.of(TaskEntry.class), 100)).extracting(TaskEntry::topic)
                .containsExactly("honest");
        assertThat(late.gossip().auditSignals().idsFetched()).as("the space came by the fetch").isPositive();
        assertThat(sf.knownEntries()).isEqualTo(31);
    }

    /**
     * P35, RE-Q-SEC-01: a forged state served in answer to a fetch by id meets the same door as a rumor or a pull. An
     * attacker whose audited set holds a line the victim lacks draws a fetch, answers it with a forged completion of the
     * victim's entry, and the victim keeps its entry.
     */
    @Test
    void aForgedStateAnsweringAFetchIsRefusedAtTheDoor() throws Exception {
        Peer a = peer("a", 1, SMALL);
        Peer b = peer("b", 2, SMALL, "a");
        Peer scratch = peer("s", 3, SMALL, "a");
        tick(6);
        ReplicatedSpace sa = a.space(clock);
        ReplicatedSpace sb = b.space(clock);
        ReplicatedSpace ss = scratch.space(clock);
        EntryHandle victim = sa.write(new TaskEntry("victim", 1), HOUR);
        tick(4);
        assertThat(sb.knownEntries()).isEqualTo(1);
        assertThat(ss.knownEntries()).isEqualTo(1);
        network.partition("s", "a");
        network.partition("s", "b");
        ss.write(new TaskEntry("bait", 2), HOUR);

        SpaceWire.EntryStateDto real = decode(sb.statesFor(List.of(victim.entryId().value()))).states().get(0);
        PeerIdentity evilIdentity = PeerIdentity.generate();
        SpaceWire.EntryStateDto forged = ForgeSupport.signState(evilIdentity,
                new SpaceWire.EntryStateDto(real.record(), real.issuerPublicKey(), real.adds(), real.removes(),
                        real.leaseStamp(), real.leaseValue(), true, null));
        byte[] poisoned = CODEC.toBytes(new SpaceWire.SyncDelta(List.of(forged), Map.of()));
        AtomicInteger served = new AtomicInteger();
        Peer evil = peer(evilIdentity, "e", 4, SMALL, "a");
        // The attacker's audited set is the scratch replica's, which holds the bait b lacks; its fetch answers lie.
        evil.gossip().reconcile(STREAM, new ReconcilableState() {
            @Override
            public byte[] digest() {
                return ss.digest();
            }

            @Override
            public byte[] streamHash() {
                return ss.streamHash();
            }

            @Override
            public AuditNode auditNode(byte[] path) {
                ai.badmonkey.agentspaces.space.crdt.AuditNode node = ss.auditNode(path);
                return new AuditNode(node.count(),
                        node.kind() == ai.badmonkey.agentspaces.space.crdt.AuditNode.Kind.INNER, node.preimage());
            }

            @Override
            public byte[] auditLines(byte[] path) {
                return ss.auditLines(path);
            }

            @Override
            public byte[] statesFor(Collection<String> ids) {
                served.incrementAndGet();
                return poisoned;
            }

            @Override
            public Bounded statesFor(Collection<String> ids, long budget, boolean first) {
                served.incrementAndGet();
                return new Bounded(poisoned, 0);
            }

            @Override
            public byte[] deltaFor(byte[] remoteDigest) {
                return poisoned;
            }

            @Override
            public void applyDelta(byte[] delta) {
            }
        });
        tick(4);

        deep(() -> b.gossip().antiEntropyWith(evil.id()));

        assertThat(served.get()).as("the victim fetched from the attacker").isPositive();
        assertThat(sb.read(Template.of(TaskEntry.class))).as("the forged completion was refused").isPresent();
        assertThat(sb.readAll(Template.of(TaskEntry.class), 10)).extracting(TaskEntry::topic).contains("victim");
        assertThat(sa.read(Template.of(TaskEntry.class))).isPresent();
    }

    /** The answer of a fetch names each state and claim it carried, which the asker reads to re-queue the rest. */
    @Test
    void aMergedFetchNamesWhatItCarried() throws Exception {
        Peer a = peer("a", 1, SMALL);
        Peer b = peer("b", 2, SMALL, "a");
        tick(6);
        ReplicatedSpace sa = a.space(clock);
        ReplicatedSpace sb = b.space(clock);
        network.partition("a", "b");
        EntryHandle one = sa.write(new TaskEntry("one", 1), HOUR);
        EntryHandle two = sa.write(new TaskEntry("two", 2), HOUR);
        byte[] delta = sa.statesFor(List.of(one.entryId().value(), two.entryId().value(), EntryId.newId().value()));

        ReconcilableState.Applied applied = sb.applySyncDeltaNamed(delta);
        assertThat(applied.changed()).isTrue();
        assertThat(applied.ids()).containsExactlyInAnyOrder(one.entryId().value(), two.entryId().value());
        assertThat(sb.knownEntries()).isEqualTo(2);
    }
}
