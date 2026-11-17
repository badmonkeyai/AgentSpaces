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
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.SpaceId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.crdt.AuditNode;
import ai.badmonkey.agentspaces.space.crdt.Dot;
import ai.badmonkey.agentspaces.space.crdt.EntryState;
import ai.badmonkey.agentspaces.space.crdt.LwwRegister;
import ai.badmonkey.agentspaces.space.crdt.SpaceStateCrdt;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The replicated space on the fingerprinted trie (srad/replication-by-entry.md phase 1): the live index beside the map
 * (RE-M-04), the claims and acknowledgements in the map's slots under one root (RE-M-07), a {@code PULL_RESP} merged as
 * one batch (RE-M-08), and takes across those batches (RE-Q-REL-05). Nothing here changes a wire byte; the digest pin
 * of {@link DigestBytesPinTest} covers that.
 */
class ReplicationByEntryTest {

    private static final CborCodec CODEC = CborCodec.defaultCodec();
    private static final Lease HOUR = Lease.of(Duration.ofHours(1));

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zEntry", PeerIdentity.generate().peerId(), GroupId.of("zEntry"),
            java.time.Instant.EPOCH, Duration.ofDays(1), "by-entry",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final List<PeerNode> nodes = new ArrayList<>();

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    /** One node of its own with the space open, so two of them hold two replicas of one space. */
    private Replica replica(String name) throws Exception {
        return replica(name, null);
    }

    /** {@link #replica(String)} with a maximum lease, as {@link DigestBytesPinTest} sets one; null for the default. */
    private Replica replica(String name, Duration maxLease) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(nodes.size() + 1).build();
        nodes.add(node);
        node.listen(network.register(name), name);
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        ReplicatedSpace.Builder builder = ReplicatedSpace.builder(runtime, "tasks", identity, "worker")
                .clock(clock).settleWindow(Duration.ZERO);
        if (maxLease != null) {
            builder.maxLease(maxLease);
        }
        return new Replica(identity, builder.build());
    }

    private record Replica(PeerIdentity identity, ReplicatedSpace space) {
    }

    private SpaceWire.EntryStateDto signedWrite(Replica author, String topic) {
        return ForgeSupport.authoredWrite(author.identity(), "worker", author.space().id(), new TaskEntry(topic, 1),
                new HlcTimestamp(clock.millis(), 0, "by-entry"), clock.millis() + Duration.ofHours(1).toMillis());
    }

    private static byte[] pullResp(List<SpaceWire.EntryStateDto> states) {
        return CODEC.toBytes(new SpaceWire.SyncDelta(states, Map.of()));
    }

    /**
     * RE-M-08: a {@code PULL_RESP} of many states merges under one hold of the lock with one generation step, and its
     * events fire after the whole batch is in, so the first listener already sees every entry. The same delta again
     * changes nothing and moves nothing: a state already held costs no copy and leaves the digest cache good.
     */
    @Test
    void aPullRespMergesAsOneBatchAndItsEventsFireAfterIt() throws Exception {
        Replica a = replica("a");
        int k = 60;
        List<SpaceWire.EntryStateDto> states = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            states.add(signedWrite(a, "batch-" + i));
        }
        byte[] delta = pullResp(states);
        List<Integer> seenAtEvent = new CopyOnWriteArrayList<>();
        a.space().notify(Template.of(TaskEntry.class),
                event -> seenAtEvent.add(a.space().knownEntries()), HOUR);
        byte[] digestBefore = a.space().digest();
        long generation = a.space().generationForTest();

        assertThat(a.space().applySyncDelta(delta)).isTrue();

        assertThat(a.space().knownEntries()).isEqualTo(k);
        assertThat(a.space().generationForTest()).as("one generation step for the whole delta")
                .isEqualTo(generation + 1);
        assertThat(seenAtEvent).as("a WRITTEN per entry").hasSize(k);
        assertThat(seenAtEvent).as("every event fired after the batch, so each saw all of it").containsOnly(k);
        assertThat(a.space().liveIndexMatchesForTest()).isTrue();

        byte[] digestAfter = a.space().digest();
        assertThat(digestAfter).isNotEqualTo(digestBefore);
        long computations = a.space().digestComputations();
        long generationAfter = a.space().generationForTest();
        assertThat(a.space().applySyncDelta(delta)).as("held already: no change").isFalse();
        assertThat(a.space().generationForTest()).isEqualTo(generationAfter);
        assertThat(a.space().digest()).isSameAs(digestAfter);
        assertThat(a.space().digestComputations()).as("the cached digest stays good").isEqualTo(computations);
        assertThat(seenAtEvent).as("no event for a state already held").hasSize(k);
    }

    /**
     * RE-M-08: the events of one {@code PULL_RESP} fire after the batch, so a listener that throws on one of them must
     * cost only its own call: the states are merged already and nothing fires those events again. Every other event of
     * the same delta still fires, in the delta's order, to every listener, the throwing one included, and the throw is
     * counted.
     */
    @Test
    void aThrowingListenerCostsOnlyItsOwnCallAndTheRestOfTheBatchStillFires() throws Exception {
        Replica a = replica("a");
        int k = 12;
        List<SpaceWire.EntryStateDto> states = new ArrayList<>();
        List<String> order = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            states.add(signedWrite(a, "ordered-" + i));
            order.add("ordered-" + i);
        }
        List<String> seenByThrower = new CopyOnWriteArrayList<>();
        List<String> seenAfter = new CopyOnWriteArrayList<>();
        // The thrower subscribes first, so its throw comes before the other listener's call for the same event.
        a.space().notify(Template.of(TaskEntry.class), event -> {
            seenByThrower.add(event.entry().topic());
            if (event.entry().topic().equals("ordered-4")) {
                throw new IllegalStateException("a listener's own fault");
            }
        }, HOUR);
        a.space().notify(Template.of(TaskEntry.class), event -> seenAfter.add(event.entry().topic()), HOUR);

        assertThat(a.space().applySyncDelta(pullResp(states))).isTrue();

        assertThat(a.space().knownEntries()).isEqualTo(k);
        assertThat(seenAfter).as("every event of the delta, in its order, to the other listener").isEqualTo(order);
        assertThat(seenByThrower).as("the thrower still hears the events after the one it threw on").isEqualTo(order);
        assertThat(a.space().listenerFailures()).isEqualTo(1);
    }

    /**
     * Only a batch's deferred events isolate listeners: a direct write fires as it did before phase 1 and as a LocalSpace
     * does, so a listener's exception reaches the writer and stops delivery to the later listeners of that event.
     */
    @Test
    void aThrowingListenerOnADirectWriteReachesTheWriter() throws Exception {
        Replica a = replica("a");
        List<String> seenAfter = new CopyOnWriteArrayList<>();
        a.space().notify(Template.of(TaskEntry.class), event -> {
            throw new IllegalStateException("meant to surface");
        }, HOUR);
        a.space().notify(Template.of(TaskEntry.class), event -> seenAfter.add(event.entry().topic()), HOUR);

        assertThatThrownBy(() -> a.space().write(new TaskEntry("direct", 1), HOUR))
                .isInstanceOf(IllegalStateException.class).hasMessage("meant to surface");
        assertThat(seenAfter).isEmpty();
        assertThat(a.space().listenerFailures()).isZero();
    }

    /**
     * RE-Q-REL-05: takes run while deltas land in batches, and every entry is taken exactly once: none is missed
     * because it landed mid-batch, and none twice because two takers saw it on either side of a batch.
     */
    @Test
    void takesAcrossBatchedMergesNeverMissAndNeverTakeTwice() throws Exception {
        Replica a = replica("a");
        int batches = 20;
        int perBatch = 40;
        int total = batches * perBatch;
        List<byte[]> deltas = new ArrayList<>();
        for (int b = 0; b < batches; b++) {
            List<SpaceWire.EntryStateDto> states = new ArrayList<>();
            for (int i = 0; i < perBatch; i++) {
                states.add(signedWrite(a, "t-" + b + "-" + i));
            }
            deltas.add(pullResp(states));
        }
        Set<String> taken = ConcurrentHashMap.newKeySet();
        AtomicInteger doubles = new AtomicInteger();
        AtomicInteger takes = new AtomicInteger();
        List<Thread> takers = new ArrayList<>();
        for (int t = 0; t < 3; t++) {
            Thread taker = new Thread(() -> {
                long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
                while (takes.get() < total && System.nanoTime() < deadline) {
                    a.space().take(Template.of(TaskEntry.class), Lease.of(Duration.ofMinutes(10)),
                            Duration.ofMillis(200)).ifPresent(entry -> {
                        takes.incrementAndGet();
                        if (!taken.add(entry.entry().topic())) {
                            doubles.incrementAndGet();
                        }
                    });
                }
            });
            taker.start();
            takers.add(taker);
        }
        for (byte[] delta : deltas) {
            assertThat(a.space().applySyncDelta(delta)).isTrue();
        }
        for (Thread taker : takers) {
            taker.join(Duration.ofSeconds(90).toMillis());
        }
        assertThat(doubles.get()).as("no entry taken twice").isZero();
        assertThat(taken).as("every entry taken once").hasSize(total);
        assertThat(a.space().claimsHeld()).isEqualTo(total);
        assertThat(a.space().liveIndexMatchesForTest()).isTrue();
    }

    /**
     * RE-M-04: the live index holds exactly the map's present entries through every kind of change: writes, merged
     * deltas, takes, completions, cancels, renewals, lapses and the sweep that collects tombstones.
     */
    @Test
    void theLiveIndexFollowsEveryChange() throws Exception {
        Replica a = replica("a");
        Random random = new Random(7);
        List<ai.badmonkey.agentspaces.api.space.EntryHandle> handles = new ArrayList<>();
        for (int round = 0; round < 200; round++) {
            switch (random.nextInt(7)) {
                case 0, 1 -> handles.add(a.space().write(new TaskEntry("w-" + round, round),
                        Lease.of(Duration.ofSeconds(5 + random.nextInt(120)))));
                case 2 -> a.space().applySyncDelta(pullResp(List.of(signedWrite(a, "m-" + round),
                        signedWrite(a, "n-" + round))));
                case 3 -> a.space().take(Template.of(TaskEntry.class), Lease.of(Duration.ofSeconds(30)),
                        Duration.ZERO).ifPresent(entry -> {
                            if (random.nextBoolean()) {
                                a.space().complete(entry);
                            }
                        });
                case 4 -> {
                    if (!handles.isEmpty()) {
                        ai.badmonkey.agentspaces.api.space.EntryHandle handle =
                                handles.remove(random.nextInt(handles.size()));
                        try {
                            if (random.nextBoolean()) {
                                handle.cancel();
                            } else {
                                handle.renew(Duration.ofMinutes(2));
                            }
                        } catch (RuntimeException gone) {
                            // a handle of an entry taken, completed or lapsed already: nothing to do
                        }
                    }
                }
                case 5 -> clock.advance(Duration.ofSeconds(random.nextInt(90)));
                default -> a.space().sweepNow();
            }
            assertThat(a.space().liveIndexMatchesForTest()).as("after round %d", round).isTrue();
        }
        clock.advance(Duration.ofHours(3));
        a.space().sweepNow();
        assertThat(a.space().liveIndexMatchesForTest()).isTrue();
    }

    /**
     * RE-M-07: a claim and an acknowledgement live in the slots of the map, so the one root of annex I.6 moves with
     * them as it moves with a state, while the text digest still lists them as {@code c:} and {@code a:} lines.
     */
    @Test
    void claimsAndAcknowledgementsLiveInTheSlotsUnderOneRoot() throws Exception {
        Replica a = replica("a");
        a.space().write(new TaskEntry("claimed", 1), HOUR);
        byte[] before = a.space().streamHash();
        String digestBefore = new String(a.space().digest(), StandardCharsets.UTF_8);
        assertThat(digestBefore).doesNotContain("c:");

        TakenEntry<TaskEntry> entry = a.space().take(Template.of(TaskEntry.class), Lease.of(Duration.ofMinutes(5)),
                Duration.ZERO).orElseThrow();
        byte[] claimed = a.space().streamHash();
        assertThat(claimed).as("the claim moved the root").isNotEqualTo(before);
        assertThat(new String(a.space().digest(), StandardCharsets.UTF_8)).contains("c:" + entry.entryId().value() + "=");

        String refused = "e:refused-entry=1a2b3c4d";
        a.space().mergeForTest(SpaceStateCrdt.empty(), Map.of(), Set.of(refused));
        assertThat(a.space().streamHash()).as("the acknowledgement moved the root").isNotEqualTo(claimed);
        String digest = new String(a.space().digest(), StandardCharsets.UTF_8);
        assertThat(digest).endsWith("a:" + refused + "\n");
        AuditNode root = a.space().auditNode(new byte[0]);
        assertThat(root.kind()).isEqualTo(AuditNode.Kind.LEAF);
        assertThat(root.lines()).as("the acknowledged line stands as the line itself").contains(refused)
                .doesNotContain("a:");
        assertThat(root.count()).as("two slots: the claimed entry and the refused one").isEqualTo(2);
    }

    /**
     * RE-M-07: the acknowledgements of a space stay bounded by {@code REFUSED_ACK_CAP}, and past it the line refused
     * least recently is forgotten first; refusing a line again makes it the most recent.
     */
    @Test
    void acknowledgementsStayCappedAndTheLeastRecentlyRefusedGoesFirst() throws Exception {
        Replica a = replica("a");
        int cap = 8192;
        Set<String> lines = new LinkedHashSet<>();
        for (int i = 0; i < cap; i++) {
            lines.add("e:refused-" + i + "=" + Integer.toHexString(i));
        }
        a.space().mergeForTest(SpaceStateCrdt.empty(), Map.of(), lines);
        // refused-0 is refused again, so refused-1 is now the least recent
        a.space().mergeForTest(SpaceStateCrdt.empty(), Map.of(), Set.of("e:refused-0=0"));
        a.space().mergeForTest(SpaceStateCrdt.empty(), Map.of(), Set.of("e:refused-new=ff"));
        String digest = new String(a.space().digest(), StandardCharsets.UTF_8);
        long acks = digest.lines().filter(line -> line.startsWith("a:")).count();
        assertThat(acks).isEqualTo(cap);
        assertThat(digest).contains("a:e:refused-0=0\n").contains("a:e:refused-new=ff\n")
                .doesNotContain("a:e:refused-1=1\n").contains("a:e:refused-2=2\n");
        assertThat(a.space().stats().acknowledgementLines()).isEqualTo(cap);
    }

    /**
     * Two replicas that reach the same states, claims and acknowledgements in different orders hold the same stream
     * hash, and the same text digest; one more line on either side parts them.
     */
    @Test
    void replicasWithTheSameLinesInAnyOrderHoldOneRoot() throws Exception {
        Replica a = replica("a");
        Replica b = replica("b");
        SpaceId spaceId = a.space().id();
        AgentId issuer = a.identity().agentIdentity("worker").id();
        List<SpaceStateCrdt> parts = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            EntryId entryId = EntryId.of("entry-" + i);
            HlcTimestamp stamp = new HlcTimestamp(clock.millis(), i, "by-entry");
            LeaseInfo lease = new LeaseInfo(issuer, clock.millis() + 3_600_000, LeaseKind.WRITE);
            EntryRecord record = new EntryRecord(entryId, spaceId, "t#1", new byte[] {(byte) i}, null, issuer, stamp,
                    lease, Map.of(), new byte[0]);
            parts.add(SpaceStateCrdt.of(entryId, EntryState.added(record, new Dot("r", i + 1),
                    new LwwRegister<>(stamp, lease))));
        }
        Map<EntryId, SpaceWire.SignedClaim> claims = DigestBytesPinTest.fullClaims(spaceId, issuer, clock.millis());
        Set<String> refused = new LinkedHashSet<>(DigestBytesPinTest.REFUSED_LINES);

        for (SpaceStateCrdt part : parts) {
            a.space().mergeForTest(part);
        }
        a.space().mergeForTest(SpaceStateCrdt.empty(), claims, refused);
        List<SpaceStateCrdt> shuffled = new ArrayList<>(parts);
        Collections.shuffle(shuffled, new Random(3));
        List<String> refusedReversed = new ArrayList<>(refused);
        Collections.reverse(refusedReversed);
        b.space().mergeForTest(SpaceStateCrdt.empty(), claims, new LinkedHashSet<>(refusedReversed));
        SpaceStateCrdt merged = SpaceStateCrdt.empty();
        for (SpaceStateCrdt part : shuffled) {
            merged = merged.merge(part);
        }
        b.space().mergeForTest(merged);

        assertThat(b.space().streamHash()).isEqualTo(a.space().streamHash());
        assertThat(b.space().digest()).isEqualTo(a.space().digest());
        b.space().mergeForTest(SpaceStateCrdt.empty(), Map.of(), Set.of("e:entry-3=0"));
        assertThat(b.space().streamHash()).isNotEqualTo(a.space().streamHash());
    }

    /**
     * {@code deltaFor} offers the same states and claims as before the trie: every state and claim the partner's
     * digest lacks, present entries first, and nothing for a partner that holds it all.
     */
    @Test
    void deltaForOffersWhatThePartnerLacks() throws Exception {
        Replica a = replica("a");
        Replica b = replica("b");
        List<SpaceWire.EntryStateDto> states = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            states.add(signedWrite(a, "d-" + i));
        }
        a.space().applySyncDelta(pullResp(states));
        a.space().take(Template.of(TaskEntry.class), Lease.of(Duration.ofMinutes(5)), Duration.ZERO).orElseThrow();
        byte[] offer = a.space().deltaFor(b.space().digest());
        SpaceWire.SyncDelta sync = CODEC.fromWire(offer, SpaceWire.SyncDelta.class);
        assertThat(sync.states()).hasSize(20);
        assertThat(sync.claims()).hasSize(1);
        assertThat(b.space().applySyncDelta(offer)).isTrue();
        assertThat(a.space().deltaFor(b.space().digest())).as("nothing left to offer").isEmpty();
        assertThat(b.space().digest()).isEqualTo(a.space().digest());
        assertThat(b.space().streamHash()).isEqualTo(a.space().streamHash());
        Map<String, Integer> topics = new HashMap<>();
        for (SpaceWire.EntryStateDto dto : sync.states()) {
            topics.merge(dto.record().entryId().value(), 1, Integer::sum);
        }
        assertThat(new HashSet<>(topics.values())).containsOnly(1);
    }

    // ---- R1 behaviour probes (reviews/RBE-REGRESSION-MATRIX.md §3: P2, P5, P6)

    /** Every event of the tasks a replica hears, in the order it hears them. */
    private static List<SpaceEvent<TaskEntry>> listen(Replica replica) {
        List<SpaceEvent<TaskEntry>> events = new CopyOnWriteArrayList<>();
        replica.space().notify(Template.of(TaskEntry.class), events::add, HOUR);
        return events;
    }

    private static List<SpaceEvent.Kind> kindsOf(List<SpaceEvent<TaskEntry>> events, EntryId entryId) {
        return events.stream().filter(e -> e.entryId().equals(entryId)).map(SpaceEvent::kind).toList();
    }

    private static List<AgentId> actorsOf(List<SpaceEvent<TaskEntry>> events, EntryId entryId) {
        return events.stream().filter(e -> e.entryId().equals(entryId)).map(SpaceEvent::actor).toList();
    }

    private static SpaceWire.SyncDelta decode(byte[] delta) {
        return delta.length == 0 ? new SpaceWire.SyncDelta(List.of(), Map.of())
                : CODEC.fromWire(delta, SpaceWire.SyncDelta.class);
    }

    /**
     * P2: a replica heard entries written, then missed their takes and completions. One {@code PULL_RESP} carries both
     * for every entry: the claim and the completed state. The replica hears TAKEN and then COMPLETED, once each, with
     * the taker as the actor, and the same delta again raises nothing.
     */
    @Test
    void aPullRespCarryingTheTakeAndTheCompletionRaisesTakenThenCompletedOnceEach() throws Exception {
        Replica holder = replica("holder");
        Replica late = replica("late");
        List<SpaceEvent<TaskEntry>> events = listen(late);
        int k = 6;
        List<EntryId> ids = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            ids.add(holder.space().write(new TaskEntry("lifecycle-" + i, i), HOUR).entryId());
        }
        assertThat(late.space().applySyncDelta(holder.space().deltaFor(late.space().digest()))).isTrue();
        for (EntryId id : ids) {
            assertThat(kindsOf(events, id)).containsExactly(SpaceEvent.Kind.WRITTEN);
        }
        AgentId taker = holder.space().writer().orElseThrow();
        for (int i = 0; i < k; i++) {
            holder.space().complete(holder.space().take(Template.of(TaskEntry.class), Lease.of(Duration.ofMinutes(10)),
                    Duration.ZERO).orElseThrow());
        }
        byte[] delta = holder.space().deltaFor(late.space().digest());
        SpaceWire.SyncDelta sync = decode(delta);
        assertThat(sync.states()).as("one completed state per entry").hasSize(k)
                .allMatch(SpaceWire.EntryStateDto::completed);
        assertThat(sync.claims()).as("and the claim it completes").hasSize(k);

        assertThat(late.space().applySyncDelta(delta)).isTrue();

        for (EntryId id : ids) {
            assertThat(kindsOf(events, id)).as("events of %s", id)
                    .containsExactly(SpaceEvent.Kind.WRITTEN, SpaceEvent.Kind.TAKEN, SpaceEvent.Kind.COMPLETED);
            assertThat(actorsOf(events, id).subList(1, 3)).as("the taker took and completed").containsOnly(taker);
        }
        assertThat(late.space().applySyncDelta(delta)).as("the same delta again changes nothing").isFalse();
        assertThat(events).as("and raises nothing").hasSize(3 * k);
        assertThat(late.space().digest()).isEqualTo(holder.space().digest());
        assertThat(late.space().listenerFailures()).isZero();
    }

    /**
     * P2: a replica that knows nothing of entries in hand learns each from one {@code PULL_RESP} carrying its written
     * state and the claim on it. Claims merge before states inside the batch, so the claim lands on an entry the
     * replica does not hold yet; the replica must still hear WRITTEN and then TAKEN, once each, so a watcher knows the
     * entry is in hand. The completions, from a later delta, raise COMPLETED once each.
     */
    @Test
    void aPullRespCarryingAnEntryAndTheClaimOnItRaisesWrittenThenTaken() throws Exception {
        Replica holder = replica("holder");
        Replica late = replica("late");
        List<SpaceEvent<TaskEntry>> events = listen(late);
        int k = 6;
        List<EntryId> ids = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            ids.add(holder.space().write(new TaskEntry("in-hand-" + i, i), HOUR).entryId());
        }
        AgentId taker = holder.space().writer().orElseThrow();
        List<TakenEntry<TaskEntry>> taken = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            taken.add(holder.space().take(Template.of(TaskEntry.class), Lease.of(Duration.ofMinutes(10)),
                    Duration.ZERO).orElseThrow());
        }
        byte[] delta = holder.space().deltaFor(late.space().digest());
        SpaceWire.SyncDelta sync = decode(delta);
        assertThat(sync.states()).hasSize(k).noneMatch(SpaceWire.EntryStateDto::completed);
        assertThat(sync.claims()).hasSize(k);

        assertThat(late.space().applySyncDelta(delta)).isTrue();

        for (EntryId id : ids) {
            assertThat(kindsOf(events, id)).as("events of %s", id)
                    .containsExactly(SpaceEvent.Kind.WRITTEN, SpaceEvent.Kind.TAKEN);
            assertThat(actorsOf(events, id).get(1)).isEqualTo(taker);
            assertThat(late.space().currentClaim(id)).isPresent();
        }
        assertThat(late.space().read(Template.of(TaskEntry.class))).as("every entry is in hand").isEmpty();

        taken.forEach(holder.space()::complete);
        assertThat(late.space().applySyncDelta(holder.space().deltaFor(late.space().digest()))).isTrue();
        assertThat(late.space().applySyncDelta(delta)).as("the older delta again changes nothing").isFalse();
        for (EntryId id : ids) {
            assertThat(kindsOf(events, id)).as("events of %s", id)
                    .containsExactly(SpaceEvent.Kind.WRITTEN, SpaceEvent.Kind.TAKEN, SpaceEvent.Kind.COMPLETED);
        }
        assertThat(late.space().digest()).isEqualTo(holder.space().digest());
    }

    /**
     * P2, all three in one: a {@code PULL_RESP} carries, for each entry, its written state, its claim and its
     * completed state, as two stored deltas forwarded together would. The replica hears WRITTEN, TAKEN and COMPLETED,
     * once each and in that order per entry, and ends with the holder's digest.
     */
    @Test
    void aPullRespCarryingWrittenTakenAndCompletedRaisesEachOnceInThatOrder() throws Exception {
        Replica holder = replica("holder");
        Replica late = replica("late");
        List<SpaceEvent<TaskEntry>> events = listen(late);
        int k = 3;
        List<EntryId> ids = new ArrayList<>();
        List<SpaceWire.EntryStateDto> written = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            EntryId id = holder.space().write(new TaskEntry("whole-life-" + i, i), HOUR).entryId();
            ids.add(id);
            written.add(holder.space().signedState(id).orElseThrow());
        }
        for (int i = 0; i < k; i++) {
            holder.space().complete(holder.space().take(Template.of(TaskEntry.class), Lease.of(Duration.ofMinutes(10)),
                    Duration.ZERO).orElseThrow());
        }
        List<SpaceWire.EntryStateDto> states = new ArrayList<>(written);
        Map<String, SpaceWire.SignedClaim> claims = new HashMap<>();
        for (EntryId id : ids) {
            states.add(holder.space().signedState(id).orElseThrow());
            claims.put(id.value(), holder.space().claimProof(id).orElseThrow());
        }
        assertThat(states.subList(k, 2 * k)).allMatch(SpaceWire.EntryStateDto::completed);

        assertThat(late.space().applySyncDelta(CODEC.toBytes(new SpaceWire.SyncDelta(states, claims)))).isTrue();

        for (EntryId id : ids) {
            assertThat(kindsOf(events, id)).as("events of %s", id)
                    .containsExactly(SpaceEvent.Kind.WRITTEN, SpaceEvent.Kind.TAKEN, SpaceEvent.Kind.COMPLETED);
        }
        assertThat(late.space().digest()).isEqualTo(holder.space().digest());
        assertThat(late.space().streamHash()).isEqualTo(holder.space().streamHash());
        assertThat(late.space().liveIndexMatchesForTest()).isTrue();
    }

    /**
     * P5: a claim-only (orphaned) slot beside the full pin of {@link DigestBytesPinTest}. While its claim lives, the
     * slot is one more audited slot and one more claim, yet no entry: nothing reads it, counts it as known or offers it
     * as a record, and a sweep before the lapse keeps it. When the claim lapses the sweep drops the slot, and the
     * replica then holds exactly the root, the digest and the pinned bytes of a replica that never had it.
     */
    @Test
    void anOrphanedClaimSlotLivesUntilItsClaimLapsesAndThenLeavesNoTrace() throws Exception {
        Replica orphaned = replica("orphaned", Duration.ofDays(1));
        Replica plain = replica("plain", Duration.ofDays(1));
        long start = clock.millis();
        AgentId issuer = orphaned.identity().agentIdentity("worker").id();
        SpaceStateCrdt state = DigestBytesPinTest.fullState(orphaned.space().id(), issuer, start);
        Map<EntryId, SpaceWire.SignedClaim> claims = DigestBytesPinTest.fullClaims(orphaned.space().id(), issuer, start);
        EntryId orphan = DigestBytesPinTest.pinId(8);
        Map<EntryId, SpaceWire.SignedClaim> withoutOrphan = new HashMap<>(claims);
        withoutOrphan.remove(orphan);
        orphaned.space().mergeForTest(state, claims, new LinkedHashSet<>(DigestBytesPinTest.REFUSED_LINES));
        plain.space().mergeForTest(state, withoutOrphan, new LinkedHashSet<>(DigestBytesPinTest.REFUSED_LINES));

        assertThat(java.util.HexFormat.of().formatHex(
                ai.badmonkey.agentspaces.common.crypto.Digests.sha256(orphaned.space().digest())))
                .as("the full pin, orphan included").isEqualTo(DigestBytesPinTest.FULL_PINNED_SHA256);
        for (int second : new int[] {0, 10}) {
            clock.advance(Duration.ofSeconds(second));
            orphaned.space().sweepNow();
            assertThat(orphaned.space().lastSweepActions().orphanClaims()).as("alive at +%ds", second).isEmpty();
            assertThat(orphaned.space().currentClaim(orphan)).isPresent();
            assertThat(orphaned.space().claimsHeld()).isEqualTo(plain.space().claimsHeld() + 1);
            assertThat(orphaned.space().knownEntries()).as("a claim alone is no entry")
                    .isEqualTo(plain.space().knownEntries());
            assertThat(orphaned.space().recordOf(orphan, true)).isEmpty();
            assertThat(orphaned.space().signedState(orphan)).isEmpty();
            assertThat(orphaned.space().auditNode(new byte[0]).count()).as("one more audited slot")
                    .isEqualTo(plain.space().auditNode(new byte[0]).count() + 1);
            assertThat(orphaned.space().streamHash()).isNotEqualTo(plain.space().streamHash());
            assertThat(new String(orphaned.space().digest(), StandardCharsets.UTF_8))
                    .contains("c:" + orphan.value() + "=");
            assertThat(orphaned.space().liveIndexMatchesForTest()).isTrue();
        }

        // The orphan's claim lapsed at 30 s; at two minutes the sweep drops it, beside the pin's own lapses.
        clock.advance(Duration.ofMinutes(2).minusSeconds(10));
        orphaned.space().sweepNow();
        assertThat(orphaned.space().lastSweepActions().orphanClaims()).containsExactly(orphan);
        plain.space().sweepNow();
        assertThat(plain.space().lastSweepActions().orphanClaims()).isEmpty();

        assertThat(orphaned.space().currentClaim(orphan)).isEmpty();
        assertThat(orphaned.space().claimsHeld()).isEqualTo(plain.space().claimsHeld());
        assertThat(orphaned.space().auditNode(new byte[0]).count())
                .isEqualTo(plain.space().auditNode(new byte[0]).count());
        assertThat(orphaned.space().streamHash()).as("no trace in the root").isEqualTo(plain.space().streamHash());
        assertThat(orphaned.space().digest()).isEqualTo(plain.space().digest());
        assertThat(java.util.HexFormat.of().formatHex(
                ai.badmonkey.agentspaces.common.crypto.Digests.sha256(orphaned.space().digest())))
                .isEqualTo(DigestBytesPinTest.FULL_SWEPT_SHA256);
        assertThat(orphaned.space().liveIndexMatchesForTest()).isTrue();
    }

    /**
     * P5: real, signed orphans. Two claims arrive alone in one {@code PULL_RESP}. The first entry's record follows
     * before its claim lapses: its slot becomes whole, it is heard written and taken, and the lapse makes it takeable
     * again (REAPPEARED) rather than dropping it. The second's record never comes before the lapse: the sweep drops
     * that slot. Anti-entropy with the holder then ends, in a few rounds, on one root.
     */
    @Test
    void anOrphanedClaimWhoseRecordFollowsBecomesWholeAndOneThatLapsesIsDroppedThenLearnedAgain() throws Exception {
        Replica holder = replica("holder");
        Replica other = replica("other");
        List<SpaceEvent<TaskEntry>> events = listen(other);
        EntryId first = holder.space().write(new TaskEntry("record-follows", 1), HOUR).entryId();
        EntryId second = holder.space().write(new TaskEntry("record-late", 2), HOUR).entryId();
        Lease brief = Lease.of(Duration.ofSeconds(30));
        holder.space().take(Template.of(TaskEntry.class), brief, Duration.ZERO).orElseThrow();
        holder.space().take(Template.of(TaskEntry.class), brief, Duration.ZERO).orElseThrow();
        Map<String, SpaceWire.SignedClaim> claims = Map.of(
                first.value(), holder.space().claimProof(first).orElseThrow(),
                second.value(), holder.space().claimProof(second).orElseThrow());

        assertThat(other.space().applySyncDelta(CODEC.toBytes(new SpaceWire.SyncDelta(List.of(), claims)))).isTrue();
        assertThat(other.space().claimsHeld()).isEqualTo(2);
        assertThat(other.space().knownEntries()).isZero();
        assertThat(other.space().auditNode(new byte[0]).count()).isEqualTo(2);
        assertThat(events).isEmpty();

        clock.advance(Duration.ofSeconds(5));
        other.space().sweepNow();
        assertThat(other.space().lastSweepActions().orphanClaims()).as("live claims stay").isEmpty();
        assertThat(other.space().applySyncDelta(CODEC.toBytes(new SpaceWire.SyncDelta(
                List.of(holder.space().signedState(first).orElseThrow()), Map.of())))).isTrue();
        assertThat(kindsOf(events, first)).containsExactly(SpaceEvent.Kind.WRITTEN, SpaceEvent.Kind.TAKEN);
        assertThat(other.space().read(Template.of(TaskEntry.class))).as("in hand").isEmpty();

        clock.advance(Duration.ofSeconds(40)); // both claims lapsed
        other.space().sweepNow();
        assertThat(other.space().lastSweepActions().orphanClaims()).containsExactly(second);
        assertThat(other.space().lastSweepActions().reappeared()).containsOnlyKeys(first);
        assertThat(other.space().currentClaim(first)).as("a whole slot keeps its claim").isPresent();
        assertThat(other.space().currentClaim(second)).isEmpty();
        assertThat(other.space().auditNode(new byte[0]).count()).isEqualTo(1);
        holder.space().sweepNow();

        int rounds = 0;
        while (rounds < 6) {
            byte[] toOther = holder.space().deltaFor(other.space().digest());
            byte[] toHolder = other.space().deltaFor(holder.space().digest());
            if (toOther.length == 0 && toHolder.length == 0) {
                break;
            }
            other.space().applySyncDelta(toOther);
            holder.space().applySyncDelta(toHolder);
            other.space().sweepNow();
            holder.space().sweepNow();
            rounds++;
        }
        assertThat(rounds).as("anti-entropy ends").isLessThan(6);
        assertThat(other.space().digest()).isEqualTo(holder.space().digest());
        assertThat(other.space().streamHash()).isEqualTo(holder.space().streamHash());
        assertThat(other.space().readAll(Template.of(TaskEntry.class), 10)).as("both takeable again").hasSize(2);
        assertThat(kindsOf(events, second)).as("the late record is news, its lapsed claim is not")
                .startsWith(SpaceEvent.Kind.WRITTEN).doesNotContain(SpaceEvent.Kind.TAKEN);
        assertThat(other.space().liveIndexMatchesForTest()).isTrue();
    }

    /**
     * P6 (the slot part; the audit's part is the next test): a refuser at and past {@code REFUSED_ACK_CAP}. A holder
     * offers honest states beside states this replica refuses and acknowledges (a logical stamp past
     * {@code LOGICAL_LIMIT}). At the cap exactly, the exchange ends after one round. Past it by K, the honest states
     * still converge in the first round and are never offered again, the acknowledgements stay at the cap, every later
     * round re-offers at most the K forgotten states, never the whole set, and a forgotten acknowledgement leaves no slot
     * behind: the root is that of a replica that holds exactly the remaining lines.
     */
    @Test
    void aRefuserPastTheAckCapKeepsItsSlotsBoundedAndReOffersOnlyTheForgotten() throws Exception {
        int cap = 8192;
        int overflow = 64;
        Replica holder = replica("holder");
        Replica refuser = replica("refuser");
        PeerIdentity stamper = PeerIdentity.generate(); // an author whose clock ran its logical counter out
        HlcTimestamp frozen = new HlcTimestamp(clock.millis(), ai.badmonkey.agentspaces.common.hlc.HybridLogicalClock
                .LOGICAL_LIMIT + 1, "frozen");
        long expires = clock.millis() + Duration.ofHours(1).toMillis();
        List<SpaceWire.EntryStateDto> honest = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            honest.add(signedWrite(holder, "honest-" + i));
        }
        List<SpaceWire.EntryStateDto> refused = new ArrayList<>();
        for (int i = 0; i < cap + overflow; i++) {
            refused.add(ForgeSupport.authoredWrite(stamper, "worker", holder.space().id(),
                    new TaskEntry("refused-" + i, i), frozen, expires));
        }
        seed(holder, honest);
        seed(holder, refused.subList(0, cap));

        // At the cap exactly: one round, then nothing left to offer.
        SpaceWire.SyncDelta firstRound = decode(holder.space().deltaFor(refuser.space().digest()));
        assertThat(firstRound.states()).hasSize(20 + cap);
        refuser.space().applySyncDelta(CODEC.toBytes(firstRound));
        assertThat(refuser.space().knownEntries()).isEqualTo(20);
        assertThat(refuser.space().stats().acknowledgementLines()).isEqualTo(cap);
        assertThat(holder.space().deltaFor(refuser.space().digest())).as("at the cap the exchange ends").isEmpty();

        // Past the cap by K.
        seed(holder, refused.subList(cap, cap + overflow));
        Set<EntryId> honestIds = new HashSet<>();
        honest.forEach(dto -> honestIds.add(dto.record().entryId()));
        for (int round = 0; round < 5; round++) {
            SpaceWire.SyncDelta offer = decode(holder.space().deltaFor(refuser.space().digest()));
            assertThat(offer.states()).as("round %d re-offers only the forgotten", round)
                    .hasSizeLessThanOrEqualTo(overflow)
                    .noneMatch(dto -> honestIds.contains(dto.record().entryId()));
            assertThat(offer.claims()).isEmpty();
            if (offer.states().isEmpty()) {
                break;
            }
            refuser.space().applySyncDelta(CODEC.toBytes(offer));
            assertThat(refuser.space().stats().acknowledgementLines()).as("round %d", round).isEqualTo(cap);
            assertThat(refuser.space().knownEntries()).isEqualTo(20);
        }
        for (EntryId id : honestIds) {
            assertThat(refuser.space().recordOf(id)).isPresent();
        }

        // No residue: the root and the digest are those of a replica holding exactly the honest states and the
        // acknowledgements still held, and the audited slots are those two sets and nothing else.
        List<String> acks = new String(refuser.space().digest(), StandardCharsets.UTF_8).lines()
                .filter(line -> line.startsWith("a:")).map(line -> line.substring(2)).toList();
        assertThat(acks).hasSize(cap);
        Replica fresh = replica("fresh");
        fresh.space().applySyncDelta(pullResp(honest));
        fresh.space().mergeForTest(SpaceStateCrdt.empty(), Map.of(), new LinkedHashSet<>(acks));
        assertThat(refuser.space().digest()).isEqualTo(fresh.space().digest());
        assertThat(refuser.space().streamHash()).isEqualTo(fresh.space().streamHash());
        assertThat(refuser.space().auditNode(new byte[0]).count()).isEqualTo(20 + cap);
        assertThat(refuser.space().liveIndexMatchesForTest()).isTrue();
    }

    /**
     * P6 (the audit's part, phase 4): the same refuser past {@code REFUSED_ACK_CAP}, now repairing by the trie audit of
     * annex I.6 over the wire. Every round after the first fetches at most the forgotten few, never the whole set; the
     * honest states converge in the first round; and once the refused states have lapsed and been collected at the
     * holder, the two replicas' audited sets are equal again, so further rounds in either direction open no audit.
     */
    @Test
    void pastTheAckCapTheAuditFetchesOnlyTheForgottenAndStopsOnceTheHolderCollects() throws Exception {
        int cap = 8192;
        int overflow = 64;
        PeerIdentity holderId = PeerIdentity.generate();
        PeerIdentity refuserId = PeerIdentity.generate();
        PeerNode holderNode = PeerNode.builder(holderId).clock(clock).randomSeed(11).build();
        nodes.add(holderNode);
        holderNode.listen(network.register("holder"), "holder");
        GroupRuntime holderRuntime = holderNode.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        PeerNode refuserNode = PeerNode.builder(refuserId).clock(clock).randomSeed(12).build();
        nodes.add(refuserNode);
        refuserNode.listen(network.register("refuser"), "refuser");
        GroupRuntime refuserRuntime = refuserNode.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new ai.badmonkey.agentspaces.api.ad.PeerAdvertisement.Endpoint("mem", "holder", 0)));
        for (int i = 0; i < 6; i++) {
            holderNode.tick();
            refuserNode.tick();
            clock.advance(Duration.ofSeconds(1));
        }
        assertThat(refuserRuntime.gossip().audits(holderId.peerId())).as("both on wire.ae=2").isTrue();
        Replica holder = new Replica(holderId, ReplicatedSpace.builder(holderRuntime, "tasks", holderId, "worker")
                .clock(clock).settleWindow(Duration.ZERO).build());
        Replica refuser = new Replica(refuserId, ReplicatedSpace.builder(refuserRuntime, "tasks", refuserId, "worker")
                .clock(clock).settleWindow(Duration.ZERO).build());

        PeerIdentity stamper = PeerIdentity.generate();
        HlcTimestamp frozen = new HlcTimestamp(clock.millis(), ai.badmonkey.agentspaces.common.hlc.HybridLogicalClock
                .LOGICAL_LIMIT + 1, "frozen");
        long expires = clock.millis() + Duration.ofMinutes(10).toMillis(); // the honest states keep an hour
        List<SpaceWire.EntryStateDto> honest = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            honest.add(signedWrite(holder, "honest-" + i));
        }
        List<SpaceWire.EntryStateDto> refused = new ArrayList<>();
        for (int i = 0; i < cap + overflow; i++) {
            refused.add(ForgeSupport.authoredWrite(stamper, "worker", holder.space().id(),
                    new TaskEntry("refused-" + i, i), frozen, expires));
        }
        seed(holder, honest);
        seed(holder, refused);

        ai.badmonkey.agentspaces.peering.gossip.GossipBus asker = refuserRuntime.gossip();
        // The first repair fetches everything the refuser lacks, in budgeted bodies that span a few rounds.
        List<Long> joining = new ArrayList<>();
        deepStack(() -> {
            for (int round = 0; round < 20 && !(refuser.space().knownEntries() == 20
                    && asker.auditSignals().fetchQueued() == 0 && round > 0); round++) {
                long before = asker.auditSignals().idsFetched();
                asker.antiEntropyWith(holderId.peerId());
                joining.add(asker.auditSignals().idsFetched() - before);
                clock.advance(Duration.ofSeconds(1));
            }
        });
        assertThat(refuser.space().knownEntries()).as("the honest states converged").isEqualTo(20);
        assertThat(refuser.space().stats().acknowledgementLines()).as("the acknowledgements stay at the cap")
                .isEqualTo(cap);
        assertThat(joining.stream().mapToLong(Long::longValue).sum()).as("the join fetched the whole set it lacked")
                .isGreaterThanOrEqualTo(20 + cap);
        // Then each round finds only the lines the cap forgot: the refuser's set and the holder's differ by them.
        List<Long> fetchedPerRound = new ArrayList<>();
        deepStack(() -> {
            for (int round = 0; round < 6; round++) {
                long before = asker.auditSignals().idsFetched();
                asker.antiEntropyWith(holderId.peerId());
                fetchedPerRound.add(asker.auditSignals().idsFetched() - before);
                clock.advance(Duration.ofSeconds(1));
            }
        });
        for (int round = 0; round < fetchedPerRound.size(); round++) {
            assertThat(fetchedPerRound.get(round)).as("round %d fetches at most the forgotten (join took %s)", round,
                    joining).isLessThanOrEqualTo(2L * overflow);
        }
        assertThat(refuser.space().knownEntries()).isEqualTo(20);

        // The refused states lapse at the holder (out of its audited set at once, RE-T-06) and are collected after
        // the lapse grace; the refuser's acknowledgements of them then name nothing anyone holds.
        clock.advance(Duration.ofMinutes(20));
        holder.space().stats(); // reads only; the rounds below sweep
        deepStack(() -> {
            for (int round = 0; round < 4; round++) {
                asker.antiEntropyWith(holderId.peerId());
                holderRuntime.gossip().antiEntropyWith(refuserId.peerId());
                clock.advance(Duration.ofSeconds(1));
            }
        });
        long refuserAudits = refuserRuntime.gossip().auditSignals().roundsAudited();
        long holderAudits = holderRuntime.gossip().auditSignals().roundsAudited();
        long holderFetched = holderRuntime.gossip().auditSignals().idsFetched();
        deepStack(() -> {
            for (int round = 0; round < 6; round++) {
                asker.antiEntropyWith(holderId.peerId());
                holderRuntime.gossip().antiEntropyWith(refuserId.peerId());
                clock.advance(Duration.ofSeconds(1));
            }
        });
        assertThat(holder.space().knownEntries()).as("the holder collected the refused states").isEqualTo(20);
        Set<String> refuserLines = new HashSet<>(List.of(new String(refuser.space().auditLines(new byte[0]),
                StandardCharsets.UTF_8).split("\n")));
        Set<String> holderLines = new HashSet<>(List.of(new String(holder.space().auditLines(new byte[0]),
                StandardCharsets.UTF_8).split("\n")));
        Set<String> onlyRefuser = new HashSet<>(refuserLines);
        onlyRefuser.removeAll(holderLines);
        Set<String> onlyHolder = new HashSet<>(holderLines);
        onlyHolder.removeAll(refuserLines);
        assertThat(java.util.HexFormat.of().formatHex(refuser.space().streamHash()))
                .as("the audited sets are equal again; only the refuser holds %d lines %s, only the holder %d %s",
                        onlyRefuser.size(), onlyRefuser.stream().limit(3).toList(), onlyHolder.size(),
                        onlyHolder.stream().limit(3).toList())
                .isEqualTo(java.util.HexFormat.of().formatHex(holder.space().streamHash()));
        assertThat(refuserRuntime.gossip().auditSignals().roundsAudited()).as("no further audit, as the partner")
                .isEqualTo(refuserAudits);
        assertThat(holderRuntime.gossip().auditSignals().roundsAudited()).isEqualTo(holderAudits);
        assertThat(holderRuntime.gossip().auditSignals().idsFetched()).as("and no further fetch")
                .isEqualTo(holderFetched);
    }

    /** Runs work on a thread with a deep stack: the simulated fabric nests every exchange of an audit in its call. */
    private static void deepStack(ThrowingWork work) throws Exception {
        java.util.concurrent.atomic.AtomicReference<Throwable> failed = new java.util.concurrent.atomic.AtomicReference<>();
        Thread thread = new Thread(null, () -> {
            try {
                work.run();
            } catch (Throwable t) {
                failed.set(t);
            }
        }, "p6-audit", 512L << 20);
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
    private interface ThrowingWork {
        void run() throws Exception;
    }

    /** Seeds a replica with signed states as held, unverified, so it offers them as any holder would. */
    private static void seed(Replica replica, List<SpaceWire.EntryStateDto> states) {
        SpaceStateCrdt held = SpaceStateCrdt.empty();
        for (SpaceWire.EntryStateDto dto : states) {
            held = held.merge(dto.record().entryId(), dto.toState());
        }
        replica.space().mergeForTest(held, states);
    }
}
