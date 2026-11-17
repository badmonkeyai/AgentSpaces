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
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.gossip.GossipBus;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;
import ai.badmonkey.agentspaces.space.crdt.Dot;
import ai.badmonkey.agentspaces.space.crdt.EntryState;
import ai.badmonkey.agentspaces.space.crdt.LwwRegister;
import ai.badmonkey.agentspaces.space.crdt.SpaceStateCrdt;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * RE-Q-PRF-02 (srad/replication-by-entry.md §3.2): what one audit round costs when d entries differ, at d = 1 and
 * d = 100, in spaces of n entries. Two replicas in the fabric hold the same n entries; the holder then writes d more,
 * and the other opens one round. The test counts the frames and bytes each way, the request bodies (the round trips),
 * the ids fetched and the fingerprints computed, and prints them beside §3.2's estimates: about 2 to 3 KB at d = 1 for
 * any n and about 200 to 250 KB at d = 100, in three to five round trips.
 *
 * <p>The sizes come from {@code -Dagentspaces.bench.round-sizes} (10,000 by default; 100,000 and 1,000,000 on request).
 * The shared entries are seeded into both replicas directly, unsigned, as the replication baseline seeds them, since
 * the round never fetches them; the d entries are written and signed as any write is.
 */
class AuditRoundCostTest {

    private static final CborCodec CODEC = CborCodec.defaultCodec();
    private static final WireCodec WIRE = new WireCodec(CODEC);

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    /** Frames and bytes seen on one direction, split by kind. */
    private static final class Tally {
        final AtomicLong frames = new AtomicLong();
        final AtomicLong bytes = new AtomicLong();
        final AtomicLong digestBytes = new AtomicLong();
        final AtomicLong pullBytes = new AtomicLong();
        final AtomicLong openings = new AtomicLong();
        final AtomicLong requests = new AtomicLong();
        final AtomicLong fetches = new AtomicLong();
        final AtomicLong answers = new AtomicLong();
        final AtomicLong pulls = new AtomicLong();
    }

    private Tally tally(String from, String to) {
        Tally tally = new Tally();
        network.tap(from, to, frame -> {
            tally.frames.incrementAndGet();
            tally.bytes.addAndGet(frame.length);
            WIRE.decode(frame).ifPresent(envelope -> {
                if (envelope.kind() == Envelope.Kind.DIGEST) {
                    tally.digestBytes.addAndGet(frame.length);
                    Bodies.Digest digest = CODEC.fromWire(envelope.body(), Bodies.Digest.class);
                    if (digest.hashes() != null) {
                        tally.openings.incrementAndGet();
                    }
                    if (digest.ids() != null) {
                        tally.fetches.incrementAndGet();
                    }
                    if (digest.tree() != null) {
                        boolean request = digest.tree().values().stream().flatMap(List::stream)
                                .anyMatch(node -> node.count() != null);
                        (request ? tally.requests : tally.answers).incrementAndGet();
                    }
                } else if (envelope.kind() == Envelope.Kind.PULL_RESP) {
                    tally.pulls.incrementAndGet();
                    tally.pullBytes.addAndGet(frame.length);
                }
            });
        });
        return tally;
    }

    @Test
    void oneRoundCostsAboutDTimesTheDepthNotN() throws Exception {
        int[] sizes = Arrays.stream(System.getProperty("agentspaces.bench.round-sizes", "10000").split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).mapToInt(Integer::parseInt).toArray();
        StringBuilder report = new StringBuilder("audit round cost (RE-Q-PRF-02; one run each, SimNetwork, "
                + "frames are whole signed frames):");
        for (int n : sizes) {
            for (int d : new int[] {1, 100}) {
                report.append(measure(n, d));
            }
        }
        System.out.println(report);
    }

    private String measure(int n, int d) throws Exception {
        GroupId groupId = GroupId.of("zRound" + n + "x" + d);
        GroupAdvertisement groupAd = new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, java.time.Instant.EPOCH, Duration.ofDays(1), "round",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
        String addressA = "a" + n + "x" + d;
        String addressB = "b" + n + "x" + d;
        PeerIdentity identityA = PeerIdentity.generate();
        PeerIdentity identityB = PeerIdentity.generate();
        PeerNode a = PeerNode.builder(identityA).clock(clock).randomSeed(1).build();
        PeerNode b = PeerNode.builder(identityB).clock(clock).randomSeed(2).build();
        nodes.add(a);
        nodes.add(b);
        a.listen(network.register(addressA), addressA);
        b.listen(network.register(addressB), addressB);
        GroupRuntime ra = a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime rb = b.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", addressA, 0)));
        for (int i = 0; i < 6; i++) {
            a.tick();
            b.tick();
            clock.advance(Duration.ofSeconds(1));
        }
        ReplicatedSpace sa = ReplicatedSpace.builder(ra, "tasks", identityA, "worker").clock(clock)
                .settleWindow(Duration.ZERO).build();
        ReplicatedSpace sb = ReplicatedSpace.builder(rb, "tasks", identityB, "worker").clock(clock)
                .settleWindow(Duration.ZERO).build();
        SpaceStateCrdt seeded = seed(n, sa, identityA);
        sa.mergeForTest(seeded);
        sb.mergeForTest(seeded);
        assertThat(sb.streamHash()).isEqualTo(sa.streamHash());

        network.partition(addressA, addressB);
        for (int i = 0; i < d; i++) {
            sa.write(new TaskEntry("differs-" + i, i), Lease.of(Duration.ofHours(1)));
        }
        network.heal();
        // The tries hash lazily: the fingerprints the d writes made stale are recomputed when the round's opening asks
        // for the stream hash, so counting from here counts them, and only them, besides the asker's checks.
        long fingerprintsBefore = SpaceStateCrdt.fingerprintsComputed();

        Tally ab = tally(addressA, addressB);
        Tally ba = tally(addressB, addressA);
        GossipBus.AuditSignals before = rb.gossip().auditSignals();
        long started = System.nanoTime();
        deep(() -> rb.gossip().antiEntropyWith(identityA.peerId()));
        double millis = (System.nanoTime() - started) / 1e6;
        long trieFingerprints = SpaceStateCrdt.fingerprintsComputed() - fingerprintsBefore;
        GossipBus.AuditSignals after = rb.gossip().auditSignals();
        network.untap(addressA, addressB);
        network.untap(addressB, addressA);

        assertThat(sb.knownEntries()).isEqualTo(n + d);
        assertThat(sb.streamHash()).isEqualTo(sa.streamHash());
        long checked = after.fingerprintsChecked() - before.fingerprintsChecked();
        long fetched = after.idsFetched() - before.idsFetched();
        assertThat(fetched).as("exactly the differing entries are fetched").isEqualTo(d);
        long roundTrips = ba.openings.get() + ba.requests.get() + (ba.fetches.get() > 0 ? 1 : 0);
        long auditBytes = ab.digestBytes.get() + ba.digestBytes.get();
        sa.close();
        sb.close();
        a.close();
        b.close();
        nodes.remove(a);
        nodes.remove(b);
        return String.format("%n  n=%,9d d=%3d: %3d frames %,9d bytes (audit %,9d B, fetched states %,9d B in %d "
                        + "PULL_RESP); round trips %d (1 opening, %d request bodies, %d fetch bodies); %,d trie "
                        + "fingerprints recomputed and %,d computed by the asker's checks; %,d ids fetched; %.0f ms",
                n, d, ab.frames.get() + ba.frames.get(), ab.bytes.get() + ba.bytes.get(), auditBytes,
                ab.pullBytes.get(), ab.pulls.get(), roundTrips, ba.requests.get(), ba.fetches.get(),
                trieFingerprints, checked, fetched, millis);
    }

    /** n present entries, merged pairwise so seeding costs n log n; unsigned, since the round never fetches them. */
    private SpaceStateCrdt seed(int n, ReplicatedSpace space, PeerIdentity identity) {
        AgentId issuer = identity.agentIdentity("worker").id();
        long now = clock.millis();
        List<SpaceStateCrdt> parts = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            EntryId entryId = EntryId.newId();
            LeaseInfo lease = new LeaseInfo(issuer, now + Duration.ofHours(2).toMillis(), LeaseKind.WRITE);
            HlcTimestamp stamp = new HlcTimestamp(now, 0, issuer.encoded());
            EntryRecord record = new EntryRecord(entryId, space.id(), "bench#1", new byte[] {1}, null, issuer, stamp,
                    lease, Map.of(), new byte[0]);
            EntryState state = EntryState.added(record, new Dot("bench", i + 1), new LwwRegister<>(stamp, lease));
            parts.add(SpaceStateCrdt.of(entryId, state));
        }
        while (parts.size() > 1) {
            List<SpaceStateCrdt> next = new ArrayList<>((parts.size() + 1) / 2);
            for (int i = 0; i < parts.size(); i += 2) {
                next.add(i + 1 < parts.size() ? parts.get(i).merge(parts.get(i + 1)) : parts.get(i));
            }
            parts = next;
        }
        return parts.get(0);
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
        }, "round-deep", 512L << 20);
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
}
