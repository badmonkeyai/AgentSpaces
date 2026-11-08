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
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.crdt.Dot;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The record-free renew (srad/agentspaces-pdt.md §3.9, group L, PD-Q-PRF-10,
 * ISSUE-RecordlessRenew) on {@code SimNetwork}: a renew of a 50 KiB inline entry
 * publishes under 1 KiB once the writer has evidence the record replicated and
 * every member advertises {@code wire.recordless}; a receiver without the record
 * keeps the state pending, emits nothing, and gets the record by anti-entropy with
 * one WRITTEN; a member without the hint restores the full form for the whole
 * stream; a recordless state for an entry nobody holds lapses after its TTL.
 */
class RecordlessRenewTest {

    /** An inline entry just under the 64 KiB inline limit. */
    public record Note(String id, byte[] body) {
    }

    private static final Lease HOUR = Lease.of(Duration.ofHours(1));

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<Peer> peers = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zRecordless");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zRecordless", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "notes",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final CborCodec codec = CborCodec.defaultCodec();

    private record Peer(String name, PeerIdentity identity, PeerNode node, GroupRuntime runtime, ReplicatedSpace space) {
    }

    @AfterEach
    void tearDown() {
        peers.forEach(p -> {
            p.space().close();
            p.node().close();
        });
    }

    private Peer newPeer(String address, long seed, boolean hint, String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).recordless(hint).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "notes", identity, "writer")
                .clock(clock).settleWindow(Duration.ZERO).build();
        Peer peer = new Peer(address, identity, node, runtime, space);
        peers.add(peer);
        return peer;
    }

    private void tickAll() {
        for (Peer p : peers) {
            p.node().tick();
        }
        clock.advance(Duration.ofMillis(250));
    }

    private boolean runUntil(Duration max, BooleanSupplier until) throws InterruptedException {
        long deadline = System.nanoTime() + max.toNanos();
        while (!until.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            tickAll();
            Thread.sleep(2);
        }
        return true;
    }

    private static Note note(String id) {
        byte[] body = new byte[50 * 1024];
        new Random(id.hashCode()).nextBytes(body);
        return new Note(id, body);
    }

    private static long leaseEnd(ReplicatedSpace space, EntryId id) {
        return space.signedState(id).map(dto -> dto.leaseValue().expiresAtMillis()).orElse(-1L);
    }

    /** Counts the bytes of every frame one node sends another. */
    private AtomicLong tap(String from, String to) {
        AtomicLong bytes = new AtomicLong();
        network.tap(from, to, frame -> bytes.addAndGet(frame.length));
        return bytes;
    }

    // ------------------------------------------------------------ PD-L-01, PD-Q-PRF-10

    @Test
    void aRenewOfAFiftyKiBEntryPublishesUnderOneKiBOnceTheRecordIsKnownToHaveReplicated() throws Exception {
        Peer a = newPeer("a", 1, true);
        Peer b = newPeer("b", 2, true, "a");
        assertThat(runUntil(Duration.ofSeconds(10), () -> a.runtime().membership().allMembers().size() == 1
                && b.runtime().membership().allMembers().size() == 1)).isTrue();
        assertThat(a.node().wireHints()).containsEntry(PeerNode.RECORDLESS_HINT, "1");
        assertThat(a.runtime().viewAdvertisesRecordless()).isTrue();

        EntryHandle handle = a.space().write(note("n1"), HOUR);
        EntryId id = handle.entryId();
        assertThat(runUntil(Duration.ofSeconds(10), () -> b.space().read(Template.of(Note.class)).isPresent())).isTrue();

        // before the evidence a renew carries the record, so a receiver that missed the write still gets it
        AtomicLong early = tap("a", "b");
        assertThat(a.space().replicatedEvidence(id)).isFalse();
        handle.renew(Duration.ofHours(2));
        assertThat(early.get()).as("a full renew carries the 50 KiB record").isGreaterThan(50 * 1024);
        assertThat(a.space().recordlessSent()).isZero();
        network.untap("a", "b");

        // the evidence: the hashed anti-entropy rounds of v0.1.17 show no line, so the quiet round after two
        // gossip periods is the digest match (PD-L-01); on the sim it arrives within a few ticks
        assertThat(runUntil(Duration.ofSeconds(10), () -> a.space().replicatedEvidence(id)))
                .as("stretched " + a.runtime().antiEntropyStretched()).isTrue();

        AtomicLong bytes = tap("a", "b");
        long before = leaseEnd(b.space(), id);
        handle.renew(Duration.ofHours(3));
        long published = bytes.get();
        network.untap("a", "b");
        System.out.printf("recordless renew of a %d-byte entry: %d bytes on the wire a->b%n",
                codec.toBytes(note("n1")).length, published);
        assertThat(published).as("PD-Q-PRF-10: the renew publishes under 1 KiB").isGreaterThan(0).isLessThan(1024);
        assertThat(a.space().recordlessSent()).isEqualTo(1);
        assertThat(runUntil(Duration.ofSeconds(5), () -> leaseEnd(b.space(), id) > before)).isTrue();
        assertThat(leaseEnd(b.space(), id)).isEqualTo(leaseEnd(a.space(), id));
        assertThat(b.space().recordlessMerged()).isEqualTo(1);
        assertThat(b.space().recordlessHeld()).isZero();
        // the signed state kept for anti-entropy is the full form on both sides
        assertThat(a.space().signedState(id).orElseThrow().record()).isNotNull();
        assertThat(b.space().signedState(id).orElseThrow().record()).isNotNull();

        // a cancel takes the same form
        AtomicLong cancelBytes = tap("a", "b");
        handle.cancel();
        assertThat(cancelBytes.get()).isGreaterThan(0).isLessThan(1024);
        assertThat(runUntil(Duration.ofSeconds(5), () -> b.space().read(Template.of(Note.class)).isEmpty())).isTrue();
        assertThat(a.space().recordlessSent()).isEqualTo(2);
    }

    // ------------------------------------------------------------ PD-L-02

    @Test
    void aReceiverWithoutTheRecordKeepsTheStatePendingAndGetsTheRecordByAntiEntropyWithOneWritten() throws Exception {
        Peer a = newPeer("a", 1, true);
        Peer b = newPeer("b", 2, true, "a");
        Peer c = newPeer("c", 3, true, "a");
        assertThat(runUntil(Duration.ofSeconds(10), () -> a.runtime().membership().allMembers().size() == 2
                && b.runtime().membership().allMembers().size() == 2
                && c.runtime().membership().allMembers().size() == 2)).isTrue();
        List<SpaceEvent.Kind> onC = new CopyOnWriteArrayList<>();
        c.space().notify(Template.of(Note.class), event -> onC.add(event.kind()), Lease.of(Duration.ofHours(1)));

        // c misses the write: cut off from both while a writes and b replicates
        network.partition("a", "c");
        network.partition("b", "c");
        EntryHandle handle = a.space().write(note("n2"), HOUR);
        EntryId id = handle.entryId();
        assertThat(runUntil(Duration.ofSeconds(10), () -> b.space().read(Template.of(Note.class)).isPresent())).isTrue();
        assertThat(runUntil(Duration.ofSeconds(10), () -> a.space().replicatedEvidence(id))).isTrue();
        assertThat(c.space().read(Template.of(Note.class))).isEmpty();
        assertThat(a.runtime().membership().allMembers()).as("c stays in a's view through the short partition").hasSize(2);

        // the heal, then a recordless renew reaches c, which lacks the record
        network.heal();
        assertThat(a.runtime().viewAdvertisesRecordless()).isTrue();
        handle.renew(Duration.ofHours(3));
        assertThat(a.space().recordlessSent()).isEqualTo(1);
        assertThat(runUntil(Duration.ofSeconds(5), () -> c.space().recordlessHeld() == 1)).isTrue();
        assertThat(c.space().pendingRecordless()).isEqualTo(1);
        assertThat(onC).as("no event for a state without its record").isEmpty();
        assertThat(c.space().read(Template.of(Note.class))).isEmpty();

        // anti-entropy ships the whole signed state; the pending state applies and WRITTEN fires once
        assertThat(runUntil(Duration.ofSeconds(20), () -> c.space().read(Template.of(Note.class)).isPresent()
                && c.space().pendingRecordless() == 0)).isTrue();
        assertThat(onC).containsExactly(SpaceEvent.Kind.WRITTEN);
        assertThat(leaseEnd(c.space(), id)).as("c holds the renewed lease").isEqualTo(leaseEnd(a.space(), id));
        for (int i = 0; i < 8; i++) {
            tickAll();
        }
        assertThat(onC).as("WRITTEN once, however many rounds follow").containsExactly(SpaceEvent.Kind.WRITTEN);
        assertThat(c.space().signedState(id).orElseThrow().record()).isNotNull();
    }

    @Test
    void aRecordlessStateForAnEntryNobodyHoldsCreatesNothingAndLapsesAfterItsTtl() throws Exception {
        Peer a = newPeer("a", 1, true);
        Peer b = newPeer("b", 2, true, "a");
        assertThat(runUntil(Duration.ofSeconds(10), () -> a.runtime().membership().allMembers().size() == 1
                && b.runtime().membership().allMembers().size() == 1)).isTrue();
        List<SpaceEvent.Kind> onA = new CopyOnWriteArrayList<>();
        a.space().notify(Template.of(Note.class), event -> onA.add(event.kind()), Lease.of(Duration.ofHours(1)));

        // a forged recordless renew for an entry that does not exist, signed by b
        EntryId unknown = EntryId.newId();
        LeaseInfo lease = new LeaseInfo(b.identity().agent("writer"), clock.millis() + 3_600_000L, LeaseKind.WRITE);
        HlcTimestamp stamp = new HlcTimestamp(clock.millis(), 0, b.identity().peerId().value());
        SpaceWire.EntryStateDto forged = new SpaceWire.EntryStateDto(null, b.identity().rawPublicKey(),
                List.of(new Dot(b.identity().peerId().value(), 1)), List.of(), stamp, lease, false, new byte[64],
                null, null, null, null, unknown);
        b.runtime().gossip().publish("space:notes", "r:" + unknown, codec.toBytes(new SpaceWire.Delta(forged, null, null)));
        assertThat(runUntil(Duration.ofSeconds(5), () -> a.space().recordlessHeld() == 1)).isTrue();
        assertThat(a.space().pendingRecordless()).isEqualTo(1);
        assertThat(onA).isEmpty();
        assertThat(a.space().readAll(Template.of(Note.class), 4)).isEmpty();
        assertThat(a.space().signedState(unknown)).isEmpty();

        clock.advance(Duration.ofMillis(ReplicatedSpace.PENDING_STATE_TTL_MILLIS + 1_000));
        a.space().sweepNow();
        assertThat(a.space().pendingRecordless()).as("dropped after the TTL").isZero();
        assertThat(onA).isEmpty();
    }

    // ------------------------------------------------------------ PD-L-03

    @Test
    void aMemberWithoutTheHintKeepsTheFullFormForTheWholeStream() throws Exception {
        Peer a = newPeer("a", 1, true);
        Peer b = newPeer("b", 2, true, "a");
        Peer c = newPeer("c", 3, false, "a");
        assertThat(runUntil(Duration.ofSeconds(10), () -> a.runtime().membership().allMembers().size() == 2
                && b.runtime().membership().allMembers().size() == 2
                && c.runtime().membership().allMembers().size() == 2)).isTrue();
        assertThat(c.node().wireHints()).doesNotContainKey(PeerNode.RECORDLESS_HINT);
        assertThat(a.runtime().advertisesRecordless(b.identity().peerId())).isTrue();
        assertThat(a.runtime().advertisesRecordless(c.identity().peerId())).isFalse();
        assertThat(a.runtime().viewAdvertisesRecordless()).isFalse();

        EntryHandle handle = a.space().write(note("n3"), HOUR);
        EntryId id = handle.entryId();
        assertThat(runUntil(Duration.ofSeconds(10), () -> b.space().read(Template.of(Note.class)).isPresent()
                && c.space().read(Template.of(Note.class)).isPresent())).isTrue();
        assertThat(runUntil(Duration.ofSeconds(10), () -> a.space().replicatedEvidence(id))).isTrue();

        AtomicLong toB = tap("a", "b");
        AtomicLong toC = tap("a", "c");
        long before = leaseEnd(c.space(), id);
        handle.renew(Duration.ofHours(3));
        assertThat(a.space().recordlessSent()).as("one member without the hint restores the full form").isZero();
        assertThat(toB.get()).isGreaterThan(50 * 1024);
        assertThat(toC.get()).isGreaterThan(50 * 1024);
        assertThat(runUntil(Duration.ofSeconds(5), () -> leaseEnd(c.space(), id) > before && leaseEnd(b.space(), id) > before)).isTrue();
        assertThat(b.space().recordlessMerged()).isZero();
        assertThat(c.space().recordlessMerged()).isZero();
    }
}
