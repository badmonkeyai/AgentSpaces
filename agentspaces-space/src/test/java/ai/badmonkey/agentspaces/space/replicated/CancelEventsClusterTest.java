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
import ai.badmonkey.agentspaces.api.error.LeaseExpiredException;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Core cancel events (2026-11-09; enterprise-platform CQRS-BUILD.md, the console issuer's gate, G7): a cancel in a
 * replicated space withdraws the entry on every member and raises {@code EXPIRED}, the kind {@code LocalSpace} raises
 * for a cancel, once on each member that held the entry, the cancelling one included. A redelivered cancel raises
 * nothing more, and a cancel that races a take, a completion, or a lease lapse still leaves one {@code EXPIRED} at
 * most per member and the entry gone everywhere. Three peers on a {@code SimNetwork}; ticks are manual.
 */
class CancelEventsClusterTest {

    private static final Lease MINUTES_30 = Lease.of(Duration.ofMinutes(30));
    private static final Lease MINUTES_10 = Lease.of(Duration.ofMinutes(10));
    private static final String STREAM = "space:tasks";

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<String> addresses = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zCancel");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zCancel", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "cancel",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(String address, PeerIdentity identity, PeerNode node, ReplicatedSpace space,
                        List<SpaceEvent<TaskEntry>> events) {

        long count(SpaceEvent.Kind kind, EntryId entryId) {
            return events.stream().filter(e -> e.kind() == kind && e.entryId().equals(entryId)).count();
        }
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Peer newPeer(String address, long seed, String... seedAddresses) throws IOException {
        return newPeer(address, seed, Duration.ZERO, seedAddresses);
    }

    private Peer newPeer(String address, long seed, Duration settleWindow, String... seedAddresses)
            throws IOException {
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
                .settleWindow(settleWindow)
                .build();
        List<SpaceEvent<TaskEntry>> events = new CopyOnWriteArrayList<>();
        space.notify(Template.of(TaskEntry.class), events::add, Lease.of(Duration.ofHours(1)));
        nodes.add(node);
        addresses.add(address);
        return new Peer(address, identity, node, space, events);
    }

    private List<Peer> cluster() throws IOException {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        Peer c = newPeer("c", 3, "a");
        tickAll(6);
        return List.of(a, b, c);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // anti-entropy paces itself by the gossip period
        }
    }

    /** Every frame between any two peers queues until {@link #releaseAll}: the peers act concurrently. */
    private void holdAll() {
        for (String from : addresses) {
            for (String to : addresses) {
                if (!from.equals(to)) {
                    network.hold(from, to);
                }
            }
        }
    }

    private void releaseAll(SimNetwork.Order order) {
        for (String from : addresses) {
            for (String to : addresses) {
                if (!from.equals(to)) {
                    network.release(from, to, order, 7);
                }
            }
        }
    }

    private static void assertGoneEverywhere(List<Peer> peers, EntryId entryId) {
        for (Peer peer : peers) {
            assertThat(peer.space().read(Template.of(TaskEntry.class))).as("no entry to read at " + peer.address())
                    .isEmpty();
            assertThat(peer.space().recordOf(entryId)).as("no live record at " + peer.address()).isEmpty();
            assertThat(peer.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO))
                    .as("nothing to take at " + peer.address()).isEmpty();
        }
    }

    @Test
    void aCancelRaisesExpiredOnEveryMemberAndTheEntryIsGoneEverywhere() throws Exception {
        List<Peer> peers = cluster();
        Peer a = peers.get(0);
        EntryHandle handle = a.space().write(new TaskEntry("withdraw-me", 1), MINUTES_30);
        EntryId entryId = handle.entryId();
        tickAll(3);
        for (Peer peer : peers) {
            assertThat(peer.count(SpaceEvent.Kind.WRITTEN, entryId)).as("written at " + peer.address()).isEqualTo(1);
        }

        handle.cancel();
        assertThat(a.count(SpaceEvent.Kind.EXPIRED, entryId)).as("the cancelling peer hears it at once").isEqualTo(1);
        tickAll(3);

        for (Peer peer : peers) {
            assertThat(peer.events()).as("events at " + peer.address()).extracting(SpaceEvent::kind)
                    .containsExactly(SpaceEvent.Kind.WRITTEN, SpaceEvent.Kind.EXPIRED);
            SpaceEvent<TaskEntry> expired = peer.events().get(1);
            assertThat(expired.entryId()).isEqualTo(entryId);
            assertThat(expired.entry()).isEqualTo(new TaskEntry("withdraw-me", 1));
            assertThat(expired.issuer()).isEqualTo(a.space().writer().orElseThrow());
            assertThat(expired.actor()).isEqualTo(a.space().writer().orElseThrow());
            assertThat(expired.details()).as("the event carries the record's view").isNotNull();
        }
        assertGoneEverywhere(peers, entryId);
    }

    @Test
    void aRedeliveredCancelRaisesNoSecondEvent() throws Exception {
        List<Peer> peers = cluster();
        Peer a = peers.get(0);
        Peer b = peers.get(1);
        EntryHandle handle = a.space().write(new TaskEntry("once", 1), MINUTES_30);
        EntryId entryId = handle.entryId();
        tickAll(3);

        holdAll();
        handle.cancel();
        releaseAll(SimNetwork.Order.DUPLICATED); // every frame of the cancel arrives twice
        tickAll(3);
        handle.cancel(); // idempotent: nothing to withdraw, nothing to send
        // the same signed state again under a new rumor id, as a relay that lost its seen-set would send it
        SpaceWire.EntryStateDto cancelled = a.space().signedState(entryId).orElseThrow();
        b.node().group(groupId).orElseThrow().gossip().publish(STREAM, "again:" + entryId,
                CborCodec.defaultCodec().toBytes(new SpaceWire.Delta(cancelled, null, null)));
        tickAll(8); // anti-entropy rounds offer the tombstone again
        peers.forEach(peer -> peer.space().sweepNow());

        for (Peer peer : peers) {
            assertThat(peer.count(SpaceEvent.Kind.EXPIRED, entryId)).as("one EXPIRED at " + peer.address())
                    .isEqualTo(1);
        }
        assertGoneEverywhere(peers, entryId);
    }

    @Test
    void aCancelRacingATakeWithdrawsTheEntryAndTheTakerCannotComplete() throws Exception {
        List<Peer> peers = cluster();
        Peer a = peers.get(0);
        Peer b = peers.get(1);
        EntryHandle handle = a.space().write(new TaskEntry("raced", 1), MINUTES_30);
        EntryId entryId = handle.entryId();
        tickAll(3);

        holdAll();
        TakenEntry<TaskEntry> taken = b.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO)
                .orElseThrow();
        handle.cancel();
        releaseAll(SimNetwork.Order.SHUFFLED);
        tickAll(3);

        assertThatThrownBy(() -> b.space().complete(taken)).isInstanceOf(LeaseExpiredException.class);
        tickAll(3);
        for (Peer peer : peers) {
            assertThat(peer.count(SpaceEvent.Kind.EXPIRED, entryId)).as("one EXPIRED at " + peer.address())
                    .isEqualTo(1);
            assertThat(peer.count(SpaceEvent.Kind.COMPLETED, entryId)).as("no completion at " + peer.address())
                    .isZero();
        }
        assertThat(b.count(SpaceEvent.Kind.TAKEN, entryId)).as("the taker heard its take first").isEqualTo(1);
        assertGoneEverywhere(peers, entryId);
    }

    /** The cancel merges while the taker waits out its settle window: the take hands out no withdrawn entry. */
    @Test
    void aCancelThatMergesDuringTheSettleWindowLeavesTheTakeEmpty() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, Duration.ofMillis(300), "a");
        tickAll(6);
        EntryHandle handle = a.space().write(new TaskEntry("settling", 1), MINUTES_30);
        EntryId entryId = handle.entryId();
        tickAll(3);

        CompletableFuture<Optional<TakenEntry<TaskEntry>>> taking = CompletableFuture.supplyAsync(() ->
                b.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ofMillis(600)));
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (a.space().claimsHeld() == 0 && System.nanoTime() < deadline) {
            Thread.sleep(5); // until b's claim reached a: b now waits out its settle window
        }
        assertThat(a.space().claimsHeld()).as("b claimed the entry").isEqualTo(1);
        handle.cancel();

        assertThat(taking.get(5, TimeUnit.SECONDS)).as("the take found the entry withdrawn").isEmpty();
        assertThat(b.count(SpaceEvent.Kind.TAKEN, entryId)).isZero();
        assertThat(b.count(SpaceEvent.Kind.EXPIRED, entryId)).isEqualTo(1);
    }

    @Test
    void aCancelThatReachesATakerFirstLeavesItNothingToTake() throws Exception {
        List<Peer> peers = cluster();
        Peer a = peers.get(0);
        Peer b = peers.get(1);
        EntryHandle handle = a.space().write(new TaskEntry("gone first", 1), MINUTES_30);
        EntryId entryId = handle.entryId();
        tickAll(3);

        handle.cancel();
        tickAll(3);

        assertThat(b.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO)).isEmpty();
        tickAll(3);
        for (Peer peer : peers) {
            assertThat(peer.events()).as("events at " + peer.address()).extracting(SpaceEvent::kind)
                    .containsExactly(SpaceEvent.Kind.WRITTEN, SpaceEvent.Kind.EXPIRED);
        }
        assertGoneEverywhere(peers, entryId);
    }

    /**
     * The holder completes before the cancel reaches it. Completion dominates (SPEC §7.3, §11a.4), so every member
     * ends with the entry completed and hears COMPLETED once; a member that merged the withdrawal first heard EXPIRED
     * as well, once, as after a lease lapse; the holder, which completed first, hears no EXPIRED.
     */
    @Test
    void aCancelRacingACompletionEndsCompletedEverywhereWithOneExpiredAtMost() throws Exception {
        for (SimNetwork.Order order : List.of(SimNetwork.Order.FIFO, SimNetwork.Order.REVERSED,
                SimNetwork.Order.DUPLICATED)) {
            List<Peer> peers = cluster();
            Peer a = peers.get(0);
            Peer b = peers.get(1);
            EntryHandle handle = a.space().write(new TaskEntry("raced " + order, 1), MINUTES_30);
            EntryId entryId = handle.entryId();
            tickAll(3);

            holdAll();
            TakenEntry<TaskEntry> taken = b.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO)
                    .orElseThrow();
            b.space().complete(taken);
            handle.cancel();
            releaseAll(order);
            tickAll(6);

            assertThat(a.count(SpaceEvent.Kind.EXPIRED, entryId)).as("the canceller, " + order).isEqualTo(1);
            assertThat(b.count(SpaceEvent.Kind.EXPIRED, entryId)).as("the holder, " + order).isZero();
            for (Peer peer : peers) {
                assertThat(peer.count(SpaceEvent.Kind.COMPLETED, entryId))
                        .as("one COMPLETED at " + peer.address() + ", " + order).isEqualTo(1);
                assertThat(peer.count(SpaceEvent.Kind.EXPIRED, entryId))
                        .as("one EXPIRED at most at " + peer.address() + ", " + order).isLessThanOrEqualTo(1);
                assertThat(peer.space().recordOf(entryId, true)).as("completed at " + peer.address()).isPresent();
                assertThat(new String(peer.space().digest(), java.nio.charset.StandardCharsets.UTF_8))
                        .as("converged at " + peer.address() + ", " + order)
                        .isEqualTo(new String(a.space().digest(), java.nio.charset.StandardCharsets.UTF_8));
            }
            assertGoneEverywhere(peers, entryId);
            nodes.forEach(PeerNode::close);
            nodes.clear();
            addresses.clear();
            network.heal();
        }
    }

    /** The writer cancels an entry it already saw completed: as on a LocalSpace, nothing happens and nothing is sent. */
    @Test
    void aCancelOfAnEntryCompletedHereRaisesNothingAndSendsNothing() throws Exception {
        List<Peer> peers = cluster();
        Peer a = peers.get(0);
        Peer b = peers.get(1);
        EntryHandle handle = a.space().write(new TaskEntry("done", 1), MINUTES_30);
        EntryId entryId = handle.entryId();
        tickAll(3);
        b.space().complete(b.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO).orElseThrow());
        tickAll(3);
        SpaceWire.EntryStateDto completed = a.space().signedState(entryId).orElseThrow();

        handle.cancel();
        tickAll(6);

        assertThat(a.space().signedState(entryId)).as("the holder's completion stays the state a forwards")
                .contains(completed);
        for (Peer peer : peers) {
            assertThat(peer.events()).as("events at " + peer.address()).extracting(SpaceEvent::kind)
                    .containsExactly(SpaceEvent.Kind.WRITTEN, SpaceEvent.Kind.TAKEN, SpaceEvent.Kind.COMPLETED);
        }
    }

    @Test
    void aCancelAfterTheLeaseLapsedRaisesNoSecondExpired() throws Exception {
        List<Peer> peers = cluster();
        Peer a = peers.get(0);
        EntryHandle handle = a.space().write(new TaskEntry("lapses", 1), Lease.of(Duration.ofMinutes(1)));
        EntryId entryId = handle.entryId();
        tickAll(3);

        clock.advance(Duration.ofMinutes(2));
        peers.forEach(peer -> peer.space().sweepNow()); // every member hears the lapse
        handle.cancel();
        tickAll(4);

        for (Peer peer : peers) {
            assertThat(peer.count(SpaceEvent.Kind.EXPIRED, entryId)).as("one EXPIRED at " + peer.address())
                    .isEqualTo(1);
        }
        assertGoneEverywhere(peers, entryId);
    }

    @Test
    void aCancelBeforeTheLeaseLapsesRaisesNoSecondExpiredAtTheLapse() throws Exception {
        List<Peer> peers = cluster();
        Peer a = peers.get(0);
        EntryHandle handle = a.space().write(new TaskEntry("cancelled, then due", 1), Lease.of(Duration.ofMinutes(1)));
        EntryId entryId = handle.entryId();
        tickAll(3);

        handle.cancel();
        tickAll(3);
        clock.advance(Duration.ofMinutes(2));
        tickAll(4);
        peers.forEach(peer -> peer.space().sweepNow());

        for (Peer peer : peers) {
            assertThat(peer.count(SpaceEvent.Kind.EXPIRED, entryId)).as("one EXPIRED at " + peer.address())
                    .isEqualTo(1);
        }
        assertGoneEverywhere(peers, entryId);
    }

    /** The lease lapsed everywhere, but no sweep ran before the cancel merged: the cancel raises the one EXPIRED. */
    @Test
    void aCancelMergedBeforeTheSweepOfALapsedEntryRaisesOneExpired() throws Exception {
        List<Peer> peers = cluster();
        Peer a = peers.get(0);
        EntryHandle handle = a.space().write(new TaskEntry("due, unswept", 1), Lease.of(Duration.ofMinutes(1)));
        EntryId entryId = handle.entryId();
        tickAll(3);

        clock.advance(Duration.ofMinutes(2));
        handle.cancel();
        tickAll(4);

        for (Peer peer : peers) {
            assertThat(peer.count(SpaceEvent.Kind.EXPIRED, entryId)).as("one EXPIRED at " + peer.address())
                    .isEqualTo(1);
        }
    }

    /**
     * Only the entry's issuer may cancel it (SPEC §11a.4). A withdrawal that another member signs, over the issuer's
     * key or its own, merges nowhere: the entry stays, and no member hears EXPIRED (core cancel events, 2026-11-09).
     */
    @Test
    void aCancelThatAnotherMemberSignsWithdrawsNothingAndRaisesNothing() throws Exception {
        List<Peer> peers = cluster();
        Peer a = peers.get(0);
        Peer b = peers.get(1);
        EntryHandle handle = a.space().write(new TaskEntry("not b's to cancel", 1), MINUTES_30);
        EntryId entryId = handle.entryId();
        tickAll(3);
        SpaceWire.EntryStateDto written = a.space().signedState(entryId).orElseThrow();
        SpaceWire.EntryStateDto overIssuersKey = ForgeSupport.signState(b.identity(),
                new SpaceWire.EntryStateDto(written.record(), written.issuerPublicKey(), written.adds(),
                        new ArrayList<>(written.adds()), written.leaseStamp(), written.leaseValue(), false, null));
        SpaceWire.EntryStateDto overOwnKey = ForgeSupport.signState(b.identity(),
                new SpaceWire.EntryStateDto(written.record(), b.identity().rawPublicKey(), written.adds(),
                        new ArrayList<>(written.adds()), written.leaseStamp(), written.leaseValue(), false, null));
        GroupRuntime runtime = b.node().group(groupId).orElseThrow();
        runtime.gossip().publish(STREAM, "forged-cancel-1:" + entryId,
                CborCodec.defaultCodec().toBytes(new SpaceWire.Delta(overIssuersKey, null, null)));
        runtime.gossip().publish(STREAM, "forged-cancel-2:" + entryId,
                CborCodec.defaultCodec().toBytes(new SpaceWire.Delta(overOwnKey, null, null)));
        tickAll(6);
        peers.forEach(peer -> peer.space().sweepNow());

        for (Peer peer : peers) {
            assertThat(peer.count(SpaceEvent.Kind.EXPIRED, entryId)).as("no EXPIRED at " + peer.address()).isZero();
            assertThat(peer.space().recordOf(entryId)).as("the entry stays at " + peer.address()).isPresent();
        }
    }

    @Test
    void aMemberThatLearnsOfTheEntryOnlyWithdrawnHearsNothing() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        tickAll(4);
        EntryHandle handle = a.space().write(new TaskEntry("history", 1), MINUTES_30);
        tickAll(3);
        handle.cancel();
        tickAll(3);

        Peer late = newPeer("c", 3, "a");
        tickAll(10);

        assertThat(late.space().knownEntries()).as("the late member holds the tombstone").isEqualTo(1);
        assertThat(late.events()).as("a withdrawn entry learned late is history").isEmpty();
        assertThat(b.count(SpaceEvent.Kind.EXPIRED, handle.entryId())).isEqualTo(1);
    }
}
