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
import ai.badmonkey.agentspaces.api.error.LeaseExpiredException;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.hlc.HybridLogicalClock;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.gossip.ReconcilableState;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.crdt.Dot;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The signed-state defense (SPEC §11a): an admitted group member cannot forge a
 * completion, a removal, or a lease that makes a victim's signed entry vanish.
 * A malicious peer here joins the group, captures a real signed state off the
 * gossip wire, flips its mutable fields, and republishes it. Honest replicas
 * must reject the forgery and keep the entry.
 */
class ForgedStateRejectionTest {

    private static final Lease MINUTES_30 = Lease.of(Duration.ofMinutes(30));
    private static final Lease MINUTES_10 = Lease.of(Duration.ofMinutes(10));
    private static final String STREAM = "space:tasks";

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zForge");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zForge", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "fleet",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(PeerNode node, GroupRuntime runtime, ReplicatedSpace space) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Peer newPeer(String address, long seed, String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "tasks", identity, address)
                .clock(clock).settleWindow(Duration.ZERO).build();
        nodes.add(node);
        return new Peer(node, runtime, space);
    }

    /** A malicious peer: joins the group and taps the space stream, no honest space. */
    private GroupRuntime attacker(String address, long seed, String seedAddress,
                                  AtomicReference<SpaceWire.EntryStateDto> captured)
            throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", seedAddress, 0)));
        runtime.gossip().onStream(STREAM, (from, itemId, payload) -> {
            SpaceWire.Delta delta = codec.fromBytes(payload, SpaceWire.Delta.class);
            if (delta != null && delta.state() != null && !delta.state().completed()) {
                captured.set(delta.state());
            }
        });
        nodes.add(node);
        return runtime;
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // anti-entropy paces itself by the gossip period
        }
    }

    @Test
    void aForgedCompletionCannotDeleteAnEntry() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        AtomicReference<SpaceWire.EntryStateDto> captured = new AtomicReference<>();
        GroupRuntime evil = attacker("evil", 3, "a", captured);
        tickAll(4);

        a.space().write(new TaskEntry("precious", 1), MINUTES_30);
        tickAll(4);
        assertThat(b.space().read(Template.of(TaskEntry.class))).isPresent();
        SpaceWire.EntryStateDto real = captured.get();
        assertThat(real).as("attacker captured the real signed write").isNotNull();

        // Forge a completion: flip completed=true with a bogus state signature.
        SpaceWire.EntryStateDto forged = new SpaceWire.EntryStateDto(
                real.record(), real.issuerPublicKey(), real.adds(), real.removes(),
                real.leaseStamp(), real.leaseValue(), true, new byte[64]);
        evil.gossip().publish(STREAM, "forge-complete:" + real.record().entryId(),
                codec.toBytes(new SpaceWire.Delta(forged, null, null)));
        tickAll(6);

        // The forgery is rejected fleet-wide: the entry is still there and takeable.
        assertThat(a.space().read(Template.of(TaskEntry.class))).isPresent();
        assertThat(b.space().read(Template.of(TaskEntry.class))).isPresent();
        Optional<TakenEntry<TaskEntry>> taken =
                b.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(taken).as("entry survived the forged completion and is takeable").isPresent();

        // And an honest completion by the real holder still replicates.
        b.space().complete(taken.get());
        tickAll(6);
        assertThat(a.space().read(Template.of(TaskEntry.class))).isEmpty();
    }

    @Test
    void aClaimWithInflatedEpochOrEternalExpiryCannotFreezeAnEntry() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        AtomicReference<SpaceWire.EntryStateDto> captured = new AtomicReference<>();
        PeerIdentity evilId = PeerIdentity.generate();
        PeerNode evilNode = PeerNode.builder(evilId).clock(clock).randomSeed(3).build();
        evilNode.listen(network.register("evil"), "evil");
        GroupRuntime evil = evilNode.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        evil.gossip().onStream(STREAM, (from, itemId, payload) -> {
            SpaceWire.Delta delta = codec.fromBytes(payload, SpaceWire.Delta.class);
            if (delta != null && delta.state() != null) {
                captured.set(delta.state());
            }
        });
        nodes.add(evilNode);
        tickAll(4);

        a.space().write(new TaskEntry("free-me", 1), MINUTES_30);
        tickAll(4);
        SpaceWire.EntryStateDto real = captured.get();
        assertThat(real).isNotNull();
        var entryId = real.record().entryId();

        // ASF-004: two self-signed hostile claims — an epoch far beyond any
        // honest history, and an eternal hold. Both verify (evil signs its own
        // claim) and both must be rejected at merge, or the entry is frozen
        // forever: the max-epoch claim wins every merge and the +1 re-take
        // epoch becomes unrepresentable.
        var evilHlc = new ai.badmonkey.agentspaces.common.hlc.HybridLogicalClock(clock, "evil");
        TakeClaim inflated = new TakeClaim(entryId, a.space().id(), 1L << 30,
                evilHlc.now(), evilId.agent("evil"), 0.0,
                clock.instant().toEpochMilli() + 60_000);
        TakeClaim eternal = new TakeClaim(entryId, a.space().id(), 2,
                evilHlc.now(), evilId.agent("evil"), 0.0, Long.MAX_VALUE);
        for (TakeClaim hostile : List.of(inflated, eternal)) {
            SpaceWire.SignedClaim signed = new SpaceWire.SignedClaim(hostile,
                    evilId.rawPublicKey(), evilId.sign(codec.toBytes(hostile)));
            evil.gossip().publish(STREAM, "freeze:" + hostile.epoch(),
                    codec.toBytes(new SpaceWire.Delta(null, entryId, signed)));
        }
        tickAll(6);

        Optional<TakenEntry<TaskEntry>> taken =
                b.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(taken)
                .as("hostile claims were rejected at merge; the entry is takeable")
                .isPresent();
    }

    @Test
    void aForgedRemovalCannotDeleteAnEntry() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        AtomicReference<SpaceWire.EntryStateDto> captured = new AtomicReference<>();
        GroupRuntime evil = attacker("evil", 3, "a", captured);
        tickAll(4);

        a.space().write(new TaskEntry("keep-me", 1), MINUTES_30);
        tickAll(4);
        SpaceWire.EntryStateDto real = captured.get();
        assertThat(real).isNotNull();

        // Forge a cancellation: copy every add-dot into removes (present() -> false)
        // with a bogus issuer signature.
        SpaceWire.EntryStateDto forged = new SpaceWire.EntryStateDto(
                real.record(), real.issuerPublicKey(), real.adds(),
                new ArrayList<>(real.adds()), real.leaseStamp(), real.leaseValue(),
                false, new byte[64]);
        evil.gossip().publish(STREAM, "forge-remove:" + real.record().entryId(),
                codec.toBytes(new SpaceWire.Delta(forged, null, null)));
        tickAll(6);

        assertThat(a.space().read(Template.of(TaskEntry.class))).isPresent();
        assertThat(b.space().read(Template.of(TaskEntry.class))).isPresent();
    }

    /** An admitted attacker whose identity the test also holds, so it can sign under its own key. */
    private record Attacker(PeerIdentity id, GroupRuntime runtime) {
    }

    private Attacker attackerWithKey(String address, long seed, String seedAddress,
                                     AtomicReference<SpaceWire.EntryStateDto> captured)
            throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", seedAddress, 0)));
        runtime.gossip().onStream(STREAM, (from, itemId, payload) -> {
            SpaceWire.Delta delta = codec.fromBytes(payload, SpaceWire.Delta.class);
            if (delta != null && delta.state() != null && !delta.state().completed()
                    && captured.get() == null) {
                captured.set(delta.state());
            }
        });
        nodes.add(node);
        return new Attacker(identity, runtime);
    }

    /** Spec §7.4/§11: a validly signed claim with a far-past stamp cannot jump the LEASE_RACE queue, while a claim a few seconds old is honoured. */
    @Test
    void aClaimWithAFarPastStampCannotJumpTheQueue() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        AtomicReference<SpaceWire.EntryStateDto> captured = new AtomicReference<>();
        Attacker evil = attackerWithKey("evil", 3, "a", captured);
        tickAll(4);

        a.space().write(new TaskEntry("contested", 1), MINUTES_30);
        tickAll(4);
        SpaceWire.EntryStateDto real = captured.get();
        assertThat(real).isNotNull();
        EntryId entryId = real.record().entryId();

        // Honest B claims first; its claim replicates.
        Optional<TakenEntry<TaskEntry>> held =
                b.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(held).isPresent();
        AgentId honest = b.node().identity().agent("b");
        assertThat(a.space().currentClaim(entryId)).map(TakeClaim::holder).contains(honest);

        // The attacker's claim is validly self-signed, same epoch, same bid, but
        // stamped at the dawn of time: under LEASE_RACE the lowest stamp wins, so
        // without a stamp bound it would take the entry away from B everywhere.
        TakeClaim farPast = new TakeClaim(entryId, a.space().id(), 1,
                new HlcTimestamp(1L, 0, "evil"), evil.id().agent("evil"), 0.0,
                clock.instant().toEpochMilli() + Duration.ofMinutes(10).toMillis());
        evil.runtime().gossip().publish(STREAM, "jump:" + entryId,
                codec.toBytes(new SpaceWire.Delta(null, entryId, ForgeSupport.signClaim(evil.id(), farPast))));
        tickAll(6);

        assertThat(a.space().currentClaim(entryId)).map(TakeClaim::holder)
                .as("the far-past claim was rejected; B still holds").contains(honest);
        b.space().complete(held.get());
        tickAll(4);
        assertThat(a.space().read(Template.of(TaskEntry.class))).isEmpty();

        // Control: a claim stamped a few seconds in the past, well inside the
        // drift bound, is an ordinary race entrant and is honoured.
        a.space().write(new TaskEntry("raced", 2), MINUTES_30);
        tickAll(2);
        EntryId raced = a.space().entryIdOf(new TaskEntry("raced", 2)).orElseThrow();
        TakeClaim recent = new TakeClaim(raced, a.space().id(), 1,
                new HlcTimestamp(clock.instant().toEpochMilli() - 5_000, 0, "evil"),
                evil.id().agent("evil"), 0.0,
                clock.instant().toEpochMilli() + Duration.ofMinutes(10).toMillis());
        evil.runtime().gossip().publish(STREAM, "recent:" + raced,
                codec.toBytes(new SpaceWire.Delta(null, raced, ForgeSupport.signClaim(evil.id(), recent))));
        tickAll(4);

        assertThat(a.space().currentClaim(raced)).map(TakeClaim::holder)
                .as("a legitimately recent claim still merges").contains(evil.id().agent("evil"));
        assertThat(b.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO))
                .as("the entry is held by the recent claim").isEmpty();
    }

    /** Spec §11a.4: a forged lease (far-past expiry) cannot expire a signed entry, whether bogus- or attacker-signed. */
    @Test
    void aForgedLeaseCannotExpireAnEntry() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        AtomicReference<SpaceWire.EntryStateDto> captured = new AtomicReference<>();
        Attacker evil = attackerWithKey("evil", 3, "a", captured);
        tickAll(4);

        a.space().write(new TaskEntry("long-lived", 1), MINUTES_30);
        tickAll(4);
        SpaceWire.EntryStateDto real = captured.get();
        assertThat(real).isNotNull();

        // A newer lease register stamp carrying an already-expired WRITE lease.
        HlcTimestamp newerStamp = new HlcTimestamp(real.leaseStamp().physical() + 1, 0, "evil");
        LeaseInfo expiredLease = new LeaseInfo(real.leaseValue().holder(), 1L, LeaseKind.WRITE);
        SpaceWire.EntryStateDto bogus = new SpaceWire.EntryStateDto(real.record(),
                real.issuerPublicKey(), real.adds(), real.removes(), newerStamp, expiredLease,
                false, new byte[64]);
        SpaceWire.EntryStateDto attackerSigned = ForgeSupport.signState(evil.id(), bogus);
        evil.runtime().gossip().publish(STREAM, "forge-lease-bogus:" + real.record().entryId(),
                codec.toBytes(new SpaceWire.Delta(bogus, null, null)));
        evil.runtime().gossip().publish(STREAM, "forge-lease-signed:" + real.record().entryId(),
                codec.toBytes(new SpaceWire.Delta(attackerSigned, null, null)));
        tickAll(6);

        // Had either merged, the LWW lease would read as expired and hide the entry.
        assertThat(a.space().read(Template.of(TaskEntry.class))).isPresent();
        assertThat(b.space().read(Template.of(TaskEntry.class))).isPresent();
    }

    /** Spec §11a.4: a completion validly signed by a member who holds no take claim is dropped. */
    @Test
    void aCompletionValidlySignedByANonHolderIsDropped() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        AtomicReference<SpaceWire.EntryStateDto> captured = new AtomicReference<>();
        Attacker evil = attackerWithKey("evil", 3, "a", captured);
        tickAll(4);

        a.space().write(new TaskEntry("attributable", 1), MINUTES_30);
        tickAll(4);
        SpaceWire.EntryStateDto real = captured.get();
        assertThat(real).isNotNull();
        EntryId entryId = real.record().entryId();

        // B holds a real, replicated claim; the attacker signs a completion under
        // its own key. The receiver must attribute completion to the claim holder.
        Optional<TakenEntry<TaskEntry>> held = b.space().take(Template.of(TaskEntry.class),
                Lease.of(Duration.ofMinutes(1)), Duration.ZERO);
        assertThat(held).isPresent();
        SpaceWire.EntryStateDto forged = ForgeSupport.signState(evil.id(),
                new SpaceWire.EntryStateDto(real.record(), real.issuerPublicKey(), real.adds(),
                        real.removes(), real.leaseStamp(), real.leaseValue(), true, null));
        evil.runtime().gossip().publish(STREAM, "forge-complete-signed:" + entryId,
                codec.toBytes(new SpaceWire.Delta(forged, entryId, null)));
        tickAll(6);

        // B's claim lapses; the entry is still uncompleted, so A can take and finish it.
        clock.advance(Duration.ofMinutes(2));
        Optional<TakenEntry<TaskEntry>> retry =
                a.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(retry).as("the forged completion never merged").isPresent();
        assertThatThrownBy(() -> b.space().complete(held.get()))
                .isInstanceOf(LeaseExpiredException.class);
        a.space().complete(retry.get());
        tickAll(4);
        assertThat(b.space().read(Template.of(TaskEntry.class))).isEmpty();
    }

    /** Spec §11a.4: a completion with no take claim at all has no authorized signer and is dropped. */
    @Test
    void aCompletionWithoutAnyClaimIsDropped() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        AtomicReference<SpaceWire.EntryStateDto> captured = new AtomicReference<>();
        Attacker evil = attackerWithKey("evil", 3, "a", captured);
        tickAll(4);

        a.space().write(new TaskEntry("unclaimed", 1), MINUTES_30);
        tickAll(4);
        SpaceWire.EntryStateDto real = captured.get();
        assertThat(real).isNotNull();

        SpaceWire.EntryStateDto forged = ForgeSupport.signState(evil.id(),
                new SpaceWire.EntryStateDto(real.record(), real.issuerPublicKey(), real.adds(),
                        real.removes(), real.leaseStamp(), real.leaseValue(), true, null));
        evil.runtime().gossip().publish(STREAM, "forge-complete-noclaim:" + real.record().entryId(),
                codec.toBytes(new SpaceWire.Delta(forged, null, null)));
        tickAll(6);

        assertThat(a.space().read(Template.of(TaskEntry.class))).isPresent();
        assertThat(b.space().read(Template.of(TaskEntry.class))).isPresent();
        assertThat(b.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO))
                .isPresent();
    }

    /** Spec §11a.4/§7.3: a forged state offered through the anti-entropy pull channel is verified and dropped too. */
    @Test
    void aForgedStateOfferedThroughAntiEntropyIsDropped() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        AtomicReference<SpaceWire.EntryStateDto> captured = new AtomicReference<>();
        Attacker evil = attackerWithKey("evil", 3, "a", captured);
        tickAll(4);

        a.space().write(new TaskEntry("pulled", 1), MINUTES_30);
        tickAll(4);
        SpaceWire.EntryStateDto real = captured.get();
        assertThat(real).isNotNull();

        // The attacker answers every digest it receives with a forged completion.
        SpaceWire.EntryStateDto forged = ForgeSupport.signState(evil.id(),
                new SpaceWire.EntryStateDto(real.record(), real.issuerPublicKey(), real.adds(),
                        real.removes(), real.leaseStamp(), real.leaseValue(), true, null));
        byte[] poisonedDelta = codec.toBytes(
                new SpaceWire.SyncDelta(List.of(forged), Map.of()));
        AtomicInteger served = new AtomicInteger();
        evil.runtime().gossip().reconcile(STREAM, new ReconcilableState() {
            @Override
            public byte[] digest() {
                return "e:poison=ffffffff\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }

            @Override
            public byte[] deltaFor(byte[] remoteDigest) {
                served.incrementAndGet();
                return poisonedDelta;
            }

            @Override
            public void applyDelta(byte[] delta) {
            }
        });
        tickAll(12);

        assertThat(served.get()).as("honest replicas did pull from the attacker").isPositive();
        assertThat(a.space().read(Template.of(TaskEntry.class))).isPresent();
        assertThat(b.space().read(Template.of(TaskEntry.class))).isPresent();
    }

    /** A stamp just under the drift ceiling with the logical counter at its maximum: the reported attack. */
    private HlcTimestamp freezingStamp(String node) {
        return new HlcTimestamp(clock.millis() + HybridLogicalClock.MAX_DRIFT_MILLIS - 1_000,
                Integer.MAX_VALUE, node);
    }

    /** Asserts a replica's space clock tracks the wall clock: a take stamps its claim from it. */
    private void assertClockNotFrozen(Peer peer, TaskEntry entry) {
        peer.space().write(entry, MINUTES_30);
        Optional<TakenEntry<TaskEntry>> taken =
                peer.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(taken).isPresent();
        assertThat(taken.get().entry()).isEqualTo(entry);
        assertThat(peer.space().currentClaim(taken.get().entryId())).get()
                .extracting(claim -> claim.stamp().physical())
                .as("the space clock was not pulled onto the hostile stamp")
                .matches(physical -> physical <= clock.millis() + 1_000);
    }

    /** Witnessed-violation strikes {@code node} holds against {@code peer} (PeerNode's test accessor). */
    private static int strikes(PeerNode node, ai.badmonkey.agentspaces.common.id.PeerId peer) throws Exception {
        java.lang.reflect.Method strikes = PeerNode.class.getDeclaredMethod("strikes",
                ai.badmonkey.agentspaces.common.id.PeerId.class);
        strikes.setAccessible(true);
        return (int) strikes.invoke(node, peer);
    }

    /** Publishes a state as the attacker, under its own item id. */
    private void publishState(Attacker evil, String itemId, SpaceWire.EntryStateDto state) {
        evil.runtime().gossip().publish(STREAM, itemId, codec.toBytes(new SpaceWire.Delta(state, null, null)));
    }

    /**
     * HLC logical counter, 2026-11-08 (Rust team report): a folded state refused for its stamps
     * is witnessed misbehavior once its signatures verify, so the receiver counts it and strikes
     * its signer, once per distinct state. A state whose signature does not cover the hostile
     * stamp proves nothing about its signer and is neither counted nor struck, and a re-offer
     * of the same state (anti-entropy, a forward) adds nothing.
     */
    @Test
    void aReceiverCountsAndStrikesAStateRefusedForItsStampsOncePerDistinctStateAfterItVerifies() throws Exception {
        Peer a = newPeer("a", 1);
        Attacker evil = attackerWithKey("evil", 3, "a", new AtomicReference<>());
        tickAll(4);
        ai.badmonkey.agentspaces.common.id.PeerId evilPeer = evil.id().peerId();
        assertThat(strikes(a.node(), evilPeer)).isZero();
        long expires = clock.millis() + Duration.ofMinutes(30).toMillis();
        SpaceWire.EntryStateDto honest = ForgeSupport.authoredWrite(evil.id(), "evil",
                a.space().id(), new TaskEntry("hostile-lease", 1),
                new HlcTimestamp(clock.millis(), 0, "evil"), expires);

        publishState(evil, "unverified", new SpaceWire.EntryStateDto(honest.record(), honest.issuerPublicKey(),
                honest.adds(), honest.removes(), freezingStamp("evil"), honest.leaseValue(), false,
                honest.stateSig()));
        tickAll(2);
        assertThat(a.space().stampsLogicalRefused()).as("an unverified state is not counted").isZero();
        assertThat(strikes(a.node(), evilPeer)).as("nor struck").isZero();

        SpaceWire.EntryStateDto hostileRecord = ForgeSupport.authoredWrite(evil.id(), "evil",
                a.space().id(), new TaskEntry("hostile-record", 2), freezingStamp("evil"), expires);
        publishState(evil, "record-1", hostileRecord);
        tickAll(2);
        assertThat(a.space().stampsLogicalRefused()).isEqualTo(1);
        assertThat(strikes(a.node(), evilPeer)).isEqualTo(1);

        publishState(evil, "record-2", hostileRecord);
        tickAll(2);
        assertThat(a.space().stampsLogicalRefused()).as("a re-offer is the same state").isEqualTo(1);
        assertThat(strikes(a.node(), evilPeer)).isEqualTo(1);

        publishState(evil, "lease", ForgeSupport.signState(evil.id(),
                new SpaceWire.EntryStateDto(honest.record(), honest.issuerPublicKey(), honest.adds(),
                        honest.removes(), freezingStamp("evil"), honest.leaseValue(), false, null)));
        tickAll(2);
        assertThat(a.space().stampsLogicalRefused()).as("a second distinct state").isEqualTo(2);
        assertThat(strikes(a.node(), evilPeer)).isEqualTo(2);
        assertThat(a.space().read(Template.of(TaskEntry.class))).as("no refused state merged").isEmpty();
        assertClockNotFrozen(a, new TaskEntry("a-own", 3));
    }

    /**
     * HLC canonical stamps, 2026-11-08: a state refused for its stamps is acknowledged in the
     * replica's anti-entropy digest, as ASF-047 acknowledges a revoked actor's state, so its
     * partners stop re-offering it. A state whose signature does not verify earns no line, so a
     * forger cannot make the replica acknowledge a state it never judged.
     */
    @Test
    void aStateRefusedForItsStampsIsAcknowledgedInTheDigestOnceItVerifies() throws Exception {
        Peer a = newPeer("a", 1);
        Attacker evil = attackerWithKey("evil", 3, "a", new AtomicReference<>());
        tickAll(4);
        long expires = clock.millis() + Duration.ofMinutes(30).toMillis();
        SpaceWire.EntryStateDto honest = ForgeSupport.authoredWrite(evil.id(), "evil",
                a.space().id(), new TaskEntry("unverified", 1),
                new HlcTimestamp(clock.millis(), 0, "evil"), expires);
        publishState(evil, "unverified", new SpaceWire.EntryStateDto(honest.record(), honest.issuerPublicKey(),
                honest.adds(), honest.removes(), freezingStamp("evil"), honest.leaseValue(), false,
                honest.stateSig()));
        SpaceWire.EntryStateDto hostile = ForgeSupport.authoredWrite(evil.id(), "evil",
                a.space().id(), new TaskEntry("refused", 2), freezingStamp("evil"), expires);
        publishState(evil, "refused", hostile);
        tickAll(2);

        String digest = new String(a.space().digest(), java.nio.charset.StandardCharsets.UTF_8);
        String line = "e:" + hostile.record().entryId().value() + "=";
        assertThat(digest).as("the refused state is acknowledged").contains("a:" + line);
        assertThat(digest).as("and not held").doesNotContain("\n" + line).doesNotStartWith(line);
        assertThat(digest).as("an unverified state is not acknowledged")
                .doesNotContain(honest.record().entryId().value());
        assertThat(a.space().read(Template.of(TaskEntry.class))).isEmpty();
    }

    /**
     * HLC canonical stamps, 2026-11-08: an agent-signed state's {@code signedAt} is a stamp the
     * agent signed, and the Python and Ruby replicas refused one whose counter is above
     * LOGICAL_LIMIT while Java merged it. Java now refuses it too, after the agent's signature
     * verifies: counted, struck, and acknowledged. The same state with an honest
     * {@code signedAt} merges, so the forge is sound.
     */
    @Test
    void anAgentSignedStateWhoseSignedAtCounterIsAboveTheLimitIsRefused() throws Exception {
        Peer a = newPeer("a", 1);
        Attacker evil = attackerWithKey("evil", 3, "a", new AtomicReference<>());
        tickAll(4);
        ai.badmonkey.agentspaces.api.spi.AgentIdentity agent =
                evil.id().subordinate("evil", clock.instant(), Duration.ofHours(1));
        long expires = clock.millis() + Duration.ofMinutes(30).toMillis();

        publishState(evil, "honest", agentSigned(agent, ForgeSupport.authoredWrite(evil.id(), "evil",
                a.space().id(), new TaskEntry("honest signedAt", 1),
                new HlcTimestamp(clock.millis(), 0, "evil"), expires),
                new HlcTimestamp(clock.millis(), HybridLogicalClock.LOGICAL_LIMIT, "evil")));
        tickAll(2);
        assertThat(a.space().read(Template.of(TaskEntry.class)))
                .as("a counter at the limit is honest").contains(new TaskEntry("honest signedAt", 1));

        SpaceWire.EntryStateDto hostile = agentSigned(agent, ForgeSupport.authoredWrite(evil.id(), "evil",
                a.space().id(), new TaskEntry("hostile signedAt", 2),
                new HlcTimestamp(clock.millis(), 0, "evil"), expires),
                new HlcTimestamp(clock.millis(), HybridLogicalClock.LOGICAL_LIMIT + 1, "evil"));
        publishState(evil, "hostile", hostile);
        tickAll(2);

        assertThat(a.space().entryIdOf(new TaskEntry("hostile signedAt", 2))).as("refused").isEmpty();
        assertThat(a.space().stampsLogicalRefused()).isEqualTo(1);
        assertThat(strikes(a.node(), evil.id().peerId())).isEqualTo(1);
        assertThat(new String(a.space().digest(), java.nio.charset.StandardCharsets.UTF_8))
                .contains("a:e:" + hostile.record().entryId().value() + "=");
    }

    /** Re-signs a peer-signed state as the given agent with the given signing time. */
    private static SpaceWire.EntryStateDto agentSigned(ai.badmonkey.agentspaces.api.spi.AgentIdentity agent,
                                                       SpaceWire.EntryStateDto state, HlcTimestamp signedAt) {
        SpaceWire.EntryStateDto unsigned = state.withAgentSigner(agent.id().encoded(), signedAt,
                agent.certificate().orElseThrow());
        return unsigned.withStateSig(agent.sign(ForgeSupport.stateSignBytes(unsigned)));
    }

    /**
     * HLC canonical stamps, 2026-11-08: a space delta that does not decode, by rumor or by
     * anti-entropy, was dropped with no trace. The replica now counts each one; a stamp that is
     * not canonical text ("007:0:evil") is one such delta.
     */
    @Test
    void aReplicaCountsTheDeltasItCannotDecode() throws Exception {
        Peer a = newPeer("a", 1);
        Attacker evil = attackerWithKey("evil", 3, "a", new AtomicReference<>());
        tickAll(4);
        assertThat(a.space().deltasUndecodable()).isZero();

        evil.runtime().gossip().publish(STREAM, "garbage", new byte[]{9, 9, 9});
        tickAll(2);
        assertThat(a.space().deltasUndecodable()).as("a rumor that is no delta").isEqualTo(1);

        SpaceWire.EntryStateDto write = ForgeSupport.authoredWrite(evil.id(), "evil",
                a.space().id(), new TaskEntry("leading zero", 1),
                new HlcTimestamp(clock.millis(), 7, "evil"), clock.millis() + Duration.ofMinutes(30).toMillis());
        String canonical = write.record().issued().encoded();
        byte[] honestBytes = codec.toBytes(new SpaceWire.Delta(write, null, null));
        byte[] padded = replaceAscii(honestBytes, canonical, canonical.replace(":7:", ":07:"));
        evil.runtime().gossip().publish(STREAM, "leading-zero", padded);
        tickAll(2);
        assertThat(a.space().deltasUndecodable()).as("a delta whose stamp is not canonical").isEqualTo(2);
        assertThat(a.space().read(Template.of(TaskEntry.class))).isEmpty();

        AtomicInteger served = new AtomicInteger();
        evil.runtime().gossip().reconcile(STREAM, new ReconcilableState() {
            @Override
            public byte[] digest() {
                return "e:poison=ffffffff\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }

            @Override
            public byte[] deltaFor(byte[] remoteDigest) {
                served.incrementAndGet();
                return new byte[]{9, 9, 9};
            }

            @Override
            public void applyDelta(byte[] delta) {
            }
        });
        tickAll(12);
        assertThat(served.get()).as("the replica pulled from the attacker").isPositive();
        assertThat(a.space().deltasUndecodable()).as("an anti-entropy delta that does not decode")
                .isGreaterThan(2);
    }

    /** Rewrites every CBOR text string {@code from} to {@code to}; both are shorter than 256 bytes. */
    private static byte[] replaceAscii(byte[] cbor, String from, String to) {
        byte[] f = textString(from);
        byte[] t = textString(to);
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        int i = 0;
        int replaced = 0;
        while (i < cbor.length) {
            if (i + f.length <= cbor.length && java.util.Arrays.equals(cbor, i, i + f.length, f, 0, f.length)) {
                out.writeBytes(t);
                i += f.length;
                replaced++;
            } else {
                out.write(cbor[i]);
                i++;
            }
        }
        assertThat(replaced).as("the stamp text was found").isPositive();
        return out.toByteArray();
    }

    /** A CBOR text string, header and bytes, for text shorter than 256 bytes. */
    private static byte[] textString(String text) {
        byte[] bytes = text.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        assertThat(bytes.length).isLessThan(256);
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        if (bytes.length < 24) {
            out.write(0x60 + bytes.length);
        } else {
            out.write(0x78);
            out.write(bytes.length);
        }
        out.writeBytes(bytes);
        return out.toByteArray();
    }

    /**
     * HLC logical counter, 2026-11-08 (Rust team report): a folded record or lease stamp with
     * a logical counter above LOGICAL_LIMIT froze every replica's space clock, the stamp
     * travelling on through gossip and anti-entropy. Both are now ignored before the merge,
     * and a record stamped at exactly LOGICAL_LIMIT still merges.
     */
    @Test
    void aRecordOrLeaseStampWithALogicalCounterAboveTheLimitIsIgnored() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        Attacker evil = attackerWithKey("evil", 3, "a", new AtomicReference<>());
        tickAll(4);
        long expires = clock.millis() + Duration.ofMinutes(30).toMillis();

        SpaceWire.EntryStateDto hostileRecord = ForgeSupport.authoredWrite(evil.id(), "evil",
                a.space().id(), new TaskEntry("hostile-record", 1), freezingStamp("evil"), expires);
        evil.runtime().gossip().publish(STREAM, "record:" + hostileRecord.record().entryId(),
                codec.toBytes(new SpaceWire.Delta(hostileRecord, null, null)));
        SpaceWire.EntryStateDto honest = ForgeSupport.authoredWrite(evil.id(), "evil",
                a.space().id(), new TaskEntry("hostile-lease", 2),
                new HlcTimestamp(clock.millis(), 0, "evil"), expires);
        SpaceWire.EntryStateDto hostileLease = ForgeSupport.signState(evil.id(),
                new SpaceWire.EntryStateDto(honest.record(), honest.issuerPublicKey(), honest.adds(),
                        honest.removes(), freezingStamp("evil"), honest.leaseValue(), false, null));
        evil.runtime().gossip().publish(STREAM, "lease:" + honest.record().entryId(),
                codec.toBytes(new SpaceWire.Delta(hostileLease, null, null)));
        tickAll(4);

        assertThat(a.space().read(Template.of(TaskEntry.class))).as("neither state merged").isEmpty();
        assertThat(b.space().read(Template.of(TaskEntry.class))).isEmpty();
        assertClockNotFrozen(a, new TaskEntry("a-own", 3));
        assertClockNotFrozen(b, new TaskEntry("b-own", 4));
        tickAll(4);

        SpaceWire.EntryStateDto atLimit = ForgeSupport.authoredWrite(evil.id(), "evil",
                a.space().id(), new TaskEntry("at-limit", 5),
                new HlcTimestamp(clock.millis(), HybridLogicalClock.LOGICAL_LIMIT, "evil"), expires);
        evil.runtime().gossip().publish(STREAM, "limit:" + atLimit.record().entryId(),
                codec.toBytes(new SpaceWire.Delta(atLimit, null, null)));
        tickAll(4);
        assertThat(a.space().entryIdOf(new TaskEntry("at-limit", 5)))
                .as("a record stamped at the limit is honest and merges").isPresent();
        assertThat(b.space().entryIdOf(new TaskEntry("at-limit", 5))).isPresent();
    }

    /**
     * HLC logical counter, 2026-11-08 (Rust team report): a validly signed claim with a logical
     * counter above LOGICAL_LIMIT froze the space clock of every replica that folded it. Each
     * replica now refuses it as a claim violation, counts it, and strikes its holder; it never
     * enters the lattice, and a claim at exactly
     * LOGICAL_LIMIT still merges.
     */
    @Test
    void aClaimWithALogicalCounterAboveTheLimitIsIgnored() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        Attacker evil = attackerWithKey("evil", 3, "a", new AtomicReference<>());
        tickAll(4);
        a.space().write(new TaskEntry("contested", 1), MINUTES_30);
        tickAll(4);
        EntryId entryId = a.space().entryIdOf(new TaskEntry("contested", 1)).orElseThrow();
        long holdUntil = clock.millis() + Duration.ofMinutes(10).toMillis();

        TakeClaim hostile = new TakeClaim(entryId, a.space().id(), 1, freezingStamp("evil"),
                evil.id().agent("evil"), 0.0, holdUntil);
        evil.runtime().gossip().publish(STREAM, "freeze:" + entryId,
                codec.toBytes(new SpaceWire.Delta(null, entryId, ForgeSupport.signClaim(evil.id(), hostile))));
        tickAll(4);

        assertThat(a.space().currentClaim(entryId)).as("the hostile claim never merged").isEmpty();
        assertThat(b.space().currentClaim(entryId)).isEmpty();
        assertThat(a.space().stampsLogicalRefused()).as("the refusal is counted").isEqualTo(1);
        assertThat(strikes(a.node(), evil.id().peerId())).as("and its holder struck").isEqualTo(1);
        Optional<TakenEntry<TaskEntry>> taken =
                b.space().take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(taken).as("the entry is still takeable").isPresent();
        assertThat(b.space().currentClaim(entryId)).get()
                .extracting(claim -> claim.stamp().physical())
                .as("B's space clock was not pulled onto the hostile stamp")
                .matches(physical -> physical <= clock.millis() + 1_000);

        a.space().write(new TaskEntry("raced", 2), MINUTES_30);
        tickAll(2);
        EntryId raced = a.space().entryIdOf(new TaskEntry("raced", 2)).orElseThrow();
        TakeClaim atLimit = new TakeClaim(raced, a.space().id(), 1,
                new HlcTimestamp(clock.millis(), HybridLogicalClock.LOGICAL_LIMIT, "evil"),
                evil.id().agent("evil"), 0.0, holdUntil);
        evil.runtime().gossip().publish(STREAM, "limit:" + raced,
                codec.toBytes(new SpaceWire.Delta(null, raced, ForgeSupport.signClaim(evil.id(), atLimit))));
        tickAll(4);
        assertThat(a.space().currentClaim(raced)).map(TakeClaim::holder)
                .as("a claim stamped at the limit is honest and merges").contains(evil.id().agent("evil"));
    }

    /** ASF-025 (SPEC §11): oversize or foreign-replica dot sets are dropped even when the entry's own issuer signs them. */
    @Test
    void oversizeOrForeignDotSetsAreDroppedEvenWhenTheIssuerSignsThem() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        AtomicReference<SpaceWire.EntryStateDto> unused = new AtomicReference<>();
        Attacker evil = attackerWithKey("evil", 3, "a", unused);
        tickAll(4);

        // The attacker authors a perfectly valid entry of its own; honest replicas store it.
        HybridLogicalClock evilHlc = new HybridLogicalClock(clock, "evil");
        SpaceWire.EntryStateDto write = ForgeSupport.authoredWrite(evil.id(), "evil",
                a.space().id(), new TaskEntry("evil-owned", 1), evilHlc.now(),
                clock.instant().toEpochMilli() + Duration.ofMinutes(30).toMillis());
        EntryId entryId = write.record().entryId();
        evil.runtime().gossip().publish(STREAM, "w:" + entryId,
                codec.toBytes(new SpaceWire.Delta(write, null, null)));
        tickAll(4);
        assertThat(a.space().read(Template.of(TaskEntry.class)))
                .contains(new TaskEntry("evil-owned", 1));
        assertThat(b.space().read(Template.of(TaskEntry.class))).isPresent();

        // Issuer-signed withdrawal carrying MAX_DOTS_PER_STATE + 1 remove dots: dropped by the cap.
        String issuerPeer = evil.id().peerId().value();
        List<Dot> inflated = IntStream.rangeClosed(1, ReplicatedSpace.MAX_DOTS_PER_STATE + 1)
                .mapToObj(i -> new Dot(issuerPeer, i)).toList();
        SpaceWire.EntryStateDto oversize = ForgeSupport.signState(evil.id(),
                new SpaceWire.EntryStateDto(write.record(), write.issuerPublicKey(), write.adds(),
                        inflated, write.leaseStamp(), write.leaseValue(), false, null));
        evil.runtime().gossip().publish(STREAM, "x-oversize:" + entryId,
                codec.toBytes(new SpaceWire.Delta(oversize, null, null)));
        tickAll(4);
        assertThat(a.space().read(Template.of(TaskEntry.class)))
                .as("the inflated dot set was dropped").isPresent();

        // Issuer-signed withdrawal naming a dot minted by a foreign replica: dropped by binding.
        SpaceWire.EntryStateDto foreign = ForgeSupport.signState(evil.id(),
                new SpaceWire.EntryStateDto(write.record(), write.issuerPublicKey(), write.adds(),
                        List.of(new Dot(issuerPeer, 1), new Dot("mallory", 1)),
                        write.leaseStamp(), write.leaseValue(), false, null));
        evil.runtime().gossip().publish(STREAM, "x-foreign:" + entryId,
                codec.toBytes(new SpaceWire.Delta(foreign, null, null)));
        tickAll(4);
        assertThat(a.space().read(Template.of(TaskEntry.class)))
                .as("the foreign-replica dot was dropped").isPresent();

        // Control: the same issuer's well-formed withdrawal merges, so the checks above bite.
        SpaceWire.EntryStateDto cancel = ForgeSupport.signState(evil.id(),
                new SpaceWire.EntryStateDto(write.record(), write.issuerPublicKey(), write.adds(),
                        write.adds(), write.leaseStamp(), write.leaseValue(), false, null));
        evil.runtime().gossip().publish(STREAM, "x-legit:" + entryId,
                codec.toBytes(new SpaceWire.Delta(cancel, null, null)));
        tickAll(4);
        assertThat(a.space().read(Template.of(TaskEntry.class))).isEmpty();
        assertThat(b.space().read(Template.of(TaskEntry.class))).isEmpty();
    }
}
