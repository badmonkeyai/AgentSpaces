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
import ai.badmonkey.agentspaces.api.space.Matchers;
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

    /**
     * HLC round six, 2026-11-08 (owner rulings F1 and F3): a delta decodes only from its own wire
     * encoding. A signed delta relabelled in transit (a boolean sent as an integer, a byte after
     * the item, a claim epoch sent as a double) once decoded through Jackson's coercions, and the
     * signatures verified over the repaired re-encoding. Each relabel now counts as undecodable,
     * merges nothing and strikes nobody, and the honest bytes still merge.
     */
    @Test
    void aRelabelledSignedDeltaIsUndecodableAndStrikesNobody() throws Exception {
        Peer a = newPeer("a", 1);
        Attacker evil = attackerWithKey("evil", 3, "a", new AtomicReference<>());
        tickAll(4);
        long expires = clock.millis() + Duration.ofMinutes(30).toMillis();
        SpaceWire.EntryStateDto write = ForgeSupport.authoredWrite(evil.id(), "evil", a.space().id(),
                new TaskEntry("relabelled", 1), new HlcTimestamp(clock.millis(), 0, "evil"), expires);
        byte[] honest = codec.toBytes(new SpaceWire.Delta(write, null, null));
        byte[] completed = concat(textString("completed"), new byte[]{(byte) 0xF4});
        java.util.Map<String, byte[]> relabels = new java.util.LinkedHashMap<>();
        relabels.put("completed as the integer 0", replaceFirst(honest, completed,
                concat(textString("completed"), new byte[]{0x00})));
        relabels.put("a byte after the item", concat(honest, new byte[]{0x00}));
        a.space().write(new TaskEntry("to take", 2), MINUTES_30);
        tickAll(2);
        EntryId taken = a.space().entryIdOf(new TaskEntry("to take", 2)).orElseThrow();
        byte[] claim = codec.toBytes(new SpaceWire.Delta(null, taken, ForgeSupport.signClaim(evil.id(),
                new TakeClaim(taken, a.space().id(), 1, new HlcTimestamp(clock.millis(), 0, "evil"),
                        evil.id().agent("evil"), 0.0, clock.millis() + Duration.ofMinutes(5).toMillis()))));
        byte[] epoch = concat(textString("epoch"), new byte[]{0x01});
        relabels.put("a claim epoch as the double 1.0", replaceFirst(claim, epoch,
                concat(textString("epoch"), new byte[]{(byte) 0xFB, 0x3F, (byte) 0xF0, 0, 0, 0, 0, 0, 0})));
        long undecodable = a.space().deltasUndecodable();
        for (java.util.Map.Entry<String, byte[]> relabel : relabels.entrySet()) {
            evil.runtime().gossip().publish(STREAM, "relabel:" + relabel.getKey(), relabel.getValue());
            tickAll(2);
            assertThat(a.space().deltasUndecodable()).as(relabel.getKey()).isEqualTo(++undecodable);
        }
        assertThat(a.space().entryIdOf(new TaskEntry("relabelled", 1))).as("nothing merged").isEmpty();
        assertThat(a.space().currentClaim(taken)).as("the relabelled claim merged nothing").isEmpty();
        assertThat(strikes(a.node(), evil.id().peerId())).as("no strike").isZero();

        evil.runtime().gossip().publish(STREAM, "honest", honest);
        evil.runtime().gossip().publish(STREAM, "honest claim", claim);
        tickAll(2);
        assertThat(a.space().entryIdOf(new TaskEntry("relabelled", 1))).as("the honest bytes merge").isPresent();
        assertThat(a.space().currentClaim(taken)).map(TakeClaim::holder).contains(evil.id().agent("evil"));
        assertThat(a.space().deltasUndecodable()).isEqualTo(undecodable);
    }

    /**
     * HLC round six, 2026-11-08 (owner ruling F4): an agent-signed state's {@code signedAt} is
     * judged by the far-future rule of decision D4, as its record and lease stamps are, in place
     * of the certificate's silent drift drop: past the margin it waits unmerged and unstruck and
     * merges once the wall catches up, and an hour beyond the margin it is refused, acknowledged,
     * counted, and its signer struck.
     */
    @Test
    void anAgentSignedStateWhoseSignedAtLeadsIsJudgedByTheFarFutureRule() throws Exception {
        Peer a = newPeer("a", 1);
        Attacker evil = attackerWithKey("evil", 3, "a", new AtomicReference<>());
        tickAll(4);
        ai.badmonkey.agentspaces.api.spi.AgentIdentity agent =
                evil.id().subordinate("writer", clock.instant(), Duration.ofHours(4));
        long expires = clock.millis() + Duration.ofMinutes(30).toMillis();
        SpaceWire.EntryStateDto ahead = ForgeSupport.agentWrite(evil.id(), agent, a.space().id(),
                new TaskEntry("signed ahead", 1), new HlcTimestamp(clock.millis(), 0, "evil"),
                new HlcTimestamp(pastTheMargin(Duration.ofSeconds(30)), 0, "evil"), expires);
        publishState(evil, "signed-ahead", ahead);
        tickAll(2);

        assertThat(a.space().knownEntries()).as("deferred and unmerged").isZero();
        assertThat(a.space().stampsFarFutureDeferred()).isEqualTo(1);
        assertThat(a.space().stampsFarFutureRefused()).isZero();
        assertThat(strikes(a.node(), evil.id().peerId())).as("not struck").isZero();

        offerByAntiEntropy(evil, codec.toBytes(new SpaceWire.SyncDelta(List.of(ahead), Map.of())));
        tickAll(35);
        assertThat(a.space().read(Template.of(TaskEntry.class))).as("merged once the wall caught up")
                .contains(new TaskEntry("signed ahead", 1));
        assertThat(a.space().stampsFarFutureDeferred()).as("once per distinct offer").isEqualTo(1);

        SpaceWire.EntryStateDto far = ForgeSupport.agentWrite(evil.id(), agent, a.space().id(),
                new TaskEntry("signed hours ahead", 2), new HlcTimestamp(clock.millis(), 0, "evil"),
                new HlcTimestamp(clock.millis() + Duration.ofHours(2).toMillis(), 0, "evil"),
                clock.millis() + Duration.ofMinutes(30).toMillis());
        publishState(evil, "signed-far", far);
        tickAll(2);
        assertThat(a.space().entryIdOf(new TaskEntry("signed hours ahead", 2))).as("refused").isEmpty();
        assertThat(a.space().stampsFarFutureRefused()).isEqualTo(1);
        assertThat(strikes(a.node(), evil.id().peerId())).isEqualTo(1);
        assertThat(new String(a.space().digest(), java.nio.charset.StandardCharsets.UTF_8))
                .as("acknowledged").contains("a:e:" + far.record().entryId().value() + "=");
    }

    /**
     * HLC round six, 2026-11-08 (ruling R10): a claim for an entry this replica never held, or
     * collected, is dropped once its expiry passes, so a stream of claims for entries that never
     * arrive leaves the claim map bounded. A claim for an entry the replica holds stays until the
     * entry is collected.
     */
    @Test
    void claimsForEntriesThisReplicaDoesNotHoldAreDroppedOnceTheyExpire() throws Exception {
        Peer a = newPeer("a", 1);
        Attacker evil = attackerWithKey("evil", 3, "a", new AtomicReference<>());
        tickAll(4);
        a.space().write(new TaskEntry("held", 1), MINUTES_30);
        tickAll(2);
        EntryId held = a.space().entryIdOf(new TaskEntry("held", 1)).orElseThrow();
        long holdUntil = clock.millis() + Duration.ofMinutes(1).toMillis();
        evil.runtime().gossip().publish(STREAM, "held", codec.toBytes(new SpaceWire.Delta(null, held,
                ForgeSupport.signClaim(evil.id(), new TakeClaim(held, a.space().id(), 1,
                        new HlcTimestamp(clock.millis(), 0, "evil"), evil.id().agent("evil"), 0.0, holdUntil)))));
        int perRound = 40;
        for (int round = 0; round < 3; round++) {
            for (int i = 0; i < perRound; i++) {
                EntryId never = EntryId.newId();
                evil.runtime().gossip().publish(STREAM, "never:" + round + ":" + i,
                        codec.toBytes(new SpaceWire.Delta(null, never, ForgeSupport.signClaim(evil.id(),
                                new TakeClaim(never, a.space().id(), 1, new HlcTimestamp(clock.millis(), 0, "evil"),
                                        evil.id().agent("evil"), 0.0,
                                        clock.millis() + Duration.ofMinutes(1).toMillis())))));
            }
            tickAll(2);
            assertThat(a.space().claimsHeld()).as("round " + round + ": merged while live")
                    .isBetween(perRound, perRound + 1);
            clock.advance(Duration.ofMinutes(2));
            a.space().sweepNow();
            assertThat(a.space().claimsHeld()).as("round " + round + ": the claim of the held entry alone")
                    .isEqualTo(1);
        }
        assertThat(a.space().currentClaim(held)).as("kept while the entry is held").isPresent();
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

    /** How far past the receiver's far-future ceiling (wall + MAX_DRIFT + the skew margin) a stamp lies. */
    private long pastTheMargin(Duration lead) {
        return clock.millis() + HybridLogicalClock.MAX_DRIFT_MILLIS + PeerNode.FAR_FUTURE_SKEW_MARGIN_MILLIS
                + lead.toMillis();
    }

    /** The attacker answers every anti-entropy pull with the given delta, as a replica holding it would. */
    private static void offerByAntiEntropy(Attacker evil, byte[] delta) {
        evil.runtime().gossip().reconcile(STREAM, new ReconcilableState() {
            @Override
            public byte[] digest() {
                return "e:offered=ffffffff\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }

            @Override
            public byte[] deltaFor(byte[] remoteDigest) {
                return delta;
            }

            @Override
            public void applyDelta(byte[] delta) {
            }
        });
    }

    /**
     * HLC round four, 2026-11-08 (decision D4): a hostile stamp can drag an honest replica's
     * clock to its ceiling, and its next write then lies past the ceiling of a receiver whose
     * wall runs behind. Such a state is deferred: not merged, not acknowledged, not counted as
     * misbehaviour, not struck. Anti-entropy offers it again, and it merges once the receiver's
     * wall catches up.
     */
    @Test
    void aRecordIssuedPastTheMarginIsDeferredAndMergesOnceTheWallCatchesUp() throws Exception {
        Peer a = newPeer("a", 1);
        Attacker evil = attackerWithKey("evil", 3, "a", new AtomicReference<>());
        tickAll(4);
        long issued = pastTheMargin(Duration.ofSeconds(30));
        SpaceWire.EntryStateDto ahead = ForgeSupport.authoredWrite(evil.id(), "evil", a.space().id(),
                new TaskEntry("ahead", 1), new HlcTimestamp(issued, 0, "evil"),
                issued + Duration.ofMinutes(30).toMillis());
        publishState(evil, "ahead", ahead);
        tickAll(2);

        assertThat(a.space().knownEntries()).as("deferred and unmerged").isZero();
        assertThat(a.space().stampsFarFutureDeferred()).isEqualTo(1);
        assertThat(a.space().stampsFarFutureRefused()).isZero();
        assertThat(strikes(a.node(), evil.id().peerId())).as("not struck").isZero();
        assertThat(new String(a.space().digest(), java.nio.charset.StandardCharsets.UTF_8))
                .as("not acknowledged").doesNotContain(ahead.record().entryId().value());

        offerByAntiEntropy(evil, codec.toBytes(new SpaceWire.SyncDelta(List.of(ahead), Map.of())));
        tickAll(10);
        assertThat(a.space().knownEntries()).as("still ahead of the wall").isZero();
        tickAll(25);

        assertThat(a.space().read(Template.of(TaskEntry.class))).as("merged once the wall caught up")
                .contains(new TaskEntry("ahead", 1));
        assertThat(a.space().stampsFarFutureDeferred()).as("once per distinct offer").isEqualTo(1);
        assertThat(a.space().stampsFarFutureRefused()).isZero();
        assertThat(strikes(a.node(), evil.id().peerId())).isZero();
    }

    /**
     * HLC round four, 2026-11-08 (decision D4): a state stamped beyond the margin plus one hour
     * is hostile. Once its signatures verify it is refused, counted once per distinct offer,
     * acknowledged in the digest, and its signer struck. A lease stamp is judged as the issue
     * stamp is.
     */
    @Test
    void aRecordOrLeaseStampedHoursAheadIsRefusedAndStruck() throws Exception {
        Peer a = newPeer("a", 1);
        Attacker evil = attackerWithKey("evil", 3, "a", new AtomicReference<>());
        tickAll(4);
        long hostile = clock.millis() + Duration.ofHours(2).toMillis();
        SpaceWire.EntryStateDto record = ForgeSupport.authoredWrite(evil.id(), "evil", a.space().id(),
                new TaskEntry("hours ahead", 1), new HlcTimestamp(hostile, 0, "evil"),
                hostile + Duration.ofMinutes(30).toMillis());
        publishState(evil, "record-1", record);
        tickAll(2);

        assertThat(a.space().knownEntries()).as("refused").isZero();
        assertThat(a.space().stampsFarFutureRefused()).isEqualTo(1);
        assertThat(a.space().stampsFarFutureDeferred()).isZero();
        assertThat(strikes(a.node(), evil.id().peerId())).isEqualTo(1);
        assertThat(new String(a.space().digest(), java.nio.charset.StandardCharsets.UTF_8))
                .as("acknowledged").contains("a:e:" + record.record().entryId().value() + "=");

        publishState(evil, "record-2", record);
        tickAll(2);
        assertThat(a.space().stampsFarFutureRefused()).as("a re-offer is the same state").isEqualTo(1);
        assertThat(strikes(a.node(), evil.id().peerId())).isEqualTo(1);

        long expires = clock.millis() + Duration.ofMinutes(30).toMillis();
        SpaceWire.EntryStateDto honest = ForgeSupport.authoredWrite(evil.id(), "evil", a.space().id(),
                new TaskEntry("lease ahead", 2), new HlcTimestamp(clock.millis(), 0, "evil"), expires);
        publishState(evil, "lease", ForgeSupport.signState(evil.id(),
                new SpaceWire.EntryStateDto(honest.record(), honest.issuerPublicKey(), honest.adds(),
                        honest.removes(), new HlcTimestamp(hostile, 0, "evil"), honest.leaseValue(), false, null)));
        tickAll(2);
        assertThat(a.space().knownEntries()).as("refused").isZero();
        assertThat(a.space().stampsFarFutureRefused()).isEqualTo(2);
        assertThat(strikes(a.node(), evil.id().peerId())).isEqualTo(2);
        assertClockNotFrozen(a, new TaskEntry("a-own", 3));
    }

    /**
     * HLC round four, 2026-11-08 (decision D4): a claim's stamp is judged as a state's is. Past
     * the margin it is deferred and merges once the wall catches up; beyond the margin plus one
     * hour it is refused, counted, and its holder struck.
     */
    @Test
    void aClaimStampedPastTheMarginIsDeferredAndHoursAheadIsRefused() throws Exception {
        Peer a = newPeer("a", 1);
        Attacker evil = attackerWithKey("evil", 3, "a", new AtomicReference<>());
        tickAll(4);
        a.space().write(new TaskEntry("deferred take", 1), MINUTES_30);
        a.space().write(new TaskEntry("hostile take", 2), MINUTES_30);
        tickAll(2);
        EntryId deferred = a.space().entryIdOf(new TaskEntry("deferred take", 1)).orElseThrow();
        EntryId hostile = a.space().entryIdOf(new TaskEntry("hostile take", 2)).orElseThrow();
        long holdUntil = clock.millis() + Duration.ofMinutes(20).toMillis();
        SpaceWire.SignedClaim ahead = ForgeSupport.signClaim(evil.id(), new TakeClaim(deferred, a.space().id(), 1,
                new HlcTimestamp(pastTheMargin(Duration.ofSeconds(30)), 0, "evil"), evil.id().agent("evil"),
                0.0, holdUntil));
        evil.runtime().gossip().publish(STREAM, "ahead:" + deferred,
                codec.toBytes(new SpaceWire.Delta(null, deferred, ahead)));
        tickAll(2);
        assertThat(a.space().currentClaim(deferred)).as("deferred").isEmpty();
        assertThat(a.space().stampsFarFutureDeferred()).isEqualTo(1);
        assertThat(strikes(a.node(), evil.id().peerId())).isZero();

        offerByAntiEntropy(evil, codec.toBytes(new SpaceWire.SyncDelta(List.of(),
                Map.of(deferred.value(), ahead))));
        tickAll(35);
        assertThat(a.space().currentClaim(deferred)).map(TakeClaim::holder)
                .as("merged once the wall caught up").contains(evil.id().agent("evil"));
        assertThat(strikes(a.node(), evil.id().peerId())).isZero();

        SpaceWire.SignedClaim far = ForgeSupport.signClaim(evil.id(), new TakeClaim(hostile, a.space().id(), 1,
                new HlcTimestamp(clock.millis() + Duration.ofHours(2).toMillis(), 0, "evil"),
                evil.id().agent("evil"), 0.0, holdUntil));
        evil.runtime().gossip().publish(STREAM, "far:" + hostile,
                codec.toBytes(new SpaceWire.Delta(null, hostile, far)));
        tickAll(2);
        assertThat(a.space().currentClaim(hostile)).as("refused").isEmpty();
        assertThat(a.space().stampsFarFutureRefused()).isEqualTo(1);
        assertThat(strikes(a.node(), evil.id().peerId())).isEqualTo(1);
    }

    /**
     * HLC round five, 2026-11-08 (decision E4): a record an agent key signs under the peer's
     * certificate is judged as a peer-signed record is. The record's certificate check no longer
     * drops an issue stamp past the drift ceiling; once the signatures and the certificate
     * verify, a write past the far-future margin is deferred, unstruck, and merges once the wall
     * catches up, and one an hour beyond the margin is refused, acknowledged, and struck. The
     * agent signs the state at the receiver's time here, so only the issue stamp leads.
     */
    @Test
    void anAgentSignedWritePastTheMarginIsDeferredAndHoursAheadIsRefused() throws Exception {
        Peer a = newPeer("a", 1);
        Attacker evil = attackerWithKey("evil", 3, "a", new AtomicReference<>());
        tickAll(4);
        ai.badmonkey.agentspaces.api.spi.AgentIdentity agent =
                evil.id().subordinate("writer", clock.instant(), Duration.ofHours(4));
        long issued = pastTheMargin(Duration.ofSeconds(30));
        SpaceWire.EntryStateDto ahead = ForgeSupport.agentWrite(evil.id(), agent, a.space().id(),
                new TaskEntry("agent ahead", 1), new HlcTimestamp(issued, 0, "evil"),
                new HlcTimestamp(clock.millis(), 0, "evil"), issued + Duration.ofMinutes(30).toMillis());
        publishState(evil, "ahead", ahead);
        tickAll(2);

        assertThat(a.space().knownEntries()).as("deferred and unmerged").isZero();
        assertThat(a.space().stampsFarFutureDeferred()).isEqualTo(1);
        assertThat(a.space().stampsFarFutureRefused()).isZero();
        assertThat(strikes(a.node(), evil.id().peerId())).as("not struck").isZero();

        offerByAntiEntropy(evil, codec.toBytes(new SpaceWire.SyncDelta(List.of(ahead), Map.of())));
        tickAll(35);
        assertThat(a.space().read(Template.of(TaskEntry.class))).as("merged once the wall caught up")
                .contains(new TaskEntry("agent ahead", 1));
        assertThat(a.space().stampsFarFutureDeferred()).as("once per distinct offer").isEqualTo(1);

        long hostile = clock.millis() + Duration.ofHours(2).toMillis();
        SpaceWire.EntryStateDto far = ForgeSupport.agentWrite(evil.id(), agent, a.space().id(),
                new TaskEntry("agent hours ahead", 2), new HlcTimestamp(hostile, 0, "evil"),
                new HlcTimestamp(clock.millis(), 0, "evil"), hostile + Duration.ofMinutes(30).toMillis());
        publishState(evil, "far", far);
        tickAll(2);
        assertThat(a.space().entryIdOf(new TaskEntry("agent hours ahead", 2))).as("refused").isEmpty();
        assertThat(a.space().stampsFarFutureRefused()).isEqualTo(1);
        assertThat(strikes(a.node(), evil.id().peerId())).isEqualTo(1);
        assertThat(new String(a.space().digest(), java.nio.charset.StandardCharsets.UTF_8))
                .as("acknowledged").contains("a:e:" + far.record().entryId().value() + "=");
    }

    /**
     * HLC round five, 2026-11-08 (decision E4): a claim an agent signs under its certificate is
     * judged as a peer-signed claim is: deferred past the margin, merged once the wall catches
     * up, and refused and struck an hour beyond it.
     */
    @Test
    void anAgentSignedClaimPastTheMarginIsDeferredAndHoursAheadIsRefused() throws Exception {
        Peer a = newPeer("a", 1);
        Attacker evil = attackerWithKey("evil", 3, "a", new AtomicReference<>());
        tickAll(4);
        a.space().write(new TaskEntry("deferred take", 1), MINUTES_30);
        a.space().write(new TaskEntry("hostile take", 2), MINUTES_30);
        tickAll(2);
        EntryId deferred = a.space().entryIdOf(new TaskEntry("deferred take", 1)).orElseThrow();
        EntryId hostile = a.space().entryIdOf(new TaskEntry("hostile take", 2)).orElseThrow();
        ai.badmonkey.agentspaces.api.spi.AgentIdentity taker =
                evil.id().subordinate("taker", clock.instant(), Duration.ofHours(4));
        long holdUntil = clock.millis() + Duration.ofMinutes(20).toMillis();
        SpaceWire.SignedClaim ahead = ForgeSupport.signClaim(evil.id(), taker, new TakeClaim(deferred,
                a.space().id(), 1, new HlcTimestamp(pastTheMargin(Duration.ofSeconds(30)), 0, "evil"),
                taker.id(), 0.0, holdUntil));
        evil.runtime().gossip().publish(STREAM, "ahead:" + deferred,
                codec.toBytes(new SpaceWire.Delta(null, deferred, ahead)));
        tickAll(2);
        assertThat(a.space().currentClaim(deferred)).as("deferred").isEmpty();
        assertThat(a.space().stampsFarFutureDeferred()).isEqualTo(1);
        assertThat(strikes(a.node(), evil.id().peerId())).isZero();

        offerByAntiEntropy(evil, codec.toBytes(new SpaceWire.SyncDelta(List.of(),
                Map.of(deferred.value(), ahead))));
        tickAll(35);
        assertThat(a.space().currentClaim(deferred)).map(TakeClaim::holder)
                .as("merged once the wall caught up").contains(taker.id());
        assertThat(strikes(a.node(), evil.id().peerId())).isZero();

        SpaceWire.SignedClaim far = ForgeSupport.signClaim(evil.id(), taker, new TakeClaim(hostile,
                a.space().id(), 1, new HlcTimestamp(clock.millis() + Duration.ofHours(2).toMillis(), 0, "evil"),
                taker.id(), 0.0, holdUntil));
        evil.runtime().gossip().publish(STREAM, "far:" + hostile,
                codec.toBytes(new SpaceWire.Delta(null, hostile, far)));
        tickAll(2);
        assertThat(a.space().currentClaim(hostile)).as("refused").isEmpty();
        assertThat(a.space().stampsFarFutureRefused()).isEqualTo(1);
        assertThat(strikes(a.node(), evil.id().peerId())).isEqualTo(1);
    }

    /**
     * HLC round five, 2026-11-08 (decision E7): a state whose {@code leaseStamp} is null or
     * absent decodes, in every implementation alike. A lease register holds nothing without its
     * stamp and its value, so the replica drops such a state, signed or not, before it judges
     * anything: nothing merges, no counter moves, nobody is struck, and the replica goes on
     * merging. Before, the signed state reached the revocation check, threw, and struck its sender.
     */
    @Test
    void aStateWithoutALeaseStampDecodesAndIsDroppedUncounted() throws Exception {
        Peer a = newPeer("a", 1);
        Attacker evil = attackerWithKey("evil", 3, "a", new AtomicReference<>());
        tickAll(4);
        long expires = clock.millis() + Duration.ofMinutes(30).toMillis();
        SpaceWire.EntryStateDto honest = ForgeSupport.authoredWrite(evil.id(), "evil", a.space().id(),
                new TaskEntry("no lease stamp", 1), new HlcTimestamp(clock.millis(), 0, "evil"), expires);
        SpaceWire.EntryStateDto nullStamp = ForgeSupport.signState(evil.id(), new SpaceWire.EntryStateDto(
                honest.record(), honest.issuerPublicKey(), honest.adds(), honest.removes(), null,
                honest.leaseValue(), false, null));
        byte[] withNull = codec.toBytes(new SpaceWire.Delta(nullStamp, null, null));
        byte[] key = ai.badmonkey.agentspaces.common.codec.CborCodec.defaultCodec().toBytes("leaseStamp");
        byte[] withoutKey = replaceFirst(withNull, concat(key, new byte[]{(byte) 0xF6}), new byte[0]);
        assertThat(codec.fromWire(withNull, SpaceWire.Delta.class).state().leaseStamp()).isNull();
        // HLC round six, 2026-11-08 (P1): leaseStamp is always present, null written as null
        assertThatThrownBy(() -> codec.fromWire(withoutKey, SpaceWire.Delta.class))
                .isInstanceOf(java.io.UncheckedIOException.class);

        SpaceWire.EntryStateDto nullValue = ForgeSupport.signState(evil.id(), new SpaceWire.EntryStateDto(
                honest.record(), honest.issuerPublicKey(), honest.adds(), honest.removes(), honest.leaseStamp(),
                null, false, null));

        evil.runtime().gossip().publish(STREAM, "null-stamp", withNull);
        evil.runtime().gossip().publish(STREAM, "no-stamp", withoutKey);
        publishState(evil, "null-value", nullValue);
        tickAll(2);
        assertThat(a.space().knownEntries()).isZero();
        assertThat(a.space().deltasUndecodable()).as("the null decodes, the absent key does not").isEqualTo(1);
        assertThat(a.space().stampsLogicalRefused() + a.space().stampsFarFutureRefused()
                + a.space().stampsFarFutureDeferred()).isZero();
        assertThat(strikes(a.node(), evil.id().peerId())).isZero();

        publishState(evil, "honest", honest);
        tickAll(2);
        assertThat(a.space().read(Template.of(TaskEntry.class))).as("the replica goes on merging")
                .contains(new TaskEntry("no lease stamp", 1));
    }

    /**
     * HLC round five, 2026-11-08 (decision E13): a claim's signature covers the canonical
     * encoding of the TakeClaim record, its fields in the record's order (entryId, spaceId,
     * epoch, stamp, holder, bid, expiresAtMillis). HLC round six, 2026-11-08 (owner ruling F3):
     * a claim whose map arrives in any other order is no longer the wire encoding of the record
     * it decodes to, so the delta is undecodable, merges nothing and strikes nobody, where Java
     * once re-encoded it in the record's order and verified it.
     */
    @Test
    void aClaimWhoseMapArrivesInAnotherOrderIsUndecodable() throws Exception {
        Peer a = newPeer("a", 1);
        Attacker evil = attackerWithKey("evil", 3, "a", new AtomicReference<>());
        tickAll(4);
        a.space().write(new TaskEntry("relayed claim", 1), MINUTES_30);
        tickAll(2);
        EntryId entryId = a.space().entryIdOf(new TaskEntry("relayed claim", 1)).orElseThrow();
        TakeClaim claim = new TakeClaim(entryId, a.space().id(), 1, new HlcTimestamp(clock.millis(), 0, "evil"),
                evil.id().agent("taker"), 0.0, clock.millis() + Duration.ofMinutes(10).toMillis());
        SpaceWire.SignedClaim signed = ForgeSupport.signClaim(evil.id(), claim);
        byte[] delta = codec.toBytes(new SpaceWire.Delta(null, entryId, signed));
        ai.badmonkey.agentspaces.common.codec.CanonicalMaps.Verbatim reversed =
                new ai.badmonkey.agentspaces.common.codec.CanonicalMaps.Verbatim();
        reversed.put("expiresAtMillis", claim.expiresAtMillis());
        reversed.put("bid", claim.bid());
        reversed.put("holder", claim.holder().encoded());
        reversed.put("stamp", claim.stamp().encoded());
        reversed.put("epoch", claim.epoch());
        reversed.put("spaceId", claim.spaceId().value());
        reversed.put("entryId", claim.entryId().value());
        byte[] relayed = replaceFirst(delta, codec.toBytes(claim), codec.toBytes(reversed));
        assertThat(relayed).isNotEqualTo(delta);

        long undecodable = a.space().deltasUndecodable();
        evil.runtime().gossip().publish(STREAM, "reversed:" + entryId, relayed);
        tickAll(2);

        assertThat(a.space().currentClaim(entryId)).as("nothing merged").isEmpty();
        assertThat(a.space().deltasUndecodable()).isEqualTo(undecodable + 1);
        assertThat(strikes(a.node(), evil.id().peerId())).isZero();
        evil.runtime().gossip().publish(STREAM, "record order:" + entryId, delta);
        tickAll(2);
        assertThat(a.space().currentClaim(entryId)).map(TakeClaim::holder).contains(evil.id().agent("taker"));
    }

    /**
     * HLC round six, 2026-11-08 (wire encoding section 3.1): an entry payload is an application
     * document signed raw. One that breaks the item rules (here an integer under a head wider than
     * it needs) still replicates, since the signature covers its bytes, and no read hands it to an
     * application, as no other implementation decodes it.
     */
    @Test
    void aPayloadOutsideTheWireEncodingReplicatesAndNoReadDecodesIt() throws Exception {
        Peer a = newPeer("a", 1);
        Attacker evil = attackerWithKey("evil", 3, "a", new AtomicReference<>());
        tickAll(4);
        long expires = clock.millis() + Duration.ofMinutes(30).toMillis();
        byte[] honest = codec.toBytes(new TaskEntry("wide", 1));
        byte[] wide = replaceFirst(honest, concat(textString("priority"), new byte[]{0x01}),
                concat(textString("priority"), new byte[]{0x18, 0x01}));
        assertThat(wide).isNotEqualTo(honest);
        AgentId issuer = evil.id().agent("evil");
        LeaseInfo lease = new LeaseInfo(issuer, expires, LeaseKind.WRITE);
        HlcTimestamp issued = new HlcTimestamp(clock.millis(), 0, "evil");
        ai.badmonkey.agentspaces.api.entry.EntryRecord record = ForgeSupport.signRecord(evil.id(),
                new ai.badmonkey.agentspaces.api.entry.EntryRecord(EntryId.newId(), a.space().id(),
                        ForgeSupport.schemaNameOf(TaskEntry.class), wide, null, issuer, issued, lease, Map.of(), null));
        publishState(evil, "wide", ForgeSupport.signState(evil.id(), new SpaceWire.EntryStateDto(record,
                evil.id().rawPublicKey(), List.of(new Dot(evil.id().peerId().value(), 1)), List.of(),
                issued, lease, false, null)));
        tickAll(2);

        assertThat(a.space().knownEntries()).as("the signed entry replicates").isEqualTo(1);
        assertThat(a.space().read(Template.of(TaskEntry.class))).as("and no read decodes it").isEmpty();
        assertThat(strikes(a.node(), evil.id().peerId())).isZero();
        publishState(evil, "canonical", ForgeSupport.authoredWrite(evil.id(), "evil", a.space().id(),
                new TaskEntry("canonical", 2), new HlcTimestamp(clock.millis(), 1, "evil"), expires));
        tickAll(2);
        assertThat(a.space().read(Template.of(TaskEntry.class))).contains(new TaskEntry("canonical", 2));
    }

    /**
     * HLC round six, 2026-11-08 (ruling R3): a reader that counts the records it refuses learns
     * from the space which live entries its template matches by type and tags but cannot decode;
     * the reads skip them, an entry that decodes is not among them, and neither is an entry whose
     * tags the template does not match.
     */
    @Test
    void theSpaceReportsTheMatchedEntriesItCannotDecode() throws Exception {
        Peer a = newPeer("a", 1);
        Attacker evil = attackerWithKey("evil", 3, "a", new AtomicReference<>());
        tickAll(4);
        long expires = clock.millis() + Duration.ofMinutes(30).toMillis();
        AgentId issuer = evil.id().agent("evil");
        LeaseInfo lease = new LeaseInfo(issuer, expires, LeaseKind.WRITE);
        HlcTimestamp issued = new HlcTimestamp(clock.millis(), 0, "evil");
        ai.badmonkey.agentspaces.api.entry.EntryRecord record = ForgeSupport.signRecord(evil.id(),
                new ai.badmonkey.agentspaces.api.entry.EntryRecord(EntryId.newId(), a.space().id(),
                        ForgeSupport.schemaNameOf(TaskEntry.class), codec.toBytes("no task"), null, issuer,
                        issued, lease, Map.of("k", "v"), null));
        publishState(evil, "no task", ForgeSupport.signState(evil.id(), new SpaceWire.EntryStateDto(record,
                evil.id().rawPublicKey(), List.of(new Dot(evil.id().peerId().value(), 1)), List.of(),
                issued, lease, false, null)));
        a.space().write(new TaskEntry("decodes", 1), MINUTES_30, Map.of("k", "v"));
        tickAll(2);

        Template<TaskEntry> tagged = Template.of(TaskEntry.class).whereTag("k", Matchers.eq("v"));
        assertThat(a.space().readAll(tagged, 10)).containsExactly(new TaskEntry("decodes", 1));
        assertThat(a.space().undecodableEntries(tagged, 10)).containsExactly(record.entryId());
        assertThat(a.space().undecodableEntries(Template.of(TaskEntry.class).whereTag("k", Matchers.eq("w")), 10))
                .isEmpty();
    }

    /**
     * HLC round six, 2026-11-08 (P2, owner ruling F1): a record's tags map text to text. A state
     * whose record holds a null tag value, honestly signed over those bytes, re-encodes to them,
     * yet no tag value is ever null, so the delta is undecodable, merges nothing and strikes nobody.
     */
    @Test
    void aStateWhoseRecordHoldsANullTagValueIsUndecodable() throws Exception {
        Peer a = newPeer("a", 1);
        Attacker evil = attackerWithKey("evil", 3, "a", new AtomicReference<>());
        tickAll(4);
        long expires = clock.millis() + Duration.ofMinutes(30).toMillis();
        AgentId issuer = evil.id().agent("evil");
        LeaseInfo lease = new LeaseInfo(issuer, expires, LeaseKind.WRITE);
        HlcTimestamp issued = new HlcTimestamp(clock.millis(), 0, "evil");
        Map<String, String> tags = new java.util.LinkedHashMap<>();
        tags.put("k", null);
        ai.badmonkey.agentspaces.api.entry.EntryRecord record = ForgeSupport.signRecord(evil.id(),
                new ai.badmonkey.agentspaces.api.entry.EntryRecord(EntryId.newId(), a.space().id(),
                        ForgeSupport.schemaNameOf(TaskEntry.class), codec.toBytes(new TaskEntry("null tag", 1)),
                        null, issuer, issued, lease, tags, null));
        publishState(evil, "null tag", ForgeSupport.signState(evil.id(), new SpaceWire.EntryStateDto(record,
                evil.id().rawPublicKey(), List.of(new Dot(evil.id().peerId().value(), 1)), List.of(),
                issued, lease, false, null)));
        tickAll(2);

        assertThat(a.space().deltasUndecodable()).isEqualTo(1);
        assertThat(a.space().knownEntries()).as("nothing merged").isZero();
        assertThat(strikes(a.node(), evil.id().peerId())).isZero();
    }

    /**
     * HLC round six, 2026-11-08 (owner rulings F1 and F3): an anti-entropy delta decodes only from
     * its own wire encoding, as a rumored delta does. A replica that forwards another holder's
     * signed claim with the claim's fields in another order offers a delta that merges nothing and
     * counts as undecodable; the same claim in record order merges.
     */
    @Test
    void aClaimRelabelledInAnAntiEntropyDeltaIsUndecodable() throws Exception {
        Peer a = newPeer("a", 1);
        Attacker evil = attackerWithKey("evil", 3, "a", new AtomicReference<>());
        tickAll(4);
        a.space().write(new TaskEntry("pulled claim", 1), MINUTES_30);
        tickAll(2);
        EntryId entryId = a.space().entryIdOf(new TaskEntry("pulled claim", 1)).orElseThrow();
        TakeClaim claim = new TakeClaim(entryId, a.space().id(), 1, new HlcTimestamp(clock.millis(), 0, "evil"),
                evil.id().agent("taker"), 0.0, clock.millis() + Duration.ofMinutes(10).toMillis());
        SpaceWire.SignedClaim signed = ForgeSupport.signClaim(evil.id(), claim);
        byte[] delta = codec.toBytes(new SpaceWire.SyncDelta(List.of(), Map.of(entryId.value(), signed)));
        ai.badmonkey.agentspaces.common.codec.CanonicalMaps.Verbatim reordered =
                new ai.badmonkey.agentspaces.common.codec.CanonicalMaps.Verbatim();
        reordered.put("spaceId", claim.spaceId().value());
        reordered.put("entryId", claim.entryId().value());
        reordered.put("epoch", claim.epoch());
        reordered.put("stamp", claim.stamp().encoded());
        reordered.put("holder", claim.holder().encoded());
        reordered.put("bid", claim.bid());
        reordered.put("expiresAtMillis", claim.expiresAtMillis());
        byte[] relabelled = replaceFirst(delta, codec.toBytes(claim), codec.toBytes(reordered));
        assertThat(relabelled).isNotEqualTo(delta);

        long undecodable = a.space().deltasUndecodable();
        offerByAntiEntropy(evil, relabelled);
        tickAll(10);
        assertThat(a.space().currentClaim(entryId)).as("nothing merged").isEmpty();
        assertThat(a.space().deltasUndecodable()).as("counted undecodable").isGreaterThan(undecodable);
        assertThat(strikes(a.node(), evil.id().peerId())).isZero();

        Attacker relay = attackerWithKey("relay", 4, "a", new AtomicReference<>());
        tickAll(4);
        offerByAntiEntropy(relay, delta);
        tickAll(10);
        assertThat(a.space().currentClaim(entryId)).map(TakeClaim::holder).contains(evil.id().agent("taker"));
    }

    /**
     * HLC round four, 2026-11-08 (decision D5): a refusal is keyed on signed content alone. The
     * signature covers a state's dots sorted, so the same state with its dots in another order
     * is one offer; two states that differ only in {@code signedAt} are two; and two claims that
     * differ only in their holder are two.
     */
    @Test
    void refusalsAreCountedOncePerDistinctSignedContent() throws Exception {
        Peer a = newPeer("a", 1);
        Attacker evil = attackerWithKey("evil", 3, "a", new AtomicReference<>());
        Attacker other = attackerWithKey("other", 4, "a", new AtomicReference<>());
        tickAll(4);
        long expires = clock.millis() + Duration.ofMinutes(30).toMillis();
        SpaceWire.EntryStateDto write = ForgeSupport.authoredWrite(evil.id(), "evil", a.space().id(),
                new TaskEntry("two dots", 1), new HlcTimestamp(clock.millis(), 0, "evil"), expires);
        String issuerPeer = evil.id().peerId().value();
        SpaceWire.EntryStateDto twoDots = ForgeSupport.signState(evil.id(), new SpaceWire.EntryStateDto(
                write.record(), write.issuerPublicKey(), List.of(new Dot(issuerPeer, 1), new Dot(issuerPeer, 2)),
                List.of(), freezingStamp("evil"), write.leaseValue(), false, null));
        publishState(evil, "sorted", twoDots);
        publishState(evil, "reversed", new SpaceWire.EntryStateDto(twoDots.record(), twoDots.issuerPublicKey(),
                List.of(new Dot(issuerPeer, 2), new Dot(issuerPeer, 1)), List.of(), twoDots.leaseStamp(),
                twoDots.leaseValue(), false, twoDots.stateSig()));
        tickAll(2);
        assertThat(a.space().stampsLogicalRefused()).as("the same signed state").isEqualTo(1);

        ai.badmonkey.agentspaces.api.spi.AgentIdentity agent =
                evil.id().subordinate("evil", clock.instant(), Duration.ofHours(1));
        SpaceWire.EntryStateDto signed = ForgeSupport.authoredWrite(evil.id(), "evil", a.space().id(),
                new TaskEntry("signedAt", 2), new HlcTimestamp(clock.millis(), 0, "evil"), expires);
        publishState(evil, "signedAt-1", agentSigned(agent, signed,
                new HlcTimestamp(clock.millis(), HybridLogicalClock.LOGICAL_LIMIT + 1, "evil")));
        publishState(evil, "signedAt-2", agentSigned(agent, signed,
                new HlcTimestamp(clock.millis(), HybridLogicalClock.LOGICAL_LIMIT + 2, "evil")));
        tickAll(2);
        assertThat(a.space().stampsLogicalRefused()).as("signedAt is signed content").isEqualTo(3);

        a.space().write(new TaskEntry("contested", 3), MINUTES_30);
        tickAll(2);
        EntryId entryId = a.space().entryIdOf(new TaskEntry("contested", 3)).orElseThrow();
        HlcTimestamp stamp = freezingStamp("shared");
        long holdUntil = clock.millis() + Duration.ofMinutes(10).toMillis();
        for (Attacker holder : List.of(evil, other)) {
            SpaceWire.SignedClaim claim = ForgeSupport.signClaim(holder.id(), new TakeClaim(entryId,
                    a.space().id(), 1, stamp, holder.id().agent("taker"), 0.0, holdUntil));
            holder.runtime().gossip().publish(STREAM, "claim:" + holder.id().peerId(),
                    codec.toBytes(new SpaceWire.Delta(null, entryId, claim)));
        }
        tickAll(2);
        assertThat(a.space().stampsLogicalRefused()).as("the holder is signed content").isEqualTo(5);
    }

    /**
     * HLC round four, 2026-11-08 (decision D3): a space delta whose stamp is any CBOR item other
     * than a definite-length, untagged, shortest-form text string does not decode. Jackson once
     * decoded each of these to the canonical stamp, and the record's signature then verified
     * over the re-encoding. Each is now counted undecodable, never verified, and strikes nobody.
     */
    @Test
    void aDeltaWhoseStampIsNoCanonicalCborTextIsCountedUndecodable() throws Exception {
        Peer a = newPeer("a", 1);
        Attacker evil = attackerWithKey("evil", 3, "a", new AtomicReference<>());
        tickAll(4);
        SpaceWire.EntryStateDto write = ForgeSupport.authoredWrite(evil.id(), "evil", a.space().id(),
                new TaskEntry("relabelled", 1), new HlcTimestamp(clock.millis(), 0, "evil"),
                clock.millis() + Duration.ofMinutes(30).toMillis());
        byte[] honest = codec.toBytes(new SpaceWire.Delta(write, null, null));
        byte[] item = textString(write.record().issued().encoded());
        String text = write.record().issued().encoded();
        byte[] ascii = text.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        byte[] head = text.substring(0, text.length() - 1).getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        int last = text.charAt(text.length() - 1);
        Map<String, byte[]> hostile = new java.util.LinkedHashMap<>();
        hostile.put("overlong UTF-8", concat(new byte[]{(byte) (0x60 | (head.length + 2))}, head,
                new byte[]{(byte) (0xC0 | (last >> 6)), (byte) (0x80 | (last & 0x3F))}));
        hostile.put("indefinite-length text", concat(new byte[]{0x7F}, item, new byte[]{(byte) 0xFF}));
        hostile.put("tag 0", concat(new byte[]{(byte) 0xC0}, item));
        hostile.put("tag 32", concat(new byte[]{(byte) 0xD8, 0x20}, item));
        hostile.put("a byte string", concat(new byte[]{(byte) (0x40 | ascii.length)}, ascii));
        long undecodable = a.space().deltasUndecodable();
        for (Map.Entry<String, byte[]> variant : hostile.entrySet()) {
            evil.runtime().gossip().publish(STREAM, "relabelled:" + variant.getKey(),
                    replaceFirst(honest, item, variant.getValue()));
            tickAll(2);
            assertThat(a.space().deltasUndecodable()).as(variant.getKey()).isEqualTo(++undecodable);
            assertThat(a.space().knownEntries()).as(variant.getKey()).isZero();
        }
        assertThat(strikes(a.node(), evil.id().peerId())).isZero();
        evil.runtime().gossip().publish(STREAM, "canonical", honest);
        tickAll(2);
        assertThat(a.space().read(Template.of(TaskEntry.class))).as("the canonical delta merges")
                .contains(new TaskEntry("relabelled", 1));
    }

    /**
     * HLC round five, 2026-11-08 (decision E8): outside its stamps a space delta is read as every
     * implementation reads CBOR. Jackson decoded an overlong letter in a record's type and dropped
     * a tag over it, and the record's signature then verified over the re-encoding, so a relay's
     * relabelled delta merged here while the other implementations refused it. Each is now counted
     * undecodable, never verified, and strikes nobody.
     */
    @Test
    void aDeltaWithATagOrOverlongUtf8OutsideItsStampsIsCountedUndecodable() throws Exception {
        Peer a = newPeer("a", 1);
        Attacker evil = attackerWithKey("evil", 3, "a", new AtomicReference<>());
        tickAll(4);
        SpaceWire.EntryStateDto write = ForgeSupport.authoredWrite(evil.id(), "evil", a.space().id(),
                new TaskEntry("relabelled type", 1), new HlcTimestamp(clock.millis(), 0, "evil"),
                clock.millis() + Duration.ofMinutes(30).toMillis());
        byte[] honest = codec.toBytes(new SpaceWire.Delta(write, null, null));
        String type = write.record().type();
        byte[] item = textString(type);
        int last = type.charAt(type.length() - 1);
        // The type's final ASCII letter as the overlong two-byte form, under a header one byte longer.
        byte[] overlong = replaceFirst(textString(type + "?"),
                ("" + (char) last + "?").getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                new byte[]{(byte) (0xC0 | (last >> 6)), (byte) (0x80 | (last & 0x3F))});
        Map<String, byte[]> hostile = new java.util.LinkedHashMap<>();
        hostile.put("overlong UTF-8 in the type", overlong);
        hostile.put("tag 0 over the type", concat(new byte[]{(byte) 0xC0}, item));
        long undecodable = a.space().deltasUndecodable();
        for (Map.Entry<String, byte[]> variant : hostile.entrySet()) {
            evil.runtime().gossip().publish(STREAM, "relabelled:" + variant.getKey(),
                    replaceFirst(honest, item, variant.getValue()));
            tickAll(2);
            assertThat(a.space().deltasUndecodable()).as(variant.getKey()).isEqualTo(++undecodable);
            assertThat(a.space().knownEntries()).as(variant.getKey()).isZero();
        }
        assertThat(strikes(a.node(), evil.id().peerId())).isZero();
        evil.runtime().gossip().publish(STREAM, "canonical", honest);
        tickAll(2);
        assertThat(a.space().read(Template.of(TaskEntry.class))).as("the canonical delta merges")
                .contains(new TaskEntry("relabelled type", 1));
    }

    private static byte[] concat(byte[]... parts) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.writeBytes(part);
        }
        return out.toByteArray();
    }

    /** Replaces the first occurrence of {@code from}, which must be present. */
    private static byte[] replaceFirst(byte[] bytes, byte[] from, byte[] to) {
        for (int i = 0; i + from.length <= bytes.length; i++) {
            if (java.util.Arrays.equals(bytes, i, i + from.length, from, 0, from.length)) {
                return concat(java.util.Arrays.copyOfRange(bytes, 0, i), to,
                        java.util.Arrays.copyOfRange(bytes, i + from.length, bytes.length));
            }
        }
        throw new AssertionError("the stamp item was not found");
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
