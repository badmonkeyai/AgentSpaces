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
