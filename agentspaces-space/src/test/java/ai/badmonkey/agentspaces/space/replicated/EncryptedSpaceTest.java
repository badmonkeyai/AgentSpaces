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
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.GroupKey;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.hlc.HybridLogicalClock;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Group payload encryption (spec §11): replicas holding the group content key
 * exchange readable entries; a group member without the key still verifies,
 * stores, and relays the signed records but cannot read their payloads.
 */
class EncryptedSpaceTest {

    private static final Lease MINUTES_30 = Lease.of(Duration.ofMinutes(30));

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zSealedFleet");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zSealedFleet", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "sealed-fleet",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(PeerNode node, ReplicatedSpace space) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Peer newPeer(String address, long seed, GroupKey key,
                         String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        ReplicatedSpace.Builder builder =
                ReplicatedSpace.builder(runtime, "tasks", identity, "worker")
                        .clock(clock)
                        .settleWindow(Duration.ZERO);
        if (key != null) {
            builder.groupKey(key);
        }
        ReplicatedSpace space = builder.build();
        nodes.add(node);
        return new Peer(node, space);
    }

    private Peer ringPeer(String address, long seed,
                          ai.badmonkey.agentspaces.common.crypto.GroupKeyRing ring,
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
                .clock(clock).settleWindow(Duration.ZERO).keyRing(ring).build();
        nodes.add(node);
        return new Peer(node, space);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // anti-entropy paces itself by the gossip period
        }
    }

    @Test
    void keyedReplicasExchangeReadableEntries() throws Exception {
        GroupKey key = GroupKey.generate();
        Peer a = newPeer("a", 1, key);
        Peer b = newPeer("b", 2, key, "a");
        tickAll(4);

        a.space().write(new TaskEntry("sealed task", 7), MINUTES_30);

        assertThat(b.space().read(Template.of(TaskEntry.class)))
                .contains(new TaskEntry("sealed task", 7));
        var taken = b.space().take(Template.of(TaskEntry.class),
                Lease.of(Duration.ofMinutes(5)), Duration.ofMillis(200));
        assertThat(taken).isPresent();
    }

    @Test
    void aKeylessMemberStoresButCannotReadTheEntries() throws Exception {
        GroupKey key = GroupKey.generate();
        Peer a = newPeer("a", 1, key);
        Peer keyless = newPeer("b", 2, null, "a");
        tickAll(4);

        a.space().write(new TaskEntry("for keyholders only", 7), MINUTES_30);

        // The record replicated (the signature verifies without the key)...
        assertThat(keyless.space().knownEntries()).isEqualTo(1);
        // ...but the payload is ciphertext to this replica.
        assertThat(keyless.space().read(Template.of(TaskEntry.class))).isEmpty();
        assertThat(keyless.space().readAll(Template.of(TaskEntry.class), 10)).isEmpty();
    }

    @Test
    void theWrongKeyReadsNothing() throws Exception {
        Peer a = newPeer("a", 1, GroupKey.generate());
        Peer wrong = newPeer("b", 2, GroupKey.generate(), "a");
        tickAll(4);

        a.space().write(new TaskEntry("sealed", 1), MINUTES_30);

        assertThat(wrong.space().knownEntries()).isEqualTo(1);
        assertThat(wrong.space().read(Template.of(TaskEntry.class))).isEmpty();
    }

    @Test
    void aKeylessRelayStillCarriesStateBetweenKeyedReplicas() throws Exception {
        GroupKey key = GroupKey.generate();
        Peer a = newPeer("a", 1, key);
        Peer keyless = newPeer("b", 2, null, "a");
        Peer c = newPeer("c", 3, key, "b");
        tickAll(4);

        // A can only reach C's replica state through the keyless middle peer's
        // gossip; the ciphertext survives the hop and C reads it with the key.
        a.space().write(new TaskEntry("through the middle", 3), MINUTES_30);
        tickAll(4);

        assertThat(c.space().read(Template.of(TaskEntry.class)))
                .contains(new TaskEntry("through the middle", 3));
    }

    /** Spec §11a.1: the AAD binds a sealed payload to spaceId|entryId, so a ciphertext replayed under another entry identity is stored but unreadable. */
    @Test
    void aSealedPayloadTransplantedOntoAnotherEntryIsUnreadable() throws Exception {
        GroupKey key = GroupKey.generate();
        Peer a = newPeer("a", 1, key);
        Peer b = newPeer("b", 2, key, "a");
        CborCodec codec = CborCodec.defaultCodec();

        // A keyless member taps the stream and captures A's sealed record.
        PeerIdentity evilId = PeerIdentity.generate();
        PeerNode evilNode = PeerNode.builder(evilId).clock(clock).randomSeed(3).build();
        evilNode.listen(network.register("evil"), "evil");
        GroupRuntime evil = evilNode.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        AtomicReference<SpaceWire.EntryStateDto> captured = new AtomicReference<>();
        evil.gossip().onStream("space:tasks", (from, itemId, payload) -> {
            SpaceWire.Delta delta = codec.fromBytes(payload, SpaceWire.Delta.class);
            if (delta != null && delta.state() != null && captured.get() == null) {
                captured.set(delta.state());
            }
        });
        nodes.add(evilNode);
        tickAll(4);

        a.space().write(new TaskEntry("secret", 7), MINUTES_30);
        tickAll(4);
        SpaceWire.EntryStateDto real = captured.get();
        assertThat(real).isNotNull();

        // Replay the same ciphertext as a fresh, validly signed entry of the attacker's own.
        AgentId evilAgent = evilId.agent("evil");
        HlcTimestamp issued = new HybridLogicalClock(clock, "evil").now();
        LeaseInfo lease = new LeaseInfo(evilAgent,
                clock.instant().toEpochMilli() + Duration.ofMinutes(30).toMillis(), LeaseKind.WRITE);
        EntryRecord transplanted = ForgeSupport.signRecord(evilId, new EntryRecord(EntryId.newId(),
                real.record().spaceId(), real.record().type(), real.record().payload(), null,
                evilAgent, issued, lease, Map.of(), null));
        SpaceWire.EntryStateDto dto = ForgeSupport.signState(evilId, new SpaceWire.EntryStateDto(
                transplanted, evilId.rawPublicKey(), List.of(new Dot(evilId.peerId().value(), 1)),
                List.of(), issued, lease, false, null));
        evil.gossip().publish("space:tasks", "transplant:" + transplanted.entryId(),
                codec.toBytes(new SpaceWire.Delta(dto, null, null)));
        tickAll(4);

        // Signatures verify, so keyed replicas store and forward it; the AAD mismatch
        // makes it undecryptable, so it never matches a template.
        assertThat(b.space().knownEntries()).isEqualTo(2);
        assertThat(b.space().readAll(Template.of(TaskEntry.class), 10))
                .containsExactly(new TaskEntry("secret", 7));
        assertThat(a.space().readAll(Template.of(TaskEntry.class), 10))
                .containsExactly(new TaskEntry("secret", 7));
    }

    /** SPEC §11a.3 v0.1.13 (TODO-9-10-11 D1/D2): after a rotation, old entries stay readable, new writes name epoch 1 and are sealed under it, and a member that never received epoch 1 reads the old entries, not the new, and asks for the missing epoch. */
    @Test
    void aRotationKeepsOldEntriesReadableAndSealsNewWritesUnderTheNewEpoch() throws Exception {
        GroupKey epochZero = GroupKey.generate();
        GroupKey epochOne = GroupKey.generate();
        var ringA = ai.badmonkey.agentspaces.common.crypto.GroupKeyRing.of(epochZero);
        var ringB = ai.badmonkey.agentspaces.common.crypto.GroupKeyRing.of(epochZero);
        var ringC = ai.badmonkey.agentspaces.common.crypto.GroupKeyRing.of(epochZero);
        List<Long> missingAtC = new java.util.concurrent.CopyOnWriteArrayList<>();
        ringC.onMissingEpoch(missingAtC::add);
        Peer a = ringPeer("a", 1, ringA);
        Peer b = ringPeer("b", 2, ringB, "a");
        Peer c = ringPeer("c", 3, ringC, "a");
        tickAll(4);

        var before = a.space().write(new TaskEntry("under epoch 0", 1), MINUTES_30);
        ringA.install(1, "rotator", epochOne, clock.instant());
        ringB.install(1, "rotator", epochOne, clock.instant());
        var after = a.space().write(new TaskEntry("under epoch 1", 2), MINUTES_30);
        tickAll(6);

        assertThat(a.space().signedState(before.entryId()).orElseThrow().record().keyEpoch())
                .as("epoch 0 records name no epoch").isNull();
        assertThat(a.space().signedState(after.entryId()).orElseThrow().record().keyEpoch())
                .isEqualTo(1L);
        assertThat(b.space().readAll(Template.of(TaskEntry.class), 10))
                .containsExactlyInAnyOrder(new TaskEntry("under epoch 0", 1), new TaskEntry("under epoch 1", 2));
        assertThat(c.space().knownEntries()).as("c stores and forwards both").isEqualTo(2);
        assertThat(c.space().readAll(Template.of(TaskEntry.class), 10))
                .as("c reads only what its ring opens").containsExactly(new TaskEntry("under epoch 0", 1));
        assertThat(missingAtC).as("c asked for the epoch it lacks").contains(1L);

        ringC.install(1, "rotator", epochOne, clock.instant());
        assertThat(c.space().readAll(Template.of(TaskEntry.class), 10)).hasSize(2);
    }

    /** SPEC §11a.3 v0.1.13 (review M-9): a writer that learned of epoch 1's cutover but never received its key stops sealing under epoch 0 once the writer grace has passed. */
    @Test
    void aLaggingWriterStopsSealingUnderARetiredEpoch() throws Exception {
        var ring = ai.badmonkey.agentspaces.common.crypto.GroupKeyRing.of(GroupKey.generate());
        ring.writerGrace(Duration.ofMinutes(5));
        Peer a = ringPeer("a", 1, ring);
        ring.announce(1, clock.instant());
        a.space().write(new TaskEntry("within grace", 1), MINUTES_30);
        clock.advance(Duration.ofMinutes(6));
        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                        a.space().write(new TaskEntry("after grace", 2), MINUTES_30))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("awaiting epoch 1");
    }
}
