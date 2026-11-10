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

import ai.badmonkey.agentspaces.api.ad.CredentialRevocation;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.AgentIdentity;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.crypto.JdkSignatureProvider;
import ai.badmonkey.agentspaces.common.crypto.SignatureProvider;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Swap amplification (ROBUSTNESS.md §10.8): a replica receives one signed state or record many times, by gossip, by
 * anti-entropy, in a graft answer, and the record again with every later state of its entry. Each repeat used to cost
 * a full Ed25519 verification, about one in five of a replica's entry verifications in the 13-peer swap workload. A
 * replica now verifies each exact signature once, and a remembered signature over other bytes is refused.
 */
class RepeatVerificationTest {

    private static final Lease MINUTES_30 = Lease.of(Duration.ofMinutes(30));
    private static final String STREAM = "space:tasks";

    /** Counts, per signature, the verifications that reach the provider. */
    private static final class CountingProvider implements SignatureProvider {
        private final JdkSignatureProvider delegate = new JdkSignatureProvider();
        final Map<String, AtomicInteger> bySignature = new ConcurrentHashMap<>();

        int verifications(byte[] signature) {
            AtomicInteger n = bySignature.get(HexFormat.of().formatHex(signature));
            return n == null ? 0 : n.get();
        }

        @Override
        public String name() {
            return "counting";
        }

        @Override
        public KeyPair generate() {
            return delegate.generate();
        }

        @Override
        public byte[] sign(PrivateKey key, byte[] bytes) {
            return delegate.sign(key, bytes);
        }

        @Override
        public boolean verify(PublicKey key, byte[] bytes, byte[] sig) {
            bySignature.computeIfAbsent(HexFormat.of().formatHex(sig), s -> new AtomicInteger()).incrementAndGet();
            return delegate.verify(key, bytes, sig);
        }

        @Override
        public boolean verifyRaw(byte[] rawPublicKey, byte[] bytes, byte[] sig) {
            bySignature.computeIfAbsent(HexFormat.of().formatHex(sig), s -> new AtomicInteger()).incrementAndGet();
            return delegate.verifyRaw(rawPublicKey, bytes, sig);
        }
    }

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zRepeatVerification");
    private final PeerIdentity founderId = PeerIdentity.generate();
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zRepeatVerification", founderId.peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "fleet",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final CountingProvider counting = new CountingProvider();

    private record Peer(PeerIdentity identity, PeerNode node, GroupRuntime runtime, ReplicatedSpace space) {
    }

    @AfterEach
    void tearDown() {
        Ed25519.select("jdk");
        nodes.forEach(PeerNode::close);
    }

    private PeerNode node(PeerIdentity identity, String address, long seed) throws IOException {
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        nodes.add(node);
        return node;
    }

    private Peer newPeer(String address, long seed, String... seedAddresses) throws IOException {
        return newPeer(PeerIdentity.generate(), address, seed, seedAddresses);
    }

    private Peer newPeer(PeerIdentity identity, String address, long seed, String... seedAddresses) throws IOException {
        PeerNode node = node(identity, address, seed);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "tasks", identity, address)
                .clock(clock).settleWindow(Duration.ZERO).build();
        return new Peer(identity, node, runtime, space);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofMillis(250));
        }
    }

    private boolean runUntil(int maxRounds, BooleanSupplier until) {
        for (int i = 0; i < maxRounds && !until.getAsBoolean(); i++) {
            tickAll(1);
        }
        return until.getAsBoolean();
    }

    private boolean isRumor(byte[] frame) {
        try {
            return codec.fromWire(frame, WireCodec.Signed.class).envelope().kind() == Envelope.Kind.RUMOR;
        } catch (RuntimeException e) {
            return false;
        }
    }

    private static long leaseEnd(ReplicatedSpace space, EntryId id) {
        return space.signedState(id).map(dto -> dto.leaseValue().expiresAtMillis()).orElse(-1L);
    }

    @Test
    void aStateThatCameByAntiEntropyVerifiesOnceWhenItsGossipCopyArrives() throws Exception {
        Peer a = newPeer("a", 1);
        Peer c = newPeer("c", 3, "a");
        Peer b = newPeer("b", 2, "a", "c");
        assertThat(runUntil(40, () -> b.runtime().membership().allMembers().size() >= 2
                && c.runtime().membership().allMembers().size() >= 2)).isTrue();

        // a's copy to b waits; c's gossip to b is lost, so b gets the write from c by anti-entropy first
        List<byte[]> rumorsToB = new java.util.concurrent.CopyOnWriteArrayList<>();
        network.tap("a", "b", frame -> {
            if (isRumor(frame)) {
                rumorsToB.add(codec.fromWire(frame, WireCodec.Signed.class).signature());
            }
        });
        network.hold("a", "b");
        network.corrupt("c", "b", frame -> isRumor(frame) ? new byte[]{0} : frame);
        EntryId id = a.space().write(new TaskEntry("by anti-entropy first", 1), MINUTES_30).entryId();
        assertThat(runUntil(80, () -> b.space().signedState(id).isPresent())).as("b holds the write by anti-entropy").isTrue();
        SpaceWire.EntryStateDto state = a.space().signedState(id).orElseThrow();

        assertThat(b.runtime().membership().member(a.identity().peerId())).as("b still counts a a member").isPresent();
        Ed25519.use(counting);
        network.release("a", "b", SimNetwork.Order.FIFO, 0);
        tickAll(4);

        assertThat(rumorsToB).as("a's gossip copy waited for b").isNotEmpty();
        assertThat(rumorsToB.stream().mapToInt(counting::verifications).sum())
                .as("b received a's gossip copy after it held the state").isPositive();
        assertThat(counting.verifications(state.record().sig()))
                .as("the gossip copy's record was verified by anti-entropy already").isZero();
        assertThat(counting.verifications(state.stateSig()))
                .as("the gossip copy's state was verified by anti-entropy already").isZero();
        assertThat(b.space().read(Template.of(TaskEntry.class))).isPresent();
    }

    @Test
    void aRenewalVerifiesItsNewStateButNotTheRecordTheReplicaHolds() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        assertThat(runUntil(40, () -> b.runtime().membership().allMembers().size() >= 1)).isTrue();
        EntryHandle handle = a.space().write(new TaskEntry("renewed", 2), MINUTES_30);
        EntryId id = handle.entryId();
        assertThat(runUntil(40, () -> b.space().signedState(id).isPresent())).isTrue();
        byte[] recordSig = a.space().signedState(id).orElseThrow().record().sig();

        Ed25519.use(counting);
        handle.renew(Duration.ofMinutes(45));
        long renewed = leaseEnd(a.space(), id);
        assertThat(runUntil(40, () -> leaseEnd(b.space(), id) == renewed)).as("b holds the renewal").isTrue();

        assertThat(counting.verifications(a.space().signedState(id).orElseThrow().stateSig()))
                .as("the renewed state is new and verified").isEqualTo(1);
        assertThat(counting.verifications(recordSig))
                .as("the record b holds verified when it arrived and is not verified again").isZero();
    }

    @Test
    void aRememberedSignatureOverOtherBytesIsRefused() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        AtomicReference<SpaceWire.EntryStateDto> captured = new AtomicReference<>();
        PeerNode evilNode = node(PeerIdentity.generate(), "evil", 3);
        GroupRuntime evil = evilNode.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        evil.gossip().onStream(STREAM, (from, itemId, payload) -> {
            SpaceWire.Delta delta = codec.fromBytes(payload, SpaceWire.Delta.class);
            if (delta != null && delta.state() != null && captured.get() == null) {
                captured.set(delta.state());
            }
        });
        assertThat(runUntil(40, () -> b.runtime().membership().allMembers().size() >= 2)).isTrue();
        EntryId id = a.space().write(new TaskEntry("honest", 3), MINUTES_30).entryId();
        assertThat(runUntil(40, () -> b.space().signedState(id).isPresent() && captured.get() != null)).isTrue();
        SpaceWire.EntryStateDto real = captured.get();
        long honestEnd = leaseEnd(b.space(), id);

        // the state b verified, its lease moved, under the signature b remembers
        LeaseInfo shorter = new LeaseInfo(real.leaseValue().holder(), real.leaseValue().expiresAtMillis() - 600_000,
                real.leaseValue().kind());
        SpaceWire.EntryStateDto movedLease = new SpaceWire.EntryStateDto(real.record(), real.issuerPublicKey(),
                real.adds(), real.removes(), new ai.badmonkey.agentspaces.common.hlc.HlcTimestamp(
                        real.leaseStamp().physical() + 1, 0, real.leaseStamp().node()),
                shorter, false, real.stateSig());
        // the record b verified, its payload swapped, under the record signature b remembers
        EntryRecord r = real.record();
        EntryRecord swapped = new EntryRecord(r.entryId(), r.spaceId(), r.type(), codec.toBytes(new TaskEntry("forged", 9)),
                r.payloadRef(), r.issuer(), r.issued(), r.lease(), r.tags(), r.sig(), r.keyEpoch());
        SpaceWire.EntryStateDto swappedRecord = new SpaceWire.EntryStateDto(swapped, real.issuerPublicKey(),
                real.adds(), real.removes(), real.leaseStamp(), real.leaseValue(), false, real.stateSig());
        evil.gossip().publish(STREAM, "moved-lease", codec.toBytes(new SpaceWire.Delta(movedLease, null, null)));
        evil.gossip().publish(STREAM, "swapped-record", codec.toBytes(new SpaceWire.Delta(swappedRecord, null, null)));
        tickAll(8);

        assertThat(leaseEnd(b.space(), id)).as("the moved lease never merged").isEqualTo(honestEnd);
        assertThat(b.space().read(Template.of(TaskEntry.class))).get().extracting(TaskEntry::topic).isEqualTo("honest");
    }

    @Test
    void aRevokedAgentsRenewalIsRefusedThoughItsRecordSignatureIsRemembered() throws Exception {
        Peer a = newPeer(founderId, "a", 1);
        Peer b = newPeer("b", 2, "a");
        assertThat(runUntil(40, () -> b.runtime().membership().allMembers().size() >= 1)).isTrue();
        AgentIdentity planner = b.identity().renewingSubordinate("planner", Duration.ofDays(1), clock);
        EntryHandle handle = b.space().as(planner).write(new TaskEntry("before the retirement", 4), MINUTES_30);
        EntryId id = handle.entryId();
        assertThat(runUntil(40, () -> a.space().signedState(id).isPresent())).as("a verified the record").isTrue();
        byte[] recordSig = a.space().signedState(id).orElseThrow().record().sig();
        long heldEnd = leaseEnd(a.space(), id);

        // a, the group's founder, retires the agent; b does not hear of it, so b still renews in the agent's name
        java.time.Instant retiredFrom = clock.instant();
        tickAll(4);
        network.hold("a", "b");
        assertThat(a.runtime().revoke(CredentialRevocation.Target.agent(planner.id()),
                CredentialRevocation.RETIRED, retiredFrom)).isPresent();
        Ed25519.use(counting);
        handle.renew(Duration.ofMinutes(45));
        byte[] renewalSig = b.space().signedState(id).orElseThrow().stateSig();
        assertThat(runUntil(40, () -> counting.verifications(renewalSig) > 0)).as("a received the renewal").isTrue();
        tickAll(4);

        assertThat(counting.verifications(recordSig)).as("the record signature came from memory").isZero();
        assertThat(leaseEnd(a.space(), id)).as("the retired agent's renewal never merged").isEqualTo(heldEnd);
        assertThat(new String(a.space().digest(), java.nio.charset.StandardCharsets.UTF_8))
                .as("refused for the revocation, after its signatures verified").contains("a:e:" + id.value());
    }
}
