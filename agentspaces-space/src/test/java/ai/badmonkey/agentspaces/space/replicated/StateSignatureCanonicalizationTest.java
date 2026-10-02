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
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.SpaceId;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The state-signature mechanics of SPEC §11a.4 that {@code ForgedStateRejectionTest}
 * does not exercise: the signature is over the <em>canonical</em> view (dots
 * sorted by replica then counter, whatever order they travel in), a state with
 * no signature at all is dropped, and anti-entropy forwards the author's stored
 * signature verbatim rather than re-signing. A scripted member with a real
 * identity but no space of its own speaks the space's wire shapes directly so
 * every byte of the delta is under test control.
 */
class StateSignatureCanonicalizationTest {

    private static final String STREAM = "space:tasks";
    private static final String TYPE = "Fixtures$TaskEntry#v1";

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zCanonical");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zCanonical", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "canonical-fleet",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Peer(PeerNode node, GroupRuntime runtime, ReplicatedSpace space) {
    }

    /** A member that runs no space but signs and publishes hand-built deltas. */
    private record Scripted(PeerIdentity identity, PeerNode node, GroupRuntime runtime) {
    }

    /** Mirrors ReplicatedSpace's private SignView (field names and order). */
    record SignView(EntryId entryId, SpaceId spaceId, String type, byte[] payload,
                    String payloadRef, AgentId issuer, HlcTimestamp issued,
                    Map<String, String> tags,
                    // v0.1.13, omitted when null
                    @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                    Long keyEpoch) {
    }

    /** Mirrors ReplicatedSpace's private StateSignView (field names and order). */
    record StateSignView(SpaceId spaceId, EntryId entryId, List<Dot> adds,
                         List<Dot> removes, HlcTimestamp leaseStamp, LeaseInfo leaseValue,
                         boolean completed,
                         // v0.1.13, omitted when null
                         @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                         String signer,
                         @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                         HlcTimestamp signedAt) {
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

    private Scripted scripted(String address, long seed, String seedAddress) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", seedAddress, 0)));
        nodes.add(node);
        return new Scripted(identity, node, runtime);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // anti-entropy paces itself by the gossip period
        }
    }

    private static List<Dot> sorted(List<Dot> dots) {
        List<Dot> copy = new ArrayList<>(dots);
        copy.sort(Comparator.comparing(Dot::replica).thenComparingLong(Dot::counter));
        return copy;
    }

    private byte[] stateSignBytes(SpaceWire.EntryStateDto dto, List<Dot> addsInView) {
        return codec.toBytes(new StateSignView(dto.record().spaceId(), dto.record().entryId(),
                addsInView, sorted(dto.removes()), dto.leaseStamp(), dto.leaseValue(),
                dto.completed(), dto.signer(), dto.signedAt()));
    }

    /**
     * A fully self-consistent initial-write state from the scripted writer's
     * own key: the record signature is genuine, the dots are bound to the
     * writer, and {@code adds} travels in exactly the order given. The state
     * signature covers the sorted view when {@code signCanonical} is true and
     * the wire order otherwise.
     */
    private SpaceWire.EntryStateDto writerState(Scripted writer, SpaceId spaceId, EntryId entryId,
                                                List<Dot> adds, boolean signCanonical) {
        PeerIdentity id = writer.identity();
        AgentId issuer = id.agent("writer");
        HlcTimestamp issued = new HlcTimestamp(clock.millis(), 1, id.peerId().value());
        byte[] payload = codec.toBytes(new TaskEntry("hand-built", 1));
        LeaseInfo lease = new LeaseInfo(issuer, clock.millis() + Duration.ofMinutes(30).toMillis(),
                LeaseKind.WRITE);
        byte[] recordSig = id.sign(codec.toBytes(new SignView(entryId, spaceId, TYPE, payload,
                null, issuer, issued, Map.of(), null)));
        EntryRecord record = new EntryRecord(entryId, spaceId, TYPE, payload, null, issuer,
                issued, lease, Map.of(), recordSig);
        SpaceWire.EntryStateDto unsigned = new SpaceWire.EntryStateDto(record, id.rawPublicKey(),
                adds, List.of(), issued, lease, false, null);
        return unsigned.withStateSig(id.sign(
                stateSignBytes(unsigned, signCanonical ? sorted(adds) : adds)));
    }

    private void publish(Scripted writer, String itemId, SpaceWire.EntryStateDto dto) {
        writer.runtime().gossip().publish(STREAM, itemId,
                codec.toBytes(new SpaceWire.Delta(dto, null, null)));
    }

    /** SPEC §11a.4: the signature is over sorted adds/removes, so dots may travel in any order and still verify, while a signature over the unsorted wire order is not the canonical one. */
    @Test
    void unsortedDotsStillVerifyAgainstTheCanonicalSortedView() throws Exception {
        Peer honest = newPeer("h", 1);
        Scripted writer = scripted("w", 2, "h");
        tickAll(4);
        SpaceId spaceId = honest.space().id();
        String replica = writer.identity().peerId().value();
        List<Dot> unsorted = List.of(new Dot(replica, 3), new Dot(replica, 1), new Dot(replica, 2));
        assertThat(unsorted).isNotEqualTo(sorted(unsorted));

        // Negative control first: signing the wire order instead of the sorted
        // view produces a signature the receiver's canonical view rejects.
        publish(writer, "wire-order", writerState(writer, spaceId,
                EntryId.of("aaaaaaaa-0000-0000-0000-000000000001"), unsorted, false));
        tickAll(2);
        assertThat(honest.space().knownEntries())
                .as("a signature over the unsorted order is not the canonical signature")
                .isZero();

        // The canonical signature verifies whatever order the dots arrive in.
        publish(writer, "canonical", writerState(writer, spaceId,
                EntryId.of("aaaaaaaa-0000-0000-0000-000000000002"), unsorted, true));
        tickAll(2);
        assertThat(honest.space().knownEntries())
                .as("dots in wire order 3,1,2 verify against the sorted view 1,2,3")
                .isEqualTo(1);
    }

    /** SPEC §11a.4: a delta whose mutable state carries no signature at all is dropped before merging. */
    @Test
    void aStateWithNoStateSigIsRejected() throws Exception {
        Peer honest = newPeer("h", 1);
        Scripted writer = scripted("w", 2, "h");
        tickAll(4);
        SpaceId spaceId = honest.space().id();
        EntryId entryId = EntryId.of("bbbbbbbb-0000-0000-0000-000000000001");
        SpaceWire.EntryStateDto signed = writerState(writer, spaceId, entryId,
                List.of(new Dot(writer.identity().peerId().value(), 1)), true);

        // Same genuine record signature, same issuer key, stateSig absent.
        publish(writer, "unsigned-state", signed.withStateSig(null));
        tickAll(2);
        assertThat(honest.space().knownEntries()).isZero();

        // Positive control: the very same state with its signature merges.
        publish(writer, "signed-state", signed);
        tickAll(2);
        assertThat(honest.space().knownEntries()).isEqualTo(1);
    }

    /** SPEC §11a.4: anti-entropy forwards the author's stored state signature verbatim; a non-author replica never re-signs. */
    @Test
    void antiEntropyForwardsTheStoredActorSignatureVerbatim() throws Exception {
        Peer author = newPeer("a", 1);
        Peer relay = newPeer("b", 2, "a");
        // An observer with no space: it captures the author's original signed
        // state off the rumor channel, and later offers an empty digest so a
        // replica must answer with everything it stores.
        AtomicReference<SpaceWire.EntryStateDto> original = new AtomicReference<>();
        List<SpaceWire.SyncDelta> synced = new CopyOnWriteArrayList<>();
        Scripted observer = scripted("o", 3, "a");
        observer.runtime().gossip().onStream(STREAM, (from, itemId, payload) -> {
            SpaceWire.Delta delta = codec.fromBytes(payload, SpaceWire.Delta.class);
            if (delta != null && delta.state() != null) {
                original.compareAndSet(null, delta.state());
            }
        });
        tickAll(4);

        author.space().write(new TaskEntry("forwarded verbatim", 1), Lease.of(Duration.ofMinutes(30)));
        tickAll(4);
        assertThat(original.get()).as("the observer saw the author's signed write").isNotNull();
        assertThat(relay.space().knownEntries()).isEqualTo(1);

        // Cut the observer off from the author: from now on only the relay, a
        // non-author, can answer its anti-entropy digests.
        network.partition("o", "a");
        observer.runtime().gossip().reconcile(STREAM, new ReconcilableState() {
            @Override
            public byte[] digest() {
                return new byte[0]; // we hold nothing: send us everything
            }

            @Override
            public byte[] deltaFor(byte[] remoteDigest) {
                return new byte[0];
            }

            @Override
            public void applyDelta(byte[] delta) {
                synced.add(codec.fromBytes(delta, SpaceWire.SyncDelta.class));
            }
        });
        for (int i = 0; i < 12 && synced.isEmpty(); i++) {
            tickAll(1);
        }
        assertThat(synced).as("the relay answered the observer's digest").isNotEmpty();

        SpaceWire.EntryStateDto forwarded = synced.get(0).states().stream()
                .filter(s -> s.record().entryId().equals(original.get().record().entryId()))
                .findFirst().orElseThrow();
        assertThat(forwarded.stateSig()).isEqualTo(original.get().stateSig());
        assertThat(forwarded.record().sig()).isEqualTo(original.get().record().sig());
        assertThat(forwarded.issuerPublicKey()).isEqualTo(original.get().issuerPublicKey());
        assertThat(forwarded.issuerPublicKey()).isEqualTo(author.node().identity().rawPublicKey());

        // The forwarded signature is the author's over the canonical view, and
        // the relay's key does not verify it: nobody re-signed in transit.
        byte[] canonical = stateSignBytes(forwarded, sorted(forwarded.adds()));
        assertThat(Ed25519.verify(Ed25519.publicKeyFromRaw(forwarded.issuerPublicKey()),
                canonical, forwarded.stateSig())).isTrue();
        assertThat(Ed25519.verifyRaw(relay.node().identity().rawPublicKey(),
                canonical, forwarded.stateSig())).isFalse();
    }
}
