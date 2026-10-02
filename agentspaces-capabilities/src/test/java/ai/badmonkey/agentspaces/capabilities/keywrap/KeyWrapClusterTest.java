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
package ai.badmonkey.agentspaces.capabilities.keywrap;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.GroupKey;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
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
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Sealed group-key distribution on a SimNetwork: an authorized member obtains
 * the key and reads the encrypted space; an unauthorized peer is refused and
 * stays locked out even though it replicates the ciphertext.
 */
class KeyWrapClusterTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zKeyWrap");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zKeyWrap", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "keywrap",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Wired(PeerNode node, PeerIdentity identity, GroupRuntime runtime,
                         GroupKeyDistributor keys) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Wired newPeer(String address, long seed, String... seedAddresses)
            throws IOException {
        return newPeer(address, seed, Set.of(), false, seedAddresses);
    }

    /**
     * A peer with explicit roles; {@code dialOnly} models the NAT-restricted
     * posture (it can reach others, nobody can dial it) as {@code RelayTest} does.
     */
    private Wired newPeer(String address, long seed, Set<PeerAdvertisement.PeerRole> roles,
                          boolean dialOnly, String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed)
                .roles(roles).build();
        if (dialOnly) {
            node.transport(network.register(address));
        } else {
            node.listen(network.register(address), address);
        }
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        GroupKeyDistributor keys = new GroupKeyDistributor(
                new CapabilityPipes(runtime, codec), identity.peerId(), codec, clock);
        nodes.add(node);
        return new Wired(node, identity, runtime, keys);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // anti-entropy runs once per gossip period on the node clock
        }
    }

    @Test
    void anAuthorizedMemberObtainsTheKeyAndReadsTheSealedSpace() throws Exception {
        Wired holder = newPeer("h", 1);
        Wired member = newPeer("m", 2, "h");
        tickAll(4);

        GroupKey key = GroupKey.generate();
        Set<PeerId> allowed = Set.of(member.identity().peerId());
        holder.keys().serve(key, allowed::contains);

        // The holder writes into the sealed space before the member has the key.
        ReplicatedSpace holderSpace = ReplicatedSpace
                .builder(holder.runtime(), "sealed", holder.identity(), "holder")
                .clock(clock).settleWindow(Duration.ZERO).groupKey(key).build();
        holderSpace.write(new TaskEntry("for members only", 5),
                Lease.of(Duration.ofMinutes(30)));

        Optional<GroupKey> obtained =
                member.keys().request(holder.identity().peerId(), Duration.ofSeconds(2));
        assertThat(obtained).isPresent();
        assertThat(obtained.get().rawBytes()).isEqualTo(key.rawBytes());

        ReplicatedSpace memberSpace = ReplicatedSpace
                .builder(member.runtime(), "sealed", member.identity(), "member")
                .clock(clock).settleWindow(Duration.ZERO).groupKey(obtained.get()).build();
        tickAll(4); // anti-entropy fills the member's replica

        assertThat(memberSpace.read(Template.of(TaskEntry.class)))
                .contains(new TaskEntry("for members only", 5));
    }

    @Test
    void anUnauthorizedPeerIsRefusedAndStaysLockedOut() throws Exception {
        Wired holder = newPeer("h", 1);
        Wired outsider = newPeer("o", 2, "h");
        tickAll(4);

        GroupKey key = GroupKey.generate();
        holder.keys().serve(key, peer -> false);

        ReplicatedSpace holderSpace = ReplicatedSpace
                .builder(holder.runtime(), "sealed", holder.identity(), "holder")
                .clock(clock).settleWindow(Duration.ZERO).groupKey(key).build();
        holderSpace.write(new TaskEntry("secret", 1), Lease.of(Duration.ofMinutes(30)));

        assertThat(outsider.keys().request(holder.identity().peerId(),
                Duration.ofSeconds(2))).isEmpty();

        // The outsider still replicates the signed record; the payload stays sealed.
        ReplicatedSpace outsiderSpace = ReplicatedSpace
                .builder(outsider.runtime(), "sealed", outsider.identity(), "outsider")
                .clock(clock).settleWindow(Duration.ZERO).build();
        tickAll(4);
        assertThat(outsiderSpace.knownEntries()).isEqualTo(1);
        assertThat(outsiderSpace.read(Template.of(TaskEntry.class))).isEmpty();
    }

    @Test
    void aPeerThatIsNotServingRefusesRequests() throws Exception {
        Wired holder = newPeer("h", 1);
        Wired member = newPeer("m", 2, "h");
        tickAll(4);

        // Nobody called serve: the request completes empty rather than hanging.
        assertThat(member.keys().request(holder.identity().peerId(),
                Duration.ofSeconds(2))).isEmpty();
    }

    /** SPEC §11a.2 authorization policy: under the membership default (ASF-026) the key follows group admission — evicted peers are refused, re-admitted peers are served again. */
    @Test
    void membershipPolicySealsOnlyForAdmittedMembers() throws Exception {
        Wired holder = newPeer("h", 1);
        Wired member = newPeer("m", 2, "h");
        tickAll(4);

        GroupKey key = GroupKey.generate();
        holder.keys().serve(key, holder.runtime().membership());

        assertThat(member.keys().request(holder.identity().peerId(), Duration.ofSeconds(2)))
                .as("an admitted member is served").hasValueSatisfying(obtained ->
                        assertThat(obtained.rawBytes()).isEqualTo(key.rawBytes()));

        // The holder ejects the member from its view (a revocation's enforcement).
        // Whether the dispatch gate (ASF-003) or the membership predicate refuses
        // first, the observable property is the same: no admission, no key.
        holder.runtime().membership().evict(member.identity().peerId());
        assertThat(holder.runtime().membership().member(member.identity().peerId())).isEmpty();
        assertThat(member.keys().request(holder.identity().peerId(), Duration.ofMillis(500)))
                .as("an evicted peer is refused").isEmpty();

        // The OPEN group re-admits the member on its next self-introduction, and
        // the same policy, consulted per request, serves it again.
        tickAll(3);
        assertThat(holder.runtime().membership().member(member.identity().peerId())).isPresent();
        assertThat(member.keys().request(holder.identity().peerId(), Duration.ofSeconds(2)))
                .as("re-admitted, served again").isPresent();
    }

    /** TODO-EFG §4 / TODO item 6 (ASF-026, SPEC §11a.2): under {@code serve(key, Authorizer, group)} the key follows the authorizer's live KEY_HOLDER decision — a permitted requester is served, a refused one denied, and a grant that lapses on the clock flips to denied. */
    @Test
    void anAuthorizerGatesTheKey() throws Exception {
        Wired holder = newPeer("h", 1);
        Wired member = newPeer("m", 2, "h");
        Wired outsider = newPeer("o", 3, "h");
        tickAll(4);

        // A stub of the OIDC posture: grants carry an expiry on the TestClock.
        java.util.Map<PeerId, java.time.Instant> grants = new java.util.HashMap<>();
        grants.put(member.identity().peerId(), clock.instant().plus(Duration.ofMinutes(10)));
        Authorizer authorizer = (peer, operation, scope) ->
                operation == Authorizer.Operation.KEY_HOLDER
                        && groupId.value().equals(scope)
                        && grants.containsKey(peer)
                        && clock.instant().isBefore(grants.get(peer));
        GroupKey key = GroupKey.generate();
        holder.keys().serve(key, authorizer, groupId);

        assertThat(member.keys().request(holder.identity().peerId(), Duration.ofSeconds(2)))
                .as("the permitted requester is served").hasValueSatisfying(obtained ->
                        assertThat(obtained.rawBytes()).isEqualTo(key.rawBytes()));
        assertThat(outsider.keys().request(holder.identity().peerId(), Duration.ofSeconds(2)))
                .as("an admitted member the authorizer refuses is denied").isEmpty();

        // The grant lapses: the same requester is now refused, per request.
        clock.advance(Duration.ofMinutes(11));
        assertThat(member.keys().request(holder.identity().peerId(), Duration.ofSeconds(2)))
                .as("a lapsed grant serves nothing").isEmpty();
    }

    /** SPEC §11a.2 authorization policy: the decision is per verified identity — an allowlisted peer is served while a different peer asking the same holder is refused. */
    @Test
    void anAllowlistedIdentityCannotBeBorrowedByAnotherPeer() throws Exception {
        Wired holder = newPeer("h", 1);
        Wired member = newPeer("m", 2, "h");
        Wired outsider = newPeer("o", 3, "h");
        tickAll(4);

        GroupKey key = GroupKey.generate();
        Set<PeerId> allowed = Set.of(member.identity().peerId());
        holder.keys().serve(key, allowed::contains);

        // Both are admitted group members; only the allowlisted identity is served.
        assertThat(holder.runtime().membership().member(outsider.identity().peerId())).isPresent();
        assertThat(outsider.keys().request(holder.identity().peerId(), Duration.ofSeconds(2)))
                .as("a member outside the allowlist is refused").isEmpty();
        assertThat(member.keys().request(holder.identity().peerId(), Duration.ofSeconds(2)))
                .as("the allowlisted member is served").hasValueSatisfying(obtained ->
                        assertThat(obtained.rawBytes()).isEqualTo(key.rawBytes()));
        // The refusal is not sticky: the outsider asking again is still refused,
        // and the member asking again is still served.
        assertThat(outsider.keys().request(holder.identity().peerId(), Duration.ofSeconds(2)))
                .isEmpty();
        assertThat(member.keys().request(holder.identity().peerId(), Duration.ofSeconds(2)))
                .isPresent();
    }

    /** SPEC §11a.2: the sealed reply travels any path the fabric offers — here both request and response cross a RELAY-role member that cannot read them. */
    @Test
    void theSealedReplyTravelsThroughARelay() throws Exception {
        Wired relay = newPeer("relay", 1, Set.of(PeerAdvertisement.PeerRole.RELAY), false);
        // Holder and requester are both dial-only: neither can reach the other
        // except through the relay, so every frame between them is relayed.
        Wired holder = newPeer("h", 2, Set.of(), true, "relay");
        Wired requester = newPeer("r", 3, Set.of(), true, "relay");
        tickAll(4);

        assertThat(holder.runtime().membership().member(requester.identity().peerId()))
                .as("the holder knows the requester only as an undialable member")
                .hasValueSatisfying(m -> assertThat(m.endpoints()).isEmpty());
        assertThat(requester.runtime().membership().member(holder.identity().peerId()))
                .hasValueSatisfying(m -> assertThat(m.endpoints()).isEmpty());

        GroupKey key = GroupKey.generate();
        holder.keys().serve(key, requester.identity().peerId()::equals);

        Optional<GroupKey> obtained =
                requester.keys().request(holder.identity().peerId(), Duration.ofSeconds(2));
        assertThat(obtained).as("the wrapped key arrives through the relay").isPresent();
        assertThat(obtained.get().rawBytes()).isEqualTo(key.rawBytes());

        // The relay carried the exchange but is not authorized: asking directly is
        // refused, and nothing it forwarded was readable to it.
        assertThat(relay.keys().request(holder.identity().peerId(), Duration.ofSeconds(2)))
                .isEmpty();
    }

    /** Mirrors of the distributor's private wire records, for hand-built frames. */
    private record KeyRequest(long nonce, byte[] encryptionPublicKey) {
    }

    private record KeyResponse(long nonce, boolean granted, byte[] ephemeralPublicKey,
                               byte[] sealed) {
    }

    /** SPEC §11a.2: a request whose "X25519 public key" is not 32 raw bytes is refused, even from an authorized identity. */
    @Test
    void malformedPublicKeysAreRefused() throws Exception {
        Wired holder = newPeer("h", 1);
        // A hand-driven requester: its own pipes handler captures the raw answer.
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(2).build();
        node.listen(network.register("raw"), "raw");
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "h", 0)));
        nodes.add(node);
        CapabilityPipes pipes = new CapabilityPipes(runtime, codec);
        List<KeyResponse> answers = new java.util.concurrent.CopyOnWriteArrayList<>();
        pipes.onCapability(GroupKeyDistributor.TYPE, (from, payload) ->
                answers.add(codec.fromBytes(payload, KeyResponse.class)));
        tickAll(4);

        holder.keys().serve(GroupKey.generate(), identity.peerId()::equals);

        pipes.send(holder.identity().peerId(), GroupKeyDistributor.TYPE,
                codec.toBytes(new KeyRequest(7, new byte[31])));
        pipes.send(holder.identity().peerId(), GroupKeyDistributor.TYPE,
                codec.toBytes(new KeyRequest(8, new byte[33])));

        assertThat(answers).hasSize(2);
        for (KeyResponse answer : answers) {
            assertThat(answer.granted()).isFalse();
            assertThat(answer.ephemeralPublicKey()).isNull();
            assertThat(answer.sealed()).isNull();
        }
        assertThat(answers).extracting(KeyResponse::nonce).containsExactly(7L, 8L);
    }

    // ------------------------------------------------ finding F-1: responses are bound

    /** Mirror of the distributor's wrap binding (field order is the wire order). */
    private record WrapBinding(String group, String holder, String requester, long nonce) {
    }

    /** A hand-driven peer: its pipes deliver every key-wrap frame to {@code onFrame}. */
    private record Raw(PeerIdentity identity, CapabilityPipes pipes) {
    }

    private Raw rawPeer(String address, long seed,
                        java.util.function.BiConsumer<PeerId, byte[]> onFrame) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "m", 0)));
        nodes.add(node);
        CapabilityPipes pipes = new CapabilityPipes(runtime, codec);
        pipes.onCapability(GroupKeyDistributor.TYPE, onFrame);
        return new Raw(identity, pipes);
    }

    private byte[] bindingBytes(PeerId holder, PeerId requester, long nonce) {
        return codec.toBytes(new WrapBinding(groupId.value(), holder.value(),
                requester.value(), nonce));
    }

    /** Finding F-1: a granted response from a peer other than the asked holder is ignored, so a member that learns a requester's public key cannot hand it a key of its choosing; the real holder's answer still completes the request. */
    @Test
    void aResponseFromAPeerOtherThanTheHolderIsIgnored() throws Exception {
        Wired member = newPeer("m", 1);
        GroupKey forged = GroupKey.generate();
        GroupKey real = GroupKey.generate();
        java.util.concurrent.atomic.AtomicReference<Raw> attacker = new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<Raw> holder = new java.util.concurrent.atomic.AtomicReference<>();
        holder.set(rawPeer("h", 2, (from, payload) -> {
            KeyRequest request = codec.fromBytes(payload, KeyRequest.class);
            // The attacker races in first: granted, sealed to the requester's
            // key, even bound exactly as the real holder would bind it.
            GroupKeyWrapAccess.Wrapped forgedWrap = GroupKeyWrapAccess.wrap(forged,
                    request.encryptionPublicKey(),
                    bindingBytes(holder.get().identity().peerId(), from, request.nonce()));
            attacker.get().pipes().send(from, GroupKeyDistributor.TYPE, codec.toBytes(
                    new KeyResponse(request.nonce(), true, forgedWrap.ephemeral(), forgedWrap.sealed())));
            GroupKeyWrapAccess.Wrapped realWrap = GroupKeyWrapAccess.wrap(real,
                    request.encryptionPublicKey(),
                    bindingBytes(holder.get().identity().peerId(), from, request.nonce()));
            holder.get().pipes().send(from, GroupKeyDistributor.TYPE, codec.toBytes(
                    new KeyResponse(request.nonce(), true, realWrap.ephemeral(), realWrap.sealed())));
        }));
        attacker.set(rawPeer("a", 3, (from, payload) -> { }));
        tickAll(4);

        Optional<GroupKey> obtained =
                member.keys().request(holder.get().identity().peerId(), Duration.ofSeconds(2));
        assertThat(obtained).as("the asked holder's key, not the attacker's")
                .hasValueSatisfying(key -> assertThat(key.rawBytes()).isEqualTo(real.rawBytes()));
    }

    /** Finding F-1: a third party cannot make a request fail by injecting a denial for its nonce. */
    @Test
    void aDenialInjectedByAThirdPartyDoesNotCompleteTheRequest() throws Exception {
        Wired member = newPeer("m", 1);
        GroupKey real = GroupKey.generate();
        java.util.concurrent.atomic.AtomicReference<Raw> attacker = new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<Raw> holder = new java.util.concurrent.atomic.AtomicReference<>();
        holder.set(rawPeer("h", 2, (from, payload) -> {
            KeyRequest request = codec.fromBytes(payload, KeyRequest.class);
            attacker.get().pipes().send(from, GroupKeyDistributor.TYPE, codec.toBytes(
                    new KeyResponse(request.nonce(), false, null, null)));
            GroupKeyWrapAccess.Wrapped wrap = GroupKeyWrapAccess.wrap(real,
                    request.encryptionPublicKey(),
                    bindingBytes(holder.get().identity().peerId(), from, request.nonce()));
            holder.get().pipes().send(from, GroupKeyDistributor.TYPE, codec.toBytes(
                    new KeyResponse(request.nonce(), true, wrap.ephemeral(), wrap.sealed())));
        }));
        attacker.set(rawPeer("a", 3, (from, payload) -> { }));
        tickAll(4);

        assertThat(member.keys().request(holder.get().identity().peerId(), Duration.ofSeconds(2)))
                .hasValueSatisfying(key -> assertThat(key.rawBytes()).isEqualTo(real.rawBytes()));
    }

    /** Finding F-1: a wrap sealed for another exchange (another nonce, or another requester) does not open, even from the asked holder. */
    @Test
    void aWrapBoundToAnotherExchangeDoesNotOpen() throws Exception {
        Wired member = newPeer("m", 1);
        java.util.concurrent.atomic.AtomicReference<Raw> holder = new java.util.concurrent.atomic.AtomicReference<>();
        holder.set(rawPeer("h", 2, (from, payload) -> {
            KeyRequest request = codec.fromBytes(payload, KeyRequest.class);
            GroupKeyWrapAccess.Wrapped replayed = GroupKeyWrapAccess.wrap(GroupKey.generate(),
                    request.encryptionPublicKey(),
                    bindingBytes(holder.get().identity().peerId(), from, request.nonce() + 1));
            holder.get().pipes().send(from, GroupKeyDistributor.TYPE, codec.toBytes(
                    new KeyResponse(request.nonce(), true, replayed.ephemeral(), replayed.sealed())));
        }));
        tickAll(4);

        assertThat(member.keys().request(holder.get().identity().peerId(), Duration.ofSeconds(2)))
                .isEmpty();
    }

    /** Finding F-1: a wrap the asked holder bound to another group (same holder, requester, and nonce) does not open. */
    @Test
    void aWrapBoundToAnotherGroupDoesNotOpen() throws Exception {
        Wired member = newPeer("m", 1);
        java.util.concurrent.atomic.AtomicReference<Raw> holder = new java.util.concurrent.atomic.AtomicReference<>();
        holder.set(rawPeer("h", 2, (from, payload) -> {
            KeyRequest request = codec.fromBytes(payload, KeyRequest.class);
            GroupKeyWrapAccess.Wrapped elsewhere = GroupKeyWrapAccess.wrap(GroupKey.generate(),
                    request.encryptionPublicKey(),
                    codec.toBytes(new WrapBinding("zAnotherGroup",
                            holder.get().identity().peerId().value(), from.value(), request.nonce())));
            holder.get().pipes().send(from, GroupKeyDistributor.TYPE, codec.toBytes(
                    new KeyResponse(request.nonce(), true, elsewhere.ephemeral(), elsewhere.sealed())));
        }));
        tickAll(4);

        assertThat(member.keys().request(holder.get().identity().peerId(), Duration.ofSeconds(2)))
                .isEmpty();
    }

    /** Finding F-1: request nonces are random, not a counter an observer can predict. */
    @Test
    void requestNoncesAreNotSequential() throws Exception {
        Wired member = newPeer("m", 1);
        List<Long> nonces = new java.util.concurrent.CopyOnWriteArrayList<>();
        Raw holder = rawPeer("h", 2, (from, payload) -> {
            KeyRequest request = codec.fromBytes(payload, KeyRequest.class);
            nonces.add(request.nonce());
        });
        tickAll(4);

        member.keys().request(holder.identity().peerId(), Duration.ofMillis(50));
        member.keys().request(holder.identity().peerId(), Duration.ofMillis(50));
        assertThat(nonces).hasSize(2);
        assertThat(Math.abs(nonces.get(1) - nonces.get(0))).isGreaterThan(1L << 20);
    }

    // ------------------------------------------- epoch-0 injection (audit 2026-10-02)

    private record EpochRequest(long nonce, byte[] encryptionPublicKey, List<Long> epochs) {
    }

    private record OfferedKey(long epoch, String rotator, String cutover, byte[] proof,
                              byte[] ephemeralPublicKey, byte[] sealed) {
    }

    private record EpochResponse(long nonce, boolean granted, byte[] ephemeralPublicKey,
                                 byte[] sealed, List<OfferedKey> keys) {
    }

    private OfferedKey offer(GroupKey key, long epoch, String rotator, PeerId holder, PeerId requester,
                             EpochRequest request) {
        byte[] binding = codec.toBytes(new GroupKeyDistributor.WrapBinding(groupId.value(),
                holder.value(), requester.value(), request.nonce(), epoch, rotator, null));
        GroupKeyWrapAccess.Wrapped wrap = GroupKeyWrapAccess.wrap(key, request.encryptionPublicKey(), binding);
        return new OfferedKey(epoch, rotator, "1970-01-01T00:00:00Z", null, wrap.ephemeral(), wrap.sealed());
    }

    /**
     * A key holder cannot plant an epoch-0 key under a rotator name of its
     * choosing (one that would sort first and become the sealing key), and it
     * cannot slip in an epoch the request did not ask for.
     */
    @Test
    void aHolderCannotPlantAnEpochZeroKeyOrAnUnrequestedEpoch() throws Exception {
        Wired member = newPeer("m", 1);
        GroupKey planted = GroupKey.generate();
        GroupKey configured = GroupKey.generate();
        java.util.concurrent.atomic.AtomicReference<Raw> holder = new java.util.concurrent.atomic.AtomicReference<>();
        holder.set(rawPeer("h", 2, (from, payload) -> {
            EpochRequest request = codec.fromBytes(payload, EpochRequest.class);
            PeerId self = holder.get().identity().peerId();
            List<OfferedKey> offered = List.of(
                    offer(planted, 0, "zzz-ground-to-sort-first", self, from, request),
                    offer(configured, 0, "", self, from, request));
            holder.get().pipes().send(from, GroupKeyDistributor.TYPE, codec.toBytes(
                    new EpochResponse(request.nonce(), true, null, null, offered)));
        }));
        tickAll(4);

        assertThat(member.keys().fetch(holder.get().identity().peerId(), List.of(7L), Duration.ofSeconds(2)))
                .as("nothing outside the requested epochs is taken").isEmpty();
        List<ai.badmonkey.agentspaces.common.crypto.GroupKeyRing.Held> zero =
                member.keys().fetch(holder.get().identity().peerId(), List.of(0L), Duration.ofSeconds(2));
        assertThat(zero).as("epoch 0 only under its own empty rotator name").singleElement()
                .satisfies(held -> {
                    assertThat(held.rotator()).isEmpty();
                    assertThat(held.key().rawBytes()).isEqualTo(configured.rawBytes());
                });
    }
}
