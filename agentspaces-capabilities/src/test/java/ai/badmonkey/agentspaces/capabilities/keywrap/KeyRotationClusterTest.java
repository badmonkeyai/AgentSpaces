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
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.GroupKey;
import ai.badmonkey.agentspaces.common.crypto.GroupKeyRing;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
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
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SPEC §11a.3 v0.1.13 (TODO-9-10-11 D3/D4, review B-2): content-key rotation
 * across a fleet. The founder rotates; members follow from the key-wrap
 * advertisements and fetch the new epoch; old entries stay readable and new
 * writes seal under the new epoch everywhere; and an epoch minted by a peer
 * without rotation authority is never installed, whoever relays it.
 */
class KeyRotationClusterTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final PeerIdentity founderId = PeerIdentity.generate();
    private final GroupId groupId = GroupId.of("zRotation");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zRotation", founderId.peerId(), groupId, java.time.Instant.EPOCH,
            Duration.ofDays(1), "rotation", GroupAdvertisement.MembershipPolicy.OPEN,
            ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());

    private record Member(PeerIdentity identity, PeerNode node, GroupKeyDistributor keys,
                          GroupKeyRing ring, ReplicatedSpace space) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Member member(PeerIdentity identity, String address, long seed, GroupKey epochZero,
                          String... seedAddresses) throws IOException {
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        DiscoveryService discovery = DiscoveryService.create(runtime, codec, identity.peerId(),
                clock, Set.of());
        GroupKeyRing ring = GroupKeyRing.of(epochZero);
        GroupKeyDistributor keys = new GroupKeyDistributor(new CapabilityPipes(runtime, codec),
                identity.peerId(), codec, clock);
        keys.serve(ring, peer -> runtime.membership().member(peer).isPresent());
        keys.follow(ring, discovery);
        CapabilityRuntime capabilities = new CapabilityRuntime(runtime, discovery, identity);
        capabilities.register(keys);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "sealed", identity, "worker")
                .clock(clock).settleWindow(Duration.ZERO).keyRing(ring).build();
        nodes.add(node);
        return new Member(identity, node, keys, ring, space);
    }

    private void tickAll(int rounds) throws InterruptedException {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
            Thread.sleep(20); // background fetches run on virtual threads
        }
    }

    @Test
    void theFoundersRotationReachesEveryMemberAndOldEntriesStayReadable() throws Exception {
        GroupKey epochZero = GroupKey.generate();
        Member founder = member(founderId, "f", 1, epochZero);
        Member a = member(PeerIdentity.generate(), "a", 2, epochZero, "f");
        Member b = member(PeerIdentity.generate(), "b", 3, epochZero, "f");
        tickAll(4);
        a.space().write(new TaskEntry("before rotation", 1), Lease.of(Duration.ofHours(1)));
        tickAll(2);

        long epoch = founder.keys().rotate(founderId, clock.instant());
        assertThat(epoch).isEqualTo(1);
        for (int i = 0; i < 40 && !(a.ring().epochs().contains(1L) && b.ring().epochs().contains(1L)); i++) {
            tickAll(1);
        }
        assertThat(a.ring().epochs()).as("a fetched the new epoch").contains(1L);
        assertThat(b.ring().epochs()).as("b fetched the new epoch").contains(1L);

        var after = b.space().write(new TaskEntry("after rotation", 2), Lease.of(Duration.ofHours(1)));
        assertThat(b.space().signedState(after.entryId()).orElseThrow().record().keyEpoch())
                .isEqualTo(1L);
        tickAll(6);
        for (Member m : List.of(founder, a, b)) {
            assertThat(m.space().readAll(Template.of(TaskEntry.class), 10))
                    .as("both entries readable at " + m.identity().peerId().display())
                    .containsExactlyInAnyOrder(new TaskEntry("before rotation", 1),
                            new TaskEntry("after rotation", 2));
        }
    }

    @Test
    void aMemberWithoutRotationAuthorityCannotRotate() throws Exception {
        GroupKey epochZero = GroupKey.generate();
        member(founderId, "f", 1, epochZero);
        Member a = member(PeerIdentity.generate(), "a", 2, epochZero, "f");
        tickAll(4);
        assertThatThrownBy(() -> a.keys().rotate(a.identity(), clock.instant()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("may not rotate");
    }

    @Test
    void anEpochMintedWithoutAuthorityIsNeverInstalledWhoeverRelaysIt() throws Exception {
        GroupKey epochZero = GroupKey.generate();
        Member founder = member(founderId, "f", 1, epochZero);
        Member rogue = member(PeerIdentity.generate(), "r", 2, epochZero, "f");
        Member victim = member(PeerIdentity.generate(), "v", 3, epochZero, "f");
        tickAll(4);
        // The rogue widens its own notion of who may rotate, mints epoch 1, and
        // advertises it; the victim (and the founder) follow only real rotators.
        rogue.keys().rotators(peer -> true);
        rogue.keys().rotate(rogue.identity(), clock.instant());
        tickAll(15);
        assertThat(victim.ring().epochs()).as("the rogue's epoch is not vouched for").containsExactly(0L);
        assertThat(founder.ring().epochs()).containsExactly(0L);
        assertThat(victim.keys().fetch(rogue.identity().peerId(), List.of(1L), Duration.ofSeconds(2)))
                .as("even a direct fetch yields nothing installable").isEmpty();
    }

    /** Review M-10: a scheduled rotator mints a new epoch each period, each cutting over after the configured delay, and members follow. */
    @Test
    void aScheduledRotatorRotatesEachPeriodAndMembersFollow() throws Exception {
        GroupKey epochZero = GroupKey.generate();
        Member founder = member(founderId, "f", 1, epochZero);
        Member a = member(PeerIdentity.generate(), "a", 2, epochZero, "f");
        tickAll(4);
        founder.keys().autoRotate(founderId, Duration.ofMinutes(30), Duration.ofSeconds(30));
        tickAll(2);
        assertThat(founder.ring().epochs()).as("not yet due").containsExactly(0L);
        clock.advance(Duration.ofMinutes(30));
        tickAll(2);
        assertThat(founder.ring().epochs()).containsExactly(0L, 1L);
        assertThat(founder.ring().cutover(1).orElseThrow())
                .as("the cutover leaves members time to fetch").isAfter(clock.instant().minusSeconds(5));
        clock.advance(Duration.ofMinutes(31));
        tickAll(2);
        assertThat(founder.ring().epochs()).containsExactly(0L, 1L, 2L);
        for (int i = 0; i < 40 && !a.ring().epochs().contains(2L); i++) {
            tickAll(1);
        }
        assertThat(a.ring().epochs()).contains(1L, 2L);
    }

    /**
     * Audit 2026-10-02: two authorized rotators racing to mint the same epoch
     * converge. Each first holds only its own key and seals under it; a record
     * that will not open under the keys held makes the ring fetch the epoch
     * again, which brings the other rotator's key; then every member opens
     * both writes and every writer seals under the one canonical key.
     */
    @Test
    void racingRotatorsConvergeOnBothKeysAndOneCanonicalSealingKey() throws Exception {
        GroupKey epochZero = GroupKey.generate();
        PeerIdentity rivalId = PeerIdentity.generate();
        Member founder = member(founderId, "f", 1, epochZero);
        Member rival = member(rivalId, "r", 2, epochZero, "f");
        Member reader = member(PeerIdentity.generate(), "a", 3, epochZero, "f");
        java.util.function.Predicate<ai.badmonkey.agentspaces.common.id.PeerId> both =
                peer -> peer.equals(founderId.peerId()) || peer.equals(rivalId.peerId());
        for (Member m : List.of(founder, rival, reader)) {
            m.keys().rotators(both);
        }
        tickAll(4);

        // A real race: neither rotator hears the other until both have minted and
        // written. Without the partition the founder's write reaches the rival
        // at once, the rival fetches the founder's key before it writes, and
        // both records seal under one key.
        network.partition("f", "r");
        network.partition("f", "a");
        network.partition("r", "a");
        assertThat(founder.keys().rotate(founderId, clock.instant())).isEqualTo(1);
        assertThat(rival.keys().rotate(rivalId, clock.instant())).isEqualTo(1);
        founder.space().write(new TaskEntry("by the founder", 1), Lease.of(Duration.ofHours(1)));
        rival.space().write(new TaskEntry("by the rival", 2), Lease.of(Duration.ofHours(1)));
        assertThat(founder.ring().openingKeys(1)).as("each rotator holds only its own key").hasSize(1);
        assertThat(rival.ring().openingKeys(1)).hasSize(1);
        network.heal();

        List<Member> fleet = List.of(founder, rival, reader);
        // Members read as applications do; a read that cannot open a record is
        // what sends a member back for the other key (re-asked at most once a
        // second per epoch), so give it up to about ten seconds.
        for (int i = 0; i < 400; i++) {
            long readable = fleet.stream()
                    .filter(m -> m.space().readAll(Template.of(TaskEntry.class), 10).size() == 2).count();
            if (readable == fleet.size()
                    && fleet.stream().allMatch(m -> m.ring().openingKeys(1).size() == 2)) {
                break;
            }
            tickAll(1);
        }
        for (Member m : fleet) {
            assertThat(m.ring().openingKeys(1)).as("both rotators' keys at " + m.identity().peerId().display())
                    .hasSize(2);
            assertThat(m.space().readAll(Template.of(TaskEntry.class), 10))
                    .as("both writes readable at " + m.identity().peerId().display()).hasSize(2);
        }
        byte[] canonical = founder.ring().sealingKey(1).orElseThrow().rawBytes();
        for (Member m : fleet) {
            assertThat(m.ring().sealingKey(1).orElseThrow().rawBytes()).as("one sealing key fleet-wide")
                    .isEqualTo(canonical);
        }
    }
}
