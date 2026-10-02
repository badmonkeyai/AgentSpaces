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
package ai.badmonkey.agentspaces.peering.membership;

import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SWIM membership mechanics (spec §5.2) in isolation: the indirect probe stage
 * asks exactly k relays and lets an asked relay vouch, a silent member lapses
 * at its TTL before any probe, the sampler bounds and excludes correctly, and
 * the view follows each member's latest leased advertisement.
 */
class GroupMembershipTest {

    private final TestClock clock = TestClock.create();
    private final PeerId self = PeerIdentity.generate().peerId();
    private final GroupId group = GroupId.of("zMembership");

    /** Records every probe; answers non-target pings honestly so only the target is ever in doubt. */
    private static final class RecordingProber implements GroupMembership.Prober {
        final GroupMembership membership;
        final PeerId target;
        final List<Long> targetPings = new ArrayList<>();
        final List<PeerId> otherPings = new ArrayList<>();
        final List<PeerId> relays = new ArrayList<>();
        final List<PeerId> reqTargets = new ArrayList<>();
        final List<Long> reqNonces = new ArrayList<>();

        RecordingProber(GroupMembership membership, PeerId target) {
            this.membership = membership;
            this.target = target;
        }

        @Override
        public void ping(PeerId to, long nonce) {
            if (to.equals(target)) {
                targetPings.add(nonce);
            } else {
                otherPings.add(to);
                membership.onAck(nonce, to); // an honest, reachable member
            }
        }

        @Override
        public void pingReq(PeerId relay, PeerId to, long nonce) {
            relays.add(relay);
            reqTargets.add(to);
            reqNonces.add(nonce);
        }
    }

    private GroupMembership membership(GroupMembership.Config config) {
        return new GroupMembership(self, clock, new Random(11), config);
    }

    private PeerAdvertisement ad(PeerId peer, Set<PeerAdvertisement.PeerRole> roles,
                                 List<PeerAdvertisement.Endpoint> endpoints) {
        return new PeerAdvertisement("aspace://" + group.value() + "/peer/" + peer.value(),
                peer, group, clock.instant(), Duration.ofMinutes(10), endpoints, roles, Map.of());
    }

    private PeerId admit(GroupMembership membership) {
        PeerId peer = PeerIdentity.generate().peerId();
        membership.onPeerAdvertisement(ad(peer, Set.of(),
                List.of(new PeerAdvertisement.Endpoint("mem", "x", 0))));
        return peer;
    }

    /** Spec §5.2: on direct timeout, PING_REQ goes to k members; an asked relay's ACK clears suspicion. */
    @Test
    void indirectProbesAreAskedOfKRelaysAndAnAskedRelayMayVouch() {
        GroupMembership membership = membership(
                new GroupMembership.Config(Duration.ofMinutes(5), Duration.ofSeconds(1), 2));
        PeerId target = admit(membership);
        for (int i = 0; i < 3; i++) {
            admit(membership);
        }
        RecordingProber prober = new RecordingProber(membership, target);

        for (int i = 0; i < 50 && prober.targetPings.isEmpty(); i++) {
            membership.tick(prober);
        }
        assertThat(prober.targetPings).as("the target was eventually probed").hasSize(1);
        assertThat(membership.member(target)).hasValueSatisfying(m ->
                assertThat(m.suspect()).as("not yet suspect: the ping is still outstanding").isFalse());

        // The direct probe times out: the target is suspected and exactly k
        // other members are asked to probe it on our behalf.
        clock.advance(Duration.ofMillis(1_500));
        membership.tick(prober);
        assertThat(membership.member(target)).hasValueSatisfying(m ->
                assertThat(m.suspect()).isTrue());
        assertThat(prober.relays).hasSize(2).doesNotContain(target, self);
        assertThat(new HashSet<>(prober.relays)).as("distinct relays").hasSize(2);
        assertThat(prober.reqTargets).containsOnly(target);
        assertThat(new HashSet<>(prober.reqNonces)).as("one nonce for the indirect round").hasSize(1);

        // An asked relay vouches: the target is credited and no longer suspect.
        membership.onAck(prober.reqNonces.get(0), prober.relays.get(0));
        assertThat(membership.member(target)).hasValueSatisfying(m ->
                assertThat(m.suspect()).isFalse());

        // Well past the indirect deadline the target is still a member.
        clock.advance(Duration.ofSeconds(5));
        membership.tick(prober);
        assertThat(membership.member(target)).isPresent();
    }

    /** Spec §5.2: a target unanswered through the indirect stage is dropped. */
    @Test
    void aTargetUnansweredThroughTheIndirectStageIsDropped() {
        GroupMembership membership = membership(
                new GroupMembership.Config(Duration.ofMinutes(5), Duration.ofSeconds(1), 2));
        PeerId target = admit(membership);
        for (int i = 0; i < 3; i++) {
            admit(membership);
        }
        RecordingProber prober = new RecordingProber(membership, target);
        for (int i = 0; i < 50 && prober.targetPings.isEmpty(); i++) {
            membership.tick(prober);
        }

        clock.advance(Duration.ofMillis(1_500));
        membership.tick(prober); // escalates to the indirect stage
        assertThat(prober.relays).hasSize(2);
        assertThat(membership.member(target)).as("still present while relays are asked").isPresent();

        clock.advance(Duration.ofMillis(1_500));
        membership.tick(prober); // nobody vouched: dropped, long before the 5-minute TTL
        assertThat(membership.member(target)).isEmpty();
        assertThat(membership.allMembers()).as("honest members are untouched").hasSize(3);
    }

    /** Spec §5.2 (P2): a member that neither gossips nor answers lapses at its TTL, before any probe. */
    @Test
    void aSilentMemberLapsesAtItsTtlBeforeAnyProbe() {
        GroupMembership membership = membership(GroupMembership.Config.defaults());
        PeerId silent = admit(membership);
        List<PeerId> pinged = new ArrayList<>();
        GroupMembership.Prober prober = new GroupMembership.Prober() {
            @Override
            public void ping(PeerId to, long nonce) {
                pinged.add(to);
            }

            @Override
            public void pingReq(PeerId relay, PeerId to, long nonce) {
                pinged.add(to);
            }
        };

        clock.advance(GroupMembership.Config.defaults().memberTtl().minusSeconds(1));
        assertThat(membership.member(silent)).as("inside the TTL: still a member").isPresent();
        membership.onAck(-1L, silent); // an uncorrelated ACK credits nothing

        // No probe round ever ran: expiry alone, checked first in the tick, drops it.
        clock.advance(Duration.ofSeconds(2));
        membership.tick(prober);
        assertThat(membership.member(silent)).as("past the TTL: dropped without ceremony").isEmpty();
        assertThat(pinged).as("the lapsed member was never probed").isEmpty();
    }

    /** Spec §5.2: {@code PeerSampler.randomMembers(n)} returns at most n distinct live members, never self. */
    @Test
    void randomMembersExcludesSelfAndBoundsN() {
        GroupMembership membership = membership(GroupMembership.Config.defaults());
        Set<PeerId> admitted = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            admitted.add(admit(membership));
        }
        membership.onPeerAdvertisement(ad(self, Set.of(), List.of()));

        List<PeerId> three = membership.randomMembers(3);
        assertThat(three).hasSize(3).doesNotHaveDuplicates().doesNotContain(self);
        assertThat(admitted).containsAll(three);
        assertThat(membership.randomMembers(10)).hasSize(5).containsExactlyInAnyOrderElementsOf(admitted);
        assertThat(membership.randomMembers(0)).isEmpty();
        assertThat(membership.member(self)).as("a node is never in its own view").isEmpty();
    }

    /** Spec §5.4/§5.2: roles and endpoints follow the latest leased advertisement; eviction is idempotent. */
    @Test
    void rolesAndEndpointsFollowTheLatestAdvertisementAndEvictionIsIdempotent() {
        GroupMembership membership = membership(GroupMembership.Config.defaults());
        PeerId peer = PeerIdentity.generate().peerId();
        PeerAdvertisement.Endpoint first = new PeerAdvertisement.Endpoint("mem", "one", 0);
        PeerAdvertisement.Endpoint second = new PeerAdvertisement.Endpoint("tcp", "two", 0);

        membership.onPeerAdvertisement(ad(peer, Set.of(), List.of(first)));
        assertThat(membership.withRole(PeerAdvertisement.PeerRole.RENDEZVOUS)).isEmpty();

        membership.onPeerAdvertisement(ad(peer,
                Set.of(PeerAdvertisement.PeerRole.RENDEZVOUS), List.of(second)));
        assertThat(membership.withRole(PeerAdvertisement.PeerRole.RENDEZVOUS)).containsExactly(peer);
        assertThat(membership.member(peer)).hasValueSatisfying(m -> {
            assertThat(m.roles()).containsExactly(PeerAdvertisement.PeerRole.RENDEZVOUS);
            assertThat(m.endpoints()).containsExactly(second);
        });

        membership.onPeerAdvertisement(ad(peer, Set.of(), List.of(first)));
        assertThat(membership.withRole(PeerAdvertisement.PeerRole.RENDEZVOUS))
                .as("a role dropped from the lease is dropped from the view").isEmpty();

        membership.evict(peer);
        membership.evict(peer);
        assertThat(membership.member(peer)).isEmpty();
        assertThat(membership.allMembers()).isEmpty();
    }
}
