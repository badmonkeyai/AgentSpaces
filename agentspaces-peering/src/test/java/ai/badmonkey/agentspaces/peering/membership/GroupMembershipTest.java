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
                new GroupMembership.Config(Duration.ofMinutes(5), Duration.ofSeconds(1), 2, Duration.ZERO)); // probe every tick: the clock stands still between ticks here
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
                new GroupMembership.Config(Duration.ofMinutes(5), Duration.ofSeconds(1), 2, Duration.ZERO)); // probe every tick: the clock stands still between ticks here
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

    /**
     * v0.1.17: on a quiet fleet the probe interval doubles at each probe, up to sixteen times the
     * base where the member TTL allows (idle-loop item 3), and the first active tick snaps it back. Every ping is answered here, so nothing
     * is ever suspected; only the cadence is under test.
     */
    /** Idle-loop item 3: past four times the base, the stretch keeps every member heard twice inside the TTL. */
    @Test
    void theQuietStretchIsBoundedByTheMemberTtlAndNeverBelowFourTimes() {
        GroupMembership membership = membership(
                new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2, Duration.ofSeconds(1)));
        assertThat(membership.maxQuietIntervalMillis(1)).as("a pair: 30 s TTL over two").isEqualTo(15_000L);
        assertThat(membership.maxQuietIntervalMillis(3)).as("four members").isEqualTo(5_000L);
        assertThat(membership.maxQuietIntervalMillis(9)).as("ten members keep the four-times stretch")
                .isEqualTo(4_000L);
        membership.probeStretch(GroupMembership.LEGACY_QUIET_STRETCH);
        assertThat(membership.maxQuietIntervalMillis(1)).as("a lowered ceiling holds").isEqualTo(4_000L);
    }

    @Test
    void theProbeIntervalStretchesWhileQuietAndSnapsBackOnActivity() {
        GroupMembership membership = membership(
                new GroupMembership.Config(Duration.ofMinutes(5), Duration.ofSeconds(1), 2, Duration.ofSeconds(1)));
        admit(membership);
        admit(membership);
        RecordingProber prober = new RecordingProber(membership, PeerIdentity.generate().peerId());
        List<Integer> probeSeconds = new ArrayList<>();
        for (int second = 0; second < 20; second++) {
            int before = prober.otherPings.size();
            membership.tick(prober, true);
            if (prober.otherPings.size() > before) {
                probeSeconds.add(second);
            }
            clock.advance(Duration.ofSeconds(1));
        }
        // idle-loop item 3: past four times the base the stretch is bounded by the member TTL; with a five-minute TTL
        // and two other members the bound is far off, so the interval doubles to the ceiling of sixteen seconds
        assertThat(membership.maxQuietIntervalMillis(2)).isEqualTo(16_000L);
        assertThat(probeSeconds).as("base, base, then doubling: 2, 4, 8 and on to sixteen seconds")
                .containsExactly(0, 1, 3, 7, 15);
        int before = prober.otherPings.size();
        membership.tick(prober, false); // something moved: back to the base interval at once
        assertThat(prober.otherPings.size()).isEqualTo(before + 1);
        clock.advance(Duration.ofSeconds(1));
        membership.tick(prober, false);
        assertThat(prober.otherPings.size()).as("active ticks probe every second").isEqualTo(before + 2);
        assertThat(membership.allMembers()).hasSize(2);
    }

    // ---- srad/agentspaces-event-loop.md phase 2: the member TTL (EL-E-05 to EL-E-07), the kept probe order (TR-4)
    // and the targeted relay (TR-1, TR-2, TR-5)

    /** A probe that never answers within the test: a judgement here is the TTL's alone. */
    private static GroupMembership.Config noTimeout(Duration memberTtl, Duration keepalive) {
        return new GroupMembership.Config(memberTtl, Duration.ofHours(10), 2, Duration.ofSeconds(1), keepalive, 1.5);
    }

    private static final GroupMembership.Prober SILENT = new GroupMembership.Prober() {
        @Override
        public void ping(PeerId target, long nonce) {
        }

        @Override
        public void pingReq(PeerId relay, PeerId target, long nonce) {
        }
    };

    private List<PeerId> admitMany(GroupMembership membership, int n) {
        List<PeerId> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            out.add(admit(membership));
        }
        return out;
    }

    /** EL-E-05: max(configured, 1.5 quiet probe cycles), the cycle on the keepalive bound, at every size. */
    @Test
    void theMemberTtlIsTheFloorOrOneAndAHalfQuietCyclesAtEverySize() {
        Map<Integer, Long> expected = new java.util.LinkedHashMap<>();
        for (int others = 1; others <= 7; others++) {
            expected.put(others, 45_000L);
        }
        expected.put(8, 48_000L);   // eight others: 8 x 4 s = 32 s, 48 s
        expected.put(9, 54_000L);   // ten members: 36 s, 54 s
        expected.put(10, 60_000L);
        expected.put(11, 66_000L);  // twelve members
        expected.put(12, 72_000L);
        expected.put(13, 78_000L);
        expected.put(14, 84_000L);
        expected.put(15, 90_000L);  // sixteen members
        expected.put(49, 294_000L); // fifty members
        for (Map.Entry<Integer, Long> e : expected.entrySet()) {
            GroupMembership membership = membership(GroupMembership.Config.defaults());
            admitMany(membership, e.getKey());
            assertThat(membership.memberTtlMillis()).as("%d others", e.getKey()).isEqualTo(e.getValue());
            assertThat(membership.quietProbeCycleMillis()).as("the cycle at %d others", e.getKey())
                    .isEqualTo(e.getKey() * membership.maxQuietIntervalMillis(e.getKey()));
        }
        // a configured TTL takes the place of the floor; the 1.5 term still applies
        GroupMembership thirty = membership(new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2));
        admitMany(thirty, 1);
        assertThat(thirty.memberTtlMillis()).isEqualTo(30_000L);
        admitMany(thirty, 8);
        assertThat(thirty.memberTtlMillis()).as("ten members at 30 s").isEqualTo(54_000L);
        GroupMembership ten = membership(new GroupMembership.Config(Duration.ofSeconds(10), Duration.ofSeconds(2), 2));
        admitMany(ten, 1);
        assertThat(ten.memberTtlMillis()).as("a pair at 10 s: 5 s quiet interval, 7.5 s").isEqualTo(10_000L);
        admitMany(ten, 8);
        assertThat(ten.memberTtlMillis()).as("ten members at 10 s").isEqualTo(54_000L);
        GroupMembership five = membership(new GroupMembership.Config(Duration.ofMinutes(5), Duration.ofSeconds(2), 2));
        admitMany(five, 9);
        assertThat(five.memberTtlMillis()).as("ten members at 5 min: 144 s cycle, 216 s").isEqualTo(300_000L);
    }

    /** EL-E-06: under the defaults this node's own keepalive reads the 30 s bound, exactly as before. */
    @Test
    void theOwnKeepaliveReadsTheThirtySecondBound() {
        GroupMembership membership = membership(GroupMembership.Config.defaults());
        assertThat(GroupMembership.Config.defaults().memberTtl()).isEqualTo(GroupMembership.MEMBER_TTL_FLOOR)
                .isEqualTo(Duration.ofSeconds(45));
        assertThat(GroupMembership.Config.defaults().keepaliveTtl()).isEqualTo(Duration.ofSeconds(30));
        assertThat(membership.maxQuietIntervalMillis(1)).isEqualTo(15_000L);
        assertThat(membership.maxQuietIntervalMillis(3)).isEqualTo(5_000L);
        assertThat(membership.maxQuietIntervalMillis(9)).isEqualTo(4_000L);
        // owner answer 10: an explicit configuration keeps its keepalive on its own TTL
        GroupMembership.Config explicit = new GroupMembership.Config(Duration.ofSeconds(20), Duration.ofSeconds(2), 2);
        assertThat(explicit.keepaliveTtl()).isEqualTo(Duration.ofSeconds(20));
        assertThat(explicit.cycleFactor()).isEqualTo(1.5);
    }

    /** Owner answer 9: a member that keeps itself alive is judged at the floor; one heard only through probes, longer. */
    @Test
    void aMemberThatKeepsItselfAliveIsJudgedAtTheFloorAndAProbeOnlyMemberByTheCycle() {
        GroupMembership membership = membership(noTimeout(Duration.ofSeconds(45), Duration.ofSeconds(30)));
        List<PeerId> nine = admitMany(membership, 9);
        PeerId direct = nine.get(0);
        membership.recordHeard(direct); // a frame it signed: it keeps itself alive
        membership.tick(SILENT);
        assertThat(membership.memberTtlMillis()).isEqualTo(54_000L);
        clock.advance(Duration.ofMillis(45_001));
        membership.tick(SILENT);
        assertThat(membership.member(direct)).as("silent past the floor: dropped").isEmpty();
        assertThat(membership.member(nine.get(1))).as("heard only through probes: kept to the cycle TTL").isPresent();
        clock.advance(Duration.ofMillis(9_000)); // 54,001 ms: past the cycle TTL of the view as it was
        membership.tick(SILENT);
        assertThat(membership.allMembers()).as("the cycle TTL never falls below the one held at the drop").isEmpty();
    }

    /** §3.11: a rise applies at once; a fall applies only to judgements that start after it. */
    @Test
    void aRiseAppliesAtOnceAndAFallOnlyToJudgementsThatStartAfterIt() {
        GroupMembership membership = membership(noTimeout(Duration.ofSeconds(45), Duration.ofSeconds(30)));
        List<PeerId> first = admitMany(membership, 8);
        membership.tick(SILENT);
        assertThat(membership.memberTtlMillis()).isEqualTo(48_000L);
        clock.advance(Duration.ofSeconds(40));
        List<PeerId> later = admitMany(membership, 7); // sixteen members: 90 s, at once
        assertThat(membership.memberTtlMillis()).isEqualTo(90_000L);
        clock.advance(Duration.ofSeconds(20)); // 60 s after the first eight were heard
        membership.tick(SILENT);
        assertThat(membership.allMembers()).as("the rise kept the first eight").hasSize(15);

        // the group shrinks back to nine: 48 s, but a member heard under 90 s keeps 90 s
        for (PeerId gone : later) {
            membership.evict(gone);
        }
        assertThat(membership.memberTtlMillis()).isEqualTo(48_000L);
        membership.tick(SILENT);
        assertThat(membership.allMembers()).as("heard 60 s ago, inside the old 90 s").hasSize(8);
        PeerId fresh = first.get(0);
        membership.onPeerAdvertisement(ad(fresh, Set.of(), List.of(new PeerAdvertisement.Endpoint("mem", "x", 0))));
        clock.advance(Duration.ofSeconds(29)); // 89 s after the others were heard
        membership.tick(SILENT);
        assertThat(membership.allMembers()).hasSize(8);
        clock.advance(Duration.ofMillis(1_001)); // 90,001 ms
        membership.tick(SILENT);
        assertThat(membership.allMembers().stream().map(GroupMembership.Member::id).toList())
                .as("the old TTL bounded only the judgements it started").containsExactly(fresh);
        clock.advance(Duration.ofMillis(18_000)); // 48,001 ms after the fresh one was heard, under the new TTL
        membership.tick(SILENT);
        assertThat(membership.allMembers()).as("heard after the fall: judged at the new TTL").isEmpty();
    }

    /** TR-4, EL-P-15: the probe order is kept, so one member's probes are at most a cycle apart plus newcomers. */
    @Test
    void theProbeOrderIsKeptAcrossCyclesSoTheGapIsOneCycle() {
        GroupMembership membership = membership(GroupMembership.Config.defaults());
        List<PeerId> nine = admitMany(membership, 9);
        Map<PeerId, Integer> lastTurn = new java.util.HashMap<>();
        Map<PeerId, Integer> sizeAtLast = new java.util.HashMap<>();
        Map<PeerId, Integer> admittedAtTurn = new java.util.HashMap<>();
        int[] turn = {0};
        int[] worstExcess = {Integer.MIN_VALUE};
        List<Integer> admissions = new ArrayList<>();
        GroupMembership.Prober prober = new GroupMembership.Prober() {
            @Override
            public void ping(PeerId target, long nonce) {
                turn[0]++;
                Integer previous = lastTurn.put(target, turn[0]);
                Integer size = sizeAtLast.put(target, membership.size());
                if (previous != null) {
                    // one cycle is one turn per member in view (the larger of then and now: a departure is skipped
                    // when the cursor reaches it), plus one per member admitted since
                    int newcomers = (int) admissions.stream().filter(t -> t >= previous).count();
                    int cycle = Math.max(size, membership.size());
                    int excess = (turn[0] - previous) - (cycle + newcomers);
                    worstExcess[0] = Math.max(worstExcess[0], excess);
                }
                membership.onAck(nonce, target);
            }

            @Override
            public void pingReq(PeerId relay, PeerId target, long nonce) {
            }
        };
        PeerId departing = nine.get(4);
        for (int second = 0; second < 40 * 36; second++) {
            if (second == 20 * 36) {
                admissions.add(turn[0]);
                admitted(membership, admittedAtTurn, turn[0]);
            }
            if (second == 25 * 36) {
                membership.evict(departing);
            }
            membership.tick(prober, true);
            clock.advance(Duration.ofSeconds(1));
        }
        assertThat(turn[0]).as("probes ran on the quiet stretch").isGreaterThan(250);
        assertThat(worstExcess[0]).as("the gap between two probes of a member, in turns, past one cycle")
                .isLessThanOrEqualTo(0);
        assertThat(membership.allMembers()).hasSize(9);
    }

    private void admitted(GroupMembership membership, Map<PeerId, Integer> at, int turn) {
        at.put(admit(membership), turn);
    }

    /** A prober with holders: records every frame; answers nothing unless told. */
    private static final class HolderProber implements GroupMembership.Prober {
        final Map<PeerId, java.util.Collection<PeerId>> link = new java.util.HashMap<>();
        final Map<PeerId, java.util.Collection<PeerId>> declared = new java.util.HashMap<>();
        final List<PeerId> pings = new ArrayList<>();
        final List<PeerId[]> reqs = new ArrayList<>();
        final List<Long> reqNonces = new ArrayList<>();
        java.util.function.BiConsumer<PeerId, Long> onPing = (t, n) -> { };

        @Override
        public void ping(PeerId target, long nonce) {
            pings.add(target);
            onPing.accept(target, nonce);
        }

        @Override
        public void pingReq(PeerId relay, PeerId target, long nonce) {
            reqs.add(new PeerId[] {relay, target});
            reqNonces.add(nonce);
        }

        @Override
        public java.util.Collection<PeerId> linkHolders(PeerId target) {
            return link.getOrDefault(target, List.of());
        }

        @Override
        public java.util.Collection<PeerId> declaredHolders(PeerId target) {
            return declared.getOrDefault(target, List.of());
        }

        long reqsTo(PeerId relay, PeerId target) {
            return reqs.stream().filter(r -> r[0].equals(relay) && r[1].equals(target)).count();
        }
    }

    private GroupMembership.Config relayConfig() {
        return new GroupMembership.Config(Duration.ofSeconds(45), Duration.ofSeconds(2), 2, Duration.ofSeconds(1),
                Duration.ofSeconds(30), 1.5);
    }

    /** Ticks a second at a time until the target's turn came, answering every other member's PING. */
    private void untilTurn(GroupMembership membership, HolderProber prober, PeerId target) {
        prober.onPing = (t, n) -> {
            if (!t.equals(target)) {
                membership.onAck(n, t);
            }
        };
        for (int i = 0; i < 200; i++) {
            long before = framesFor(prober, target);
            membership.tick(prober, true);
            if (framesFor(prober, target) > before) {
                return;
            }
            clock.advance(Duration.ofSeconds(1));
        }
        throw new AssertionError("the target's turn never came");
    }

    private static long framesFor(HolderProber prober, PeerId target) {
        return prober.pings.stream().filter(target::equals).count()
                + prober.reqs.stream().filter(r -> r[1].equals(target)).count();
    }

    /** TR-1, TR-2: a target only its holders reach gets no direct PING; its holders are asked at once. */
    @Test
    void aTargetWithLinkHoldersIsAskedThroughThemAndNeverPinged() {
        GroupMembership membership = membership(relayConfig());
        List<PeerId> members = admitMany(membership, 4);
        PeerId thin = members.get(0);
        PeerId seed = members.get(1);
        HolderProber prober = new HolderProber();
        prober.link.put(thin, List.of(seed));
        prober.declared.put(thin, List.of(seed));
        untilTurn(membership, prober, thin);
        assertThat(prober.pings).doesNotContain(thin);
        assertThat(prober.reqsTo(seed, thin)).isEqualTo(1);
        assertThat(prober.reqs).as("only the holder").hasSize(1);
        assertThat(membership.member(thin)).hasValueSatisfying(m -> assertThat(m.suspect()).isFalse());
        GroupMembership.ProbeStats stats = membership.probeStats();
        assertThat(stats.holderRounds()).isEqualTo(1);
        assertThat(stats.holderPingReqs()).isEqualTo(1);
        assertThat(stats.holderPingReqsByRelay()).containsEntry(seed, 1L);

        // the holder brings the target's ACK: liveness without contact, so the next turn asks the holder again
        membership.onAck(prober.reqNonces.get(0), seed);
        assertThat(membership.probesPending()).isFalse();
        prober.reqs.clear();
        prober.reqNonces.clear();
        untilTurn(membership, prober, thin);
        assertThat(prober.reqsTo(seed, thin)).as("not rested: the relayed ACK is no contact").isEqualTo(1);
        assertThat(membership.probesRested()).isZero();
    }

    /** TR-5: a target that names no holder in view is probed directly, as before. */
    @Test
    void aTargetWithNoHolderInViewIsPingedDirectly() {
        GroupMembership membership = membership(relayConfig());
        List<PeerId> members = admitMany(membership, 3);
        PeerId target = members.get(0);
        HolderProber prober = new HolderProber();
        prober.link.put(target, List.of(PeerIdentity.generate().peerId())); // a holder not in this view
        untilTurn(membership, prober, target);
        assertThat(prober.pings).contains(target);
        assertThat(prober.reqs).isEmpty();
        assertThat(membership.probeStats().holderRounds()).isZero();
    }

    /** Owner answer 11: a failed holder round escalates to holders plus random members before any drop. */
    @Test
    void aFailedHolderRoundEscalatesToHoldersAndRandomMembersBeforeADrop() {
        GroupMembership membership = membership(relayConfig());
        List<PeerId> members = admitMany(membership, 5);
        PeerId thin = members.get(0);
        PeerId seed = members.get(1);
        HolderProber prober = new HolderProber();
        prober.link.put(thin, List.of(seed));
        prober.declared.put(thin, List.of(seed));
        untilTurn(membership, prober, thin);
        assertThat(prober.reqs).hasSize(1);
        clock.advance(Duration.ofMillis(2_001));
        membership.tick(prober, true); // the holder round timed out
        assertThat(membership.member(thin)).hasValueSatisfying(m -> assertThat(m.suspect()).isTrue());
        List<PeerId[]> second = prober.reqs.subList(1, prober.reqs.size());
        assertThat(second).as("the holder again, and two random members").hasSize(3);
        assertThat(second.get(0)[0]).as("the holder first").isEqualTo(seed);
        assertThat(second.stream().map(r -> r[0]).toList()).doesNotHaveDuplicates().doesNotContain(thin, self);
        assertThat(membership.probeStats().escalations()).isEqualTo(1);
        assertThat(membership.probeStats().randomPingReqs()).isEqualTo(2);

        // a random relay of the second round vouches: kept
        long nonce = prober.reqNonces.get(1);
        membership.onAck(nonce, second.get(2)[0]);
        assertThat(membership.member(thin)).hasValueSatisfying(m -> assertThat(m.suspect()).isFalse());

        // the next holder round and its escalation both fail: dropped, counted as a drop after a holder round
        prober.reqs.clear();
        prober.reqNonces.clear();
        untilTurn(membership, prober, thin);
        clock.advance(Duration.ofMillis(2_001));
        membership.tick(prober, true);
        clock.advance(Duration.ofMillis(2_001));
        membership.tick(prober, true);
        assertThat(membership.member(thin)).isEmpty();
        assertThat(membership.probeStats().dropsAfterHolderRound()).isEqualTo(1);
        assertThat(membership.probeStats().dropsAfterIndirectRound()).isZero();
        assertThat(membership.probeStats().holderPingReqsByRelay()).as("the seed is still in view").containsKey(seed);
    }

    /** TR-2: a reachable member whose direct PING failed is asked of its declared holders first, then random ones. */
    @Test
    void anEscalationFromAFailedPingAsksTheDeclaredHoldersFirst() {
        GroupMembership membership = membership(relayConfig());
        List<PeerId> members = admitMany(membership, 5);
        PeerId target = members.get(0);
        PeerId holder = members.get(1);
        HolderProber prober = new HolderProber();
        prober.declared.put(target, List.of(holder)); // reachable directly: no link holders
        untilTurn(membership, prober, target);
        assertThat(prober.pings).contains(target);
        clock.advance(Duration.ofMillis(2_001));
        membership.tick(prober, true);
        assertThat(prober.reqs).as("the holder, then one random member up to two").hasSize(2);
        assertThat(prober.reqs.get(0)[0]).isEqualTo(holder);
        assertThat(prober.reqs.get(1)[0]).isNotIn(holder, target, self);
        assertThat(membership.probeStats().escalations()).as("not a holder round's escalation").isZero();
    }

    /** A routine holder round is no doubt: the quiet stretch keeps growing through it. */
    @Test
    void aHolderRoundKeepsTheQuietStretch() {
        GroupMembership membership = membership(relayConfig());
        List<PeerId> members = admitMany(membership, 9);
        PeerId thin = members.get(0);
        PeerId seed = members.get(1);
        HolderProber prober = new HolderProber();
        prober.link.put(thin, List.of(seed));
        prober.declared.put(thin, List.of(seed));
        prober.onPing = (t, n) -> membership.onAck(n, t);
        int answered = 0;
        for (int second = 0; second < 300; second++) {
            membership.tick(prober, true);
            while (answered < prober.reqNonces.size()) {
                membership.onAck(prober.reqNonces.get(answered++), seed); // the holder answers in time
            }
            clock.advance(Duration.ofSeconds(1));
        }
        assertThat(membership.probeStats().holderRounds()).isGreaterThanOrEqualTo(5);
        assertThat(membership.currentProbeIntervalMillis()).as("stretched to the quiet ceiling at ten members")
                .isEqualTo(membership.maxQuietIntervalMillis(9));
        assertThat(membership.allMembers()).hasSize(9);
    }

    /** TR-1: holders are filtered (self, the target, non-members, repeats) and capped at indirectPeers. */
    @Test
    void holdersAreFilteredAndCapped() {
        GroupMembership membership = membership(relayConfig());
        List<PeerId> members = admitMany(membership, 5);
        PeerId thin = members.get(0);
        List<PeerId> named = new ArrayList<>();
        named.add(self);
        named.add(thin);
        named.add(PeerIdentity.generate().peerId());
        named.add(members.get(1));
        named.add(members.get(1));
        named.add(members.get(2));
        named.add(members.get(3));
        for (int i = 0; i < 1_000; i++) {
            named.add(PeerId.of("z" + i));
        }
        HolderProber prober = new HolderProber();
        prober.link.put(thin, named);
        untilTurn(membership, prober, thin);
        assertThat(prober.reqs.stream().map(r -> r[0]).toList()).containsExactly(members.get(1), members.get(2));
    }
}
