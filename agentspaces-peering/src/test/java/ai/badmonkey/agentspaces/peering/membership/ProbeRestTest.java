/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A probe rests when the member's own verified frame reached this node within the probe period and the probe would
 * carry nothing else (idle-loop O6). A member that falls silent is still probed, suspected and dropped as before.
 */
class ProbeRestTest {

    private static final Duration INTERVAL = Duration.ofSeconds(1);
    private static final Duration PING_TIMEOUT = Duration.ofSeconds(1);
    private final TestClock clock = TestClock.create();
    private final PeerId self = PeerIdentity.generate().peerId();
    private final GroupId group = GroupId.of("zProbeRest");
    private final GroupMembership membership = new GroupMembership(self, clock, new Random(3),
            new GroupMembership.Config(Duration.ofMinutes(5), PING_TIMEOUT, 2, INTERVAL));

    /** Pings answered or not, per the test; news per the test. */
    private final class Prober implements GroupMembership.Prober {
        final List<PeerId> pinged = new ArrayList<>();
        boolean answer = true;
        boolean news;

        @Override
        public void ping(PeerId target, long nonce) {
            pinged.add(target);
            if (answer) {
                membership.onAck(nonce, target);
            }
        }

        @Override
        public void pingReq(PeerId relay, PeerId target, long nonce) {
        }

        @Override
        public boolean carriesNews(PeerId target, long sinceMillis) {
            return news;
        }
    }

    private PeerId admit() {
        PeerId peer = PeerIdentity.generate().peerId();
        membership.onPeerAdvertisement(new PeerAdvertisement("aspace://" + group.value() + "/peer/" + peer.value(),
                peer, group, clock.instant(), Duration.ofMinutes(10),
                List.of(new PeerAdvertisement.Endpoint("mem", "x", 0)), Set.of(), Map.of()));
        return peer;
    }

    /** One 250 ms tick at the base interval (active, so the period never stretches). */
    private void tick(Prober prober) {
        membership.tick(prober, false);
        clock.advance(Duration.ofMillis(250));
    }

    @Test
    void aMemberHeardFromEveryPeriodIsNeverProbed() {
        PeerId member = admit();
        Prober prober = new Prober();
        for (int i = 0; i < 80; i++) { // 20 s
            if (i % 2 == 0) {
                membership.recordHeard(member); // its own frame every 500 ms, a probe of ours for example
            }
            tick(prober);
        }
        assertThat(prober.pinged).as("only the first turn, which has no previous turn to measure from").hasSizeLessThanOrEqualTo(1);
        assertThat(membership.probesRested()).isGreaterThanOrEqualTo(15);
    }

    @Test
    void aSilentMemberIsProbedEveryPeriodAsBefore() {
        PeerId member = admit();
        Prober prober = new Prober();
        for (int i = 0; i < 80; i++) {
            tick(prober);
        }
        // an ACK answers this node's own probe: liveness, not contact, so no turn rests and every period probes
        assertThat(prober.pinged).hasSizeBetween(18, 21).containsOnly(member);
        assertThat(membership.probesRested()).isZero();
    }

    @Test
    void twoMembersWhoseTurnsFallTogetherStillRestHalfTheirProbes() {
        // a real lock-step pair: two views, each probe delivered to the other side at once, both ticked at one instant
        PeerId other = PeerIdentity.generate().peerId();
        GroupMembership mine = membership;
        GroupMembership theirs = new GroupMembership(other, clock, new Random(5),
                new GroupMembership.Config(Duration.ofMinutes(5), PING_TIMEOUT, 2, INTERVAL));
        mine.onPeerAdvertisement(new PeerAdvertisement("aspace://" + group.value() + "/peer/" + other.value(),
                other, group, clock.instant(), Duration.ofMinutes(10),
                List.of(new PeerAdvertisement.Endpoint("mem", "x", 0)), Set.of(), Map.of()));
        theirs.onPeerAdvertisement(new PeerAdvertisement("aspace://" + group.value() + "/peer/" + self.value(),
                self, group, clock.instant(), Duration.ofMinutes(10),
                List.of(new PeerAdvertisement.Endpoint("mem", "x", 0)), Set.of(), Map.of()));
        int[] pings = new int[2];
        GroupMembership.Prober fromMine = new GroupMembership.Prober() {
            @Override public void ping(PeerId target, long nonce) {
                pings[0]++;
                theirs.recordHeard(self); // its PING reaches the other side: contact there
                mine.onAck(nonce, other);  // and is answered
            }
            @Override public void pingReq(PeerId relay, PeerId target, long nonce) { }
        };
        GroupMembership.Prober fromTheirs = new GroupMembership.Prober() {
            @Override public void ping(PeerId target, long nonce) {
                pings[1]++;
                mine.recordHeard(other);
                theirs.onAck(nonce, self);
            }
            @Override public void pingReq(PeerId relay, PeerId target, long nonce) { }
        };
        for (int i = 0; i < 80; i++) { // 20 s, 20 turns each
            mine.tick(fromMine, false);
            theirs.tick(fromTheirs, false);
            clock.advance(Duration.ofMillis(250));
        }
        assertThat(pings[0] + pings[1]).as("the pair's probes over 20 turns each: half rest").isBetween(18, 23);
        assertThat(mine.member(other)).isPresent();
        assertThat(theirs.member(self)).isPresent();
    }

    @Test
    void aProbeWithNewsToCarryIsSentEvenToAMemberJustHeard() {
        PeerId member = admit();
        Prober prober = new Prober();
        prober.news = true;
        for (int i = 0; i < 40; i++) {
            membership.recordHeard(member);
            tick(prober);
        }
        assertThat(prober.pinged).hasSizeGreaterThanOrEqualTo(9);
        assertThat(membership.probesRested()).isZero();
    }

    @Test
    void aMemberThatFallsSilentAfterContactIsDroppedWithinTheUsualBound() {
        PeerId member = admit();
        Prober prober = new Prober();
        for (int i = 0; i < 20; i++) {
            membership.recordHeard(member);
            tick(prober);
        }
        prober.answer = false; // it dies: no frame, no ACK
        long diedAt = clock.millis();
        int ticks = 0;
        while (membership.member(member).isPresent() && ticks < 200) {
            tick(prober);
            ticks++;
        }
        long detected = clock.millis() - diedAt;
        assertThat(membership.member(member)).isEmpty();
        // one period before the rest ends, then the direct and the indirect timeouts (no relay here: dropped at once)
        assertThat(detected).isLessThanOrEqualTo(INTERVAL.toMillis() * 2 + PING_TIMEOUT.toMillis() * 2 + 500);
    }

    /**
     * A member that leaves and comes back between two drains is two departures, not none, and the probes awaiting its
     * answer leave with it, so a re-admission is not undone by a probe of its earlier stay (HorizonLaggingCopyTest,
     * 2026-11-18: a loss never recorded was never retried, and the fleet stayed split).
     */
    @Test
    void everyDepartureIsDrainedAndTakesItsPendingProbesWithIt() {
        PeerId member = admit();
        Prober prober = new Prober();
        prober.answer = false;
        tick(prober); // a probe to the member, unanswered
        assertThat(membership.probesPending()).isTrue();
        membership.evict(member);
        assertThat(membership.probesPending()).as("the dropped member's probe went with it").isFalse();

        PeerId other = admit();
        for (int i = 0; i < 40 && membership.member(other).isPresent(); i++) {
            tick(prober); // unanswered, and no relay to ask: dropped when the direct probe times out
        }
        assertThat(membership.member(other)).isEmpty();
        membership.onPeerAdvertisement(new PeerAdvertisement("aspace://" + group.value() + "/peer/" + other.value(),
                other, group, clock.instant(), Duration.ofMinutes(10),
                List.of(new PeerAdvertisement.Endpoint("mem", "x", 0)), Set.of(), Map.of()));
        assertThat(membership.member(other)).as("back from a fresh card").isPresent();
        assertThat(membership.drainDeparted()).as("every departure, each once, though the member is back")
                .contains(member, other);
        assertThat(membership.drainDeparted()).isEmpty();
    }
}
