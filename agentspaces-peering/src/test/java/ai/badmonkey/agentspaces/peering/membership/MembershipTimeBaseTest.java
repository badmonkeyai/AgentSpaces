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
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The contract between the membership view and the time it is given (srad/agentspaces-event-loop.md §3.6, EL-C-04,
 * EL-C-14): given a time source, every period is reckoned on its monotonic base, so a wall step neither expires a member
 * early nor keeps one late; given a plain clock, on that clock, as hand-driven tests expect; and a requested resume
 * probe asks every member once, at the next turn.
 */
class MembershipTimeBaseTest {

    private final PeerId self = PeerIdentity.generate().peerId();
    private final GroupId group = GroupId.of("zTimeBase");

    private static final class Pings implements GroupMembership.Prober {
        final List<PeerId> pinged = new ArrayList<>();

        @Override
        public void ping(PeerId target, long nonce) {
            pinged.add(target);
        }

        @Override
        public void pingReq(PeerId relay, PeerId target, long nonce) {
        }
    }

    private PeerId admit(GroupMembership membership, InstantSource clock) {
        PeerId peer = PeerIdentity.generate().peerId();
        membership.onPeerAdvertisement(new PeerAdvertisement("aspace://" + group.value() + "/peer/" + peer.value(),
                peer, group, clock.instant(), Duration.ofMinutes(10),
                List.of(new PeerAdvertisement.Endpoint("mem", "x", 0)), Set.of(), Map.of()));
        return peer;
    }

    /** Probes only after a long interval, so nothing but the TTL judges the members here. */
    private static GroupMembership.Config quietConfig() {
        return new GroupMembership.Config(Duration.ofSeconds(45), Duration.ofSeconds(1), 2, Duration.ofSeconds(60));
    }

    @Test
    void onATimeSourceAWallStepNeitherExpiresAMemberEarlyNorKeepsOneLate() {
        VirtualTime time = VirtualTime.create();
        GroupMembership membership = new GroupMembership(self, time, new Random(3), quietConfig());
        PeerId a = admit(membership, time);
        Pings prober = new Pings();

        time.stepWall(Duration.ofMinutes(4)); // a forward step longer than the TTL
        membership.tick(prober, true);
        assertThat(membership.member(a)).as("not expired by a forward wall step").isPresent();

        time.stepWall(Duration.ofMinutes(-10)); // a back step
        time.advance(Duration.ofMillis(membership.memberTtlMillis() + 1));
        membership.tick(prober, true);
        assertThat(membership.member(a)).as("expired on time after a back step, one TTL on the monotonic clock")
                .isEmpty();
        assertThat(membership.localMillis()).isEqualTo(time.monoMillis());
    }

    @Test
    void onAPlainClockEveryPeriodFollowsThatClock() {
        TestClock clock = TestClock.create();
        GroupMembership membership = new GroupMembership(self, clock, new Random(3), quietConfig());
        PeerId a = admit(membership, clock);
        clock.advance(Duration.ofMillis(membership.memberTtlMillis() + 1));
        membership.tick(new Pings(), true);
        assertThat(membership.member(a)).as("a hand-driven test that moves its clock moves the TTL").isEmpty();
        assertThat(membership.localMillis()).isEqualTo(clock.millis());
    }

    @Test
    void aResumeProbeAsksEveryMemberOnceAtTheNextTurn() {
        VirtualTime time = VirtualTime.create();
        GroupMembership membership = new GroupMembership(self, time, new Random(3), quietConfig());
        List<PeerId> members = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            members.add(admit(membership, time));
        }
        Pings prober = new Pings();
        membership.tick(prober, true); // the first turn probes one member
        assertThat(prober.pinged).hasSize(1);
        PeerId underProbe = prober.pinged.get(0);
        prober.pinged.clear();
        membership.tick(prober, true);
        assertThat(prober.pinged).as("not a probe turn yet").isEmpty();

        membership.probeEachOnce();
        membership.tick(prober, true);
        List<PeerId> others = new ArrayList<>(members);
        others.remove(underProbe);
        assertThat(prober.pinged).as("every member not already under a probe, once")
                .containsExactlyInAnyOrderElementsOf(others);
        assertThat(membership.resumeProbes()).isEqualTo(4);
        prober.pinged.clear();
        membership.tick(prober, true);
        assertThat(prober.pinged).as("once, not every turn").isEmpty();
    }
}
