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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SWIM probe integrity (ASF-011): liveness credit goes only to the probe's own
 * target, only on an ACK from a party this node actually asked, under nonces an
 * attacker cannot predict. A member spraying forged ACKs can neither keep a
 * dead peer alive nor cancel probes it never saw.
 */
class ProbeForgeryTest {

    private final TestClock clock = TestClock.create();
    private final PeerId self = PeerIdentity.generate().peerId();
    private final PeerId target = PeerIdentity.generate().peerId();
    private final PeerId attacker = PeerIdentity.generate().peerId();
    private final GroupId group = GroupId.of("zProbe");
    private final GroupMembership membership = new GroupMembership(
            self, clock, new Random(11),
            new GroupMembership.Config(Duration.ofMinutes(5), Duration.ofSeconds(1), 2));

    private void admit(PeerId peer) {
        membership.onPeerAdvertisement(new PeerAdvertisement(
                "aspace://" + group.value() + "/peer/" + peer.value(), peer, group,
                clock.instant(), Duration.ofMinutes(10),
                List.of(new PeerAdvertisement.Endpoint("mem", "x", 0)),
                Set.of(), Map.of()));
    }

    @Test
    void aForgedAckFromAPeerNeverAskedNeitherClearsTheProbeNorCreditsLiveness() {
        // Only the target is a member, so no relay is ever asked: the attacker
        // is a party this node never spoke to for this probe. (An asked relay
        // may vouch — that is SWIM's indirect-probe trust model by design; the
        // fix is that NOBODY ELSE can.)
        admit(target);
        List<Long> nonces = new ArrayList<>();
        GroupMembership.Prober prober = new GroupMembership.Prober() {
            @Override
            public void ping(PeerId to, long nonce) {
                nonces.add(nonce);
            }

            @Override
            public void pingReq(PeerId relay, PeerId to, long nonce) {
                nonces.add(nonce);
            }
        };

        // Drive ticks until the target is probed, spraying forged ACKs from the
        // attacker for every real nonce: none may count, so the silent target
        // is evicted exactly as if no ACK ever arrived.
        for (int i = 0; i < 20 && membership.member(target).isPresent(); i++) {
            membership.tick(prober);
            for (long nonce : List.copyOf(nonces)) {
                membership.onAck(nonce, attacker);
            }
            clock.advance(Duration.ofSeconds(2));
        }
        assertThat(membership.member(target))
                .as("the silent target is evicted despite the forged-ACK spray")
                .isEmpty();
    }

    @Test
    void anHonestAckFromTheProbedTargetStillCounts() {
        admit(target);
        List<Long> nonces = new ArrayList<>();
        GroupMembership.Prober prober = new GroupMembership.Prober() {
            @Override
            public void ping(PeerId to, long nonce) {
                nonces.add(nonce);
            }

            @Override
            public void pingReq(PeerId relay, PeerId to, long nonce) {
                nonces.add(nonce);
            }
        };
        for (int i = 0; i < 10 && nonces.isEmpty(); i++) {
            membership.tick(prober);
        }
        assertThat(nonces).isNotEmpty();
        membership.onAck(nonces.get(nonces.size() - 1), target);

        // Liveness credited: the target survives many probe rounds because each
        // real ACK we simulate comes from the target itself.
        for (int i = 0; i < 10; i++) {
            clock.advance(Duration.ofSeconds(2));
            membership.tick(prober);
            membership.onAck(nonces.get(nonces.size() - 1), target);
        }
        assertThat(membership.member(target)).isPresent();
    }

    /**
     * The targeted relay (srad/agentspaces-event-loop.md §3.12, TR-3): an ACK on a holder round's nonce counts only
     * from a holder this node asked (or the target itself); a member that was not asked credits nothing, and the
     * target lapses as if no ACK arrived.
     */
    @Test
    void anAckOnAHolderRoundFromAMemberNotAskedCreditsNothing() {
        PeerId holder = PeerIdentity.generate().peerId();
        GroupMembership view = new GroupMembership(self, clock, new Random(11), new GroupMembership.Config(
                Duration.ofSeconds(45), Duration.ofSeconds(1), 2, Duration.ofSeconds(1), Duration.ofSeconds(30), 1.5));
        for (PeerId peer : List.of(target, holder, attacker)) {
            view.onPeerAdvertisement(new PeerAdvertisement(
                    "aspace://" + group.value() + "/peer/" + peer.value(), peer, group,
                    clock.instant(), Duration.ofMinutes(10), List.of(), Set.of(), Map.of()));
        }
        Map<Long, Set<PeerId>> askedUnder = new java.util.HashMap<>();
        GroupMembership.Prober prober = new GroupMembership.Prober() {
            @Override
            public void ping(PeerId to, long nonce) {
                if (!to.equals(target)) {
                    view.onAck(nonce, to); // the holder and the attacker answer their own probes
                }
            }

            @Override
            public void pingReq(PeerId relay, PeerId to, long nonce) {
                if (to.equals(target)) {
                    askedUnder.computeIfAbsent(nonce, n -> new java.util.HashSet<>()).add(relay);
                }
            }

            @Override
            public java.util.Collection<PeerId> linkHolders(PeerId to) {
                return to.equals(target) ? List.of(holder) : List.of();
            }

            @Override
            public java.util.Collection<PeerId> declaredHolders(PeerId to) {
                return linkHolders(to);
            }
        };
        for (int i = 0; i < 120 && view.member(target).isPresent(); i++) {
            view.tick(prober, true);
            askedUnder.forEach((nonce, relays) -> {
                if (!relays.contains(attacker)) {
                    view.onAck(nonce, attacker); // a member, but not asked under this nonce
                }
            });
            clock.advance(Duration.ofSeconds(1));
        }
        assertThat(askedUnder.values()).as("routine rounds asked the holder alone")
                .anyMatch(relays -> relays.equals(Set.of(holder)));
        assertThat(view.member(target)).as("the forged vouching kept nobody").isEmpty();
        assertThat(view.member(holder)).isPresent();
        assertThat(view.probeStats().dropsAfterHolderRound()).isEqualTo(1);
    }
}
