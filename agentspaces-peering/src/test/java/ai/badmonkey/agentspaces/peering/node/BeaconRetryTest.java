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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.spi.BeaconHint;
import ai.badmonkey.agentspaces.api.spi.BeaconRequest;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.bootstrap.MulticastBeacon;
import ai.badmonkey.agentspaces.peering.bootstrap.MulticastLocator;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The beacon's retries (srad/agentspaces-event-loop.md EL-O-11, the owner's rule of 2026-11-22): a fixed table, 1 min,
 * 2 min, 10 min, 20 min, read by one position per beacon and wrapped back to 1 min; a success resets the position; the
 * hints a node learns from datagrams outlive the longest interval by about 1.5 times; the loop's stretch never moves
 * the beacon. Everything runs on {@link VirtualTime}, so 36 minutes of beaconing cost no wall time.
 */
@Timeout(120)
class BeaconRetryTest {

    private static final Duration TICK = Duration.ofMillis(250);
    private static final long MINUTE_MILLIS = 60_000L;

    private final SimNetwork network = new SimNetwork();
    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-22T00:00:00Z"));
    private final Air air = new Air();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zBeaconRetry");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zBeaconRetry",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
            "beacon-retry", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    /** One datagram on the air: who sent it, when (monotonic ms), and its bytes. */
    private record Sent(String from, long atMillis, byte[] datagram) {
    }

    /** An in-memory datagram medium: every endpoint hears every other's sends, on the sender's thread. */
    private final class Air {
        private final List<Endpoint> endpoints = new CopyOnWriteArrayList<>();
        private final List<Sent> sent = new CopyOnWriteArrayList<>();

        Endpoint endpoint(String name) {
            Endpoint endpoint = new Endpoint(name);
            endpoints.add(endpoint);
            return endpoint;
        }

        List<Long> sendTimes(String from) {
            return sent.stream().filter(s -> s.from().equals(from)).map(Sent::atMillis).toList();
        }

        Sent last(String from) {
            List<Sent> mine = sent.stream().filter(s -> s.from().equals(from)).toList();
            return mine.get(mine.size() - 1);
        }

        final class Endpoint implements MulticastBeacon.Datagrams {
            private final String name;
            private volatile Consumer<byte[]> receiver = d -> {
            };

            Endpoint(String name) {
                this.name = name;
            }

            @Override
            public void send(byte[] datagram) {
                sent.add(new Sent(name, time.monoMillis(), datagram));
                for (Endpoint other : endpoints) {
                    if (other != this) {
                        other.receiver.accept(datagram.clone());
                    }
                }
            }

            @Override
            public void onReceive(Consumer<byte[]> receiver) {
                this.receiver = Objects.requireNonNull(receiver);
            }

            @Override
            public void close() {
                endpoints.remove(this);
            }
        }
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    /** A node on the virtual time with the beacon at its default first interval (1 min), its loop started. */
    private PeerNode beaconing(String address, long seed) throws IOException {
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(time).time(time).randomSeed(seed)
                .beacon(air.endpoint(address), MulticastBeacon.FIRST_RETRY).build();
        node.listen(network.register(address), address);
        node.startTicking(TICK);
        nodes.add(node);
        return node;
    }

    private GroupRuntime join(PeerNode node) {
        return node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
    }

    private static List<Long> gapsInMinutes(List<Long> times) {
        List<Long> gaps = new ArrayList<>();
        for (int i = 1; i < times.size(); i++) {
            long gap = times.get(i) - times.get(i - 1);
            assertThat(gap % MINUTE_MILLIS).as("every gap is whole minutes on virtual time: %s", times).isZero();
            gaps.add(gap / MINUTE_MILLIS);
        }
        return gaps;
    }

    /** EL-O-11: 1, 2, 10, 20 minutes by position, then from the top again; the position cycles 0 to 3. */
    @Test
    void theBeaconRetriesOnTheTableAndWrapsBackToOneMinuteAfterTwenty() throws Exception {
        assertThat(MulticastBeacon.RETRY_INTERVALS).as("the static table")
                .containsExactly(Duration.ofMinutes(1), Duration.ofMinutes(2), Duration.ofMinutes(10),
                        Duration.ofMinutes(20));
        PeerNode a = beaconing("a", 1);
        time.advance(Duration.ofMinutes(2));
        assertThat(air.sendTimes("a")).as("no group joined: nothing announced and no retry spent").isEmpty();
        assertThat(a.beacon().position()).isZero();

        join(a);
        time.runDue();
        assertThat(air.sendTimes("a")).as("a join announces at once").hasSize(1);
        assertThat(a.beacon().position()).isEqualTo(1);

        time.advance(Duration.ofMinutes(36).plusSeconds(30)); // sends at 0, 1, 3, 13, 33, 34 and 36 min
        assertThat(gapsInMinutes(air.sendTimes("a"))).as("the table, wrapped once")
                .containsExactly(1L, 2L, 10L, 20L, 1L, 2L);
        assertThat(a.beacon().position()).as("seven retries read 0,1,2,3,0,1,2").isEqualTo(3);
        long last = air.last("a").atMillis();
        assertThat(a.loop().dueOf("beacon") / 1_000_000L - last).as("the next is 10 min out")
                .isEqualTo(10 * MINUTE_MILLIS);
        assertThat(a.loopStats().participantThrows()).isEmpty();
    }

    /**
     * EL-O-11: a success (a sender the node had no live entry for) resets the position to 0; the retry already armed
     * keeps its time and the wait after it is 1 min again. A sender it already knows, with the same endpoints, resets
     * nothing.
     */
    @Test
    void aSuccessResetsThePositionAndAKnownSenderDoesNot() throws Exception {
        PeerNode a = beaconing("a", 1);
        PeerNode b = beaconing("b", 2);
        join(a);
        time.runDue();
        long t0 = air.sendTimes("a").get(0);
        time.advance(Duration.ofMinutes(4)); // a's sends at 0, 1 and 3 min: the next reads position 3
        assertThat(a.beacon().position()).isEqualTo(3);
        long armed = a.loop().dueOf("beacon");

        join(b); // b announces at once; a has never heard it
        time.runDue();
        assertThat(a.beacon().position()).as("a new sender heard: the position is reset").isZero();
        assertThat(a.loop().dueOf("beacon")).as("the armed retry keeps its time (13 min)").isEqualTo(armed);
        assertThat(a.beaconLearned(groupId).peers()).isEqualTo(1);

        time.advance(Duration.ofMinutes(17)); // to t0 + 21 min
        List<Long> aTimes = air.sendTimes("a").stream().map(t -> (t - t0) / MINUTE_MILLIS).toList();
        assertThat(aTimes).as("13 min as armed, then 1 min (position 0), then 2 min").containsExactly(0L, 1L, 3L,
                13L, 14L, 16L);
        assertThat(air.sendTimes("b").stream().filter(t -> t - t0 > 16 * MINUTE_MILLIS).count())
                .as("b announced again after a's 16-min retry").isPositive();
        assertThat(a.beacon().position()).as("b's later datagrams name a sender a knows: no reset").isEqualTo(3);
        assertThat(a.loop().dueOf("beacon") / 1_000_000L - (t0 + 16 * MINUTE_MILLIS))
                .as("so a's next waits 10 min").isEqualTo(10 * MINUTE_MILLIS);
    }

    /**
     * EL-O-11 and the TTL rule: a hint learned from a datagram is held about 1.5 times the longest interval (30 min),
     * so across a sender's 20-minute step it stays alive even though the advertisement it came in (10 min) expired.
     */
    @Test
    void learnedHintsStayAliveAcrossTheSendersTwentyMinuteStep() throws Exception {
        PeerNode a = beaconing("a", 1);
        PeerNode b = beaconing("b", 2);
        join(a);
        time.runDue();
        time.advance(Duration.ofSeconds(30));
        join(b);
        time.runDue();
        // Find b's 20-minute step: the retry whose next deadline is 20 min after it.
        long stepFrom = -1;
        for (int i = 0; i < 80 && stepFrom < 0; i++) {
            time.advance(Duration.ofSeconds(30));
            long lastB = air.last("b").atMillis();
            if (b.loop().dueOf("beacon") / 1_000_000L - lastB == 20 * MINUTE_MILLIS) {
                stepFrom = lastB;
            }
        }
        assertThat(stepFrom).as("b reached its 20-minute step within 40 min").isPositive();
        long nextB = stepFrom + 20 * MINUTE_MILLIS;
        time.advanceTo((nextB - 30_000L) * 1_000_000L); // 30 s before b's next datagram

        PeerNode.SignedPeerAd signed = CborCodec.defaultCodec().fromBytes(air.last("b").datagram(),
                PeerNode.SignedPeerAd.class);
        PeerAdvertisement adB = CborCodec.defaultCodec().fromBytes(signed.adBytes(), PeerAdvertisement.class);
        assertThat(adB.issued().plus(adB.ttl())).as("the control: b's last advertisement has expired")
                .isBefore(time.instant());
        PeerNode.BeaconLearned learned = a.beaconLearned(groupId);
        assertThat(learned.peers()).as("a still holds what b's beacon taught it").isEqualTo(1);
        assertThat(learned.ttl()).as("for about 10 more minutes (30 min hold, 20 min gone)")
                .isBetween(Duration.ofMinutes(9), Duration.ofMinutes(11));
        BeaconHint hint = new MulticastLocator(a).locate(new BeaconRequest("lan", Optional.of(groupId), "mem",
                Set.of("mem")));
        assertThat(hint.endpoints()).extracting(PeerAdvertisement.Endpoint::address).containsExactly("b");
        assertThat(a.beacon().learnedHold()).isEqualTo(Duration.ofMinutes(30));

        time.advance(Duration.ofMinutes(1)); // b's datagram renews the hold
        assertThat(air.last("b").atMillis()).isEqualTo(nextB);
        assertThat(a.beaconLearned(groupId).ttl()).isGreaterThan(Duration.ofMinutes(29));
    }

    /** EL-O-11: the beacon takes no part in the loop's stretch; under ×4 its next retry is still the table's. */
    @Test
    void theStretchNeverMovesTheBeacon() throws Exception {
        PeerNode a = beaconing("a", 1);
        join(a);
        time.runDue();
        assertThat(a.loop().stats().stretchable()).as("the beacon is not a stretching participant")
                .doesNotContain("beacon").contains("zBeaconRetry/lost");
        time.advance(Duration.ofSeconds(59));
        a.loop().requestStretch(4);
        time.runDue();
        assertThat(a.loop().stretch()).isEqualTo(4);

        time.advance(Duration.ofSeconds(2)); // the 1-min retry falls due under the stretch
        assertThat(a.loop().stretch()).as("the stretch is still in force").isEqualTo(4);
        assertThat(gapsInMinutes(air.sendTimes("a"))).as("1 min, not 4").containsExactly(1L);
        long last = air.last("a").atMillis();
        assertThat(a.loop().dueOf("beacon") / 1_000_000L - last).as("the next is 2 min out, not 8")
                .isEqualTo(2 * MINUTE_MILLIS);
        time.advance(Duration.ofMinutes(2));
        assertThat(gapsInMinutes(air.sendTimes("a"))).containsExactly(1L, 2L);
    }
}
