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
package ai.badmonkey.agentspaces.capabilities.orderedlog;

import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.spi.DueWork;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Raft on its own deadlines on a started loop (srad/agentspaces-event-loop.md EL-T-02, EL-T-14, EL-H-09; owner
 * answers 2 and 4, 2026-11-22). Three members, each on its own loop on one virtual time, the logs registered on their
 * capability runtimes, nobody calling {@code tick()}: the election timeout is 1.25 to 2.25 s of silence, the leader's
 * heartbeats are 250 ms apart, its lease card is republished at about half its span rather than every tick, a
 * {@link RaftLog#stepDown()} makes the next card a follower's, and a leader whose loop is wedged steps down through its
 * lease guard a minimum election timeout after its last heartbeat, or when its last card's lease ends if that is sooner
 * (EL-SP-24's bound).
 */
@Timeout(180)
class RaftDueTest {

    private static final Duration TICK = Duration.ofMillis(250);
    private static final long MS = 1_000_000L;

    private final SimNetwork network = new SimNetwork();
    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-22T00:00:00Z"));
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zRaftDue");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zRaftDue",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "raft-due",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final List<PeerNode> nodes = new ArrayList<>();

    private record Member(PeerNode node, GroupRuntime runtime, RaftLog raft, CapabilityRuntime capabilities) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private List<Member> cluster(int size) throws Exception {
        return cluster(size, TICK);
    }

    private List<Member> cluster(int size, Duration tick) throws Exception {
        List<PeerIdentity> identities = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            identities.add(PeerIdentity.generate());
        }
        List<PeerId> ids = identities.stream().map(PeerIdentity::peerId).toList();
        List<Member> members = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            PeerNode node = PeerNode.builder(identities.get(i)).clock(time).time(time).randomSeed(100 + i).build();
            node.listen(network.register("n" + i), "n" + i);
            nodes.add(node);
            GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                    i == 0 ? List.of() : List.of(new PeerAdvertisement.Endpoint("mem", "n0", 0)));
            RaftLog raft = new RaftLog(new CapabilityPipes(runtime, codec), codec, ids.get(i), ids, time, 1000 + i,
                    (command, index) -> { });
            DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, time), codec, ids.get(i));
            members.add(new Member(node, runtime, raft, new CapabilityRuntime(runtime, discovery, identities.get(i))));
            node.startTicking(tick);
        }
        long waited = 0;
        while (!members.stream().allMatch(m -> m.runtime().membership().allMembers().size() == size - 1)) {
            time.advance(TICK);
            waited += TICK.toMillis();
            assertThat(waited).as("the view forms").isLessThan(60_000);
        }
        members.forEach(m -> m.capabilities().register(m.raft()));
        return members;
    }

    private Member awaitLeader(List<Member> members, Member except) {
        for (int i = 0; i < 2_000; i++) {
            for (Member m : members) {
                if (m != except && m.raft().isLeader()) {
                    return m;
                }
            }
            time.advance(Duration.ofMillis(10));
        }
        throw new AssertionError("no leader within 20 s");
    }

    @Test
    void anIdleFollowerElectsWithinTheTimeoutAndTheLeaderBeatsEveryQuarterSecond() throws Exception {
        List<Member> members = cluster(3);
        Member leader = awaitLeader(members, null);
        time.advance(Duration.ofSeconds(5));
        long beats = leader.raft().heartbeats();
        time.advance(Duration.ofSeconds(10));
        long inTen = leader.raft().heartbeats() - beats;
        System.out.printf("[raft-due] heartbeats in 10 s: %d%n", inTen);
        assertThat(inTen).as("every 250 ms").isBetween(39L, 41L);
        assertThat(members.stream().filter(m -> m.raft().isLeader()).count()).as("one leader, no churn")
                .isEqualTo(1);

        // Silence: the leader's node goes away. A follower's timer, 5 to 9 times 250 ms, runs out.
        long lastBeat = leader.raft().lastHeartbeatNanos();
        leader.node().close();
        Member next = awaitLeader(members, leader);
        long afterMillis = (time.monoNanos() - lastBeat) / MS;
        System.out.printf("[raft-due] a new leader %d ms after the last heartbeat%n", afterMillis);
        assertThat(afterMillis).as("an election timeout of 1.25 to 2.25 s, plus the vote's round trip")
                .isBetween(1_250L, 2_350L);
        assertThat(next.raft().term()).isGreaterThan(0);
        members.stream().filter(m -> m != leader).forEach(m ->
                assertThat(m.node().loopStats().backstopFoundWork()).as("no missed wake").isZero());
    }

    /**
     * Owner decision 2 (2026-11-23): the election timeout is a fixed 1.25 to 2.25 s whatever {@code tick-millis} says.
     * At a 50 ms tick the base used to follow the tick, so a follower stood for election after 250 to 450 ms of
     * silence, barely more than one heartbeat, and the leader's lease card spanned as little.
     */
    @Test
    void aShortTickMillisNeitherShortensTheElectionTimeoutNorTheLease() throws Exception {
        List<Member> members = cluster(3, Duration.ofMillis(50));
        Member leader = awaitLeader(members, null);
        time.advance(Duration.ofSeconds(2));
        CapabilityAdvertisement card = leader.capabilities().providersOf(RaftLog.TYPE).stream()
                .filter(ad -> "leader".equals(ad.parameters().get("role"))).findFirst().orElseThrow();
        long leaseMillis = Duration.between(card.issued(), Instant.parse(card.parameters().get("leaseUntil")))
                .toMillis();
        System.out.printf("[raft-due] at a 50 ms tick the lease spans %d ms%n", leaseMillis);
        assertThat(leaseMillis).as("5 to 9 heartbeats of 250 ms").isBetween(1_250L, 2_250L);

        long lastBeat = leader.raft().lastHeartbeatNanos();
        leader.node().close();
        Member next = awaitLeader(members, leader);
        long afterMillis = (time.monoNanos() - lastBeat) / MS;
        System.out.printf("[raft-due] at a 50 ms tick a new leader %d ms after the last heartbeat%n", afterMillis);
        assertThat(afterMillis).as("the fixed 1.25 to 2.25 s timeout, plus the vote's round trip")
                .isBetween(1_250L, 2_350L);
        assertThat(next.raft().isLeader()).isTrue();
    }

    @Test
    void theLeaseCardIsRepublishedAtHalfItsSpanNotEveryTickAndStepDownMakesAFollowerCard() throws Exception {
        List<Member> members = cluster(3);
        Member leader = awaitLeader(members, null);
        time.advance(Duration.ofSeconds(3));
        Set<Instant> issued = new HashSet<>();
        for (int i = 0; i < 1_000; i++) {
            time.advance(Duration.ofMillis(10));
            leader.capabilities().providersOf(RaftLog.TYPE).stream()
                    .filter(ad -> "leader".equals(ad.parameters().get("role")))
                    .map(CapabilityAdvertisement::issued).forEach(issued::add);
        }
        System.out.printf("[raft-due] lease cards in 10 s: %d (the tick published 40)%n", issued.size());
        assertThat(issued.size()).as("about one per half lease (0.6 to 1.1 s), not one per 250 ms tick")
                .isBetween(8, 20);

        leader.raft().stepDown();
        time.advance(Duration.ofMillis(10));
        assertThat(leader.capabilities().providersOf(RaftLog.TYPE).stream()
                .filter(ad -> ad.issuer().equals(leader.node().peerId())).findFirst().orElseThrow()
                .parameters()).as("the next card after a step-down is a follower's").containsEntry("role", "follower");
        assertThat(leader.raft().isLeader()).isFalse();
    }

    @Test
    void aLeaderWhoseLoopIsWedgedStepsDownThroughItsLeaseGuard() throws Exception {
        List<Member> members = cluster(3);
        Member leader = awaitLeader(members, null);
        time.advance(Duration.ofSeconds(3));
        AtomicBoolean wedge = new AtomicBoolean();
        leader.runtime().onDue("wedger", now -> {
            if (wedge.getAndSet(false)) {
                try {
                    time.sleep(Duration.ofSeconds(4)); // the leader's loop is held in another participant
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return DueWork.NEVER;
        });
        time.advance(Duration.ofMillis(100));
        wedge.set(true);
        long stepdowns = leader.raft().stepdowns();
        leader.runtime().wake();
        time.runDue();
        long lastBeat = leader.raft().lastHeartbeatNanos();
        CapabilityAdvertisement card = leader.capabilities().providersOf(RaftLog.TYPE).stream()
                .filter(ad -> ad.issuer().equals(leader.node().peerId())).findFirst().orElseThrow();
        long leaseEnd = time.monoNanos() + Duration.between(time.instant(),
                Instant.parse(card.parameters().get("leaseUntil"))).toNanos();
        long bound = Math.min(lastBeat + 1_250 * MS, leaseEnd);
        long steppedAt = -1;
        for (int i = 0; i < 400 && steppedAt < 0; i++) {
            time.advance(Duration.ofMillis(10));
            if (!leader.raft().isLeader()) {
                steppedAt = time.monoNanos();
            }
        }
        assertThat(steppedAt).as("stepped down while its loop was wedged").isPositive();
        long after = (steppedAt - lastBeat) / MS;
        System.out.printf("[raft-due] the wedged leader stepped down %d ms after its last heartbeat (its card's lease"
                + " ended %d ms after it)%n", after, (leaseEnd - lastBeat) / MS);
        assertThat((steppedAt - bound) / MS).as("at its last heartbeat plus the minimum election timeout, 1.25 s, or at"
                + " the end of the lease its last card advertised, whichever is sooner (EL-SP-24)").isBetween(0L, 10L);
        assertThat(leader.raft().stepdowns()).isGreaterThan(stepdowns);
        time.advance(Duration.ofSeconds(5));
        assertThat(members.stream().filter(m -> m.raft().isLeader()).count()).as("one leader again").isEqualTo(1);
    }
    /**
     * Seam wd09 (srad/agentspaces-event-loop.md §10.33, §10.37; EL-H-09): the log registers its leader's lease with
     * the watchdog, which judges it on every tick. The leader's loop is wedged just after a heartbeat, placed so that
     * the watchdog's next tick falls 600 to 850 ms later: the heartbeat is then overdue past the grace, and the
     * watchdog forces a republish of the lease card (core.raft.lease-forced) before the leader's own lease guard steps
     * it down at 1.25 s. Before §10.37 only {@code WatchdogTest}'s fake holder reached this judgement, and EL-SP-24's
     * placements always let the guard act first (lease-forced read 0).
     */
    @Test
    void theWatchdogJudgesTheLeadersLeaseThatTheLogRegistered() throws Exception {
        List<Member> members = cluster(3);
        Member leader = awaitLeader(members, null);
        time.advance(Duration.ofSeconds(3));
        AtomicBoolean wedge = new AtomicBoolean();
        leader.runtime().onDue("wedger", now -> {
            if (wedge.getAndSet(false)) {
                try {
                    time.sleep(Duration.ofSeconds(4)); // the leader's loop is held in another participant
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return DueWork.NEVER;
        });
        ai.badmonkey.agentspaces.common.time.Watchdog wd = time.watchdog();
        long beat = leader.raft().lastHeartbeatNanos();
        for (int i = 0; ; i++) {
            assertThat(i).as("a heartbeat with the watchdog's tick 600 to 850 ms ahead, within 10 s").isLessThan(2_000);
            long now = leader.raft().lastHeartbeatNanos();
            if (now != beat) {
                beat = now;
                long ahead = wd.nextTickNanos() - time.monoNanos();
                if (ahead >= 600 * MS && ahead <= 850 * MS) {
                    break;
                }
            }
            time.advance(Duration.ofMillis(5));
        }
        long forced0 = wd.leaseForced();
        wedge.set(true);
        leader.runtime().wake();
        time.runDue();
        long steppedAt = -1;
        for (int i = 0; i < 300 && steppedAt < 0; i++) {
            time.advance(Duration.ofMillis(10));
            if (!leader.raft().isLeader()) {
                steppedAt = time.monoNanos();
            }
        }
        System.out.printf("[raft-due] lease-forced %d before the guard stepped the leader down %d ms after its last"
                + " heartbeat%n", wd.leaseForced() - forced0, (steppedAt - beat) / MS);
        assertThat(wd.leaseForced() - forced0).as("the watchdog forced a republish of the registered lease")
                .isPositive();
        assertThat(wd.detections()).as("named for the log").anySatisfy(d -> {
            assertThat(d.kind()).isEqualTo("raft-lease");
            assertThat(d.name()).isEqualTo("raft:" + leader.node().peerId().display());
        });
        assertThat(steppedAt).as("and the guard still stepped it down while wedged").isPositive();
        time.advance(Duration.ofSeconds(5));
        assertThat(members.stream().filter(m -> m.raft().isLeader()).count()).as("one leader again").isEqualTo(1);
    }
}
