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
import ai.badmonkey.agentspaces.measure.Direction;
import ai.badmonkey.agentspaces.measure.Kind;
import ai.badmonkey.agentspaces.measure.Measure;
import ai.badmonkey.agentspaces.measure.Params;
import ai.badmonkey.agentspaces.measure.Rec;
import ai.badmonkey.agentspaces.measure.Unit;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.spoilers.Action;
import ai.badmonkey.agentspaces.spoilers.Script;
import ai.badmonkey.agentspaces.spoilers.ScriptRunner;
import ai.badmonkey.agentspaces.spoilers.Trigger;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The phase-2 gate's Raft spoilers of srad/agentspaces-event-loop.md §5.1, on virtual time with every node on its own
 * loop ({@link PeerNode#startTicking}) and nobody calling {@code tick()}: EL-SP-10 (the leader falls silent; a
 * follower calls an election within the band), EL-SP-18 (a live leader in a group idle for 10 minutes; no follower
 * ever reads its {@code leaseUntil} in the past and no election runs) and EL-SP-24 (the leader's loop wedges 4 s in an
 * unrelated participant; no follower reads the leader's {@code leaseUntil} in the past while it still acts as leader,
 * and there is at most one leader per term). A follower "reads" the lease as the thin clients do (`ordered.ts`,
 * `ordered.py`, `ordered.rb`): the leader card with the highest term in its own discovery cache. The figures go to the
 * measurements store ({@code harness=loop}); the protocol outcomes are hard assertions.
 */
@Timeout(900)
class RaftLeaseSpoilerTest {

    private static final Duration TICK = Duration.ofMillis(250);
    private static final Duration PERIOD = Duration.ofSeconds(1); // the gossip period the script runner counts in
    private static final long MS = 1_000_000L;

    private final SimNetwork network = new SimNetwork();
    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-22T00:00:00Z"));
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zRaftLeaseSpoiler");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zRaftLeaseSpoiler",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "raft-lease-spoiler",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final List<PeerNode> nodes = new ArrayList<>();

    private record Member(String name, PeerNode node, GroupRuntime runtime, RaftLog raft,
                          CapabilityRuntime capabilities) {
    }

    /** What the followers read of the leader's lease over a window. */
    private static final class LeaseWatch {
        long reads;
        long lapses;
        long firstLapseMillis = -1;
        final Map<Long, Set<PeerId>> leadersByTerm = new HashMap<>();
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private List<Member> cluster() throws Exception {
        List<PeerIdentity> identities = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            identities.add(PeerIdentity.generate());
        }
        List<PeerId> ids = identities.stream().map(PeerIdentity::peerId).toList();
        List<Member> members = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            String name = "r" + i;
            PeerNode node = PeerNode.builder(identities.get(i)).clock(time).time(time).randomSeed(300 + i).build();
            node.listen(network.register(name), name);
            nodes.add(node);
            GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                    i == 0 ? List.of() : List.of(new PeerAdvertisement.Endpoint("mem", "r0", 0)));
            RaftLog raft = new RaftLog(new CapabilityPipes(runtime, codec), codec, ids.get(i), ids, time, 3000 + i,
                    (command, index) -> { });
            DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, time), codec, ids.get(i));
            members.add(new Member(name, node, runtime, raft,
                    new CapabilityRuntime(runtime, discovery, identities.get(i))));
            node.startTicking(TICK);
        }
        long waited = 0;
        while (!members.stream().allMatch(m -> m.runtime().membership().allMembers().size() == 2)) {
            time.advance(TICK);
            waited += TICK.toMillis();
            assertThat(waited).as("the view forms").isLessThan(60_000);
        }
        members.forEach(m -> m.capabilities().register(m.raft()));
        return members;
    }

    private Member awaitLeader(List<Member> members) {
        for (int i = 0; i < 3_000; i++) {
            List<Member> leading = members.stream().filter(m -> m.raft().isLeader()).toList();
            if (leading.size() == 1) {
                return leading.get(0);
            }
            time.advance(Duration.ofMillis(10));
        }
        throw new AssertionError("no single leader within 30 s");
    }

    /** The leader card a member reads: the highest term among the cards that say leader, as the thin clients pick. */
    private static Optional<CapabilityAdvertisement> leaderCard(Member reader) {
        return reader.capabilities().providersOf(RaftLog.TYPE).stream()
                .filter(ad -> "leader".equals(ad.parameters().get("role")) && ad.parameters().containsKey("leaseUntil"))
                .max((a, b) -> Long.compare(Long.parseLong(a.parameters().get("term")),
                        Long.parseLong(b.parameters().get("term"))));
    }

    /**
     * One read by every follower of {@code leader}: a lapse is a read of the leader's own card, at its current term,
     * whose {@code leaseUntil} is in the past while the leader still acts as leader in that term.
     */
    private void read(List<Member> members, Member leader, long term, LeaseWatch watch, long startMillis) {
        for (Member m : members) {
            if (m.raft().isLeader()) {
                watch.leadersByTerm.computeIfAbsent(m.raft().term(), t -> new HashSet<>()).add(m.node().peerId());
            }
        }
        boolean acting = leader.raft().isLeader() && leader.raft().term() == term;
        for (Member follower : members) {
            if (follower == leader) {
                continue;
            }
            Optional<CapabilityAdvertisement> card = leaderCard(follower);
            if (card.isEmpty() || !card.get().issuer().equals(leader.node().peerId())
                    || Long.parseLong(card.get().parameters().get("term")) != term) {
                continue;
            }
            watch.reads++;
            Instant until = Instant.parse(card.get().parameters().get("leaseUntil"));
            if (acting && until.isBefore(time.instant())) {
                watch.lapses++;
                if (watch.firstLapseMillis < 0) {
                    watch.firstLapseMillis = time.millis() - startMillis;
                    System.out.printf("[EL-SP lease] %s read %s's leaseUntil %s at %s (%d ms past), the leader acting%n",
                            follower.name(), leader.name(), until, time.instant(),
                            Duration.between(until, time.instant()).toMillis());
                }
            }
        }
    }

    private static Rec rec(Object... params) {
        return Rec.with().params(Params.of(params)).kind(Kind.PERF);
    }

    // EL-SP-10 ---------------------------------------------------------------------------------------------------

    /**
     * EL-SP-10 on the loop: a three-member ordered log idles two minutes under a live leader; then every pipe frame the
     * leader sends is dropped (it stays a member, like a stalled Raft loop). REPAIRED when a follower calls an
     * election, within the §5.4 limit of twice the longest election timeout (2 × 9 × 250 ms).
     */
    @Test
    void elSp10aFollowerCallsAnElectionWithinTheBandAfterItsLeaderFallsSilent() throws Exception {
        List<Member> members = cluster();
        Member leader = awaitLeader(members);
        time.advance(Duration.ofMinutes(2)); // the group goes quiet: the probe and anti-entropy rounds stretch
        assertThat(leader.raft().isLeader()).as("the same leader after two idle minutes").isTrue();
        long term = leader.raft().term();
        Script script = Script.of(leader.name()).seed(2210)
                .rule("leader-silent").when(Trigger.frame(Envelope.Kind.PIPE_DATA).from(leader.name()))
                .act(Action.drop()).always()
                .build();
        ScriptRunner runner = ScriptRunner.attach(network, time, PERIOD, PeerNode.ChannelAuth.SIGNED, script);
        long found0 = members.stream().mapToLong(m -> m.node().loopStats().backstopFoundWork()).sum();
        long start = time.millis();
        long called = -1;
        long elected = -1;
        for (int step = 0; elected < 0; step++) {
            time.advance(Duration.ofMillis(10));
            if (step % 100 == 99) {
                runner.tick();
            }
            assertThat(step).as("a new leader within 30 s").isLessThan(3_000);
            for (Member m : members) {
                if (m == leader) {
                    continue;
                }
                if (called < 0 && m.raft().term() > term) {
                    called = time.millis() - start;
                }
                if (m.raft().isLeader()) {
                    elected = time.millis() - start;
                }
            }
        }
        runner.finish();
        Measure.record("core.raft.election-after-silence", Unit.MS, Direction.LOWER, called,
                rec("members", 3, "harness", "loop", "until", "called"));
        Measure.record("core.raft.election-after-silence", Unit.MS, Direction.LOWER, elected,
                rec("members", 3, "harness", "loop", "until", "elected"));
        System.out.printf("EL-SP-10 on the loop: election called %d ms, new leader %d ms after the leader fell silent%n",
                called, elected);
        long longest = TICK.toMillis() * RaftLog.ELECTION_TIMEOUT_MAX_PERIODS;
        assertThat(called).as("§5.4 limit: twice the longest election timeout").isLessThanOrEqualTo(2 * longest);
        long found = members.stream().mapToLong(m -> m.node().loopStats().backstopFoundWork()).sum();
        assertThat(found - found0).as("EL-Q-REL-03: no backstop run found work").isZero();
    }

    // EL-SP-18 ---------------------------------------------------------------------------------------------------

    /**
     * EL-SP-18: a three-member ordered log idles 10 minutes with a live leader. Each follower reads the leader's lease
     * every 25 ms. PREVENTED: no read finds {@code leaseUntil} in the past, no election runs, the leader never changes.
     * Ten minutes of 25 ms reads cost about 15 s of CPU, so it runs in the soak profile ({@code -Psoak}, at phase gates).
     */
    @Tag("soak")
    @Test
    void elSp18aLiveLeadersLeaseNeverLapsesInATenMinuteIdle() throws Exception {
        List<Member> members = cluster();
        Member leader = awaitLeader(members);
        time.advance(Duration.ofSeconds(5)); // the followers hold the leader's first card
        long term = leader.raft().term();
        long beats = leader.raft().heartbeats();
        LeaseWatch watch = new LeaseWatch();
        Set<Instant> cards = new HashSet<>();
        long start = time.millis();
        for (int i = 0; i < 24_000; i++) { // 10 minutes in 25 ms reads
            time.advance(Duration.ofMillis(25));
            read(members, leader, term, watch, start);
            leaderCard(members.get(leader == members.get(0) ? 1 : 0)).map(CapabilityAdvertisement::issued)
                    .ifPresent(cards::add);
        }
        long beatsIn = leader.raft().heartbeats() - beats;
        Measure.record("core.raft.lease-lapses", Unit.COUNT, Direction.LOWER, watch.lapses,
                Rec.with().params(Params.of("members", 3, "harness", "loop", "scenario", "EL-SP-18")).kind(Kind.PERF));
        System.out.printf("EL-SP-18 on the loop: %d reads, %d lapses, %d lease cards seen by a follower and %d heartbeats"
                + " in 10 min, terms %s%n", watch.reads, watch.lapses, cards.size(), beatsIn,
                members.stream().map(m -> m.raft().term()).toList());
        assertThat(watch.reads).as("the followers read the lease all along").isGreaterThan(40_000);
        assertThat(watch.lapses).as("PREVENTED: no follower reads a live leader's leaseUntil in the past").isZero();
        assertThat(leader.raft().isLeader()).as("the same leader").isTrue();
        assertThat(members.stream().map(m -> m.raft().term()).distinct().toList()).as("no election ran")
                .containsExactly(term);
        assertThat(beatsIn).as("a heartbeat every 250 ms").isBetween(2_390L, 2_410L);
    }

    // EL-SP-24 ---------------------------------------------------------------------------------------------------

    /**
     * EL-SP-24: the leader's loop wedges 4 s in an unrelated participant. Two placements: 100 ms after a heartbeat
     * ({@code 0}), and the adversarial one, 20 ms before the leader's half-lease republish would run ({@code 1}), so
     * the last card the followers hold is as old as it can be when the loop stops. PREVENTED: no follower reads the
     * leader's {@code leaseUntil} in the past while it still acts as leader; at most one leader per term; one leader
     * again after the wedge.
     */
    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void elSp24aWedgedLeaderNeverActsPastTheLeaseItAdvertised(int placement) throws Exception {
        List<Member> members = cluster();
        Member leader = awaitLeader(members);
        time.advance(Duration.ofSeconds(3));
        long term = leader.raft().term();
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
        if (placement == 0) {
            long beat = leader.raft().lastHeartbeatNanos();
            while (time.monoNanos() < beat + 100 * MS) {
                time.advance(Duration.ofMillis(5));
            }
        } else {
            CapabilityAdvertisement own = leader.capabilities().providersOf(RaftLog.TYPE).stream()
                    .filter(ad -> ad.issuer().equals(leader.node().peerId())).findFirst().orElseThrow();
            Instant issued = own.issued();
            long span = Duration.between(issued, Instant.parse(own.parameters().get("leaseUntil"))).toMillis();
            Instant target = issued.plusMillis(span / 2 - 20);
            assertThat(time.instant()).as("the half-lease point is ahead").isBefore(target);
            while (time.instant().isBefore(target)) {
                time.advance(Duration.ofMillis(5));
            }
        }
        long forced0 = time.watchdog().leaseForced();
        long stepdownsForced0 = time.watchdog().stepdownsForced();
        long stepdowns0 = leader.raft().stepdowns();
        wedge.set(true);
        leader.runtime().wake();
        time.runDue();
        LeaseWatch watch = new LeaseWatch();
        long start = time.millis();
        long steppedDown = -1;
        for (int i = 0; i < 900; i++) { // 9 s in 10 ms reads: the wedge and 5 s after it
            time.advance(Duration.ofMillis(10));
            read(members, leader, term, watch, start);
            if (steppedDown < 0 && !(leader.raft().isLeader() && leader.raft().term() == term)) {
                steppedDown = time.millis() - start;
            }
        }
        long maxLeaders = watch.leadersByTerm.values().stream().mapToLong(Set::size).max().orElse(0);
        long forced = time.watchdog().leaseForced() - forced0;
        long stepdownsForced = time.watchdog().stepdownsForced() - stepdownsForced0;
        Params params = Params.of("members", 3, "harness", "loop", "scenario", "EL-SP-24", "placement",
                placement == 0 ? "after-heartbeat" : "before-republish");
        Measure.record("core.raft.lease-lapses", Unit.COUNT, Direction.LOWER, watch.lapses,
                Rec.with().params(params).kind(Kind.PERF));
        Measure.record("core.raft.stepdowns-forced", Unit.COUNT, Direction.LOWER, stepdownsForced,
                Rec.with().params(params).kind(Kind.PERF));
        Measure.record("core.raft.lease-forced", Unit.COUNT, Direction.LOWER, forced,
                Rec.with().params(params).kind(Kind.PERF));
        System.out.printf("EL-SP-24 on the loop (%s): %d reads, %d lapses (first at %d ms), the leader stood down at"
                        + " %d ms; guard step-downs %d, watchdog lease-forced %d, step-downs forced %d; leaders per term %s%n",
                placement == 0 ? "after a heartbeat" : "before the half-lease republish", watch.reads, watch.lapses,
                watch.firstLapseMillis, steppedDown, leader.raft().stepdowns() - stepdowns0, forced, stepdownsForced,
                watch.leadersByTerm);
        assertThat(steppedDown).as("the wedged leader stood down while wedged").isBetween(0L, 4_000L);
        assertThat(watch.lapses).as("PREVENTED: no follower reads the leader's leaseUntil in the past while it acts")
                .isZero();
        assertThat(maxLeaders).as("at most one leader per term").isLessThanOrEqualTo(1);
        assertThat(members.stream().filter(m -> m.raft().isLeader()).count()).as("one leader again").isEqualTo(1);
    }
}
