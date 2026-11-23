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
import ai.badmonkey.agentspaces.api.spi.DueWork;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.measure.Direction;
import ai.badmonkey.agentspaces.measure.Kind;
import ai.badmonkey.agentspaces.measure.Measure;
import ai.badmonkey.agentspaces.measure.Params;
import ai.badmonkey.agentspaces.measure.Rec;
import ai.badmonkey.agentspaces.measure.Unit;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An idle node's loop (srad/agentspaces-event-loop.md §3.3, EL-L-08): rewritten on virtual time against
 * {@link PeerNode#loopStats()} when the idle divider (idle-loop O8) and the sleeping ticker (item 1) were removed. An
 * idle node wakes only for its participants' deadlines, the backstop finds no missed wake, the first activity after a
 * quiet stretch brings the probe and the anti-entropy round back to the base at once (EL-L-13), the deprecated
 * {@code onTick} adapter keeps {@code tick-millis} (EL-L-05), and a wake names what it asks (EL-L-15).
 *
 * <p>Before the rewrite this test ran a 10 ms wall-clock ticker and counted {@code idleSleeps()} and
 * {@code idleTicksSkipped()}, which no longer exist; each of its claims has a successor here: "an idle group skips most
 * scheduled ticks" is "an idle node wakes only for deadlines", "work brings it back" is the activity snap-back, and
 * "the view holds through it" is asserted after every idle span.
 */
@Timeout(120)
class IdleTickTest {

    private static final Duration TICK = Duration.ofMillis(250);

    private final SimNetwork network = new SimNetwork();
    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-21T00:00:00Z"));
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zIdleTick");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zIdleTick",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
            "idle-tick", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode node(String address, long seed) throws IOException {
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(time).time(time).randomSeed(seed).build();
        node.listen(network.register(address), address);
        nodes.add(node);
        return node;
    }

    private record Pair(PeerNode a, PeerNode b, GroupRuntime ra, GroupRuntime rb) {
    }

    /** Two members on one virtual time, their loops started, joined and past the first quiet stretch. */
    private Pair idlePair() throws IOException {
        PeerNode a = node("a", 1);
        PeerNode b = node("b", 2);
        GroupRuntime ra = a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime rb = b.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        ra.gossip().onStream("s", (from, id, payload) -> { });
        rb.gossip().onStream("s", (from, id, payload) -> { });
        a.startTicking(TICK);
        b.startTicking(TICK);
        time.advance(Duration.ofSeconds(5));
        assertThat(ra.membership().allMembers()).as("the pair sees each other").hasSize(1);
        assertThat(rb.membership().allMembers()).hasSize(1);
        time.advance(Duration.ofMinutes(3)); // the probe and the round stretch out
        return new Pair(a, b, ra, rb);
    }

    @Test
    void anIdleNodeWakesOnlyForItsDeadlinesAndTheBackstopFindsNoMissedWake() throws Exception {
        Pair pair = idlePair();
        PeerNode.LoopStats a0 = pair.a().loopStats();
        PeerNode.LoopStats b0 = pair.b().loopStats();
        time.advance(Duration.ofMinutes(1));
        PeerNode.LoopStats a1 = pair.a().loopStats();
        PeerNode.LoopStats b1 = pair.b().loopStats();
        for (PeerNode.LoopStats[] s : List.of(new PeerNode.LoopStats[] {a0, a1}, new PeerNode.LoopStats[] {b0, b1})) {
            long wakes = (s[1].timerWakes() - s[0].timerWakes()) + (s[1].eventWakes() - s[0].eventWakes())
                    - (s[1].backstopOnlyRuns() - s[0].backstopOnlyRuns()); // runs that had work (§5.4)
            Measure.record("core.idle.wakes", Unit.COUNT, Direction.LOWER, wakes,
                    Rec.with().params(Params.of("harness", "virtual", "members", 2, "spaces", 0, "source", "all"))
                            .kind(Kind.COUNT));
            assertThat(wakes).as("runs with work in an idle minute, the backstop's bare sweeps excluded (EL-Q-PRF-01:"
                    + " goal 20, limit 60)").isLessThanOrEqualTo(60);
            assertThat(s[1].backstopFoundWork() - s[0].backstopFoundWork())
                    .as("EL-Q-REL-03: no participant's deadline moved earlier without a wake; by name: %s",
                            s[1].backstopFoundBy()).isZero();
            assertThat(s[1].participantThrows()).as("NL-4: nothing threw").isEmpty();
            assertThat(s[1].directRuns()).as("nobody called tick()").isZero();
            assertThat(wakes).as("the probe, the refresh and the rounds still run").isPositive();
        }
        assertThat(pair.ra().membership().allMembers()).as("the view holds through the idle minute").hasSize(1);
        assertThat(pair.rb().membership().allMembers()).hasSize(1);
        assertThat(pair.a().loopParticipants()).as("the tick body as named participants (EL-L-01, DW-4)")
                .contains("trust", "beacon", "zIdleTick/self-ad", "zIdleTick/membership", "zIdleTick/view",
                        "zIdleTick/gossip", "zIdleTick/anti-entropy", "zIdleTick/newcomer", "zIdleTick/lost",
                        "zIdleTick/reach", "zIdleTick/mesh", "zIdleTick/pins", "zIdleTick/offload",
                        "zIdleTick/persist");
        assertThat(Thread.getAllStackTraces().keySet().stream().map(Thread::getName))
                .as("no ticker thread: the loop runs on the time source").noneMatch(n -> n.startsWith("peer-tick-"));
    }

    @Test
    void theFirstActivityAfterAQuietStretchBringsTheProbeAndTheRoundBackToTheBase() throws Exception {
        Pair pair = idlePair();
        long base = pair.ra().membership().probeIntervalMillis();
        assertThat(pair.ra().antiEntropyStretched()).as("a quiet group's round has stretched").isTrue();
        assertThat(pair.rb().antiEntropyStretched()).isTrue();
        assertThat(pair.ra().membership().currentProbeIntervalMillis()).as("and so has its probe").isGreaterThan(base);
        assertThat(pair.rb().membership().currentProbeIntervalMillis()).isGreaterThan(base);

        pair.ra().gossip().publish("s", "item-1", "x".getBytes(StandardCharsets.UTF_8));
        long waited = 0;
        while (waited < 1_000 && !snappedBack(pair, base)) {
            time.advance(Duration.ofMillis(10));
            waited += 10;
        }
        Measure.record("core.activity.snapback", Unit.MS, Direction.LOWER, waited,
                Rec.with().params(Params.of("harness", "virtual", "members", 2)).kind(Kind.PERF));
        assertThat(snappedBack(pair, base)).as("EL-L-13: back at the base on both members").isTrue();
        assertThat(waited).as("EL-Q-REL-09: within 250 ms of the first write (goal 250 ms, limit 1 s)")
                .isLessThanOrEqualTo(250);
    }

    private static boolean snappedBack(Pair pair, long base) {
        return !pair.ra().antiEntropyStretched() && !pair.rb().antiEntropyStretched()
                && pair.ra().membership().currentProbeIntervalMillis() == base
                && pair.rb().membership().currentProbeIntervalMillis() == base;
    }

    @Test
    void theOnTickAdapterKeepsTickMillisAndOnDueWorkRunsWhenDueOrAsked() throws Exception {
        Pair pair = idlePair();
        AtomicInteger ticks = new AtomicInteger();
        AtomicInteger dueRuns = new AtomicInteger();
        @SuppressWarnings("deprecation")
        AutoCloseable adapter = pair.ra().onTick(ticks::incrementAndGet);
        DueWork.Registration due = pair.ra().onDue("probe-test", now -> {
            dueRuns.incrementAndGet();
            return DueWork.NEVER;
        });
        time.advance(Duration.ofSeconds(10));
        assertThat(ticks.get()).as("EL-L-05: the adapter runs every tick-millis (250 ms) while registered")
                .isBetween(39, 41);
        long sweeps = Duration.ofSeconds(10).toMillis() / NodeLoop.DEFAULT_BACKSTOP.toMillis();
        assertThat(dueRuns.get()).as("registered: asked once; then only the backstop sweeps it (decision 3 a, 15 s)")
                .isBetween((int) Math.max(1, sweeps), (int) sweeps + 2);
        int before = dueRuns.get();
        due.wake();
        time.runDue();
        assertThat(dueRuns.get()).as("EL-L-15: its registration asks it").isEqualTo(before + 1);
        pair.ra().wake();
        time.runDue();
        assertThat(dueRuns.get()).as("EL-L-15: a group wake asks every participant of the group").isEqualTo(before + 2);
        int ticksBefore = ticks.get();
        pair.a().tick();
        assertThat(ticks.get()).as("EL-L-04: tick() runs every adapter, due or not").isEqualTo(ticksBefore + 1);
        adapter.close();
        due.close();
        int closedAt = ticks.get();
        int dueClosedAt = dueRuns.get();
        time.advance(Duration.ofSeconds(5));
        assertThat(ticks.get()).as("closed: runs no more").isEqualTo(closedAt);
        assertThat(dueRuns.get()).isEqualTo(dueClosedAt);
        assertThat(pair.a().loopStats().backstopFoundWork()).isZero();
    }
}
