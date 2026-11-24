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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.spi.DueWork;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.common.time.LoopPool;
import ai.badmonkey.agentspaces.common.time.SystemTime;
import ai.badmonkey.agentspaces.common.time.TimeSource;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.measure.Measure;
import ai.badmonkey.agentspaces.measure.Params;
import ai.badmonkey.agentspaces.measure.Wait;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * EL-SP-11's flood, read for the two security rows it carries (srad/agentspaces-event-loop.md §10.35):
 * <ul>
 *   <li>EL-Q-SEC-05: armed timers stay bounded however many frames land; no inbound frame arms a task of its own;</li>
 *   <li>EL-Q-SEC-07: with the loop pool at its max, a flood past the token bucket weakens no wire promise: the bucket
 *   bounds the frames handled, any stretch stays at most 4 and refreshed first, and no frame re-initialises the loop
 *   or changes the pool.</li>
 * </ul>
 */
@Timeout(180)
class FloodBoundsSpoilerTest {

    private static final Duration TICK = Duration.ofMillis(250);
    private static final int BUDGET = 100;

    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zFloodBounds");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zFloodBounds",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "flood-bounds",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private GroupRuntime member(TimeSource time, String name, String seed, boolean limited) throws Exception {
        PeerNode.Builder b = PeerNode.builder(PeerIdentity.generate()).clock(time).time(time).randomSeed(name.hashCode());
        if (limited) {
            b.rateLimit(BUDGET, BUDGET, time::monoNanos);
        }
        PeerNode node = b.build();
        node.listen(network.register(name), name);
        nodes.add(node);
        GroupRuntime rt = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                seed == null ? List.of() : List.of(new PeerAdvertisement.Endpoint("mem", seed, 0)));
        node.startTicking(TICK);
        return rt;
    }

    /** EL-Q-SEC-05 on virtual time: 800 working frames in 10 s arm no task of their own. */
    @Test
    void floodedArmedTimersStayBoundedAndNoFrameArmsATaskOfItsOwn() throws Exception {
        VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-23T00:00:00Z"));
        GroupRuntime victim = member(time, "v", null, true);
        GroupRuntime hostile = member(time, "h", "v", false);
        long waited = 0;
        while (victim.membership().allMembers().isEmpty() || hostile.membership().allMembers().isEmpty()) {
            time.advance(TICK);
            waited += TICK.toMillis();
            assertThat(waited).isLessThan(120_000);
        }
        time.advance(Duration.ofSeconds(30));
        AtomicLong handled = new AtomicLong();
        victim.onKind(Envelope.Kind.PIPE_DATA, (from, body) -> handled.incrementAndGet());
        victim.onDue("app", now -> DueWork.NEVER);
        time.runDue();
        int idle = time.pending();
        PeerId to = nodes.get(0).peerId();
        int most = idle;
        long seq = 0;
        for (int step = 0; step < 100; step++) {
            for (int i = 0; i < 8; i++) {
                hostile.send(to, Envelope.Kind.PIPE_DATA, Map.of("seq", ++seq));
            }
            most = Math.max(most, time.pending());
            time.advance(Duration.ofMillis(100));
            most = Math.max(most, time.pending());
        }
        System.out.printf("EL-Q-SEC-05: %d frames, %d handled; armed tasks idle %d, at most %d during the flood%n",
                seq, handled.get(), idle, most);
        assertThat(handled.get()).as("every frame at the limit was handled").isEqualTo(seq);
        assertThat(most - idle).as("armed tasks during the flood beyond the idle set (each node's loop and flush timer"
                + " move, never multiply)").isLessThanOrEqualTo(4);
    }

    /** EL-Q-SEC-07 over real time, on an isolated system source whose loop pool is at its max (1 thread). */
    @Test
    void aFloodPastTheBucketWithThePoolAtItsMaxWeakensNoPromise() throws Exception {
        SystemTime time = SystemTime.isolated("sec07-timer");
        LoopPool pool = time.loopPool();
        pool.options(pool.options().withMax(1));
        GroupRuntime victim = member(time, "v", null, true);
        GroupRuntime hostile = member(time, "h", "v", false);
        Measure.await("core.harness.wait", Duration.ofSeconds(30),
                () -> !victim.membership().allMembers().isEmpty() && !hostile.membership().allMembers().isEmpty(),
                Wait.with().poll(Duration.ofMillis(50)).params(Params.of("what", "form", "members", 2)));
        Measure.await("core.harness.wait", Duration.ofSeconds(10), pool::atMax, // judged on the watchdog's tick
                Wait.with().poll(Duration.ofMillis(50)).params(Params.of("what", "pool-at-max")));
        AtomicLong handled = new AtomicLong();
        victim.onKind(Envelope.Kind.PIPE_DATA, (from, body) -> handled.incrementAndGet());
        PeerNode v = nodes.get(0);
        long generation = v.loopStats().generation();
        long reinits = time.watchdog().stats().reinits();
        long grows = pool.stats().grows();
        PeerId to = v.peerId();
        long start = time.monoNanos();
        long sent = 0;
        int maxStretch = 1;
        for (int step = 0; step < 30; step++) { // 3 s at 300 frames a second, three times the bucket's refill
            for (int i = 0; i < 30; i++) {
                hostile.send(to, Envelope.Kind.PIPE_DATA, Map.of("seq", ++sent));
            }
            maxStretch = Math.max(maxStretch, v.loopStats().stretch());
            time.sleep(Duration.ofMillis(100));
        }
        double seconds = (time.monoNanos() - start) / 1e9;
        PeerNode.LoopStats s = v.loopStats();
        System.out.printf("EL-Q-SEC-07: %d frames in %.1f s, %d handled (bucket %d + %d/s); stretch at most %d,"
                        + " unrefreshed %d; generation %d -> %d; pool grows %d%n", sent, seconds, handled.get(), BUDGET,
                BUDGET, maxStretch, s.stretchUnrefreshed(), generation, s.generation(), pool.stats().grows() - grows);
        assertThat(handled.get()).as("the token bucket bounds what a flood gets handled")
                .isLessThanOrEqualTo((long) (BUDGET + BUDGET * (seconds + 0.5)));
        assertThat(maxStretch).as("a stretch moves local periods by at most 4").isLessThanOrEqualTo(4);
        assertThat(s.stretch()).isLessThanOrEqualTo(4);
        assertThat(s.stretchUnrefreshed()).as("never before the wire-visible timers are refreshed").isZero();
        assertThat(s.generation()).as("no frame re-initialises the loop").isEqualTo(generation);
        assertThat(time.watchdog().stats().reinits()).isEqualTo(reinits);
        assertThat(pool.stats().grows() - grows).as("no frame changes the pool").isZero();
        assertThat(pool.threads()).isLessThanOrEqualTo(1);
    }
}
