/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
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
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PERF2 §3.3: rumors to a peer that advertises {@code wire.batch=1} coalesce into one signed BATCH
 * frame per window; a peer without the hint keeps receiving single frames; every item arrives once.
 */
class FrameCoalescingTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zBatch");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zBatch",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
            "batch", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode node(String address, long seed, Duration coalesce) throws IOException {
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(seed)
                .coalesce(coalesce).build();
        node.listen(network.register(address), address);
        nodes.add(node);
        return node;
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    private static List<String> items(GroupRuntime runtime) {
        List<String> seen = new CopyOnWriteArrayList<>();
        runtime.gossip().onStream("news", (from, id, payload) -> seen.add(new String(payload, StandardCharsets.UTF_8)));
        return seen;
    }

    @Test
    @Timeout(30)
    void rumorsToABatchingPeerCoalesceAndAPlainPeerStillGetsEveryItem() throws Exception {
        PeerNode a = node("a", 1, Duration.ofMillis(40));
        PeerNode b = node("b", 2, Duration.ofMillis(40));   // advertises the hint
        PeerNode c = node("c", 3, null);                     // does not
        GroupRuntime ra = a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime rb = b.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        GroupRuntime rc = c.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        tickAll(4);
        assertThat(ra.membership().allMembers()).hasSize(2);
        List<String> atB = items(rb);
        List<String> atC = items(rc);

        PeerNode.Traffic before = a.traffic();
        for (int i = 0; i < 20; i++) {
            ra.gossip().publish("news", "item-" + i, ("n" + i).getBytes(StandardCharsets.UTF_8));
        }
        // Wait for both receivers and for A's outbox toward B to flush (the plain peer's forwards
        // can reach B before A's own batch leaves, which is the point of the window).
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while ((atB.size() < 20 || atC.size() < 20 || a.traffic().sentByKind().getOrDefault(Envelope.Kind.BATCH, 0L)
                == before.sentByKind().getOrDefault(Envelope.Kind.BATCH, 0L)) && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertThat(atC).as("the plain peer received every item once").hasSize(20);
        PeerNode.Traffic after = a.traffic();
        long batches = after.sentByKind().getOrDefault(Envelope.Kind.BATCH, 0L)
                - before.sentByKind().getOrDefault(Envelope.Kind.BATCH, 0L);
        long rumors = after.sentByKind().getOrDefault(Envelope.Kind.RUMOR, 0L)
                - before.sentByKind().getOrDefault(Envelope.Kind.RUMOR, 0L);
        assertThat(batches).as("B's twenty rumors went as a few BATCH frames").isBetween(1L, 10L);
        assertThat(rumors).as("C's twenty rumors went as single frames (plus any B got alone)").isBetween(20L, 30L);
        assertThat(atB).as("the batching peer received every item once").hasSize(20).doesNotHaveDuplicates();
    }

    /**
     * The flush timer on a node (srad/agentspaces-event-loop.md EL-B-01, EL-B-06, FT-1): an idle coalescing node arms
     * no flush timer however long it idles, and a burst arms one, which delivers the burst as BATCH frames one window
     * after the first enqueue. No peer-flush thread exists.
     */
    @Test
    @Timeout(60)
    void anIdleNodeArmsNoFlushTimerAndABurstFlushesOneWindowAfterItsFirstFrame() throws Exception {
        VirtualTime time = VirtualTime.create();
        Duration window = Duration.ofMillis(20);
        PeerNode a = PeerNode.builder(PeerIdentity.generate()).clock(time).time(time).randomSeed(11)
                .coalesce(window).build();
        a.listen(network.register("va"), "va");
        nodes.add(a);
        PeerNode b = PeerNode.builder(PeerIdentity.generate()).clock(time).time(time).randomSeed(12)
                .coalesce(window).build();
        b.listen(network.register("vb"), "vb");
        nodes.add(b);
        GroupRuntime ra = a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        GroupRuntime rb = b.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "va", 0)));
        for (int i = 0; i < 4; i++) {
            a.tick();
            b.tick();
            time.advance(Duration.ofSeconds(1));
        }
        assertThat(ra.membership().allMembers()).hasSize(1);
        assertThat(Thread.getAllStackTraces().keySet().stream().map(Thread::getName))
                .as("EL-B-06: no peer-flush thread").noneMatch(n -> n.startsWith("peer-flush-"));
        List<String> atB = items(rb);

        long flushRuns = a.loopStats().flushRuns() + b.loopStats().flushRuns();
        long tasks = time.tasksRun();
        time.advance(Duration.ofMinutes(1)); // idle, untouched: no tick, no traffic
        assertThat(a.loopStats().flushRuns() + b.loopStats().flushRuns())
                .as("FT-1: an idle node arms no flush timer").isEqualTo(flushRuns);
        assertThat(time.tasksRun()).as("nothing ran in an idle minute").isEqualTo(tasks);

        PeerNode.Traffic before = a.traffic();
        for (int i = 0; i < 20; i++) {
            ra.gossip().publish("news", "item-" + i, ("n" + i).getBytes(StandardCharsets.UTF_8));
        }
        assertThat(atB).as("held in the outbox until the window passes").isEmpty();
        time.advance(window.minusMillis(1));
        assertThat(atB).isEmpty();
        time.advance(Duration.ofMillis(1));
        assertThat(atB).as("delivered one window after the first frame").hasSize(20).doesNotHaveDuplicates();
        long batches = a.traffic().sentByKind().getOrDefault(Envelope.Kind.BATCH, 0L)
                - before.sentByKind().getOrDefault(Envelope.Kind.BATCH, 0L);
        assertThat(batches).as("twenty rumors in one BATCH").isEqualTo(1);
        assertThat(a.loopStats().flushWaitMaxNanos()).as("FT-2").isLessThanOrEqualTo(window.toNanos());
    }
}
