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
import ai.badmonkey.agentspaces.common.time.Watchdog;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The flush timer under the watchdog (srad/agentspaces-event-loop.md §10.33): seam WD12, which no test drove (the flush
 * timer's watchdog registration and {@code Coalescer.reinitialise}), and combination flow C10 of EL-A-03, a watchdog
 * re-initialisation of the node loop and of the flush timer during one burst with a flush armed. REPAIRED: each lost
 * dispatch or fire is found within one watchdog period plus the grace, and every frame of the burst arrives once, in
 * its writer's order.
 */
@Timeout(300)
class FlushTimerWatchdogSpoilerTest {

    private static final Duration TICK = Duration.ofMillis(250);
    private static final int BURST = 40;

    private final SimNetwork network = new SimNetwork();
    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-23T00:00:00Z"));
    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<GroupRuntime> runtimes = new ArrayList<>();
    private final Map<String, Map<String, AtomicInteger>> delivered = new ConcurrentHashMap<>();
    private final Map<String, List<String>> order = new ConcurrentHashMap<>();
    private final GroupId groupId = GroupId.of("zFlushWatchdog");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zFlushWatchdog",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
            "flush-watchdog", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private void fleet() throws Exception {
        fleet(Duration.ofMillis(20));
    }

    private void fleet(Duration window) throws Exception {
        for (String name : List.of("a", "b", "c")) {
            PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(time).time(time)
                    .randomSeed(name.hashCode()).coalesce(window).build();
            node.listen(network.register(name), name);
            nodes.add(node);
            GroupRuntime rt = node.joinGroup(groupAd, GroupMembership.Config.defaults(), name.equals("a") ? List.of()
                    : List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
            Map<String, AtomicInteger> mine = new ConcurrentHashMap<>();
            List<String> seen = Collections.synchronizedList(new ArrayList<>());
            delivered.put(name, mine);
            order.put(name, seen);
            rt.gossip().onStream("s", (from, id, payload) -> {
                mine.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
                seen.add(id);
            });
            node.startTicking(TICK);
            runtimes.add(rt);
        }
        time.advance(Duration.ofSeconds(window.compareTo(Duration.ofSeconds(1)) > 0 ? 60 : 10));
        for (GroupRuntime rt : runtimes) {
            assertThat(rt.membership().allMembers()).as("the three see each other").hasSize(2);
        }
    }

    private void assertBurstDeliveredOnceInOrder(String prefix) {
        for (String name : List.of("b", "c")) {
            List<String> mine = new ArrayList<>(order.get(name));
            mine.removeIf(id -> !id.startsWith(prefix));
            for (int i = 0; i < BURST; i++) {
                AtomicInteger n = delivered.get(name).get(prefix + i);
                assertThat(n).as("%s received %s%d", name, prefix, i).isNotNull();
                assertThat(n.get()).as("%s received %s%d once", name, prefix, i).isEqualTo(1);
            }
            List<String> expected = new ArrayList<>();
            for (int i = 0; i < BURST; i++) {
                expected.add(prefix + i);
            }
            assertThat(mine).as("%s saw the writer's order", name).containsExactlyElementsOf(expected);
        }
    }

    private static long kinds(Watchdog wd, String kind, String namePrefix) {
        return wd.detections().stream().filter(d -> d.kind().equals(kind) && d.name().startsWith(namePrefix)).count();
    }

    /** WD12: the flush timer's fire is lost mid-burst; the watchdog finds the missed fire and the reinit flushes. */
    @Test
    void aLostFlushFireMidBurstIsFoundAndEveryQueuedFrameIsSentOnce() throws Exception {
        fleet();
        Watchdog wd = time.watchdog().markScripted();
        PeerNode a = nodes.get(0);
        for (int i = 0; i < BURST; i++) {
            runtimes.get(0).gossip().publish("s", "wd12-" + i, ("x" + i).getBytes(StandardCharsets.UTF_8));
            if (i == BURST / 2) {
                a.coalescer().dropArmedTimer(); // the armed fire is lost; frames keep queueing behind it
            }
        }
        time.advance(Duration.ofSeconds(5));
        assertThat(kinds(wd, "missed-fire", "flush:")).as("the flush timer's lost fire is found").isPositive();
        time.advance(Duration.ofSeconds(30));
        assertBurstDeliveredOnceInOrder("wd12-");
    }

    /**
     * Seam r02c (srad/agentspaces-event-loop.md §10.33, §10.37; EL-O-05, refresh before slowdown): the node wires its
     * loop's stretch refresh to its outboxes ({@code loop.afterRefresh(coalescer::flushAllNow)}), so what a refresh
     * action sends leaves at the stretch, before any period stretches, and not when the coalescing window would flush
     * it. With a 5 s window the difference is visible: the refresh's frame reaches the others within 100 ms of the
     * stretch. EL-SP-29's 20 ms window delivered it in time either way, so no test failed when the wiring was cut.
     */
    @Test
    void aStretchesRefreshLeavesTheOutboxesAtTheStretchNotAtTheCoalescingWindow() throws Exception {
        fleet(Duration.ofSeconds(5));
        PeerNode a = nodes.get(0);
        long[] periods = new long[1];
        a.loop().register("refresh-probe", NodeLoop.Kind.OTHER, "", now -> now + 1_000_000_000L,
                NodeLoop.Timing.wireVisible(Duration.ZERO, period -> {
                    periods[0] = period;
                    runtimes.get(0).gossip().publish("s", "refreshed", "lease".getBytes(StandardCharsets.UTF_8));
                }));
        time.advance(Duration.ofSeconds(12)); // past any flush the registration's own frames armed
        a.loop().requestStretch(2);
        time.advance(Duration.ofMillis(100));
        assertThat(periods[0]).as("the refresh ran with the stretched period").isEqualTo(2_000_000_000L);
        // a's eager peers have it at once; a lazy one may GRAFT it through its own 5 s outbox, so at least one
        assertThat(List.of("b", "c")).as("the refresh's frame left a within 100 ms of the stretch, not after the 5 s"
                + " window").anyMatch(name -> delivered.get(name).containsKey("refreshed"));
    }

    /**
     * C10 (EL-A-03): in one burst with a flush armed, the node loop's next dispatch and the flush timer's fire are both
     * lost. Both are found and re-initialised; the loop's queued wake survives; no frame is lost or sent twice.
     */
    @Test
    void aLoopAndAFlushReinitialisedInOneBurstLoseNoFrameAndSendNoneTwice() throws Exception {
        fleet();
        Watchdog wd = time.watchdog().markScripted();
        PeerNode a = nodes.get(0);
        List<Long> queuedRan = new CopyOnWriteArrayList<>();
        DueWork.Registration queued = runtimes.get(0).onDue("c10-queued", now -> {
            queuedRan.add(now);
            return DueWork.NEVER;
        });
        time.runDue();
        queuedRan.clear();
        a.loop().dropNextDispatch();
        queued.wake();
        for (int i = 0; i < BURST; i++) {
            runtimes.get(0).gossip().publish("s", "c10-" + i, ("y" + i).getBytes(StandardCharsets.UTF_8));
            if (i == BURST / 2) {
                a.coalescer().dropArmedTimer();
            }
        }
        time.advance(Duration.ofSeconds(5));
        assertThat(kinds(wd, "missed-wake", "node:")).as("the loop's lost dispatch is found").isPositive();
        assertThat(kinds(wd, "missed-fire", "flush:")).as("the flush timer's lost fire is found").isPositive();
        assertThat(queuedRan).as("the loop's queued wake survived its re-initialisation").isNotEmpty();
        time.advance(Duration.ofSeconds(30));
        assertBurstDeliveredOnceInOrder("c10-");
        assertThat(wd.stats().escalations()).as("two re-initialisations, no escalation").isZero();
    }
}
