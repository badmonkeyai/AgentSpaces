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
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.DueWork;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.time.LatencyRing;
import ai.badmonkey.agentspaces.common.time.LoopPool;
import ai.badmonkey.agentspaces.common.time.SystemTime;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.measure.Direction;
import ai.badmonkey.agentspaces.measure.Kind;
import ai.badmonkey.agentspaces.measure.Measure;
import ai.badmonkey.agentspaces.measure.Params;
import ai.badmonkey.agentspaces.measure.Rec;
import ai.badmonkey.agentspaces.measure.Unit;
import ai.badmonkey.agentspaces.measure.Wait;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.Fixtures;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The loop pool's spoiler scenarios of srad/agentspaces-event-loop.md §5.1 that phase 4 scripts: EL-SP-08 (one node's
 * 400 ms participant burst among ten in one JVM), EL-SP-17 (a spin on every virtual-thread carrier), EL-SP-27
 * (sustained overrun past the pool's max: the stretch, each step after its refresh), EL-SP-28 (a burst that grows,
 * then shrinks, the pool) and EL-SP-29 (a slowdown that must extend leases first). EL-SP-22 (the timer thread dies,
 * with every node's runs now on the loop pool) is in {@link WatchdogSpoilerTest}.
 *
 * <p>EL-SP-08 and EL-SP-17 run ten members over TCP on an isolated system time source, so the pool, the timer and
 * the watchdog are the scenario's own; EL-SP-27 and EL-SP-28 are long real-time runs at the default bounds and carry
 * the {@code soak} tag (run once, {@code -Psoak}); EL-SP-29 runs on virtual time, with the stretch asked directly,
 * because what it judges is the order of refresh and stretch, not the trigger, which {@code NodeLoopPoolTest} drives.
 * Every outcome is a hard assertion; the latencies go to the measurements store.
 */
@Timeout(600)
class LoopPoolSpoilerTest {

    private static final long MS = 1_000_000L;
    private static final Duration TICK = Duration.ofMillis(250);

    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<NodeLoop> loops = new ArrayList<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (AutoCloseable c : closeables) {
            try {
                c.close();
            } catch (Exception ignored) {
                // best effort
            }
        }
        nodes.forEach(PeerNode::close);
        loops.forEach(NodeLoop::close);
    }

    private static GroupAdvertisement group(String name) {
        GroupId id = GroupId.of("z" + name);
        return new GroupAdvertisement("aspace://z" + name, PeerIdentity.generate().peerId(), id, Instant.EPOCH,
                Duration.ofDays(1), name, GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
    }

    private static Rec rec(String... params) {
        return Rec.with().params(Params.of((Object[]) params)).kind(Kind.PERF);
    }

    /** Ten members over TCP on one isolated system source, coalescing at 20 ms, their loops at 250 ms, formed. */
    private List<GroupRuntime> tcpFleet(SystemTime system, GroupAdvertisement ad, int n) throws Exception {
        List<GroupRuntime> runtimes = new ArrayList<>();
        int seedPort = 0;
        for (int i = 0; i < n; i++) {
            int port = ai.badmonkey.agentspaces.test.Ports.free();
            PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(system).time(system)
                    .coalesce(Duration.ofMillis(20)).build();
            nodes.add(node);
            node.listen(new TcpTransport(), "127.0.0.1:" + port);
            GroupRuntime rt = node.joinGroup(ad, GroupMembership.Config.defaults(), i == 0 ? List.of()
                    : List.of(new PeerAdvertisement.Endpoint("tcp", "127.0.0.1:" + seedPort, 0)));
            if (i == 0) {
                seedPort = port;
            }
            node.startTicking(TICK);
            runtimes.add(rt);
        }
        Measure.await("core.harness.wait", Duration.ofSeconds(60),
                () -> runtimes.stream().allMatch(rt -> rt.membership().allMembers().size() == n - 1),
                Wait.with().poll(Duration.ofMillis(100)).params(Params.of("what", "form", "members", n))
                        .diagnostics(() -> runtimes.stream().map(rt -> rt.membership().allMembers().size()).toList()
                                .toString()));
        return runtimes;
    }

    /**
     * Watches every member's view on a platform thread of its own (the carriers may be spun), every 20 ms: the fewest
     * members any view held, and each member newly suspected by an observer.
     */
    private static final class ViewWatch implements AutoCloseable {
        final AtomicInteger minView = new AtomicInteger(Integer.MAX_VALUE);
        final AtomicLong suspicions = new AtomicLong();
        final Map<String, Boolean> suspected = new ConcurrentHashMap<>();
        final AtomicBoolean stop = new AtomicBoolean();
        final Thread thread;

        ViewWatch(List<GroupRuntime> runtimes) {
            thread = Thread.ofPlatform().daemon(true).name("spoiler-view-watch").start(() -> {
                while (!stop.get()) {
                    for (int i = 0; i < runtimes.size(); i++) {
                        List<GroupMembership.Member> view = runtimes.get(i).membership().allMembers();
                        minView.accumulateAndGet(view.size(), Math::min);
                        for (GroupMembership.Member m : view) {
                            String key = i + ">" + m.id().value();
                            boolean now = m.suspect();
                            Boolean before = suspected.put(key, now);
                            if (now && !Boolean.TRUE.equals(before)) {
                                suspicions.incrementAndGet();
                            }
                        }
                    }
                    LockSupport.parkNanos(20 * MS);
                }
            });
        }

        @Override
        public void close() throws InterruptedException {
            stop.set(true);
            thread.join();
        }
    }

    // EL-SP-08 ---------------------------------------------------------------------------------------------------

    @Test
    void elSp08aFourHundredMillisecondBurstOnOneNodeLeavesTheOthersDispatchInBandAndTimesOutNoProbe()
            throws Exception {
        SystemTime system = SystemTime.isolated("sp08-timer");
        List<GroupRuntime> rts = tcpFleet(system, group("LoopPoolSp08"), 10);
        Thread.sleep(3_000); // settle: the idle pool is back to what idle needs
        int threadsBefore = system.loopPool().threads();
        AtomicInteger armed = new AtomicInteger();
        AtomicInteger bursts = new AtomicInteger();
        DueWork.Registration burst = rts.get(3).onDue("burst", now -> {
            if (armed.getAndSet(0) > 0) {
                bursts.incrementAndGet();
                LockSupport.parkNanos(400 * MS); // the scripted 400 ms participant run (it blocks on purpose)
            }
            return DueWork.NEVER;
        });
        long mark = system.dispatchCount();
        long[] runsBefore = nodes.stream().mapToLong(node -> node.loopStats().runs()).toArray();
        long[] lateMax = new long[rts.size()];
        try (ViewWatch watch = new ViewWatch(rts)) {
            for (int i = 0; i < 3; i++) {
                armed.set(1);
                burst.wake();
                Thread.sleep(1_000);
            }
            Thread.sleep(2_000);
            watch.close();
            LatencyRing.Figures d = system.dispatchLatencySince(mark);
            for (int i = 0; i < nodes.size(); i++) {
                lateMax[i] = nodes.get(i).loopStats().latenessMaxNanos();
            }
            LoopPool.Stats pool = system.loopPool().stats();
            System.out.printf("EL-SP-08: %d bursts of 400 ms on one of ten nodes; dispatch latency over %d dispatches"
                            + " p50 %.2f ms, p99 %.2f ms, max %.2f ms; pool %d -> %d threads (%d prompt, %d tick"
                            + " grows); suspicions %d, smallest view %d%n", bursts.get(), d.samples(),
                    d.p50Nanos() / 1e6, d.p99Nanos() / 1e6, d.maxNanos() / 1e6, threadsBefore, pool.threads(),
                    pool.promptGrows(), pool.grows(), watch.suspicions.get(), watch.minView.get());
            Measure.record("core.timer.dispatch-latency", Unit.MS, Direction.LOWER, d.p99Nanos() / 1e6,
                    rec("scenario", "participant-burst-400ms", "nodes", "10", "harness", "tcp", "stat", "p99"));
            assertThat(bursts.get()).isEqualTo(3);
            // Each of the nine others ran its own due work through the bursts. The old floor (more than 50 dispatches
            // in 5 s) counted the 1 s backstop's own sweeps, ten nodes times five; with the 15 s backstop (decision 3)
            // the dispatches are the due work alone, so the floor is per node, plus enough samples for a p99.
            for (int i = 0; i < nodes.size(); i++) {
                if (i != 3) {
                    assertThat(nodes.get(i).loopStats().runs() - runsBefore[i])
                            .as("node %d kept dispatching through the bursts", i).isPositive();
                }
            }
            assertThat(d.samples()).as("enough dispatches for a p99").isGreaterThanOrEqualTo(20);
            assertThat(d.p99Nanos()).as("PREVENTED: the others' dispatch latency p99 within the 50 ms limit")
                    .isLessThanOrEqualTo(50 * MS);
            assertThat(watch.suspicions.get()).as("no probe timed out because of the burst").isZero();
            assertThat(watch.minView.get()).as("no member left any view").isEqualTo(9);
        }
    }

    // EL-SP-17 ---------------------------------------------------------------------------------------------------

    @Test
    void elSp17aSpinOnEveryCarrierForThreeSecondsDropsNoMemberAndDispatchIsBackInBandWithinASecond()
            throws Exception {
        SystemTime system = SystemTime.isolated("sp17-timer");
        List<GroupRuntime> rts = tcpFleet(system, group("LoopPoolSp17"), 10);
        Thread.sleep(3_000);
        int carriers = Integer.getInteger("jdk.virtualThreadScheduler.parallelism",
                Runtime.getRuntime().availableProcessors());
        long markBefore = system.dispatchCount();
        long[] runsBefore = new long[nodes.size()];
        for (int i = 0; i < nodes.size(); i++) {
            runsBefore[i] = nodes.get(i).loopStats().runs();
        }
        try (ViewWatch watch = new ViewWatch(rts)) {
            long spinEnd = System.nanoTime() + 3_000 * MS;
            List<Thread> spinners = new ArrayList<>();
            for (int i = 0; i < carriers; i++) {
                // the script: one virtual thread per carrier that never parks or yields for 3 s
                spinners.add(Thread.ofVirtual().name("sp17-spin-" + i).start(() -> {
                    while (System.nanoTime() - spinEnd < 0) {
                        Thread.onSpinWait();
                    }
                }));
            }
            long duringMark = system.dispatchCount();
            while (System.nanoTime() - spinEnd < 0) {
                LockSupport.parkNanos(10 * MS);
            }
            LatencyRing.Figures during = system.dispatchLatencySince(duringMark);
            long[] runsDuring = new long[nodes.size()];
            for (int i = 0; i < nodes.size(); i++) {
                runsDuring[i] = nodes.get(i).loopStats().runs() - runsBefore[i];
            }
            // Recovery: from the spin's end, the first 100 ms window whose dispatch p99 is within the 5 ms goal.
            long recovered = -1;
            long window = system.dispatchCount();
            long windowStart = System.nanoTime();
            while (System.nanoTime() - spinEnd < 4_000 * MS) {
                LockSupport.parkNanos(100 * MS);
                LatencyRing.Figures f = system.dispatchLatencySince(window);
                window = f.total();
                if (f.samples() > 0 && f.p99Nanos() <= 5 * MS) {
                    recovered = Math.max(0, windowStart - spinEnd);
                    break;
                }
                windowStart = System.nanoTime();
            }
            for (Thread t : spinners) {
                t.join();
            }
            Thread.sleep(5_000); // the 2 s timeout plus the indirect probe outlast the spin: any suspicion clears
            watch.close();
            System.out.printf("EL-SP-17: %d carriers spun 3 s; loop runs during the spin per node %s; dispatch during"
                            + " the spin p99 %.2f ms over %d; back in band %d ms after; suspicions %d, smallest view"
                            + " %d%n", carriers, java.util.Arrays.toString(runsDuring), during.p99Nanos() / 1e6,
                    during.samples(), recovered / 1_000_000, watch.suspicions.get(), watch.minView.get());
            Measure.record("core.timer.dispatch-recovery", Unit.MS, Direction.LOWER, recovered / 1e6,
                    rec("scenario", "carrier-spin-3s", "nodes", "10", "harness", "tcp"));
            Measure.record("core.timer.dispatch-latency", Unit.MS, Direction.LOWER, during.p99Nanos() / 1e6,
                    rec("scenario", "carrier-spin-3s", "nodes", "10", "harness", "tcp", "stat", "p99"));
            Measure.record("core.carrier-spin.suspicions", Unit.COUNT, Direction.LOWER,
                    watch.suspicions.get(), Rec.with().params(Params.of("nodes", "10")).kind(Kind.COUNT));
            assertThat(java.util.Arrays.stream(runsDuring).min().orElse(0)).as("the loops ran on the pool during the"
                    + " spin (decision 2)").isGreaterThan(0);
            assertThat(recovered).as("dispatch back in band within the 4 s limit of the spin's end")
                    .isBetween(0L, 4_000 * MS);
            assertThat(watch.minView.get()).as("REPAIRED: a member may be suspected, none is dropped").isEqualTo(9);
            for (GroupRuntime rt : rts) {
                assertThat(rt.membership().allMembers()).noneMatch(GroupMembership.Member::suspect);
            }
            assertThat(markBefore).isGreaterThanOrEqualTo(0);
        }
    }

    // EL-SP-27 ---------------------------------------------------------------------------------------------------

    @Test
    @Tag("soak")
    void elSp27sustainedOverrunPastThePoolsMaxStretchesEachStepAfterItsRefreshAndClearsWhenTheLoadEnds()
            throws Exception {
        SystemTime system = SystemTime.isolated("sp27-timer");
        LoopPool pool = system.loopPool();
        // A max of 3 on any host: ten nodes that each want a thread all the time are then over three times the max,
        // so lateness stays past the stretch bound. At this host's default (5) the overrun sat at the bound, and the
        // stretch cleared during the load (the first soak run, srad/agentspaces-event-loop.md §10.21).
        pool.options(pool.options().withMax(3));
        GroupAdvertisement ad = group("LoopPoolSp27");
        List<GroupRuntime> rts = tcpFleet(system, ad, 10);
        Thread.sleep(3_000);
        AtomicBoolean load = new AtomicBoolean(true);
        List<List<Long>> refreshAt = new ArrayList<>();
        for (GroupRuntime rt : rts) {
            // the scripted overrun: 95 ms of CPU, due again at once; ten nodes want ten threads, the max is less
            rt.onDue("load", now -> {
                if (load.get()) {
                    long end = System.nanoTime() + 95 * MS;
                    while (System.nanoTime() - end < 0) {
                        Thread.onSpinWait();
                    }
                    return now + MS;
                }
                return DueWork.NEVER;
            });
            List<Long> mine = new CopyOnWriteArrayList<>();
            refreshAt.add(mine);
            long[] own = {0};
            rt.onDue("lease", now -> { // a wire-visible renewal, 1 s
                if (own[0] == 0 || now - own[0] >= 0) {
                    own[0] = now + 1_000 * MS;
                }
                return own[0];
            }, NodeLoop.Timing.wireVisible(Duration.ZERO, period -> mine.add(System.nanoTime())));
        }
        List<Double> poolThreads = new ArrayList<>();
        int maxStretch = 0;
        long x2At = 0;
        long x4At = 0;
        long aeCapNanos = ad.gossip().period().toNanos() * 32;
        long worstAe = 0;
        List<String> timeline = new ArrayList<>();
        int[] lastF = new int[nodes.size()];
        java.util.Arrays.fill(lastF, 1);
        int minLateStretch = Integer.MAX_VALUE;
        try (ViewWatch watch = new ViewWatch(rts)) {
            long loadStart = System.nanoTime();
            long loadEnd = loadStart + Duration.ofMinutes(2).toNanos();
            while (System.nanoTime() - loadEnd < 0) {
                Thread.sleep(1_000);
                poolThreads.add((double) pool.threads());
                boolean late = loadEnd - System.nanoTime() < 30_000 * MS;
                for (int i = 0; i < nodes.size(); i++) {
                    int f = nodes.get(i).loop().stretch();
                    if (f != lastF[i]) {
                        timeline.add(String.format("%.0fs n%d x%d", (System.nanoTime() - loadStart) / 1e9, i, f));
                        lastF[i] = f;
                    }
                    if (late) {
                        minLateStretch = Math.min(minLateStretch, f);
                    }
                    maxStretch = Math.max(maxStretch, f);
                    if (f >= 2 && x2At == 0) {
                        x2At = System.nanoTime();
                    }
                    if (f >= 4 && x4At == 0) {
                        x4At = System.nanoTime();
                    }
                    long ae = nodes.get(i).loop().dueOf(ad.group().value() + "/anti-entropy");
                    if (ae != DueWork.NEVER) {
                        worstAe = Math.max(worstAe, ae - system.monoNanos());
                    }
                }
            }
            load.set(false);
            long ended = System.nanoTime();
            long ticksAtEnd = system.watchdog().stats().ticks();
            long cleared = -1;
            long clearTicks = -1;
            while (System.nanoTime() - ended < Duration.ofSeconds(90).toNanos()) {
                Thread.sleep(250);
                if (nodes.stream().allMatch(n -> n.loop().stretch() == 1)) {
                    cleared = System.nanoTime() - ended;
                    clearTicks = system.watchdog().stats().ticks() - ticksAtEnd;
                    break;
                }
            }
            Thread.sleep(2_000);
            watch.close();
            long unrefreshed = 0;
            long stretches = 0;
            long refreshes = 0;
            for (PeerNode node : nodes) {
                PeerNode.LoopStats s = node.loopStats();
                unrefreshed += s.stretchUnrefreshed();
                stretches += s.stretches();
                refreshes += s.refreshes();
            }
            System.out.println("EL-SP-27 stretch timeline: " + timeline);
            System.out.printf("EL-SP-27: pool max %d, threads over the load %s; stretch reached x%d (x2 %d s, x4 %d s"
                            + " into the load); %d steps up, %d refreshes; worst anti-entropy deadline %.1f s ahead"
                            + " (cap %.0f s); cleared %d ms and %d watchdog ticks after the load (mean tick %.1f ms);"
                            + " suspicions %d, smallest view %d%n",
                    pool.options().max(), poolThreads, maxStretch, x2At == 0 ? -1 : (x2At - (loadEnd - 120_000 * MS))
                            / 1_000_000_000L, x4At == 0 ? -1 : (x4At - (loadEnd - 120_000 * MS)) / 1_000_000_000L,
                    stretches, refreshes, worstAe / 1e9, aeCapNanos / 1e9, cleared / 1_000_000, clearTicks,
                    clearTicks > 0 ? cleared / 1e6 / clearTicks : -1.0, watch.suspicions.get(), watch.minView.get());
            Measure.samples("core.loop.pool-threads", Unit.COUNT, Direction.LOWER,
                    poolThreads.stream().mapToDouble(Double::doubleValue).toArray(),
                    Rec.with().params(Params.of("scenario", "sustained-overrun", "nodes", "10")).kind(Kind.COUNT));
            Measure.record("core.loop.stretch-unrefreshed", Unit.COUNT, Direction.LOWER, unrefreshed,
                    Rec.with().params(Params.of("scenario", "sustained-overrun")).kind(Kind.COUNT));
            Measure.record("core.loop.stretch-clear", Unit.MS, Direction.LOWER, cleared / 1e6,
                    rec("scenario", "sustained-overrun", "nodes", "10"));
            assertThat(poolThreads.stream().mapToDouble(Double::doubleValue).max().orElse(0))
                    .as("the pool reached its max").isEqualTo(pool.options().max());
            assertThat(maxStretch).as("x2 then x4").isEqualTo(4);
            assertThat(minLateStretch).as("the stretch holds while the overrun lasts (its last 30 s)")
                    .isGreaterThanOrEqualTo(2);
            for (int i = 0; i < nodes.size(); i++) {
                PeerNode.LoopStats s = nodes.get(i).loopStats();
                assertThat(s.refreshes()).as("node %d: a refresh before each step up (EL-O-05)", i)
                        .isGreaterThanOrEqualTo(Math.min(s.stretches(), 2));
            }
            assertThat(unrefreshed).as("core.loop.stretch-unrefreshed").isZero();
            assertThat(worstAe).as("no SPEC floor crossed: anti-entropy within 32 periods").isLessThanOrEqualTo(
                    aeCapNanos + 1_000 * MS);
            assertThat(watch.minView.get()).as("no member dropped").isEqualTo(9);
            // The rule's worst case from the load's end, in its own unit, the watchdog's tick: the first tick after
            // the load still judges the load's waits, the second sees lateness in band, the clear time is 15 ticks of
            // 2 s, and the second halving comes a tick after the first: 18 ticks. The SRAD's "30 s plus a period"
            // counts from the first tick in band (srad/agentspaces-event-loop.md §10.21). The ticks are not a fixed
            // rate: each re-arms a period after the last check ended (EL-T-13), so 18 ticks take 36 s plus each
            // tick's check and park overshoot, and the 250 ms poll above adds its own step. A wall-clock bound of
            // exactly 36 s therefore failed at 36.18 s (close-out gate, srad/agentspaces-event-loop.md §10.39).
            assertThat(cleared).as("cleared after the load").isPositive();
            assertThat(clearTicks).as("cleared within 18 watchdog ticks of the load's end (30 s plus three periods)")
                    .isBetween(1L, 18L);
        }
    }

    // EL-SP-28 ---------------------------------------------------------------------------------------------------

    @Test
    @Tag("soak")
    void elSp28aBurstOfQueuedRunsGrowsThePoolATickAtATimeAndItIsBackAtItsMinWithinSixtySecondsPlusAPeriod()
            throws Exception {
        SystemTime system = SystemTime.isolated("sp28-timer");
        LoopPool pool = system.loopPool();
        AtomicBoolean burst = new AtomicBoolean(false);
        for (int i = 0; i < 10; i++) {
            NodeLoop loop = new NodeLoop("node:sp28-" + i, system, NodeLoop.DEFAULT_BACKSTOP);
            loops.add(loop);
            // Each loop's idle deadline sits on its own phase of the 500 ms grid, 45 ms apart. With one shared phase
            // the ten loops, started in the same millisecond, fell due together every 500 ms, and the watchdog's
            // tick (every 2 s, started with them) could sample the ten queued runs at once: "queued runs exceed its
            // threads" (EL-O-03) grew the idle pool to 2 before the burst (phase-6 gate soak, 2026-11-23).
            long phase = i * 45 * MS;
            loop.register("work", NodeLoop.Kind.OTHER, "g", now -> {
                if (!burst.get()) {
                    return now - Math.floorMod(now - phase, 500 * MS) + 500 * MS;
                }
                long end = System.nanoTime() + 15 * MS; // short runs, under the 20 ms bound: no prompt growth
                while (System.nanoTime() - end < 0) {
                    Thread.onSpinWait();
                }
                return now + MS;
            });
            loop.start();
        }
        Thread.sleep(3_000);
        assertThat(pool.threads()).as("one thread at idle (EL-Q-PRF-08)").isEqualTo(1);
        List<double[]> overTime = new ArrayList<>();
        long start = System.nanoTime();
        burst.set(true);
        long burstEnd = start + 20_000 * MS;
        long grows = pool.stats().grows();
        while (System.nanoTime() - burstEnd < 0) {
            Thread.sleep(500);
            overTime.add(new double[] {(System.nanoTime() - start) / 1e9, pool.threads()});
        }
        burst.set(false);
        int peak = pool.stats().peakThreads();
        long ticksDuring = 20_000 / 2_000;
        long grewDuring = pool.stats().grows() - grows;
        long back = -1;
        // What holds a thread past the idle bound after the burst, for the record: a tick that judged waits over the
        // grow bound, or runs queued when sampled (each restarts the 60 s; the close-out gate saw one run whose pool
        // stayed at 5 for 75 s while two later runs, with tick-level traces, saw no pressure: §10.39).
        List<String> pressure = new ArrayList<>();
        long lastP99 = -1;
        while (System.nanoTime() - burstEnd < 75_000 * MS) {
            Thread.sleep(500);
            overTime.add(new double[] {(System.nanoTime() - start) / 1e9, pool.threads()});
            LoopPool.Stats s = pool.stats();
            if (s.lastTickP99() != lastP99 && s.lastTickP99() > pool.options().growLateness().toNanos()
                    || s.queued() > 0) {
                pressure.add(String.format("%.1fs p99 %.1f ms queued %d", (System.nanoTime() - burstEnd) / 1e9,
                        s.lastTickP99() / 1e6, s.queued()));
            }
            lastP99 = s.lastTickP99();
            if (pool.threads() == 1) {
                back = System.nanoTime() - burstEnd;
                break;
            }
        }
        System.out.printf("EL-SP-28: pool threads over time (s, threads) %s; peak %d (max %d); %d tick grows and %d"
                        + " prompt grows during the burst; back at the min %d ms after the burst; after the burst,"
                        + " pressure seen %s, skipped ticks %d%n",
                overTime.stream().map(a -> String.format("%.1f:%d", a[0], (int) a[1])).toList(), peak,
                pool.options().max(), grewDuring, pool.stats().promptGrows(), back / 1_000_000, pressure,
                pool.stats().skippedTicks());
        Measure.samples("core.loop.pool-threads", Unit.COUNT, Direction.LOWER,
                overTime.stream().mapToDouble(a -> a[1]).toArray(),
                Rec.with().params(Params.of("scenario", "burst-20s", "nodes", "10")).kind(Kind.COUNT));
        Measure.record("core.loop.pool-back-at-min", Unit.MS, Direction.LOWER, back / 1e6,
                rec("scenario", "burst-20s", "nodes", "10"));
        assertThat(peak).as("the burst grew the pool").isGreaterThan(1);
        assertThat(grewDuring).as("at most one thread a tick while runs wait").isLessThanOrEqualTo(ticksDuring + 1);
        assertThat(pool.stats().promptGrows()).as("short runs hold no thread past the bound").isZero();
        assertThat(back).as("REPAIRED: back at the min within 60 s plus one period (2 s)")
                .isBetween(0L, 62_000 * MS + 500 * MS);
    }

    // EL-SP-29 ---------------------------------------------------------------------------------------------------

    /** One renewal on a lease a peer judges: renewed every period for its span; its refresh extends the span. */
    private static final class Renewal {
        final String name;
        final long periodNanos;
        volatile Duration span;
        final java.util.function.Consumer<Duration> renew;
        final List<Long> refreshedAt = new CopyOnWriteArrayList<>();
        long own;

        Renewal(String name, Duration period, Duration span, java.util.function.Consumer<Duration> renew) {
            this.name = name;
            this.periodNanos = period.toNanos();
            this.span = span;
            this.renew = renew;
        }

        long run(long now) {
            if (own == 0 || now - own >= 0) {
                renew.accept(span);
                own = now + periodNanos;
            }
            return own;
        }

        /** EL-O-05: renew now for at least twice the stretched period, and keep that span for later renewals. */
        void refresh(long stretchedPeriodNanos, long now) {
            Duration needed = Duration.ofNanos(2 * stretchedPeriodNanos);
            if (needed.compareTo(span) > 0) {
                span = needed;
            }
            renew.accept(span);
            refreshedAt.add(now);
        }
    }

    private record Sp29Outcome(long lapses, long refreshes, long stretchUnrefreshed, List<String> lapsed) {
    }

    /**
     * Three members on virtual time; member a holds a take lease, a holding (a written entry's lease) and a card (an
     * entry the card stands for, re-issued with a lease) in one replicated space, each renewed by a wire-visible
     * participant. The stretch goes x2, then x4 (so a 1 s renewal would come every 4 s, past every span), then clears.
     * Members b and c read the three every 100 ms: a holding or card absent or expired, or a claim expired or gone, is
     * a lapse a peer reads.
     */
    private Sp29Outcome sp29(boolean refreshFirst) throws Exception {
        VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-22T00:00:00Z"));
        SimNetwork network = new SimNetwork();
        GroupAdvertisement ad = group(refreshFirst ? "LoopPoolSp29" : "LoopPoolSp29Control");
        List<PeerNode> fleet = new ArrayList<>();
        List<ReplicatedSpace> spaces = new ArrayList<>();
        List<GroupRuntime> rts = new ArrayList<>();
        for (String name : List.of("a", "b", "c")) {
            PeerIdentity identity = PeerIdentity.generate();
            PeerNode node = PeerNode.builder(identity).clock(time).time(time).randomSeed(name.hashCode())
                    .coalesce(Duration.ofMillis(20)).build();
            node.listen(network.register(name), name);
            nodes.add(node);
            fleet.add(node);
            GroupRuntime rt = node.joinGroup(ad, GroupMembership.Config.defaults(), name.equals("a") ? List.of()
                    : List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
            ReplicatedSpace space = ReplicatedSpace.builder(rt, "leases", identity, "worker").clock(time).build();
            closeables.add(space);
            spaces.add(space);
            rts.add(rt);
            node.startTicking(TICK);
        }
        time.advance(Duration.ofSeconds(10));
        for (GroupRuntime rt : rts) {
            assertThat(rt.membership().allMembers()).hasSize(2);
        }
        ReplicatedSpace a = spaces.get(0);
        EntryHandle holding = a.write(new Fixtures.TaskEntry("holding", 1), Lease.of(Duration.ofSeconds(3)));
        EntryHandle card = a.write(new Fixtures.TaskEntry("card", 2), Lease.of(Duration.ofSeconds(4)));
        spaces.get(1).write(new Fixtures.TaskEntry("job", 3), Lease.of(Duration.ofMinutes(30)));
        time.advance(Duration.ofSeconds(2));
        java.util.concurrent.CompletableFuture<VirtualTime.Outcome<Optional<TakenEntry<Fixtures.TaskEntry>>>> took =
                time.start(() -> a.take(Template.of(Fixtures.TaskEntry.class).where("topic",
                        ai.badmonkey.agentspaces.api.space.Matchers.eq("job")),
                        Lease.of(Duration.ofMillis(2_500)), Duration.ofSeconds(5)));
        for (int i = 0; i < 200 && !took.isDone(); i++) {
            time.advance(Duration.ofMillis(50));
        }
        TakenEntry<Fixtures.TaskEntry> taken = took.join().value().orElseThrow();
        List<Renewal> renewals = List.of(
                new Renewal("take-lease", Duration.ofSeconds(1), Duration.ofMillis(2_500), taken::renew),
                new Renewal("holding", Duration.ofSeconds(1), Duration.ofSeconds(3), holding::renew),
                new Renewal("card", Duration.ofMillis(1_500), Duration.ofSeconds(4), card::renew));
        GroupRuntime ra = rts.get(0);
        for (Renewal r : renewals) {
            ra.onDue(r.name, r::run, refreshFirst
                    ? NodeLoop.Timing.wireVisible(Duration.ZERO, p -> r.refresh(p, time.monoNanos()))
                    : NodeLoop.Timing.local(Duration.ZERO)); // the control: the same periods, stretched unrefreshed
        }
        time.advance(Duration.ofSeconds(5));
        AtomicLong lapses = new AtomicLong();
        List<String> lapsed = new CopyOnWriteArrayList<>();
        Runnable observe = () -> {
            long now = time.millis();
            for (int peer = 1; peer <= 2; peer++) {
                ReplicatedSpace s = spaces.get(peer);
                for (EntryHandle h : List.of(holding, card)) {
                    Optional<EntryRecord> r = s.recordOf(h.entryId());
                    if (r.isEmpty() || r.get().lease().expiresAtMillis() <= now) {
                        lapses.incrementAndGet();
                        lapsed.add(peer + ":" + h.entryId() + "@" + now);
                    }
                }
                var claim = s.currentClaim(taken.entryId());
                if (claim.isEmpty() || claim.get().expiresAtMillis() <= now) {
                    lapses.incrementAndGet();
                    lapsed.add(peer + ":take@" + now);
                }
            }
        };
        PeerNode nodeA = fleet.get(0);
        for (int f : new int[] {2, 4, 4, 1}) {
            if (f != nodeA.loop().stretch()) {
                nodeA.loop().requestStretch(f);
            }
            for (int i = 0; i < 100; i++) { // 10 s at each step, read every 100 ms
                time.advance(Duration.ofMillis(100));
                observe.run();
            }
        }
        PeerNode.LoopStats s = nodeA.loopStats();
        assertThat(s.stretches()).isEqualTo(2);
        assertThat(s.restores()).isEqualTo(1);
        return new Sp29Outcome(lapses.get(), s.refreshes(), s.stretchUnrefreshed(), lapsed);
    }

    @Test
    void elSp29aStretchThatWouldOutlastTheLeasesSpansRenewsAndReissuesThemFirstSoNoPeerReadsALapse()
            throws Exception {
        Sp29Outcome control = sp29(false);
        Sp29Outcome run = sp29(true);
        System.out.printf("EL-SP-29: with refresh before slowdown %d lapses read by peers, %d refreshes; the control"
                        + " (the same stretch, no refresh) %d lapses, first %s%n", run.lapses(), run.refreshes(),
                control.lapses(), control.lapsed().isEmpty() ? "-" : control.lapsed().get(0));
        Measure.record("core.loop.peer-lapses", Unit.COUNT, Direction.LOWER, run.lapses(),
                Rec.with().params(Params.of("scenario", "stretch-x4", "harness", "virtual")).kind(Kind.COUNT));
        Measure.record("core.loop.stretch-unrefreshed", Unit.COUNT, Direction.LOWER, run.stretchUnrefreshed(),
                Rec.with().params(Params.of("scenario", "stretch-x4", "harness", "virtual")).kind(Kind.COUNT));
        assertThat(control.lapses()).as("the control shows the scenario bites: unrefreshed, peers read lapses")
                .isGreaterThan(0);
        assertThat(run.refreshes()).as("three wire-visible participants, refreshed at x2 and again at x4")
                .isEqualTo(6);
        assertThat(run.stretchUnrefreshed()).as("core.loop.stretch-unrefreshed").isZero();
        assertThat(run.lapses()).as("PREVENTED: core.loop.peer-lapses = 0; " + run.lapsed()).isZero();
    }
}
