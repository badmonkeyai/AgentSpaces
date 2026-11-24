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
import ai.badmonkey.agentspaces.common.time.SystemTime;
import ai.badmonkey.agentspaces.common.time.Watchdog;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.measure.Direction;
import ai.badmonkey.agentspaces.measure.Kind;
import ai.badmonkey.agentspaces.measure.Measure;
import ai.badmonkey.agentspaces.measure.Params;
import ai.badmonkey.agentspaces.measure.Rec;
import ai.badmonkey.agentspaces.measure.Unit;
import ai.badmonkey.agentspaces.measure.Wait;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The watchdog's spoiler scenarios of srad/agentspaces-event-loop.md §5.1 that phase 1 scripts: EL-SP-20 (a loop
 * thread dies, and a lost nudge), EL-SP-21 (a loop wedged in a participant), EL-SP-22 (the timer thread dies),
 * EL-SP-23 (the watchdog starved, then its check throws), EL-SP-25 (a virtual-time advance blocked on a real socket)
 * and EL-SP-26 (a participant that overruns repeatedly). Each is REPAIRED or PREVENTED as the SRAD states and
 * measured: detection latency, re-initialisation latency, lateness and time to failure go to the measurements store,
 * and the protocol outcomes (no frame lost or doubled, views held, no participant running twice at once) are hard
 * assertions. The faults are scripted in-process, as the scenarios are written; each watchdog is marked scripted so
 * its findings stay out of the reactor's {@code core.watchdog.detections}.
 */
@Timeout(300)
class WatchdogSpoilerTest {

    private static final long MS = 1_000_000L;
    private static final Duration TICK = Duration.ofMillis(250);

    private final SimNetwork network = new SimNetwork();
    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-21T00:00:00Z"));
    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<NodeLoop> loops = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zWatchdogSpoiler");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zWatchdogSpoiler",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
            "watchdog-spoiler", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    /** Deliveries per node, per item id. */
    private final Map<String, Map<String, AtomicInteger>> delivered = new ConcurrentHashMap<>();

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
        loops.forEach(NodeLoop::close);
    }

    private record Fleet(PeerNode a, GroupRuntime ra, List<PeerNode> others, List<GroupRuntime> otherRuntimes) {
    }

    /** Three members on one virtual time, coalescing on (two outboxes from A), their loops started and settled. */
    private Fleet fleet() throws Exception {
        List<PeerNode> all = new ArrayList<>();
        List<GroupRuntime> runtimes = new ArrayList<>();
        for (String name : List.of("a", "b", "c")) {
            PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(time).time(time)
                    .randomSeed(name.hashCode()).coalesce(Duration.ofMillis(20)).build();
            node.listen(network.register(name), name);
            nodes.add(node);
            GroupRuntime rt = node.joinGroup(groupAd, GroupMembership.Config.defaults(), name.equals("a") ? List.of()
                    : List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
            Map<String, AtomicInteger> mine = new ConcurrentHashMap<>();
            delivered.put(name, mine);
            rt.gossip().onStream("s", (from, id, payload) -> mine.computeIfAbsent(id, k -> new AtomicInteger())
                    .incrementAndGet());
            node.startTicking(TICK);
            all.add(node);
            runtimes.add(rt);
        }
        time.advance(Duration.ofSeconds(10));
        for (GroupRuntime rt : runtimes) {
            assertThat(rt.membership().allMembers()).as("the three see each other").hasSize(2);
        }
        return new Fleet(all.get(0), runtimes.get(0), all.subList(1, 3), runtimes.subList(1, 3));
    }

    private void assertDeliveredOnce(String item) {
        for (String name : List.of("b", "c")) {
            AtomicInteger n = delivered.get(name).get(item);
            assertThat(n).as("%s received %s", name, item).isNotNull();
            assertThat(n.get()).as("%s received %s once", name, item).isEqualTo(1);
        }
    }

    private static Rec rec(String... params) {
        return Rec.with().params(Params.of((Object[]) params)).kind(Kind.PERF);
    }

    // EL-SP-20 ---------------------------------------------------------------------------------------------------

    @Test
    void elSp20aLoopThreadThatDiesIsFoundWithinOnePeriodAndNoQueuedWakeOrFrameIsLost() throws Exception {
        Fleet f = fleet();
        Watchdog wd = time.watchdog().markScripted();
        AtomicBoolean kill = new AtomicBoolean();
        List<Long> queuedRanAt = new CopyOnWriteArrayList<>();
        f.ra().onDue("killer", now -> {
            if (kill.getAndSet(false)) {
                throw new InternalError("EL-SP-20: scripted, the drain's thread dies");
            }
            return DueWork.NEVER;
        });
        DueWork.Registration queued = f.ra().onDue("queued", now -> {
            queuedRanAt.add(now);
            return DueWork.NEVER;
        });
        time.runDue();
        queuedRanAt.clear();
        long t0 = time.monoNanos();
        f.ra().gossip().publish("s", "sp20-dead", "x".getBytes(StandardCharsets.UTF_8)); // two outboxes, B and C
        queued.wake();
        kill.set(true);
        f.a().loop().wakeAll();
        assertThatThrownBy(() -> time.advance(Duration.ofMillis(10)))
                .as("the scripted error escapes and kills the drain").hasRootCauseInstanceOf(InternalError.class);
        assertThat(f.a().loop().runThread()).as("a run marked started on a dead thread").isNotNull();
        assertThat(f.a().loop().runThread().isAlive()).isFalse();
        time.advance(Duration.ofSeconds(3));
        Watchdog.Detection d = wd.detections().stream().filter(x -> x.kind().equals("dead")).findFirst()
                .orElseThrow(() -> new AssertionError("no dead-loop detection: " + wd.detections()));
        long detect = d.atNanos() - t0;
        assertThat(detect).as("found within one period").isLessThanOrEqualTo(2_000 * MS + 10 * MS);
        assertThat(queuedRanAt).as("the queued wake survived the re-initialisation").isNotEmpty();
        long reinit = queuedRanAt.get(0) - d.atNanos();
        Measure.record("core.watchdog.detect-latency", Unit.MS, Direction.LOWER, detect / 1e6,
                rec("check", "dead", "harness", "virtual"));
        Measure.record("core.watchdog.reinit-latency", Unit.MS, Direction.LOWER, reinit / 1e6,
                rec("check", "dead", "harness", "virtual"));
        assertThat(reinit).as("the loop runs again at the detection").isBetween(0L, 50 * MS);
        assertDeliveredOnce("sp20-dead");
        time.advance(Duration.ofSeconds(30));
        assertDeliveredOnce("sp20-dead");
        for (GroupRuntime rt : f.otherRuntimes()) {
            assertThat(rt.membership().allMembers()).as("A stays in every view").hasSize(2);
        }
        assertThat(f.ra().membership().allMembers()).hasSize(2);
        assertThat(f.a().loopStats().generation()).isEqualTo(1);
    }

    @Test
    void elSp20bALostNudgeIsFoundWithinOnePeriodPlusTheGraceAndNoQueuedWakeOrFrameIsLost() throws Exception {
        Fleet f = fleet();
        Watchdog wd = time.watchdog().markScripted();
        List<Long> queuedRanAt = new CopyOnWriteArrayList<>();
        DueWork.Registration queued = f.ra().onDue("queued", now -> {
            queuedRanAt.add(now);
            return DueWork.NEVER;
        });
        time.runDue();
        queuedRanAt.clear();
        long t0 = time.monoNanos();
        f.a().loop().dropNextDispatch(); // the script swallows the dispatch of A's next pending run
        queued.wake();
        f.ra().gossip().publish("s", "sp20-nudge", "y".getBytes(StandardCharsets.UTF_8));
        assertThat(f.a().loop().pendingRun()).isTrue();
        time.advance(Duration.ofSeconds(3));
        Watchdog.Detection d = wd.detections().stream().filter(x -> x.kind().equals("missed-wake")).findFirst()
                .orElseThrow(() -> new AssertionError("no missed-wake detection: " + wd.detections()));
        long detect = d.atNanos() - t0;
        assertThat(detect).as("found within one period plus the grace").isLessThanOrEqualTo(2_250 * MS);
        assertThat(queuedRanAt).as("the pending wake survived").isNotEmpty();
        long reinit = queuedRanAt.get(0) - d.atNanos();
        Measure.record("core.watchdog.detect-latency", Unit.MS, Direction.LOWER, detect / 1e6,
                rec("check", "missed-wake", "harness", "virtual"));
        Measure.record("core.watchdog.reinit-latency", Unit.MS, Direction.LOWER, reinit / 1e6,
                rec("check", "missed-wake", "harness", "virtual"));
        assertThat(reinit).isBetween(0L, 50 * MS);
        time.advance(Duration.ofSeconds(30));
        assertDeliveredOnce("sp20-nudge");
        assertThat(wd.stats().reinits()).isEqualTo(1);
    }

    // EL-SP-21 ---------------------------------------------------------------------------------------------------

    @Test
    void elSp21aLoopWedgedInAParticipantIsFencedTheOthersRunItEscalatesAtTwoMinutesAndItsLateReturnReArmsNothing()
            throws Exception {
        Fleet f = fleet();
        Watchdog wd = time.watchdog().markScripted();
        List<NodeLoop.Health> healths = new CopyOnWriteArrayList<>();
        f.a().onLoopHealth(healths::add);
        AtomicBoolean wedge = new AtomicBoolean();
        AtomicBoolean latch = new AtomicBoolean();
        AtomicInteger inside = new AtomicInteger();
        AtomicInteger maxInside = new AtomicInteger();
        AtomicInteger wedgerRuns = new AtomicInteger();
        AtomicInteger otherRuns = new AtomicInteger();
        AtomicInteger otherInside = new AtomicInteger();
        AtomicInteger maxOtherInside = new AtomicInteger();
        f.ra().onDue("wedger", now -> {
            maxInside.accumulateAndGet(inside.incrementAndGet(), Math::max);
            try {
                wedgerRuns.incrementAndGet();
                if (wedge.getAndSet(false)) {
                    while (!latch.get()) { // a latch that never opens until the script opens it
                        try {
                            time.sleep(Duration.ofSeconds(1));
                        } catch (InterruptedException e) {
                            // wedged for real: deaf to the watchdog's interrupt
                        }
                    }
                    return time.monoNanos() + MS; // discarded (WD-3)
                }
                return DueWork.NEVER;
            } finally {
                inside.decrementAndGet();
            }
        });
        f.ra().onDue("other", now -> {
            maxOtherInside.accumulateAndGet(otherInside.incrementAndGet(), Math::max);
            otherRuns.incrementAndGet();
            otherInside.decrementAndGet();
            return now + 1_000 * MS;
        });
        time.runDue();
        long t0 = time.monoNanos();
        wedge.set(true);
        f.a().loop().wakeAll();
        time.runDue();
        assertThat(f.a().loop().runningIn()).endsWith("/wedger");
        time.advance(Duration.ofSeconds(10));
        Watchdog.Detection d = wd.detections().stream().filter(x -> x.kind().equals("wedged")).findFirst()
                .orElseThrow(() -> new AssertionError("no wedge detection: " + wd.detections()));
        long detect = d.atNanos() - t0;
        assertThat(d.participant()).endsWith("/wedger");
        assertThat(detect).as("within one period after the 5 s bound").isLessThanOrEqualTo(7_000 * MS);
        Measure.record("core.watchdog.detect-latency", Unit.MS, Direction.LOWER, detect / 1e6,
                rec("check", "wedged", "harness", "virtual"));
        int otherAt = otherRuns.get();
        time.advance(Duration.ofSeconds(60));
        assertThat(otherRuns.get() - otherAt).as("every other participant runs while one is fenced")
                .isGreaterThanOrEqualTo(55);
        for (GroupRuntime rt : f.otherRuntimes()) {
            assertThat(rt.membership().allMembers()).as("A's protocol still runs: it stays in every view")
                    .hasSize(2);
        }
        assertThat(f.a().loopHealth().state()).as("not yet two minutes").isNotEqualTo(NodeLoop.HealthState.DOWN);
        time.advance(Duration.ofSeconds(65));
        assertThat(f.a().loopHealth().state()).as("decision 4: escalated at 2 minutes, reported")
                .isEqualTo(NodeLoop.HealthState.DOWN);
        assertThat(healths).extracting(NodeLoop.Health::state).containsExactly(NodeLoop.HealthState.DOWN);
        long escalatedAt = wd.detections().stream().filter(x -> x.kind().equals("escalation")).findFirst()
                .orElseThrow().atNanos();
        assertThat(escalatedAt - d.atNanos()).as("the fence limit").isBetween(120_000 * MS, 124_000 * MS);
        int runsBeforeOpen = wedgerRuns.get();
        time.advance(Duration.ofMinutes(3).minus(Duration.ofNanos(time.monoNanos() - t0)));
        latch.set(true);
        time.advance(Duration.ofSeconds(5));
        assertThat(f.a().loop().fencedSinceNanos()).as("the fence lifts when the old run returns").isZero();
        assertThat(wedgerRuns.get()).as("the new generation asked it again").isGreaterThan(runsBeforeOpen);
        assertThat(f.a().loop().dueOf(groupId.value() + "/wedger")).as("its late deadline was discarded (WD-3)")
                .isEqualTo(DueWork.NEVER);
        assertThat(maxInside.get()).as("no participant runs twice at once").isEqualTo(1);
        assertThat(maxOtherInside.get()).isEqualTo(1);
        assertThat(wd.stats().reinits()).isEqualTo(1);
        assertThat(f.ra().membership().allMembers()).hasSize(2);
    }

    // EL-SP-22 ---------------------------------------------------------------------------------------------------

    @Test
    void elSp22theTimerThreadDiesAndTheWatchdogRestartsItWithEveryPendingDeadline() {
        SystemTime system = SystemTime.isolated("sp22-timer");
        Watchdog wd = Watchdog.threaded("sp22-watchdog", system, Watchdog.Options.defaults()).markScripted();
        int n = 10;
        long grid = 1_000 * MS;
        long epoch = System.nanoTime();
        AtomicLong[] lastRun = new AtomicLong[n];
        AtomicLong[] firstAfter = new AtomicLong[n];
        AtomicBoolean faulted = new AtomicBoolean();
        List<String> ranOnAfter = new CopyOnWriteArrayList<>();
        List<NodeLoop> mine = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            // A backstop longer than the grid, and every deadline on one shared 1 s grid, so the script can kill
            // the timer at an instant with no run in flight: then nothing schedules and only the watchdog can
            // repair it (a schedule would start a new timer on its own, EL-Q-AVL-02's other path).
            NodeLoop loop = new NodeLoop("node:sp22-" + i, system, Duration.ofSeconds(5));
            loops.add(loop);
            mine.add(loop);
            AtomicLong last = new AtomicLong();
            AtomicLong after = new AtomicLong();
            lastRun[i] = last;
            firstAfter[i] = after;
            loop.register("p", NodeLoop.Kind.PROTOCOL, "g", now -> {
                last.set(now);
                if (faulted.get() && after.compareAndSet(0, now)) {
                    ranOnAfter.add(Thread.currentThread().getName());
                }
                return epoch + ((now - epoch) / grid + 1) * grid;
            });
            loop.start();
        }
        Measure.await("core.spoiler.loops-started", Duration.ofSeconds(10),
                () -> Arrays.stream(lastRun).allMatch(a -> a.get() > epoch + grid));
        // 300 ms past the grid instant every loop last ran at: every run is over and every loop is armed at the next.
        long lastGrid = epoch + ((lastRun[0].get() - epoch) / grid) * grid;
        Measure.await("core.spoiler.quiet-instant", Duration.ofSeconds(5),
                () -> System.nanoTime() - (lastGrid + 300 * MS) >= 0);
        long[] armedAtFault = new long[n];
        for (int i = 0; i < n; i++) {
            armedAtFault[i] = mine.get(i).armedNanos();
        }
        long t0 = System.nanoTime();
        faulted.set(true);
        system.scriptTimerFault(new InternalError("EL-SP-22: scripted, the timer thread dies"));
        Measure.await("core.spoiler.timer-dead-repair", Duration.ofSeconds(20),
                () -> Arrays.stream(firstAfter).allMatch(a -> a.get() != 0),
                Wait.with().diagnostics(() -> "detections " + wd.detections()));
        Watchdog.Detection d = wd.detections().stream().filter(x -> x.name().startsWith("timer:")).findFirst()
                .orElseThrow(() -> new AssertionError("no timer detection: " + wd.detections()));
        long detect = d.atNanos() - t0;
        Measure.record("core.watchdog.detect-latency", Unit.MS, Direction.LOWER, detect / 1e6,
                rec("check", "timer-dead", "harness", "system"));
        assertThat(d.kind()).isEqualTo("dead");
        assertThat(detect).as("found within one period").isLessThanOrEqualTo(2_000 * MS + 100 * MS);
        assertThat(system.timerRestarts()).isEqualTo(1);
        double[] lateness = new double[n];
        for (int i = 0; i < n; i++) {
            long late = firstAfter[i].get() - armedAtFault[i];
            lateness[i] = Math.max(0, late) / 1e6;
            assertThat(late).as("node %d late by at most a period plus the grace", i)
                    .isLessThanOrEqualTo(2_250 * MS + 100 * MS);
        }
        Measure.samples("core.loop.lateness", Unit.MS, Direction.LOWER, lateness,
                rec("scenario", "timer-dead", "harness", "system", "nodes", "10"));
        assertThat(wd.detections()).as("the loops' missed fires were left to the timer's restart")
                .noneMatch(x -> x.kind().equals("missed-fire"));
        assertThat(ranOnAfter).as("phase 4: the ten nodes share the one timer, and after its restart every run is"
                + " still on its loop pool (decision 1, decision 2)").hasSize(n)
                .allSatisfy(name -> assertThat(name).startsWith("sp22-timer-loop-"));
    }

    // EL-SP-23 ---------------------------------------------------------------------------------------------------

    @Test
    void elSp23aStarvedWatchdogIsRestartedByTheTimerAndOneWhoseCheckThrowsByItsHandler() {
        SystemTime system = SystemTime.isolated("sp23-timer");
        Duration period = Duration.ofMillis(200);
        Watchdog wd = Watchdog.threaded("sp23-watchdog", system, Watchdog.Options.defaults().withPeriod(period))
                .markScripted();
        NodeLoop loop = new NodeLoop("node:sp23", system, NodeLoop.DEFAULT_BACKSTOP);
        loops.add(loop);
        AtomicInteger asked = new AtomicInteger();
        DueWork.Registration reg = loop.register("asked", NodeLoop.Kind.OTHER, "g", now -> {
            asked.incrementAndGet();
            return DueWork.NEVER;
        });
        loop.register("ticker", NodeLoop.Kind.PROTOCOL, "g", now -> now + 300 * MS); // keeps the timer waking
        loop.start();
        Measure.await("core.spoiler.loops-started", Duration.ofSeconds(10), () -> asked.get() > 0);
        Thread first = wd.thread();
        AtomicBoolean park = new AtomicBoolean(true);
        AtomicBoolean throwOnce = new AtomicBoolean();
        wd.beforeCheck(() -> {
            if (park.getAndSet(false)) {
                long until = System.nanoTime() + 10 * period.toNanos(); // starved for 10 periods
                while (System.nanoTime() - until < 0) {
                    LockSupport.parkNanos(until - System.nanoTime());
                }
            } else if (throwOnce.getAndSet(false)) {
                throw new IllegalStateException("EL-SP-23: scripted, the watchdog's check throws");
            }
        });
        Measure.await("core.spoiler.watchdog-parked", Duration.ofSeconds(5), () -> !park.get());
        // A loop fault inside the starved window: a lost nudge.
        int askedBefore = asked.get();
        long tFault = System.nanoTime();
        loop.dropNextDispatch();
        reg.wake();
        Measure.await("core.spoiler.watchdog-restarted", Duration.ofSeconds(10),
                () -> wd.stats().restartsStarved() == 1,
                Wait.with().diagnostics(() -> "stats " + wd.stats()));
        Measure.await("core.spoiler.window-fault-found", Duration.ofSeconds(10), () -> asked.get() > askedBefore,
                Wait.with().diagnostics(() -> "detections " + wd.detections()));
        Watchdog.Detection d = wd.detections().stream().filter(x -> x.kind().equals("missed-wake")).findFirst()
                .orElseThrow();
        Measure.record("core.watchdog.detect-latency", Unit.MS, Direction.LOWER, (d.atNanos() - tFault) / 1e6,
                rec("check", "missed-wake", "harness", "system", "scenario", "watchdog-starved", "period-ms",
                        "200"));
        assertThat(wd.thread()).as("a new watchdog thread").isNotSameAs(first);
        assertThat(d.atNanos() - tFault).as("found after the restart, not left for the parked thread")
                .isLessThan(10 * period.toNanos());
        Thread second = wd.thread();
        throwOnce.set(true);
        Measure.await("core.spoiler.watchdog-rethrown", Duration.ofSeconds(10),
                () -> wd.stats().restartsThrown() == 1 && wd.thread() != second,
                Wait.with().diagnostics(() -> "stats " + wd.stats()));
        long ticks = wd.stats().ticks();
        Measure.await("core.spoiler.watchdog-ticking", Duration.ofSeconds(10), () -> wd.stats().ticks() > ticks + 2);
        assertThat(wd.stats().restartsStarved()).isEqualTo(1);
        assertThat(wd.stats().restartsThrown()).isEqualTo(1);
        Measure.await("core.spoiler.watchdog-old-thread-gone", Duration.ofSeconds(10), () -> !first.isAlive());
    }

    // EL-SP-25 ---------------------------------------------------------------------------------------------------

    @Test
    void elSp25anAdvanceBlockedOnARealSocketReadFailsWithinTheQuiescenceBoundNamingTheThread() throws Exception {
        Duration bound = Duration.ofMillis(500);
        time.quiescenceBound(bound);
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
             Socket client = new Socket(InetAddress.getLoopbackAddress(), server.getLocalPort());
             Socket accepted = server.accept()) {
            time.schedule(Duration.ofSeconds(1), () -> {
                try (InputStream in = client.getInputStream()) {
                    in.read(); // a binder thread blocked on real I/O
                } catch (Exception e) {
                    // released when the test closes the socket
                }
            });
            long t0 = System.nanoTime();
            assertThatThrownBy(() -> time.advance(Duration.ofSeconds(2)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("did not settle within")
                    .hasMessageContaining("virtual-time-task-")
                    .satisfies(e -> assertThat(e.getMessage()).containsAnyOf("SocketInputStream", "NioSocketImpl",
                            "Socket"));
            long took = System.nanoTime() - t0;
            Measure.record("core.virtual-time.quiescence-failure", Unit.MS, Direction.LOWER, took / 1e6,
                    rec("bound-ms", "500"));
            assertThat(took).as("the test cannot hang: it fails near the bound")
                    .isLessThan(bound.toNanos() + 5_000 * MS);
            accepted.close();
            client.close();
        }
    }

    // EL-SP-26 ---------------------------------------------------------------------------------------------------

    @Test
    void elSp26aParticipantThatOverrunsRepeatedlyIsNamedThenMovedAndProtocolLatenessReturnsToBand() {
        SystemTime system = SystemTime.isolated("sp26-timer");
        Watchdog.threaded("sp26-watchdog", system, Watchdog.Options.defaults()).markScripted();
        int n = 10;
        NodeLoop slowNode = null;
        List<Long> lateBefore = new CopyOnWriteArrayList<>();
        List<Long> lateAfter = new CopyOnWriteArrayList<>();
        AtomicInteger slowRuns = new AtomicInteger();
        for (int i = 0; i < n; i++) {
            NodeLoop loop = new NodeLoop("node:sp26-" + i, system, NodeLoop.DEFAULT_BACKSTOP);
            loops.add(loop);
            boolean slow = i == 0;
            AtomicLong due = new AtomicLong();
            NodeLoop self = loop;
            loop.register("protocol", NodeLoop.Kind.PROTOCOL, "g", now -> {
                long d = due.get();
                if (slow && d != 0 && now - d >= 0) {
                    (self.stats().offloaded().isEmpty() ? lateBefore : lateAfter).add(now - d);
                }
                long next = now + 100 * MS;
                due.set(next);
                return next;
            });
            if (slow) {
                slowNode = loop;
                loop.register("overrun", NodeLoop.Kind.OTHER, "g", now -> {
                    slowRuns.incrementAndGet();
                    LockSupport.parkNanos(300 * MS); // 300 ms of scripted work per run
                    return now + 500 * MS;
                });
            }
            loop.start();
        }
        NodeLoop watched = slowNode;
        Measure.await("core.spoiler.overrun-offloaded", Duration.ofSeconds(30),
                () -> !watched.stats().offloaded().isEmpty(),
                Wait.with().diagnostics(() -> "slow " + watched.stats().slowParticipants()));
        assertThat(watched.stats().slowParticipants()).as("each slow run counted and named")
                .containsEntry("overrun", 3L);
        assertThat(watched.health().state()).isEqualTo(NodeLoop.HealthState.DEGRADED);
        int moved = slowRuns.get();
        Measure.await("core.spoiler.overrun-after", Duration.ofSeconds(30),
                () -> lateAfter.size() >= 30 && slowRuns.get() >= moved + 4);
        double[] before = lateBefore.stream().mapToDouble(v -> v / 1e6).toArray();
        double[] after = lateAfter.stream().mapToDouble(v -> v / 1e6).toArray();
        Measure.samples("core.loop.lateness", Unit.MS, Direction.LOWER, after,
                rec("scenario", "overrun-offloaded", "harness", "system", "nodes", "10"));
        Arrays.sort(before);
        Arrays.sort(after);
        double maxBefore = before.length == 0 ? 0 : before[before.length - 1];
        double p90After = after[(int) Math.floor(after.length * 0.9)];
        System.out.printf("EL-SP-26: protocol lateness max %.1f ms before the move (%d runs), p90 %.1f ms after (%d"
                + " runs)%n", maxBefore, before.length, p90After, after.length);
        assertThat(maxBefore).as("the overrun delayed the protocol participant before the move")
                .isGreaterThan(100);
        assertThat(p90After).as("after the move the protocol participant is no longer held behind 300 ms runs")
                .isLessThan(100);
        assertThat(watched.stats().slowParticipants().get("overrun")).as("not counted on the loop after the move")
                .isEqualTo(3L);
    }
}
