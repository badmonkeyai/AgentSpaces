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
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.measure.Direction;
import ai.badmonkey.agentspaces.measure.Kind;
import ai.badmonkey.agentspaces.measure.Measure;
import ai.badmonkey.agentspaces.measure.Params;
import ai.badmonkey.agentspaces.measure.Rec;
import ai.badmonkey.agentspaces.measure.Unit;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.spoilers.Action;
import ai.badmonkey.agentspaces.spoilers.Script;
import ai.badmonkey.agentspaces.spoilers.ScriptRunner;
import ai.badmonkey.agentspaces.spoilers.Trigger;
import ai.badmonkey.agentspaces.test.FrameHeader;
import ai.badmonkey.agentspaces.test.LinkFilter;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The phase-1 gate spoilers of srad/agentspaces-event-loop.md §5.1 that run on the loop's own pace, on virtual time:
 * EL-SP-01 (a silent member), EL-SP-06 (racing flushes, an enqueue during the timer's walk), EL-SP-07 ({@code close()}
 * during a burst), EL-SP-09 (a newcomer in an idle group), EL-SP-11 (a member at its token-bucket limit), EL-SP-14 (the
 * first write after a quiet stretch) and EL-SP-19 (a GRAFT, then a forwarded rumor, on a quiet node). Unlike
 * {@code EventLoopBaselineTest}, which drives one direct {@link PeerNode#tick()} per node per 250 ms (today's protocol
 * at the full tick rate), every node here sleeps between its participants' deadlines and wakes only as the loop wakes
 * it ({@link PeerNode#startTicking}); nobody calls {@code tick()}. Each scenario is PREVENTED or REPAIRED as the SRAD
 * states, the protocol outcomes are hard assertions, and each figure goes to the measurements store
 * ({@code harness=loop}). EL-SP-02 is {@code BlockRetrySpoilerTest} (it needs a space) and EL-SP-15
 * {@code TrustCutSpoilerTest} (it needs TLS).
 */
@Timeout(600)
class EventLoopSpoilerTest {

    private static final Duration TICK = Duration.ofMillis(250);
    private static final Duration PERIOD = Duration.ofSeconds(1); // the gossip period the script runner counts in
    private static final Duration WINDOW = Duration.ofMillis(20); // the starter's coalescing window

    private final SimNetwork network = new SimNetwork();
    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-21T00:00:00Z"));
    private final Map<String, PeerNode> nodes = new LinkedHashMap<>();
    private final Map<String, GroupRuntime> runtimes = new LinkedHashMap<>();
    /** Deliveries per node, per item id. */
    private final Map<String, Map<String, AtomicInteger>> delivered = new ConcurrentHashMap<>();
    /** Item ids per node, in the order its handler saw them. */
    private final Map<String, List<String>> deliveryOrder = new ConcurrentHashMap<>();
    private final GroupId groupId = GroupId.of("zEventLoopSpoiler");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zEventLoopSpoiler",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
            "event-loop-spoiler", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    @AfterEach
    void tearDown() {
        nodes.values().forEach(PeerNode::close);
    }

    private GroupRuntime member(String name, String seed, Duration coalesce, UnaryOperator<PeerNode.Builder> more)
            throws IOException {
        PeerNode.Builder builder = PeerNode.builder(PeerIdentity.generate()).clock(time).time(time)
                .randomSeed(name.hashCode());
        if (coalesce != null) {
            builder.coalesce(coalesce);
        }
        PeerNode node = more.apply(builder).build();
        node.listen(network.register(name), name);
        nodes.put(name, node);
        GroupRuntime rt = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                seed == null ? List.of() : List.of(new PeerAdvertisement.Endpoint("mem", seed, 0)));
        Map<String, AtomicInteger> mine = new ConcurrentHashMap<>();
        List<String> order = Collections.synchronizedList(new ArrayList<>());
        delivered.put(name, mine);
        deliveryOrder.put(name, order);
        rt.gossip().onStream("s", (from, id, payload) -> {
            mine.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
            order.add(id);
        });
        runtimes.put(name, rt);
        node.startTicking(TICK);
        return rt;
    }

    private GroupRuntime member(String name, String seed) throws IOException {
        return member(name, seed, null, b -> b);
    }

    /** Waits on virtual time until every member sees every other, then lets the group go quiet for {@code quiet}. */
    private void formAndIdle(Duration quiet) {
        int size = runtimes.size();
        long waited = 0;
        while (!runtimes.values().stream().allMatch(rt -> rt.membership().allMembers().size() == size - 1)) {
            time.advance(TICK);
            waited += TICK.toMillis();
            assertThat(waited).as("the group forms within 120 s of virtual time").isLessThan(120_000);
        }
        time.advance(quiet);
    }

    private PeerId id(String name) {
        return nodes.get(name).peerId();
    }

    private static Rec rec(Object... params) {
        return Rec.with().params(Params.of(params)).kind(Kind.PERF);
    }

    private static long backstopFound(Map<String, PeerNode> nodes) {
        return nodes.values().stream().mapToLong(n -> n.loopStats().backstopFoundWork()).sum();
    }

    /** Counts the frames a filter sees, by kind, inside a BATCH or a RELAY too. */
    private static int carried(FrameHeader header, String kind, String stream) {
        int n = header.kind().equals(kind) && (stream == null || header.streams().contains(stream)) ? 1 : 0;
        for (FrameHeader inner : header.inner()) {
            n += carried(inner, kind, stream);
        }
        return n;
    }

    // EL-SP-01 ---------------------------------------------------------------------------------------------------

    /**
     * EL-SP-01 on the loop: a member stops answering anything at t0, at 2 and 10 members; REPAIRED when no other view
     * holds it, within the limit of §5.4 (the member TTL plus 1 s), and it never comes back while silent.
     */
    @ParameterizedTest
    @ValueSource(ints = {2, 10})
    void elSp01aSilentMemberLeavesEveryViewWithinTheMemberTtl(int size) throws Exception {
        for (int i = 0; i < size; i++) {
            member("m" + i, i == 0 ? null : "m0");
        }
        formAndIdle(Duration.ofMinutes(2)); // the probe interval stretches as far as a quiet group may
        long found0 = backstopFound(nodes);
        String silent = "m" + (size - 1);
        PeerId gone = id(silent);
        Script script = Script.of(silent).seed(2101)
                .rule("silent-out").when(Trigger.frame().from(silent)).act(Action.drop()).always()
                .rule("silent-in").when(Trigger.frame().to(silent)).act(Action.drop()).always()
                .build();
        ScriptRunner runner = ScriptRunner.attach(network, time, PERIOD, PeerNode.ChannelAuth.SIGNED, script);
        long start = time.millis();
        long steps = 0;
        while (anyHolds(silent, gone)) {
            time.advance(TICK);
            if (++steps % 4 == 0) {
                runner.tick();
            }
            assertThat(steps).as("removed within 200 s of virtual time").isLessThan(800);
        }
        long detected = time.millis() - start;
        int returns = 0;
        for (int i = 0; i < 60; i++) { // a minute more: it must not come back while silent
            time.advance(PERIOD);
            runner.tick();
            returns += anyHolds(silent, gone) ? 1 : 0;
        }
        runner.finish();
        Measure.record("core.detect.silent-member", Unit.MS, Direction.LOWER, detected,
                rec("members", size, "harness", "loop"));
        System.out.printf("EL-SP-01 on the loop: %d members, the silent member gone from every view after %d ms%n",
                size, detected);
        long ttl = GroupMembership.Config.defaults().memberTtl().toMillis();
        assertThat(detected).as("§5.4 limit: the member TTL plus 1 s").isLessThanOrEqualTo(ttl + 1_000);
        assertThat(returns).as("a silent member is not re-admitted from a copy of its card").isZero();
        assertThat(backstopFound(nodes) - found0).as("EL-Q-REL-03: no backstop run found work").isZero();
    }

    private boolean anyHolds(String silent, PeerId gone) {
        for (Map.Entry<String, GroupRuntime> e : runtimes.entrySet()) {
            if (!e.getKey().equals(silent) && e.getValue().membership().member(gone).isPresent()) {
                return true;
            }
        }
        return false;
    }

    // EL-SP-06 ---------------------------------------------------------------------------------------------------

    /**
     * EL-SP-06: four writers enqueue to one peer while size-triggered flushes (on the writers' threads) and timer
     * flushes (on the time source) overlap, and the script lands one enqueue while the timer walks the outboxes
     * (FT-5): it starts a fifth writer from inside the timer's send and lets the walk go on only once that writer has
     * finished or is blocked behind the walk. PREVENTED: every frame crosses the wire once and arrives once, and each
     * writer's frames arrive in its order.
     */
    @Test
    void elSp06RacingFlushesLoseNothingSendNothingTwiceAndKeepEachWritersOrder() throws Exception {
        GroupRuntime a = member("a", null, WINDOW, b -> b);
        member("b", "a", WINDOW, b -> b);
        formAndIdle(Duration.ofSeconds(10));
        AtomicInteger onWire = new AtomicInteger();
        AtomicBoolean walkHooked = new AtomicBoolean();
        AtomicInteger hookOutcome = new AtomicInteger(); // 1 finished first, 2 blocked behind the walk
        List<Thread> lateWriter = new CopyOnWriteArrayList<>();
        AutoCloseable counter = network.filter("a", "b", frame -> {
            FrameHeader h = frame.header().orElse(null);
            if (h == null) {
                return LinkFilter.Verdict.pass();
            }
            int rumors = carried(h, "RUMOR", "s");
            onWire.addAndGet(rumors);
            if (rumors > 0 && h.kind().equals("BATCH") && !Thread.currentThread().getName().startsWith("writer-")
                    && walkHooked.compareAndSet(false, true)) {
                // FT-5, scripted: the timer is inside its walk; an enqueue lands now
                Thread late = Thread.ofPlatform().name("writer-late").start(() ->
                        a.gossip().publish("s", "late-0", "late".getBytes(StandardCharsets.UTF_8)));
                lateWriter.add(late);
                long ceiling = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                while (System.nanoTime() < ceiling) {
                    Thread.State s = late.getState();
                    if (s == Thread.State.TERMINATED) {
                        hookOutcome.set(1);
                        break;
                    }
                    if (s == Thread.State.BLOCKED || s == Thread.State.WAITING) {
                        hookOutcome.set(2);
                        break;
                    }
                    Thread.onSpinWait();
                }
            }
            return LinkFilter.Verdict.pass();
        });
        int writers = 4;
        int each = 400;
        List<Thread> threads = new ArrayList<>();
        AtomicInteger done = new AtomicInteger();
        for (int w = 0; w < writers; w++) {
            int writer = w;
            threads.add(Thread.ofPlatform().name("writer-" + w).start(() -> {
                for (int i = 0; i < each; i++) {
                    // the bus deduplicates by content, so every payload names its item
                    a.gossip().publish("s", "w" + writer + "-" + i,
                            ("w" + writer + "-" + i).getBytes(StandardCharsets.UTF_8));
                    if (i % 16 == 0) {
                        Thread.yield(); // interleave the writers with the timer's walks
                    }
                }
                done.incrementAndGet();
            }));
        }
        while (done.get() < writers) {
            time.advance(Duration.ofMillis(5)); // timer flushes race the writers' size flushes
        }
        for (Thread t : threads) {
            t.join();
        }
        for (Thread t : lateWriter) {
            t.join();
        }
        time.advance(Duration.ofMillis(200));
        counter.close();
        int expected = writers * each + lateWriter.size();
        assertThat(walkHooked).as("the script caught a timer walk").isTrue();
        assertThat(hookOutcome.get()).as("the late enqueue finished or waited behind the walk").isIn(1, 2);
        assertThat(onWire.get()).as("every frame crossed the wire once").isEqualTo(expected);
        Map<String, AtomicInteger> got = delivered.get("b");
        PeerNode nb = nodes.get("b");
        assertThat(got.size()).as("every frame arrived; at B: stale %d, replayed %d, undecodable %d, logical %d, "
                        + "far-future %d, received %s", nb.framesStale(), nb.framesReplayed(), nb.framesDropped(),
                nb.framesLogicalRefused(), nb.framesFarFuture(), nb.traffic().receivedByKind())
                .isEqualTo(expected);
        assertThat(got.values()).as("and arrived once").allMatch(n -> n.get() == 1);
        int[] last = new int[writers];
        java.util.Arrays.fill(last, -1);
        for (String item : List.copyOf(deliveryOrder.get("b"))) {
            if (item.startsWith("w")) {
                int dash = item.indexOf('-');
                int w = Integer.parseInt(item.substring(1, dash));
                int seq = Integer.parseInt(item.substring(dash + 1));
                assertThat(seq).as("writer %d's frames arrive in its order", w).isGreaterThan(last[w]);
                last[w] = seq;
            }
        }
        System.out.printf("EL-SP-06: %d frames, each once and in order; the walk's enqueue %s%n", expected,
                hookOutcome.get() == 1 ? "finished inside the walk" : "waited behind it");
    }

    // EL-SP-07 ---------------------------------------------------------------------------------------------------

    /** EL-SP-07: {@code close()} during a burst with the flush timer armed. PREVENTED: every queued frame is flushed first. */
    @Test
    void elSp07CloseDuringABurstFlushesEveryQueuedFrameBeforeTheTransportsClose() throws Exception {
        GroupRuntime a = member("a", null, WINDOW, b -> b);
        member("b", "a", WINDOW, b -> b);
        formAndIdle(Duration.ofSeconds(10));
        AtomicInteger onWire = new AtomicInteger();
        AutoCloseable counter = network.filter("a", "b", frame -> {
            frame.header().ifPresent(h -> onWire.addAndGet(carried(h, "RUMOR", "s")));
            return LinkFilter.Verdict.pass();
        });
        int burst = 300; // four size flushes of 64, and 44 left in the outbox under an armed timer
        for (int i = 0; i < burst; i++) {
            a.gossip().publish("s", "burst-" + i, ("burst-" + i).getBytes(StandardCharsets.UTF_8));
        }
        int beforeClose = delivered.get("b").size();
        assertThat(beforeClose).as("frames are still queued when close() starts").isLessThan(burst);
        nodes.get("a").close();
        counter.close();
        assertThat(onWire.get()).as("frames sent against frames enqueued").isEqualTo(burst);
        assertThat(delivered.get("b")).as("every queued frame was flushed before the transports closed").hasSize(burst);
        System.out.printf("EL-SP-07: %d queued at close(), all %d sent before the transports closed%n",
                burst - beforeClose, burst);
    }

    // EL-SP-09 ---------------------------------------------------------------------------------------------------

    /** EL-SP-09 on the loop: every member idle, a newcomer admitted; REPAIRED by the targeted round within 250 ms. */
    @Test
    void elSp09aNewcomerInAnIdleGroupGetsItsTargetedRoundWithin250Ms() throws Exception {
        int size = 4;
        for (int i = 0; i < size; i++) {
            member("m" + i, i == 0 ? null : "m0");
        }
        formAndIdle(Duration.ofMinutes(2));
        long found0 = backstopFound(nodes);
        List<Integer> before = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            before.add(nodes.get("m" + i).newcomerRoundMillis().size());
        }
        member("late", "m0");
        long waited = 0;
        while (!roundEverywhere(size, before)) {
            time.advance(Duration.ofMillis(10));
            waited += 10;
            assertThat(waited).as("every member admits the newcomer within 60 s of virtual time").isLessThan(60_000);
        }
        double[] rounds = new double[size];
        for (int i = 0; i < size; i++) {
            List<Long> list = nodes.get("m" + i).newcomerRoundMillis();
            rounds[i] = list.get(list.size() - 1);
        }
        Measure.samples("core.newcomer.targeted-round", Unit.MS, Direction.LOWER, rounds,
                rec("members", size, "harness", "loop"));
        System.out.printf("EL-SP-09 on the loop: targeted round after admission %s ms%n",
                java.util.Arrays.toString(rounds));
        for (double r : rounds) {
            assertThat(r).as("EL-L-07: the targeted round within 250 ms of the admission").isLessThanOrEqualTo(250);
        }
        assertThat(backstopFound(nodes) - found0).as("EL-Q-REL-03: the admission's wake, not the backstop").isZero();
    }

    private boolean roundEverywhere(int size, List<Integer> before) {
        for (int i = 0; i < size; i++) {
            if (nodes.get("m" + i).newcomerRoundMillis().size() <= before.get(i)) {
                return false;
            }
        }
        return true;
    }

    // EL-SP-11 ---------------------------------------------------------------------------------------------------

    /**
     * EL-SP-11: a hostile member sends working frames at its token-bucket limit (PN:1037-1060, here 100 a second with a
     * burst of 100 on the victim's monotonic time). Working frames are the kinds that ask a group's registrants to run
     * (PIPE_DATA here; gossip's RUMOR, DIGEST and PULL_RESP are handled inline and wake nothing, PN:4655). PREVENTED: at
     * most one run is pending however many frames land between two runs, runs a second stay bounded by the frame
     * budget, nothing is struck, and every frame is handled.
     */
    @Test
    void elSp11AMemberAtItsTokenBucketLimitLeavesAtMostOneRunPendingAndRunsBoundedByTheBudget() throws Exception {
        int budget = 100;
        GroupRuntime victim = member("v", null, null, b -> b.rateLimit(budget, budget, time::monoNanos));
        GroupRuntime hostile = member("h", "v");
        formAndIdle(Duration.ofSeconds(30));
        AtomicLong appRuns = new AtomicLong();
        AtomicLong handled = new AtomicLong();
        victim.onKind(Envelope.Kind.PIPE_DATA, (from, body) -> handled.incrementAndGet());
        victim.onDue("app", now -> {
            appRuns.incrementAndGet();
            return DueWork.NEVER;
        });
        time.runDue();
        PeerNode v = nodes.get("v");
        PeerId to = id("v");
        AtomicLong seq = new AtomicLong();

        // one run pending, however many frames land before it
        long runs0 = appRuns.get();
        for (int i = 0; i < 20; i++) {
            hostile.send(to, Envelope.Kind.PIPE_DATA, Map.of("seq", seq.incrementAndGet()));
        }
        assertThat(handled.get()).as("twenty working frames handled").isEqualTo(20);
        assertThat(v.loop().pendingRun()).as("they asked for a run").isTrue();
        time.runDue();
        assertThat(appRuns.get() - runs0).as("twenty working frames, one run of the registrant").isEqualTo(1);

        // sustained at the limit: 80 working frames a second, plus the hostile member's own protocol frames, for 10 s
        PeerNode.LoopStats s0 = v.loopStats();
        long app0 = appRuns.get();
        long handled0 = handled.get();
        int seconds = 10;
        int perStep = 8;
        int steps = seconds * 10;
        for (int step = 0; step < steps; step++) {
            for (int i = 0; i < perStep; i++) {
                hostile.send(to, Envelope.Kind.PIPE_DATA, Map.of("seq", seq.incrementAndGet()));
            }
            time.advance(Duration.ofMillis(100));
        }
        PeerNode.LoopStats s1 = v.loopStats();
        long loopRuns = s1.runs() - s0.runs();
        long app = appRuns.get() - app0;
        double runsPerSecond = loopRuns / (double) seconds;
        Measure.record("core.loop.flood-runs", Unit.COUNT, Direction.LOWER, runsPerSecond,
                Rec.with().params(Params.of("harness", "loop", "budget", budget, "members", 2)).kind(Kind.COUNT));
        System.out.printf("EL-SP-11: %d working frames at the limit over %d s: %d loop runs (%.1f a second), %d "
                + "registrant runs, %d bursts%n", steps * perStep, seconds, loopRuns, runsPerSecond, app, steps);
        assertThat(app).as("at most one registrant run per burst, plus the backstop's sweeps")
                .isLessThanOrEqualTo(steps + seconds + 1);
        assertThat(runsPerSecond).as("runs a second bounded by the frame budget").isLessThanOrEqualTo(budget);
        assertThat(v.quarantined(id("h"))).as("at the limit is not over it: not struck out").isFalse();
        assertThat(handled.get() - handled0).as("every frame at the limit was handled").isEqualTo(steps * perStep);
        assertThat(s1.backstopFoundWork() - s0.backstopFoundWork()).as("EL-Q-REL-03").isZero();
        assertThat(s1.participantThrows()).isEmpty();
    }

    // EL-SP-14 ---------------------------------------------------------------------------------------------------

    /**
     * EL-SP-14: a quiet group of four whose probe interval and anti-entropy period have stretched; one member writes
     * once. REPAIRED: both are back at the base within 250 ms on every member holding the stream.
     */
    @Test
    void elSp14TheFirstWriteAfterAQuietStretchBringsEveryHolderBackToTheBase() throws Exception {
        int size = 4;
        for (int i = 0; i < size; i++) {
            member("m" + i, i == 0 ? null : "m0");
        }
        formAndIdle(Duration.ofMinutes(3));
        long found0 = backstopFound(nodes);
        long base = runtimes.get("m0").membership().probeIntervalMillis();
        for (GroupRuntime rt : runtimes.values()) {
            assertThat(rt.antiEntropyStretched()).as("a quiet group's round has stretched").isTrue();
            assertThat(rt.membership().currentProbeIntervalMillis()).as("and so has its probe").isGreaterThan(base);
        }
        runtimes.get("m2").gossip().publish("s", "sp14-write", "w".getBytes(StandardCharsets.UTF_8));
        long waited = 0;
        while (waited < 1_000 && !everyHolderAtBase(base)) {
            time.advance(Duration.ofMillis(10));
            waited += 10;
        }
        Measure.record("core.activity.snapback", Unit.MS, Direction.LOWER, waited,
                rec("harness", "loop", "members", size));
        System.out.printf("EL-SP-14: every holder back at the base %d ms after the first write%n", waited);
        assertThat(everyHolderAtBase(base)).as("EL-L-13: back at the base on every holder (limit 1 s)").isTrue();
        assertThat(waited).as("EL-Q-REL-09: within 250 ms of the first write").isLessThanOrEqualTo(250);
        assertThat(backstopFound(nodes) - found0).as("the activity wake, not the backstop").isZero();
        time.advance(Duration.ofSeconds(10)); // a lazy member's copy comes by its HAVE and GRAFT
        for (String name : List.of("m0", "m1", "m3")) { // the writer's own handler never hears its own write
            assertThat(delivered.get(name)).as("%s holds the write", name).containsKey("sp14-write");
            assertThat(delivered.get(name).get("sp14-write").get()).as("%s holds it once", name).isEqualTo(1);
        }
    }

    private boolean everyHolderAtBase(long base) {
        for (GroupRuntime rt : runtimes.values()) {
            if (rt.antiEntropyStretched() || rt.membership().currentProbeIntervalMillis() != base) {
                return false;
            }
        }
        return true;
    }

    // EL-SP-19 ---------------------------------------------------------------------------------------------------

    /**
     * EL-SP-19: a quiet node N with two pruned members (P and T each sent it a scripted PRUNE, as a member that saw a
     * duplicate does) receives a scripted GRAFT from T, which restores T's edge, then forwards a rumor. REPAIRED: its
     * HAVE reaches the pruned member P within 250 ms plus the window, and T, whose eager copy rode N's outbox, never
     * grafts the item (no probe piggyback named it early).
     */
    @Test
    void elSp19AGraftThenAForwardOnAQuietNodeSendsItsHaveAtOnceAndNoSpuriousGraft() throws Exception {
        GroupRuntime n = member("n", null, WINDOW, b -> b);
        GroupRuntime p = member("p", "n", WINDOW, b -> b);
        GroupRuntime t = member("t", "n", WINDOW, b -> b);
        formAndIdle(Duration.ofSeconds(10));
        p.send(id("n"), Envelope.Kind.PRUNE, new Bodies.Prune("s", new byte[32]));
        t.send(id("n"), Envelope.Kind.PRUNE, new Bodies.Prune("s", new byte[32]));
        time.advance(Duration.ofMillis(100));
        assertThat(n.gossip().lazyPeers()).as("N has pruned P and T").contains(id("p"), id("t"));
        time.advance(Duration.ofMinutes(2)); // quiet: the young tree's announcements have stopped

        List<Long> haveToP = new CopyOnWriteArrayList<>();
        List<String> grafts = new CopyOnWriteArrayList<>();
        AtomicLong after = new AtomicLong(Long.MAX_VALUE);
        AutoCloseable watch = network.filter(null, null, frame -> {
            FrameHeader h = frame.header().orElse(null);
            if (h != null && time.millis() >= after.get()) {
                if (frame.from().equals("n") && frame.to().equals("p") && carried(h, "HAVE", null) > 0) {
                    haveToP.add(time.millis());
                }
                if (carried(h, "GRAFT", null) > 0) {
                    grafts.add(frame.from() + ">" + frame.to() + "@" + time.millis());
                }
            }
            return LinkFilter.Verdict.pass();
        });
        t.send(id("n"), Envelope.Kind.GRAFT, new Bodies.Graft("s", new byte[32]));
        time.advance(Duration.ofMillis(30));
        assertThat(n.gossip().lazyPeers()).as("the GRAFT restored T's edge, P stays pruned")
                .doesNotContain(id("t")).contains(id("p"));
        long found0 = backstopFound(nodes);
        long t0 = time.millis();
        after.set(t0);
        n.gossip().publish("s", "sp19-z", "z".getBytes(StandardCharsets.UTF_8));
        long waited = 0;
        while (haveToP.isEmpty() && waited < 1_000) {
            time.advance(Duration.ofMillis(1));
            waited++;
        }
        assertThat(haveToP).as("N's HAVE reached P (limit 1 s)").isNotEmpty();
        long latency = haveToP.get(0) - t0;
        Measure.record("core.gossip.have-latency", Unit.MS, Direction.LOWER, latency,
                rec("harness", "loop", "members", 3, "window-ms", WINDOW.toMillis()));
        time.advance(Duration.ofSeconds(10));
        watch.close();
        System.out.printf("EL-SP-19: HAVE to the pruned member %d ms after the forward; GRAFTs after it %s%n",
                latency, grafts);
        assertThat(latency).as("EL-T-15: within 250 ms plus the window").isLessThanOrEqualTo(250 + WINDOW.toMillis());
        assertThat(grafts).as("T took the eager copy: no spurious GRAFT from it")
                .noneMatch(g -> g.startsWith("t>"));
        assertThat(delivered.get("t")).as("T holds the item once").containsKey("sp19-z");
        assertThat(delivered.get("p")).as("P holds the item").containsKey("sp19-z");
        assertThat(delivered.get("t").get("sp19-z").get()).isEqualTo(1);
        assertThat(delivered.get("p").get("sp19-z").get()).isEqualTo(1);
        assertThat(backstopFound(nodes) - found0).as("the HAVE wake, not the backstop").isZero();
    }
}
