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
package ai.badmonkey.agentspaces.it.idle;

import ai.badmonkey.agentspaces.measure.Direction;
import ai.badmonkey.agentspaces.measure.Kind;
import ai.badmonkey.agentspaces.measure.Measure;
import ai.badmonkey.agentspaces.measure.Params;
import ai.badmonkey.agentspaces.measure.Rec;
import ai.badmonkey.agentspaces.measure.Unit;
import ai.badmonkey.agentspaces.measure.Wait;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The idle cost of a group of core peers (srad/agentspaces-event-loop.md EL-M-01 to EL-M-04 and EL-M-06): each member
 * is its own JVM ({@link IdleCostNode}) on TCP with the starter's defaults, so a member's CPU is its process's CPU. One
 * shape per run (owner rule 2026-11-11), chosen by system properties:
 *
 * <ul>
 *   <li>{@code idle.members} (2): members, the TypeScript one included;</li>
 *   <li>{@code idle.spaces} (4): 4 for the content, pdt, plans and work spaces, 1 for the work space alone;</li>
 *   <li>{@code idle.coalesceMillis} (20): the starter's {@code agentspaces.coalesce-millis}; 0 turns batching off;</li>
 *   <li>{@code idle.settleSeconds} (120) and {@code idle.idleSeconds} (300): the warm-up's floor, then the measured
 *       window;</li>
 *   <li>{@code idle.warmup} (true): the warm-up of EL-Q-PRF-01 and EL-Q-PRF-02 (owner decision 2026-11-23: idle-CPU
 *       readings start after a warm-up so JIT compilation is not counted). After the floor, every Java member's JIT
 *       compile count ({@code jstat -compiler}, read from outside the member) is read once a minute, and the window
 *       starts at the first minute in which no member compiled more than {@code idle.warmupCompilesPerMin} (5) methods,
 *       or {@code idle.warmupMaxSeconds} (1800) after the group formed, whichever is first (the report says which).
 *       {@code false} is the old method: the window starts at the floor. With the warm-up on, the floor's first
 *       {@code idle.idleSeconds} are also read as the cold reading, context only, under the old method's params;</li>
 *   <li>{@code idle.ts} (unset): an {@code agentspaces-typescript} directory; when set, the last member is an idle
 *       TypeScript peer that dials the first Java member, and every Java member counts its departures;</li>
 *   <li>{@code idle.detect} (true unless {@code idle.ts}) and {@code idle.newcomer} (likewise): after the window, one Java
 *       member is stopped with SIGSTOP and the time until no other view holds it is recorded; then, once the group is
 *       quiet again, a newcomer joins and each member's admission-to-targeted-round time is recorded.</li>
 * </ul>
 *
 * <p>Runs only with {@code -Didle.cost=true}. The floor, the warm-up's minutes and the window are the experiment's design,
 * not waits on a condition; every wait on a condition is a measured wait with a generous ceiling.
 */
@EnabledIfSystemProperty(named = "idle.cost", matches = "true")
class IdleCostIT {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Duration ANSWER = Duration.ofSeconds(30);

    private final List<Child> children = new ArrayList<>();

    /** One member process and its stdout, line by line. */
    static final class Child {
        final String name;
        final Process process;
        final Writer in;
        final BlockingQueue<String> lines = new LinkedBlockingQueue<>();
        final boolean java;
        String peer;
        long pid;

        Child(String name, Process process, boolean java) {
            this.name = name;
            this.process = process;
            this.java = java;
            this.in = new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8);
            Thread.ofVirtual().name("idle-read-" + name).start(() -> {
                try (BufferedReader out = new BufferedReader(new InputStreamReader(process.getInputStream(),
                        StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = out.readLine()) != null) {
                        lines.add(line);
                    }
                } catch (IOException ignored) {
                    // the process ended
                }
            });
        }

        /** The next line that starts with the prefix; other lines (logs) are skipped. */
        String next(String prefix) throws InterruptedException {
            long deadline = System.nanoTime() + ANSWER.toNanos(); // fm: (a) hang guard on a local pipe, not a band
            while (true) {
                String line = lines.poll(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                if (line == null) {
                    throw new AssertionError(name + " did not answer " + prefix + " (alive=" + process.isAlive() + ")");
                }
                if (line.startsWith(prefix)) {
                    return line.substring(prefix.length());
                }
                if (line.startsWith("STATS ") || line.startsWith("VIEW ")) {
                    continue; // an answer to an earlier command
                }
                System.out.println("[" + name + "] " + line);
            }
        }

        String ask(String command, String prefix) throws IOException, InterruptedException {
            in.write(command + "\n");
            in.flush();
            return next(prefix);
        }

        JsonNode stats() throws IOException, InterruptedException {
            return JSON.readTree(ask("stats", "STATS "));
        }

        List<String> view() throws IOException, InterruptedException {
            String ids = ask("view", "VIEW ").trim();
            return ids.isEmpty() ? List.of() : Arrays.asList(ids.split(","));
        }
    }

    @AfterEach
    void stopAll() {
        for (Child child : children) {
            try {
                child.in.write("quit\n");
                child.in.flush();
            } catch (IOException ignored) {
                // gone already
            }
        }
        for (Child child : children) {
            try {
                if (!child.process.waitFor(5, TimeUnit.SECONDS)) {
                    child.process.destroyForcibly();
                }
            } catch (InterruptedException e) {
                child.process.destroyForcibly();
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    void anIdleGroupsCost() throws Exception {
        int members = Integer.getInteger("idle.members", 2);
        int spaces = Integer.getInteger("idle.spaces", 4);
        long coalesce = Long.getLong("idle.coalesceMillis", 20);
        long settle = Long.getLong("idle.settleSeconds", 120);
        long idle = Long.getLong("idle.idleSeconds", 300);
        boolean warmup = Boolean.parseBoolean(System.getProperty("idle.warmup", "true"));
        long warmQuiet = Long.getLong("idle.warmupCompilesPerMin", 5);
        long warmMax = Long.getLong("idle.warmupMaxSeconds", 1800);
        String ts = System.getProperty("idle.ts");
        boolean detect = Boolean.parseBoolean(System.getProperty("idle.detect", String.valueOf(ts == null)));
        boolean newcomer = Boolean.parseBoolean(System.getProperty("idle.newcomer", String.valueOf(ts == null)));
        int javaMembers = ts == null ? members : members - 1;
        assertThat(javaMembers).as("at least two Java members").isGreaterThanOrEqualTo(2);
        Params shape = Params.of("members", members, "spaces", spaces, "coalesce-ms", coalesce,
                "ts", ts == null ? 0 : 1);
        String founding = "idle-cost-" + ProcessHandle.current().pid() + "-" + System.currentTimeMillis();
        Path dir = Files.createTempDirectory("idle-cost");
        Path out = Path.of("target", "idle-cost");
        Files.createDirectories(out);
        System.out.printf("IdleCostIT shape %s: %d Java members%s, floor %d s, %s, window %d s%n", shape, javaMembers,
                ts == null ? "" : " and one TypeScript member", settle, warmup ? "warm-up until at most " + warmQuiet
                        + " compilations a minute on every member (at most " + warmMax + " s after the group formed)"
                        : "no warm-up", idle);

        int seedPort = freePort();
        for (int i = 0; i < javaMembers; i++) {
            children.add(startJava("m" + i, i == 0 ? seedPort : freePort(), i == 0 ? 0 : seedPort, dir, founding,
                    spaces, coalesce, ts != null));
            Thread.sleep(i == 0 ? 2000 : 300); // stagger the starts as the scratch harness did
        }
        Child thin = null;
        if (ts != null) {
            thin = startTypeScript(ts, founding, seedPort);
            children.add(thin);
        }
        List<Child> java = children.stream().filter(c -> c.java).toList();
        // every member sees every other member
        Measure.await("core.harness.wait", Duration.ofSeconds(180), () -> converged(java, members - 1),
                Wait.with().poll(Duration.ofMillis(500)).params(Params.of("what", "converge", "members", members))
                        .diagnostics(() -> views(java)));
        long formed = System.nanoTime();
        Thread.sleep(Duration.ofSeconds(settle)); // the warm-up's floor (EL-M-01's settle): the design of the experiment

        // The warm-up (owner decision 2026-11-23): read each member's compile count once a minute from outside it, and
        // start the window at the first minute in which no member compiled more than warmQuiet methods.
        Map<String, JsonNode> coldBefore = snapshot(java);
        long coldStart = System.nanoTime();
        Map<String, JsonNode> coldAfter = null;
        List<Map<String, Object>> warmTrace = new ArrayList<>();
        boolean capped = false;
        if (warmup) {
            Map<String, Long> last = compiles(java);
            long lastAt = System.nanoTime();
            while (true) {
                Thread.sleep(Duration.ofSeconds(60)); // one warm-up minute: the design of the experiment
                Map<String, Long> now = compiles(java);
                long at = System.nanoTime();
                Map<String, Double> perMin = compileRates(last, now, (at - lastAt) / 60e9);
                last = now;
                lastAt = at;
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("afterFormedSeconds", Math.round((at - formed) / 1e9));
                row.put("compilesPerMin", perMin);
                warmTrace.add(row);
                if (coldAfter == null && at - coldStart >= TimeUnit.SECONDS.toNanos(idle)) {
                    coldAfter = snapshot(java); // the old method's window, read for context
                }
                if (quiet(perMin, java.size(), warmQuiet)) {
                    break;
                }
                if (at - formed >= TimeUnit.SECONDS.toNanos(warmMax)) {
                    capped = true;
                    break;
                }
            }
        }
        Map<String, JsonNode> before = warmup ? snapshot(java) : coldBefore;
        Map<String, Long> compilesBefore = compiles(java);
        long warmedSeconds = Math.round((System.nanoTime() - formed) / 1e9);
        System.out.printf("IdleCostIT window starts %d s after the group formed%s%n", warmedSeconds,
                !warmup ? " (no warm-up)" : capped ? " (warm-up capped; the JIT was still compiling)" : " (JIT quiet)");
        Thread.sleep(Duration.ofSeconds(idle)); // the measured window
        Map<String, JsonNode> after = snapshot(java);
        Map<String, Long> compilesAfter = compiles(java);

        Map<String, Object> report = new LinkedHashMap<>();
        report.put("shape", shape.asMap());
        report.put("founding", founding);
        Map<String, Object> warm = new LinkedHashMap<>();
        warm.put("on", warmup);
        warm.put("floorSeconds", settle);
        warm.put("quietCompilesPerMin", warmQuiet);
        warm.put("maxSeconds", warmMax);
        warm.put("windowStartAfterFormedSeconds", warmedSeconds);
        warm.put("capped", capped);
        warm.put("trace", warmTrace);
        report.put("warmup", warm);
        List<Map<String, Object>> perMember = new ArrayList<>();
        double[] cpu = new double[java.size()];
        double[] wakes = new double[java.size()];
        double[] tickWakes = new double[java.size()];
        double[] flushWakes = new double[java.size()];
        double[] lateP99 = new double[java.size()];
        double[] keepalive = new double[java.size()];
        double[] watchdogDetections = new double[java.size()];
        double[] watchdogReinits = new double[java.size()];
        double[] watchdogCpu = new double[java.size()];
        double[] timerCpu = new double[java.size()];
        double[] poolThreads = new double[java.size()];
        double[] jitWindowMs = new double[java.size()];
        double[] jitBeforeMs = new double[java.size()];
        boolean jitTimed = true;
        boolean threadCpu = false;
        List<Double> flushWaitP99 = new ArrayList<>();
        // participant (group prefix dropped) -> cause -> per-member rates a minute
        Map<String, Map<String, List<Double>>> trace = new TreeMap<>();
        for (int i = 0; i < java.size(); i++) {
            JsonNode a = before.get(java.get(i).name);
            JsonNode b = after.get(java.get(i).name);
            double minutes = (b.get("monoNanos").asLong() - a.get("monoNanos").asLong()) / 60e9;
            cpu[i] = 100.0 * (b.get("cpuNanos").asLong() - a.get("cpuNanos").asLong())
                    / (b.get("monoNanos").asLong() - a.get("monoNanos").asLong());
            // the node loop's timer and event wakes, less the backstop's bare sweeps: the runs that had work
            // (srad/agentspaces-event-loop.md §5.4)
            tickWakes[i] = (delta(a, b, "loop", "timerWakes") + delta(a, b, "loop", "eventWakes")
                    - delta(a, b, "loop", "backstopOnlyRuns")) / minutes;
            flushWakes[i] = delta(a, b, "loop", "flushRuns") / minutes;
            wakes[i] = tickWakes[i] + flushWakes[i];
            lateP99[i] = b.get("loop").get("latenessP99Nanos").asLong() / 1e6;
            keepalive[i] = delta(a, b, "keepalive", "refreshFrames") / minutes;
            JsonNode wd = b.get("watchdog"); // the process watchdog, since the member started (phase 1 part 3)
            if (wd != null) {
                watchdogDetections[i] = wd.get("detections").asLong();
                watchdogReinits[i] = wd.get("reinits").asLong();
            }
            JsonNode pa = a.get("platformThreadCpuNanos");
            JsonNode pb = b.get("platformThreadCpuNanos");
            if (pa != null && pb != null) { // phase 4 gate: the watchdog's and the timer's own CPU, the pool at idle
                threadCpu = true;
                double window = b.get("monoNanos").asLong() - a.get("monoNanos").asLong();
                watchdogCpu[i] = 100.0 * (pb.path("agentspaces-watchdog").asLong() - pa.path("agentspaces-watchdog")
                        .asLong()) / window;
                timerCpu[i] = 100.0 * (pb.path("agentspaces-timer").asLong() - pa.path("agentspaces-timer").asLong())
                        / window;
                int loopThreads = 0;
                for (var it = pb.fieldNames(); it.hasNext(); ) {
                    loopThreads += it.next().startsWith("agentspaces-loop-") ? 1 : 0;
                }
                poolThreads[i] = loopThreads;
            }
            JsonNode waits = b.get("loop").get("flushWaitSamples");
            if (waits != null && waits.asLong() > 0) { // the flush timer's first-frame wait (EL-B-04), phase 1 on
                flushWaitP99.add(b.get("loop").get("flushWaitP99Nanos").asLong() / 1e6);
            }
            long jitA = a.path("jitMillis").asLong(-1);
            long jitB = b.path("jitMillis").asLong(-1);
            jitTimed &= jitA >= 0 && jitB >= 0;
            jitBeforeMs[i] = jitA;
            jitWindowMs[i] = jitB - jitA;
            double jitPercent = 100.0 * jitWindowMs[i] * 1e6 / (b.get("monoNanos").asLong() - a.get("monoNanos").asLong());
            String who = java.get(i).name;
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("member", who);
            m.put("cpuPercent", round(cpu[i]));
            // the JIT in the window (compile time is the compiler threads' elapsed time; context, not subtracted)
            m.put("jitCompilesInWindow", compilesAfter.getOrDefault(who, -1L) - compilesBefore.getOrDefault(who, 0L));
            m.put("jitMillisInWindow", jitWindowMs[i]);
            m.put("jitPercentInWindow", round4(jitPercent));
            m.put("cpuLessJitPercent", round4(cpu[i] - jitPercent));
            m.put("jitCompilesBeforeWindow", compilesBefore.getOrDefault(who, -1L));
            m.put("jitMillisBeforeWindow", jitA);
            m.put("wakesPerMin", round(wakes[i]));
            m.put("tickWakesPerMin", round(tickWakes[i]));
            m.put("flushWakesPerMin", round(flushWakes[i]));
            m.put("flushesWithFramesPerMin", round(delta(a, b, "loop", "flushesWithFrames") / minutes));
            m.put("eventWakesPerMin", round(delta(a, b, "loop", "eventWakes") / minutes));
            m.put("runsPerMin", round(delta(a, b, "loop", "runs") / minutes));
            m.put("backstopRunsPerMin", round(delta(a, b, "loop", "backstopRuns") / minutes));
            m.put("backstopFoundWork", delta(a, b, "loop", "backstopFoundWork"));
            m.put("backstopFoundBy", b.get("loop").get("backstopFoundBy")); // since start, by participant (EL-Q-REL-03)
            m.put("overruns", delta(a, b, "loop", "overruns"));
            m.put("slowRuns", delta(a, b, "loop", "slowRuns"));
            m.put("latenessP50Ms", round(b.get("loop").get("latenessP50Nanos").asLong() / 1e6));
            m.put("latenessP99Ms", round(lateP99[i]));
            m.put("latenessMaxMs", round(b.get("loop").get("latenessMaxNanos").asLong() / 1e6));
            m.put("busyPercent", round(100.0 * delta(a, b, "loop", "busyNanos")
                    / (b.get("monoNanos").asLong() - a.get("monoNanos").asLong())));
            m.put("keepaliveFramesPerMin", round(keepalive[i]));
            m.put("keepaliveRefreshesPerMin", round(delta(a, b, "keepalive", "refreshes") / minutes));
            m.put("selfAdFloodsPerMin", round(delta(a, b, "keepalive", "floods") / minutes));
            if (b.get("loop").has("dueRuns")) { // the per-participant wake counters (phase 4)
                m.put("dueRunsPerMin", byParticipant(a.get("loop").get("dueRuns"), b.get("loop").get("dueRuns"),
                        minutes, trace, "due"));
                m.put("askedRunsPerMin", byParticipant(a.get("loop").get("askedRuns"),
                        b.get("loop").get("askedRuns"), minutes, trace, "asked"));
                m.put("wakeCallsPerMin", byParticipant(a.get("loop").get("wakeCalls"),
                        b.get("loop").get("wakeCalls"), minutes, trace, "raised"));
                m.put("timer", b.get("timer"));
            }
            m.put("sentPerMin", perMinute(a.get("sentByKind"), b.get("sentByKind"), minutes));
            m.put("receivedPerMin", perMinute(a.get("receivedByKind"), b.get("receivedByKind"), minutes));
            m.put("viewSize", b.get("view").size());
            m.put("threads", b.get("threads").asInt());
            if (pb != null) {
                m.put("watchdogCpuPercent", round4(watchdogCpu[i]));
                m.put("timerCpuPercent", round4(timerCpu[i]));
                m.put("loopPoolThreads", (int) poolThreads[i]);
                m.put("platformThreads", pb.size());
            }
            if (wd != null) {
                m.put("watchdog", wd);
                m.put("loopGeneration", b.get("loop").get("generation").asLong());
                m.put("loopHealth", b.get("loop").get("health").asText());
            }
            if (thin != null) {
                JsonNode gone = b.get("departures").get(thin.peer);
                m.put("thinPeerDepartures", gone == null ? 0 : gone.asLong());
                m.put("thinPeerInViewAtEnd", contains(b.get("view"), thin.peer));
            }
            perMember.add(m);
        }
        report.put("members", perMember);
        if (!trace.isEmpty()) {
            // The wake trace: per participant name, the median per member a minute of the runs it caused.
            Map<String, Map<String, Double>> medians = new TreeMap<>();
            trace.forEach((name, causes) -> {
                Map<String, Double> row = new LinkedHashMap<>();
                causes.forEach((cause, rates) -> {
                    List<Double> sorted = new ArrayList<>(rates);
                    while (sorted.size() < java.size()) {
                        sorted.add(0.0); // members where it caused nothing
                    }
                    Collections.sort(sorted);
                    row.put(cause, round(sorted.get(sorted.size() / 2)));
                });
                medians.put(name, row);
            });
            report.put("wakeTrace", medians);
            System.out.println("IdleCostIT wake trace (median per member a minute; due = a timer wake's cause, asked ="
                    + " an event wake's cause, raised = wakes raised by it): " + medians);
        }
        // EL-Q-PRF-01 and EL-Q-PRF-02 are judged on the warm window (warmup=jit); the old method's series keep the shape
        Params idleShape = warmup ? with(shape, "warmup", "jit") : shape;
        Rec rec = Rec.with().params(idleShape);
        Measure.samples("core.idle.peer-cpu", Unit.PERCENT, Direction.LOWER, cpu, rec.kind(Kind.RESOURCE));
        Measure.samples("core.idle.wakes", Unit.COUNT, Direction.LOWER, wakes,
                Rec.with().params(with(idleShape, "source", "all")).kind(Kind.COUNT));
        Measure.samples("core.idle.wakes", Unit.COUNT, Direction.LOWER, tickWakes,
                Rec.with().params(with(idleShape, "source", "tick")).kind(Kind.COUNT));
        Measure.samples("core.idle.wakes", Unit.COUNT, Direction.LOWER, flushWakes,
                Rec.with().params(with(idleShape, "source", "flush")).kind(Kind.COUNT));
        if (warmup) {
            Measure.record("core.idle.warmup", Unit.S, Direction.LOWER, warmedSeconds,
                    Rec.with().params(with(shape, "capped", capped ? 1 : 0)).kind(Kind.PERF));
        }
        if (jitTimed) {
            Measure.samples("core.idle.jit-time", Unit.MS, Direction.LOWER, jitBeforeMs,
                    Rec.with().params(with(idleShape, "part", "before-window")).kind(Kind.RESOURCE));
            Measure.samples("core.idle.jit-time", Unit.MS, Direction.LOWER, jitWindowMs,
                    Rec.with().params(with(idleShape, "part", "window")).kind(Kind.RESOURCE));
        }
        if (coldAfter != null) { // context only: the old method's window (the floor's first idleSeconds)
            double[] coldCpu = new double[java.size()];
            double[] coldWakes = new double[java.size()];
            List<Map<String, Object>> cold = new ArrayList<>();
            for (int i = 0; i < java.size(); i++) {
                JsonNode a = coldBefore.get(java.get(i).name);
                JsonNode b = coldAfter.get(java.get(i).name);
                long span = b.get("monoNanos").asLong() - a.get("monoNanos").asLong();
                coldCpu[i] = 100.0 * (b.get("cpuNanos").asLong() - a.get("cpuNanos").asLong()) / span;
                coldWakes[i] = (delta(a, b, "loop", "timerWakes") + delta(a, b, "loop", "eventWakes")
                        - delta(a, b, "loop", "backstopOnlyRuns") + delta(a, b, "loop", "flushRuns")) / (span / 60e9);
                long jit = b.path("jitMillis").asLong(-1) - a.path("jitMillis").asLong(-1);
                cold.add(Map.of("member", java.get(i).name, "cpuPercent", round(coldCpu[i]),
                        "wakesPerMin", round(coldWakes[i]), "jitMillis", jit,
                        "jitPercent", round4(100.0 * jit * 1e6 / span), "seconds", Math.round(span / 1e9)));
            }
            report.put("cold", cold);
            Measure.samples("core.idle.peer-cpu", Unit.PERCENT, Direction.LOWER, coldCpu,
                    Rec.with().params(shape).kind(Kind.RESOURCE));
            Measure.samples("core.idle.wakes", Unit.COUNT, Direction.LOWER, coldWakes,
                    Rec.with().params(with(shape, "source", "all")).kind(Kind.COUNT));
        }
        Measure.samples("core.loop.lateness", Unit.MS, Direction.LOWER, lateP99,
                Rec.with().params(with(shape, "stat", "p99")).kind(Kind.PERF));
        Measure.samples("core.idle.keepalive-frames", Unit.FRAMES, Direction.LOWER, keepalive,
                Rec.with().params(shape).kind(Kind.COUNT));
        if (after.values().iterator().next().get("watchdog") != null) {
            Measure.samples("core.watchdog.detections", Unit.COUNT, Direction.LOWER, watchdogDetections,
                    Rec.with().params(shape).kind(Kind.COUNT));
            Measure.samples("core.watchdog.reinits", Unit.COUNT, Direction.LOWER, watchdogReinits,
                    Rec.with().params(shape).kind(Kind.COUNT));
        }
        if (threadCpu) {
            Measure.samples("core.watchdog.cpu", Unit.PERCENT, Direction.LOWER, watchdogCpu,
                    Rec.with().params(shape).kind(Kind.RESOURCE));
            Measure.samples("core.timer.cpu", Unit.PERCENT, Direction.LOWER, timerCpu,
                    Rec.with().params(shape).kind(Kind.RESOURCE));
            Measure.samples("core.loop.pool-threads", Unit.COUNT, Direction.LOWER, poolThreads,
                    Rec.with().params(with(shape, "load", "idle")).kind(Kind.COUNT));
        }
        if (!flushWaitP99.isEmpty()) {
            Measure.samples("core.flush.first-frame-latency", Unit.MS, Direction.LOWER,
                    flushWaitP99.stream().mapToDouble(Double::doubleValue).toArray(),
                    Rec.with().params(with(shape, "stat", "p99")).kind(Kind.PERF));
        }

        if (thin != null) {
            long departures = 0;
            int absentAtEnd = 0;
            for (Map<String, Object> m : perMember) {
                departures += (Long) m.get("thinPeerDepartures");
                absentAtEnd += Boolean.TRUE.equals(m.get("thinPeerInViewAtEnd")) ? 0 : 1;
            }
            report.put("thinPeer", Map.of("peer", thin.peer, "departuresInWindow", departures,
                    "javaViewsWithoutItAtEnd", absentAtEnd, "itsOwnView", thin.view().size()));
            Measure.record("core.thin-peer.departures", Unit.COUNT, Direction.LOWER, departures,
                    Rec.with().params(shape).kind(Kind.COUNT));
        }

        if (detect) {
            Child victim = java.get(java.size() - 1);
            List<Child> others = java.subList(0, java.size() - 1);
            new ProcessBuilder("kill", "-STOP", Long.toString(victim.pid)).inheritIO().start().waitFor();
            // fm: (a) ceiling 180 s: about three member TTLs at ten members (54 s, EL-E-05); the value is the series
            var detected = Measure.await("core.detect.silent-member", Duration.ofSeconds(180),
                    () -> noneHolds(others, victim.peer),
                    Wait.with().poll(Duration.ofMillis(200)).params(Params.of("members", members, "harness", "tcp",
                            "coalesce-ms", coalesce)).diagnostics(() -> views(others)));
            report.put("detectSilentMemberMs", round(detected.value()));
            victim.process.destroyForcibly();
            children.remove(victim);
            if (newcomer) {
                Thread.sleep(Duration.ofSeconds(60)); // the group goes quiet again: the design of the experiment
                Map<String, Integer> seen = new LinkedHashMap<>();
                for (Child c : others) {
                    seen.put(c.name, c.stats().get("newcomerRoundMillis").size());
                }
                Child late = startJava("late", freePort(), seedPort, dir, founding, spaces, coalesce, false);
                children.add(late);
                Measure.await("core.harness.wait", Duration.ofSeconds(120), () -> newcomerRoundEverywhere(others,
                                seen),
                        Wait.with().poll(Duration.ofMillis(200)).params(Params.of("what", "newcomer-round",
                                "members", members)).diagnostics(() -> views(others)));
                double[] rounds = new double[others.size()];
                for (int i = 0; i < others.size(); i++) {
                    JsonNode list = others.get(i).stats().get("newcomerRoundMillis");
                    rounds[i] = list.get(list.size() - 1).asDouble();
                }
                report.put("newcomerTargetedRoundMs", rounds);
                Measure.samples("core.newcomer.targeted-round", Unit.MS, Direction.LOWER, rounds,
                        Rec.with().params(Params.of("members", members, "harness", "tcp", "coalesce-ms", coalesce))
                                .kind(Kind.PERF));
            }
        }
        String name = "members-" + members + "-spaces-" + spaces + "-coalesce-" + coalesce + (ts == null ? "" : "-ts");
        ObjectNode tree = JSON.valueToTree(report);
        Files.writeString(out.resolve(name + ".json"), JSON.writerWithDefaultPrettyPrinter().writeValueAsString(tree));
        System.out.println("IDLE-COST " + JSON.writeValueAsString(tree));
        for (Child c : java) {
            assertThat(c.process.isAlive() || !children.contains(c)).as(c.name + " is alive").isTrue();
        }
    }

    static Child startJava(String name, int port, int seed, Path dir, String founding, int spaces, long coalesce,
                            boolean watchViews) throws IOException, InterruptedException {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String cp = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        List<String> command = new ArrayList<>(List.of(java, "-cp", cp));
        String jvm = System.getProperty("idle.jvmArgs", "");
        if (!jvm.isBlank()) {
            command.addAll(Arrays.asList(jvm.trim().split("\\s+")));
        }
        command.addAll(List.of(IdleCostNode.class.getName(), name, Integer.toString(port), Integer.toString(seed),
                dir.toString(), founding, Integer.toString(spaces), Long.toString(coalesce),
                Boolean.toString(watchViews)));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        Child child = new Child(name, process, true);
        String[] ready = child.next("READY ").split(" ");
        child.peer = ready[0];
        child.pid = Long.parseLong(ready[2]);
        return child;
    }

    static Child startTypeScript(String base, String founding, int seedPort) throws IOException, InterruptedException {
        Path script = Path.of("src", "test", "resources", "idle", "ts-idle-peer.mjs").toAbsolutePath();
        Process process = new ProcessBuilder("node", script.toString(), base, founding, Integer.toString(seedPort))
                .redirectErrorStream(true).start();
        Child child = new Child("ts", process, false);
        String[] ready = child.next("READY ").split(" ");
        child.peer = ready[0];
        child.pid = Long.parseLong(ready[2]);
        return child;
    }

    /**
     * Each Java member's JIT compile count, read with {@code jstat -compiler} from the JVM's performance counters (a
     * memory-mapped file), so reading it runs nothing inside the member; -1 when it cannot be read.
     */
    static Map<String, Long> compiles(List<Child> java) throws IOException, InterruptedException {
        Path jstat = Path.of(System.getProperty("java.home"), "bin", "jstat");
        String command = Files.isExecutable(jstat) ? jstat.toString() : "jstat";
        Map<String, Long> out = new LinkedHashMap<>();
        for (Child c : java) {
            Process p = new ProcessBuilder(command, "-compiler", Long.toString(c.pid)).redirectErrorStream(true).start();
            String text = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            out.put(c.name, p.waitFor() == 0 ? compiledCount(text) : -1L);
        }
        return out;
    }

    /** The {@code Compiled} column of {@code jstat -compiler}'s output, or -1. */
    static long compiledCount(String jstatOutput) {
        String[] lines = jstatOutput.strip().split("\\R");
        if (lines.length < 2 || !lines[0].trim().startsWith("Compiled")) {
            return -1;
        }
        try {
            return Long.parseLong(lines[1].trim().split("\\s+")[0]);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /** Compilations a minute per member between two reads; a member unread in either is left out. */
    static Map<String, Double> compileRates(Map<String, Long> before, Map<String, Long> after, double minutes) {
        Map<String, Double> out = new LinkedHashMap<>();
        after.forEach((name, n) -> {
            Long was = before.get(name);
            if (n >= 0 && was != null && was >= 0) {
                out.put(name, round((n - was) / minutes));
            }
        });
        return out;
    }

    /** The warm-up's end: all {@code members} read, and none compiled more than {@code max} methods a minute. */
    static boolean quiet(Map<String, Double> perMin, int members, long max) {
        return perMin.size() == members && perMin.values().stream().allMatch(r -> r <= max);
    }

    static Map<String, JsonNode> snapshot(List<Child> java) throws IOException, InterruptedException {
        Map<String, JsonNode> out = new LinkedHashMap<>();
        for (Child c : java) {
            out.put(c.name, c.stats());
        }
        return out;
    }

    static boolean converged(List<Child> java, int others) {
        try {
            for (Child c : java) {
                if (c.view().size() < others) {
                    return false;
                }
            }
            return true;
        } catch (IOException | InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    static boolean noneHolds(List<Child> others, String peer) {
        try {
            for (Child c : others) {
                if (c.view().contains(peer)) {
                    return false;
                }
            }
            return true;
        } catch (IOException | InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    private static boolean newcomerRoundEverywhere(List<Child> others, Map<String, Integer> seen) {
        try {
            for (Child c : others) {
                if (c.stats().get("newcomerRoundMillis").size() <= seen.get(c.name)) {
                    return false;
                }
            }
            return true;
        } catch (IOException | InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    static String views(List<Child> java) {
        StringBuilder out = new StringBuilder();
        for (Child c : java) {
            try {
                out.append(c.name).append('=').append(c.view().size()).append(' ');
            } catch (IOException | InterruptedException | AssertionError e) {
                out.append(c.name).append("=? ");
            }
        }
        return out.toString();
    }

    static boolean contains(JsonNode array, String value) {
        for (JsonNode n : array) {
            if (n.asText().equals(value)) {
                return true;
            }
        }
        return false;
    }

    static long delta(JsonNode a, JsonNode b, String group, String field) {
        return b.get(group).get(field).asLong() - a.get(group).get(field).asLong();
    }

    /**
     * Per participant a minute, with the group id dropped from each name so a member's four spaces and its groups
     * read as one row ({@code <group>/anti-entropy} reads {@code anti-entropy}; {@code group:<id>/other} reads
     * {@code group:*}/other); each member's rate also goes into the trace under the cause.
     */
    private static Map<String, Double> byParticipant(JsonNode a, JsonNode b, double minutes,
                                                     Map<String, Map<String, List<Double>>> trace, String cause) {
        Map<String, Double> out = new TreeMap<>();
        if (b == null) {
            return out;
        }
        b.fieldNames().forEachRemaining(k -> {
            long d = b.get(k).asLong() - (a != null && a.has(k) ? a.get(k).asLong() : 0);
            if (d > 0) {
                out.merge(participantName(k), d / minutes, Double::sum);
            }
        });
        out.replaceAll((k, v) -> round(v));
        out.forEach((k, v) -> trace.computeIfAbsent(k, x -> new TreeMap<>())
                .computeIfAbsent(cause, x -> new ArrayList<>()).add(v));
        return out;
    }

    private static String participantName(String key) {
        if (key.startsWith("group:")) {
            int slash = key.indexOf('/');
            return "group:*" + (slash < 0 ? "" : key.substring(slash));
        }
        int slash = key.indexOf('/');
        return slash < 0 ? key : key.substring(slash + 1);
    }

    private static Map<String, Double> perMinute(JsonNode a, JsonNode b, double minutes) {
        Map<String, Double> out = new LinkedHashMap<>();
        b.fieldNames().forEachRemaining(k -> {
            long d = b.get(k).asLong() - (a.has(k) ? a.get(k).asLong() : 0);
            if (d > 0) {
                out.put(k, round(d / minutes));
            }
        });
        return out;
    }

    private static Params with(Params shape, String key, Object value) {
        Map<String, Object> map = new LinkedHashMap<>(shape.asMap());
        map.put(key, value);
        List<Object> pairs = new ArrayList<>();
        map.forEach((k, v) -> {
            pairs.add(k);
            pairs.add(v);
        });
        return Params.of(pairs.toArray());
    }

    static double round(double v) {
        return Math.round(v * 1000.0) / 1000.0;
    }

    /** Four decimals, for shares of a core under 0.01% (core.watchdog.cpu's goal). */
    static double round4(double v) {
        return Math.round(v * 10000.0) / 10000.0;
    }

    /**
     * The next port below every OS's ephemeral range that binds now. A port from {@code new ServerSocket(0)} is an
     * ephemeral one, which an outbound connection of the running members can take between this check and the member's
     * own bind (the ten-member run of 2026-11-21 failed so: "m9: Address already in use"); outbound connections never
     * take a port below 32768 (Linux) or 49152 (macOS), so ports are handed out from a random base in 21000 to 28999.
     */
    static synchronized int freePort() throws IOException {
        while (nextPort < 32_000) {
            int port = nextPort++;
            try (ServerSocket socket = new ServerSocket()) {
                socket.setReuseAddress(false);
                socket.bind(new java.net.InetSocketAddress("127.0.0.1", port));
                return port;
            } catch (IOException taken) {
                // in use: the next one
            }
        }
        throw new IOException("no free port below the ephemeral range");
    }

    private static int nextPort = 21_000 + new java.util.Random().nextInt(8_000);
}
