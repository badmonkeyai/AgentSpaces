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
package ai.badmonkey.agentspaces.it.idle;

import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.measure.Direction;
import ai.badmonkey.agentspaces.measure.Kind;
import ai.badmonkey.agentspaces.measure.Measure;
import ai.badmonkey.agentspaces.measure.Params;
import ai.badmonkey.agentspaces.measure.Rec;
import ai.badmonkey.agentspaces.measure.Unit;
import ai.badmonkey.agentspaces.measure.Wait;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * EL-SP-30 over TCP (srad/agentspaces-event-loop.md §3.11, EL-E-06, R16): a mixed-TTL group of one released 0.6.0 Java
 * member ({@code idle/legacy/LegacyIdleNode.java}, compiled here against the 0.6.0 jars: a 30 s member TTL, the
 * reshuffled probe order, no targeted relay), four members of this tree (the new member TTL, the 30 s keepalive) and
 * one idle TypeScript peer seeded on a new member; idle {@code mixed.idleSeconds} after a 2-minute settle (the run of
 * record used 10 minutes: one 30-minute run, EL-SP-31 at ten, is enough, owner rule 2026-11-22).
 *
 * <p>First it asserts that the 0.6.0 member admits the new members' cards (risk 5: a wire refusal would look like a TTL
 * failure). PREVENTED: no Java member leaves any view, and no new member drops the TypeScript peer; each new member's
 * quiet probe cycle is the one a 30 s keepalive gives at its view size (EL-E-06). Its PING rate against the 0.6.0
 * member's is recorded, not judged: 0.6.0 stretches less and has no probe rest, so the rates differ by design. The 0.6.0 member's TypeScript departures are
 * recorded, not judged.
 *
 * <p>EL-SP-30b ({@code -Dmixed.thinSeed=legacy}, the phase-2 gate, §10.15): the TypeScript peer dials the 0.6.0 member
 * instead and names it in {@code relay.via}, so every new member's holder rounds go to a released member, which has no
 * targeted relay but answers a PING_REQ as 0.6.0 always did. PREVENTED as above, and every new member asked the 0.6.0
 * member alone as the holder and ran holder rounds.
 *
 * <p>Then, unless {@code -Dmixed.sigstop=false}, silent members in the mixed group: a new member is stopped with SIGSTOP
 * and every other Java member must drop it within its own member TTL plus 5 s (the 0.6.0 member at its 30 s), then the
 * 0.6.0 member is stopped and every new member must drop it within its quiet probe cycle plus 5 s; neither may come
 * back while stopped (20 s each).
 *
 * <p>Runs only with {@code -Dmixed.ttl=true}; {@code mixed.legacyCp} names a file holding the 0.6.0 classpath (one
 * line, as {@code mvn dependency:build-classpath} writes it); {@code mixed.ts} the {@code agentspaces-typescript}
 * directory; {@code mixed.idleSeconds} (1800).
 */
@EnabledIfSystemProperty(named = "mixed.ttl", matches = "true")
class MixedTtlIT {

    private static final ObjectMapper JSON = new ObjectMapper();
    private final List<IdleCostIT.Child> children = new ArrayList<>();

    @AfterEach
    void stopAll() {
        for (IdleCostIT.Child child : children) {
            try {
                child.in.write("quit\n");
                child.in.flush();
            } catch (IOException ignored) {
                // gone already
            }
        }
        for (IdleCostIT.Child child : children) {
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

    private static IdleCostIT.Child startLegacy(Path classes, String legacyCp, int port, int seed, String founding)
            throws IOException, InterruptedException {
        String launcher = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Process process = new ProcessBuilder(launcher, "-cp", classes + java.io.File.pathSeparator + legacyCp,
                "LegacyIdleNode", "legacy", Integer.toString(port), Integer.toString(seed), founding)
                .redirectErrorStream(true).start();
        IdleCostIT.Child child = new IdleCostIT.Child("legacy", process, true);
        String[] ready = child.next("READY ").split(" ");
        child.peer = ready[0];
        child.pid = Long.parseLong(ready[2]);
        return child;
    }

    /**
     * Stops {@code victim} with SIGSTOP, so its links stay open and it answers nothing, and returns the milliseconds
     * until each watcher's view no longer holds it, polled every 200 ms up to {@code ceilingMillis}; a watcher still
     * holding it at the ceiling is absent from the map.
     */
    private static Map<String, Long> stopAndTime(IdleCostIT.Child victim, List<IdleCostIT.Child> watchers,
                                                 long ceilingMillis) throws IOException, InterruptedException {
        new ProcessBuilder("kill", "-STOP", Long.toString(victim.pid)).inheritIO().start().waitFor();
        long start = System.nanoTime();
        Map<String, Long> gone = new LinkedHashMap<>();
        while (gone.size() < watchers.size() && (System.nanoTime() - start) / 1_000_000L < ceilingMillis) {
            for (IdleCostIT.Child w : watchers) {
                if (!gone.containsKey(w.name) && !w.view().contains(victim.peer)) {
                    gone.put(w.name, (System.nanoTime() - start) / 1_000_000L);
                }
            }
            Thread.sleep(200);
        }
        return gone;
    }

    /** Watches for {@code window} that no watcher admits the stopped {@code victim} again; returns the re-admissions. */
    private static long returnsWhileStopped(IdleCostIT.Child victim, List<IdleCostIT.Child> watchers, Duration window)
            throws IOException, InterruptedException {
        long returns = 0;
        long end = System.nanoTime() + window.toNanos();
        while (System.nanoTime() < end) {
            for (IdleCostIT.Child w : watchers) {
                returns += w.view().contains(victim.peer) ? 1 : 0;
            }
            Thread.sleep(500);
        }
        return returns;
    }

    private static double perMinute(JsonNode a, JsonNode b, String kind) {
        double minutes = (b.get("monoNanos").asLong() - a.get("monoNanos").asLong()) / 60e9;
        long before = a.get("sentByKind").has(kind) ? a.get("sentByKind").get(kind).asLong() : 0;
        long after = b.get("sentByKind").has(kind) ? b.get("sentByKind").get(kind).asLong() : 0;
        return (after - before) / minutes;
    }

    @Test
    void aReleasedMemberAndNewMembersKeepOneAnotherAndTheNewOnesKeepTheThinPeer() throws Exception {
        String legacyCp = Files.readString(Path.of(System.getProperty("mixed.legacyCp"))).trim();
        String ts = System.getProperty("mixed.ts");
        assertThat(ts).as("-Dmixed.ts=<agentspaces-typescript dir>").isNotBlank();
        long settle = Long.getLong("mixed.settleSeconds", 120);
        long idle = Long.getLong("mixed.idleSeconds", 1800);
        boolean legacyHolds = "legacy".equals(System.getProperty("mixed.thinSeed", "n0")); // EL-SP-30b
        Path classes = Files.createTempDirectory("legacy-classes");
        Path source = Path.of("src", "test", "resources", "idle", "legacy", "LegacyIdleNode.java").toAbsolutePath();
        JavaCompiler javac = ToolProvider.getSystemJavaCompiler();
        int compiled = javac.run(null, null, null, "-nowarn", "-cp", legacyCp, "-d", classes.toString(),
                source.toString());
        assertThat(compiled).as("LegacyIdleNode compiles against the 0.6.0 jars").isZero();

        String founding = "mixed-ttl-" + ProcessHandle.current().pid() + "-" + System.currentTimeMillis();
        Path dir = Files.createTempDirectory("mixed-ttl");
        Path out = Path.of("target", "mixed-ttl");
        Files.createDirectories(out);
        int seedPort = IdleCostIT.freePort();
        List<IdleCostIT.Child> fresh = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            IdleCostIT.Child c = IdleCostIT.startJava("n" + i, i == 0 ? seedPort : IdleCostIT.freePort(),
                    i == 0 ? 0 : seedPort, dir, founding, 1, 20, true);
            fresh.add(c);
            children.add(c);
            Thread.sleep(i == 0 ? 2000 : 300);
        }
        int legacyPort = IdleCostIT.freePort();
        IdleCostIT.Child legacy = startLegacy(classes, legacyCp, legacyPort, seedPort, founding);
        children.add(legacy);
        IdleCostIT.Child thin = IdleCostIT.startTypeScript(ts, founding, legacyHolds ? legacyPort : seedPort);
        children.add(thin);
        List<IdleCostIT.Child> java = new ArrayList<>(fresh);
        java.add(legacy);

        // risk 5 first: the released member admits every new member's card, and they admit it
        Measure.await("core.harness.wait", Duration.ofSeconds(120), () -> {
            try {
                List<String> v = legacy.view();
                return fresh.stream().allMatch(f -> v.contains(f.peer));
            } catch (IOException | InterruptedException e) {
                throw new AssertionError(e);
            }
        }, Wait.with().poll(Duration.ofMillis(500)).params(Params.of("what", "legacy-admits", "members", 6))
                .diagnostics(() -> IdleCostIT.views(java)));
        Measure.await("core.harness.wait", Duration.ofSeconds(180), () -> IdleCostIT.converged(fresh, 5),
                Wait.with().poll(Duration.ofMillis(500)).params(Params.of("what", "converge", "members", 6))
                        .diagnostics(() -> IdleCostIT.views(java)));
        Thread.sleep(Duration.ofSeconds(settle)); // the settle: the design of the experiment
        Map<String, JsonNode> before = IdleCostIT.snapshot(java);
        Thread.sleep(Duration.ofSeconds(idle)); // the idle window
        Map<String, JsonNode> after = IdleCostIT.snapshot(java);

        Map<String, Object> report = new LinkedHashMap<>();
        List<String> faults = new ArrayList<>();
        List<Map<String, Object>> rows = new ArrayList<>();
        double legacyPings = perMinute(before.get("legacy"), after.get("legacy"), "PING");
        // today's probe rule: a membership configured with an explicit 30 s TTL keeps a 30 s keepalive (owner answer 10)
        GroupMembership thirtySecond = new GroupMembership(PeerId.of(fresh.get(0).peer), Clock.systemUTC(),
                new Random(0), new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2));
        List<String> javaPeers = java.stream().map(c -> c.peer).toList();
        long legacyThinDepartures = 0;
        for (IdleCostIT.Child c : java) {
            JsonNode b = after.get(c.name);
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("member", c.name);
            row.put("pingPerMin", IdleCostIT.round(perMinute(before.get(c.name), b, "PING")));
            row.put("pingReqPerMin", IdleCostIT.round(perMinute(before.get(c.name), b, "PING_REQ")));
            row.put("rumorPerMin", IdleCostIT.round(perMinute(before.get(c.name), b, "RUMOR")));
            if (b.has("keepalive")) {
                double minutes = (b.get("monoNanos").asLong() - before.get(c.name).get("monoNanos").asLong()) / 60e9;
                row.put("keepaliveFramesPerMin", IdleCostIT.round(
                        IdleCostIT.delta(before.get(c.name), b, "keepalive", "refreshFrames") / minutes));
            }
            row.put("departures", b.get("departures"));
            row.put("viewSize", b.get("view").size());
            rows.add(row);
            JsonNode gone = b.get("departures");
            for (String peer : javaPeers) {
                if (!peer.equals(c.peer) && gone.has(peer) && gone.get(peer).asLong() > 0) {
                    faults.add(c.name + " dropped Java member " + peer + " " + gone.get(peer).asLong() + " time(s)");
                }
            }
            long thinGone = gone.has(thin.peer) ? gone.get(thin.peer).asLong() : 0;
            if (c == legacy) {
                legacyThinDepartures = thinGone; // recorded, not judged: 0.6.0 has no targeted relay
            } else if (thinGone > 0 || !IdleCostIT.contains(b.get("view"), thin.peer)) {
                faults.add(c.name + " dropped the TypeScript peer (" + thinGone + " departures)");
            }
            if (c != legacy) {
                // Recorded, not judged: 0.6.0 caps the quiet stretch at two doublings and has no probe rest (idle-loop
                // O6, 2026-11-18), so this tree probed less than 0.6.0 before decision 17 (§10.13).
                double ratio = legacyPings == 0 ? 0 : perMinute(before.get(c.name), b, "PING") / legacyPings;
                row.put("pingCadenceVsLegacy", IdleCostIT.round(ratio));
                // Judged (EL-E-06): the quiet probe cycle is today's, the one a 30 s keepalive gives at this view size,
                // not one stretched by the 45 s member TTL this node judges others by.
                int others = b.get("view").size();
                long cycle = b.get("membership").get("quietProbeCycleMillis").asLong();
                long today = others * thirtySecond.maxQuietIntervalMillis(others);
                row.put("quietProbeCycleMillis", cycle);
                row.put("quietProbeCycleAt30sMillis", today);
                if (cycle != today) {
                    faults.add(c.name + " has a quiet probe cycle of " + cycle + " ms at " + others
                            + " others, not the 30 s keepalive's " + today + " ms");
                }
                JsonNode pa = before.get(c.name).get("membership");
                JsonNode pb = b.get("membership");
                long holderRounds = pb.get("holderRounds").asLong() - pa.get("holderRounds").asLong();
                row.put("holderRounds", holderRounds);
                row.put("escalations", pb.get("escalations").asLong() - pa.get("escalations").asLong());
                row.put("randomPingReqs", pb.get("randomPingReqs").asLong() - pa.get("randomPingReqs").asLong());
                row.put("holderPingReqsByRelay", pb.get("holderPingReqsByRelay"));
                if (legacyHolds) {
                    // EL-SP-30b: the holder is the released member; every holder round goes to it
                    pb.get("holderPingReqsByRelay").fieldNames().forEachRemaining(relay -> {
                        if (!relay.equals(legacy.peer)) {
                            faults.add(c.name + " asked " + relay + ", not the 0.6.0 holder, as a holder");
                        }
                    });
                    if (holderRounds == 0) {
                        faults.add(c.name + " never probed the TypeScript peer through the 0.6.0 holder");
                    }
                }
            }
        }
        if (Boolean.parseBoolean(System.getProperty("mixed.sigstop", "true"))) {
            // Silent members in the mixed group (the plan's wire check 4, the phase-2 gate): SIGSTOP a new member,
            // then the 0.6.0 member. The released member judges at its 30 s TTL, a new member at its computed TTL;
            // a stopped 0.6.0 member is probed directly by every new member once a quiet cycle, so it goes within one
            // cycle plus the PING and indirect timeouts (4 s). Neither comes back while stopped: only a newly issued
            // ad re-admits (PN:6734-6736). 1 s covers the poll.
            Map<String, Object> silent = new LinkedHashMap<>();
            IdleCostIT.Child stoppedNew = fresh.get(3);
            List<IdleCostIT.Child> watchNew = new ArrayList<>(List.of(legacy, fresh.get(0), fresh.get(1), fresh.get(2)));
            Map<String, Long> ttlOf = new LinkedHashMap<>();
            for (IdleCostIT.Child w : watchNew) {
                ttlOf.put(w.name, w == legacy ? 30_000L : w.stats().get("membership").get("memberTtlMillis").asLong());
            }
            Map<String, Long> newGone = stopAndTime(stoppedNew, watchNew, 120_000);
            long newReturns = returnsWhileStopped(stoppedNew, watchNew, Duration.ofSeconds(20));
            silent.put("newMemberStopped", stoppedNew.name);
            silent.put("newMemberGoneMillis", newGone);
            silent.put("newMemberTtlMillis", ttlOf);
            silent.put("newMemberReturns", newReturns);
            for (IdleCostIT.Child w : watchNew) {
                Long at = newGone.get(w.name);
                long bound = ttlOf.get(w.name) + 5_000;
                if (at == null || at > bound) {
                    faults.add(w.name + " held the stopped new member " + stoppedNew.name + " for "
                            + (at == null ? "over 120,000" : at) + " ms (bound " + bound + " ms)");
                }
            }
            if (newReturns > 0) {
                faults.add("the stopped new member came back " + newReturns + " time(s) while stopped");
            }
            stoppedNew.process.destroyForcibly();
            children.remove(stoppedNew);

            List<IdleCostIT.Child> watchLegacy = new ArrayList<>(List.of(fresh.get(0), fresh.get(1), fresh.get(2)));
            Map<String, Long> cycleOf = new LinkedHashMap<>();
            for (IdleCostIT.Child w : watchLegacy) {
                cycleOf.put(w.name, w.stats().get("membership").get("quietProbeCycleMillis").asLong());
            }
            Map<String, Long> legacyGone = stopAndTime(legacy, watchLegacy, 120_000);
            long legacyReturns = returnsWhileStopped(legacy, watchLegacy, Duration.ofSeconds(20));
            silent.put("legacyGoneMillis", legacyGone);
            silent.put("quietProbeCycleMillis", cycleOf);
            silent.put("legacyReturns", legacyReturns);
            for (IdleCostIT.Child w : watchLegacy) {
                Long at = legacyGone.get(w.name);
                long bound = cycleOf.get(w.name) + 5_000;
                if (at == null || at > bound) {
                    faults.add(w.name + " held the stopped 0.6.0 member for " + (at == null ? "over 120,000" : at)
                            + " ms (bound: its quiet cycle plus 5 s, " + bound + " ms)");
                }
            }
            if (legacyReturns > 0) {
                faults.add("the stopped 0.6.0 member came back " + legacyReturns + " time(s) while stopped");
            }
            legacy.process.destroyForcibly();
            children.remove(legacy);
            report.put("silentMembers", silent);
        }
        report.put("scenario", legacyHolds ? "EL-SP-30b" : "EL-SP-30");
        report.put("legacyThinDepartures", legacyThinDepartures);
        report.put("members", rows);
        Files.writeString(out.resolve(legacyHolds ? "el-sp-30b.json" : "el-sp-30.json"),
                JSON.writerWithDefaultPrettyPrinter().writeValueAsString(report));
        System.out.println("MIXED-TTL " + JSON.writeValueAsString(report));
        Measure.record("core.thin-peer.departures", Unit.COUNT, Direction.LOWER,
                faults.stream().filter(f -> f.contains("TypeScript")).count(),
                Rec.with().params(Params.of("members", 6, "harness", "tcp", "ts", 1, "mixed",
                                legacyHolds ? "0.6.0-holder" : "0.6.0"))
                        .kind(Kind.COUNT));
        assertThat(faults).as((legacyHolds ? "EL-SP-30b" : "EL-SP-30") + ": PREVENTED").isEmpty();
    }
}
