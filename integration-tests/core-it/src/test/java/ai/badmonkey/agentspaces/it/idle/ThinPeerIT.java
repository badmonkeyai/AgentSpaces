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

import ai.badmonkey.agentspaces.measure.Direction;
import ai.badmonkey.agentspaces.measure.Kind;
import ai.badmonkey.agentspaces.measure.Measure;
import ai.badmonkey.agentspaces.measure.Params;
import ai.badmonkey.agentspaces.measure.Rec;
import ai.badmonkey.agentspaces.measure.Unit;
import ai.badmonkey.agentspaces.measure.Wait;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * EL-SP-31 over TCP (srad/agentspaces-event-loop.md §3.12, EL-P-13 to EL-P-15, EL-E-05): a quiet group of Java members,
 * each its own JVM ({@link IdleCostNode}, the starter's defaults, 4 spaces, coalescing 20 ms), and one real idle
 * TypeScript peer that dials m0 and names it in {@code relay.via}; a 2-minute settle, then 30 minutes idle. PREVENTED:
 * no Java view ever loses the TypeScript peer, and every non-seed member's probes of it go as holder PING_REQs to m0.
 *
 * <ul>
 *   <li>{@code thin.members} (10): members, the TypeScript one included (ten is the run of record; twelve and sixteen
 *       are covered on virtual time by {@code TargetedRelaySpoilerTest}, owner rule 2026-11-22);</li>
 *   <li>{@code thin.ts}: the {@code agentspaces-typescript} directory (its {@code build/src/peer.js});</li>
 *   <li>{@code thin.settleSeconds} (120) and {@code thin.idleSeconds} (1800).</li>
 * </ul>
 *
 * <p>Runs only with {@code -Dthin.peer=true}. One run (owner rules 2026-11-11 and 2026-11-22).
 */
@EnabledIfSystemProperty(named = "thin.peer", matches = "true")
class ThinPeerIT {

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

    @Test
    void aDialOnlyTypeScriptPeerStaysInEveryViewOfAQuietGroup() throws Exception {
        int members = Integer.getInteger("thin.members", 10);
        String ts = System.getProperty("thin.ts");
        assertThat(ts).as("-Dthin.ts=<agentspaces-typescript dir>").isNotBlank();
        long settle = Long.getLong("thin.settleSeconds", 120);
        long idle = Long.getLong("thin.idleSeconds", 1800);
        int spaces = 4;
        long coalesce = 20;
        int javaMembers = members - 1;
        Params shape = Params.of("members", members, "harness", "tcp", "ts", 1);
        String founding = "thin-peer-" + ProcessHandle.current().pid() + "-" + System.currentTimeMillis();
        Path dir = Files.createTempDirectory("thin-peer");
        Path out = Path.of("target", "thin-peer");
        Files.createDirectories(out);
        System.out.printf("ThinPeerIT: %d Java members and one TypeScript peer, settle %d s, window %d s%n",
                javaMembers, settle, idle);

        int seedPort = IdleCostIT.freePort();
        for (int i = 0; i < javaMembers; i++) {
            children.add(IdleCostIT.startJava("m" + i, i == 0 ? seedPort : IdleCostIT.freePort(),
                    i == 0 ? 0 : seedPort, dir, founding, spaces, coalesce, true));
            Thread.sleep(i == 0 ? 2000 : 300);
        }
        IdleCostIT.Child thin = IdleCostIT.startTypeScript(ts, founding, seedPort);
        children.add(thin);
        List<IdleCostIT.Child> java = children.stream().filter(c -> c.java).toList();
        Measure.await("core.harness.wait", Duration.ofSeconds(180), () -> IdleCostIT.converged(java, members - 1),
                Wait.with().poll(Duration.ofMillis(500)).params(Params.of("what", "converge", "members", members))
                        .diagnostics(() -> IdleCostIT.views(java)));
        Thread.sleep(Duration.ofSeconds(settle)); // the settle: the design of the experiment
        Map<String, JsonNode> before = IdleCostIT.snapshot(java);
        Thread.sleep(Duration.ofSeconds(idle)); // the 30-minute window
        Map<String, JsonNode> after = IdleCostIT.snapshot(java);

        String seed = java.get(0).peer;
        long departures = 0;
        int absentAtEnd = 0;
        List<Map<String, Object>> perMember = new ArrayList<>();
        double[] ttl = new double[java.size()];
        List<Double> reqsPerMinute = new ArrayList<>();
        List<String> faults = new ArrayList<>();
        for (int i = 0; i < java.size(); i++) {
            IdleCostIT.Child c = java.get(i);
            JsonNode a = before.get(c.name);
            JsonNode b = after.get(c.name);
            double minutes = (b.get("monoNanos").asLong() - a.get("monoNanos").asLong()) / 60e9;
            JsonNode gone = b.get("departures").get(thin.peer);
            long left = gone == null ? 0 : gone.asLong();
            departures += left;
            boolean present = IdleCostIT.contains(b.get("view"), thin.peer);
            absentAtEnd += present ? 0 : 1;
            JsonNode pa = a.get("membership");
            JsonNode pb = b.get("membership");
            ttl[i] = pb.get("memberTtlMillis").asDouble();
            long holderRounds = pb.get("holderRounds").asLong() - pa.get("holderRounds").asLong();
            long randomReqs = pb.get("randomPingReqs").asLong() - pa.get("randomPingReqs").asLong();
            long escalations = pb.get("escalations").asLong() - pa.get("escalations").asLong();
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("member", c.name);
            m.put("thinPeerDepartures", left);
            m.put("thinPeerInViewAtEnd", present);
            m.put("memberTtlMillis", pb.get("memberTtlMillis").asLong());
            m.put("quietProbeCycleMillis", pb.get("quietProbeCycleMillis").asLong());
            m.put("holderRounds", holderRounds);
            m.put("randomPingReqs", randomReqs);
            m.put("escalations", escalations);
            m.put("holderPingReqsByRelay", pb.get("holderPingReqsByRelay"));
            m.put("keepaliveFramesPerMin", IdleCostIT.round(IdleCostIT.delta(a, b, "keepalive", "refreshFrames")
                    / minutes));
            perMember.add(m);
            if (i > 0) {
                JsonNode byRelay = pb.get("holderPingReqsByRelay");
                long toSeed = byRelay.has(seed) ? byRelay.get(seed).asLong() : 0;
                long toSeedBefore = pa.get("holderPingReqsByRelay").has(seed)
                        ? pa.get("holderPingReqsByRelay").get(seed).asLong() : 0;
                reqsPerMinute.add((toSeed - toSeedBefore) / minutes);
                byRelay.fieldNames().forEachRemaining(relay -> {
                    if (!relay.equals(seed)) {
                        faults.add(c.name + " asked " + relay + ", not the seed, as a holder");
                    }
                });
                if (holderRounds == 0) {
                    faults.add(c.name + " never probed the TypeScript peer through its holder");
                }
            }
        }
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("shape", shape.asMap());
        report.put("thinPeer", thin.peer);
        report.put("seed", seed);
        report.put("departures", departures);
        report.put("javaViewsWithoutItAtEnd", absentAtEnd);
        report.put("members", perMember);
        Files.writeString(out.resolve("members-" + members + ".json"),
                JSON.writerWithDefaultPrettyPrinter().writeValueAsString(report));
        System.out.println("THIN-PEER " + JSON.writeValueAsString(report));
        Measure.record("core.thin-peer.departures", Unit.COUNT, Direction.LOWER, departures,
                Rec.with().params(shape).kind(Kind.COUNT));
        Measure.samples("core.membership.member-ttl", Unit.MS, Direction.LOWER, ttl,
                Rec.with().params(shape).kind(Kind.PERF));
        Measure.samples("core.thin-peer.holder-pingreqs", Unit.COUNT, Direction.LOWER,
                reqsPerMinute.stream().mapToDouble(Double::doubleValue).toArray(),
                Rec.with().params(shape).kind(Kind.COUNT));
        assertThat(departures).as("PREVENTED: the TypeScript peer never left a Java view in the window").isZero();
        assertThat(absentAtEnd).as("every Java view holds it at the end").isZero();
        assertThat(faults).as("the non-seed members probed it through the seed alone").isEmpty();
    }
}
