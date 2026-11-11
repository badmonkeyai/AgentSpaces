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
package ai.badmonkey.agentspaces.examples.pdt;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The acceptance test of example 33 (srad/agentspaces-pdt.md §7.3 step 6): the
 * composition over loopback tcp at the quick sizes, asserting PD-Q-PRF-01 to 06
 * at their as-built bounds plus an allowance for lag and retries, the equal
 * roots of PD-Q-REL-02 across the Java readers, and PD-Q-INT-01 against the
 * Python reader when it is present.
 */
class PersistentValuesTest {

    private static final long KIB = 1024;
    private static final long MIB = 1024 * 1024;
    private static final Pattern CHUNK = Pattern.compile("edited chunk (\\d+) bytes");

    /*
     * Real paths lag, spike, and drop. A request is asked again after a quiet source, an endgame
     * batch goes to a second holder, a delta that arrives before its base is fetched rather than
     * applied inline. The rows' bounds are the clean-path cost; these allowances sit on top so a
     * spike of lag on a busy machine passes, while work far past the clean path still fails.
     * Correctness (equal roots, Java and Python agreeing, fetches completing) gets no allowance.
     */
    /** Sequential rounds a retried or re-routed request may add to a fetch (PD-Q-PRF-06). */
    private static final int RETRY_ROUNDS = 2;
    /** Bytes received may reach this multiple of the clean-path bytes: retries and endgame duplicates. */
    private static final long RESEND_FACTOR = 2;

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        }
    }

    @Test
    @Timeout(600)
    void theCompositionRunsOverTcpWithinTheRowsBounds(@TempDir Path dir) throws Exception {
        int curatorPort = freePort();
        String seed = "127.0.0.1:" + curatorPort;
        try (PersistentValuesFleet.Peer curator = PersistentValuesFleet.start("curator", curatorPort, List.of(), dir.resolve("curator"));
             PersistentValuesFleet.Peer a1 = PersistentValuesFleet.start("analyst-1", freePort(), List.of(seed), dir.resolve("a1"));
             PersistentValuesFleet.Peer a2 = PersistentValuesFleet.start("analyst-2", freePort(), List.of(seed), dir.resolve("a2"));
             PersistentValuesFleet.Peer a3 = PersistentValuesFleet.start("analyst-3", freePort(), List.of(seed), dir.resolve("a3"));
             PersistentValuesFleet.Peer meter = PersistentValuesFleet.start("meter", Map.of("tcp", freePort()), Set.of(),
                     List.of(seed), dir.resolve("meter"), false)) {
            Composition.Fleet fleet = new Composition.Fleet(curator, List.of(a1, a2, a3), meter,
                    () -> PersistentValuesFleet.start("auditor", Map.of("tcp", freePort()), Set.of(), List.of(seed),
                            dir.resolve("auditor"), false));
            // PD-Q-INT-01: the cross-language root check is part of the row, so a missing interpreter
            // skips the test through an assumption (visible in the surefire count) rather than
            // degrading it to a Java-only run
            org.junit.jupiter.api.Assumptions.assumeTrue(PythonReader.available(),
                    "python reader: skipped (" + PythonReader.whyUnavailable() + ")");
            PythonReader python = new PythonReader("127.0.0.1", curatorPort);
            Composition.Report report = Composition.run(fleet, Composition.Sizes.quick(), dir.resolve("work"), python, System.out);
            System.out.print(report.summary());

            // PD-Q-PRF-01 and PD-Q-PRF-05: one key in the 10,000-entry map moves its delta, inline, over no pipe
            Composition.Update oneKey = report.update("1 facts one key");
            assertThat(report.factsBytes()).as("the facts map is about 2 MiB").isGreaterThan(MIB);
            assertThat(oneKey.deltaBytes()).as("PD-Q-PRF-01: " + oneKey).isLessThanOrEqualTo(8 * KIB);
            assertThat(oneKey.maxReaderBytes()).as("PD-Q-PRF-01 per reader: " + oneKey)
                    .isBetween(oneKey.deltaBytes(), RESEND_FACTOR * oneKey.deltaBytes());
            boolean allInline = true;
            for (Composition.ReaderCost cost : oneKey.readers()) {
                // inline is the clean path (PD-X-07); a reader that saw the delta late may fetch it instead
                assertThat(cost.inline() + cost.fetched()).as(cost.reader() + " took the delta: " + oneKey).isGreaterThanOrEqualTo(1);
                allInline &= cost.fetched() == 0;
            }
            if (allInline) {
                assertThat(oneKey.pipeNodes()).as("PD-Q-PRF-05: tree nodes over the pipes during an inline delta").isZero();
            }
            assertThat(oneKey.meter().complete()).as("the meter fetched the delta").isTrue();
            assertThat(oneKey.meter().bytes()).as("the meter received about the delta alone")
                    .isBetween(oneKey.deltaBytes(), RESEND_FACTOR * oneKey.deltaBytes());
            assertThat(oneKey.meter().roundTrips()).as("PD-Q-PRF-06 with the base held").isLessThanOrEqualTo(2 + RETRY_ROUNDS);

            // PD-Q-PRF-06 without a base: the whole map in depth plus one round trips on the clean path
            Composition.Update v0 = report.update("1 facts v0");
            assertThat(v0.meter().complete()).isTrue();
            assertThat(v0.meter().roundTrips()).as("PD-Q-PRF-06 without the base, depth " + report.factsDepth() + ": " + v0)
                    .isLessThanOrEqualTo(report.factsDepth() + 1 + RETRY_ROUNDS);
            for (Composition.ReaderCost cost : v0.readers()) {
                assertThat(cost.fetched()).as(cost.reader() + " fetched v0").isGreaterThanOrEqualTo(1);
                assertThat(cost.bytes()).as(cost.reader() + " received about the map once").isBetween(report.factsBytes(),
                        RESEND_FACTOR * report.factsBytes());
            }

            // PD-Q-REL-02 and PD-Q-INT-01: equal roots after every round and after the fold
            assertThat(new HashSet<>(report.factsRoots().values())).as("PD-Q-REL-02: " + report.factsRoots()).hasSize(1);
            for (Composition.RootCheck check : report.rootChecks()) {
                assertThat(check.pythonRoot()).as("the Python reader answered " + check.when() + ": " + report.pythonNote()).isNotNull();
                assertThat(check.equal()).as("PD-Q-INT-01 " + check).isTrue();
            }
            for (Composition.Update round : report.updates("2 round")) {
                assertThat(round.meter().complete()).as(round.step()).isTrue();
                // inline is the clean path; a revision that lands before its base is fetched instead
                long inline = 0;
                long fetched = 0;
                for (Composition.ReaderCost cost : round.readers()) {
                    assertThat(cost.inline() + cost.fetched()).as(round.step() + ": " + cost.reader() + " took revisions").isPositive();
                    inline += cost.inline();
                    fetched += cost.fetched();
                }
                assertThat(inline).as(round.step() + ": most revisions went inline: " + round).isGreaterThanOrEqualTo(fetched);
            }

            // PD-Q-PRF-02: one byte in place moves the edited chunk and its path
            Composition.Update edit = report.update("3 edit one byte");
            Matcher chunk = CHUNK.matcher(edit.note());
            assertThat(chunk.find()).as(edit.note()).isTrue();
            long chunkBytes = Long.parseLong(chunk.group(1));
            assertThat(edit.deltaBytes()).as("PD-Q-PRF-02 delta: " + edit).isLessThanOrEqualTo(chunkBytes + 16 * KIB);
            assertThat(edit.maxReaderBytes()).as("PD-Q-PRF-02 worst case: " + edit)
                    .isLessThanOrEqualTo(RESEND_FACTOR * (2 * 256 * KIB + 16 * KIB));
            assertThat(edit.meter().bytes()).as("the meter received about the delta alone: " + edit)
                    .isBetween(edit.deltaBytes(), RESEND_FACTOR * edit.deltaBytes());
            assertThat(edit.meter().roundTrips()).as("PD-Q-PRF-06 with the base held").isLessThanOrEqualTo(2 + RETRY_ROUNDS);

            // PD-Q-PRF-04: 1 MiB appended moves at most 1 MiB plus the old last chunk plus the path
            List<Composition.Update> appends = report.updates("3 append");
            assertThat(appends).isNotEmpty();
            for (Composition.Update append : appends) {
                assertThat(append.maxReaderBytes()).as("PD-Q-PRF-04: " + append)
                        .isLessThanOrEqualTo(RESEND_FACTOR * (MIB + 256 * KIB + 16 * KIB));
                assertThat(append.meter().roundTrips()).as("PD-Q-PRF-06 with the base held").isLessThanOrEqualTo(2 + RETRY_ROUNDS);
            }

            // PD-Q-PRF-03: 100 bytes inserted move at most 256 KiB at the median
            List<Composition.Update> inserts = report.updates("3 insert");
            assertThat(inserts).isNotEmpty();
            List<Long> insertBytes = new ArrayList<>();
            for (Composition.Update insert : inserts) {
                insertBytes.add(insert.maxReaderBytes());
                assertThat(insert.meter().roundTrips()).as("PD-Q-PRF-06 with the base held").isLessThanOrEqualTo(2 + RETRY_ROUNDS);
            }
            insertBytes.sort(null);
            long median = insertBytes.get(insertBytes.size() / 2);
            System.out.printf("PD-Q-PRF-03 over %d inserts: median %d bytes, max %d bytes%n", inserts.size(), median,
                    insertBytes.get(insertBytes.size() - 1));
            assertThat(median).as("PD-Q-PRF-03 median of " + insertBytes).isLessThanOrEqualTo(256 * KIB);

            // every fetched delta of the table reached every reader (once on the clean path, again after a lost request)
            for (Composition.Update u : report.updates("3 ")) {
                for (Composition.ReaderCost cost : u.readers()) {
                    assertThat(cost.fetched()).as(u.step() + ": " + cost.reader()).isGreaterThanOrEqualTo(1);
                }
            }

            // PD-Q-REL-04: the auditor's cold fetch completes after an analyst dies
            Composition.ColdFetch cold = report.cold();
            assertThat(cold).isNotNull();
            assertThat(cold.complete()).as("the cold fetch completed: " + cold).isTrue();
            assertThat(cold.roundTrips()).as("PD-Q-PRF-06 without a base, depth " + cold.depth() + ": " + cold)
                    .isLessThanOrEqualTo(cold.depth() + 1 + RETRY_ROUNDS);
            assertThat(cold.bytesReceived()).as("no more than " + RESEND_FACTOR + "x the blob was shipped: " + cold)
                    .isLessThanOrEqualTo(RESEND_FACTOR * cold.blobBytes() + MIB);
            assertThat(cold.bytesBySource()).as("the survivors served the rest").hasSizeGreaterThanOrEqualTo(2);

            // PD-E-08: the accumulator grows by delta
            assertThat(report.plansUnable()).as("every member runs the tree exchange (PD-E-06)").isEmpty();
            assertThat(report.plans()).hasSize(Composition.Sizes.quick().planSteps());
            Composition.PlanStep last = report.plans().get(report.plans().size() - 1);
            assertThat(last.payloadBytes()).as("the accumulator grew past the threshold many times over").isGreaterThan(8 * PersistentValuesFleet.ESCALATE_ABOVE);
            for (Composition.PlanStep step : report.plans()) {
                if (step.step() >= 3) {
                    assertThat(step.wrote()).as("step " + step.step() + " escalated").isGreaterThan(0);
                    for (Map.Entry<String, Long> received : step.received().entrySet()) {
                        assertThat(received.getValue()).as("PD-E-08 step " + step.step() + " " + received.getKey() + ": " + step)
                                .isLessThanOrEqualTo(RESEND_FACTOR * step.wrote());
                    }
                }
            }
        }
    }
}
