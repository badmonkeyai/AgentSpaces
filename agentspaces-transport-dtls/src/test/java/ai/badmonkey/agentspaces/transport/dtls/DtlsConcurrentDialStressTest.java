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

package ai.badmonkey.agentspaces.transport.dtls;

import ai.badmonkey.agentspaces.identity.PeerIdentity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link DtlsRelayTest#oneListenerServesThirtyTwoConcurrentAssociations} round
 * after round, with busy threads competing for the CPU. It found the macOS
 * port collision behind that test's one-in-a-few-hundred failure: a wildcard
 * dial socket handed its own listener's loopback port handshakes with the
 * listener's socket and times out (see {@link BcSupport#dialSocket}). A round
 * that fails or stalls prints both ends' counters; a stall past six seconds
 * also writes a thread dump to {@code target/}.
 *
 * <p>Opt-in: {@code -Ddtls.stress=true}; {@code -Ddtls.stress.rounds=N}
 * (default 200, 1,500 for a confident pass); {@code -Ddtls.stress.load=N} busy
 * threads (default one per processor, 0 for none). For example:
 * {@code mvn -pl agentspaces-transport-dtls test -Dtest=DtlsConcurrentDialStressTest -Ddtls.stress=true}.
 */
@EnabledIfSystemProperty(named = "dtls.stress", matches = "true")
class DtlsConcurrentDialStressTest {

    private static final int DIALS = 32;
    private static final Duration STALL = Duration.ofSeconds(6);

    @Test
    void thirtyTwoConcurrentDialsSucceedRoundAfterRound() throws Exception {
        int rounds = Integer.getInteger("dtls.stress.rounds", 200);
        int load = Integer.getInteger("dtls.stress.load", Runtime.getRuntime().availableProcessors());
        List<Thread> busy = new ArrayList<>();
        for (int i = 0; i < load; i++) {
            busy.add(Thread.ofPlatform().daemon().name("dtls-stress-load-" + i).start(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    // spin
                }
            }));
        }
        List<String> failedRounds = new ArrayList<>();
        try {
            for (int round = 0; round < rounds; round++) {
                String failure = round(round);
                if (failure != null) {
                    failedRounds.add(failure);
                }
            }
        } finally {
            busy.forEach(Thread::interrupt);
        }
        System.out.println("dtls stress: " + rounds + " rounds of " + DIALS + " dials under " + load
                + " busy threads, " + failedRounds.size() + " failed");
        assertThat(failedRounds).isEmpty();
    }

    /** One listener, one dialer, 32 concurrent dials; returns a description of the failure, or null. */
    private String round(int round) throws Exception {
        DtlsTransport listener = new DtlsTransport(PeerIdentity.generate())
                .options(DtlsOptions.defaults().withHelloRatePerSource(1_000));
        DtlsTransport dialer = new DtlsTransport(PeerIdentity.generate());
        try (listener; dialer) {
            DtlsTransport.Bound bound = listener.listenBound("127.0.0.1:0", c -> { });
            List<String> failures = Collections.synchronizedList(new ArrayList<>());
            List<Thread> threads = new ArrayList<>();
            long start = System.nanoTime();
            for (int i = 0; i < DIALS; i++) {
                int n = i;
                threads.add(Thread.ofVirtual().start(() -> {
                    long began = System.nanoTime();
                    try {
                        dialer.dial("127.0.0.1:" + bound.port()).send(("from-" + n).getBytes());
                    } catch (Exception e) {
                        failures.add("dial " + n + " after " + (System.nanoTime() - began) / 1_000_000 + " ms: "
                                + e.getMessage());
                    }
                }));
            }
            String dump = null;
            for (Thread thread : threads) {
                while (!thread.join(Duration.ofMillis(200))) {
                    if (dump == null && System.nanoTime() - start > STALL.toNanos()) {
                        dump = threadDump(round);
                    }
                }
            }
            long millis = (System.nanoTime() - start) / 1_000_000;
            if (failures.isEmpty() && dump == null) {
                return null;
            }
            String report = "round " + round + " took " + millis + " ms, port " + bound.port() + ", " + failures
                    + (dump == null ? "" : ", thread dump " + dump)
                    + "\n  listener " + counters(listener) + ", halfOpen=" + bound.listener().halfOpen()
                    + "\n  dialer   " + counters(dialer);
            System.out.println(report);
            return failures.isEmpty() ? null : report; // a stall that recovered is reported, not failed
        }
    }

    private static String counters(DtlsTransport transport) {
        DtlsCounters c = transport.counters();
        return c + " datagramsDropped=" + c.datagramsDropped() + " unknownConnectionIds=" + c.unknownConnectionIds();
    }

    private static String threadDump(int round) {
        try {
            Path file = Path.of("target", "dtls-stress-dump-" + round + ".txt").toAbsolutePath();
            Files.createDirectories(file.getParent());
            new ProcessBuilder("jcmd", Long.toString(ProcessHandle.current().pid()),
                    "Thread.dump_to_file", "-overwrite", file.toString())
                    .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD)
                    .start().waitFor();
            return file.toString();
        } catch (Exception e) {
            return "unavailable (" + e.getMessage() + ")";
        }
    }
}
