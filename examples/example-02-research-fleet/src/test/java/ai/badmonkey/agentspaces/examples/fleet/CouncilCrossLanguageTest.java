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
package ai.badmonkey.agentspaces.examples.fleet;

import ai.badmonkey.agentspaces.examples.fleet.CouncilFleet.Council;
import ai.badmonkey.agentspaces.examples.fleet.CouncilFleet.Outcome;
import ai.badmonkey.agentspaces.examples.fleet.ResearchFleet.Finding;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The cross-language proof (L3L4-COVERAGE.md §6.6, phase I): the Java
 * council coordinator with a Python worker, a TypeScript worker, and a Ruby
 * worker in their own processes. Each worker takes tasks through its decorated agent, casts
 * the council ballot, contributes to the push-sum epoch, answers the semantic
 * query, and serves the remote action its card declares. Once over plain TCP,
 * and once over the {@code tls} binding with attestation required (SPEC §5.6):
 * every worker presents its channel certificate, the coordinator attests it,
 * and each worker's CHANNEL_HELLO lets the coordinator send it bare frames.
 *
 * <p>Needs a Python with the {@code cryptography} package ({@code PYTHON}, or
 * {@code python3}), a Node with {@code agentspaces-typescript} built, and a Ruby
 * 3.3 or newer ({@code RUBY}, or Homebrew's, or {@code ruby}). Without
 * them the test is skipped, unless {@code -Dcouncil.required=true} makes the
 * missing prerequisite a failure (the gate in {@code verify-all.sh} does).
 */
class CouncilCrossLanguageTest {

    private static final Path ROOT = Path.of("").toAbsolutePath().getParent().getParent().getParent();

    /** Research tasks per act: ten for each of the three workers. */
    private static final int TASKS = 30;

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        }
    }

    private static void require(boolean condition, String why) {
        if (Boolean.getBoolean("council.required")) {
            assertThat(condition).as(why).isTrue();
        }
        Assumptions.assumeTrue(condition, why);
    }

    private static boolean runs(List<String> command, Path cwd) {
        try {
            Process probe = new ProcessBuilder(command).directory(cwd.toFile())
                    .redirectErrorStream(true).start();
            return probe.waitFor(20, TimeUnit.SECONDS) && probe.exitValue() == 0;
        } catch (IOException | InterruptedException e) {
            return false;
        }
    }

    @Test
    @Timeout(240)
    void pythonTypeScriptAndRubyWorkersSitOnTheJavaCouncil() throws Exception {
        council(false);
    }

    @Test
    @Timeout(240)
    void theWorkersSitOnTheCouncilOverTlsWithAttestationRequired() throws Exception {
        council(true);
    }

    /** The council act with the three workers, over tcp or over tls with attestation required. */
    private void council(boolean tls) throws Exception {
        String python = System.getenv().getOrDefault("PYTHON", "python3");
        Path pythonDir = ROOT.resolve("agentspaces-python");
        Path nodeDir = ROOT.resolve("agentspaces-typescript");
        Path councilJs = nodeDir.resolve("build/demo/council.js");
        require(Files.exists(pythonDir.resolve("demo_council_worker.py")), "python client at " + pythonDir);
        require(runs(List.of(python, "-c", "import cryptography, agentspaces"), pythonDir),
                python + " with the cryptography package");
        require(Files.exists(councilJs), "typescript client built (npm test) at " + nodeDir);
        require(runs(List.of("node", "--version"), nodeDir), "node on the PATH");
        Path rubyDir = ROOT.resolve("agentspaces-rb/examples/ruby-fleet");
        String ruby = System.getenv().getOrDefault("RUBY",
                Files.exists(Path.of("/opt/homebrew/opt/ruby/bin/ruby")) ? "/opt/homebrew/opt/ruby/bin/ruby" : "ruby");
        require(Files.exists(rubyDir.resolve("council_worker.rb")), "ruby client at " + rubyDir);
        require(runs(List.of(ruby, "-e", "exit(RUBY_VERSION >= '3.3' ? 0 : 1)"), rubyDir), ruby + " 3.3 or newer");

        int port = freePort();
        // A "tls://" seed makes each worker dial TLS 1.3 and present its channel certificate.
        String seed = tls ? "tls://127.0.0.1" : "127.0.0.1";
        // Each worker's output goes to target/council-logs/<variant>-<language>.log, where a failure
        // can be read afterwards (inherited output is lost inside the surefire fork).
        Path logs = Path.of("target", "council-logs");
        Files.createDirectories(logs);
        String variant = tls ? "tls" : "tcp";
        try (Council council = CouncilFleet.start(port, tls)) {
            Process py = new ProcessBuilder(python, "demo_council_worker.py", seed, String.valueOf(port),
                    "10", "150").directory(pythonDir.toFile()).redirectErrorStream(true)
                    .redirectOutput(logs.resolve(variant + "-python.log").toFile()).start();
            Process ts = new ProcessBuilder("node", councilJs.toString(), seed, String.valueOf(port),
                    "30", "150").directory(nodeDir.toFile()).redirectErrorStream(true)
                    .redirectOutput(logs.resolve(variant + "-typescript.log").toFile()).start();
            Process rb = new ProcessBuilder(ruby, "council_worker.rb", seed, String.valueOf(port),
                    "40", "150").directory(rubyDir.toFile()).redirectErrorStream(true)
                    .redirectOutput(logs.resolve(variant + "-ruby.log").toFile()).start();
            try {
                // Ten tasks per worker: the workers race for them, and the queue must outlast the
                // slowest worker's first take. The TypeScript take ends its settle wait at the next
                // fold while the Python and Ruby takes wait the full 600 ms, so over tls, where
                // frames arrive bare and fast, two tasks per worker left the Ruby worker without
                // one in most runs.
                Outcome outcome = CouncilFleet.act(council, 3, TASKS, 20.0, Duration.ofSeconds(90));

                Set<String> workers = outcome.findings().stream().map(Finding::worker).collect(Collectors.toSet());
                assertThat(outcome.findings()).as("every task found").hasSizeGreaterThanOrEqualTo(TASKS);
                assertThat(workers).as("every worker took tasks").contains("py-worker", "ts-worker", "rb-worker");

                assertThat(outcome.decision()).as("the council decided").isPresent();
                assertThat(outcome.decision().get().winner()).isEqualTo("yes");
                assertThat(outcome.decision().get().tally()).containsEntry("yes", 4);

                assertThat(outcome.estimate()).as("the epoch settled").isPresent();
                assertThat(outcome.estimate().getAsDouble()).as("mean of 20, 10, 30, and 40").isCloseTo(25.0,
                        org.assertj.core.data.Offset.offset(1.0));

                Set<String> cards = outcome.cards().stream().map(c -> c.agent().localName()).collect(Collectors.toSet());
                assertThat(cards).as("the semantic query ranked every worker's card")
                        .contains("py-worker", "ts-worker", "rb-worker");

                assertThat(outcome.remote()).as("a worker served the remote action").isPresent();
                assertThat(outcome.remote().get().topic()).isEqualTo("council-remote");
                assertThat(outcome.remote().get().worker()).isIn("py-worker", "ts-worker", "rb-worker");

                if (tls) {
                    // Attestation was required, so every frame above came from a worker the handshake
                    // attested; and each worker announced CHANNEL_HELLO, so the coordinator answered bare.
                    assertThat(council.node().bareFramesSent())
                            .as("the coordinator sent bare frames to the attested workers that announced")
                            .isPositive();
                }
            } finally {
                for (Process worker : List.of(py, ts, rb)) {
                    worker.destroy();
                }
                for (Process worker : List.of(py, ts, rb)) {
                    worker.waitFor(10, TimeUnit.SECONDS);
                    worker.destroyForcibly();
                }
            }
        }
    }
}
