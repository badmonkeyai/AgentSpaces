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
 * council coordinator with a Python worker and a TypeScript worker in their
 * own processes. Each worker takes tasks through its decorated agent, casts
 * the council ballot, contributes to the push-sum epoch, answers the semantic
 * query, and serves the remote action its card declares.
 *
 * <p>Needs a Python with the {@code cryptography} package ({@code PYTHON}, or
 * {@code python3}) and a Node with {@code agentspaces-typescript} built. Without
 * them the test is skipped, unless {@code -Dcouncil.required=true} makes the
 * missing prerequisite a failure (the gate in {@code verify-all.sh} does).
 */
class CouncilCrossLanguageTest {

    private static final Path ROOT = Path.of("").toAbsolutePath().getParent().getParent().getParent();

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
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
    void pythonAndTypeScriptWorkersSitOnTheJavaCouncil() throws Exception {
        String python = System.getenv().getOrDefault("PYTHON", "python3");
        Path pythonDir = ROOT.resolve("agentspaces-python");
        Path nodeDir = ROOT.resolve("agentspaces-typescript");
        Path councilJs = nodeDir.resolve("build/demo/council.js");
        require(Files.exists(pythonDir.resolve("demo_council_worker.py")), "python client at " + pythonDir);
        require(runs(List.of(python, "-c", "import cryptography, agentspaces"), pythonDir),
                python + " with the cryptography package");
        require(Files.exists(councilJs), "typescript client built (npm test) at " + nodeDir);
        require(runs(List.of("node", "--version"), nodeDir), "node on the PATH");

        int port = freePort();
        try (Council council = CouncilFleet.start(port)) {
            Process py = new ProcessBuilder(python, "demo_council_worker.py", "127.0.0.1", String.valueOf(port),
                    "10", "150").directory(pythonDir.toFile()).inheritIO().start();
            Process ts = new ProcessBuilder("node", councilJs.toString(), "127.0.0.1", String.valueOf(port),
                    "30", "150").directory(nodeDir.toFile()).inheritIO().start();
            try {
                Outcome outcome = CouncilFleet.act(council, 2, 20.0, Duration.ofSeconds(90));

                Set<String> workers = outcome.findings().stream().map(Finding::worker).collect(Collectors.toSet());
                assertThat(outcome.findings()).as("every task found").hasSizeGreaterThanOrEqualTo(4);
                assertThat(workers).as("both workers took tasks").contains("py-worker", "ts-worker");

                assertThat(outcome.decision()).as("the council decided").isPresent();
                assertThat(outcome.decision().get().winner()).isEqualTo("yes");
                assertThat(outcome.decision().get().tally()).containsEntry("yes", 3);

                assertThat(outcome.estimate()).as("the epoch settled").isPresent();
                assertThat(outcome.estimate().getAsDouble()).as("mean of 20, 10, and 30").isCloseTo(20.0,
                        org.assertj.core.data.Offset.offset(1.0));

                Set<String> cards = outcome.cards().stream().map(c -> c.agent().localName()).collect(Collectors.toSet());
                assertThat(cards).as("the semantic query ranked both workers' cards").contains("py-worker", "ts-worker");

                assertThat(outcome.remote()).as("a worker served the remote action").isPresent();
                assertThat(outcome.remote().get().topic()).isEqualTo("council-remote");
                assertThat(outcome.remote().get().worker()).isIn("py-worker", "ts-worker");
            } finally {
                py.destroy();
                ts.destroy();
                py.waitFor(10, TimeUnit.SECONDS);
                ts.waitFor(10, TimeUnit.SECONDS);
                py.destroyForcibly();
                ts.destroyForcibly();
            }
        }
    }
}
