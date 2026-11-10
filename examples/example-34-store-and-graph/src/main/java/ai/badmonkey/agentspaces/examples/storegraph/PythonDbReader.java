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
package ai.badmonkey.agentspaces.examples.storegraph;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * The Python reader of example 34 (P2-O-03, P2-D-09): drives {@code agentspaces-python/tests/workers/pdt_db_worker.py},
 * which dials a Java peer over tcp, joins the group by its founding, follows the ref space lazily, and answers
 * {@code lookup} and {@code datoms} on the {@code db} ref through the client's {@code DbReader}. The interpreter is
 * {@code $PYTHON} or {@code /tmp/aspace-venv4/bin/python}, as the other cross-language tests use.
 */
public final class PythonDbReader implements AutoCloseable {

    static final String PYTHON = System.getenv().getOrDefault("PYTHON", "/tmp/aspace-venv4/bin/python");
    private static final Duration ANSWER_TIMEOUT = Duration.ofSeconds(180);
    private static final ObjectMapper JSON = new ObjectMapper();

    private final String host;
    private final int port;
    private final BlockingQueue<String> answers = new LinkedBlockingQueue<>();
    private Process worker;
    private Writer stdin;

    public PythonDbReader(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /** The {@code agentspaces-python} directory beside the core, found by walking up from the working directory. */
    static Path pythonDir() {
        Path dir = Path.of("").toAbsolutePath();
        while (dir != null) {
            Path candidate = dir.resolve("agentspaces-python");
            if (Files.isDirectory(candidate)) {
                return candidate;
            }
            dir = dir.getParent();
        }
        return null;
    }

    static Path worker() {
        Path dir = pythonDir();
        return dir == null ? null : dir.resolve("tests/workers/pdt_db_worker.py");
    }

    /** Whether the worker file and an interpreter with the client's pdt package are present. */
    public static boolean available() {
        return whyUnavailable() == null;
    }

    /** Why the reader is unavailable, or null when it can run. */
    public static String whyUnavailable() {
        Path worker = worker();
        if (worker == null || !Files.isRegularFile(worker)) {
            return "no agentspaces-python/tests/workers/pdt_db_worker.py beside the core";
        }
        if (!Files.isExecutable(Path.of(PYTHON))) {
            return "no interpreter at " + PYTHON + " (set PYTHON)";
        }
        try {
            Process check = new ProcessBuilder(PYTHON, "-c", "import agentspaces.pdt.db, agentspaces.capabilities")
                    .directory(pythonDir().toFile()).redirectErrorStream(true).start();
            String output = new String(check.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            if (!check.waitFor(30, TimeUnit.SECONDS) || check.exitValue() != 0) {
                return PYTHON + " cannot import agentspaces.pdt.db: " + output.trim();
            }
        } catch (IOException | InterruptedException e) {
            return PYTHON + " failed: " + e.getMessage();
        }
        return null;
    }

    /**
     * Starts the worker and waits for its join line.
     *
     * @param founding the group's founding text
     * @param refName  the {@code db} ref to read
     * @throws IOException when the worker fails to start or to join
     */
    public void start(String founding, String refName) throws IOException {
        worker = new ProcessBuilder(PYTHON, worker().toString(), host, String.valueOf(port), founding, refName,
                StoreGraphFleet.REF_SPACE, StoreGraphFleet.CONTENT_SPACE).directory(pythonDir().toFile())
                .redirectErrorStream(true).start();
        stdin = new OutputStreamWriter(worker.getOutputStream(), StandardCharsets.UTF_8);
        Thread.ofVirtual().name("python-db-reader").start(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(worker.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith("{")) {
                        answers.add(line);
                    } else {
                        System.out.println("  [python] " + line);
                    }
                }
            } catch (IOException ignored) {
                // the worker ended
            }
        });
        JsonNode joined = answer();
        if (!joined.path("ok").asBoolean()) {
            throw new IOException("the Python reader did not join: " + joined);
        }
    }

    /**
     * Waits until the reader's ref derefs to a root: the reader pulls the ref space and walks no index.
     *
     * @return the answer, with {@code root}, {@code heads}, and {@code held}
     */
    public synchronized JsonNode await(String root, Duration max) throws IOException {
        return command("await " + root + " " + max.toSeconds());
    }

    /** {@code (d/entid db [attr value])} and the entity's attributes; the answer carries {@code e} and {@code entity}. */
    public synchronized JsonNode lookup(String attr, Object value) throws IOException {
        return command("lookup " + attr + " " + JSON.writeValueAsString(value));
    }

    /** {@code (d/datoms db index c1 c2 ...)}; the answer carries {@code datoms} as {@code [e, a, v, tx]} arrays. */
    public synchronized JsonNode datoms(String index, Object... components) throws IOException {
        return command("datoms " + index + " " + JSON.writeValueAsString(components));
    }

    private JsonNode command(String command) throws IOException {
        stdin.write(command);
        stdin.write('\n');
        stdin.flush();
        JsonNode answer = answer();
        if (!answer.path("ok").asBoolean()) {
            throw new IOException("the Python reader failed '" + command + "': " + answer);
        }
        return answer;
    }

    private JsonNode answer() throws IOException {
        try {
            String line = answers.poll(ANSWER_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            if (line == null) {
                throw new IOException("no answer from the Python reader within " + ANSWER_TIMEOUT.toSeconds() + " s");
            }
            return JSON.readTree(line);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted waiting for the Python reader", e);
        }
    }

    @Override
    public void close() {
        if (worker == null) {
            return;
        }
        try {
            stdin.write("quit\n");
            stdin.flush();
            worker.waitFor(10, TimeUnit.SECONDS);
        } catch (IOException | InterruptedException ignored) {
            // the worker is going away either way
        }
        worker.destroy();
        try {
            worker.waitFor(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        worker.destroyForcibly();
        worker = null;
    }
}
