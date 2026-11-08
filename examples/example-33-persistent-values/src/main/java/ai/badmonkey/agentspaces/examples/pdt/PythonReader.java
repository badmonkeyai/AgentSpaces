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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The Python reader of §7.3 (PD-O-04, PD-C-04): drives
 * {@code agentspaces-python/tests/workers/pdt_worker.py}, which dials a Java
 * peer over tcp, joins the group by its founding, holds the {@code facts} map
 * ref, and answers {@code deref} with the root it computed, so the example can
 * compare it with the Java readers' root (PD-Q-INT-01). The interpreter is
 * {@code $PYTHON} or {@code /tmp/aspace-venv4/bin/python}, as the other
 * cross-language tests use; the reader is skipped when either is absent.
 */
public final class PythonReader implements AutoCloseable {

    static final String PYTHON = System.getenv().getOrDefault("PYTHON", "/tmp/aspace-venv4/bin/python");
    private static final Duration ANSWER_TIMEOUT = Duration.ofSeconds(180);
    private static final Pattern ROOT = Pattern.compile("\"root\":\"([^\"]+)\"");
    private static final Pattern PEER = Pattern.compile("\"peerId\":\"([^\"]+)\"");

    private final String host;
    private final int port;
    private final BlockingQueue<String> answers = new LinkedBlockingQueue<>();
    private Process worker;
    private Writer stdin;
    private String peerId;

    public PythonReader(String host, int port) {
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
        return dir == null ? null : dir.resolve("tests/workers/pdt_worker.py");
    }

    /** Whether the worker file and an interpreter with the client's pdt package are present. */
    public static boolean available() {
        return whyUnavailable() == null;
    }

    /** Why the reader is unavailable, or null when it can run. */
    public static String whyUnavailable() {
        Path worker = worker();
        if (worker == null || !Files.isRegularFile(worker)) {
            return "no agentspaces-python/tests/workers/pdt_worker.py beside the core";
        }
        if (!Files.isExecutable(Path.of(PYTHON))) {
            return "no interpreter at " + PYTHON + " (set PYTHON)";
        }
        try {
            Process check = new ProcessBuilder(PYTHON, "-c", "import agentspaces.pdt, agentspaces.capabilities")
                    .directory(pythonDir().toFile()).redirectErrorStream(true).start();
            String output = new String(check.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            if (!check.waitFor(30, TimeUnit.SECONDS) || check.exitValue() != 0) {
                return PYTHON + " cannot import agentspaces.pdt: " + output.trim();
            }
        } catch (IOException | InterruptedException e) {
            return PYTHON + " failed: " + e.getMessage();
        }
        return null;
    }

    /**
     * Starts the worker and waits for its join line.
     *
     * @param founding     the group's founding text
     * @param refName      the map ref to hold
     * @param refSpace     the ref space
     * @param contentSpace the content swarm's space
     * @throws IOException when the worker fails to start or to join
     */
    public void start(String founding, String refName, String refSpace, String contentSpace) throws IOException {
        worker = new ProcessBuilder(PYTHON, worker().toString(), host, String.valueOf(port), founding, refName, refSpace,
                contentSpace).directory(pythonDir().toFile()).redirectErrorStream(true).start();
        stdin = new OutputStreamWriter(worker.getOutputStream(), StandardCharsets.UTF_8);
        Thread.ofVirtual().name("python-reader").start(() -> {
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
        String joined = answer();
        if (!joined.contains("\"ok\":true")) {
            throw new IOException("the Python reader did not join: " + joined);
        }
        Matcher matcher = PEER.matcher(joined);
        peerId = matcher.find() ? matcher.group(1) : "?";
    }

    public String peerId() {
        return peerId;
    }

    public boolean running() {
        return worker != null && worker.isAlive();
    }

    /** Asks the worker to pull the ref space and deref; the root of the value it computed. */
    public synchronized String derefRoot() throws IOException {
        String line = command("deref");
        Matcher matcher = ROOT.matcher(line);
        if (!line.contains("\"ok\":true") || !matcher.find()) {
            throw new IOException("the Python reader failed to deref: " + abbreviate(line));
        }
        return matcher.group(1);
    }

    /**
     * Asks the worker to assoc one key of the map ref it holds (a Python-side swap, PD-Q-INT-03);
     * the root of the value it computed.
     */
    public synchronized String swap(String key, String value) throws IOException {
        String line = command("swap " + key + " " + value);
        Matcher matcher = ROOT.matcher(line);
        if (!line.contains("\"ok\":true") || !matcher.find()) {
            throw new IOException("the Python reader failed to swap: " + abbreviate(line));
        }
        return matcher.group(1);
    }

    /**
     * Asks the worker to pull an application space and report its live entries of a schema whose
     * payload it holds, an escalated payload reassembled through its facade (PD-E-01, PD-C-04);
     * the raw JSON answer line.
     */
    public synchronized String read(String space, String schema) throws IOException {
        String line = command("read " + space + " " + schema);
        if (!line.contains("\"ok\":true")) {
            throw new IOException("the Python reader failed to read: " + abbreviate(line));
        }
        return line;
    }

    /**
     * Asks the worker for the roots of the maps and files in a CBOR file (PD-Q-INT-01); the raw JSON
     * answer line with {@code maps} and {@code files} lists of roots.
     */
    public synchronized String roots(java.nio.file.Path file) throws IOException {
        String line = command("roots " + file.toAbsolutePath());
        if (!line.contains("\"ok\":true")) {
            throw new IOException("the Python reader failed to compute roots: " + abbreviate(line));
        }
        return line;
    }

    private String command(String command) throws IOException {
        stdin.write(command);
        stdin.write('\n');
        stdin.flush();
        return answer();
    }

    private String answer() throws IOException {
        try {
            String line = answers.poll(ANSWER_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            if (line == null) {
                throw new IOException("no answer from the Python reader within " + ANSWER_TIMEOUT.toSeconds() + " s");
            }
            return line;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted waiting for the Python reader", e);
        }
    }

    private static String abbreviate(String line) {
        return line.length() > 300 ? line.substring(0, 300) + "..." : line;
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

    static List<String> describe() {
        return List.of("interpreter " + PYTHON, "worker " + worker());
    }
}
