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
package ai.badmonkey.agentspaces.examples.content;

import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.content.Fetch;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A Workers cell holds an artifact in R2 and a JVM peer fetches it (srad/large-networks-bulk-data.md
 * LB-Q-INT-04). The cell side is the TypeScript {@code R2PieceStore} under Node over a bucket that
 * answers ranged gets as R2 does ({@code packages/workers/test/workers/r2-holder.ts}); it joins a
 * JVM hub on {@code ws}, publishes the object, and serves each piece by a ranged get. The JVM
 * fetcher joins the hub on {@code tcp}, so the pieces cross the hub's relay.
 */
class WorkersR2HolderTest {

    private static final Path ROOT = Path.of("").toAbsolutePath().getParent().getParent().getParent();
    private static final Path WORKERS_DIR = ROOT.resolve("agentspaces-typescript/packages/workers");
    private static final Path HOLDER = WORKERS_DIR.resolve("build/test/workers/r2-holder.js");
    private static final int MEBIBYTES = 3;

    private static int freeTcp() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static boolean nodeRuns() {
        try {
            Process process = new ProcessBuilder("node", "--version").redirectErrorStream(true).start();
            process.getInputStream().readAllBytes();
            return process.waitFor(30, TimeUnit.SECONDS) && process.exitValue() == 0;
        } catch (IOException | InterruptedException e) {
            return false;
        }
    }

    @Test
    @Timeout(180)
    void aJvmPeerFetchesAnArtifactAWorkersCellServesFromR2(@TempDir Path dir) throws Exception {
        boolean ready = Files.exists(HOLDER) && nodeRuns();
        if (Boolean.getBoolean("tsclient.required")) {
            assertThat(ready).as("built Workers R2 holder at " + HOLDER).isTrue();
        }
        Assumptions.assumeTrue(ready, "built Workers R2 holder at " + HOLDER + " and node");
        int ws = freeTcp();
        int tcp = freeTcp();
        try (ContentSwarmFleet.Peer hub = ContentSwarmFleet.start("hub", Map.of("ws", ws, "tcp", tcp),
                     Set.of(PeerAdvertisement.PeerRole.RELAY), List.of(), null, dir.resolve("hub"));
             ContentSwarmFleet.Peer fetcher = ContentSwarmFleet.start("fetcher", Map.of("tcp", freeTcp()), Set.of(),
                     List.of("tcp://127.0.0.1:" + tcp), null, dir.resolve("fetcher"))) {
            Process cell = new ProcessBuilder("node", HOLDER.toString(), "ws", "127.0.0.1", String.valueOf(ws),
                    ContentSwarmFleet.FOUNDING, String.valueOf(MEBIBYTES))
                    .directory(WORKERS_DIR.toFile()).redirectErrorStream(true).start();
            StringBuilder output = new StringBuilder();
            Thread pump = Thread.ofVirtual().start(() -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(cell.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        synchronized (output) {
                            output.append(line).append('\n');
                        }
                        System.out.println("  [workers cell] " + line);
                    }
                } catch (IOException ignored) {
                    // the cell ended
                }
            });
            try {
                Pattern rootLine = Pattern.compile("\"root\":\"([^\"]+)\"");
                String root = null;
                long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
                while (root == null && System.nanoTime() < deadline && cell.isAlive()) {
                    synchronized (output) {
                        Matcher m = rootLine.matcher(output);
                        root = m.find() ? m.group(1) : null;
                    }
                    Thread.sleep(100);
                }
                assertThat(root).as("the cell published its R2 object: " + output).isNotNull();

                Fetch fetch = fetcher.content().fetch(root);
                assertThat(fetch.await(Duration.ofSeconds(90))).as(fetch.trace().toString()).isTrue();
                byte[] bytes;
                try (SeekableByteChannel channel = fetch.open()) {
                    ByteBuffer buffer = ByteBuffer.allocate((int) channel.size());
                    while (buffer.hasRemaining() && channel.read(buffer) >= 0) {
                        // drain
                    }
                    bytes = buffer.array();
                }
                assertThat(bytes).hasSize(MEBIBYTES * 1024 * 1024);
                for (int i = 0; i < bytes.length; i++) {
                    if (bytes[i] != (byte) ((i * 31 + 7) & 0xff)) {
                        throw new AssertionError("byte " + i + " differs from the R2 object");
                    }
                }

                OutputStream stdin = cell.getOutputStream();
                stdin.write("stop\n".getBytes(StandardCharsets.UTF_8));
                stdin.flush();
                assertThat(cell.waitFor(30, TimeUnit.SECONDS)).as(output.toString()).isTrue();
                pump.join(5_000);
                Matcher ranged = Pattern.compile("\"ranged\":(\\d+)").matcher(output);
                assertThat(ranged.find()).as(output.toString()).isTrue();
                int gets = Integer.parseInt(ranged.group(1));
                System.out.printf("LB-Q-INT-04: %d ranged R2 gets served %d bytes to a JVM peer%n", gets, bytes.length);
                assertThat(gets).as("the cell served by ranged get, not whole-object reads").isPositive();
            } finally {
                cell.destroy();
                cell.waitFor(5, TimeUnit.SECONDS);
                cell.destroyForcibly();
            }
        }
    }
}
