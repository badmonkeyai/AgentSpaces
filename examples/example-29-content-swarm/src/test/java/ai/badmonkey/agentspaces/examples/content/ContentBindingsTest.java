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

import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.content.Fetch;
import ai.badmonkey.agentspaces.content.PublishOptions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The content swarm on every binding (srad/large-networks-bulk-data.md LB-Q-INT-03) and on a
 * mixed fleet (LB-O-06): three JVM peers on {@code tcp}, {@code tls}, and {@code dtls} around a
 * trainer that listens on all of them plus {@code ws}, where a Node peer joins, fetches, and
 * publishes back. {@code mem} is covered by the module's SimNetwork suite.
 */
class ContentBindingsTest {

    private static final Path ROOT = Path.of("").toAbsolutePath().getParent().getParent().getParent();
    private static final Path NODE_DIR = ROOT.resolve("agentspaces-typescript");
    private static final Path WORKER = NODE_DIR.resolve("build/test/workers/content-worker.js");

    private static int freeTcp() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static int freeUdp() throws IOException {
        try (DatagramSocket socket = new DatagramSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static int freePort(String scheme) throws IOException {
        return scheme.equals("dtls") || scheme.equals("quic") ? freeUdp() : freeTcp();
    }

    private static byte[] readAll(Fetch fetch) throws Exception {
        try (SeekableByteChannel channel = fetch.open()) {
            ByteBuffer buffer = ByteBuffer.allocate((int) channel.size());
            while (buffer.hasRemaining() && channel.read(buffer) >= 0) {
                // drain
            }
            return buffer.array();
        }
    }

    /** Waits until a peer's view holds {@code count} other members (the view leaves the peer itself out). */
    private static boolean awaitMembers(ContentSwarmFleet.Peer peer, int count) throws InterruptedException {
        for (int i = 0; i < 150 && peer.runtime().membership().allMembers().size() < count; i++) {
            Thread.sleep(100);
        }
        return peer.runtime().membership().allMembers().size() >= count;
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"tcp", "tls", "dtls", "quic", "ws", "http"})
    @Timeout(120)
    void twoPeersMoveAnArtifactOnTheBinding(String scheme, @TempDir Path dir) throws Exception {
        int port = freePort(scheme);
        try (ContentSwarmFleet.Peer a = ContentSwarmFleet.start("a-" + scheme, Map.of(scheme, port), Set.of(), List.of(),
                     null, dir.resolve("a"));
             ContentSwarmFleet.Peer b = ContentSwarmFleet.start("b-" + scheme, Map.of(scheme, freePort(scheme)), Set.of(),
                     List.of(scheme + "://127.0.0.1:" + port), null, dir.resolve("b"))) {
            assertThat(awaitMembers(b, 1)).as(scheme + ": the two peers meet").isTrue();
            byte[] artifact = ContentSwarmFleet.artifact(2 * 1024 * 1024 + 17, 30);
            Manifest manifest = a.content().publish(artifact,
                    PublishOptions.defaults().withPieceSize(PublishOptions.pieceSizeFor(Set.of())).withName(scheme + ".bin"));
            Fetch fetch = b.content().fetch(manifest.root());
            assertThat(fetch.await(Duration.ofSeconds(90))).as(scheme + ": " + fetch.trace()).isTrue();
            assertThat(readAll(fetch)).isEqualTo(artifact);
        }
    }

    @Test
    @Timeout(240)
    void aMixedFleetOnTcpTlsDtlsAndANodePeerOnWsSwarmsOneArtifact(@TempDir Path dir) throws Exception {
        Map<String, Integer> trainerPorts = Map.of("tcp", freeTcp(), "tls", freeTcp(), "dtls", freeUdp(), "ws", freeTcp());
        try (ContentSwarmFleet.Peer trainer = ContentSwarmFleet.start("trainer", trainerPorts,
                     Set.of(PeerAdvertisement.PeerRole.RELAY), List.of(), null, dir.resolve("t"));
             ContentSwarmFleet.Peer onTcp = ContentSwarmFleet.start("on-tcp", Map.of("tcp", freeTcp()), Set.of(),
                     List.of("tcp://127.0.0.1:" + trainerPorts.get("tcp")), null, dir.resolve("w1"));
             ContentSwarmFleet.Peer onTls = ContentSwarmFleet.start("on-tls", Map.of("tls", freeTcp()), Set.of(),
                     List.of("tls://127.0.0.1:" + trainerPorts.get("tls")), null, dir.resolve("w2"));
             ContentSwarmFleet.Peer onDtls = ContentSwarmFleet.start("on-dtls", Map.of("dtls", freeUdp()), Set.of(),
                     List.of("dtls://127.0.0.1:" + trainerPorts.get("dtls")), null, dir.resolve("w3"))) {
            assertThat(awaitMembers(trainer, 3)).as("the trainer sees three workers").isTrue();
            byte[] checkpoint = ContentSwarmFleet.artifact(4 * 1024 * 1024 + 99, 31);
            Manifest manifest = trainer.content().publish(checkpoint,
                    PublishOptions.defaults().withPieceSize(ContentSwarmFleet.PIECE_SIZE).withName("checkpoint.bin"));
            List<Fetch> fetches = List.of(onTcp.content().fetch(manifest.root()), onTls.content().fetch(manifest.root()),
                    onDtls.content().fetch(manifest.root()));
            // LB-Q-REL-05: while pieces flow, no peer suspects another (a range never holds a probe for long)
            List<ContentSwarmFleet.Peer> fleet = List.of(trainer, onTcp, onTls, onDtls);
            int suspicions = 0;
            long until = System.nanoTime() + Duration.ofSeconds(120).toNanos();
            while (System.nanoTime() < until && !fetches.stream().allMatch(Fetch::complete)) {
                for (ContentSwarmFleet.Peer p : fleet) {
                    suspicions += (int) p.runtime().membership().allMembers().stream().filter(m -> m.suspect()).count();
                }
                Thread.sleep(50);
            }
            for (Fetch fetch : fetches) {
                assertThat(fetch.await(Duration.ofSeconds(1))).as(fetch.trace().toString()).isTrue();
                assertThat(readAll(fetch)).isEqualTo(checkpoint);
                System.out.printf("LB-Q-PRF-04: window fill per source %s%n", fetch.windowFill());
                // a source that served ranges kept its window at least half full (sources that served
                // nothing hold no samples and read 0)
                assertThat(fetch.windowFill().values().stream().mapToDouble(Double::doubleValue).max().orElse(0))
                        .as("LB-Q-PRF-04 window fill of the busiest source").isGreaterThanOrEqualTo(0.5);
            }
            System.out.printf("LB-Q-REL-05: %d suspicion observations across the swarm on tcp, tls, and dtls%n", suspicions);
            assertThat(suspicions).as("LB-Q-REL-05").isZero();

            // the Node peer on ws: it fetches from the fleet and publishes back
            boolean nodeReady = Files.exists(WORKER) && runs(List.of("node", "--version"));
            if (Boolean.getBoolean("tsclient.required")) {
                assertThat(nodeReady).as("built TypeScript content worker at " + WORKER).isTrue();
            }
            Assumptions.assumeTrue(nodeReady, "built TypeScript content worker at " + WORKER + " and node");
            Process worker = new ProcessBuilder("node", WORKER.toString(), "127.0.0.1", String.valueOf(trainerPorts.get("ws")),
                    ContentSwarmFleet.FOUNDING, manifest.root(), "180", "ws")
                    .directory(NODE_DIR.toFile()).redirectErrorStream(true).start();
            StringBuilder output = new StringBuilder();
            Thread pump = Thread.ofVirtual().start(() -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(worker.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        output.append(line).append('\n');
                        System.out.println("  [node on ws] " + line);
                    }
                } catch (IOException ignored) {
                    // the worker ended
                }
            });
            try {
                String nodeRoot = null;
                long deadline = System.nanoTime() + Duration.ofSeconds(150).toNanos();
                while (nodeRoot == null && System.nanoTime() < deadline && worker.isAlive()) {
                    for (Manifest m : trainer.content().list(100)) {
                        if ("from-typescript.bin".equals(m.name())) {
                            nodeRoot = m.root();
                        }
                    }
                    Thread.sleep(200);
                }
                assertThat(nodeRoot).as("the Node peer published over ws: " + output).isNotNull();
                Fetch back = trainer.content().fetch(nodeRoot);
                Fetch backOnTcp = onTcp.content().fetch(nodeRoot);
                assertThat(back.await(Duration.ofSeconds(60))).as(back.trace().toString()).isTrue();
                assertThat(backOnTcp.await(Duration.ofSeconds(60))).as(backOnTcp.trace().toString()).isTrue();
                assertThat(worker.waitFor(120, TimeUnit.SECONDS)).as(output.toString()).isTrue();
                pump.join(5_000);
                String report = output.toString();
                assertThat(worker.exitValue()).as(report).isZero();
                assertThat(report).contains("\"ok\":true").contains("\"scheme\":\"ws\"");
                assertThat(report).contains(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(checkpoint)));
            } finally {
                worker.destroy();
                worker.waitFor(5, TimeUnit.SECONDS);
                worker.destroyForcibly();
            }
        }
    }

    private static boolean runs(List<String> command) {
        try {
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            process.getInputStream().readAllBytes();
            return process.waitFor(30, TimeUnit.SECONDS) && process.exitValue() == 0;
        } catch (IOException | InterruptedException e) {
            return false;
        }
    }
}
