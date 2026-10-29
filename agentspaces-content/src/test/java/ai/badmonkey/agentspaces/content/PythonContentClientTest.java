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
package ai.badmonkey.agentspaces.content;

import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Python client in the swarm (srad/large-networks-bulk-data.md §10, LB-C rows): a
 * Java relay publishes an artifact, a second Java member mirrors it, the Python worker
 * fetches it from both (the relay directly, the member through the relay) and publishes
 * one of its own, which both Java members fetch back (one of them through the relay).
 * Skipped when the worker or a Python with {@code cryptography} is missing; set
 * {@code -Dtsclient.required=true} to fail instead.
 */
class PythonContentClientTest {

    private static final Path ROOT = Path.of("").toAbsolutePath().getParent().getParent();
    private static final Path PYTHON_DIR = ROOT.resolve("agentspaces-python");
    private static final Path WORKER = PYTHON_DIR.resolve("tests/workers/content_worker.py");
    private static final String PYTHON = System.getenv().getOrDefault("PYTHON", "/tmp/aspace-venv4/bin/python");
    private static final String FOUNDING = "py-content-client-v1";
    private static final int PIECE_SIZE = 256 * 1024;
    private static final CborCodec CODEC = CborCodec.defaultCodec();

    private record Member(PeerNode node, GroupRuntime runtime, ReplicatedSpace space, Content content) {
        void close() {
            content.close();
            space.close();
            node.close();
        }
    }

    private final List<Member> members = new ArrayList<>();
    private Process worker;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (worker != null) {
            worker.destroy();
            worker.waitFor(5, TimeUnit.SECONDS);
            worker.destroyForcibly();
        }
        members.forEach(Member::close);
    }

    private static void require(boolean condition, String why) {
        if (Boolean.getBoolean("tsclient.required")) {
            assertThat(condition).as(why).isTrue();
        }
        Assumptions.assumeTrue(condition, why);
    }

    private static boolean runs(List<String> command, Path directory) {
        try {
            Process process = new ProcessBuilder(command).directory(directory.toFile())
                    .redirectErrorStream(true).start();
            process.getInputStream().readAllBytes();
            return process.waitFor(30, TimeUnit.SECONDS) && process.exitValue() == 0;
        } catch (IOException | InterruptedException e) {
            return false;
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static String field(String json, String name) {
        Matcher matcher = Pattern.compile("\"" + name + "\":\"([^\"]+)\"").matcher(json);
        assertThat(matcher.find()).as(name + " in " + json).isTrue();
        return matcher.group(1);
    }

    private Member start(String name, int port, List<String> seeds, Set<PeerAdvertisement.PeerRole> roles) throws IOException {
        GroupId groupId = GroupId.fromFounding(FOUNDING.getBytes(StandardCharsets.UTF_8));
        GroupAdvertisement group = new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(365), FOUNDING,
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).roles(roles).build();
        node.listen(new TcpTransport(), "127.0.0.1:" + port);
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String seed : seeds) {
            endpoints.add(new PeerAdvertisement.Endpoint("tcp", seed, 0));
        }
        GroupRuntime runtime = node.joinGroup(group, GroupMembership.Config.defaults(), endpoints);
        BlockExchange blocks = new BlockExchange(runtime, CODEC);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "content", identity, "content")
                .blocks(blocks).build();
        Content content = Content.builder(new CapabilityPipes(runtime, CODEC), runtime, space, identity.peerId())
                .codec(CODEC).store(new MemoryPieceStore()).blocks(blocks).random(new Random(name.hashCode())).build();
        runtime.onTick(content::tick);
        content.start();
        node.startTicking(Duration.ofMillis(100));
        Member member = new Member(node, runtime, space, content);
        members.add(member);
        return member;
    }

    private static byte[] readAll(Fetch fetch) throws IOException {
        try (SeekableByteChannel channel = fetch.open()) {
            ByteBuffer buffer = ByteBuffer.allocate((int) channel.size());
            while (buffer.hasRemaining() && channel.read(buffer) >= 0) {
                // drain
            }
            return buffer.array();
        }
    }

    private static byte[] pythonArtifact() {
        byte[] out = new byte[3 * 65_536 + 77];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) ((i * 13 + 5) & 0xFF);
        }
        return out;
    }

    @Test
    @Timeout(240)
    void aPythonPeerFetchesThroughTheRelayAndServesBackToIt() throws Exception {
        require(Files.exists(WORKER), "python content worker at " + WORKER);
        require(runs(List.of(PYTHON, "-c", "import cryptography"), PYTHON_DIR),
                PYTHON + " with the cryptography package (set PYTHON to another interpreter)");
        int relayPort = freePort();
        Member relay = start("relay", relayPort, List.of(), Set.of(PeerAdvertisement.PeerRole.RELAY));
        Member mirror = start("mirror", freePort(), List.of("127.0.0.1:" + relayPort), Set.of());
        for (int i = 0; i < 100 && mirror.runtime().membership().allMembers().size() < 2; i++) {
            Thread.sleep(100);
        }
        byte[] artifact = new byte[3 * 1024 * 1024 + 123];
        new Random(29).nextBytes(artifact);
        Manifest manifest = relay.content().publish(artifact,
                PublishOptions.defaults().withPieceSize(PIECE_SIZE).withName("from-java.bin"));
        Fetch mirrored = mirror.content().fetch(manifest.root());
        assertThat(mirrored.await(Duration.ofSeconds(60))).as(mirrored.trace().toString()).isTrue();

        worker = new ProcessBuilder(PYTHON, WORKER.toString(), "127.0.0.1", String.valueOf(relayPort), FOUNDING,
                manifest.root(), "120").directory(PYTHON_DIR.toFile()).redirectErrorStream(true).start();
        StringBuilder output = new StringBuilder();
        Thread pump = Thread.ofVirtual().start(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(worker.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append('\n');
                    System.out.println("  [python content] " + line);
                }
            } catch (IOException ignored) {
                // the worker ended
            }
        });

        // The Python peer publishes once its fetch completed; both Java members fetch that root,
        // the mirror through the relay (the Python peer's relay.via hint names it).
        String pythonRoot = null;
        long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
        while (pythonRoot == null && System.nanoTime() < deadline) {
            for (Manifest m : relay.content().list(100)) {
                if ("from-python.bin".equals(m.name())) {
                    pythonRoot = m.root();
                }
            }
            Thread.sleep(200);
        }
        assertThat(pythonRoot).as("the Python manifest reached the relay").isNotNull();
        Fetch onRelay = relay.content().fetch(pythonRoot);
        Fetch onMirror = mirror.content().fetch(pythonRoot);
        assertThat(onRelay.await(Duration.ofSeconds(90))).as(onRelay.trace().toString()).isTrue();
        assertThat(onMirror.await(Duration.ofSeconds(90))).as(onMirror.trace().toString()).isTrue();
        assertThat(readAll(onRelay)).isEqualTo(pythonArtifact());
        assertThat(readAll(onMirror)).isEqualTo(pythonArtifact());

        assertThat(worker.waitFor(120, TimeUnit.SECONDS)).as("worker finished: " + output).isTrue();
        pump.join(5_000);
        String report = output.toString();
        assertThat(worker.exitValue()).as(report).isZero();
        assertThat(report).contains("\"ok\":true");
        assertThat(field(report, "sha256")).isEqualTo(HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(artifact)));
        assertThat(report).contains("\"bytes\":" + artifact.length);
        assertThat(field(report, "seedPeer")).isEqualTo(relay.node().peerId().value());
        // bytes came from both holders: the relay directly and the mirror through the relay
        assertThat(report).contains("\"" + relay.node().peerId().value() + "\":")
                .contains("\"" + mirror.node().peerId().value() + "\":");
        assertThat(report).contains("\"javaHolders\":[");
        assertThat(report).contains(relay.node().peerId().value()).contains(mirror.node().peerId().value());
        assertThat(mirror.node().traffic().receivedByKind()).as("the mirror received relayed frames")
                .containsKey(ai.badmonkey.agentspaces.peering.wire.Envelope.Kind.RELAY_FRAME);
    }
}
