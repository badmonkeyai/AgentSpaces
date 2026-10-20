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
package ai.badmonkey.agentspaces.capabilities.orderedlog;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.space.replicated.TakeClaim;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A TypeScript peer takes through the ordered log as a Raft client
 * (srad/agentspaces-js.md JS-V-05; reviews/L3L4-COVERAGE.md §4 "a Raft client,
 * not a Raft voter"): three Java voters over TCP run the log and the ordered
 * takes coordinator on a {@code jobs} space; the Node worker dials the first,
 * finds the log in the advertisements, submits its signed claim command, sees
 * the committed claim fold back from the space, completes the Job, and writes
 * a Done. Every voter's commit listener saw the client's claim, and the client
 * was admitted under {@code RAFT_CLIENT} without ever voting. Needs Node and
 * the built client; skipped otherwise unless {@code -Dtsclient.required=true}.
 */
class TypeScriptOrderedTakeTest {

    public record Job(String jobId, String payload) {
    }

    public record Done(String jobId, String by) {
    }

    private record Member(PeerNode node, ReplicatedSpace space, RaftLog raft, OrderedTakes ordered,
                          List<TakeClaim> committed, TcpTransport transport) {
    }

    private static final Path ROOT = Path.of("").toAbsolutePath().getParent().getParent();
    private static final Path NODE_DIR = ROOT.resolve("agentspaces-typescript");
    private static final Path WORKER = NODE_DIR.resolve("build/test/workers/ordered-worker.js");
    private static final String FOUNDING = "ts-ordered-take-v1";
    private final CborCodec codec = CborCodec.defaultCodec();
    private final List<Member> members = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (Member member : members) {
            member.space().close();
            member.node().close();
        }
    }

    private static void require(boolean condition, String why) {
        if (Boolean.getBoolean("tsclient.required")) {
            assertThat(condition).as(why).isTrue();
        }
        Assumptions.assumeTrue(condition, why);
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    @Timeout(180)
    void aTypeScriptClientTakesExactlyOnceThroughThreeJavaVoters() throws Exception {
        require(Files.exists(WORKER), "typescript client built (npm test) at " + NODE_DIR);
        GroupId groupId = GroupId.fromFounding(FOUNDING.getBytes(StandardCharsets.UTF_8));
        GroupAdvertisement group = new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(365), FOUNDING,
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
        List<PeerIdentity> identities = List.of(PeerIdentity.generate(), PeerIdentity.generate(),
                PeerIdentity.generate());
        List<PeerId> memberIds = identities.stream().map(PeerIdentity::peerId).toList();
        int seedPort = freePort();
        for (int i = 0; i < 3; i++) {
            PeerIdentity identity = identities.get(i);
            PeerNode node = PeerNode.builder(identity).build();
            TcpTransport transport = new TcpTransport();
            int port = i == 0 ? seedPort : freePort();
            node.listen(transport, "127.0.0.1:" + port);
            GroupRuntime runtime = node.joinGroup(group,
                    new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2),
                    i == 0 ? List.of() : List.of(new PeerAdvertisement.Endpoint("tcp", "127.0.0.1:" + seedPort, 0)));
            ReplicatedSpace space = ReplicatedSpace.builder(runtime, "jobs", identity, "host")
                    .settleWindow(Duration.ofMillis(150)).build();
            List<TakeClaim> committed = new CopyOnWriteArrayList<>();
            OrderedTakes[] holder = new OrderedTakes[1];
            RaftLog raft = new RaftLog(new CapabilityPipes(runtime, codec), codec, identity.peerId(), memberIds,
                    Clock.systemUTC(), 4000 + i, (command, index) -> {
                        if (holder[0] != null) {
                            holder[0].onCommand(command, index);
                        }
                    });
            OrderedTakes ordered = new OrderedTakes(raft, space, identity, "host", codec, Clock.systemUTC());
            holder[0] = ordered;
            ordered.onCommitted((entryId, claim) -> committed.add(claim));
            DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, Clock.systemUTC()),
                    codec, identity.peerId());
            new CapabilityRuntime(runtime, discovery, identity).register(raft);
            node.startTicking(Duration.ofMillis(250));
            members.add(new Member(node, space, raft, ordered, committed, transport));
        }
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline && members.stream().noneMatch(m -> m.raft().isLeader())) {
            Thread.sleep(200);
        }
        assertThat(members.stream().anyMatch(m -> m.raft().isLeader())).as("a leader was elected").isTrue();
        Member seed = members.get(0);
        seed.space().write(new Job("job-1", "settle the invoice"), Lease.of(Duration.ofMinutes(10)));

        Process worker = new ProcessBuilder("node", WORKER.toString(), "127.0.0.1", String.valueOf(seedPort),
                FOUNDING, Job.class.getName() + "#v1", Done.class.getName() + "#v1", "60")
                .directory(NODE_DIR.toFile()).redirectErrorStream(true).start();
        StringBuilder output = new StringBuilder();
        Thread pump = Thread.ofVirtual().start(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(worker.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append('\n');
                    System.out.println("  [node ordered] " + line);
                }
            } catch (IOException ignored) {
                // the worker ended
            }
        });
        try {
            Optional<Done> done = Optional.empty();
            long wait = System.nanoTime() + Duration.ofSeconds(90).toNanos();
            while (done.isEmpty() && System.nanoTime() < wait) {
                done = seed.space().read(Template.of(Done.class));
                Thread.sleep(200);
            }
            String counters = members.stream().map(m -> m.node().peerId().value().substring(0, 8) + ": submits="
                    + m.raft().clientSubmits() + " refused=" + m.raft().clientSubmitsRefused() + " leader="
                    + m.raft().isLeader() + " committed=" + m.committed().size()).toList().toString();
            assertThat(done).as("the client's Done arrived\n" + output + counters).isPresent();
            assertThat(done.get().by()).isEqualTo("typescript-ordered");
            assertThat(worker.waitFor(30, TimeUnit.SECONDS)).isTrue();
            pump.join(5_000);
            assertThat(worker.exitValue()).as(output.toString()).isZero();
            assertThat(output.toString()).contains("\"ok\":true").contains("\"epoch\":1");
            // The log decided the generation: every voter's commit listener saw the client's claim once.
            for (Member member : members) {
                assertThat(member.committed()).as("committed at " + member.node().peerId()).hasSize(1);
                assertThat(member.committed().getFirst().holder().encoded()).endsWith("/ts-worker");
            }
            assertThat(members.stream().mapToLong(m -> m.raft().clientSubmits()).sum())
                    .as("the client's submit was admitted under RAFT_CLIENT").isPositive();
            assertThat(seed.space().read(Template.of(Job.class))).as("the job was completed").isEmpty();
        } finally {
            worker.destroy();
            worker.waitFor(5, TimeUnit.SECONDS);
            worker.destroyForcibly();
        }
    }
}
