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
package ai.badmonkey.agentspaces.transport.http;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The TypeScript client over the HTTP bindings (srad/agentspaces-js.md
 * JS-Q-REL-02, JS-H-05, JS-H-06, JS-H-07): a Java peer listens on {@code ws}
 * and {@code http} in SESSION mode and writes a Job; a Node worker running
 * the portable core dials each scheme, joins by the founding, takes the Job
 * under the claim lattice, completes it, and writes a Done; the Java side
 * reads the Done and both sides counted frames under the session key. Needs
 * Node and the built TypeScript client; skipped otherwise unless
 * {@code -Dtsclient.required=true}.
 */
class TypeScriptClientTest {

    public record Job(String jobId, String payload) {
    }

    public record Done(String jobId, String by) {
    }

    private static final Path ROOT = Path.of("").toAbsolutePath().getParent().getParent();
    private static final Path NODE_DIR = ROOT.resolve("agentspaces-typescript");
    private static final Path WORKER = NODE_DIR.resolve("build/test/workers/fleet-worker.js");
    private static final String FOUNDING = "ts-http-fleet-v1";

    private static void require(boolean condition, String why) {
        if (Boolean.getBoolean("tsclient.required")) {
            assertThat(condition).as(why).isTrue();
        }
        Assumptions.assumeTrue(condition, why);
    }

    @Test
    @Timeout(180)
    void aTypeScriptPeerJoinsOverWsAndOverTheHttpExchangeUnderSessionAttestation() throws Exception {
        require(Files.exists(WORKER), "typescript client built (npm test) at " + NODE_DIR);
        for (String scheme : new String[]{"ws", "http"}) {
            runOne(scheme);
        }
    }

    private void runOne(String scheme) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).channelAuth(PeerNode.ChannelAuth.SESSION).build();
        int port = Wire.freePort();
        HttpBinding transport = scheme.equals("ws") ? new WebSocketTransport()
                : new HttpExchangeTransport().options(HttpOptions.defaults().withLongPoll(Duration.ofSeconds(2)));
        GroupId groupId = GroupId.fromFounding(FOUNDING.getBytes(StandardCharsets.UTF_8));
        try (transport) {
            node.listen(transport, "127.0.0.1:" + port);
            GroupRuntime runtime = node.joinGroup(new GroupAdvertisement("aspace://" + groupId.value(),
                    PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(365), FOUNDING,
                    GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                    GroupAdvertisement.GossipParameters.defaults()),
                    new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2), List.of());
            ReplicatedSpace space = ReplicatedSpace.builder(runtime, "jobs", identity, "host")
                    .settleWindow(Duration.ofMillis(150)).build();
            node.startTicking(Duration.ofMillis(250));
            space.write(new Job("job-1", "compress the archives"), Lease.of(Duration.ofMinutes(10)));

            Process worker = new ProcessBuilder("node", WORKER.toString(), scheme, "127.0.0.1", String.valueOf(port),
                    FOUNDING, Job.class.getName() + "#v1", Done.class.getName() + "#v1", "60")
                    .directory(NODE_DIR.toFile()).redirectErrorStream(true).start();
            StringBuilder output = new StringBuilder();
            Thread pump = Thread.ofVirtual().start(() -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(worker.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        output.append(line).append('\n');
                        System.out.println("  [node " + scheme + "] " + line);
                    }
                } catch (java.io.IOException ignored) {
                    // the worker ended
                }
            });
            try {
                Optional<Done> done = Optional.empty();
                long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
                while (done.isEmpty() && System.nanoTime() < deadline) {
                    done = space.read(Template.of(Done.class));
                    Thread.sleep(200);
                }
                assertThat(done).as(scheme + ": the worker's Done arrived\n" + output).isPresent();
                assertThat(done.get().by()).isEqualTo("typescript-" + scheme);
                assertThat(worker.waitFor(30, TimeUnit.SECONDS)).as("worker exited").isTrue();
                pump.join(5_000);
                assertThat(worker.exitValue()).as(output.toString()).isZero();
                assertThat(output.toString()).contains("\"ok\":true").contains("\"channel\":\"" + scheme + "\"");
                assertThat(output.toString()).doesNotContain("\"sessionFramesSent\":0,");
                assertThat(node.sessionFramesSent()).as("the Java side tagged frames to the worker").isPositive();
                assertThat(space.read(Template.of(Job.class))).as("the job was completed").isEmpty();
            } finally {
                worker.destroy();
                worker.waitFor(5, TimeUnit.SECONDS);
                worker.destroyForcibly();
                space.close();
                node.close();
            }
        }
    }
}
