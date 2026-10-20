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
 * A browser peer in a Java fleet (srad/agentspaces-js.md JS-Q-REL-02,
 * JS-Q-TST-02, JS-Q-SEC-01): a Java peer listens on {@code ws} and
 * {@code http} with the page's origin allowed, in SESSION mode; a headless
 * browser (Chromium through Playwright, driven by a Node script) loads the
 * portable core from a page, dials each scheme, joins by the founding, takes
 * the Job, completes it, and writes a Done, which the Java side reads. The
 * {@code http} run crosses origins, so it exercises the CORS preflight.
 * Needs Node, the built client, and Playwright's Chromium; skipped otherwise
 * unless {@code -Dbrowser.required=true}.
 */
class BrowserFleetTest {

    public record Job(String jobId, String payload) {
    }

    public record Done(String jobId, String by) {
    }

    private static final Path ROOT = Path.of("").toAbsolutePath().getParent().getParent();
    private static final Path NODE_DIR = ROOT.resolve("agentspaces-typescript");
    private static final Path DRIVER = NODE_DIR.resolve("build/test/workers/browser-worker.js");
    private static final String FOUNDING = "browser-fleet-v1";

    private static void require(boolean condition, String why) {
        if (Boolean.getBoolean("browser.required")) {
            assertThat(condition).as(why).isTrue();
        }
        Assumptions.assumeTrue(condition, why);
    }

    @Test
    @Timeout(240)
    void aBrowserPeerJoinsOverWsAndTheHttpExchangeFromAnAllowedOrigin() throws Exception {
        require(Files.exists(DRIVER), "typescript client built (npm test) at " + NODE_DIR);
        require(Files.isDirectory(NODE_DIR.resolve("node_modules/playwright")), "playwright installed in " + NODE_DIR);
        for (String scheme : new String[]{"ws", "http"}) {
            runOne(scheme);
        }
    }

    private void runOne(String scheme) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).channelAuth(PeerNode.ChannelAuth.SESSION).build();
        int port = Wire.freePort();
        int pagePort = Wire.freePort();
        HttpOptions options = HttpOptions.defaults()
                .withOrigins(OriginPolicy.of(List.of("http://127.0.0.1:" + pagePort)))
                .withLongPoll(Duration.ofSeconds(2));
        HttpBinding transport = scheme.equals("ws") ? new WebSocketTransport().options(options)
                : new HttpExchangeTransport().options(options);
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

            Process worker = new ProcessBuilder("node", DRIVER.toString(), "chromium", String.valueOf(pagePort),
                    scheme, "127.0.0.1", String.valueOf(port), FOUNDING, Job.class.getName() + "#v1",
                    Done.class.getName() + "#v1", "60").directory(NODE_DIR.toFile()).redirectErrorStream(true).start();
            StringBuilder output = new StringBuilder();
            Thread pump = Thread.ofVirtual().start(() -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(worker.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        output.append(line).append('\n');
                        System.out.println("  [browser " + scheme + "] " + line);
                    }
                } catch (java.io.IOException ignored) {
                    // the driver ended
                }
            });
            try {
                Optional<Done> done = Optional.empty();
                long deadline = System.nanoTime() + Duration.ofSeconds(100).toNanos();
                while (done.isEmpty() && System.nanoTime() < deadline) {
                    done = space.read(Template.of(Done.class));
                    Thread.sleep(200);
                }
                assertThat(done).as(scheme + ": the browser's Done arrived\n" + output).isPresent();
                assertThat(done.get().by()).isEqualTo("typescript-" + scheme);
                assertThat(worker.waitFor(60, TimeUnit.SECONDS)).as("driver exited").isTrue();
                pump.join(5_000);
                assertThat(worker.exitValue()).as(output.toString()).isZero();
                assertThat(output.toString()).contains("\"ok\":true").contains("\"pageErrors\":[]");
                assertThat(node.sessionFramesSent()).as("the Java side tagged frames to the browser").isPositive();
                assertThat(transport.counters().refused()).doesNotContainKey("origin");
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
