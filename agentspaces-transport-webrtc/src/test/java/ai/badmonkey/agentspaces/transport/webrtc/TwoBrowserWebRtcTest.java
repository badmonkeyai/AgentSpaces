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

package ai.badmonkey.agentspaces.transport.webrtc;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.transport.http.HttpOptions;
import ai.badmonkey.agentspaces.transport.http.OriginPolicy;
import ai.badmonkey.agentspaces.transport.http.WebSocketTransport;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * JS-W-12: two headless Chromium pages that share a RELAY-role seed open a direct {@code webrtc}
 * connection and exchange frames. The seed is a Java {@link PeerNode} with the RELAY role over the
 * {@code ws} listener of agentspaces-transport-http (the faithful choice: a Node service host
 * forwards rumors but not a {@code PIPE_DATA} addressed to another member, so it cannot relay the
 * signals, while a Java RELAY node forwards the {@code RELAY_FRAME} the browsers wrap their signals
 * in). A {@code node} driver (packages/webrtc/test/two-browser-driver.js) bundles the page with
 * esbuild, launches the two pages, and reports their results.
 *
 * <p>Opt-in ({@code -Dwebrtc.browser=true}): it needs the TypeScript build, Playwright, and a
 * Chromium, so it runs in the browser CI lane, not on every unit run — the same gate
 * {@code BrowserFleetTest} uses.
 */
class TwoBrowserWebRtcTest {

    private static final Path ROOT = Path.of("").toAbsolutePath().getParent().getParent();
    private static final Path NODE_DIR = ROOT.resolve("agentspaces-typescript");
    private static final Path DRIVER = NODE_DIR.resolve("packages/webrtc/build/test/two-browser-driver.js");
    private static final String FOUNDING = "two-browser-webrtc-v1";

    private static void require(boolean condition, String why) {
        if (Boolean.getBoolean("webrtc.browser")) {
            assertThat(condition).as(why).isTrue();
        }
        Assumptions.assumeTrue(condition, why);
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    @Timeout(240)
    void twoBrowsersOpenAWebRtcConnectionThroughAJavaRelaySeed() throws Exception {
        require(Boolean.getBoolean("webrtc.browser"), "set -Dwebrtc.browser=true to run");
        require(Files.exists(DRIVER), "the webrtc package is built (npm run build) at " + DRIVER);
        require(Files.isDirectory(NODE_DIR.resolve("node_modules/playwright")), "playwright installed in " + NODE_DIR);

        PeerIdentity identity = PeerIdentity.generate();
        // The seed forwards the browsers' RELAY_FRAMEs: the RELAY role, over a plaintext ws listener
        // a browser can dial. Session attestation carries the browsers' in-band identity (JS-H-05).
        PeerNode seed = PeerNode.builder(identity).channelAuth(PeerNode.ChannelAuth.SESSION)
                .roles(Set.of(PeerAdvertisement.PeerRole.RELAY)).build();
        int wsPort = freePort();
        int pagePort = freePort();
        HttpOptions options = HttpOptions.defaults()
                .withOrigins(OriginPolicy.of(List.of("http://127.0.0.1:" + pagePort)));
        GroupId groupId = GroupId.fromFounding(FOUNDING.getBytes(StandardCharsets.UTF_8));
        try (WebSocketTransport transport = new WebSocketTransport().options(options)) {
            seed.listen(transport, "127.0.0.1:" + wsPort);
            seed.joinGroup(new GroupAdvertisement("aspace://" + groupId.value(),
                    PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(365), FOUNDING,
                    GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                    GroupAdvertisement.GossipParameters.defaults()),
                    new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2), List.of());
            seed.startTicking(Duration.ofMillis(250));

            // The driver serves the page on the port the seed's origin allowlist names.
            Process driver = new ProcessBuilder("node", DRIVER.toString(), String.valueOf(wsPort), FOUNDING, "90",
                    String.valueOf(pagePort))
                    .directory(NODE_DIR.toFile()).redirectErrorStream(true).start();
            StringBuilder output = new StringBuilder();
            Thread pump = Thread.ofVirtual().start(() -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(driver.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        output.append(line).append('\n');
                        System.out.println("  [browsers] " + line);
                    }
                } catch (java.io.IOException ignored) {
                    // the driver ended
                }
            });
            try {
                assertThat(driver.waitFor(180, TimeUnit.SECONDS)).as("the driver exited\n" + output).isTrue();
                pump.join(5_000);
                assertThat(driver.exitValue()).as(output.toString()).isZero();
                assertThat(output.toString()).contains("\"ok\":true");
            } finally {
                driver.destroy();
                driver.waitFor(5, TimeUnit.SECONDS);
                driver.destroyForcibly();
                seed.close();
            }
        }
    }
}
