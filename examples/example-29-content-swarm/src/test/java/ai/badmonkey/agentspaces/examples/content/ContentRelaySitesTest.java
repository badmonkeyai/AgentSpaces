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
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.Fetch;
import ai.badmonkey.agentspaces.content.PublishOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Scenario 3 of srad/large-networks-bulk-data.md over real sockets (LB-Q-TST-02): two dial-only kits,
 * each behind its own site relay, with both site relays attached to a headquarters relay. Loopback
 * cannot hide a listener, so the sites are kept apart by binding: site A speaks {@code ws}, site B
 * speaks {@code tls}, and the site relays reach headquarters on {@code tcp}. A kit holds only its
 * site's binding, so it can dial its own relay and nothing else, and a site relay cannot dial the
 * other site's relay. Every piece crosses relay A, headquarters, and relay B on a pinned path, and
 * the swarm never names a binding.
 */
class ContentRelaySitesTest {

    private static final Set<PeerAdvertisement.PeerRole> RELAY = Set.of(PeerAdvertisement.PeerRole.RELAY);

    private static int freeTcp() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    @Timeout(120)
    void aDialOnlyKitFetchesFromAnotherSitesDialOnlyKitAcrossTheRelays(@TempDir Path dir) throws Exception {
        int hqPort = freeTcp();
        int raPort = freeTcp();
        int rbPort = freeTcp();
        try (ContentSwarmFleet.Peer hq = ContentSwarmFleet.start("hq", Map.of("tcp", hqPort), RELAY, List.of(), null,
                     dir.resolve("hq"));
             ContentSwarmFleet.Peer ra = ContentSwarmFleet.start("relay-a", Map.of("ws", raPort), RELAY,
                     List.of("tcp://127.0.0.1:" + hqPort), null, dir.resolve("ra"));
             ContentSwarmFleet.Peer rb = ContentSwarmFleet.start("relay-b", Map.of("tls", rbPort), RELAY,
                     List.of("tcp://127.0.0.1:" + hqPort), null, dir.resolve("rb"));
             ContentSwarmFleet.Peer a1 = ContentSwarmFleet.start("kit-a1", Map.of(), Set.of(),
                     List.of("ws://127.0.0.1:" + raPort), null, dir.resolve("a1"));
             ContentSwarmFleet.Peer b1 = ContentSwarmFleet.start("kit-b1", Map.of(), Set.of(),
                     List.of("tls://127.0.0.1:" + rbPort), null, dir.resolve("b1"))) {
            int hops = -1;
            for (int i = 0; i < 300 && hops != 3; i++) {
                hops = a1.runtime().hopsTo(b1.node().peerId());
                Thread.sleep(100);
            }
            assertThat(hops).as("kit-a1 reaches kit-b1 through relay A, headquarters, and relay B").isEqualTo(3);

            byte[] tiles = ContentSwarmFleet.artifact(6 * 1024 * 1024 + 17, 3);
            Manifest manifest = b1.content().publish(tiles,
                    PublishOptions.defaults().withPieceSize(256 * 1024).withName("tiles.pmtiles"));
            Fetch fetch = a1.content().fetch(manifest.root());
            assertThat(fetch.await(Duration.ofSeconds(90))).as(fetch.trace().toString()).isTrue();
            try (SeekableByteChannel channel = fetch.open()) {
                ByteBuffer buffer = ByteBuffer.allocate((int) channel.size());
                while (buffer.hasRemaining() && channel.read(buffer) >= 0) {
                    // drain
                }
                assertThat(buffer.array()).isEqualTo(tiles);
            }
            assertThat(fetch.progress().bytesBySource()).containsKey(b1.node().peerId());

            String kind = "PIPE_DATA:" + Content.TYPE;
            StringBuilder carried = new StringBuilder();
            for (ContentSwarmFleet.Peer relay : List.of(ra, hq, rb)) {
                Map<String, Long> byKind = relay.node().relayBytesByKind(relay.runtime().id());
                carried.append(String.format("%s=%d ", relay.name(), byKind.getOrDefault(kind, 0L)));
            }
            System.out.printf("LB-Q-TST-02 scenario 3 over ws, tcp, and tls: %d bytes at %d relay hops; content bytes relayed %s%n",
                    tiles.length, a1.runtime().hopsTo(b1.node().peerId()), carried.toString().trim());
            assertThat(a1.runtime().hopsTo(b1.node().peerId())).as("the path stayed pinned").isEqualTo(3);
            for (ContentSwarmFleet.Peer relay : List.of(ra, hq, rb)) {
                assertThat(relay.node().relayBytesByKind(relay.runtime().id()).getOrDefault(kind, 0L))
                        .as(relay.name() + " carried every piece").isGreaterThanOrEqualTo(tiles.length);
            }
        }
    }
}
