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
package ai.badmonkey.agentspaces.examples.data;

import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.connect.ConnectorRuntime;
import ai.badmonkey.agentspaces.connect.DataQueryClient;
import ai.badmonkey.agentspaces.connect.TableAssetProvider;
import ai.badmonkey.agentspaces.examples.data.DataFleet.Peer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Discovery by meaning plus pull-once economics over real TCP. */
class DataFleetFlowTest {

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    @Timeout(90)
    void twoAnalystsOneSourceExecution() throws Exception {
        int seedPort = freePort();
        Peer connectorPeer = DataFleet.startPeer("pg-connector", seedPort, 0);
        Peer analystA = DataFleet.startPeer("analyst-a", freePort(), seedPort);
        Peer analystB = DataFleet.startPeer("analyst-b", freePort(), seedPort);
        List<Peer> fleet = List.of(connectorPeer, analystA, analystB);
        TableAssetProvider provider = DataFleet.ordersTable();
        try (ConnectorRuntime connector = new ConnectorRuntime(connectorPeer.data(),
                connectorPeer.discovery(), connectorPeer.node().identity(),
                "pg-connector", provider, connectorPeer.runtime().id(),
                InstantSource.system(), Duration.ofMinutes(1))) {
            connector.start();
            Thread.sleep(1500);

            var matches = analystA.semantic().remoteQuery(
                    "who holds customer order history?", 3, Duration.ofSeconds(5));
            assertThat(matches).isNotEmpty();
            assertThat(matches.get(0).advertisement()).isInstanceOf(AssetCard.class);
            AssetCard card = (AssetCard) matches.get(0).advertisement();
            assertThat(card.asset()).isEqualTo("orders");

            DataQueryClient clientA = new DataQueryClient(analystA.data(), "analyst-a");
            var first = clientA.fetch("orders", Map.of("region", "eu"),
                    Duration.ofSeconds(15)).orElseThrow();
            assertThat(first.fromCache()).isFalse();
            assertThat(first.result().rows()).hasSize(3);

            // The result replicates; the second analyst's identical question is
            // answered without touching the source.
            DataQueryClient clientB = new DataQueryClient(analystB.data(), "analyst-b");
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            DataQueryClient.Fetched second = null;
            while (System.nanoTime() < deadline) {
                var fetched = clientB.fetch("orders", Map.of("region", "eu"),
                        Duration.ofSeconds(5));
                if (fetched.isPresent() && fetched.get().fromCache()) {
                    second = fetched.get();
                    break;
                }
                Thread.sleep(200);
            }
            assertThat(second).isNotNull();
            assertThat(second.result().rows()).hasSize(3);
            assertThat(provider.executions()).isEqualTo(1);
        } finally {
            fleet.forEach(Peer::close);
        }
    }
}
