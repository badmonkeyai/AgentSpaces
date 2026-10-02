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
package ai.badmonkey.agentspaces.examples.learning;

import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.capabilities.learn.GossipLearner;
import ai.badmonkey.agentspaces.examples.learning.GossipLearningFleet.Peer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/** Example 14 over TCP: three local models converge on the fleet mean with no server, on the peer tick alone. */
class GossipLearningFleetFlowTest {

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    @Timeout(90)
    void everyPeerConvergesOnTheFleetMeanWithoutAServer() throws Exception {
        int seedPort = freePort();
        Peer north = GossipLearningFleet.startPeer("north", seedPort, 0);
        Peer south = GossipLearningFleet.startPeer("south", freePort(), seedPort);
        Peer east = GossipLearningFleet.startPeer("east", freePort(), seedPort);
        List<Peer> fleet = List.of(north, south, east);
        try {
            Thread.sleep(1500);
            GossipLearningFleet.train(north, 1.0, 2.0, 3.0);
            GossipLearningFleet.train(south, 3.0, 2.0, 1.0);
            GossipLearningFleet.train(east, 2.0, 5.0, 2.0);
            double[] expected = GossipLearningFleet.mean(List.of(
                    new double[] {1, 2, 3}, new double[] {3, 2, 1}, new double[] {2, 5, 2}));

            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (System.nanoTime() < deadline && !converged(fleet, expected)) {
                Thread.sleep(200);
            }
            assertThat(converged(fleet, expected)).as("every peer holds the fleet mean").isTrue();
            assertThat(fleet).allSatisfy(p -> assertThat(p.rounds()).isGreaterThan(0));
            // The learner is a capability like any other: advertised, discoverable, on the peer tick.
            assertThat(north.capabilities().providersOf(GossipLearner.TYPE))
                    .extracting(CapabilityAdvertisement::issuer)
                    .contains(north.node().peerId());
        } finally {
            fleet.forEach(Peer::close);
        }
    }

    private static boolean converged(List<Peer> fleet, double[] expected) {
        for (Peer peer : fleet) {
            double[] model = peer.model().orElse(null);
            if (model == null) {
                return false;
            }
            for (int i = 0; i < expected.length; i++) {
                if (Math.abs(model[i] - expected[i]) > 0.05) {
                    return false;
                }
            }
        }
        return true;
    }
}
