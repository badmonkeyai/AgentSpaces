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
package ai.badmonkey.agentspaces.pdt.refs;

import ai.badmonkey.agentspaces.pdt.values.PMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SESSION test fleets (ROBUSTNESS.md §10.10): the ref fleet runs every node in {@code ChannelAuth.SESSION}, so each
 * connection costs one signed SESSION_HELLO each way and every frame after it carries a MAC (SPEC §5.6). The fabric
 * attests nobody, so no frame travels bare. Production defaults stay {@code SIGNED}, and the per-frame signing path
 * keeps its own suites, among them the SIGNED gossip fleet of {@code LazyPushClusterTest}.
 */
class RefFleetSessionTest {

    private final RefFleet fleet = new RefFleet();

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    @Test
    void theFleetsConnectionsRunSessionsAndAWriteCostsNoFrameSignature() throws Exception {
        RefFleet.Peer writer = fleet.newPeer("w", 1);
        fleet.newPeer("r0", 2, "w");
        fleet.newPeer("r1", 3, "w");
        fleet.settle(12);
        for (RefFleet.Peer p : fleet.peers) {
            p.pdt().mapRef("ledger", RefFleet.STR, RefFleet.STR); // every peer follows the ref
        }
        for (RefFleet.Peer p : fleet.peers) {
            assertThat(p.node().channelModes()).as("%s's connections", p.name()).hasSize(2)
                    .allSatisfy((peer, mode) -> assertThat(mode).isEqualTo("session"));
            assertThat(p.node().bareFramesSent()).as("the fabric attests nobody").isZero();
        }
        long[] signedBefore = fleet.peers.stream().mapToLong(p -> p.node().signedFramesSent()).toArray();
        long[] sessionBefore = fleet.peers.stream().mapToLong(p -> p.node().sessionFramesSent()).toArray();

        PMap<String, String> value = writer.pdt().mapRef("ledger", RefFleet.STR, RefFleet.STR)
                .swap(m -> m.assoc("k", "v"));
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> fleet.allHold(value.root()))).isTrue();
        for (int i = 0; i < fleet.peers.size(); i++) {
            RefFleet.Peer p = fleet.peers.get(i);
            assertThat(p.node().signedFramesSent() - signedBefore[i]).as("%s signed no frame for the write", p.name())
                    .isZero();
            assertThat(p.node().sessionFramesSent() - sessionBefore[i]).as("%s sent MAC frames", p.name()).isPositive();
        }
    }
}
