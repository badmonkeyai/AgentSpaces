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

import ai.badmonkey.agentspaces.peering.node.PeerNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SRAD srad/agentspaces-js.md JS-W-11 and JS-Q-PRF-03 on the fake engine: two dial-only members
 * behind a RELAY seed exchange frames through it until more than the threshold crossed in a
 * minute; the node then dials the member's {@code webrtc} endpoint from its tick, the channel
 * opens, and from then on the seed carries none of their frames in either direction.
 */
@Timeout(60)
class RelayOffloadTest {

    private RelayedFleet fleet;

    @AfterEach
    void tearDown() {
        if (fleet != null) {
            fleet.close();
        }
    }

    @Test
    void pastTheThresholdTheNodeOpensADirectChannelAndTheRelayCarriesNoMoreOfTheirFrames() throws Exception {
        fleet = new RelayedFleet(WebRtcOptions.defaults(), PeerNode.RelayOffload.defaults());
        fleet.converge();
        assertThat(fleet.alice.relayOffload()).isEqualTo(new PeerNode.RelayOffload(30, Duration.ofMinutes(5)));
        assertThat(fleet.alice.connectionSchemes()).doesNotContainKey(fleet.bobId.peerId());

        // Thirty-one application frames in one burst: every one crosses the seed, since Alice has
        // no connection to Bob and never dials a signalled endpoint from a send.
        fleet.sendApp(fleet.atAlice, fleet.bobId.peerId(), "relayed ", 31);
        fleet.await("the relayed frames to reach Bob", () -> fleet.atBobSaw.size() >= 31);
        assertThat(fleet.relayedTo(fleet.bobId.peerId())).isGreaterThanOrEqualTo(31);
        assertThat(fleet.alice.relayedWithInLastMinute(fleet.groupId, fleet.bobId.peerId())).isGreaterThan(30);

        // The tick dials; the fake engine opens; both nodes hold the channel, attested.
        fleet.await("the offload to open a channel", () ->
                "webrtc".equals(fleet.alice.connectionSchemes().get(fleet.bobId.peerId()))
                        && "webrtc".equals(fleet.bob.connectionSchemes().get(fleet.aliceId.peerId())));
        assertThat(fleet.aliceRtc.counters().sessionsOpened()).isEqualTo(1);
        assertThat(fleet.bobRtc.counters().sessionsAnswered()).isEqualTo(1);
        assertThat(fleet.alice.relayOffloadStats().get(fleet.groupId)).isEqualTo(new PeerNode.OffloadStats(1, 1, 0));
        assertThat(fleet.alice.channelModes()).containsKey(fleet.bobId.peerId());
        assertThat(fleet.bob.channelModes()).containsKey(fleet.aliceId.peerId());

        // JS-Q-PRF-03: the seed's per-target counters for the pair stop rising, both ways.
        long toBobBefore = fleet.relayedTo(fleet.bobId.peerId());
        long toAliceBefore = fleet.relayedTo(fleet.aliceId.peerId());
        long chunksBefore = fleet.aliceRtc.counters().chunksSent();
        fleet.sendApp(fleet.atAlice, fleet.bobId.peerId(), "direct ", 5);
        fleet.sendApp(fleet.atBob, fleet.aliceId.peerId(), "back ", 5);
        fleet.await("ten frames over the channel", () -> fleet.atBobSaw.size() >= 36 && fleet.atAliceSaw.size() >= 5);
        assertThat(fleet.aliceRtc.counters().chunksSent()).isGreaterThanOrEqualTo(chunksBefore + 5);
        assertThat(fleet.bobRtc.counters().chunksSent()).isGreaterThanOrEqualTo(5);
        assertThat(fleet.relayedTo(fleet.bobId.peerId())).as("the relay carried none of Alice's frames").isEqualTo(toBobBefore);
        assertThat(fleet.relayedTo(fleet.aliceId.peerId())).as("nor any of Bob's").isEqualTo(toAliceBefore);
        assertThat(fleet.alice.relayOffloadStats().get(fleet.groupId)).as("one dial, no more").isEqualTo(new PeerNode.OffloadStats(1, 1, 0));
    }

    @Test
    void belowTheThresholdNothingDialsAndTheSettingsAreValidated() throws Exception {
        fleet = new RelayedFleet(WebRtcOptions.defaults(), new PeerNode.RelayOffload(1_000, Duration.ofMinutes(5)));
        fleet.converge();
        fleet.sendApp(fleet.atAlice, fleet.bobId.peerId(), "few ", 31);
        fleet.await("the relayed frames to reach Bob", () -> fleet.atBobSaw.size() >= 31);
        fleet.tickAll(5);
        Thread.sleep(100);
        assertThat(fleet.aliceRtc.counters().sessionsOffered()).isZero();
        assertThat(fleet.alice.relayOffloadStats().getOrDefault(fleet.groupId, new PeerNode.OffloadStats(0, 0, 0)).attempts()).isZero();
        assertThat(fleet.alice.connectionSchemes()).doesNotContainKey(fleet.bobId.peerId());
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new PeerNode.RelayOffload(0, Duration.ofMinutes(5)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("frames-per-minute");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new PeerNode.RelayOffload(30, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("retry-after");
    }
}
