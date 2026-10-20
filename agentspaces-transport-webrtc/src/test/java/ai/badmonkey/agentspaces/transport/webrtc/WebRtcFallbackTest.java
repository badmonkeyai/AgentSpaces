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
 * SRAD srad/agentspaces-js.md JS-Q-REL-08 and the backoff of JS-W-11: the engine never completes
 * ICE, so the offload's dial ends with {@code END(2)} at the dial timeout, the frames sent
 * meanwhile all arrive through the relay, the node keeps the relay path, and the next attempt
 * waits the retry interval however heavy the traffic stays.
 */
@Timeout(60)
class WebRtcFallbackTest {

    private RelayedFleet fleet;

    @AfterEach
    void tearDown() {
        if (fleet != null) {
            fleet.close();
        }
    }

    @Test
    void aDialThatCannotCompleteFallsBackToTheRelayLosesNoFrameAndBacksOff() throws Exception {
        // The requirement's bound is the default dial timeout; the test shortens it to run fast.
        assertThat(WebRtcOptions.defaults().dialTimeout()).isEqualTo(Duration.ofSeconds(15));
        WebRtcOptions options = WebRtcOptions.defaults().withDialTimeout(Duration.ofSeconds(1));
        fleet = new RelayedFleet(options, new PeerNode.RelayOffload(30, Duration.ofMinutes(5)));
        fleet.engine.neverOpen = true;
        fleet.converge();

        fleet.sendApp(fleet.atAlice, fleet.bobId.peerId(), "before ", 31);
        fleet.await("the relayed frames to reach Bob", () -> fleet.atBobSaw.size() >= 31);
        long started = System.nanoTime();
        fleet.await("the offload to start a dial", () -> fleet.aliceRtc.counters().sessionsOffered() == 1);
        // Frames sent while the dial is pending take the relay: the dial runs on its own thread and
        // a send never waits for a signalled endpoint.
        fleet.sendApp(fleet.atAlice, fleet.bobId.peerId(), "during ", 20);
        fleet.await("the frames sent during the dial", () -> fleet.atBobSaw.size() >= 51);
        fleet.await("the dial to time out", () ->
                fleet.alice.relayOffloadStats().get(fleet.groupId).failed() == 1);
        long elapsed = Duration.ofNanos(System.nanoTime() - started).toSeconds();
        assertThat(elapsed).as("fell back within the dial bound (15 s by default; 1 s here)").isLessThan(15);
        assertThat(fleet.aliceRtc.counters().failed()).containsEntry("timeout", 1L);
        assertThat(fleet.bobRtc.counters().failed()).as("Bob received END(2)").containsEntry("timeout", 1L);
        assertThat(fleet.alice.connectionSchemes()).doesNotContainKey(fleet.bobId.peerId());

        // No frame was lost: everything before, during, and after the failed dial arrived by relay.
        fleet.sendApp(fleet.atAlice, fleet.bobId.peerId(), "after ", 20);
        fleet.await("the frames sent after the dial", () -> fleet.atBobSaw.size() >= 71);
        assertThat(fleet.atBobSaw).hasSize(71).doesNotHaveDuplicates();
        assertThat(fleet.relayedTo(fleet.bobId.peerId())).isGreaterThanOrEqualTo(71);

        // The backoff holds: a minute of heavy traffic and ticks earns no second dial ...
        fleet.tickAll(60);
        Thread.sleep(100);
        assertThat(fleet.alice.relayOffloadStats().get(fleet.groupId)).isEqualTo(new PeerNode.OffloadStats(1, 0, 1));
        assertThat(fleet.aliceRtc.counters().sessionsOffered()).isEqualTo(1);
        // ... and once the retry interval has passed, traffic still above the threshold earns one.
        fleet.clock.advance(Duration.ofMinutes(4));
        fleet.sendApp(fleet.atAlice, fleet.bobId.peerId(), "later ", 31);
        fleet.await("the retry", () -> fleet.aliceRtc.counters().sessionsOffered() == 2);
        fleet.await("the retry to time out", () ->
                fleet.alice.relayOffloadStats().get(fleet.groupId).failed() == 2);
        assertThat(fleet.alice.relayOffloadStats().get(fleet.groupId)).isEqualTo(new PeerNode.OffloadStats(2, 0, 2));
        assertThat(fleet.atBobSaw).hasSize(102);
    }
}
