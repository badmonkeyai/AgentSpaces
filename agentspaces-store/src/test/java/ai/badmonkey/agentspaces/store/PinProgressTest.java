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
package ai.badmonkey.agentspaces.store;

import ai.badmonkey.agentspaces.agent.content.StorePin;
import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.planner.StoreChooser;
import ai.badmonkey.agentspaces.test.SimNetwork;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Swarm retries, B9 (ROBUSTNESS.md §10.3): a store's fetch of a pinned root runs while it makes progress, however
 * long the root takes, and the operator's fetch timeout counts from the last byte received; a pin the store abandons
 * cancels its fetch, so nothing runs on unrecorded.
 */
class PinProgressTest {

    private final StoreFleet fleet = new StoreFleet();

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    private static List<AssetCard> storeCards(StoreFleet.Peer peer) {
        return peer.discovery().find(AssetCard.class, c -> StoreChooser.KIND.equals(c.access().get("kind")));
    }

    private StoreFleet.Peer store(String name, long seed, Duration fetchTimeout) throws Exception {
        return fleet.newPeer(name, seed, StoreFleet.Spec.store(StorePolicy.of(1L << 30))
                .storeBuilder(sb -> sb.fetchTimeout(fetchTimeout).takeLease(Duration.ofDays(1))));
    }

    /** Delivers every frame held from one node to another and keeps holding the link. */
    private void deliverHeld(String from, String to) {
        while (fleet.network.releaseOne(from, to)) {
            // one frame at a time, in order
        }
    }

    private EntryHandle pin(StoreFleet.Peer writer, String root) {
        StorePin pin = new StorePin(StorePin.VERSION, root, StorePin.KIND_TREE, 3600, 1, writer.id(), List.of(), null);
        return fleet.writePin(writer, pin, Duration.ofMinutes(10));
    }

    @Test
    void aPinWhoseFetchKeepsMakingProgressIsHeldThoughItTakesLongerThanTheTimeout() throws Exception {
        StoreFleet.Peer s = store("slow-store", 21, Duration.ofSeconds(2));
        StoreFleet.Peer writer = fleet.newPeer("writer", 22, StoreFleet.Spec.member(), "slow-store");
        fleet.settle(6);
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> storeCards(writer).size() == 1)).isTrue();
        String root = StoreFleet.facts(writer, "slow", 2_000, 5).root();
        writer.content().holdTree(root);
        fleet.settle(4);
        // every answer from the writer to the store runs a second late, so the fetch takes several seconds in all
        fleet.network.hold("writer", "slow-store");
        pin(writer, root);
        long start = fleet.clock.millis();
        while (!s.contentStore().ledger().holds(root) && fleet.clock.millis() - start < 60_000) {
            fleet.settle(4);
            deliverHeld("writer", "slow-store");
            Thread.sleep(20); // the store's take loop runs on its own thread
        }
        long took = fleet.clock.millis() - start;
        assertThat(s.contentStore().ledger().holds(root)).as("held after %d ms: %s", took, s.contentStore().status()).isTrue();
        assertThat(took).as("the fetch took longer than the two-second timeout").isGreaterThan(2_000);
        assertThat(s.contentStore().status().abandoned()).as("progress kept the take alive").isZero();
    }

    @Test
    void anAbandonedPinCancelsItsFetch() throws Exception {
        StoreFleet.Peer s = store("stalled-store", 31, Duration.ofSeconds(1));
        StoreFleet.Peer writer = fleet.newPeer("writer", 32, StoreFleet.Spec.member(), "stalled-store");
        fleet.settle(6);
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> storeCards(writer).size() == 1)).isTrue();
        String root = StoreFleet.facts(writer, "stalled", 500, 6).root();
        writer.content().holdTree(root);
        fleet.settle(4);
        fleet.network.hold("writer", "stalled-store");
        EntryHandle handle = pin(writer, root);
        deliverHeld("writer", "stalled-store"); // the store sees the pin; the answers to its fetch stay held
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> s.contentSpace().currentClaim(handle.entryId()).isPresent()))
                .as("the store took the pin").isTrue();
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> s.contentStore().status().abandoned() > 0))
                .as("no byte for the timeout: abandoned").isTrue();
        fleet.settle(1); // the swarm's tick reaps the cancelled fetch
        assertThat(s.content().stats().treeFetchesActive()).as("the abandoned pin's fetch was cancelled").isZero();
        fleet.network.release("writer", "stalled-store", SimNetwork.Order.FIFO, 1); // the late answers, and all after them
        fleet.settle(8);
        assertThat(s.store().complete(root)).as("nothing more was stored for the abandoned pin").isFalse();
        assertThat(s.contentStore().ledger().holds(root)).isFalse();
    }
}
