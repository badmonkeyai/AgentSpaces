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
import ai.badmonkey.agentspaces.planner.StoreChooser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The content store's card under a burst of pins (srad/agentspaces-event-loop.md §10.14, the phase 2 wire adversary).
 * Owner answer 6 republishes the card when what it says changes; every pin moves {@code free}, so a burst of pins that
 * land on separate runs republished a signed card on each, where the store before phase 2 published one per holding
 * interval. The change-driven republish is now paced at one per holding interval: the card still follows what the store
 * holds within one interval, and a burst costs no more cards than before.
 */
class StoreCardRateTest {

    private final StoreFleet fleet = new StoreFleet();

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    private Optional<AssetCard> card(StoreFleet.Peer peer) {
        java.time.Instant now = fleet.clock.instant();
        return peer.discovery().find(AssetCard.class, c -> StoreChooser.KIND.equals(c.access().get("kind")))
                .stream().filter(c -> c.expiresAt().isAfter(now)).max(Comparator.comparing(AssetCard::issued));
    }

    @Test
    void aBurstOfPinsRepublishesTheCardAtMostOncePerHoldingInterval() throws Exception {
        StoreFleet.Peer store = fleet.newPeer("burst-store", 41, StoreFleet.Spec.store(StorePolicy.of(1L << 30)));
        StoreFleet.Peer writer = fleet.newPeer("writer", 42, StoreFleet.Spec.member(), "burst-store");
        fleet.settle(6);
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> card(writer).isPresent())).isTrue();
        List<String> roots = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            String root = StoreFleet.facts(writer, "burst-" + i, 20, 100 + i).root();
            writer.content().holdTree(root);
            roots.add(root);
        }
        fleet.settle(4);
        // one card at start; let the half-TTL refresh stay out of the window
        long cardsBefore = store.contentStore().cardsPublished();
        long start = fleet.clock.millis();
        for (String root : roots) {
            fleet.writePin(writer, new StorePin(StorePin.VERSION, root, StorePin.KIND_TREE, 3600, 1, writer.id(),
                    List.of(), null), Duration.ofMinutes(10));
        }
        // the store's take loop runs on its own thread and pauses between scans on the fleet's time, so it keeps pace
        // with the ticks and the pins land on separate runs within a few holding intervals, as a burst does
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (!roots.stream().allMatch(r -> store.contentStore().ledger().holds(r))) {
            assertThat(System.nanoTime()).as("all pinned: %s", store.contentStore().status()).isLessThan(deadline);
            fleet.tickAll();
        }
        long elapsed = fleet.clock.millis() - start;
        long cards = store.contentStore().cardsPublished() - cardsBefore;
        long interval = StoreFleet.HOLDING.toMillis();
        System.out.printf("[store-card-rate] 8 pins over %d ms: %d cards (one per %d ms interval allows %d)%n",
                elapsed, cards, interval, 1 + elapsed / interval);
        assertThat(cards).as("cards over %d ms of pins", elapsed).isLessThanOrEqualTo(1 + elapsed / interval);
        // and the card still follows what the store holds, within one holding interval
        long free = store.contentStore().status().free();
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> card(writer)
                .map(c -> Long.parseLong(c.access().get("free")) == free).orElse(false)))
                .as("the card's free follows the store's holdings").isTrue();
    }
}
