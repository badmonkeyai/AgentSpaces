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
package ai.badmonkey.agentspaces.planner;

import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The planner's choice of content stores by their cards (PDT 2 phase 3, P2-S-11; srad/agentspaces-pdt2.md
 * §3.2.1): only {@code content-store} cards, read from their text values, filtered by room, blindness, tier,
 * and price, ranked cheapest then roomiest, one card per store.
 */
class StoreChooserTest {

    private static final Instant NOW = Instant.parse("2026-11-09T12:00:00Z");
    private static final GroupId GROUP = GroupId.of("zStores");

    private static AssetCard card(String peer, long free, boolean blind, String tier, String price) {
        Map<String, String> access = new LinkedHashMap<>();
        access.put("kind", StoreChooser.KIND);
        access.put("free", Long.toString(free));
        access.put("maxObject", Long.toString(1L << 40));
        access.put("blind", Boolean.toString(blind));
        access.put("tier", tier);
        Map<String, String> cost = price == null ? Map.of() : Map.of(StoreChooser.PRICE_HINT, price);
        return new AssetCard("aspace://zStores/asset/" + peer, PeerId.of(peer), GROUP, NOW.minusSeconds(10), Duration.ofMinutes(15),
                "content-store:" + peer, "aspace://zStores/store/" + peer, "a store", "store", "PT10S", cost, access);
    }

    @Test
    void theCheapestRoomyStoresWinOnePerIssuer() {
        List<AssetCard> cards = List.of(
                card("zA", 10_000, false, "hot", "3"),
                card("zB", 50_000, false, "hot", null),
                card("zC", 90_000, false, "hot", null),
                card("zD", 500, false, "hot", "0"),
                card("zC", 90_000, false, "hot", null));
        List<AssetCard> chosen = StoreChooser.choose(cards, StoreChooser.Need.of(1_000, 3), NOW);
        assertThat(chosen).extracting(c -> c.issuer().value()).as("zD has no room; free beats priced; roomier first")
                .containsExactly("zC", "zB", "zA");
    }

    @Test
    void blindnessTierPriceAndAvoidanceFilter() {
        List<AssetCard> cards = List.of(
                card("zBlind", 90_000, true, "hot", null),
                card("zCold", 90_000, false, "cold", null),
                card("zDear", 90_000, false, "hot", "9"),
                card("zPlain", 10_000, false, "hot", "1"));
        StoreChooser.Need need = StoreChooser.Need.of(1_000, 4);
        assertThat(StoreChooser.choose(cards, need, NOW)).extracting(c -> c.issuer().value())
                .as("a blind store holds only sealed content").doesNotContain("zBlind");
        assertThat(StoreChooser.choose(cards, need.sealed(true), NOW)).extracting(c -> c.issuer().value()).contains("zBlind");
        assertThat(StoreChooser.choose(cards, need.tier("cold"), NOW)).extracting(c -> c.issuer().value()).containsExactly("zCold");
        assertThat(StoreChooser.choose(cards, need.maxPrice(2), NOW)).extracting(c -> c.issuer().value()).doesNotContain("zDear");
        assertThat(StoreChooser.choose(cards, need.avoid(Set.of(PeerId.of("zCold"))), NOW)).extracting(c -> c.issuer().value())
                .doesNotContain("zCold");
    }

    @Test
    void otherCardsExpiredCardsAndUnreadableValuesAreSkipped() {
        AssetCard table = new AssetCard("aspace://zStores/asset/t", PeerId.of("zT"), GROUP, NOW, Duration.ofMinutes(15),
                "table:t", "aspace://zStores/t", "a table", "rows", "PT1M", Map.of(), Map.of("kind", "table"));
        AssetCard expired = new AssetCard("aspace://zStores/asset/x", PeerId.of("zX"), GROUP, NOW.minusSeconds(3600),
                Duration.ofMinutes(15), "content-store:x", "aspace://zStores/store/x", "a store", "store", "PT10S", Map.of(),
                card("zX", 90_000, false, "hot", null).access());
        Map<String, String> garbled = new LinkedHashMap<>(card("zG", 90_000, false, "hot", null).access());
        garbled.put("free", "lots");
        AssetCard unreadable = new AssetCard("aspace://zStores/asset/g", PeerId.of("zG"), GROUP, NOW, Duration.ofMinutes(15),
                "content-store:g", "aspace://zStores/store/g", "a store", "store", "PT10S", Map.of(), garbled);
        assertThat(StoreChooser.choose(List.of(table, expired, unreadable), StoreChooser.Need.of(1, 3), NOW)).isEmpty();
    }
}
