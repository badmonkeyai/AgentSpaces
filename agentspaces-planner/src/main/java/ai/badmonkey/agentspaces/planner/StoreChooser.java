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
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Chooses content stores by their asset cards (PDT 2 phase 3, P2-S-11; srad/agentspaces-pdt2.md §3.2.1):
 * a planner finds the {@code content-store} cards through discovery and asks this chooser which stores
 * should hold a root. The chooser is pure over the cards, as every planner here is. It reads the card's
 * text values (rule 19 of §6.10.3): {@code free} and {@code maxObject} in canonical decimal, {@code blind}
 * as {@code true} or {@code false}, {@code tier} as {@code hot} or {@code cold}, and the
 * {@link #PRICE_HINT} cost hint per GiB-month. A card with a value it cannot read is skipped.
 *
 * <p>The rules: only an unexpired card of kind {@link #KIND} with room for the root; a blind store only
 * for sealed content, since a blind store refuses what it could read (§3.2.5); the tier and the highest
 * price when the need names them; no store the need avoids. The rest rank by price (no hint is free),
 * then by free bytes, most first, then by issuer, and the chooser keeps one card per store, at most the
 * copies the need asks for.
 */
public final class StoreChooser {

    /** The {@code access.kind} of a content store's card. */
    public static final String KIND = "content-store";

    /** The {@code costHints} key a store's owner sets to a price per GiB-month. */
    public static final String PRICE_HINT = "pricePerGibMonth";

    private StoreChooser() {
    }

    /**
     * What a root needs from its stores.
     *
     * @param bytes    the bytes the root holds
     * @param copies   how many stores should hold it
     * @param sealed   whether the content is sealed (sealed nodes or sealed hashing), so a blind store may hold it
     * @param tier     the tier wanted, or null for any
     * @param maxPrice the highest price per GiB-month, or null for any
     * @param avoid    stores not to choose, such as ones that failed challenges
     */
    public record Need(long bytes, int copies, boolean sealed, String tier, Long maxPrice, Set<PeerId> avoid) {

        public Need {
            if (bytes < 0) {
                throw new IllegalArgumentException("bytes must not be negative");
            }
            if (copies < 1) {
                throw new IllegalArgumentException("copies must be at least 1");
            }
            avoid = avoid == null ? Set.of() : Set.copyOf(avoid);
        }

        /** A need of plain content in any tier at any price. */
        public static Need of(long bytes, int copies) {
            return new Need(bytes, copies, false, null, null, Set.of());
        }

        public Need sealed(boolean value) {
            return new Need(bytes, copies, value, tier, maxPrice, avoid);
        }

        public Need tier(String value) {
            return new Need(bytes, copies, sealed, value, maxPrice, avoid);
        }

        public Need maxPrice(long value) {
            return new Need(bytes, copies, sealed, tier, value, avoid);
        }

        public Need avoid(Set<PeerId> value) {
            return new Need(bytes, copies, sealed, tier, maxPrice, value);
        }
    }

    /** One card as read. */
    private record Read(AssetCard card, long free, long price) {
    }

    /**
     * The stores to ask, best first.
     *
     * @param cards the asset cards discovery found
     * @param need  what the root needs
     * @param now   the instant expiry is judged by
     * @return at most {@code need.copies()} cards, one per store
     */
    public static List<AssetCard> choose(List<AssetCard> cards, Need need, Instant now) {
        Objects.requireNonNull(cards, "cards");
        Objects.requireNonNull(need, "need");
        Objects.requireNonNull(now, "now");
        List<Read> fit = new ArrayList<>();
        for (AssetCard card : cards) {
            read(card, need, now).ifPresent(fit::add);
        }
        fit.sort(Comparator.comparingLong(Read::price).thenComparing(Comparator.comparingLong(Read::free).reversed())
                .thenComparing(r -> r.card().issuer().value()));
        Map<PeerId, AssetCard> chosen = new LinkedHashMap<>();
        for (Read r : fit) {
            if (chosen.size() == need.copies()) {
                break;
            }
            chosen.putIfAbsent(r.card().issuer(), r.card());
        }
        return List.copyOf(chosen.values());
    }

    private static Optional<Read> read(AssetCard card, Need need, Instant now) {
        Map<String, String> access = card.access();
        if (!KIND.equals(access.get("kind")) || !card.expiresAt().isAfter(now) || need.avoid().contains(card.issuer())) {
            return Optional.empty();
        }
        Optional<Long> free = decimal(access.get("free"));
        Optional<Long> maxObject = decimal(access.get("maxObject"));
        String blind = access.get("blind");
        String priceText = card.costHints().get(PRICE_HINT);
        Optional<Long> price = priceText == null ? Optional.of(0L) : decimal(priceText);
        if (free.isEmpty() || maxObject.isEmpty() || price.isEmpty() || !("true".equals(blind) || "false".equals(blind))) {
            return Optional.empty();
        }
        if (free.get() < need.bytes() || maxObject.get() < need.bytes() || ("true".equals(blind) && !need.sealed())) {
            return Optional.empty();
        }
        if (need.tier() != null && !need.tier().equals(access.get("tier"))) {
            return Optional.empty();
        }
        if (need.maxPrice() != null && price.get() > need.maxPrice()) {
            return Optional.empty();
        }
        return Optional.of(new Read(card, free.get(), price.get()));
    }

    /** Canonical decimal text: no sign, no leading zero, within a long. */
    private static Optional<Long> decimal(String text) {
        if (text == null || !text.matches("0|[1-9][0-9]{0,18}")) {
            return Optional.empty();
        }
        try {
            return Optional.of(Long.parseLong(text));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }
}
