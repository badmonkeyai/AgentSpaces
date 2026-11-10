/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.badmonkey.agentspaces.discovery;

import ai.badmonkey.agentspaces.api.ad.Advertisement;
import ai.badmonkey.agentspaces.api.ad.AppCard;
import ai.badmonkey.agentspaces.api.ad.CardSpill;
import ai.badmonkey.agentspaces.common.codec.CborCodec;

import java.time.Instant;
import java.time.InstantSource;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The publisher's half of the card spill (SPEC §6.1a.5, v0.1.32; ISSUE-AppCardOriginFormats FR-12 and FR-13;
 * AppCard origin and formats, 2026-11-09). {@link #prepare} measures the card a publisher would sign, after the
 * AppCard level has removed what it removes, and returns it, or its stub once the parts document is on the content
 * swarm:
 *
 * <ol>
 *   <li>a card above {@link CardSpill#WARN_THRESHOLD_BYTES} logs a warning, once per id and size;</li>
 *   <li>a card of at most {@link CardSpill#SPILL_THRESHOLD_BYTES} travels inline;</li>
 *   <li>a larger card travels inline when the gate is closed, no {@link SpillStore} serves the group, the store
 *       refuses the document, or no field of the spill set holds anything, counted in
 *       {@link #cardsOversizeInline()};</li>
 *   <li>otherwise its parts document goes to the store and the stub names it, counted in {@link #cardsSpilled()}.</li>
 * </ol>
 *
 * <p>The spiller holds each root it named until one {@code ttl} after the last card that named it, and then releases
 * it; {@link #sweep()} does the releasing, and the discovery service calls it on every tick.
 */
public final class CardSpiller {

    private static final System.Logger LOG = System.getLogger(CardSpiller.class.getName());

    private final CborCodec codec;
    private final InstantSource clock;
    private volatile SpillStore store;
    /** Each root this spiller named, to the moment the last card that named it expires. */
    private final Map<String, Instant> holds = new ConcurrentHashMap<>();
    /** The size each card id last warned at, so a refresh at the same size stays quiet. */
    private final Map<String, Integer> warned = new ConcurrentHashMap<>();
    private final AtomicLong cardsSpilled = new AtomicLong();
    private final AtomicLong cardsOversizeInline = new AtomicLong();

    /**
     * Creates a spiller with no store: every card travels inline until {@link #store(SpillStore)} installs one.
     *
     * @param codec the codec cards are signed in
     * @param clock the time source of the holds
     */
    public CardSpiller(CborCodec codec, InstantSource clock) {
        this.codec = Objects.requireNonNull(codec, "codec");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Installs the content swarm that takes parts documents; null removes it.
     *
     * @param value the store
     */
    public void store(SpillStore value) {
        this.store = value;
    }

    /** The installed store, or null. */
    public SpillStore store() {
        return store;
    }

    /**
     * Whether a card's encoding passes the spill threshold, so the card's form follows the gate.
     *
     * @param card a card
     * @return true when it is a spilling kind above {@link CardSpill#SPILL_THRESHOLD_BYTES}
     */
    public boolean oversized(Advertisement card) {
        return Spills.spillable(card) && codec.toBytes(card).length > CardSpill.SPILL_THRESHOLD_BYTES;
    }

    /**
     * The card a publisher signs (FR-12, FR-13): the card itself, or its stub once the parts document is published.
     *
     * @param card     a full card, after the AppCard level has removed what it removes
     * @param gateOpen whether every member of the view is at the AppCard level that reads {@code spill}
     * @param <C>      the card kind
     * @return the card to sign
     */
    public <C extends Advertisement> CompletableFuture<C> prepare(C card, boolean gateOpen) {
        Objects.requireNonNull(card, "card");
        if (!Spills.spillable(card) || Spills.spillOf(card) != null) {
            return CompletableFuture.completedFuture(card);
        }
        int size = codec.toBytes(card).length;
        SpillStore installed = store;
        java.util.List<String> parts = size > CardSpill.SPILL_THRESHOLD_BYTES ? Spills.nonEmptyOf(card) : java.util.List.of();
        // a card whose size sits outside the spill set (a long name or description) has nothing to move
        boolean spills = !parts.isEmpty() && gateOpen && installed != null;
        warn(card, size, spills);
        if (size <= CardSpill.SPILL_THRESHOLD_BYTES) {
            return CompletableFuture.completedFuture(card);
        }
        if (!spills) {
            cardsOversizeInline.incrementAndGet();
            return CompletableFuture.completedFuture(card);
        }
        byte[] document = codec.toBytes(Spills.partsOf(card));
        return installed.put(card.id(), document).handle((root, failure) -> {
            if (failure != null) {
                // the swarm did not take the document: the card still travels, inline in full
                LOG.log(System.Logger.Level.WARNING, "the content swarm refused the parts document of " + card.id()
                        + "; the card travels inline", failure);
                cardsOversizeInline.incrementAndGet();
                return card;
            }
            holds.merge(root, card.expiresAt(), (held, now) -> now.isAfter(held) ? now : held);
            cardsSpilled.incrementAndGet();
            return Spills.stub(card, new CardSpill(root, document.length, parts));
        });
    }

    /** Releases every root whose last naming card has expired. */
    public void sweep() {
        SpillStore installed = store;
        Instant now = clock.instant();
        for (Map.Entry<String, Instant> hold : holds.entrySet()) {
            if (!now.isBefore(hold.getValue()) && holds.remove(hold.getKey(), hold.getValue()) && installed != null) {
                installed.release(hold.getKey());
            }
        }
    }

    /** The roots this spiller holds now. */
    public java.util.Set<String> heldRoots() {
        return java.util.Set.copyOf(holds.keySet());
    }

    /** Cards issued as stubs. */
    public long cardsSpilled() {
        return cardsSpilled.get();
    }

    /** Cards above the spill threshold issued inline: a closed gate, no store, a refused document, or nothing to move. */
    public long cardsOversizeInline() {
        return cardsOversizeInline.get();
    }

    private void warn(Advertisement card, int size, boolean spills) {
        if (size <= CardSpill.WARN_THRESHOLD_BYTES || Integer.valueOf(size).equals(warned.put(card.id(), size))) {
            return;
        }
        String kind = card instanceof AppCard app ? "AppCard (kind " + app.kind() + ")" : card.getClass().getSimpleName();
        String fate = size <= CardSpill.SPILL_THRESHOLD_BYTES ? "it travels inline until it passes the threshold"
                : spills ? "it spills its large parts to the content swarm"
                : "it travels inline, since the view is below the AppCard level, no content swarm serves the group,"
                        + " or no field of its spill set holds anything";
        LOG.log(System.Logger.Level.WARNING, "{0} {1} encodes to {2} bytes, past the {3}-byte warning; the spill threshold"
                + " is {4} bytes, and {5}", kind, card.id(), size, CardSpill.WARN_THRESHOLD_BYTES, CardSpill.SPILL_THRESHOLD_BYTES, fate);
    }
}
