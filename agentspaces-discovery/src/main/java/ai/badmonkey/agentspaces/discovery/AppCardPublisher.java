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

import ai.badmonkey.agentspaces.api.ad.AppCard;
import ai.badmonkey.agentspaces.identity.AdvertisementSigner;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Issues a peer's {@link AppCard}s under the AppCard level and the card spill (SPEC §6.1a.2 and §6.1a.5, v0.1.32;
 * ISSUE-AppCardOriginFormats §9.3; AppCard origin and formats, 2026-11-09). It keeps each full card, signs the card
 * {@code CardSpiller.prepare(AppCards.forView(card, gate), gate)} returns, and reissues every card at once, under a
 * strictly newer {@code issued}, when the view's level changes. It never needs the stripped card or the stub to restore
 * the full one.
 */
public final class AppCardPublisher implements AutoCloseable {

    private final PeerIdentity identity;
    private final GroupRuntime runtime;
    private final DiscoveryService discovery;
    private final AdvertisementSigner signer = new AdvertisementSigner();
    private final Map<String, AppCard> full = new ConcurrentHashMap<>();
    private final Map<String, AppCard> signed = new ConcurrentHashMap<>();
    private final AtomicLong appCardsDowngraded = new AtomicLong();
    private final AutoCloseable gateRegistration;

    /**
     * Creates a publisher on one group.
     *
     * @param identity  the issuing peer
     * @param runtime   the group, whose view the gate reads
     * @param discovery the group's discovery service, which publishes the cards and holds the spiller
     */
    public AppCardPublisher(PeerIdentity identity, GroupRuntime runtime, DiscoveryService discovery) {
        this.identity = Objects.requireNonNull(identity, "identity");
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.discovery = Objects.requireNonNull(discovery, "discovery");
        this.gateRegistration = discovery.onAppCardGateChange(open -> full.values().stream()
                .filter(card -> !AppCards.forView(card, false).equals(card) || discovery.spiller().oversized(card))
                .forEach(this::issue));
    }

    /**
     * Publishes a full card now and keeps it, so a change of the view's level reissues it.
     *
     * @param card the full card, signed by this publisher's peer
     * @return the card as signed: the full card, the card without its gated fields, or its stub
     */
    public CompletableFuture<AppCard> publish(AppCard card) {
        Objects.requireNonNull(card, "card");
        if (!card.issuer().equals(identity.peerId())) {
            throw new IllegalArgumentException("a publisher issues its own peer's cards: " + card.issuer());
        }
        full.put(card.id(), card);
        return issue(card);
    }

    /**
     * Stops issuing a card; the last one signed lapses at its {@code ttl}, and the spiller releases its parts
     * document one {@code ttl} after the last card that named it.
     *
     * @param id the card's id
     */
    public void withdraw(String id) {
        full.remove(id);
        signed.remove(id);
    }

    /** Cards issued without a gated field because of the AppCard level. */
    public long appCardsDowngraded() {
        return appCardsDowngraded.get();
    }

    private synchronized CompletableFuture<AppCard> issue(AppCard card) {
        AppCard last = signed.get(card.id());
        Instant issued = card.issued();
        if (last != null && !issued.isAfter(last.issued())) {
            issued = last.issued().plusMillis(1); // the cache keeps only a strictly newer version
        }
        boolean open = AppCards.gateOpen(runtime);
        AppCard current = AppCards.issuedAt(card, issued);
        AppCard gated = AppCards.forView(current, open);
        if (!gated.equals(current)) {
            appCardsDowngraded.incrementAndGet();
        }
        signed.put(card.id(), gated);
        return discovery.spiller().prepare(gated, open).thenApply(prepared -> {
            if (full.containsKey(card.id())) {
                discovery.publish(signer.sign(prepared, identity));
            }
            return prepared;
        });
    }

    @Override
    public void close() {
        try {
            gateRegistration.close();
        } catch (Exception e) {
            throw new IllegalStateException("failed leaving the AppCard gate", e);
        }
    }
}
