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
package ai.badmonkey.agentspaces.discovery;

import ai.badmonkey.agentspaces.api.ad.Advertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.ad.RevocationAdvertisement;
import ai.badmonkey.agentspaces.api.ad.SignedAdvertisement;
import ai.badmonkey.agentspaces.api.space.Matchers;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.peering.membership.RevocationRegistry;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.wire.Envelope;

import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;

/**
 * Layer 2 discovery on one group (spec §6): publish signed advertisements into
 * the group's gossip, find them local-first in the {@link AdCache}, and escalate
 * to a scoped, hop-budgeted gossip query when the local cache is not enough.
 * Query predicates are structural (type plus field equality), so the base
 * protocol stays model-free; semantic discovery is a Layer 4 capability.
 */
public final class DiscoveryService implements AutoCloseable {

    /** The bus registrations this service owns; released by {@link #close()} (QA4 A4-9). */
    private final AutoCloseable adsRegistration;
    private final AutoCloseable queryRegistration;
    private final AutoCloseable reconcileRegistration;

    /** The stream discovery publishes advertisements on. */
    public static final String ADS_STREAM = "ads";
    /** The stream scoped queries travel on. */
    public static final String QUERY_STREAM = "ad-query";
    /**
     * The peering stream revocations travel on (spec §6.1): a
     * {@link RevocationAdvertisement} is not an ad-cache type. It is enforced
     * by the group's {@link RevocationRegistry}, so {@link #publish} routes it
     * there and onto this stream, the same path {@link GroupRuntime#revoke}
     * takes. Mirrors the constant in {@code PeerNode}.
     */
    public static final String REVOCATION_STREAM = "revocations";

    /** The gossip stream credential revocations ride (SPEC §6.1, v0.1.13); peering owns it. */
    public static final String CREDENTIAL_REVOCATION_STREAM = "credential-revocations";
    /**
     * How much larger a RENDEZVOUS peer's ad cache is than the default (spec
     * §5.4: the rendezvous keeps "a larger ad cache" because constrained
     * newcomers query it in place of the whole group).
     */
    public static final int RENDEZVOUS_CACHE_MULTIPLIER = 4;

    private record AdQuery(String queryId, PeerId asker, String adType,
                           Map<String, String> fieldEquals) {
    }

    private record QueryHits(String queryId, List<AdCache.StoredAd> hits) {
    }

    /**
     * The {@code fieldEquals} keys an AppCard query may carry beside its record fields (SPEC §6.1a.2, v0.1.32;
     * ISSUE-AppCardOriginFormats FR-8b): {@code origin.system}, {@code origin.ref} (a ref ending in {@code /*} matches
     * every ref under it), and {@code formats} (membership, {@code type/*} matching every media type of that type).
     */
    public static final java.util.Set<String> APP_CARD_QUERY_KEYS = java.util.Set.of("origin.system", "origin.ref", "formats");

    private final GroupRuntime runtime;
    private final AdCache cache;
    private final CborCodec codec;
    private final PeerId self;
    private final Map<String, CountDownLatch> pendingQueries = new ConcurrentHashMap<>();
    /** The publisher's half of the card spill (SPEC §6.1a.5, v0.1.32). */
    private final CardSpiller spiller;
    /** The largest parts document {@link #resolve} fetches. */
    private volatile long maxSpillFetchBytes = ai.badmonkey.agentspaces.api.ad.CardSpill.DEFAULT_MAX_FETCH_BYTES;
    private final java.util.concurrent.atomic.AtomicLong spillsRefused = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong spillsOverLimit = new java.util.concurrent.atomic.AtomicLong();
    private final List<java.util.function.Consumer<Boolean>> gateListeners = new java.util.concurrent.CopyOnWriteArrayList<>();
    /** The AppCard gate as the last tick saw it. */
    private volatile boolean gateOpen;
    /** The tick work that watches the gate and sweeps the spill holds, registered once something needs it. */
    private AutoCloseable tickRegistration;

    /**
     * Wires discovery into a group runtime.
     *
     * @param runtime the group runtime
     * @param cache   the group's ad cache
     * @param codec   the CBOR codec
     * @param self    the local peer id, used as the query return address
     */
    public DiscoveryService(GroupRuntime runtime, AdCache cache, CborCodec codec, PeerId self) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.cache = Objects.requireNonNull(cache, "cache");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.self = Objects.requireNonNull(self, "self");
        this.spiller = new CardSpiller(codec, cache.clock());
        // ASF-047: a revoked peer's advertisements leave the cache and stay out.
        cache.refuseIssuers(runtime.revocations()::revoked);
        runtime.revocations().addListener(revocation -> cache.purgeIssuer(revocation.revoked()));
        cache.refuseAgents(runtime.revocationView());
        runtime.credentialRevocations().addListener(revocation -> {
            if (revocation.target().agent() != null) {
                cache.purgeAgent(revocation.target().agent());
            }
        });

        this.adsRegistration = runtime.gossip().onStream(ADS_STREAM, (from, itemId, payload) ->
                decodeStored(payload).ifPresent(cache::accept));
        // Spec §6.2: caches MUST NOT forward expired advertisements; ASF-012: never
        // amplify content this node cannot verify end-to-end. Forward only what the
        // cache itself admits (signature, issuer key, TTL, skew), mirroring the
        // peers-stream filter in PeerNode.
        runtime.gossip().forwardFilter(ADS_STREAM, payload ->
                decodeStored(payload).flatMap(cache::accept).isPresent());
        this.queryRegistration = runtime.gossip().onStream(QUERY_STREAM,
                (from, itemId, payload) -> answerQuery(payload));
        this.reconcileRegistration = runtime.gossip().reconcile(ADS_STREAM,
                new ai.badmonkey.agentspaces.peering.gossip.ReconcilableState() {
            @Override
            public byte[] digest() {
                return cache.digest();
            }

            @Override
            public byte[] deltaFor(byte[] remoteDigest) {
                return cache.deltaFor(remoteDigest);
            }

            @Override
            public void applyDelta(byte[] delta) {
                cache.applyDelta(delta);
            }
        });
        runtime.onKind(Envelope.Kind.QUERY_HIT, (from, body) -> onQueryHit(body));
        // Direct queries: how constrained peers ask a rendezvous (spec §5.4, §6.3).
        runtime.onKind(Envelope.Kind.QUERY, (from, body) -> answerQuery(body));
    }

    /**
     * Wires discovery into a group runtime with an ad cache sized for the local
     * peer's topology roles (spec §5.4): a peer advertising
     * {@link PeerAdvertisement.PeerRole#RENDEZVOUS} gets a cache
     * {@link #RENDEZVOUS_CACHE_MULTIPLIER} times the default bound, any other
     * peer the default. Pass the same role set given to
     * {@code PeerNode.Builder.roles}.
     *
     * @param runtime    the group runtime
     * @param codec      the CBOR codec
     * @param self       the local peer id, used as the query return address
     * @param clock      the time source for TTL eviction
     * @param localRoles the roles the local node advertises
     * @return the discovery service
     */
    public static DiscoveryService create(GroupRuntime runtime, CborCodec codec, PeerId self,
                                          InstantSource clock,
                                          Set<PeerAdvertisement.PeerRole> localRoles) {
        return new DiscoveryService(runtime,
                new AdCache(codec, clock, cacheBoundFor(localRoles), cacheBytesFor(localRoles)), codec, self);
    }

    /**
     * The ad-cache byte budget for a peer advertising the given roles (SPEC §6.3, v0.1.32; AppCard origin and formats,
     * 2026-11-09).
     *
     * @param localRoles the roles the local node advertises
     * @return {@link AdCache#DEFAULT_MAX_BYTES}, multiplied by {@link #RENDEZVOUS_CACHE_MULTIPLIER} for a rendezvous
     */
    public static long cacheBytesFor(Set<PeerAdvertisement.PeerRole> localRoles) {
        Objects.requireNonNull(localRoles, "localRoles");
        return localRoles.contains(PeerAdvertisement.PeerRole.RENDEZVOUS)
                ? AdCache.DEFAULT_MAX_BYTES * RENDEZVOUS_CACHE_MULTIPLIER
                : AdCache.DEFAULT_MAX_BYTES;
    }

    /**
     * The ad-cache bound for a peer advertising the given roles (spec §5.4).
     *
     * @param localRoles the roles the local node advertises
     * @return {@link AdCache#DEFAULT_MAX_ENTRIES}, multiplied by
     *         {@link #RENDEZVOUS_CACHE_MULTIPLIER} for a rendezvous
     */
    public static int cacheBoundFor(Set<PeerAdvertisement.PeerRole> localRoles) {
        Objects.requireNonNull(localRoles, "localRoles");
        return localRoles.contains(PeerAdvertisement.PeerRole.RENDEZVOUS)
                ? AdCache.DEFAULT_MAX_ENTRIES * RENDEZVOUS_CACHE_MULTIPLIER
                : AdCache.DEFAULT_MAX_ENTRIES;
    }

    /** Returns the underlying cache. */
    public AdCache cache() {
        return cache;
    }

    /**
     * Installs the content swarm that serves this group's parts documents (SPEC §6.1a.5, v0.1.32): with it a card
     * above the spill threshold travels as a stub, and {@link #resolve} completes a stub; null removes it.
     *
     * @param store the store
     * @return this service
     */
    public DiscoveryService spillStore(SpillStore store) {
        spiller.store(store);
        if (store != null) {
            tick();
        }
        return this;
    }

    /**
     * Sets the largest parts document {@link #resolve} fetches ({@code CardSpill.DEFAULT_MAX_FETCH_BYTES} by default).
     *
     * @param bytes the bound
     * @return this service
     */
    public DiscoveryService maxSpillFetchBytes(long bytes) {
        if (bytes <= 0) {
            throw new IllegalArgumentException("the spill fetch bound must be positive: " + bytes);
        }
        this.maxSpillFetchBytes = bytes;
        return this;
    }

    /** The largest parts document {@link #resolve} fetches. */
    public long maxSpillFetchBytes() {
        return maxSpillFetchBytes;
    }

    /** The publisher's half of the card spill, which every card publisher of this group passes its cards through. */
    public CardSpiller spiller() {
        return spiller;
    }

    /** Whether this node and every member of the group's current view read the AppCard level's gated fields. */
    public boolean appCardGateOpen() {
        return AppCards.gateOpen(runtime);
    }

    /**
     * Calls a listener with the new state each time the view's AppCard level changes (SPEC §6.1a.2, v0.1.32), checked
     * on every tick of the group.
     *
     * @param listener receives true when the gate opens and false when it closes
     * @return closing it removes the listener
     */
    public AutoCloseable onAppCardGateChange(java.util.function.Consumer<Boolean> listener) {
        Objects.requireNonNull(listener, "listener");
        gateListeners.add(listener);
        tick();
        return () -> gateListeners.remove(listener);
    }

    /**
     * The full card of a stub (SPEC §6.1a.5, v0.1.32; ISSUE-AppCardOriginFormats FR-15): a card without {@code spill}
     * as it is, and otherwise the stub with its parts document's values in place. A {@code size} above
     * {@link #maxSpillFetchBytes()} fails before any fetch ({@code spillsOverLimit}); a document of the wrong length
     * (found by this service, or by the store before it fetches, see {@link SpillStore#get}), one that does not decode
     * as the kind's parts record, or one whose non-empty fields are not exactly {@code spill.parts} fails the
     * resolution ({@code spillsRefused}). A document no holder serves in time fails it uncounted. No failure strikes
     * anyone or evicts the stub.
     *
     * @param card a card, often from {@link #find}
     * @param <C>  the card kind
     * @return the full card
     */
    public <C extends Advertisement> java.util.concurrent.CompletableFuture<C> resolve(C card) {
        Objects.requireNonNull(card, "card");
        ai.badmonkey.agentspaces.api.ad.CardSpill spill = Spills.spillOf(card);
        if (spill == null) {
            return java.util.concurrent.CompletableFuture.completedFuture(card);
        }
        if (spill.size() > maxSpillFetchBytes) {
            spillsOverLimit.incrementAndGet();
            return java.util.concurrent.CompletableFuture.failedFuture(new IllegalStateException("the parts document of "
                    + card.id() + " is " + spill.size() + " bytes, above the fetch bound of " + maxSpillFetchBytes));
        }
        SpillStore store = spiller.store();
        if (store == null) {
            return java.util.concurrent.CompletableFuture.failedFuture(new IllegalStateException(
                    "no content swarm serves this group, so the stub " + card.id() + " stays a stub"));
        }
        return store.get(spill.root(), spill.size()).handle((document, failure) -> {
            Throwable cause = failure instanceof java.util.concurrent.CompletionException wrapped && wrapped.getCause() != null
                    ? wrapped.getCause() : failure;
            if (cause != null && !(cause instanceof IllegalArgumentException)) {
                // no holder served the document in time, or the fetch failed: no check failed, so nothing counts
                throw cause instanceof RuntimeException runtime ? runtime : new java.util.concurrent.CompletionException(cause);
            }
            try {
                if (cause != null) {
                    throw (IllegalArgumentException) cause; // the store refused the artifact for its length (SpillStore#get)
                }
                if (document.length != spill.size()) {
                    throw new IllegalArgumentException("the parts document is " + document.length + " bytes; the stub states "
                            + spill.size());
                }
                return Spills.complete(card, document, codec);
            } catch (RuntimeException refused) {
                spillsRefused.incrementAndGet();
                throw new IllegalStateException("the parts document of " + card.id() + " does not complete its stub", refused);
            }
        });
    }

    /** Resolutions that failed a check of the parts document (SPEC §6.1a.5). */
    public long spillsRefused() {
        return spillsRefused.get();
    }

    /** Resolutions refused because the stub's {@code size} passed the fetch bound. */
    public long spillsOverLimit() {
        return spillsOverLimit.get();
    }

    /**
     * Joins the group's tick once a listener watches the AppCard gate or a store takes spill documents (SPEC §6.1a.2
     * and §6.1a.5, v0.1.32): a change of the view's level reissues the cards it shapes, and the spill holds lapse one
     * ttl after the last card that named them. A service that needs neither adds no tick work.
     */
    private synchronized void tick() {
        if (tickRegistration == null) {
            gateOpen = AppCards.gateOpen(runtime);
            tickRegistration = runtime.onTick(this::onTick);
        }
    }

    private void onTick() {
        boolean open = AppCards.gateOpen(runtime);
        if (open != gateOpen) {
            gateOpen = open;
            gateListeners.forEach(listener -> listener.accept(open));
        }
        spiller.sweep();
    }

    /**
     * Publishes a signed advertisement into the group: admitted to the local
     * cache, pushed by rumor, and covered by anti-entropy from then on.
     *
     * <p>A {@link RevocationAdvertisement} is the one type that does not live
     * in the ad cache (spec §6.1): it is routed to the group's
     * {@link RevocationRegistry}, which verifies the signature and the
     * issuer's authority exactly as every receiver will, and on acceptance it
     * travels on the {@link #REVOCATION_STREAM} with its own anti-entropy.
     * What the registry refuses (an issuer that is not the trust root, a
     * foreign group, a far-future stamp) is never gossiped, matching how an ad
     * the local cache refuses is never gossiped.
     *
     * @param signed the signed advertisement
     */
    public void publish(SignedAdvertisement<?> signed) {
        Objects.requireNonNull(signed, "signed");
        if (signed.advertisement() instanceof RevocationAdvertisement revocation) {
            publishRevocation(revocation, signed.issuerPublicKey(), signed.signature());
            return;
        }
        if (signed.advertisement() instanceof ai.badmonkey.agentspaces.api.ad.CredentialRevocation revocation) {
            RevocationRegistry.SignedRevocation wire = new RevocationRegistry.SignedRevocation(
                    codec.toBytes(revocation), signed.issuerPublicKey(), signed.signature());
            if (runtime.credentialRevocations().verify(wire) != null) {
                runtime.credentialRevocations().accept(wire);
                runtime.gossip().publish(CREDENTIAL_REVOCATION_STREAM, "credential-revocation:"
                        + revocation.target().key() + ":" + revocation.issued().toEpochMilli(),
                        codec.toBytes(wire));
            }
            return;
        }
        AdCache.StoredAd stored = cache.toStored(
                signed.advertisement(), signed.issuerPublicKey(), signed.signature());
        if (cache.accept(stored).isEmpty()) {
            return; // spec §6.2: what our own cache refuses (expired, over-TTL,
                    // future-issued, unverifiable) is never gossiped
        }
        String itemId = signed.advertisement().id() + ":"
                + signed.advertisement().issued().toEpochMilli();
        runtime.gossip().publish(ADS_STREAM, itemId, codec.toBytes(stored));
    }

    /**
     * Local-first find (spec §6.3): the cache only, never the network.
     *
     * @param type      the advertisement type
     * @param predicate the filter
     * @param <A>       the advertisement type
     * @return matching cached advertisements
     */
    public <A extends Advertisement> List<A> find(Class<A> type, Predicate<A> predicate) {
        return cache.find(type, predicate);
    }

    /**
     * Escalating find: publish a scoped query, wait briefly for direct
     * QUERY_HIT answers to merge into the cache, then return the local matches.
     *
     * @param type        the advertisement type
     * @param fieldEquals structural predicate: record field name to expected
     *                    string value ({@code String.valueOf} comparison)
     * @param wait        how long to wait for answers
     * @param <A>         the advertisement type
     * @return the matches present after the wait
     */
    public <A extends Advertisement> List<A> remoteFind(
            Class<A> type, Map<String, String> fieldEquals, Duration wait) {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(fieldEquals, "fieldEquals");
        Objects.requireNonNull(wait, "wait");
        // Spec §6.3: the predicate is validated against the target type before
        // anything reaches the wire, so an unknown field fails fast here rather
        // than after the wait and after a query nobody could answer.
        Predicate<A> localMatcher = matcher(type, fieldEquals);
        String queryId = UUID.randomUUID().toString();
        CountDownLatch latch = new CountDownLatch(1);
        pendingQueries.put(queryId, latch);
        try {
            AdQuery query = new AdQuery(queryId, self, type.getSimpleName(), fieldEquals);
            byte[] queryBytes = codec.toBytes(query);
            // Escalation order (spec §6.3): the group's rendezvous peers first,
            // by direct frame, then the scoped gossip query.
            for (ai.badmonkey.agentspaces.common.id.PeerId rendezvous : runtime.membership()
                    .withRole(ai.badmonkey.agentspaces.api.ad.PeerAdvertisement.PeerRole.RENDEZVOUS)) {
                runtime.send(rendezvous, Envelope.Kind.QUERY, query);
            }
            runtime.gossip().publish(QUERY_STREAM, queryId, queryBytes);
            if (!wait.isZero()) {
                latch.await(wait.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            pendingQueries.remove(queryId);
        }
        return find(type, localMatcher);
    }

    // ---------------------------------------------------------------- internals

    private void publishRevocation(RevocationAdvertisement revocation, byte[] publicKey,
                                   byte[] signature) {
        RevocationRegistry.SignedRevocation signed = new RevocationRegistry.SignedRevocation(
                codec.toBytes(revocation), publicKey, signature);
        if (runtime.revocations().accept(signed).isEmpty()) {
            return; // unauthorized, foreign, malformed, or already known here
        }
        runtime.gossip().publish(REVOCATION_STREAM,
                "revocation:" + revocation.revoked().value(), codec.toBytes(signed));
    }

    private void answerQuery(byte[] payload) {
        AdQuery query;
        try {
            query = codec.fromWire(payload, AdQuery.class);
        } catch (RuntimeException e) {
            return;
        }
        if (query == null || query.asker() == null || query.adType() == null) {
            return;
        }
        if (query.asker().equals(self)) {
            return; // our own query came back around
        }
        List<AdCache.StoredAd> hits = cache.storedAds().stream()
                .filter(stored -> stored.adType().equals(query.adType()))
                .filter(stored -> decodeMatches(stored, query))
                .toList();
        if (!hits.isEmpty()) {
            runtime.send(query.asker(), Envelope.Kind.QUERY_HIT,
                    new QueryHits(query.queryId(), hits));
        }
    }

    private boolean decodeMatches(AdCache.StoredAd stored, AdQuery query) {
        return cache.accept(stored)
                .map(ad -> matchesFields(ad, query.fieldEquals()))
                .orElse(false);
    }

    private void onQueryHit(byte[] body) {
        QueryHits hits;
        try {
            hits = codec.fromWire(body, QueryHits.class);
        } catch (RuntimeException e) {
            return;
        }
        if (hits == null || hits.hits() == null) {
            return;
        }
        hits.hits().forEach(cache::accept);
        CountDownLatch latch = pendingQueries.get(hits.queryId());
        if (latch != null) {
            latch.countDown();
        }
    }

    private java.util.Optional<AdCache.StoredAd> decodeStored(byte[] payload) {
        try {
            return java.util.Optional.ofNullable(
                    codec.fromWire(payload, AdCache.StoredAd.class));
        } catch (RuntimeException e) {
            return java.util.Optional.empty();
        }
    }

    private static <A extends Advertisement> Predicate<A> matcher(
            Class<A> type, Map<String, String> fieldEquals) {
        if (fieldEquals.isEmpty()) {
            return ad -> true;
        }
        Template<A> template = Template.of(type);
        Predicate<A> keys = ad -> true;
        for (Map.Entry<String, String> e : fieldEquals.entrySet()) {
            if (type == ai.badmonkey.agentspaces.api.ad.AppCard.class && APP_CARD_QUERY_KEYS.contains(e.getKey())) {
                Predicate<ai.badmonkey.agentspaces.api.ad.AppCard> key = appCardKey(e.getKey(), e.getValue());
                keys = keys.and(ad -> key.test((ai.badmonkey.agentspaces.api.ad.AppCard) ad));
                continue;
            }
            template = template.where(e.getKey(),
                    Matchers.predicate(v -> String.valueOf(v).equals(e.getValue())));
        }
        Template<A> built = template;
        return keys.and(built::matches);
    }

    private static boolean matchesFields(Advertisement ad, Map<String, String> fieldEquals) {
        if (fieldEquals == null || fieldEquals.isEmpty()) {
            return true;
        }
        @SuppressWarnings("unchecked")
        Class<Advertisement> type = (Class<Advertisement>) ad.getClass();
        try {
            return matcher(type, fieldEquals).test(ad);
        } catch (IllegalArgumentException unknownField) {
            return false;
        }
    }

    /** One of {@link #APP_CARD_QUERY_KEYS} as a predicate on the card's inline fields, which a stub keeps. */
    private static Predicate<ai.badmonkey.agentspaces.api.ad.AppCard> appCardKey(String key, String value) {
        return switch (key) {
            case "origin.system" -> card -> card.fromOrigin(value);
            case "origin.ref" -> card -> card.origin() != null && card.fromOrigin(card.origin().system(), value);
            default -> card -> card.handles(value);
        };
    }

    /**
     * Leaves the gossip bus, so another discovery service may be wired for this
     * group runtime and stray advertisements stop reaching a cache nobody reads
     * (QA4 A4-9). The cache itself is untouched.
     */
    @Override
    public void close() {
        try {
            adsRegistration.close();
            queryRegistration.close();
            reconcileRegistration.close();
            synchronized (this) {
                if (tickRegistration != null) {
                    tickRegistration.close();
                    tickRegistration = null;
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("failed deregistering discovery from the bus", e);
        }
    }
}
