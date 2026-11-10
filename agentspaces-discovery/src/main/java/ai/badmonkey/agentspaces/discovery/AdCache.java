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
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.ad.SpaceAdvertisement;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/**
 * The local advertisement cache of one group (spec §6.3): populated by gossip,
 * queried local-first, evicting by TTL, and admitting nothing unverified. Every
 * stored advertisement was checked twice: the carried public key hashes to the
 * advertisement's self-certifying issuer, and the signature verifies over the
 * exact bytes received.
 *
 * <p>The cache also acts as a {@code ReconcilableState}-style participant through
 * {@link #digest()}, {@link #deltaFor(byte[])}, and {@link #applyDelta(byte[])},
 * so anti-entropy converges caches that rumors missed.
 */
public final class AdCache {

    /**
     * The wire form of a cached advertisement: type tag, the exact canonical
     * bytes, the issuer's raw key, and the signature over those bytes.
     *
     * @param adType    the advertisement type tag, e.g. {@code AgentCard}
     * @param adBytes   canonical CBOR of the advertisement
     * @param publicKey the issuer's raw Ed25519 public key
     * @param signature signature over {@code adBytes}
     */
    public record StoredAd(String adType, byte[] adBytes, byte[] publicKey, byte[] signature) {
    }

    private record CacheEntry(Advertisement advertisement, StoredAd stored) {
    }

    private static final Map<String, Class<? extends Advertisement>> AD_TYPES = Map.of(
            "PeerAdvertisement", PeerAdvertisement.class,
            "GroupAdvertisement", GroupAdvertisement.class,
            "SpaceAdvertisement", SpaceAdvertisement.class,
            "CapabilityAdvertisement", CapabilityAdvertisement.class,
            "AgentCard", AgentCard.class,
            "AssetCard", ai.badmonkey.agentspaces.api.ad.AssetCard.class,
            "ModelCard", ai.badmonkey.agentspaces.api.ad.ModelCard.class,
            "AppCard", ai.badmonkey.agentspaces.api.ad.AppCard.class);

    /**
     * The default bound on cached advertisements (ASF-019). A peer serving the
     * RENDEZVOUS role (spec §5.4) answers other peers' queries from its cache
     * and so configures a larger one; see {@code DiscoveryService.cacheBoundFor}.
     */
    public static final int DEFAULT_MAX_ENTRIES = 8_192;

    /**
     * The default byte budget (SPEC §6.3, v0.1.32; AppCard origin and formats, 2026-11-09): 32 MiB, the sum of the
     * cached advertisements' encodings. A rendezvous multiplies it as it multiplies the entry bound; see
     * {@code DiscoveryService.cacheBytesFor}.
     */
    public static final long DEFAULT_MAX_BYTES = 33_554_432L;

    private final CborCodec codec;
    private final InstantSource clock;
    /** Bound on cached advertisements (ASF-019). */
    private final int maxEntries;
    /** Bound on the sum of the cached advertisements' encodings (SPEC §6.3, v0.1.32). */
    private final long maxBytes;
    /** The sum of {@code adBytes} over the cached entries; written under this cache's lock. */
    private long bytes;
    private final java.util.concurrent.atomic.AtomicLong adsRefusedCacheFull = new java.util.concurrent.atomic.AtomicLong();

    /** How far in the future an ad's {@code issued} may run (ASF-027). */
    private static final java.time.Duration MAX_ISSUED_SKEW = java.time.Duration.ofMinutes(10);
    /** Longest TTL a cached ad may claim (ASF-027); leased state refreshes in
     * minutes, so anything beyond a month is hostile or misconfigured. */
    private static final java.time.Duration MAX_TTL = java.time.Duration.ofDays(30);

    private final Map<String, CacheEntry> byId = new ConcurrentHashMap<>();
    private static final ai.badmonkey.agentspaces.identity.AgentCertificates CERTIFICATES =
            new ai.badmonkey.agentspaces.identity.AgentCertificates();
    private volatile Predicate<PeerId> refusedIssuers = issuer -> false;
    private volatile ai.badmonkey.agentspaces.api.security.RevocationView revocations =
            ai.badmonkey.agentspaces.api.security.RevocationView.NONE;

    /**
     * Creates the cache.
     *
     * @param codec the CBOR codec
     * @param clock the time source for TTL eviction
     */
    public AdCache(CborCodec codec, InstantSource clock) {
        this(codec, clock, DEFAULT_MAX_ENTRIES);
    }

    /**
     * Creates the cache with an explicit bound (ASF-019; spec §5.4 for the
     * larger rendezvous cache).
     *
     * @param codec      the CBOR codec
     * @param clock      the time source for TTL eviction
     * @param maxEntries the most advertisements the cache retains; a new id is
     *                   refused while the cache is full
     */
    public AdCache(CborCodec codec, InstantSource clock, int maxEntries) {
        this(codec, clock, maxEntries, DEFAULT_MAX_BYTES);
    }

    /**
     * Creates the cache with both bounds (SPEC §6.3, v0.1.32; AppCard origin and formats, 2026-11-09).
     *
     * @param codec      the CBOR codec
     * @param clock      the time source for TTL eviction
     * @param maxEntries the most advertisements the cache retains
     * @param maxBytes   the most bytes of advertisement encodings the cache retains
     */
    public AdCache(CborCodec codec, InstantSource clock, int maxEntries, long maxBytes) {
        this.codec = Objects.requireNonNull(codec, "codec");
        this.clock = Objects.requireNonNull(clock, "clock");
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be positive: " + maxEntries);
        }
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be positive: " + maxBytes);
        }
        this.maxEntries = maxEntries;
        this.maxBytes = maxBytes;
    }

    /**
     * Sets the issuers whose advertisements this cache refuses, consulted on
     * every acceptance: the group's revoked peers (ASF-047).
     *
     * @param refused the issuers to refuse
     */
    public void refuseIssuers(Predicate<PeerId> refused) {
        this.refusedIssuers = Objects.requireNonNull(refused, "refused");
    }

    /**
     * Removes every cached advertisement an issuer published, as its
     * revocation is accepted (ASF-047).
     *
     * @param issuer the issuer
     * @return how many advertisements were removed
     */
    public int purgeIssuer(PeerId issuer) {
        Objects.requireNonNull(issuer, "issuer");
        return removeIf(entry -> entry.advertisement().issuer().equals(issuer));
    }

    /**
     * Refuses the cards of revoked agents (SPEC §6.1, v0.1.13): a card naming
     * an agent, or carrying an agent key, that the view revokes is not cached.
     *
     * @param revocations the group's revocation view
     */
    public void refuseAgents(ai.badmonkey.agentspaces.api.security.RevocationView revocations) {
        this.revocations = Objects.requireNonNull(revocations, "revocations");
    }

    /**
     * Removes every cached card of an agent, as its revocation is accepted.
     *
     * @param agent the agent
     * @return how many cards were removed
     */
    public int purgeAgent(ai.badmonkey.agentspaces.common.id.AgentId agent) {
        Objects.requireNonNull(agent, "agent");
        return removeIf(entry -> entry.advertisement()
                instanceof ai.badmonkey.agentspaces.api.ad.AgentCard card && card.agent().equals(agent));
    }

    /** Returns the bound on cached advertisements. */
    public int maxEntries() {
        return maxEntries;
    }

    /** Returns the byte budget: the most bytes of advertisement encodings the cache retains (SPEC §6.3, v0.1.32). */
    public long maxBytes() {
        return maxBytes;
    }

    /** Returns the sum of the cached advertisements' encodings, expired ones included until a read evicts them. */
    public synchronized long bytes() {
        return bytes;
    }

    /** How many advertisements this cache refused under its entry bound or its byte budget (SPEC §6.3, v0.1.32). */
    public long adsRefusedCacheFull() {
        return adsRefusedCacheFull.get();
    }

    /** The time source this cache evicts by, which the discovery service's spill holds also follow. */
    InstantSource clock() {
        return clock;
    }

    /**
     * Builds the wire form for a locally issued advertisement.
     *
     * @param advertisement the advertisement
     * @param publicKey     the issuer's raw public key
     * @param signature     the signature over the advertisement's canonical bytes
     * @return the wire form
     */
    public StoredAd toStored(Advertisement advertisement, byte[] publicKey, byte[] signature) {
        return new StoredAd(typeTag(advertisement), codec.toBytes(advertisement),
                publicKey, signature);
    }

    /**
     * Verifies and admits an advertisement in wire form. Expired, malformed,
     * mistyped, or badly signed advertisements are rejected.
     *
     * @param stored the wire form
     * @return the admitted advertisement, or empty when rejected
     */
    public Optional<Advertisement> accept(StoredAd stored) {
        if (stored == null || stored.adType() == null || stored.adBytes() == null
                || stored.publicKey() == null || stored.signature() == null) {
            return Optional.empty();
        }
        Class<? extends Advertisement> type = AD_TYPES.get(stored.adType());
        if (type == null
                || stored.publicKey().length != Ed25519.RAW_PUBLIC_KEY_LENGTH) {
            return Optional.empty();
        }
        Advertisement ad;
        try {
            ad = codec.fromWire(stored.adBytes(), type);
        } catch (RuntimeException e) {
            return Optional.empty();
        }
        if (refusedIssuers.test(ad.issuer())) {
            return Optional.empty(); // ASF-047: a revoked peer publishes nothing here
        }
        if (!PeerId.fromPublicKey(stored.publicKey()).equals(ad.issuer())
                || !Ed25519.verify(Ed25519.publicKeyFromRaw(stored.publicKey()),
                        stored.adBytes(), stored.signature())
                || ad.expired(clock.instant())) {
            return Optional.empty();
        }
        // ASF-027: `issued` is issuer-chosen, so a far-future stamp would make
        // an ad un-evictable and permanently newest in last-writer-wins. Bounded
        // clock skew is honest; anything beyond it is refused, and TTLs are
        // capped so no ad outlives the refresh cadence by orders of magnitude.
        if (ad.issued().isAfter(clock.instant().plus(MAX_ISSUED_SKEW))
                || ad.ttl().compareTo(MAX_TTL) > 0) {
            return Optional.empty();
        }
        // SPEC §6.1 v0.1.13 (TODO-9-10-11 B6): a card that carries its agent's
        // certificate must prove it, under the card issuer's key, covering the
        // card's issue time; an unprovable attestation is refused, not shown.
        if (ad instanceof ai.badmonkey.agentspaces.api.ad.AgentCard card
                && card.agentCertificate() != null
                && !CERTIFICATES.verifyAt(card.agentCertificate(), stored.publicKey(), card.agent(),
                        card.issued(), clock.instant())) {
            return Optional.empty();
        }
        if (ad instanceof ai.badmonkey.agentspaces.api.ad.AgentCard card
                && revocations.revoked(card.agent(), card.agentPublicKey())) {
            return Optional.empty(); // v0.1.13: a revoked agent advertises nothing
        }
        return admit(new CacheEntry(ad, stored)) ? Optional.of(ad) : Optional.empty();
    }

    /**
     * ASF-019 and SPEC §6.3 (v0.1.32; AppCard origin and formats, 2026-11-09): the cache is bounded against
     * advertisement floods by entry count and by bytes. A brand-new id is refused while either bound would pass, and a
     * newer version of a known id is refused while it would pass the byte budget, which keeps the version held; each
     * check runs again after the expired entries leave. A flood can fill spare headroom but never evicts a live
     * advertisement. An older or equal version of a known id leaves the held one in place.
     */
    private synchronized boolean admit(CacheEntry candidate) {
        String id = candidate.advertisement().id();
        long length = candidate.stored().adBytes().length;
        if (!fits(id, candidate, length)) {
            evictExpired();
            if (!fits(id, candidate, length)) {
                adsRefusedCacheFull.incrementAndGet();
                return false;
            }
        }
        CacheEntry existing = byId.get(id);
        if (existing == null) {
            byId.put(id, candidate);
            bytes += length;
        } else if (candidate.advertisement().issued().isAfter(existing.advertisement().issued())) {
            byId.put(id, candidate);
            bytes += length - existing.stored().adBytes().length;
        }
        return true;
    }

    /** Whether the candidate fits both bounds, given what the cache holds for its id. */
    private boolean fits(String id, CacheEntry candidate, long length) {
        CacheEntry existing = byId.get(id);
        if (existing == null) {
            return byId.size() < maxEntries && bytes + length <= maxBytes;
        }
        if (!candidate.advertisement().issued().isAfter(existing.advertisement().issued())) {
            return true; // the held version stays; nothing grows
        }
        return bytes - existing.stored().adBytes().length + length <= maxBytes;
    }

    /**
     * Finds unexpired advertisements of a type matching a predicate; local-first
     * and never blocking (spec §6.3).
     *
     * @param type      the advertisement type
     * @param predicate the filter
     * @param <A>       the advertisement type
     * @return matching advertisements
     */
    public <A extends Advertisement> List<A> find(Class<A> type, Predicate<A> predicate) {
        evictExpired();
        List<A> results = new ArrayList<>();
        for (CacheEntry entry : byId.values()) {
            if (type.isInstance(entry.advertisement())) {
                A ad = type.cast(entry.advertisement());
                if (predicate.test(ad)) {
                    results.add(ad);
                }
            }
        }
        return results;
    }

    /** Returns the number of cached, unexpired advertisements. */
    public int size() {
        evictExpired();
        return byId.size();
    }

    /** Returns the wire forms of all cached advertisements; used to answer queries. */
    public List<StoredAd> storedAds() {
        evictExpired();
        return byId.values().stream().map(CacheEntry::stored).toList();
    }

    // ------------------------------------------------- anti-entropy participation

    /** Returns a digest of cached ad ids and issue times. */
    public byte[] digest() {
        evictExpired();
        TreeMap<String, Long> summary = new TreeMap<>();
        for (CacheEntry e : byId.values()) {
            summary.put(e.advertisement().id(), e.advertisement().issued().toEpochMilli());
        }
        StringBuilder sb = new StringBuilder();
        summary.forEach((id, issued) -> sb.append(id).append('=').append(issued).append('\n'));
        return hashPrefixed(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Computes the ads a remote cache is missing, given its digest.
     *
     * @param remoteDigest the remote digest
     * @return CBOR list of missing {@link StoredAd}s; empty when none
     */
    public byte[] deltaFor(byte[] remoteDigest) {
        evictExpired(); // spec §6.2: caches MUST NOT forward expired advertisements
        Set<String> remoteKeys = parseDigestKeys(remoteDigest);
        List<StoredAd> missing = new ArrayList<>();
        for (CacheEntry e : byId.values()) {
            String key = e.advertisement().id() + "="
                    + e.advertisement().issued().toEpochMilli();
            if (!remoteKeys.contains(key)) {
                missing.add(e.stored());
            }
        }
        return missing.isEmpty() ? new byte[0] : codec.toBytes(new Delta(missing));
    }

    /**
     * Applies a delta of stored ads, verifying each.
     *
     * @param delta the delta bytes
     */
    public void applyDelta(byte[] delta) {
        if (delta.length == 0) {
            return;
        }
        Delta parsed;
        try {
            parsed = codec.fromWire(delta, Delta.class);
        } catch (RuntimeException e) {
            return;
        }
        if (parsed != null && parsed.ads() != null) {
            parsed.ads().forEach(this::accept);
        }
    }

    /** Wire form of an anti-entropy delta. */
    public record Delta(List<StoredAd> ads) {
    }

    // ---------------------------------------------------------------- internals

    private void evictExpired() {
        var now = clock.instant();
        removeIf(e -> e.advertisement().expired(now));
    }

    /** Removes the matching entries and their bytes from the sum. */
    private synchronized int removeIf(Predicate<CacheEntry> doomed) {
        int removed = 0;
        for (var it = byId.values().iterator(); it.hasNext(); ) {
            CacheEntry entry = it.next();
            if (doomed.test(entry)) {
                it.remove();
                bytes -= entry.stored().adBytes().length;
                removed++;
            }
        }
        return removed;
    }

    private static String typeTag(Advertisement ad) {
        for (Map.Entry<String, Class<? extends Advertisement>> e : AD_TYPES.entrySet()) {
            if (e.getValue() == ad.getClass()) {
                return e.getKey();
            }
        }
        throw new IllegalArgumentException("unknown advertisement class: " + ad.getClass());
    }

    /**
     * The digest is the raw key list plus a hash prefix. Keys travel in the clear
     * so {@link #deltaFor} can diff without a second round trip.
     */
    private static byte[] hashPrefixed(byte[] keys) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(keys);
            byte[] out = new byte[hash.length + keys.length];
            System.arraycopy(hash, 0, out, 0, hash.length);
            System.arraycopy(keys, 0, out, hash.length, keys.length);
            return out;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static Set<String> parseDigestKeys(byte[] digest) {
        if (digest.length <= 32) {
            return Set.of();
        }
        String keys = new String(digest, 32, digest.length - 32, StandardCharsets.UTF_8);
        Set<String> parsed = new HashSet<>();
        for (String line : keys.split("\n")) {
            if (!line.isEmpty()) {
                parsed.add(line);
            }
        }
        return parsed;
    }
}
