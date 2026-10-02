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
package ai.badmonkey.agentspaces.capabilities.semantic;

import ai.badmonkey.agentspaces.api.ad.Advertisement;
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.ad.SpaceAdvertisement;
import ai.badmonkey.agentspaces.api.spi.CapabilityProvider;
import ai.badmonkey.agentspaces.api.spi.Embedder;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;

import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The {@code aspace:cap/semantic-discovery} capability (spec §8): lookup by
 * meaning over the group's advertisements. The structural path answers "who
 * produces a Finding#v1?"; this capability answers "who can summarize legal
 * PDFs cheaply?" and "who holds customer order history?" by ranking AgentCards,
 * AssetCards, and CapabilityAdvertisements against a free-text query under the
 * pluggable {@link Embedder}.
 *
 * <p>Queries are local-first (spec P7): {@link #query} ranks the local
 * ad-cache and costs no network. {@link #remoteQuery} escalates the way
 * structural discovery does, asking rendezvous members first and sampled
 * members otherwise; each answers with its own top matches as stored signed
 * advertisements, and the asker re-verifies every one through the ad-cache's
 * admission path before it can appear in results, so a lying responder can
 * inject nothing the signature discipline would reject.
 */
public final class SemanticDiscovery implements CapabilityProvider {

    /** The capability type URI. */
    public static final String TYPE = "aspace:cap/semantic-discovery";

    /**
     * The similarity floor below which a candidate is noise, not a match.
     * Feature hashing collides occasionally, and between short texts a single
     * colliding bucket already scores around 0.15, so the floor asks for
     * roughly two tokens of shared meaning before anything ranks.
     */
    public static final double MIN_SCORE = 0.2;

    /**
     * One ranked match.
     *
     * @param advertisement the matching advertisement
     * @param score         cosine similarity to the query, higher first
     */
    public record Match(Advertisement advertisement, double score) {
    }

    private record SemQuery(long nonce, String text, int limit) {
    }

    private record SemHit(String adType, byte[] adBytes, byte[] publicKey, byte[] signature) {
    }

    private record SemResponse(long nonce, List<SemHit> hits) {
    }

    private final CapabilityPipes pipes;
    private final GroupRuntime runtime;
    private final DiscoveryService discovery;
    private final PeerId self;
    private final CborCodec codec;
    private static final System.Logger LOG = System.getLogger(SemanticDiscovery.class.getName());

    private final InstantSource clock;
    private final Embedder embedder;
    private final AtomicLong nonces = new AtomicLong();
    private final Set<PeerId> mismatchWarned = ConcurrentHashMap.newKeySet();
    private volatile boolean requireMatchingEmbedder = true;
    private final Map<Long, CountDownLatch> pending = new ConcurrentHashMap<>();

    /**
     * Creates the capability.
     *
     * @param pipes     the group's capability pipes
     * @param runtime   the group runtime (topology roles for escalation)
     * @param discovery the group's discovery service; its cache is the index
     * @param self      the local peer id
     * @param codec     the CBOR codec
     * @param clock     the time source for advertisement freshness
     * @param embedder  the embedder; {@link HashingEmbedder} is the model-free default
     */
    public SemanticDiscovery(CapabilityPipes pipes, GroupRuntime runtime,
                             DiscoveryService discovery, PeerId self,
                             CborCodec codec, InstantSource clock, Embedder embedder) {
        this.pipes = Objects.requireNonNull(pipes, "pipes");
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.discovery = Objects.requireNonNull(discovery, "discovery");
        this.self = Objects.requireNonNull(self, "self");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.embedder = Objects.requireNonNull(embedder, "embedder");
        pipes.onCapability(TYPE, this::onFrame);
    }

    @Override
    public String capabilityType() {
        return TYPE;
    }

    @Override
    public CapabilityAdvertisement describe(GroupId group) {
        return new CapabilityAdvertisement(
                "aspace://" + group.value() + "/cap/semantic-discovery/" + self.value(),
                self, group, clock.instant(), Duration.ofMinutes(15),
                TYPE, "0.1", "pipe",
                Map.of("embedder", embedder.identity(),
                        "dimensions", String.valueOf(embedder.dimensions()),
                        "normalized", String.valueOf(embedder.normalized())),
                Map.of());
    }

    /**
     * Whether {@link #remoteQuery} asks only peers that advertise the same
     * embedder (identity and dimensions); on by default. Peers that rank with a
     * different embedding return different matches, so mixing them silently
     * degrades discovery.
     *
     * @param require whether to require a matching embedder
     * @return this capability
     */
    public SemanticDiscovery requireMatchingEmbedder(boolean require) {
        this.requireMatchingEmbedder = require;
        return this;
    }

    /** Whether remote queries go only to peers advertising the same embedder. */
    public boolean requiresMatchingEmbedder() {
        return requireMatchingEmbedder;
    }

    /** The embedder this peer ranks with. */
    public Embedder embedder() {
        return embedder;
    }

    /**
     * Whether a peer advertises the same embedder as this one. A peer that
     * advertises no semantic-discovery capability, or no embedder, is taken to
     * match, so older peers keep answering.
     *
     * @param peer the peer
     * @return whether its advertised embedder matches ours
     */
    public boolean embedderMatches(PeerId peer) {
        for (CapabilityAdvertisement ad : discovery.find(CapabilityAdvertisement.class,
                candidate -> TYPE.equals(candidate.capabilityType()) && peer.equals(candidate.issuer()))) {
            String theirs = ad.parameters().get("embedder");
            String dimensions = ad.parameters().get("dimensions");
            if (theirs != null && (!theirs.equals(embedder.identity())
                    || (dimensions != null && !dimensions.equals(String.valueOf(embedder.dimensions()))))) {
                if (mismatchWarned.add(peer)) {
                    LOG.log(System.Logger.Level.WARNING, "peer " + peer.display() + " ranks with embedder '"
                            + theirs + "' (" + dimensions + " dimensions), this peer with '"
                            + embedder.identity() + "' (" + embedder.dimensions() + "); semantic queries skip it."
                            + " Configure one embedder across the group.");
                }
                return false;
            }
        }
        return true;
    }

    /**
     * Ranks the local ad-cache against a free-text query. Local-first: no
     * network, no blocking.
     *
     * @param text  the query, e.g. {@code "who holds customer order history?"}
     * @param limit the maximum matches to return
     * @return matches with positive similarity, best first
     */
    public List<Match> query(String text, int limit) {
        Objects.requireNonNull(text, "text");
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive: " + limit);
        }
        double[] queryVector = embedder.embed(text);
        List<Match> matches = new ArrayList<>();
        for (Advertisement ad : indexable()) {
            double score = HashingEmbedder.cosine(queryVector, embedder.embed(textOf(ad)));
            if (score >= MIN_SCORE) {
                matches.add(new Match(ad, score));
            }
        }
        matches.sort(Comparator.comparingDouble(Match::score).reversed());
        return matches.size() > limit ? matches.subList(0, limit) : matches;
    }

    /**
     * Escalating query (spec §6.3 applied to meaning): ask rendezvous members
     * first and sampled members otherwise, fold every verified answer into the
     * local cache, and rank locally. Answers a partitioned or empty group
     * cannot supply simply never arrive, and the local ranking still returns.
     *
     * @param text    the query
     * @param limit   the maximum matches to return
     * @param timeout how long to wait for remote answers
     * @return matches with positive similarity, best first, remote finds included
     */
    public List<Match> remoteQuery(String text, int limit, Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        Set<PeerId> targets = new LinkedHashSet<>(
                runtime.membership().withRole(PeerAdvertisement.PeerRole.RENDEZVOUS));
        if (targets.isEmpty()) {
            targets.addAll(runtime.sampler().randomMembers(3));
        }
        targets.remove(self);
        if (requireMatchingEmbedder) {
            targets.removeIf(peer -> !embedderMatches(peer));
        }
        if (!targets.isEmpty()) {
            long nonce = nonces.incrementAndGet();
            CountDownLatch latch = new CountDownLatch(targets.size());
            pending.put(nonce, latch);
            try {
                byte[] frame = codec.toBytes(new SemQuery(nonce, text, limit));
                for (PeerId target : targets) {
                    pipes.send(target, TYPE, frame);
                }
                latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                pending.remove(nonce);
            }
        }
        return query(text, limit);
    }

    // ---------------------------------------------------------------- internals

    private void onFrame(PeerId from, byte[] payload) {
        Object frame = decode(payload);
        if (frame instanceof SemQuery query && query.text() != null) {
            answer(from, query);
        } else if (frame instanceof SemResponse response && response.hits() != null) {
            for (SemHit hit : response.hits()) {
                if (hit != null && hit.adType() != null) {
                    // Admission re-verifies the signature and the self-certifying
                    // issuer key; a bad hit dies here.
                    discovery.cache().accept(new AdCache.StoredAd(
                            hit.adType(), hit.adBytes(), hit.publicKey(), hit.signature()));
                }
            }
            CountDownLatch latch = pending.get(response.nonce());
            if (latch != null) {
                latch.countDown();
            }
        }
    }

    private Object decode(byte[] payload) {
        try {
            SemQuery query = codec.fromBytes(payload, SemQuery.class);
            if (query != null && query.text() != null) {
                return query;
            }
        } catch (RuntimeException ignored) {
            // Not a query; try the response shape.
        }
        try {
            return codec.fromBytes(payload, SemResponse.class);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private void answer(PeerId from, SemQuery query) {
        double[] queryVector = embedder.embed(query.text());
        record ScoredStored(AdCache.StoredAd stored, double score) {
        }
        List<ScoredStored> scored = new ArrayList<>();
        for (AdCache.StoredAd stored : discovery.cache().storedAds()) {
            discovery.cache().accept(stored).ifPresent(ad -> {
                double score = HashingEmbedder.cosine(queryVector, embedder.embed(textOf(ad)));
                if (score >= MIN_SCORE) {
                    scored.add(new ScoredStored(stored, score));
                }
            });
        }
        scored.sort(Comparator.comparingDouble(ScoredStored::score).reversed());
        int limit = Math.max(1, Math.min(query.limit(), 16));
        List<SemHit> hits = new ArrayList<>();
        for (ScoredStored s : scored.subList(0, Math.min(limit, scored.size()))) {
            hits.add(new SemHit(s.stored().adType(), s.stored().adBytes(),
                    s.stored().publicKey(), s.stored().signature()));
        }
        pipes.send(from, TYPE, codec.toBytes(new SemResponse(query.nonce(), hits)));
    }

    private List<Advertisement> indexable() {
        List<Advertisement> ads = new ArrayList<>();
        ads.addAll(discovery.find(AgentCard.class, card -> true));
        ads.addAll(discovery.find(AssetCard.class, card -> true));
        ads.addAll(discovery.find(CapabilityAdvertisement.class, ad -> true));
        ads.addAll(discovery.find(SpaceAdvertisement.class, ad -> true));
        return ads;
    }

    /** The text an advertisement is ranked by: its self-description. */
    static String textOf(Advertisement ad) {
        if (ad instanceof AgentCard card) {
            StringBuilder text = new StringBuilder(card.description()).append(' ')
                    .append(String.join(" ", card.goals()))
                    .append(' ').append(String.join(" ", card.consumes()))
                    .append(' ').append(String.join(" ", card.produces()));
            if (card.actions() != null) {
                // SPEC §6.1 v0.1.13: each declared action's name and description are indexed.
                for (ai.badmonkey.agentspaces.api.ad.CardAction action : card.actions()) {
                    text.append(' ').append(action.name()).append(' ').append(action.description());
                }
            }
            return text.toString();
        }
        if (ad instanceof AssetCard card) {
            return card.description() + " " + card.asset() + " " + card.uri()
                    + " " + card.shape();
        }
        if (ad instanceof CapabilityAdvertisement cap) {
            return cap.capabilityType() + " " + cap.binding() + " " + cap.parameters();
        }
        if (ad instanceof SpaceAdvertisement space) {
            return space.spaceName() + " " + String.join(" ", space.schemaHints());
        }
        return ad.id();
    }
}
