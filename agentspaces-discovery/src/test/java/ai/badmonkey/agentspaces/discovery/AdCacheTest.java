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
import ai.badmonkey.agentspaces.api.ad.SignedAdvertisement;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.AdvertisementSigner;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The ad-cache on its own (spec §6.2, §6.3; security findings ASF-019 and
 * ASF-027): read-time TTL eviction on every read path, verification before
 * admission, bounded skew and TTL, and a bounded size that a flood of fresh
 * ids cannot use to evict legitimate live advertisements.
 */
class AdCacheTest {

    private static final int MAX_ENTRIES = AdCache.DEFAULT_MAX_ENTRIES; // ASF-019

    private final TestClock clock = TestClock.create();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final AdvertisementSigner signer = new AdvertisementSigner();
    private final PeerIdentity issuer = PeerIdentity.generate();
    private final PeerIdentity other = PeerIdentity.generate();
    private final GroupId groupId = GroupId.of("zCache");
    private final AdCache cache = new AdCache(codec, clock);

    // ------------------------------------------------------------ §6.2 eviction

    /** SPEC §6.2: expired ads leave every read path: find, size, storedAds, digest, and deltaFor. */
    @Test
    void expiredAdsLeaveEveryReadPathIncludingDeltasAndDigests() {
        AgentCard longLived = card("long", clock.instant(), Duration.ofMinutes(15));
        AgentCard shortLived = card("short", clock.instant(), Duration.ofMinutes(1));
        assertThat(cache.accept(stored(longLived, issuer))).isPresent();
        assertThat(cache.accept(stored(shortLived, issuer))).isPresent();
        assertThat(cache.size()).isEqualTo(2);

        clock.advance(Duration.ofMinutes(2));

        assertThat(cache.size()).isEqualTo(1);
        assertThat(cache.find(AgentCard.class, c -> true))
                .singleElement().extracting(AgentCard::id).isEqualTo(longLived.id());
        assertThat(cache.storedAds()).hasSize(1);
        String digestKeys = new String(cache.digest(), 32, cache.digest().length - 32,
                StandardCharsets.UTF_8);
        assertThat(digestKeys).contains(longLived.id() + "=").doesNotContain(shortLived.id());

        AdCache.Delta delta = codec.fromBytes(cache.deltaFor(new byte[0]), AdCache.Delta.class);
        assertThat(delta.ads()).as("MUST NOT forward expired: the delta omits it").hasSize(1);
        assertThat(codec.fromBytes(delta.ads().get(0).adBytes(), AgentCard.class).id())
                .isEqualTo(longLived.id());

        AdCache receiver = new AdCache(codec, clock);
        receiver.applyDelta(cache.deltaFor(new byte[0]));
        assertThat(receiver.size()).isEqualTo(1);
    }

    /** SPEC §6.2: an ad that is already past issued + ttl is refused on arrival. */
    @Test
    void anAlreadyExpiredAdIsRefusedOnArrival() {
        AgentCard stale = card("stale", clock.instant().minus(Duration.ofMinutes(16)),
                Duration.ofMinutes(15));

        assertThat(cache.accept(stored(stale, issuer))).isEmpty();
        assertThat(cache.size()).isZero();
    }

    // ------------------------------------------------------------ §6.1 verification

    /** SPEC §6.1/TECH-SPEC §6.2: a carried key that does not hash to the issuer is refused. */
    @Test
    void aKeyThatDoesNotHashToTheIssuerIsRefused() {
        AdCache.StoredAd good = stored(card("x", clock.instant(), Duration.ofMinutes(15)), issuer);
        AdCache.StoredAd foreignKey = new AdCache.StoredAd(good.adType(), good.adBytes(),
                other.rawPublicKey(), good.signature());

        assertThat(cache.accept(foreignKey)).isEmpty();
        assertThat(cache.size()).isZero();
        assertThat(cache.accept(good)).isPresent();
    }

    /** SPEC §6.1: a signature by another key over the issuer's bytes is refused, whichever key travels. */
    @Test
    void aSignatureByAnotherKeyIsRefused() {
        AdCache.StoredAd good = stored(card("x", clock.instant(), Duration.ofMinutes(15)), issuer);
        byte[] forgedSignature = other.sign(good.adBytes());

        assertThat(cache.accept(new AdCache.StoredAd(good.adType(), good.adBytes(),
                good.publicKey(), forgedSignature))).isEmpty();
        assertThat(cache.accept(new AdCache.StoredAd(good.adType(), good.adBytes(),
                other.rawPublicKey(), forgedSignature))).isEmpty();
        assertThat(cache.size()).isZero();
    }

    /** SPEC §6.1: malformed wire forms (bad key length, unknown or mismatched type tag, nulls) are refused. */
    @Test
    void malformedWireFormsAreRefused() {
        AdCache.StoredAd good = stored(card("x", clock.instant(), Duration.ofMinutes(15)), issuer);

        assertThat(cache.accept(null)).isEmpty();
        assertThat(cache.accept(new AdCache.StoredAd(null, good.adBytes(), good.publicKey(),
                good.signature()))).isEmpty();
        assertThat(cache.accept(new AdCache.StoredAd(good.adType(), good.adBytes(),
                new byte[31], good.signature()))).isEmpty();
        assertThat(cache.accept(new AdCache.StoredAd("Bogus", good.adBytes(), good.publicKey(),
                good.signature()))).isEmpty();
        assertThat(cache.accept(new AdCache.StoredAd("RevocationAdvertisement", good.adBytes(),
                good.publicKey(), good.signature()))).isEmpty();
        assertThat(cache.accept(new AdCache.StoredAd("AssetCard", good.adBytes(),
                good.publicKey(), good.signature()))).isEmpty();
        assertThat(cache.size()).isZero();
    }

    // ------------------------------------------------------------ ASF-027 skew + TTL cap

    /** ASF-027: an issued stamp more than 10 minutes in the future is refused; within it is honest skew. */
    @Test
    void futureIssuedAdsBeyondTheSkewAreRefused() {
        assertThat(cache.accept(stored(card("far", clock.instant().plus(Duration.ofMinutes(11)),
                Duration.ofMinutes(15)), issuer))).isEmpty();
        assertThat(cache.accept(stored(card("near", clock.instant().plus(Duration.ofMinutes(9)),
                Duration.ofMinutes(15)), issuer))).isPresent();
        assertThat(cache.size()).isEqualTo(1);
    }

    /** ASF-027: a TTL beyond 30 days is refused; exactly 30 days is the ceiling. */
    /** SPEC §6.1b: an advertisement carrying an enum value this peer does not know is undecodable and dropped. */
    @Test
    void anAdvertisementWithAnUnknownEnumValueIsRefusedNotDefaulted() {
        // The same canonical shape a SpaceAdvertisement encodes to (indefinite map,
        // declared field order), but with an admission rule minted by a future
        // revision. Signed by a genuine issuer so only the decode can refuse it.
        Map<String, Object> future = new java.util.LinkedHashMap<>();
        future.put("id", "aspace://" + groupId.value() + "/tasks");
        future.put("issuer", issuer.peerId().value());
        future.put("group", groupId.value());
        future.put("issued", clock.instant().toString());
        future.put("ttl", "PT15M");
        future.put("spaceName", "tasks");
        future.put("schemaHints", List.of());
        future.put("strategy", "LEASE_RACE");
        future.put("admission", "QUANTUM_ENTANGLED");
        future.put("replication", "FULL");
        byte[] bytes = codec.toBytes(future);
        AdCache.StoredAd stored = new AdCache.StoredAd("SpaceAdvertisement", bytes,
                issuer.rawPublicKey(), issuer.sign(bytes));

        assertThat(cache.accept(stored)).as("unknown admission value").isEmpty();
        assertThat(cache.size()).isZero();

        // Control: the identical document with a known value is accepted.
        future.put("admission", "ALLOWLIST");
        byte[] known = codec.toBytes(future);
        assertThat(cache.accept(new AdCache.StoredAd("SpaceAdvertisement", known,
                issuer.rawPublicKey(), issuer.sign(known)))).isPresent();
        assertThat(cache.size()).isEqualTo(1);
    }

    @Test
    void ttlsBeyondTheCapAreRefused() {
        assertThat(cache.accept(stored(card("long", clock.instant(), Duration.ofDays(31)),
                issuer))).isEmpty();
        assertThat(cache.accept(stored(card("cap", clock.instant(), Duration.ofDays(30)),
                issuer))).isPresent();
        assertThat(cache.size()).isEqualTo(1);
    }

    /** ASF-027: a far-future reissue cannot pin an ad as permanently newest in last-writer-wins. */
    @Test
    void aFarFutureReissueCannotPinAnHonestAd() {
        Instant honestIssued = clock.instant();
        AgentCard honest = card("x", honestIssued, Duration.ofMinutes(15));
        assertThat(cache.accept(stored(honest, issuer))).isPresent();

        AgentCard pinned = card("x", clock.instant().plus(Duration.ofMinutes(11)),
                Duration.ofMinutes(15));
        assertThat(cache.accept(stored(pinned, issuer))).isEmpty();

        assertThat(cache.find(AgentCard.class, c -> true))
                .singleElement().extracting(AgentCard::issued).isEqualTo(honestIssued);
    }

    // ------------------------------------------------------------ ASF-019 bounded cache

    /** ASF-019: once full, new ids are refused while updates land; expiry frees room again. */
    @Test
    @Timeout(120)
    void aFloodOfNewIdsCannotEvictLiveAdvertisements() {
        Instant issued = clock.instant();
        for (int i = 0; i < MAX_ENTRIES; i++) {
            assertThat(cache.accept(stored(card("flood-" + i, issued, Duration.ofMinutes(15)),
                    issuer))).isPresent();
        }
        assertThat(cache.size()).isEqualTo(MAX_ENTRIES);

        assertThat(cache.accept(stored(card("one-too-many", issued, Duration.ofMinutes(15)),
                issuer))).as("a brand-new id is refused while the cache is full").isEmpty();
        assertThat(cache.size()).isEqualTo(MAX_ENTRIES);

        clock.advance(Duration.ofMinutes(1));
        Instant refreshed = clock.instant();
        assertThat(cache.accept(stored(card("flood-0", refreshed, Duration.ofMinutes(15)),
                issuer))).as("a refresh of a known id always lands").isPresent();
        assertThat(cache.find(AgentCard.class, c -> c.id().endsWith("/flood-0")))
                .singleElement().extracting(AgentCard::issued).isEqualTo(refreshed);

        clock.advance(Duration.ofMinutes(14)); // 15 minutes on: everything but the refresh has expired
        assertThat(cache.accept(stored(card("newcomer", clock.instant(), Duration.ofMinutes(15)),
                issuer))).as("expiry frees room for new ids").isPresent();
        assertThat(cache.size()).isEqualTo(2);
    }

    /** SPEC §5.4 / ASF-019: the cache bound is configurable; the default stays 8,192 and a smaller bound refuses new ids at its own limit. */
    @Test
    void theCacheBoundIsConfigurable() {
        assertThat(cache.maxEntries()).isEqualTo(8_192);
        assertThat(new AdCache(codec, clock, 4 * AdCache.DEFAULT_MAX_ENTRIES).maxEntries())
                .isEqualTo(32_768);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new AdCache(codec, clock, 0))
                .isInstanceOf(IllegalArgumentException.class);

        AdCache small = new AdCache(codec, clock, 2);
        Instant issued = clock.instant();
        assertThat(small.accept(stored(card("one", issued, Duration.ofMinutes(15)), issuer)))
                .isPresent();
        assertThat(small.accept(stored(card("two", issued, Duration.ofMinutes(15)), issuer)))
                .isPresent();
        assertThat(small.accept(stored(card("three", issued, Duration.ofMinutes(15)), issuer)))
                .as("a brand-new id is refused at the configured bound").isEmpty();
        assertThat(small.size()).isEqualTo(2);
        assertThat(small.accept(stored(card("one", issued.plusSeconds(1),
                Duration.ofMinutes(15)), issuer))).as("a refresh of a known id lands").isPresent();
    }

    // ------------------------------------------------------------ helpers

    /** ASF-047: a revoked issuer's cached advertisements are purged, and its advertisements are refused from then on. */
    @Test
    void aRevokedIssuersAdvertisementsArePurgedAndRefused() {
        assertThat(cache.accept(stored(card("researcher", clock.instant(), Duration.ofMinutes(10)),
                issuer))).isPresent();
        java.util.Set<ai.badmonkey.agentspaces.common.id.PeerId> revoked =
                java.util.concurrent.ConcurrentHashMap.newKeySet();
        cache.refuseIssuers(revoked::contains);

        revoked.add(issuer.peerId());
        assertThat(cache.purgeIssuer(issuer.peerId())).isEqualTo(1);
        assertThat(cache.find(AgentCard.class, c -> true)).isEmpty();
        assertThat(cache.accept(stored(card("researcher", clock.instant(), Duration.ofMinutes(10)),
                issuer))).as("refused from now on").isEmpty();
    }

    /** SPEC §6.1 v0.1.13 (TODO-9-10-11 B6): a card carrying its agent's certificate is admitted only when the certificate verifies under the card issuer's key and covers the card's issue time; a card cannot be built with a certificate for another key. */
    @Test
    void aCardsAgentCertificateMustProveItself() {
        var agent = issuer.subordinate("researcher", clock.instant(), Duration.ofHours(1));
        AgentCard keyed = new AgentCard("aspace://" + groupId.value() + "/agent/researcher",
                issuer.peerId(), groupId, clock.instant(), Duration.ofMinutes(10),
                issuer.agent("researcher"), "Researches topics", List.of("research"),
                List.of("ResearchTask#v1"), List.of("Finding#v1"), Map.of(), Map.of(),
                agent.publicKey()).withAgentCertificate(agent.certificate().orElseThrow());
        assertThat(cache.accept(stored(keyed, issuer))).as("a proven attestation").isPresent();

        var foreign = other.subordinate("researcher", clock.instant(), Duration.ofHours(1));
        AgentCard misattributed = new AgentCard("aspace://" + groupId.value() + "/agent/other",
                other.peerId(), groupId, clock.instant(), Duration.ofMinutes(10),
                other.agent("researcher"), "Researches topics", List.of("research"),
                List.of("ResearchTask#v1"), List.of("Finding#v1"), Map.of(), Map.of(),
                foreign.publicKey()).withAgentCertificate(foreign.certificate().orElseThrow());
        assertThat(cache.accept(stored(misattributed, other))).isPresent();

        // A certificate issued after the card's issue time does not cover it.
        clock.advance(Duration.ofMinutes(1));
        var later = issuer.subordinate("late", clock.instant(), Duration.ofHours(1));
        AgentCard backdated = new AgentCard("aspace://" + groupId.value() + "/agent/late",
                issuer.peerId(), groupId, clock.instant().minus(Duration.ofSeconds(30)),
                Duration.ofMinutes(10), issuer.agent("late"), "x", List.of(), List.of(), List.of(),
                Map.of(), Map.of(), later.publicKey()).withAgentCertificate(later.certificate().orElseThrow());
        assertThat(cache.accept(stored(backdated, issuer))).as("certificate not covering the card").isEmpty();

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> keyed.withAgentCertificate(
                        foreign.certificate().orElseThrow()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** SPEC §6.1 v0.1.13: a revoked agent's card is refused on arrival and purged once held; its sibling's stays. */
    @Test
    void aRevokedAgentsCardsAreRefusedAndPurged() {
        java.util.Set<ai.badmonkey.agentspaces.common.id.AgentId> revoked =
                java.util.concurrent.ConcurrentHashMap.newKeySet();
        cache.refuseAgents(new ai.badmonkey.agentspaces.api.security.RevocationView() {
            @Override
            public boolean revoked(ai.badmonkey.agentspaces.common.id.PeerId peer) {
                return false;
            }

            @Override
            public boolean refuses(ai.badmonkey.agentspaces.common.id.AgentId agent, byte[] agentKey,
                                   Instant signingTime) {
                return revoked.contains(agent);
            }
        });
        assertThat(cache.accept(stored(card("planner", clock.instant(), Duration.ofMinutes(15)), issuer)))
                .isPresent();
        assertThat(cache.accept(stored(card("sibling", clock.instant(), Duration.ofMinutes(15)), issuer)))
                .isPresent();

        revoked.add(issuer.agent("planner"));
        assertThat(cache.purgeAgent(issuer.agent("planner"))).isEqualTo(1);
        clock.advance(Duration.ofSeconds(1));
        assertThat(cache.accept(stored(card("planner", clock.instant(), Duration.ofMinutes(15)), issuer)))
                .as("a fresh card of the revoked agent is refused").isEmpty();
        assertThat(cache.find(AgentCard.class, c -> true)).extracting(c -> c.agent().localName())
                .containsExactly("sibling");
    }

    private AgentCard card(String localName, Instant issued, Duration ttl) {
        return new AgentCard("aspace://" + groupId.value() + "/agent/" + localName,
                issuer.peerId(), groupId, issued, ttl, issuer.agent(localName),
                "Researches topics", List.of("research"), List.of("ResearchTask#v1"),
                List.of("Finding#v1"), Map.of());
    }

    private <A extends Advertisement> AdCache.StoredAd stored(A ad, PeerIdentity identity) {
        SignedAdvertisement<A> signed = signer.sign(ad, identity);
        return cache.toStored(signed.advertisement(), signed.issuerPublicKey(),
                signed.signature());
    }
}
