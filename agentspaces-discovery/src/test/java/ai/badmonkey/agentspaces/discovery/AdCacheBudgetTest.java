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

import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.ad.SignedAdvertisement;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.AdvertisementSigner;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ISSUE-AppCardOriginFormats FR-11 (SPEC §6.3; AppCard origin and formats, 2026-11-09): every advertisement cache
 * bounds its entries by count and by bytes, the sum of the cached advertisements' encodings. A full cache refuses a new
 * id, and a newer version that would pass the byte bound while it keeps the version it holds; it never evicts a live
 * advertisement to make room.
 */
class AdCacheBudgetTest {

    private final TestClock clock = TestClock.create();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final AdvertisementSigner signer = new AdvertisementSigner();
    private final PeerIdentity issuer = PeerIdentity.generate();
    private final PeerIdentity other = PeerIdentity.generate();
    private final GroupId groupId = GroupId.of("zBudget");

    private AgentCard card(PeerIdentity who, String localName, Instant issued, Duration ttl, int padding) {
        return new AgentCard("aspace://" + groupId.value() + "/agent/" + who.peerId().value() + "/" + localName,
                who.peerId(), groupId, issued, ttl, who.agent(localName), "x".repeat(padding), List.of("research"),
                List.of("ResearchTask#v1"), List.of("Finding#v1"), Map.of());
    }

    private AdCache.StoredAd stored(AdCache cache, AgentCard ad, PeerIdentity who) {
        SignedAdvertisement<AgentCard> signed = signer.sign(ad, who);
        return cache.toStored(signed.advertisement(), signed.issuerPublicKey(), signed.signature());
    }

    private int size(AgentCard card) {
        return codec.toBytes(card).length;
    }

    @Test
    void theDefaultsAndTheRendezvousMultiplier() {
        AdCache cache = new AdCache(codec, clock);
        assertThat(cache.maxEntries()).isEqualTo(8_192);
        assertThat(cache.maxBytes()).isEqualTo(AdCache.DEFAULT_MAX_BYTES).isEqualTo(33_554_432L);
        assertThat(new AdCache(codec, clock, 10).maxBytes()).isEqualTo(AdCache.DEFAULT_MAX_BYTES);
        assertThat(cache.bytes()).isZero();
        assertThat(DiscoveryService.cacheBytesFor(Set.of())).isEqualTo(AdCache.DEFAULT_MAX_BYTES);
        assertThat(DiscoveryService.cacheBytesFor(Set.of(PeerAdvertisement.PeerRole.RENDEZVOUS)))
                .isEqualTo(4 * AdCache.DEFAULT_MAX_BYTES);
        assertThat(DiscoveryService.cacheBoundFor(Set.of(PeerAdvertisement.PeerRole.RENDEZVOUS))).isEqualTo(4 * 8_192);
    }

    @Test
    void aNewIdIsRefusedAtTheByteBudgetAfterExpiredEntriesLeave() {
        AgentCard first = card(issuer, "first", clock.instant(), Duration.ofMinutes(1), 400);
        // PT1M and PT5M encode at one length, so the third card is the first card's size
        AgentCard second = card(issuer, "second", clock.instant(), Duration.ofMinutes(5), 400);
        AgentCard third = card(issuer, "third", clock.instant(), Duration.ofMinutes(5), 400);
        AdCache cache = new AdCache(codec, clock, 100, size(first) + size(second));
        assertThat(cache.accept(stored(cache, first, issuer))).isPresent();
        assertThat(cache.accept(stored(cache, second, issuer))).isPresent();
        assertThat(cache.bytes()).isEqualTo(size(first) + size(second));
        assertThat(cache.accept(stored(cache, third, issuer))).as("the budget is full").isEmpty();
        assertThat(cache.adsRefusedCacheFull()).isEqualTo(1);
        assertThat(cache.find(AgentCard.class, c -> true)).extracting(AgentCard::id).containsExactlyInAnyOrder(first.id(), second.id());

        clock.advance(Duration.ofMinutes(2));
        assertThat(cache.accept(stored(cache, third, issuer))).as("the expired entry left room").isPresent();
        assertThat(cache.bytes()).isEqualTo(size(second) + size(third));
        assertThat(cache.adsRefusedCacheFull()).isEqualTo(1);
    }

    @Test
    void aNewIdIsRefusedAtTheEntryBound() {
        AdCache cache = new AdCache(codec, clock, 2, AdCache.DEFAULT_MAX_BYTES);
        assertThat(cache.accept(stored(cache, card(issuer, "a", clock.instant(), Duration.ofMinutes(15), 1), issuer))).isPresent();
        assertThat(cache.accept(stored(cache, card(issuer, "b", clock.instant(), Duration.ofMinutes(15), 1), issuer))).isPresent();
        assertThat(cache.accept(stored(cache, card(issuer, "c", clock.instant(), Duration.ofMinutes(15), 1), issuer))).isEmpty();
        assertThat(cache.adsRefusedCacheFull()).isEqualTo(1);
    }

    @Test
    void aGrowingVersionThatWouldPassTheBudgetIsRefusedAndTheOldOneKept() {
        AgentCard small = card(issuer, "grow", clock.instant(), Duration.ofMinutes(15), 300);
        AdCache cache = new AdCache(codec, clock, 100, size(small) + 200);
        assertThat(cache.accept(stored(cache, small, issuer))).isPresent();
        clock.advance(Duration.ofSeconds(1));
        AgentCard fits = card(issuer, "grow", clock.instant(), Duration.ofMinutes(15), 500);
        assertThat(size(fits)).isEqualTo(size(small) + 200);
        assertThat(cache.accept(stored(cache, fits, issuer))).as("a version that fits replaces").isPresent();
        assertThat(cache.bytes()).isEqualTo(size(fits));
        clock.advance(Duration.ofSeconds(1));
        AgentCard tooBig = card(issuer, "grow", clock.instant(), Duration.ofMinutes(15), 501);
        assertThat(cache.accept(stored(cache, tooBig, issuer))).isEmpty();
        assertThat(cache.adsRefusedCacheFull()).isEqualTo(1);
        assertThat(cache.find(AgentCard.class, c -> true)).singleElement().extracting(AgentCard::issued).isEqualTo(fits.issued());
        assertThat(cache.bytes()).isEqualTo(size(fits));
    }

    @Test
    void anAdLargerThanTheWholeBudgetNeverEnters() {
        AgentCard huge = card(issuer, "huge", clock.instant(), Duration.ofMinutes(15), 2_000);
        AdCache cache = new AdCache(codec, clock, 100, 1_000);
        assertThat(cache.accept(stored(cache, huge, issuer))).isEmpty();
        assertThat(cache.bytes()).isZero();
        assertThat(cache.size()).isZero();
    }

    @Test
    void theByteSumFollowsEveryRemoval() {
        AdCache cache = new AdCache(codec, clock);
        AgentCard mine = card(issuer, "mine", clock.instant(), Duration.ofMinutes(15), 50);
        AgentCard theirs = card(other, "theirs", clock.instant(), Duration.ofMinutes(15), 60);
        AgentCard brief = card(other, "brief", clock.instant(), Duration.ofMinutes(1), 70);
        cache.accept(stored(cache, mine, issuer));
        cache.accept(stored(cache, theirs, other));
        cache.accept(stored(cache, brief, other));
        assertThat(cache.bytes()).isEqualTo(size(mine) + size(theirs) + size(brief));
        clock.advance(Duration.ofMinutes(2));
        assertThat(cache.size()).isEqualTo(2);
        assertThat(cache.bytes()).as("expiry").isEqualTo(size(mine) + size(theirs));
        assertThat(cache.purgeAgent(theirs.agent())).isEqualTo(1);
        assertThat(cache.bytes()).as("purgeAgent").isEqualTo(size(mine));
        assertThat(cache.purgeIssuer(issuer.peerId())).isEqualTo(1);
        assertThat(cache.bytes()).as("purgeIssuer").isZero();
        assertThat(cache.adsRefusedCacheFull()).isZero();
    }
}
