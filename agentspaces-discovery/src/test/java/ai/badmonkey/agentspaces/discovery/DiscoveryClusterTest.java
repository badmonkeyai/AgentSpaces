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

import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.RevocationAdvertisement;
import ai.badmonkey.agentspaces.api.ad.SignedAdvertisement;
import ai.badmonkey.agentspaces.api.ad.SpaceAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.AdvertisementSigner;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DiscoveryClusterTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final AdvertisementSigner signer = new AdvertisementSigner();
    private final GroupId groupId = GroupId.of("zDiscoGroup");
    /** The group's founder: the trust root revocations must be issued by. */
    private final PeerIdentity founder = PeerIdentity.generate();
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zDiscoGroup", founder.peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "disco",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Wired(PeerNode node, PeerIdentity identity, GroupRuntime runtime,
                         DiscoveryService discovery) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Wired newPeer(String address, long seed, String... seedAddresses) throws IOException {
        return newPeer(address, seed, PeerIdentity.generate(), seedAddresses);
    }

    private Wired newPeer(String address, long seed, PeerIdentity identity,
                          String... seedAddresses) throws IOException {
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        DiscoveryService discovery = new DiscoveryService(
                runtime, new AdCache(codec, clock), codec, identity.peerId());
        nodes.add(node);
        return new Wired(node, identity, runtime, discovery);
    }

    private SignedAdvertisement<AgentCard> researcherCard(PeerIdentity identity) {
        AgentCard card = new AgentCard(
                "aspace://" + groupId.value() + "/agent/" + identity.peerId().value(),
                identity.peerId(), groupId, clock.instant(), Duration.ofMinutes(15),
                identity.agent("researcher"), "Researches topics from the shared task space",
                List.of("research"), List.of("ResearchTask#v1"), List.of("Finding#v1"),
                Map.of());
        return signer.sign(card, identity);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
        }
    }

    @Test
    void publishedCardsReachEveryCacheByRumor() throws Exception {
        Wired a = newPeer("a", 1);
        Wired b = newPeer("b", 2, "a");
        Wired c = newPeer("c", 3, "a");
        tickAll(4);

        a.discovery().publish(researcherCard(a.identity()));

        assertThat(b.discovery().find(AgentCard.class, card -> true)).hasSize(1);
        assertThat(c.discovery().find(AgentCard.class,
                card -> card.produces().contains("Finding#v1"))).hasSize(1);
    }

    @Test
    void lateJoinerFindsCardsThroughRemoteQuery() throws Exception {
        Wired a = newPeer("a", 1);
        Wired b = newPeer("b", 2, "a");
        tickAll(3);
        a.discovery().publish(researcherCard(a.identity()));

        // C joins after the rumor is over, seeding on A so its join
        // self-introduction admits it into A's view (the data plane serves
        // members only, ASF-003). To isolate the QUERY path from anti-entropy,
        // C learns A's address directly and no ticks run.
        Wired c = newPeer("c", 3, "a");
        c.runtime().membership().onPeerAdvertisement(new PeerAdvertisement(
                "aspace://" + groupId.value() + "/peer/" + a.identity().peerId().value(),
                a.identity().peerId(), groupId, clock.instant(), Duration.ofMinutes(10),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)),
                java.util.Set.of(), Map.of()));
        assertThat(c.discovery().find(AgentCard.class, card -> true)).isEmpty();

        List<AgentCard> found = c.discovery().remoteFind(AgentCard.class,
                Map.of("description", "Researches topics from the shared task space"),
                Duration.ofSeconds(2));

        assertThat(found).hasSize(1);
        assertThat(found.get(0).agent().localName()).isEqualTo("researcher");
    }

    @Test
    void lateJoinerAlsoConvergesThroughAntiEntropy() throws Exception {
        Wired a = newPeer("a", 1);
        tickAll(1);
        a.discovery().publish(researcherCard(a.identity()));

        Wired b = newPeer("b", 2, "a");
        tickAll(6); // anti-entropy digests flow; deltas fill B's cache

        assertThat(b.discovery().find(AgentCard.class, card -> true)).hasSize(1);
    }

    @Test
    void expiredCardsAgeOutOfTheCache() throws Exception {
        Wired a = newPeer("a", 1);
        Wired b = newPeer("b", 2, "a");
        tickAll(3);
        a.discovery().publish(researcherCard(a.identity()));
        assertThat(b.discovery().find(AgentCard.class, card -> true)).hasSize(1);

        clock.advance(Duration.ofMinutes(16)); // beyond the 15-minute TTL

        assertThat(b.discovery().find(AgentCard.class, card -> true)).isEmpty();
    }

    @Test
    void tamperedAdsAreRejectedByEveryCache() throws Exception {
        Wired a = newPeer("a", 1);
        AdCache cache = a.discovery().cache();
        SignedAdvertisement<AgentCard> signed = researcherCard(a.identity());

        AdCache.StoredAd good = cache.toStored(signed.advertisement(),
                signed.issuerPublicKey(), signed.signature());
        byte[] tamperedBytes = good.adBytes().clone();
        tamperedBytes[tamperedBytes.length / 2] ^= 1;
        AdCache.StoredAd tampered = new AdCache.StoredAd(good.adType(), tamperedBytes,
                good.publicKey(), good.signature());

        assertThat(cache.accept(tampered)).isEmpty();
        assertThat(cache.accept(good)).isPresent();
    }

    // ------------------------------------------------------------ §6.2 MUST NOT forward

    /** SPEC §6.2: caches MUST NOT forward expired (or unverifiable) ads, on the rumor path too. */
    @Test
    void expiredOrUnverifiableAdsAreNeverForwarded() throws Exception {
        Wired a = newPeer("a", 1);
        Wired b = newPeer("b", 2, "a");
        Wired c = newPeer("c", 3, "b");
        tickAll(4);

        // C observes the raw ads stream, so we see exactly what B re-forwards.
        List<byte[]> reachedC = new CopyOnWriteArrayList<>();
        c.runtime().gossip().replaceHandler(DiscoveryService.ADS_STREAM,
                (from, itemId, payload) -> reachedC.add(payload));

        AdCache cache = a.discovery().cache();
        SignedAdvertisement<AgentCard> shortLived = signer.sign(
                researcherCard(a.identity(), clock.instant(), Duration.ofSeconds(30)),
                a.identity());
        AdCache.StoredAd expired = cache.toStored(shortLived.advertisement(),
                shortLived.issuerPublicKey(), shortLived.signature());
        SignedAdvertisement<AgentCard> fresh = researcherCard(a.identity());
        AdCache.StoredAd good = cache.toStored(fresh.advertisement(),
                fresh.issuerPublicKey(), fresh.signature());
        byte[] tamperedBytes = good.adBytes().clone();
        tamperedBytes[tamperedBytes.length / 2] ^= 1;
        AdCache.StoredAd tampered = new AdCache.StoredAd(good.adType(), tamperedBytes,
                good.publicKey(), good.signature());
        AdCache.StoredAd mistyped = new AdCache.StoredAd("Bogus", good.adBytes(),
                good.publicKey(), good.signature());

        clock.advance(Duration.ofSeconds(31)); // the short-lived card is expired on arrival

        inject(b, "expired", expired);
        inject(b, "tampered", tampered);
        inject(b, "mistyped", mistyped);
        assertThat(b.discovery().find(AgentCard.class, card -> true)).isEmpty();
        assertThat(reachedC).as("B never re-forwards what its own cache rejected").isEmpty();

        inject(b, "good", good);
        assertThat(b.discovery().find(AgentCard.class, card -> true)).hasSize(1);
        assertThat(reachedC).as("a verified, unexpired ad still travels").hasSize(1);
    }

    /** SPEC §6.2: an issuer's own publish gossips nothing its own cache refuses. */
    @Test
    void publishSkipsGossipWhenItsOwnCacheRejectsTheAd() throws Exception {
        Wired a = newPeer("a", 1);
        Wired b = newPeer("b", 2, "a");
        tickAll(3);
        List<byte[]> reachedB = new CopyOnWriteArrayList<>();
        b.runtime().gossip().replaceHandler(DiscoveryService.ADS_STREAM,
                (from, itemId, payload) -> reachedB.add(payload));

        AgentCard alreadyExpired = researcherCard(a.identity(),
                clock.instant().minus(Duration.ofMinutes(16)), Duration.ofMinutes(15));
        AgentCard overlongTtl = researcherCard(a.identity(), clock.instant(), Duration.ofDays(31));
        a.discovery().publish(signer.sign(alreadyExpired, a.identity()));
        a.discovery().publish(signer.sign(overlongTtl, a.identity()));

        assertThat(a.discovery().find(AgentCard.class, card -> true)).isEmpty();
        assertThat(reachedB).as("nothing refused locally reaches the wire").isEmpty();

        a.discovery().publish(researcherCard(a.identity()));
        assertThat(reachedB).hasSize(1);
    }

    // ------------------------------------------------------------ §6.2 refresh + LWW

    /** SPEC §6.2: re-publishing with a new issued extends life; an older reissue never wins. */
    @Test
    void refreshedCardsOutliveTheirOriginalTtlAndOlderReissuesAreIgnored() throws Exception {
        Wired a = newPeer("a", 1);
        Wired b = newPeer("b", 2, "a");
        tickAll(3);

        SignedAdvertisement<AgentCard> original = researcherCard(a.identity());
        a.discovery().publish(original);
        AdCache.StoredAd originalStored = b.discovery().cache().toStored(
                original.advertisement(), original.issuerPublicKey(), original.signature());

        clock.advance(Duration.ofMinutes(10));
        Instant refreshedAt = clock.instant();
        a.discovery().publish(researcherCard(a.identity())); // same id, newer issued

        clock.advance(Duration.ofMinutes(2)); // the original is still unexpired here
        // Characterization: accept reports the candidate even when LWW keeps the newer entry.
        assertThat(b.discovery().cache().accept(originalStored)).isPresent();
        assertThat(b.discovery().find(AgentCard.class, card -> true))
                .singleElement().extracting(AgentCard::issued).isEqualTo(refreshedAt);

        clock.advance(Duration.ofMinutes(8)); // 20 minutes after the original: it would be gone
        assertThat(b.discovery().find(AgentCard.class, card -> true))
                .singleElement().extracting(AgentCard::issued).isEqualTo(refreshedAt);
    }

    // ------------------------------------------------------------ §6.3 query semantics

    /** SPEC §6.3 (characterization): a peer never answers its own query when it comes back around. */
    @Test
    void aPeerNeverAnswersItsOwnQuery() throws Exception {
        Wired a = newPeer("a", 1);
        Wired b = newPeer("b", 2, "a");
        tickAll(3);
        a.discovery().publish(researcherCard(a.identity()));

        List<byte[]> queries = new CopyOnWriteArrayList<>();
        b.runtime().gossip().replaceHandler(DiscoveryService.QUERY_STREAM,
                (from, itemId, payload) -> queries.add(payload)); // B records, never answers
        AtomicInteger hitsAtA = new AtomicInteger();
        a.runtime().onKind(Envelope.Kind.QUERY_HIT, (from, body) -> hitsAtA.incrementAndGet());

        assertThat(a.discovery().remoteFind(AgentCard.class,
                Map.of("description", "Researches topics from the shared task space"),
                Duration.ZERO)).as("local-first: the asker's own cache answers").hasSize(1);
        assertThat(queries).hasSize(1);

        // A's own query rumor returns to A, bypassing the seen-cache.
        a.runtime().gossip().onBootstrapRumor(b.node().peerId(),
                new Bodies.Rumor(DiscoveryService.QUERY_STREAM, "replay", 6, queries.get(0)));

        assertThat(hitsAtA).hasValue(0);
    }

    /** SPEC §6.3: an unknown field in remoteFind fails fast with IllegalArgumentException, before the wait and before any query reaches the wire. */
    @Test
    void remoteFindWithAnUnknownFieldFailsWithIllegalArgument() throws Exception {
        Wired a = newPeer("a", 1);
        Wired b = newPeer("b", 2, "a");
        tickAll(3);
        a.discovery().publish(researcherCard(a.identity()));

        // A records every scoped query it receives instead of answering it.
        List<byte[]> queriesAtA = new CopyOnWriteArrayList<>();
        a.runtime().gossip().replaceHandler(DiscoveryService.QUERY_STREAM,
                (from, itemId, payload) -> queriesAtA.add(payload));
        AtomicInteger directQueriesAtA = new AtomicInteger();
        a.runtime().onKind(Envelope.Kind.QUERY, (from, body) -> directQueriesAtA.incrementAndGet());

        long started = System.nanoTime();
        assertThatThrownBy(() -> b.discovery().remoteFind(AgentCard.class,
                Map.of("noSuchField", "x"), Duration.ofSeconds(30)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("noSuchField");
        Duration elapsed = Duration.ofNanos(System.nanoTime() - started);

        assertThat(elapsed).as("validated before the wait").isLessThan(Duration.ofSeconds(5));
        assertThat(queriesAtA).as("no QUERY rumor was gossiped").isEmpty();
        assertThat(directQueriesAtA).as("no direct QUERY frame was sent").hasValue(0);
        // The responder side is untouched by the refused predicate.
        assertThat(a.discovery().find(AgentCard.class, card -> true)).hasSize(1);
    }

    /** SPEC §6.3: the query rumor crosses a peer running no discovery, and the hit routes directly to the asker. */
    @Test
    void queryRumorsCrossRelayPeersAndHitsRouteDirectlyToTheAsker() throws Exception {
        Wired a = newPeer("a", 1);
        newBareNode("b", 2, "a"); // joins the group but runs no DiscoveryService
        tickAll(3);
        a.discovery().publish(researcherCard(a.identity())); // B caches nothing, forwards to nobody

        network.partition("a", "c");
        Wired c = newPeer("c", 3, "b");
        // Membership converges through B. The clock moves so A's per-tick
        // self-advertisement is a new payload each time; otherwise B's dedup
        // swallows the copies it would forward to C. No ad state can reach C.
        for (int i = 0; i < 4; i++) {
            clock.advance(Duration.ofSeconds(1));
            tickAll(1);
        }
        assertThat(c.runtime().membership().member(a.identity().peerId())).isPresent();
        assertThat(c.discovery().find(AgentCard.class, card -> true)).isEmpty();

        // The query reaches A through B, but A's QUERY_HIT goes straight to C: blocked.
        assertThat(c.discovery().remoteFind(AgentCard.class,
                Map.of("description", "Researches topics from the shared task space"),
                Duration.ofMillis(300))).isEmpty();

        network.heal();
        List<AgentCard> found = c.discovery().remoteFind(AgentCard.class,
                Map.of("description", "Researches topics from the shared task space"),
                Duration.ofSeconds(2));
        assertThat(found).hasSize(1);
        assertThat(found.get(0).issuer()).isEqualTo(a.identity().peerId());
    }

    // ------------------------------------------------------------ §6.1 other ad types

    /** SPEC §6.1/§7.5: a SpaceAdvertisement round-trips the ad-cache and matches structurally. */
    @Test
    void spaceAdvertisementsRoundTripTheCache() throws Exception {
        Wired a = newPeer("a", 1);
        Wired b = newPeer("b", 2, "a");
        tickAll(3);

        SpaceAdvertisement tasks = new SpaceAdvertisement(
                "aspace://" + groupId.value() + "/space/tasks", a.identity().peerId(), groupId,
                clock.instant(), Duration.ofHours(24), "tasks", List.of("ResearchTask#v1"),
                ConflictStrategyType.LEASE_RACE);
        a.discovery().publish(signer.sign(tasks, a.identity()));

        List<SpaceAdvertisement> found = b.discovery().find(SpaceAdvertisement.class,
                ad -> ad.spaceName().equals("tasks"));
        assertThat(found).hasSize(1);
        assertThat(found.get(0).strategy()).isEqualTo(ConflictStrategyType.LEASE_RACE);
        assertThat(found.get(0).schemaHints()).containsExactly("ResearchTask#v1");
    }

    /** SPEC §7.5/§6.1: a space built with advertise(discovery::publish) publishes its SpaceAdvertisement on creation; another peer finds it by name with admission GROUP and replication FULL, and refreshAdvertisement keeps it alive past its TTL. */
    @Test
    void spacesAdvertiseThemselvesThroughDiscovery() throws Exception {
        Wired a = newPeer("a", 1);
        Wired b = newPeer("b", 2, "a");
        tickAll(3);

        try (ReplicatedSpace tasks = ReplicatedSpace.builder(a.runtime(), "tasks", a.identity(), "founder")
                .clock(clock).settleWindow(Duration.ZERO)
                .schemaHints(AgentCard.class)
                .advertise(a.discovery()::publish)
                .build()) {
            Instant created = clock.instant();
            List<SpaceAdvertisement> found = b.discovery().find(SpaceAdvertisement.class,
                    s -> s.spaceName().equals("tasks"));
            assertThat(found).hasSize(1);
            SpaceAdvertisement ad = found.get(0);
            assertThat(ad.issuer()).isEqualTo(a.identity().peerId());
            assertThat(ad.group()).isEqualTo(groupId);
            assertThat(ad.issued()).isEqualTo(created);
            assertThat(ad.admission()).isEqualTo(SpaceAdvertisement.Admission.GROUP);
            assertThat(ad.replication()).isEqualTo(SpaceAdvertisement.Replication.FULL);
            assertThat(ad.strategy()).isEqualTo(ConflictStrategyType.LEASE_RACE);
            assertThat(ad.schemaHints()).isNotEmpty().allMatch(h -> h.contains("AgentCard"));
            assertThat(tasks.advertisement()).map(SignedAdvertisement::advertisement).hasValue(ad);

            // The founder refreshes within the TTL; the refreshed ad outlives the original.
            clock.advance(ad.ttl().minus(Duration.ofMinutes(1)));
            assertThat(tasks.refreshAdvertisement()).isPresent();
            Instant refreshedAt = clock.instant();
            clock.advance(Duration.ofMinutes(2)); // the original would have expired here
            assertThat(b.discovery().find(SpaceAdvertisement.class, s -> s.spaceName().equals("tasks")))
                    .singleElement().extracting(SpaceAdvertisement::issued).isEqualTo(refreshedAt);

            // Unrefreshed, it lapses: letting the advertisement expire releases the space.
            clock.advance(ad.ttl());
            assertThat(b.discovery().find(SpaceAdvertisement.class, s -> true)).isEmpty();
        }
    }

    /** SPEC §6.1: a RevocationAdvertisement published through discovery is routed to the group's RevocationRegistry (not the ad cache) and travels the revocation stream to every node. */
    @Test
    void publishingARevocationThroughDiscoveryReachesTheRegistry() throws Exception {
        Wired a = newPeer("a", 1, founder);
        Wired b = newPeer("b", 2, "a");
        Wired victim = newPeer("c", 3, "a");
        tickAll(4);
        assertThat(a.runtime().membership().member(victim.identity().peerId())).isPresent();
        assertThat(b.runtime().membership().member(victim.identity().peerId())).isPresent();

        RevocationAdvertisement revocation = new RevocationAdvertisement(
                "aspace://" + groupId.value() + "/revocation/" + victim.identity().peerId().value(),
                founder.peerId(), groupId, clock.instant(), Duration.ofDays(1),
                victim.identity().peerId(), "compromised", null);
        a.discovery().publish(signer.sign(revocation, founder));

        // Routed to the registry, never the ad cache; enforced on the founder's node at once.
        assertThat(a.runtime().revocations().revoked(victim.identity().peerId())).isTrue();
        assertThat(a.discovery().cache().size()).isZero();
        assertThat(a.runtime().membership().member(victim.identity().peerId())).isEmpty();

        tickAll(4);
        assertThat(b.runtime().revocations().revoked(victim.identity().peerId())).isTrue();
        assertThat(b.runtime().membership().member(victim.identity().peerId())).isEmpty();
    }

    /** SPEC §6.1: a revocation whose issuer is not the trust root is refused by the registry and never gossiped. */
    @Test
    void anUnauthorizedRevocationThroughDiscoveryIsRefusedAndNotGossiped() throws Exception {
        Wired a = newPeer("a", 1);
        Wired b = newPeer("b", 2, "a");
        Wired victim = newPeer("c", 3, "a");
        tickAll(4);
        List<byte[]> revocationsAtB = new CopyOnWriteArrayList<>();
        b.runtime().gossip().replaceHandler(DiscoveryService.REVOCATION_STREAM,
                (from, itemId, payload) -> revocationsAtB.add(payload));

        RevocationAdvertisement forged = new RevocationAdvertisement(
                "aspace://" + groupId.value() + "/revocation/" + victim.identity().peerId().value(),
                a.identity().peerId(), groupId, clock.instant(), Duration.ofDays(1),
                victim.identity().peerId(), "not mine to revoke", null);
        a.discovery().publish(signer.sign(forged, a.identity()));

        assertThat(a.runtime().revocations().revoked(victim.identity().peerId())).isFalse();
        assertThat(a.runtime().membership().member(victim.identity().peerId())).isPresent();
        assertThat(revocationsAtB).as("what the local registry refuses is never gossiped").isEmpty();
    }

    // ------------------------------------------------------------ helpers

    private AgentCard researcherCard(PeerIdentity identity, Instant issued, Duration ttl) {
        return new AgentCard(
                "aspace://" + groupId.value() + "/agent/" + identity.peerId().value(),
                identity.peerId(), groupId, issued, ttl,
                identity.agent("researcher"), "Researches topics from the shared task space",
                List.of("research"), List.of("ResearchTask#v1"), List.of("Finding#v1"),
                Map.of());
    }

    /** A group member with no DiscoveryService: no ads handler, filter, or reconcile state. */
    private PeerNode newBareNode(String address, long seed, String... seedAddresses)
            throws IOException {
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(clock)
                .randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        nodes.add(node);
        return node;
    }

    /** Delivers a stored ad to {@code via} as an inbound rumor from an unknown sender. */
    private void inject(Wired via, String itemId, AdCache.StoredAd stored) {
        via.runtime().gossip().onRumor(PeerIdentity.generate().peerId(),
                new Bodies.Rumor(DiscoveryService.ADS_STREAM, itemId, 6, codec.toBytes(stored)));
    }
}
