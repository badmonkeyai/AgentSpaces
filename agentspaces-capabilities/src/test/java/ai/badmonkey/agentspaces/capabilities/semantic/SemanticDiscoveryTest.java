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

import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.ad.SpaceAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.spi.Embedder;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.AdvertisementSigner;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Semantic discovery on a SimNetwork: meaning-ranked lookup over the ad-cache,
 * local-first, escalating to the rendezvous, with every remote answer
 * re-verified through the cache's signature admission.
 */
class SemanticDiscoveryTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final AdvertisementSigner signer = new AdvertisementSigner();
    private final GroupId groupId = GroupId.of("zSemantic");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zSemantic", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "semantic",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Wired(PeerNode node, PeerIdentity identity, GroupRuntime runtime,
                         DiscoveryService discovery, SemanticDiscovery semantic) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Wired newPeer(String address, long seed, Set<PeerAdvertisement.PeerRole> roles,
                          String... seedAddresses) throws IOException {
        return newPeer(address, seed, roles, new HashingEmbedder(), seedAddresses);
    }

    private Wired newPeer(String address, long seed, Set<PeerAdvertisement.PeerRole> roles,
                          ai.badmonkey.agentspaces.api.spi.Embedder embedder,
                          String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity)
                .clock(clock).randomSeed(seed).roles(roles).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        DiscoveryService discovery = new DiscoveryService(
                runtime, new AdCache(codec, clock), codec, identity.peerId());
        SemanticDiscovery semantic = new SemanticDiscovery(
                new CapabilityPipes(runtime, codec), runtime, discovery,
                identity.peerId(), codec, clock, embedder);
        nodes.add(node);
        return new Wired(node, identity, runtime, discovery, semantic);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1)); // anti-entropy runs once per gossip period on the node clock
        }
    }

    private void publishFixtures(Wired peer) {
        AgentCard summarizer = new AgentCard(
                "aspace://" + groupId.value() + "/agent/summarizer",
                peer.identity().peerId(), groupId, clock.instant(), Duration.ofMinutes(15),
                peer.identity().agent("summarizer"),
                "Summarizes legal contracts and PDF documents cheaply",
                List.of("summarize"), List.of("Document#v1"), List.of("Summary#v1"), Map.of());
        AssetCard orders = new AssetCard(
                "aspace://" + groupId.value() + "/asset/orders",
                peer.identity().peerId(), groupId, clock.instant(), Duration.ofMinutes(15),
                "orders", "postgres://ops/public.orders",
                "Customer order history with line items and settlement status",
                "com.example.OrderRow#v1", "PT5M", Map.of(), Map.of("mode", "read-only"));
        peer.discovery().publish(signer.sign(summarizer, peer.identity()));
        peer.discovery().publish(signer.sign(orders, peer.identity()));
    }

    /** SPEC §6.1 v0.1.13 (TODO item 6): an action's own description is indexed, so a query matching one action ranks the card that declares it above a card that does not. */
    @Test
    void anActionsDescriptionRanksItsCard() throws Exception {
        Wired peer = newPeer("a", 1, Set.of());
        AgentCard plain = new AgentCard("aspace://" + groupId.value() + "/agent/plain",
                peer.identity().peerId(), groupId, clock.instant(), Duration.ofMinutes(15),
                peer.identity().agent("plain"), "General office helper",
                List.of("help"), List.of("Request#v1"), List.of("Reply#v1"), Map.of());
        AgentCard desk = new AgentCard("aspace://" + groupId.value() + "/agent/desk",
                peer.identity().peerId(), groupId, clock.instant(), Duration.ofMinutes(15),
                peer.identity().agent("desk"), "General office helper",
                List.of("help"), List.of("Request#v1"), List.of("Reply#v1"), Map.of())
                .withActions(List.of(new ai.badmonkey.agentspaces.api.ad.CardAction("reconcile",
                        "Reconciles quarterly invoices against bank statements",
                        List.of("Request#v1"), List.of("Reply#v1"), null,
                        ai.badmonkey.agentspaces.api.ad.CardAction.TAKE)));
        peer.discovery().publish(signer.sign(plain, peer.identity()));
        peer.discovery().publish(signer.sign(desk, peer.identity()));

        List<SemanticDiscovery.Match> matches =
                peer.semantic().query("reconcile invoices with bank statements", 5);
        assertThat(matches).isNotEmpty();
        assertThat(((AgentCard) matches.get(0).advertisement()).agent().localName())
                .isEqualTo("desk");
    }

    @Test
    void ranksTheLocalCacheByMeaning() throws Exception {
        Wired peer = newPeer("a", 1, Set.of());
        publishFixtures(peer);

        List<SemanticDiscovery.Match> forData =
                peer.semantic().query("who holds customer order history?", 5);
        assertThat(forData).isNotEmpty();
        assertThat(forData.get(0).advertisement()).isInstanceOf(AssetCard.class);

        List<SemanticDiscovery.Match> forSkill =
                peer.semantic().query("summarize legal PDF contracts", 5);
        assertThat(forSkill.get(0).advertisement()).isInstanceOf(AgentCard.class);
    }

    /**
     * Publishes the fixtures on {@code holder} while its link to {@code cold} is
     * cut, so the publish-time rumor never reaches {@code cold} and, with no
     * peer tick afterwards, neither does anti-entropy: {@code cold}'s cache is
     * provably empty before the semantic query escalates over the healed link.
     */
    private void publishFixturesUnseenBy(Wired holder, String holderAddress,
                                         Wired cold, String coldAddress) {
        network.partition(holderAddress, coldAddress);
        publishFixtures(holder);
        network.heal();
        assertThat(cold.discovery().find(AssetCard.class, c -> true))
                .as("the newcomer's cache is cold by construction").isEmpty();
        assertThat(cold.discovery().find(AgentCard.class, c -> true)).isEmpty();
    }

    /** SPEC §8 (v0.1.12): the advertisement names the embedder, its dimensions, and whether it normalizes. */
    @Test
    void theAdvertisementCarriesTheEmbedderIdentity() throws Exception {
        Wired peer = newPeer("a", 1, Set.of());
        Map<String, String> parameters = peer.semantic().describe(groupId).parameters();
        assertThat(parameters).containsEntry("embedder", "hashing")
                .containsEntry("dimensions", "256").containsEntry("normalized", "true");
    }

    /**
     * SPEC §8 (v0.1.12): a remote query goes only to peers that advertise the
     * same embedder, because a peer ranking with a different embedding returns
     * different matches; the requirement can be lifted.
     */
    @Test
    void aRendezvousWithADifferentEmbedderIsSkippedUnlessMatchingIsLifted() throws Exception {
        HashingEmbedder hashing = new HashingEmbedder();
        ai.badmonkey.agentspaces.api.spi.Embedder other = new ai.badmonkey.agentspaces.api.spi.Embedder() {
            @Override
            public double[] embed(String text) {
                return hashing.embed(text);
            }

            @Override
            public int dimensions() {
                return hashing.dimensions();
            }

            @Override
            public String identity() {
                return "spring-ai:other-model";
            }
        };
        Wired rendezvous = newPeer("rdv", 1, Set.of(PeerAdvertisement.PeerRole.RENDEZVOUS), other);
        Wired newcomer = newPeer("new", 2, Set.of(), "rdv");
        rendezvous.discovery().publish(signer.sign(rendezvous.semantic().describe(groupId),
                rendezvous.identity()));
        tickAll(6); // membership converges and the capability advertisement reaches the newcomer
        assertThat(newcomer.semantic().embedderMatches(rendezvous.identity().peerId())).isFalse();
        publishFixturesUnseenBy(rendezvous, "rdv", newcomer, "new");

        assertThat(newcomer.semantic().remoteQuery("who holds customer order history?", 5,
                Duration.ofMillis(500))).as("the mismatched rendezvous was not asked").isEmpty();

        newcomer.semantic().requireMatchingEmbedder(false);
        assertThat(newcomer.semantic().remoteQuery("who holds customer order history?", 5,
                Duration.ofSeconds(2))).isNotEmpty();
    }

    /** SPEC §8 semantic-discovery escalation (TECH §8.6): a newcomer with a provably cold cache finds meaning through the rendezvous and caches the verified answers. */
    @Test
    void aNewcomerFindsMeaningThroughTheRendezvous() throws Exception {
        Wired rendezvous = newPeer("rdv", 1,
                Set.of(PeerAdvertisement.PeerRole.RENDEZVOUS));
        Wired newcomer = newPeer("new", 2, Set.of(), "rdv");
        tickAll(4); // membership converges both ways; nothing published yet
        publishFixturesUnseenBy(rendezvous, "rdv", newcomer, "new");

        List<SemanticDiscovery.Match> matches = newcomer.semantic()
                .remoteQuery("who holds customer order history?", 5, Duration.ofSeconds(2));

        assertThat(matches).isNotEmpty();
        assertThat(matches.get(0).advertisement()).isInstanceOf(AssetCard.class);
        AssetCard found = (AssetCard) matches.get(0).advertisement();
        assertThat(found.uri()).isEqualTo("postgres://ops/public.orders");
        // The verified answers are now cached locally (local-first from here on).
        assertThat(newcomer.discovery().find(AssetCard.class, c -> true)).hasSize(1);
        assertThat(newcomer.semantic().query("who holds customer order history?", 5))
                .as("answered from the local cache without the network")
                .isNotEmpty();
    }

    /** SPEC §8 semantic-discovery escalation (§6.3): with no rendezvous declared, the query samples members instead and still finds the answer. */
    @Test
    void withoutARendezvousTheQuerySamplesMembers() throws Exception {
        Wired holder = newPeer("a", 1, Set.of());
        Wired newcomer = newPeer("new", 2, Set.of(), "a");
        tickAll(4);
        assertThat(newcomer.runtime().membership()
                .withRole(PeerAdvertisement.PeerRole.RENDEZVOUS)).isEmpty();
        publishFixturesUnseenBy(holder, "a", newcomer, "new");

        List<SemanticDiscovery.Match> matches = newcomer.semantic()
                .remoteQuery("summarize legal PDF contracts", 5, Duration.ofSeconds(2));

        assertThat(matches).isNotEmpty();
        assertThat(matches.get(0).advertisement()).isInstanceOf(AgentCard.class);
        assertThat(newcomer.discovery().find(AgentCard.class, c -> true)).hasSize(1);
    }

    /** SPEC §8 semantic-discovery: the index spans CapabilityAdvertisements and SpaceAdvertisements too, not only the agent and asset cards. */
    @Test
    void ranksCapabilityAndSpaceAdvertisementsToo() throws Exception {
        Wired peer = newPeer("a", 1, Set.of());
        publishFixtures(peer);
        CapabilityAdvertisement voteCap = new CapabilityAdvertisement(
                "aspace://" + groupId.value() + "/cap/vote/" + peer.identity().peerId().value(),
                peer.identity().peerId(), groupId, clock.instant(), Duration.ofMinutes(15),
                "aspace:cap/vote", "0.1", "space:votes",
                Map.of("modes", "QUORUM,MAJORITY_GOSSIP"), Map.of());
        SpaceAdvertisement voteSpace = new SpaceAdvertisement(
                "aspace://" + groupId.value() + "/space/votes",
                peer.identity().peerId(), groupId, clock.instant(), Duration.ofMinutes(15),
                "votes", List.of("Proposal#v1", "Ballot#v1"), ConflictStrategyType.LEASE_RACE);
        peer.discovery().publish(signer.sign(voteCap, peer.identity()));
        peer.discovery().publish(signer.sign(voteSpace, peer.identity()));

        List<SemanticDiscovery.Match> forCapability =
                peer.semantic().query("quorum majority gossip", 5);
        assertThat(forCapability).isNotEmpty();
        assertThat(forCapability.get(0).advertisement()).isInstanceOf(CapabilityAdvertisement.class);
        assertThat(((CapabilityAdvertisement) forCapability.get(0).advertisement()).capabilityType())
                .isEqualTo("aspace:cap/vote");

        List<SemanticDiscovery.Match> forSpace =
                peer.semantic().query("votes Ballot#v1 Proposal#v1", 5);
        assertThat(forSpace).isNotEmpty();
        assertThat(forSpace.get(0).advertisement()).isInstanceOf(SpaceAdvertisement.class);
        assertThat(((SpaceAdvertisement) forSpace.get(0).advertisement()).spaceName())
                .isEqualTo("votes");
    }

    /** A four-dimensional keyword embedder: texts about orders land on one axis, everything else on another. */
    static final class KeywordEmbedder implements Embedder {
        @Override
        public double[] embed(String text) {
            boolean orders = text != null && text.toLowerCase(java.util.Locale.ROOT).contains("order");
            return orders ? new double[]{1, 0, 0, 0} : new double[]{0, 0, 0, 1};
        }

        @Override
        public int dimensions() {
            return 4;
        }
    }

    /** SPEC §8 semantic-discovery: the embedder sits behind an SPI — a custom embedder drives ranking and is named in the advertisement, with no protocol change. */
    @Test
    void aCustomEmbedderPlugsInThroughTheSpi() throws Exception {
        Wired peer = newPeer("a", 1, Set.of());
        publishFixtures(peer);
        SemanticDiscovery custom = new SemanticDiscovery(
                new CapabilityPipes(peer.runtime(), codec), peer.runtime(), peer.discovery(),
                peer.identity().peerId(), codec, clock, new KeywordEmbedder());

        CapabilityAdvertisement ad = custom.describe(groupId);
        assertThat(ad.parameters()).containsEntry("embedder", "KeywordEmbedder")
                .containsEntry("dimensions", "4");

        List<SemanticDiscovery.Match> matches = custom.query("where are the orders?", 5);
        assertThat(matches).hasSize(1);
        assertThat(matches.get(0).advertisement()).isInstanceOf(AssetCard.class);
        assertThat(matches.get(0).score()).isCloseTo(1.0, org.assertj.core.data.Offset.offset(1e-9));
        // Under this embedder the summarizer is orthogonal to an orders query.
        assertThat(custom.query("summarize legal PDF contracts", 5))
                .allSatisfy(m -> assertThat(m.advertisement()).isInstanceOf(AgentCard.class))
                .isNotEmpty();
    }

    /** Mirrors of the capability's private wire records, for a hand-built rogue responder. */
    private record SemQuery(long nonce, String text, int limit) {
    }

    private record SemHit(String adType, byte[] adBytes, byte[] publicKey, byte[] signature) {
    }

    private record SemResponse(long nonce, List<SemHit> hits) {
    }

    /** TECH §8.6 / SPEC §6.3: remote hits are re-verified through the cache's admission path — a rogue rendezvous can inject nothing whose signature or issuer key does not check out. */
    @Test
    void forgedRemoteHitsAreDroppedByAdmission() throws Exception {
        // The rogue is the group's only rendezvous, so it is who the newcomer asks.
        PeerIdentity rogueIdentity = PeerIdentity.generate();
        PeerNode rogueNode = PeerNode.builder(rogueIdentity).clock(clock).randomSeed(1)
                .roles(Set.of(PeerAdvertisement.PeerRole.RENDEZVOUS)).build();
        rogueNode.listen(network.register("rogue"), "rogue");
        GroupRuntime rogueRuntime = rogueNode.joinGroup(groupAd,
                GroupMembership.Config.defaults(), List.of());
        nodes.add(rogueNode);
        Wired newcomer = newPeer("new", 2, Set.of(), "rogue");
        tickAll(4);

        // A genuinely signed card, then three ways to lie about it.
        AssetCard orders = new AssetCard(
                "aspace://" + groupId.value() + "/asset/orders",
                rogueIdentity.peerId(), groupId, clock.instant(), Duration.ofMinutes(15),
                "orders", "postgres://ops/public.orders",
                "Customer order history with line items and settlement status",
                "com.example.OrderRow#v1", "PT5M", Map.of(), Map.of());
        var signed = signer.sign(orders, rogueIdentity);
        AdCache.StoredAd genuine = new AdCache(codec, clock).toStored(
                signed.advertisement(), signed.issuerPublicKey(), signed.signature());
        byte[] tamperedBytes = genuine.adBytes().clone();
        tamperedBytes[tamperedBytes.length - 1] ^= 0x01;
        byte[] foreignKey = PeerIdentity.generate().rawPublicKey();
        List<SemHit> forged = List.of(
                // Issuer key swapped: the self-certifying PeerId no longer matches.
                new SemHit(genuine.adType(), genuine.adBytes(), foreignKey, genuine.signature()),
                // Payload tampered: the signature no longer verifies.
                new SemHit(genuine.adType(), tamperedBytes, genuine.publicKey(), genuine.signature()),
                // Unknown advertisement type tag.
                new SemHit("BogusCard", genuine.adBytes(), genuine.publicKey(), genuine.signature()));

        java.util.concurrent.atomic.AtomicReference<SemQuery> asked =
                new java.util.concurrent.atomic.AtomicReference<>();
        CapabilityPipes roguePipes = new CapabilityPipes(rogueRuntime, codec);
        roguePipes.onCapability(SemanticDiscovery.TYPE, (from, payload) -> {
            SemQuery query = codec.fromBytes(payload, SemQuery.class);
            asked.set(query);
            roguePipes.send(from, SemanticDiscovery.TYPE,
                    codec.toBytes(new SemResponse(query.nonce(), forged)));
        });

        List<SemanticDiscovery.Match> matches = newcomer.semantic()
                .remoteQuery("who holds customer order history?", 5, Duration.ofSeconds(2));

        assertThat(asked.get()).as("the rogue rendezvous was asked").isNotNull();
        assertThat(asked.get().text()).isEqualTo("who holds customer order history?");
        assertThat(matches).as("no forged hit ranks").isEmpty();
        assertThat(newcomer.discovery().find(AssetCard.class, c -> true))
                .as("nothing forged entered the cache").isEmpty();
        assertThat(newcomer.discovery().cache().storedAds())
                .extracting(AdCache.StoredAd::adType)
                .doesNotContain("AssetCard", "BogusCard");
    }

    @Test
    void unrelatedQueriesMatchNothing() throws Exception {
        Wired peer = newPeer("a", 1, Set.of());
        publishFixtures(peer);

        List<SemanticDiscovery.Match> matches =
                peer.semantic().query("quantum flux capacitor telemetry", 5);
        assertThat(matches).isEmpty();
    }
}
