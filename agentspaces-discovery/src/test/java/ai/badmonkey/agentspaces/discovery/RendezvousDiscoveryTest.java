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
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
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
 * The rendezvous role (spec §5.4, §6.3): a constrained newcomer whose only
 * connectivity is the rendezvous still discovers the fleet's advertisements.
 * remoteFind fires the direct rendezvous QUERY and the scoped gossip query
 * together, so the first test alone does not tell them apart; the two tests
 * that follow silence the rendezvous's gossip handler to isolate the direct
 * path and its gating on the RENDEZVOUS role.
 */
class RendezvousDiscoveryTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final AdvertisementSigner signer = new AdvertisementSigner();
    private final GroupId groupId = GroupId.of("zRdv");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zRdv", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "rdv",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());

    private record Wired(PeerNode node, PeerIdentity identity, GroupRuntime runtime,
                         DiscoveryService discovery) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Wired newPeer(String address, long seed, Set<PeerAdvertisement.PeerRole> roles,
                          String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed)
                .roles(roles).build();
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

    /** SPEC §5.4: a peer advertising the RENDEZVOUS role keeps a larger ad cache (4x the default); any other peer gets the default bound. */
    @Test
    void aRendezvousPeerGetsALargerAdCache() throws Exception {
        Wired rendezvous = newPeer("r", 1, Set.of(PeerAdvertisement.PeerRole.RENDEZVOUS));
        Wired plain = newPeer("a", 2, Set.of(), "r");

        // One discovery service per runtime (QA4 A4-9): release the fixture's before
        // wiring the role-aware ones this test is about.
        rendezvous.discovery().close();
        plain.discovery().close();
        DiscoveryService atRendezvous = DiscoveryService.create(rendezvous.runtime(), codec,
                rendezvous.identity().peerId(), clock,
                Set.of(PeerAdvertisement.PeerRole.RENDEZVOUS));
        DiscoveryService atPlain = DiscoveryService.create(plain.runtime(), codec,
                plain.identity().peerId(), clock, Set.of());

        assertThat(atRendezvous.cache().maxEntries())
                .isEqualTo(AdCache.DEFAULT_MAX_ENTRIES * DiscoveryService.RENDEZVOUS_CACHE_MULTIPLIER)
                .isEqualTo(32_768);
        assertThat(atPlain.cache().maxEntries()).isEqualTo(AdCache.DEFAULT_MAX_ENTRIES);
        assertThat(DiscoveryService.cacheBoundFor(
                Set.of(PeerAdvertisement.PeerRole.RELAY, PeerAdvertisement.PeerRole.RENDEZVOUS)))
                .isEqualTo(32_768);
    }

    @Test
    void newcomerReachesTheFleetThroughTheRendezvousAlone() throws Exception {
        // The publisher A and the rendezvous R know each other; A holds the card.
        Wired rendezvous = newPeer("r", 1,
                Set.of(PeerAdvertisement.PeerRole.RENDEZVOUS));
        Wired publisher = newPeer("a", 2, Set.of(), "r");
        for (int i = 0; i < 4; i++) {
            nodes.forEach(PeerNode::tick);
        }
        AgentCard card = new AgentCard("aspace://" + groupId.value() + "/agent/researcher",
                publisher.identity().peerId(), groupId, clock.instant(), Duration.ofMinutes(15),
                publisher.identity().agent("researcher"), "Researches topics",
                List.of(), List.of(), List.of("Finding#v1"), Map.of());
        publisher.discovery().publish(signer.sign(card, publisher.identity()));
        assertThat(rendezvous.discovery().find(AgentCard.class, c -> true)).hasSize(1);

        // The newcomer C can reach ONLY the rendezvous: A is unreachable for it.
        network.partition("c", "a");
        Wired newcomer = newPeer("c", 3, Set.of(), "r");
        rendezvous.node().tick(); // C learns R (and R's advertised role)
        assertThat(newcomer.runtime().membership()
                .withRole(PeerAdvertisement.PeerRole.RENDEZVOUS))
                .containsExactly(rendezvous.node().peerId());
        assertThat(newcomer.discovery().find(AgentCard.class, c -> true)).isEmpty();

        List<AgentCard> found = newcomer.discovery().remoteFind(AgentCard.class,
                Map.of("description", "Researches topics"), Duration.ofSeconds(2));

        assertThat(found).hasSize(1);
        assertThat(found.get(0).agent().localName()).isEqualTo("researcher");
    }

    /** SPEC §6.3: the direct QUERY frame to a rendezvous succeeds with the gossip query silenced. */
    @Test
    void theDirectRendezvousQueryWorksWithTheGossipQuerySilenced() throws Exception {
        Wired rendezvous = newPeer("r", 1, Set.of(PeerAdvertisement.PeerRole.RENDEZVOUS));
        Wired publisher = newPeer("a", 2, Set.of(), "r");
        for (int i = 0; i < 4; i++) {
            nodes.forEach(PeerNode::tick);
        }
        publisher.discovery().publish(signer.sign(researcherCard(publisher), publisher.identity()));
        network.partition("c", "a");
        Wired newcomer = newPeer("c", 3, Set.of(), "r");
        rendezvous.node().tick();
        assertThat(newcomer.runtime().membership()
                .withRole(PeerAdvertisement.PeerRole.RENDEZVOUS))
                .containsExactly(rendezvous.node().peerId());

        // Replace R's gossip-query handler: R no longer answers scoped gossip queries.
        java.util.concurrent.atomic.AtomicInteger gossipQueriesAtR =
                new java.util.concurrent.atomic.AtomicInteger();
        rendezvous.runtime().gossip().replaceHandler(DiscoveryService.QUERY_STREAM,
                (from, itemId, payload) -> gossipQueriesAtR.incrementAndGet());

        List<AgentCard> found = newcomer.discovery().remoteFind(AgentCard.class,
                Map.of("description", "Researches topics"), Duration.ofSeconds(2));

        assertThat(found).hasSize(1);
        assertThat(gossipQueriesAtR.get())
                .as("the gossip query did reach R and was swallowed; the direct frame answered")
                .isGreaterThanOrEqualTo(1);
    }

    /** SPEC §6.3: without the RENDEZVOUS role there is no direct query; discovery then rests on gossip alone. */
    @Test
    void withoutTheRendezvousRoleTheNewcomerDependsOnTheGossipQuery() throws Exception {
        Wired plainHub = newPeer("r", 1, Set.of());
        Wired publisher = newPeer("a", 2, Set.of(), "r");
        for (int i = 0; i < 4; i++) {
            nodes.forEach(PeerNode::tick);
        }
        publisher.discovery().publish(signer.sign(researcherCard(publisher), publisher.identity()));
        network.partition("c", "a");
        Wired newcomer = newPeer("c", 3, Set.of(), "r");
        plainHub.node().tick();
        assertThat(newcomer.runtime().membership()
                .withRole(PeerAdvertisement.PeerRole.RENDEZVOUS)).isEmpty();

        plainHub.runtime().gossip().replaceHandler(DiscoveryService.QUERY_STREAM,
                (from, itemId, payload) -> { });

        assertThat(newcomer.discovery().remoteFind(AgentCard.class,
                Map.of("description", "Researches topics"), Duration.ofMillis(300)))
                .as("no role, no direct frame; the silenced gossip path was the only one")
                .isEmpty();
    }

    private AgentCard researcherCard(Wired publisher) {
        return new AgentCard("aspace://" + groupId.value() + "/agent/researcher",
                publisher.identity().peerId(), groupId, clock.instant(), Duration.ofMinutes(15),
                publisher.identity().agent("researcher"), "Researches topics",
                List.of(), List.of(), List.of("Finding#v1"), Map.of());
    }
}
