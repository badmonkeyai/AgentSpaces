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

import ai.badmonkey.agentspaces.api.ad.AssetCard;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * AssetCards (spec §6.1a, the v0.1.1 addendum): data-provider peers advertise
 * the assets they hold, agents discover them like everything else, and the
 * lease is the liveness contract.
 */
class AssetDiscoveryTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final AdvertisementSigner signer = new AdvertisementSigner();
    private final GroupId groupId = GroupId.of("zAssetGroup");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zAssetGroup", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "assets",
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
        PeerIdentity identity = PeerIdentity.generate();
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

    private AssetCard ordersTable(PeerIdentity connector, Duration ttl) {
        return new AssetCard(
                "aspace://" + groupId.value() + "/asset/orders",
                connector.peerId(), groupId, clock.instant(), ttl,
                "orders", "postgres://ops/public.orders",
                "Customer order history with line items and settlement status",
                "com.example.OrderRow#v1", "PT5M",
                Map.of("rows", "1200000", "bytes", "350000000"),
                Map.of("mode", "read-only", "query", "aspace:cap/data-query"));
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
        }
    }

    @Test
    void assetCardsReachEveryCacheAndMatchStructurally() throws Exception {
        Wired connector = newPeer("conn", 1);
        Wired agent = newPeer("agent", 2, "conn");
        tickAll(4);

        connector.discovery().publish(
                signer.sign(ordersTable(connector.identity(), Duration.ofMinutes(15)),
                        connector.identity()));

        List<AssetCard> found = agent.discovery().find(AssetCard.class,
                card -> card.shape().equals("com.example.OrderRow#v1")
                        && "read-only".equals(card.access().get("mode")));
        assertThat(found).hasSize(1);
        assertThat(found.get(0).uri()).isEqualTo("postgres://ops/public.orders");
        assertThat(found.get(0).freshness()).isEqualTo("PT5M");
    }

    @Test
    void aDeadConnectorsAssetsAgeOutOfEveryCache() throws Exception {
        Wired connector = newPeer("conn", 1);
        Wired agent = newPeer("agent", 2, "conn");
        tickAll(4);

        connector.discovery().publish(
                signer.sign(ordersTable(connector.identity(), Duration.ofSeconds(30)),
                        connector.identity()));
        assertThat(agent.discovery().find(AssetCard.class, card -> true)).hasSize(1);

        // The connector dies; nobody refreshes the card; the lease does the rest.
        clock.advance(Duration.ofSeconds(31));
        assertThat(agent.discovery().find(AssetCard.class, card -> true)).isEmpty();
    }

    /** ISSUE-ModelCard FR-2: model cards are found by task, engine, and residency site through the same discovery. */
    @Test
    void findsModelCardsByTaskEngineAndSite() throws Exception {
        Wired server = newPeer("srv", 11);
        Wired agent = newPeer("agt", 12, "srv");
        tickAll(4);
        ai.badmonkey.agentspaces.api.ad.ModelCard classify = new ai.badmonkey.agentspaces.api.ad.ModelCard(
                "aspace://" + groupId.value() + "/model/bm-classify-1b@1", server.identity().peerId(), groupId, clock.instant(), Duration.ofMinutes(15),
                "bm-classify-1b", "1", List.of("classify", "embed"), "text", Map.of(), "Apache-2.0", Map.of(), Map.of(), Map.of(),
                Map.of("sites", "on-premises"), "onnx", List.of(server.identity().peerId()));
        ai.badmonkey.agentspaces.api.ad.ModelCard chat = new ai.badmonkey.agentspaces.api.ad.ModelCard(
                "aspace://" + groupId.value() + "/model/frontier@1", server.identity().peerId(), groupId, clock.instant(), Duration.ofMinutes(15),
                "frontier", "1", List.of("chat"), "text", Map.of(), "proprietary", Map.of("upstream", "provider"), Map.of(), Map.of(),
                Map.of("sites", "cloud"), "provider", List.of());
        server.discovery().publish(signer.sign(classify, server.identity()));
        server.discovery().publish(signer.sign(chat, server.identity()));
        assertThat(agent.discovery().find(ai.badmonkey.agentspaces.api.ad.ModelCard.class, c -> c.serves("classify"))).singleElement()
                .satisfies(c -> assertThat(c.name()).isEqualTo("bm-classify-1b"));
        assertThat(agent.discovery().find(ai.badmonkey.agentspaces.api.ad.ModelCard.class, c -> "provider".equals(c.engine()))).singleElement()
                .satisfies(c -> assertThat(c.name()).isEqualTo("frontier"));
        assertThat(agent.discovery().find(ai.badmonkey.agentspaces.api.ad.ModelCard.class, c -> c.residentAt("on-premises"))).hasSize(1);
        assertThat(agent.discovery().find(ai.badmonkey.agentspaces.api.ad.ModelCard.class, c -> c.serves("chat") && c.residentAt("on-premises"))).isEmpty();
    }
}
