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
package ai.badmonkey.agentspaces.console;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The traffic panel over two simulated peers: totals and per-kind counts from the node, rates
 * over the window between reads, the robustness counters, and the per-group gossip figures,
 * served through the console's panel API.
 */
class TrafficPanelTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final ObjectMapper json = new ObjectMapper();
    private final GroupId groupId = GroupId.of("zTrafficPanel");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zTrafficPanel",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "traffic",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private PeerNode a;
    private PeerNode b;
    private GroupRuntime atA;
    private GroupRuntime atB;

    @BeforeEach
    void setUp() throws Exception {
        a = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(1).build();
        a.listen(network.register("a"), "a");
        b = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(2).build();
        b.listen(network.register("b"), "b");
        atA = a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        atB = b.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        atA.gossip().onStream("news", (from, id, payload) -> { });
        atB.gossip().onStream("news", (from, id, payload) -> { });
        tickAll(6);
        assertThat(atA.membership().allMembers()).hasSize(1);
    }

    @AfterEach
    void tearDown() {
        a.close();
        b.close();
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            a.tick();
            b.tick();
            clock.advance(Duration.ofSeconds(1));
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> read(TrafficPanel panel) {
        return (Map<String, Object>) panel.data();
    }

    @Test
    @SuppressWarnings("unchecked")
    void reportsTotalsThenRatesOverTheWindowBetweenReads() {
        TrafficPanel panel = TrafficPanel.of(a, clock);
        Map<String, Object> first = read(panel);
        assertThat(first.get("peer")).isEqualTo(a.peerId().value());
        Map<String, Object> totals = (Map<String, Object>) first.get("totals");
        assertThat((Long) totals.get("framesSent")).isPositive();
        assertThat((Map<String, Long>) totals.get("sentByKind")).containsKey("RUMOR");
        assertThat((Map<String, Object>) first.get("rates")).as("no window yet").isEmpty();
        assertThat((Map<String, Long>) first.get("channels")).containsEntry("signed", 1L);

        for (int i = 0; i < 10; i++) {
            atA.gossip().publish("news", "n" + i, ("item-" + i).getBytes(StandardCharsets.UTF_8));
        }
        tickAll(5);
        Map<String, Object> second = read(panel);
        Map<String, Object> rates = (Map<String, Object>) second.get("rates");
        assertThat((Double) rates.get("windowSeconds")).isEqualTo(5.0);
        assertThat((Double) rates.get("framesSentPerSecond")).isGreaterThanOrEqualTo(2.0);
        assertThat((Map<String, Double>) rates.get("sentByKindPerSecond")).containsKey("RUMOR");

        // A read inside a second keeps the previous window rather than zeroing it.
        Map<String, Object> third = read(panel);
        assertThat(third.get("rates")).isSameAs(rates);
    }

    @Test
    @SuppressWarnings("unchecked")
    void countsWhatTheNetworkDidAndWhatEachGroupsGossipIsDoing() {
        TrafficPanel panel = TrafficPanel.of(b, clock);
        Map<String, Object> doc = read(panel);
        Map<String, Object> totals = (Map<String, Object>) doc.get("totals");
        assertThat(totals).containsKeys("droppedUnverifiable", "droppedStale", "droppedReplayed",
                "signedSent", "sessionSent", "bareSent", "bytesSentByKind");
        List<Map<String, Object>> groups = (List<Map<String, Object>>) doc.get("groups");
        assertThat(groups).hasSize(1);
        Map<String, Object> group = groups.get(0);
        assertThat(group.get("group")).isEqualTo(groupId.value());
        assertThat(group.get("members")).isEqualTo(1);
        assertThat(group.get("suspects")).isEqualTo(0L);
        Map<String, Object> relay = (Map<String, Object>) group.get("relay");
        assertThat(relay).containsKeys("framesForwarded", "bytesForwarded", "droppedNotMember", "droppedByPolicy", "targets",
                "offloadAttempts", "offloadOpened", "offloadFailed");
        Map<String, Object> bootstrap = (Map<String, Object>) group.get("bootstrap");
        assertThat(bootstrap).containsKeys("trace", "advertised");
        assertThat((List<String>) bootstrap.get("advertised")).isNotEmpty();
        assertThat((Map<String, Long>) group.get("treeFramesSent")).containsKeys("HAVE", "GRAFT", "PRUNE");
        assertThat((Map<String, Long>) group.get("rumorsSentByStream")).isNotNull();
        assertThat(group).containsKeys("antiEntropyRounds", "lazyPeers");
    }

    @Test
    void isServedThroughThePanelApi() throws Exception {
        ConsoleView view = ConsoleView.builder().members(() -> atA.membership().allMembers()).build();
        try (FleetConsoleServer server = FleetConsoleServer.builder(view).panel(TrafficPanel.of(a, clock)).build()) {
            int port = server.start(0);
            HttpClient http = HttpClient.newHttpClient();
            HttpResponse<String> index = http.send(HttpRequest.newBuilder(
                    URI.create("http://127.0.0.1:" + port + "/api/v1/panels")).build(), HttpResponse.BodyHandlers.ofString());
            assertThat(index.statusCode()).isEqualTo(200);
            JsonNode panels = json.readTree(index.body()).get("panels");
            assertThat(panels).anySatisfy(p -> {
                assertThat(p.get("id").asText()).isEqualTo("traffic");
                assertThat(p.get("title").asText()).isEqualTo("Wire traffic");
                assertThat(p.get("hasFragment").asBoolean()).isFalse();
            });
            HttpResponse<String> data = http.send(HttpRequest.newBuilder(
                    URI.create("http://127.0.0.1:" + port + "/api/v1/panels/traffic/data")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(data.statusCode()).isEqualTo(200);
            JsonNode doc = json.readTree(data.body());
            assertThat(doc.get("peer").asText()).isEqualTo(a.peerId().value());
            assertThat(doc.get("totals").get("framesSent").asLong()).isPositive();
            assertThat(doc.get("totals").get("sentByKind").has("RUMOR")).isTrue();
            assertThat(doc.get("groups").get(0).get("members").asInt()).isEqualTo(1);
        }
    }
}
