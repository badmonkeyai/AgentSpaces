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
package ai.badmonkey.agentspaces.console;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The console shows its node's standing in the group (reviews/LOST-MEMBER-RETIREMENT-2026-11-18.md): the state in the
 * overview, the lost and retired members on the peers page, and each change of state in the activity stream.
 */
class MembershipConsoleTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final GroupId groupId = GroupId.of("zConsoleMembership");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zConsoleMembership",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "membership",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final HttpClient http = HttpClient.newHttpClient();
    private final ObjectMapper json = new ObjectMapper();
    private FleetConsoleServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
        nodes.forEach(PeerNode::close);
    }

    private PeerNode node(String address, long seed) throws Exception {
        PeerNode node = PeerNode.builder(PeerIdentity.generate()).clock(clock).randomSeed(seed)
                .retireAfter(Duration.ofMinutes(5)).build();
        node.listen(network.register(address), address);
        nodes.add(node);
        return node;
    }

    private void seconds(int n) {
        for (int i = 0; i < n; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    private JsonNode get(int port, String path) throws Exception {
        HttpResponse<String> response = http.send(HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + path)).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return json.readTree(response.body());
    }

    @Test
    void theConsoleShowsTheStateTheLostAndRetiredMembersAndEachChange() throws Exception {
        PeerNode a = node("a", 1);
        PeerNode b = node("b", 2);
        a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        b.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        ConsoleView view = ConsoleView.builder().members(() -> a.group(groupId).orElseThrow().membership().allMembers())
                .membership(a, groupId).build();
        server = FleetConsoleServer.builder(view).fleetName("membership").build();
        int port = server.start(0);
        seconds(20);
        assertThat(get(port, "/api/v1/overview").path("membership").path("state").asText()).isEqualTo("CONNECTED");

        network.partition("a", "b");
        seconds(60);
        JsonNode peers = get(port, "/api/v1/peers");
        assertThat(peers.path("membership").path("state").asText()).isEqualTo("RECONNECTING");
        assertThat(peers.path("lost")).hasSize(1);
        assertThat(peers.path("lost").get(0).path("peer").asText()).isEqualTo(b.peerId().value());
        assertThat(peers.path("lost").get(0).path("attempts").asInt()).isPositive();

        seconds(6 * 60);
        JsonNode overview = get(port, "/api/v1/overview");
        assertThat(overview.path("membership").path("state").asText()).isEqualTo("ISOLATED");
        assertThat(overview.path("membership").path("retired").asInt()).isEqualTo(1);
        assertThat(get(port, "/api/v1/peers").path("retired").get(0).path("peer").asText())
                .isEqualTo(b.peerId().value());

        network.heal();
        for (int i = 0; i < 30 && !"CONNECTED".equals(view.membership().orElseThrow().state().name()); i++) {
            seconds(10);
            // b retired a too, and b has a's address as its seed: it rejoins from its seed
        }
        assertThat(view.membership().orElseThrow().state()).isEqualTo(PeerNode.MembershipState.CONNECTED);
        List<String> kinds = view.eventsAfter(0).stream().map(ConsoleEvent::kind).toList();
        assertThat(kinds).as("the transitions in the activity stream").containsSubsequence(
                "GROUP_STATUS", "GROUP_ISOLATED", "GROUP_REJOINED");
        assertThat(view.eventsAfter(0)).filteredOn(e -> e.kind().equals("GROUP_ISOLATED")).singleElement()
                .satisfies(e -> {
                    assertThat(e.space()).isEqualTo(groupId.value());
                    assertThat(e.entryType()).isEqualTo("ISOLATED");
                    assertThat(e.worker()).isEqualTo("RECONNECTING");
                });
    }
}
