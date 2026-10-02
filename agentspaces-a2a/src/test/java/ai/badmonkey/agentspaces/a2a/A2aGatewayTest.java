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
package ai.badmonkey.agentspaces.a2a;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/** The gateway's HTTP surface, exercised with the JDK HTTP client. */
class A2aGatewayTest {

    private final List<AgentCard> cards = new CopyOnWriteArrayList<>();
    private final ObjectMapper json = new ObjectMapper();
    private final HttpClient http = HttpClient.newHttpClient();
    private A2aGateway gateway;
    private int port;

    private static AgentCard card(String name, String description, List<String> goals) {
        PeerId peer = PeerId.fromPublicKey(name.getBytes(StandardCharsets.UTF_8));
        GroupId group = GroupId.fromFounding("fleet".getBytes(StandardCharsets.UTF_8));
        return new AgentCard("aspace://" + group.value() + "/agent/" + name,
                peer, group, Instant.parse("2026-01-01T00:00:00Z"),
                Duration.ofMinutes(15), new AgentId(peer, name), description, goals,
                List.of("com.example.Task#v1"), List.of("com.example.Result#v1"),
                Map.of());
    }

    @BeforeEach
    void setUp() throws Exception {
        gateway = new A2aGateway("audit-fleet", "Acme", () -> List.copyOf(cards));
        port = gateway.start(0);
    }

    @AfterEach
    void tearDown() {
        gateway.close();
    }

    private JsonNode get(String path, int expectedStatus) throws Exception {
        return get(port, path, expectedStatus);
    }

    private JsonNode get(int atPort, String path, int expectedStatus) throws Exception {
        HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + atPort + path)).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(expectedStatus);
        assertThat(response.headers().firstValue("Content-Type"))
                .hasValueSatisfying(v -> assertThat(v).startsWith("application/json"));
        return json.readTree(response.body());
    }

    @Test
    void theWellKnownCardRepresentsTheWholeFleet() throws Exception {
        cards.add(card("extractor", "Extracts fields from scans", List.of("extract")));
        cards.add(card("auditor", "Audits extractions", List.of("audit", "approve")));

        JsonNode node = get("/.well-known/agent-card.json", 200);

        assertThat(node.get("name").asText()).isEqualTo("audit-fleet");
        assertThat(node.get("protocolVersion").asText()).isEqualTo("1.0");
        assertThat(node.get("skills")).hasSize(3);
        assertThat(node.get("capabilities").get("streaming").asBoolean()).isFalse();
    }

    @Test
    void agentsAreListedAndFetchableByName() throws Exception {
        cards.add(card("extractor", "Extracts fields from scans", List.of("extract")));

        JsonNode list = get("/agents", 200);
        assertThat(list).hasSize(1);
        assertThat(list.get(0).get("name").asText()).isEqualTo("extractor");

        JsonNode one = get("/agents/extractor", 200);
        assertThat(one.get("description").asText()).isEqualTo("Extracts fields from scans");
        assertThat(one.get("url").asText()).endsWith("/agents/extractor");
        assertThat(one.get("metadata").get("agentspaces.group").asText()).isNotEmpty();
    }

    @Test
    void unknownAgentsAre404() throws Exception {
        JsonNode node = get("/agents/nobody", 404);
        assertThat(node.get("error").asText()).contains("nobody");
    }

    @Test
    void theSurfaceTracksTheLiveCardSource() throws Exception {
        assertThat(get("/agents", 200)).isEmpty();

        cards.add(card("latecomer", "Joined after start", List.of()));
        assertThat(get("/agents", 200)).hasSize(1);

        cards.clear(); // the card lapsed from the discovery cache
        assertThat(get("/agents", 200)).isEmpty();
    }

    /** SPEC §12 / TECH-SPEC §9.4: every card URL follows the configured external base URL, trailing slash dropped. */
    @Test
    void cardUrlsFollowTheConfiguredExternalBaseUrl() throws Exception {
        cards.add(card("extractor", "Extracts fields from scans", List.of("extract")));
        cards.add(card("auditor", "Audits extractions", List.of("audit")));
        try (A2aGateway external = new A2aGateway("audit-fleet", "Acme", () -> List.copyOf(cards))
                .externalBaseUrl("https://fleet.example.com/a2a/")) {
            int externalPort = external.start(0);

            JsonNode fleet = get(externalPort, "/.well-known/agent-card.json", 200);
            assertThat(fleet.get("url").asText()).isEqualTo("https://fleet.example.com/a2a/agents");
            assertThat(fleet.get("provider").get("url").asText())
                    .isEqualTo("https://fleet.example.com/a2a");

            JsonNode agents = get(externalPort, "/agents", 200);
            assertThat(agents).hasSize(2);
            for (JsonNode agent : agents) {
                assertThat(agent.get("url").asText())
                        .startsWith("https://fleet.example.com/a2a/agents/");
                assertThat(agent.get("provider").get("url").asText())
                        .isEqualTo("https://fleet.example.com/a2a");
            }
        }
    }

    /** SPEC §12 / TECH-SPEC §9.4: with no external URL configured, card URLs follow the actual bound address, not a hard-coded localhost. */
    @Test
    void cardUrlsFollowTheBoundAddressByDefault() throws Exception {
        cards.add(card("extractor", "Extracts fields from scans", List.of("extract")));
        try (A2aGateway bound = new A2aGateway("audit-fleet", "Acme", () -> List.copyOf(cards))) {
            int boundPort = bound.start(new InetSocketAddress("127.0.0.1", 0));
            String expected = "http://127.0.0.1:" + boundPort;

            JsonNode fleet = get(boundPort, "/.well-known/agent-card.json", 200);
            assertThat(fleet.get("url").asText()).isEqualTo(expected + "/agents");

            JsonNode agent = get(boundPort, "/agents/extractor", 200);
            assertThat(agent.get("url").asText()).isEqualTo(expected + "/agents/extractor");
            assertThat(agent.get("provider").get("url").asText()).isEqualTo(expected);
        }
    }

    /** TECH-SPEC §9.4: a discovery-only gateway answers every JSON-RPC method with -32601. */
    @Test
    void discoveryOnlyGatewayRefusesRpc() throws Exception {
        cards.add(card("extractor", "Extracts fields from scans", List.of("extract")));
        HttpResponse<String> response = http.send(HttpRequest.newBuilder(
                        URI.create("http://localhost:" + port + "/agents/extractor"))
                .POST(HttpRequest.BodyPublishers.ofString("""
                        {"jsonrpc":"2.0","id":1,"method":"message/send","params":{"message":
                        {"role":"user","parts":[{"kind":"text","text":"hi"}],"messageId":"m1"}}}
                        """))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode body = json.readTree(response.body());
        assertThat(body.path("id").asInt()).isEqualTo(1);
        assertThat(body.path("error").path("code").asInt()).isEqualTo(-32601);
        assertThat(body.path("error").path("message").asText()).contains("no task binding");
    }
}
