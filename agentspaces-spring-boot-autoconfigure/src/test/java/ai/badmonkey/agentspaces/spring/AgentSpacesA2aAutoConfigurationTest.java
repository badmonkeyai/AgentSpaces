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
package ai.badmonkey.agentspaces.spring;

import ai.badmonkey.agentspaces.a2a.A2aGateway;
import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.SpaceNotify;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.Fixtures.FindingEntry;
import ai.badmonkey.agentspaces.test.TestClock;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * TODO item 7 (QA2 R): the A2A gateway turns on from properties as the
 * console does, under the console's bearer rule — a static token wins, else
 * the identity provider's JWT gate, else ungated on loopback only — and its
 * task binding runs on the fabric's clock.
 */
class AgentSpacesA2aAutoConfigurationTest {

    private final AgentSpacesAutoConfiguration fabricConfig = new AgentSpacesAutoConfiguration();
    private final AgentSpacesA2aAutoConfiguration a2aConfig = new AgentSpacesA2aAutoConfiguration();
    private final ObjectMapper json = new ObjectMapper();
    private final HttpClient http = HttpClient.newHttpClient();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    /** A carded agent that never takes A2A tasks, so the gateway has something to serve. */
    @AgentSpec(name = "analyst", description = "Answers fleet questions", goals = {"answer"})
    public static class Analyst {
        @SpaceNotify(space = "tasks")
        public void observe(FindingEntry finding) {
        }
    }

    @AfterEach
    void tearDown() {
        for (AutoCloseable closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // Best effort.
            }
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private AgentSpacesProperties properties(int bindPort) {
        AgentSpacesProperties properties = new AgentSpacesProperties();
        properties.setBind("127.0.0.1:" + bindPort);
        properties.setTickMillis(200);
        AgentSpacesProperties.SpaceDef tasks = new AgentSpacesProperties.SpaceDef();
        tasks.setName("tasks");
        tasks.setSettleWindowMillis(100);
        AgentSpacesProperties.Group group = new AgentSpacesProperties.Group();
        group.setName("a2a-fleet");
        group.setFounding("spring-a2a-test-v1");
        group.setSpaces(new ArrayList<>(List.of(tasks)));
        properties.setGroups(List.of(group));
        properties.getA2a().setEnabled(true);
        properties.getA2a().setSpace("tasks");
        properties.getA2a().setPort(0);
        return properties;
    }

    private record Fabric(AgentSpaces spaces, AgentSpacesLifecycle lifecycle) {
    }

    private Fabric fabric(AgentSpacesProperties properties, java.time.InstantSource clock) {
        PeerIdentity identity = fabricConfig.agentSpacesIdentity(properties);
        PeerNode node = fabricConfig.agentSpacesNode(properties, identity);
        AgentSpaces spaces = fabricConfig.agentSpaces(properties, node, identity,
                fabricConfig.agentSpacesAuthorizers(properties, identity, clock),
                fabricConfig.agentSpacesEmbedder(), clock);
        AgentSpacesLifecycle lifecycle = fabricConfig.agentSpacesLifecycle(properties, node, spaces);
        closeables.add(lifecycle::stop);
        lifecycle.start();
        spaces.group("a2a-fleet").bind(new Analyst());
        return new Fabric(spaces, lifecycle);
    }

    private AgentSpacesA2aLifecycle start(AgentSpacesProperties properties, AgentSpaces spaces) {
        A2aGateway gateway = a2aConfig.agentSpacesA2aGateway(properties, spaces);
        AgentSpacesA2aLifecycle lifecycle = a2aConfig.agentSpacesA2aLifecycle(properties, gateway);
        closeables.add(lifecycle::stop);
        lifecycle.start();
        return lifecycle;
    }

    private HttpResponse<String> rpc(int port, String bearer, String body) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + port + "/agents/analyst"))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .header("Content-Type", "application/json");
        if (bearer != null) {
            request.header("Authorization", "Bearer " + bearer);
        }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static final String SEND = """
            {"jsonrpc":"2.0","id":1,"method":"message/send","params":{"message":
              {"role":"user","parts":[{"kind":"text","text":"what changed?"}],"messageId":"m1"}}}
            """;

    @Test
    void theGatewayIsOffUnlessEnabled() {
        assertThat(new AgentSpacesProperties().getA2a().isEnabled()).isFalse();
        ConditionalOnProperty condition =
                AgentSpacesA2aAutoConfiguration.class.getAnnotation(ConditionalOnProperty.class);
        assertThat(condition.prefix()).isEqualTo("agentspaces.a2a");
        assertThat(condition.havingValue()).isEqualTo("true");
        assertThat(new AgentSpacesProperties().getA2a().getPort())
                .as("not the console's 7590 nor a quickstart peer port").isEqualTo(7594);
    }

    @Test
    @Timeout(60)
    void onLoopbackTheGatewayServesTheFleetsCards() throws Exception {
        AgentSpacesProperties properties = properties(freePort());
        Fabric fabric = fabric(properties, java.time.InstantSource.system());
        AgentSpacesA2aLifecycle lifecycle = start(properties, fabric.spaces());
        assertThat(lifecycle.getPhase()).isGreaterThan(fabric.lifecycle().getPhase());

        HttpResponse<String> card = http.send(HttpRequest.newBuilder(URI.create(
                        "http://127.0.0.1:" + lifecycle.boundPort() + "/.well-known/agent-card.json"))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(card.statusCode()).isEqualTo(200);
        JsonNode node = json.readTree(card.body());
        assertThat(node.get("name").asText()).isEqualTo("a2a-fleet");
        assertThat(node.get("skills").toString()).contains("analyst");
        assertThat(rpc(lifecycle.boundPort(), null, SEND).statusCode())
                .as("ungated on loopback").isEqualTo(200);
    }

    @Test
    @Timeout(60)
    void aStaticTokenWinsOverTheIdentityProvider() throws Exception {
        AgentSpacesProperties properties = properties(freePort());
        properties.getA2a().setToken("a2a-token");
        oidc(properties);
        Fabric fabric = fabric(properties, java.time.InstantSource.system());
        AgentSpacesA2aLifecycle lifecycle = start(properties, fabric.spaces());

        assertThat(rpc(lifecycle.boundPort(), null, SEND).statusCode()).isEqualTo(401);
        assertThat(rpc(lifecycle.boundPort(), "wrong", SEND).statusCode()).isEqualTo(401);
        assertThat(rpc(lifecycle.boundPort(), "a2a-token", SEND).statusCode())
                .as("the static token is the gate, though OIDC is configured").isEqualTo(200);
    }

    @Test
    @Timeout(60)
    void withoutATokenTheIdentityProvidersGateIsSelected() throws Exception {
        AgentSpacesProperties properties = properties(freePort());
        oidc(properties);
        Fabric fabric = fabric(properties, java.time.InstantSource.system());
        AgentSpacesA2aLifecycle lifecycle = start(properties, fabric.spaces());

        assertThat(rpc(lifecycle.boundPort(), null, SEND).statusCode())
                .as("gated even on loopback once OIDC is configured").isEqualTo(401);
        assertThat(rpc(lifecycle.boundPort(), "not-a-jwt", SEND).statusCode()).isEqualTo(401);
    }

    /** SPEC §12 v0.1.13: under the starter's OIDC gate a valid token without the A2A client scope is 403, with it 200. */
    @Test
    @Timeout(60)
    void aValidTokenWithoutTheClientScopeIsForbidden() throws Exception {
        com.nimbusds.jose.jwk.RSAKey idpKey =
                new com.nimbusds.jose.jwk.gen.RSAKeyGenerator(2048).keyID("idp-key").generate();
        byte[] document = new com.nimbusds.jose.jwk.JWKSet(idpKey.toPublicJWK())
                .toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        com.sun.net.httpserver.HttpServer jwks = com.sun.net.httpserver.HttpServer.create(
                new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        jwks.createContext("/jwks", exchange -> {
            exchange.sendResponseHeaders(200, document.length);
            try (java.io.OutputStream out = exchange.getResponseBody()) {
                out.write(document);
            }
        });
        jwks.start();
        closeables.add(() -> jwks.stop(0));
        AgentSpacesProperties properties = properties(freePort());
        AgentSpacesProperties.Security.Oidc oidc = properties.getSecurity().getOidc();
        oidc.setIssuer("https://idp.example.com");
        oidc.setAudience("agentspaces");
        oidc.setJwksUrl("http://127.0.0.1:" + jwks.getAddress().getPort() + "/jwks");
        Fabric fabric = fabric(properties, java.time.InstantSource.system());
        AgentSpacesA2aLifecycle lifecycle = start(properties, fabric.spaces());

        java.util.function.Function<String, String> jwt = scope -> {
            try {
                com.nimbusds.jwt.SignedJWT token = new com.nimbusds.jwt.SignedJWT(
                        new com.nimbusds.jose.JWSHeader.Builder(com.nimbusds.jose.JWSAlgorithm.RS256)
                                .keyID(idpKey.getKeyID()).build(),
                        new com.nimbusds.jwt.JWTClaimsSet.Builder()
                                .issuer("https://idp.example.com").audience("agentspaces")
                                .subject("client-app").claim("scope", scope)
                                .expirationTime(java.util.Date.from(
                                        java.time.Instant.now().plusSeconds(3600)))
                                .build());
                token.sign(new com.nimbusds.jose.crypto.RSASSASigner(idpKey));
                return token.serialize();
            } catch (com.nimbusds.jose.JOSEException e) {
                throw new IllegalStateException(e);
            }
        };
        HttpResponse<String> forbidden = rpc(lifecycle.boundPort(),
                jwt.apply("openid aspace:console:operate"), SEND);
        assertThat(forbidden.statusCode()).as("authenticated, but not an A2A client").isEqualTo(403);
        assertThat(forbidden.body()).contains(ai.badmonkey.agentspaces.a2a.A2aGateway.CLIENT_SCOPE);
        assertThat(rpc(lifecycle.boundPort(), jwt.apply("openid "
                + ai.badmonkey.agentspaces.a2a.A2aGateway.CLIENT_SCOPE), SEND).statusCode())
                .isEqualTo(200);
    }

    @Test
    @Timeout(60)
    void aRoutableBindWithoutAGateFailsAtStartup() throws Exception {
        AgentSpacesProperties properties = properties(freePort());
        properties.getA2a().setBind("0.0.0.0");
        Fabric fabric = fabric(properties, java.time.InstantSource.system());
        assertThatThrownBy(() -> a2aConfig.agentSpacesA2aGateway(properties, fabric.spaces()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("routable");
    }

    @Test
    @Timeout(60)
    void aMissingTaskSpaceFailsAtStartup() throws Exception {
        AgentSpacesProperties properties = properties(freePort());
        properties.getA2a().setSpace("");
        Fabric fabric = fabric(properties, java.time.InstantSource.system());
        assertThatThrownBy(() -> a2aConfig.agentSpacesA2aGateway(properties, fabric.spaces()))
                .hasMessageContaining("agentspaces.a2a.space");
    }

    /** Review M-8: the task binding and the space run on the fabric's clock, so a test clock lapses an unworked task without sleeping. */
    @Test
    @Timeout(60)
    void theTaskBindingRunsOnTheFabricsClock() throws Exception {
        AgentSpacesProperties properties = properties(freePort());
        properties.getA2a().setTaskLeaseMillis(2_000);
        TestClock clock = TestClock.create();
        Fabric fabric = fabric(properties, clock);
        AgentSpacesA2aLifecycle lifecycle = start(properties, fabric.spaces());

        JsonNode sent = json.readTree(rpc(lifecycle.boundPort(), null, SEND).body());
        String taskId = sent.path("result").path("id").asText();
        assertThat(taskId).isNotBlank();

        clock.advance(Duration.ofSeconds(3));
        Space tasks = fabric.spaces().group("a2a-fleet").space("tasks");
        ((ReplicatedSpace) tasks).sweepNow();
        JsonNode status = json.readTree(rpc(lifecycle.boundPort(), null, """
                {"jsonrpc":"2.0","id":2,"method":"tasks/get","params":{"id":"%s"}}
                """.formatted(taskId)).body()).path("result").path("status");
        assertThat(status.path("state").asText()).isEqualTo("failed");
    }

    private static void oidc(AgentSpacesProperties properties) {
        AgentSpacesProperties.Security.Oidc oidc = properties.getSecurity().getOidc();
        oidc.setIssuer("https://idp.invalid");
        oidc.setAudience("agentspaces");
        oidc.setJwksUrl("https://idp.invalid/.well-known/jwks.json");
    }
}
