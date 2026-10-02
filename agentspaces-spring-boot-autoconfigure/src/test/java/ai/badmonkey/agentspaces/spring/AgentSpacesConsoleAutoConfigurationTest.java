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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.console.ConsolePanel;
import ai.badmonkey.agentspaces.console.ConsoleView;
import ai.badmonkey.agentspaces.console.FleetConsoleServer;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.Fixtures.FindingEntry;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The console autoconfiguration exercised as plain objects, the way Spring
 * will call it: the default view registration over the configured groups, the
 * customizer hook, panel bean collection, and the lifecycle serving on the
 * configured port.
 */
class AgentSpacesConsoleAutoConfigurationTest {

    private final AgentSpacesAutoConfiguration fabricConfig =
            new AgentSpacesAutoConfiguration();
    private final AgentSpacesConsoleAutoConfiguration consoleConfig =
            new AgentSpacesConsoleAutoConfiguration();
    private final ObjectMapper json = new ObjectMapper();
    private final HttpClient http = HttpClient.newHttpClient();
    private final List<AutoCloseable> closeables = new ArrayList<>();

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
        group.setName("console-fleet");
        group.setFounding("spring-console-test-v1");
        group.setSpaces(new java.util.ArrayList<>(List.of(tasks)));
        properties.setGroups(List.of(group));
        properties.getConsole().setEnabled(true);
        properties.getConsole().setPort(0);
        return properties;
    }

    @Test
    @Timeout(60)
    void consoleServesTheConfiguredFabricWithCustomizersAndPanels() throws Exception {
        AgentSpacesProperties properties = properties(freePort());
        PeerIdentity identity = fabricConfig.agentSpacesIdentity(properties);
        PeerNode node = fabricConfig.agentSpacesNode(properties, identity);
        AgentSpaces spaces = fabricConfig.agentSpaces(properties, node, identity);
        AgentSpacesLifecycle fabricLifecycle =
                fabricConfig.agentSpacesLifecycle(properties, node, spaces);
        closeables.add(fabricLifecycle::stop);
        fabricLifecycle.start();

        ConsoleViewCustomizer customizer = (builder, fabric) ->
                builder.results("tasks", FindingEntry.class,
                        entry -> ((FindingEntry) entry).summary());
        ConsoleView view = consoleConfig.agentSpacesConsoleView(spaces, node,
                List.of(customizer));
        closeables.add(view);
        ConsolePanel panel = new ConsolePanel() {
            @Override
            public String id() {
                return "wiring";
            }

            @Override
            public String title() {
                return "Wiring";
            }

            @Override
            public Object data() {
                return Map.of("wired", true);
            }
        };
        FleetConsoleServer server = consoleConfig.agentSpacesConsoleServer(
                properties, view, List.of(panel), List.of());
        AgentSpacesConsoleLifecycle lifecycle =
                consoleConfig.agentSpacesConsoleLifecycle(properties, server);
        closeables.add(lifecycle::stop);
        lifecycle.start();
        assertThat(lifecycle.isRunning()).isTrue();
        assertThat(lifecycle.getPhase()).isGreaterThan(fabricLifecycle.getPhase());
        int port = lifecycle.boundPort();

        spaces.group("console-fleet").space("tasks")
                .write(new TaskEntry("count me", 1), Lease.of(Duration.ofMinutes(10)));

        JsonNode overview = json.readTree(body(port, "/api/v1/overview"));
        assertThat(overview.get("fleet").asText()).isEqualTo("console-fleet");
        assertThat(overview.get("written").asInt()).isEqualTo(1);
        assertThat(overview.get("spaces").get(0).get("name").asText())
                .isEqualTo("tasks");

        JsonNode panels = json.readTree(body(port, "/api/v1/panels"));
        assertThat(panels.get("panels").get(0).get("id").asText()).isEqualTo("wiring");
        JsonNode data = json.readTree(body(port, "/api/v1/panels/wiring/data"));
        assertThat(data.get("wired").asBoolean()).isTrue();

        lifecycle.stop();
        assertThat(lifecycle.isRunning()).isFalse();
    }

    @Test
    @Timeout(60)
    void commandAndControlWiresFromPropertiesAndGatesOnTheToken() throws Exception {
        AgentSpacesProperties properties = properties(freePort());
        // Add a control space to the group and turn C2 on.
        AgentSpacesProperties.SpaceDef control = new AgentSpacesProperties.SpaceDef();
        control.setName("fleet-control");
        control.setSettleWindowMillis(100);
        properties.getGroups().get(0).getSpaces().add(control);
        properties.getConsole().getCommand().setEnabled(true);
        properties.getConsole().getCommand().setToken("wiring-token");
        properties.getConsole().getCommand().setAuditSpace("c2-audit"); // not configured: audit off

        PeerIdentity identity = fabricConfig.agentSpacesIdentity(properties);
        PeerNode node = fabricConfig.agentSpacesNode(properties, identity);
        AgentSpaces spaces = fabricConfig.agentSpaces(properties, node, identity);
        AgentSpacesLifecycle fabricLifecycle =
                fabricConfig.agentSpacesLifecycle(properties, node, spaces);
        closeables.add(fabricLifecycle::stop);
        fabricLifecycle.start();

        ai.badmonkey.agentspaces.console.ConsoleCommand ping =
                new ai.badmonkey.agentspaces.console.ConsoleCommand() {
                    @Override
                    public String id() {
                        return "ping";
                    }

                    @Override
                    public String title() {
                        return "Ping";
                    }

                    @Override
                    public Outcome execute(java.util.Map<String, String> args,
                            ai.badmonkey.agentspaces.console.CommandContext ctx) {
                        return Outcome.ok("pong from " + ctx.operator());
                    }
                };
        ConsoleView view = consoleConfig.agentSpacesConsoleView(spaces, node, List.of());
        closeables.add(view);
        ai.badmonkey.agentspaces.console.FleetCommander commander =
                consoleConfig.agentSpacesCommander(properties, spaces, view, List.of(ping));
        ai.badmonkey.agentspaces.console.FleetConsoleServer server =
                consoleConfig.agentSpacesConsoleServer(properties, view, List.of(),
                        List.of(commander));
        AgentSpacesConsoleLifecycle lifecycle =
                consoleConfig.agentSpacesConsoleLifecycle(properties, server);
        closeables.add(lifecycle::stop);
        lifecycle.start();
        int port = lifecycle.boundPort();

        assertThat(json.readTree(body(port, "/api/v1")).get("commandAndControl").asBoolean())
                .isTrue();
        assertThat(json.readTree(body(port, "/api/v1/commands")).get("enabled").asBoolean())
                .isTrue();

        // Wrong token refused, right token accepted.
        assertThat(post(port, "/api/v1/commands/ping", "nope", "{}").statusCode())
                .isEqualTo(401);
        HttpResponse<String> ok = post(port, "/api/v1/commands/ping", "wiring-token",
                "{\"operator\":\"al\"}");
        assertThat(ok.statusCode()).isEqualTo(200);
        // TODO-EFG §5: the static gate audits every holder as "operator"; the
        // body's self-declared name is never the attributed identity.
        assertThat(json.readTree(ok.body()).get("message").asText())
                .contains("pong from operator");

        lifecycle.stop();
    }

    // ------------------------------------------------------------ TODO-EFG §5: OIDC on the console

    private static final String ISSUER = "https://idp.example.com";
    private static final String AUDIENCE = "agentspaces-console";
    private static com.nimbusds.jose.jwk.RSAKey idpKey;
    private static com.sun.net.httpserver.HttpServer jwks;

    @org.junit.jupiter.api.BeforeAll
    static void identityProvider() throws Exception {
        idpKey = new com.nimbusds.jose.jwk.gen.RSAKeyGenerator(2048).keyID("idp-key").generate();
        byte[] document = new com.nimbusds.jose.jwk.JWKSet(idpKey.toPublicJWK())
                .toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        jwks = com.sun.net.httpserver.HttpServer.create(
                new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        jwks.createContext("/jwks", exchange -> {
            exchange.sendResponseHeaders(200, document.length);
            try (java.io.OutputStream out = exchange.getResponseBody()) {
                out.write(document);
            }
        });
        jwks.start();
    }

    @org.junit.jupiter.api.AfterAll
    static void stopIdentityProvider() {
        jwks.stop(0);
    }

    private static String jwt(String subject, String scope) throws Exception {
        com.nimbusds.jwt.SignedJWT token = new com.nimbusds.jwt.SignedJWT(
                new com.nimbusds.jose.JWSHeader.Builder(com.nimbusds.jose.JWSAlgorithm.RS256)
                        .keyID(idpKey.getKeyID()).build(),
                new com.nimbusds.jwt.JWTClaimsSet.Builder()
                        .issuer(ISSUER).audience(AUDIENCE).subject(subject)
                        .claim("scope", scope)
                        .expirationTime(java.util.Date.from(
                                java.time.Instant.now().plusSeconds(3600)))
                        .build());
        token.sign(new com.nimbusds.jose.crypto.RSASSASigner(idpKey));
        return token.serialize();
    }

    /** C2-enabled properties with the identity provider configured and no static token. */
    private AgentSpacesProperties oidcConsoleProperties() throws IOException {
        AgentSpacesProperties properties = properties(freePort());
        AgentSpacesProperties.SpaceDef control = new AgentSpacesProperties.SpaceDef();
        control.setName("fleet-control");
        control.setSettleWindowMillis(100);
        properties.getGroups().get(0).getSpaces().add(control);
        properties.getConsole().getCommand().setEnabled(true);
        properties.getSecurity().getOidc().setIssuer(ISSUER);
        properties.getSecurity().getOidc().setAudience(AUDIENCE);
        properties.getSecurity().getOidc().setJwksUrl(
                "http://127.0.0.1:" + jwks.getAddress().getPort() + "/jwks");
        return properties;
    }

    private record Served(int port, AgentSpacesConsoleLifecycle lifecycle) {
    }

    /** Wires fabric, commander, and server from the properties and serves the console. */
    private Served serve(AgentSpacesProperties properties) {
        PeerIdentity identity = fabricConfig.agentSpacesIdentity(properties);
        PeerNode node = fabricConfig.agentSpacesNode(properties, identity);
        AgentSpaces spaces = fabricConfig.agentSpaces(properties, node, identity);
        AgentSpacesLifecycle fabricLifecycle =
                fabricConfig.agentSpacesLifecycle(properties, node, spaces);
        closeables.add(fabricLifecycle::stop);
        fabricLifecycle.start();
        ai.badmonkey.agentspaces.console.ConsoleCommand ping =
                new ai.badmonkey.agentspaces.console.ConsoleCommand() {
                    @Override
                    public String id() {
                        return "ping";
                    }

                    @Override
                    public String title() {
                        return "Ping";
                    }

                    @Override
                    public Outcome execute(java.util.Map<String, String> args,
                            ai.badmonkey.agentspaces.console.CommandContext ctx) {
                        return Outcome.ok("pong from " + ctx.operator());
                    }
                };
        ConsoleView view = consoleConfig.agentSpacesConsoleView(spaces, node, List.of());
        closeables.add(view);
        ai.badmonkey.agentspaces.console.FleetCommander commander =
                consoleConfig.agentSpacesCommander(properties, spaces, view, List.of(ping));
        FleetConsoleServer server = consoleConfig.agentSpacesConsoleServer(properties, view,
                List.of(), List.of(commander));
        AgentSpacesConsoleLifecycle lifecycle =
                consoleConfig.agentSpacesConsoleLifecycle(properties, server);
        closeables.add(lifecycle::stop);
        lifecycle.start();
        return new Served(lifecycle.boundPort(), lifecycle);
    }

    private HttpResponse<String> postBearer(int port, String path, String bearer, String body)
            throws Exception {
        HttpRequest.Builder req = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (bearer != null) {
            req.header("Authorization", "Bearer " + bearer);
        }
        return http.send(req.build(), HttpResponse.BodyHandlers.ofString());
    }

    /** TODO-EFG §5 / TODO item 7 (SPEC §12): with agentspaces.security.oidc.* configured and no static token, the console's command routes are gated by a JWT validator requiring agentspaces.console.command.required-scope, and the audited operator is the token subject. */
    @Test
    @Timeout(60)
    void oidcPropertiesSelectTheJwtGateForTheConsole() throws Exception {
        AgentSpacesProperties properties = oidcConsoleProperties();
        Served served = serve(properties);
        int port = served.port();

        assertThat(json.readTree(body(port, "/api/v1")).get("commandAndControl").asBoolean())
                .isTrue();
        HttpResponse<String> ok = postBearer(port, "/api/v1/commands/ping",
                jwt("alice", "openid aspace:console:operate"), "{\"operator\":\"mallory\"}");
        assertThat(ok.statusCode()).isEqualTo(200);
        assertThat(json.readTree(ok.body()).get("message").asText())
                .as("the audited operator is the token subject, never the body")
                .contains("pong from alice");

        assertThat(postBearer(port, "/api/v1/commands/ping", jwt("bob", "openid"), "{}")
                .statusCode()).as("a principal without the required scope").isEqualTo(403);
        assertThat(postBearer(port, "/api/v1/commands/ping", "not-a-jwt", "{}").statusCode())
                .as("an invalid token").isEqualTo(401);
        assertThat(postBearer(port, "/api/v1/commands/ping", null, "{}").statusCode())
                .as("no token").isEqualTo(401);
        assertThat(post(port, "/api/v1/commands/ping", "some-static-token", "{}").statusCode())
                .as("no static token is configured, so none is accepted").isEqualTo(401);

        // The required scope is a property.
        served.lifecycle().stop();
        properties.getConsole().getCommand().setRequiredScope("aspace:console:admin");
        properties.setBind("127.0.0.1:" + freePort());
        int narrowed = serve(properties).port();
        assertThat(postBearer(narrowed, "/api/v1/commands/ping",
                jwt("alice", "aspace:console:operate"), "{}").statusCode()).isEqualTo(403);
        assertThat(postBearer(narrowed, "/api/v1/commands/ping",
                jwt("alice", "aspace:console:admin"), "{}").statusCode()).isEqualTo(200);
    }

    /** TODO-EFG §5 (the explicit deviation): a non-blank static agentspaces.console.command.token wins over the identity provider — the shared secret commands, a JWT does not. */
    @Test
    @Timeout(60)
    void aStaticTokenOverridesTheJwtGate() throws Exception {
        AgentSpacesProperties properties = oidcConsoleProperties();
        properties.getConsole().getCommand().setToken("static-secret");
        int port = serve(properties).port();

        HttpResponse<String> ok = post(port, "/api/v1/commands/ping", "static-secret", "{}");
        assertThat(ok.statusCode()).isEqualTo(200);
        assertThat(json.readTree(ok.body()).get("message").asText()).contains("pong from operator");
        assertThat(postBearer(port, "/api/v1/commands/ping",
                jwt("alice", "aspace:console:operate"), "{}").statusCode())
                .as("a valid JWT is not the configured secret").isEqualTo(401);
    }

    private HttpResponse<String> post(int port, String path, String token, String body)
            throws Exception {
        HttpRequest.Builder req = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (token != null) {
            req.header("X-Console-Token", token);
        }
        return http.send(req.build(), HttpResponse.BodyHandlers.ofString());
    }

    private String body(int port, String path) throws Exception {
        HttpResponse<String> response = http.send(HttpRequest.newBuilder()
                        .uri(URI.create("http://127.0.0.1:" + port + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(path).isEqualTo(200);
        return response.body();
    }
}
