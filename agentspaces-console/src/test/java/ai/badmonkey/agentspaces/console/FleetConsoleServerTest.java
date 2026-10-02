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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import ai.badmonkey.agentspaces.api.security.BearerAuthenticator;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.auth.oidc.BearerJwtValidator;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The served surface: HAL documents, panels, the SSE stream, the UI, and the
 * bearer gate on the command routes (SPEC §12, TODO-EFG §5).
 */
class FleetConsoleServerTest {

    private static final Lease LONG = Lease.of(Duration.ofHours(1));
    private static final String ISSUER = "https://idp.example.com";
    private static final String AUDIENCE = "agentspaces-console";
    private static RSAKey idpKey;

    private final ObjectMapper json = new ObjectMapper();
    private final HttpClient http = HttpClient.newHttpClient();
    private LocalSpace space;
    private ConsoleView view;
    private FleetConsoleServer server;
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        space = LocalSpace.builder("tasks",
                new AgentId(PeerIdentity.generate().peerId(), "console")).build();
        view = ConsoleView.builder().space("tasks", space, TaskEntry.class).build();
        server = FleetConsoleServer.builder(view)
                .fleetName("research-fleet")
                .panel(new ConsolePanel() {
                    @Override
                    public String id() {
                        return "budget";
                    }

                    @Override
                    public String title() {
                        return "Token budget";
                    }

                    @Override
                    public Object data() {
                        return Map.of("spent", 12, "limit", 100);
                    }
                })
                .panel(new ConsolePanel() {
                    @Override
                    public String id() {
                        return "custom";
                    }

                    @Override
                    public String title() {
                        return "Custom";
                    }

                    @Override
                    public Object data() {
                        return Map.of();
                    }

                    @Override
                    public String htmlFragment() {
                        return "<p id=\"custom-body\">hand-rolled</p>";
                    }
                })
                .build();
        port = server.start(0);
    }

    @AfterEach
    void tearDown() {
        server.close();
        view.close();
    }

    @BeforeAll
    static void idp() throws Exception {
        idpKey = new RSAKeyGenerator(2048).keyID("idp-key").generate();
    }

    /** Mints an operator token the way the identity provider would. */
    private static String jwt(String subject, String scope) throws Exception {
        SignedJWT token = new SignedJWT(
                new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(idpKey.getKeyID()).build(),
                new JWTClaimsSet.Builder()
                        .issuer(ISSUER).audience(AUDIENCE).subject(subject)
                        .claim("scope", scope)
                        .expirationTime(Date.from(Instant.now().plusSeconds(600)))
                        .build());
        token.sign(new RSASSASigner(idpKey));
        return token.serialize();
    }

    private static BearerJwtValidator jwtGate() {
        return new BearerJwtValidator(ISSUER, AUDIENCE,
                new ImmutableJWKSet<SecurityContext>(new JWKSet(idpKey.toPublicJWK())));
    }

    /** A command-and-control console over control and audit spaces, gated as given. */
    private final class C2Console implements AutoCloseable {
        final LocalSpace control;
        final LocalSpace audit;
        final ConsoleView c2View;
        final FleetConsoleServer c2Server;
        final int c2Port;

        C2Console(BearerAuthenticator gate) throws Exception {
            AgentId console = new AgentId(PeerIdentity.generate().peerId(), "console");
            control = LocalSpace.builder("fleet-control", console).build();
            audit = LocalSpace.builder("c2-audit", console).build();
            c2View = ConsoleView.builder().space("control", control, Directive.class).build();
            FleetCommander commander = FleetCommander.builder(name -> switch (name) {
                case "fleet-control" -> control;
                case "c2-audit" -> audit;
                default -> null;
            }).controlSpace("fleet-control").auditSpace("c2-audit").view(c2View).build();
            c2Server = FleetConsoleServer.builder(c2View)
                    .fleetName("gated")
                    .commandAndControl(commander, gate)
                    .build();
            c2Port = c2Server.start(0);
        }

        HttpResponse<String> post(String path, String authorization, String body)
                throws Exception {
            HttpRequest.Builder req = HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + c2Port + path))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body));
            if (authorization != null) {
                req.header("Authorization", authorization);
            }
            return http.send(req.build(), HttpResponse.BodyHandlers.ofString());
        }

        HttpResponse<String> get(String path) throws Exception {
            return http.send(HttpRequest.newBuilder()
                            .uri(URI.create("http://127.0.0.1:" + c2Port + path)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
        }

        List<FleetCommander.C2Record> auditRecords() {
            return audit.readAll(Template.of(FleetCommander.C2Record.class), 10).stream()
                    .map(FleetCommander.C2Record.class::cast).toList();
        }

        @Override
        public void close() {
            c2Server.close();
            c2View.close();
        }
    }

    private static final String PAUSE_ALL_AS_MALLORY =
            "{\"operator\":\"mallory\",\"action\":\"PAUSE\",\"target\":\"*\"}";

    /** SPEC §12: the audit trail records the token's subject, never a self-declared name. */
    @Test
    void aJwtPrincipalIsAttributedInTheAuditLog() throws Exception {
        try (C2Console c2 = new C2Console(jwtGate())) {
            HttpResponse<String> response = c2.post("/api/v1/commands/directive",
                    "Bearer " + jwt("alice", "openid aspace:console:operate"),
                    PAUSE_ALL_AS_MALLORY);
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(json.readTree(response.body()).get("operator").asText())
                    .isEqualTo("alice");

            List<FleetCommander.C2Record> records = c2.auditRecords();
            assertThat(records).hasSize(1);
            assertThat(records.get(0).operator()).isEqualTo("alice");
            assertThat(records.get(0).detail()).doesNotContain("mallory");
            assertThat(c2.control.readAll(Template.of(Directive.class), 10).stream()
                    .map(Directive.class::cast).map(Directive::operator))
                    .containsExactly("alice");
        }
    }

    /** SPEC §12: a scoped principal without {@code aspace:console:operate} is 403; a bad token 401. */
    @Test
    void aTokenWithoutTheOperateScopeIsRefused() throws Exception {
        try (C2Console c2 = new C2Console(jwtGate())) {
            HttpResponse<String> forbidden = c2.post("/api/v1/commands/directive",
                    "Bearer " + jwt("bob", "openid aspace:a2a:client"), PAUSE_ALL_AS_MALLORY);
            assertThat(forbidden.statusCode()).isEqualTo(403);
            assertThat(json.readTree(forbidden.body()).get("error").asText())
                    .contains("aspace:console:operate");

            HttpResponse<String> garbage = c2.post("/api/v1/commands/directive",
                    "Bearer not-a-jwt", PAUSE_ALL_AS_MALLORY);
            assertThat(garbage.statusCode()).isEqualTo(401);
            assertThat(garbage.headers().firstValue("WWW-Authenticate")).hasValue("Bearer");

            HttpResponse<String> missing = c2.post("/api/v1/commands/directive",
                    null, PAUSE_ALL_AS_MALLORY);
            assertThat(missing.statusCode()).isEqualTo(401);

            assertThat(c2.auditRecords()).isEmpty();
            assertThat(c2.control.readAll(Template.of(Directive.class), 10)).isEmpty();
        }
    }

    /** The static shared-secret gate: constant-time compare, every holder audited as "operator". */
    @Test
    void theStaticTokenGateStillWorks() throws Exception {
        try (C2Console c2 = new C2Console(BearerAuthenticator.staticToken("s3cr3t", "operator"))) {
            assertThat(c2.post("/api/v1/commands/directive", "Bearer wrong",
                    PAUSE_ALL_AS_MALLORY).statusCode()).isEqualTo(401);

            HttpResponse<String> ok = c2.post("/api/v1/commands/directive",
                    "Bearer s3cr3t", PAUSE_ALL_AS_MALLORY);
            assertThat(ok.statusCode()).isEqualTo(200);
            assertThat(json.readTree(ok.body()).get("operator").asText()).isEqualTo("operator");
            assertThat(c2.auditRecords()).extracting(FleetCommander.C2Record::operator)
                    .containsExactly("operator");
        }
    }

    /** Every GET, the command catalog included, stays open under the JWT gate. */
    @Test
    void discoveryStaysOpen() throws Exception {
        try (C2Console c2 = new C2Console(jwtGate())) {
            for (String path : List.of("/api/v1", "/api/v1/overview", "/api/v1/spaces",
                    "/api/v1/peers", "/api/v1/ads", "/api/v1/panels", "/api/v1/commands", "/")) {
                assertThat(c2.get(path).statusCode()).as(path).isEqualTo(200);
            }
            JsonNode catalog = json.readTree(c2.get("/api/v1/commands").body());
            assertThat(catalog.get("enabled").asBoolean()).isTrue();
            assertThat(json.readTree(c2.get("/api/v1").body())
                    .get("commandAndControl").asBoolean()).isTrue();
        }
    }

    private JsonNode get(String path) throws Exception {
        HttpResponse<String> response = http.send(HttpRequest.newBuilder()
                        .uri(URI.create("http://127.0.0.1:" + port + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(path).isEqualTo(200);
        assertThat(response.headers().firstValue("Content-Type").orElse(""))
                .contains("hal+json");
        return json.readTree(response.body());
    }

    @Test
    void halRootLinksTheWholeSurface() throws Exception {
        JsonNode root = get("/api/v1");
        assertThat(root.get("fleet").asText()).isEqualTo("research-fleet");
        for (String rel : new String[] {"overview", "spaces", "peers", "ads",
                "panels", "events", "self"}) {
            assertThat(root.get("_links").get(rel).get("href").asText())
                    .as(rel).isNotEmpty();
        }
    }

    @Test
    void overviewAndSpaceResourcesReflectTheView() throws Exception {
        space.write(new TaskEntry("served", 1), LONG);
        space.write(new TaskEntry("also served", 2), LONG);

        JsonNode overview = get("/api/v1/overview");
        assertThat(overview.get("written").asInt()).isEqualTo(2);
        assertThat(overview.get("queued").asInt()).isEqualTo(2);
        assertThat(overview.get("spaces").get(0).get("name").asText()).isEqualTo("tasks");

        JsonNode one = get("/api/v1/spaces/tasks");
        assertThat(one.get("written").asInt()).isEqualTo(2);
        assertThat(one.get("_links").get("spaces").get("href").asText())
                .isEqualTo("/api/v1/spaces");

        HttpResponse<String> missing = http.send(HttpRequest.newBuilder()
                        .uri(URI.create("http://127.0.0.1:" + port + "/api/v1/spaces/nope"))
                        .GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(missing.statusCode()).isEqualTo(404);
    }

    @Test
    void panelsServeIndexDataAndFragment() throws Exception {
        JsonNode index = get("/api/v1/panels");
        assertThat(index.get("panels")).hasSize(2);
        assertThat(index.get("panels").get(0).get("id").asText()).isEqualTo("budget");
        assertThat(index.get("panels").get(0).get("hasFragment").asBoolean()).isFalse();
        assertThat(index.get("panels").get(1).get("hasFragment").asBoolean()).isTrue();

        JsonNode data = get("/api/v1/panels/budget/data");
        assertThat(data.get("spent").asInt()).isEqualTo(12);

        HttpResponse<String> fragment = http.send(HttpRequest.newBuilder()
                        .uri(URI.create("http://127.0.0.1:" + port
                                + "/api/v1/panels/custom/fragment")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(fragment.statusCode()).isEqualTo(200);
        assertThat(fragment.headers().firstValue("Content-Type").orElse(""))
                .contains("text/html");
        assertThat(fragment.body()).contains("hand-rolled");

        HttpResponse<String> missing = http.send(HttpRequest.newBuilder()
                        .uri(URI.create("http://127.0.0.1:" + port
                                + "/api/v1/panels/nope/data")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(missing.statusCode()).isEqualTo(404);
    }

    @Test
    void uiServesTheSingleFilePageWithTheFleetName() throws Exception {
        HttpResponse<String> page = http.send(HttpRequest.newBuilder()
                        .uri(URI.create("http://127.0.0.1:" + port + "/")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(page.statusCode()).isEqualTo(200);
        assertThat(page.headers().firstValue("Content-Type").orElse(""))
                .contains("text/html");
        assertThat(page.body())
                .contains("research-fleet")
                .contains("/api/v1/overview")
                .contains("EventSource")
                .doesNotContain("%%FLEET%%");
    }

    @Test
    void nonGetIsRejected() throws Exception {
        HttpResponse<String> response = http.send(HttpRequest.newBuilder()
                        .uri(URI.create("http://127.0.0.1:" + port + "/api/v1/overview"))
                        .POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(405);
    }

    @Test
    void eventStreamDeliversActivityAndResumesAfterAnId() throws Exception {
        space.write(new TaskEntry("before the stream", 1), LONG);
        space.write(new TaskEntry("also before", 2), LONG);

        // Resume after seq 1: only the second write replays, then live events follow.
        HttpResponse<java.io.InputStream> stream = http.send(HttpRequest.newBuilder()
                        .uri(URI.create("http://127.0.0.1:" + port + "/api/v1/events?after=1"))
                        .GET().build(),
                HttpResponse.BodyHandlers.ofInputStream());
        assertThat(stream.statusCode()).isEqualTo(200);
        assertThat(stream.headers().firstValue("Content-Type").orElse(""))
                .contains("text/event-stream");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                stream.body(), StandardCharsets.UTF_8))) {
            String replayId = null;
            String replayData = null;
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                if (line.startsWith("id: ")) {
                    replayId = line.substring(4);
                }
                if (line.startsWith("data: ")) {
                    replayData = line.substring(6);
                    break;
                }
            }
            assertThat(replayId).isEqualTo("2");
            JsonNode event = json.readTree(replayData);
            assertThat(event.get("kind").asText()).isEqualTo("written");
            assertThat(event.get("entryType").asText()).isEqualTo("TaskEntry");

            space.write(new TaskEntry("live while streaming", 3), LONG);
            String liveData = null;
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                if (line.startsWith("data: ")) {
                    liveData = line.substring(6);
                    break;
                }
            }
            assertThat(json.readTree(liveData).get("seq").asLong()).isEqualTo(3);
        }
    }
}
