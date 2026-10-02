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
import com.sun.net.httpserver.HttpServer;
import ai.badmonkey.agentspaces.a2a.A2aMessages.A2aTaskEntry;
import ai.badmonkey.agentspaces.a2a.A2aMessages.A2aTaskResult;
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.auth.oidc.BearerJwtValidator;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
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
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A2A push notifications: the gateway POSTs task updates to a registered
 * webhook, token echoed, ending with the terminal state.
 */
class A2aPushTest {

    private record Received(String token, JsonNode task) {
    }

    private final ObjectMapper json = new ObjectMapper();
    private final HttpClient http = HttpClient.newHttpClient();
    private final List<Received> received = new CopyOnWriteArrayList<>();
    private LocalSpace space;
    private A2aGateway gateway;
    private HttpServer webhook;
    private int port;
    private int webhookPort;

    private static AgentCard workerCard() {
        PeerId peer = PeerId.fromPublicKey("worker".getBytes(StandardCharsets.UTF_8));
        GroupId group = GroupId.fromFounding("fleet".getBytes(StandardCharsets.UTF_8));
        return new AgentCard("aspace://" + group.value() + "/agent/worker",
                peer, group, Instant.parse("2026-01-01T00:00:00Z"), Duration.ofMinutes(15),
                new AgentId(peer, "worker"), "Answers questions", List.of("answer"),
                List.of(), List.of(), Map.of());
    }

    @BeforeEach
    void setUp() throws Exception {
        PeerId peer = PeerId.fromPublicKey("gateway".getBytes(StandardCharsets.UTF_8));
        space = LocalSpace.builder("a2a-push", new AgentId(peer, "gateway")).build();
        gateway = new A2aGateway("fleet", "Acme", () -> List.of(workerCard()))
                .taskBinding(new A2aTaskBinding(space))
                // The test webhook is loopback, which the SSRF policy denies by
                // default (ASF-007); the allowlist is the operator's override.
                .allowWebhookHost("localhost");
        port = gateway.start(0);

        webhook = HttpServer.create(new InetSocketAddress(0), 0);
        webhook.createContext("/hook", exchange -> {
            byte[] body = exchange.getRequestBody().readAllBytes();
            received.add(new Received(
                    exchange.getRequestHeaders().getFirst("X-A2A-Notification-Token"),
                    json.readTree(body)));
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
        });
        webhook.start();
        webhookPort = webhook.getAddress().getPort();
    }

    @AfterEach
    void tearDown() {
        gateway.close();
        webhook.stop(0);
        space.close();
    }

    private JsonNode rpc(String body) throws Exception {
        return json.readTree(http.send(HttpRequest.newBuilder(
                        URI.create("http://localhost:" + port + "/agents/worker"))
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString()).body());
    }

    @Test
    @Timeout(60)
    void updatesReachTheWebhookWithTheTokenUntilTerminal() throws Exception {
        String taskId = rpc("""
                {"jsonrpc":"2.0","id":1,"method":"message/send","params":{"message":
                {"role":"user","parts":[{"kind":"text","text":"push me"}],
                "messageId":"m1"}}}
                """).path("result").path("id").asText();

        JsonNode set = rpc("""
                {"jsonrpc":"2.0","id":2,"method":"tasks/pushNotificationConfig/set",
                "params":{"taskId":"%s","pushNotificationConfig":
                {"url":"http://localhost:%d/hook","token":"s3cret"}}}
                """.formatted(taskId, webhookPort));
        assertThat(set.path("result").path("pushNotificationConfig")
                .path("url").asText()).contains("/hook");

        // The worker completes; the gateway pushes the transitions.
        var taken = space.take(Template.of(A2aTaskEntry.class),
                Lease.of(Duration.ofMinutes(1)), Duration.ofSeconds(5)).orElseThrow();
        Thread.sleep(300);
        space.complete(taken, new A2aTaskResult(taskId, "worker", "pushed answer"),
                Lease.of(Duration.ofHours(1)));

        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline && received.stream().noneMatch(r ->
                "completed".equals(r.task().path("status").path("state").asText()))) {
            Thread.sleep(100);
        }

        assertThat(received).isNotEmpty();
        assertThat(received).allSatisfy(r -> {
            assertThat(r.token()).isEqualTo("s3cret");
            assertThat(r.task().path("id").asText()).isEqualTo(taskId);
        });
        Received last = received.get(received.size() - 1);
        assertThat(last.task().path("status").path("state").asText())
                .isEqualTo("completed");
        assertThat(last.task().path("artifacts").path(0).path("parts").path(0)
                .path("text").asText()).isEqualTo("pushed answer");

        // The registration reads back, with the callback token redacted: only
        // the webhook receiver may learn it, never a taskId holder (ASF-037).
        JsonNode get = rpc("""
                {"jsonrpc":"2.0","id":3,"method":"tasks/pushNotificationConfig/get",
                "params":{"id":"%s"}}""".formatted(taskId));
        assertThat(get.path("result").path("pushNotificationConfig")
                .path("url").asText()).contains("/hook");
        assertThat(get.path("result").path("pushNotificationConfig")
                .path("token").isNull()).isTrue();
    }

    @Test
    void privateAddressWebhooksAreRefused() throws Exception {
        String taskId = rpc("""
                {"jsonrpc":"2.0","id":1,"method":"message/send","params":{"message":
                {"role":"user","parts":[{"kind":"text","text":"ssrf probe"}],
                "messageId":"m-ssrf"}}}""")
                .path("result").path("id").asText();
        // ASF-007: the classic blind-SSRF targets are denied by default; only
        // the allowlisted host ("localhost" here) may be loopback or private.
        for (String url : java.util.List.of(
                "http://169.254.169.254/latest/meta-data",
                "http://10.0.0.1/internal",
                "http://127.0.0.1:9/hook",
                "ftp://example.com/x")) {
            JsonNode refused = rpc("""
                    {"jsonrpc":"2.0","id":9,"method":"tasks/pushNotificationConfig/set",
                    "params":{"taskId":"%s","pushNotificationConfig":
                    {"url":"%s"}}}""".formatted(taskId, url));
            assertThat(refused.path("error").path("code").asInt())
                    .as(url).isEqualTo(-32602);
        }
    }

    @Test
    void theBearerGateRefusesUnauthenticatedRpc() throws Exception {
        try (A2aGateway gated = new A2aGateway("fleet", "Acme", () -> List.of(workerCard()))
                .taskBinding(new A2aTaskBinding(space))
                .operatorToken("tok-1")) {
            int gatedPort = gated.start(0);
            String body = """
                    {"jsonrpc":"2.0","id":1,"method":"message/send","params":{"message":
                    {"role":"user","parts":[{"kind":"text","text":"hi"}],
                    "messageId":"m-auth"}}}""";
            URI url = URI.create("http://localhost:" + gatedPort + "/agents/worker");

            HttpResponse<String> missing = http.send(HttpRequest.newBuilder(url)
                    .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(missing.statusCode()).isEqualTo(401);

            HttpResponse<String> wrong = http.send(HttpRequest.newBuilder(url)
                    .header("Authorization", "Bearer nope")
                    .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(wrong.statusCode()).isEqualTo(401);

            HttpResponse<String> right = http.send(HttpRequest.newBuilder(url)
                    .header("Authorization", "Bearer tok-1")
                    .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(right.statusCode()).isEqualTo(200);
            assertThat(json.readTree(right.body()).path("result").path("id").asText())
                    .isNotEmpty();
        }
    }

    @Test
    void configForAnUnknownTaskIsAnError() throws Exception {
        assertThat(rpc("""
                {"jsonrpc":"2.0","id":4,"method":"tasks/pushNotificationConfig/set",
                "params":{"taskId":"nope","pushNotificationConfig":
                {"url":"http://localhost:1/hook"}}}""")
                .path("error").path("code").asInt()).isEqualTo(-32001);
        assertThat(rpc("""
                {"jsonrpc":"2.0","id":5,"method":"tasks/pushNotificationConfig/get",
                "params":{"id":"nope"}}""")
                .path("error").path("code").asInt()).isEqualTo(-32001);
    }

    @Test
    void pushCapabilityIsAdvertised() throws Exception {
        JsonNode card = json.readTree(http.send(HttpRequest.newBuilder(URI.create(
                        "http://localhost:" + port + "/.well-known/agent-card.json"))
                .build(), HttpResponse.BodyHandlers.ofString()).body());
        assertThat(card.path("capabilities").path("pushNotifications").asBoolean())
                .isTrue();
    }

    /** ASF-007: the bearer gate covers the RPC surface only; discovery stays open and 401s name the scheme. */
    @Test
    void theBearerGateLeavesDiscoveryOpenAndAdvertisesTheScheme() throws Exception {
        try (A2aGateway gated = new A2aGateway("fleet", "Acme", () -> List.of(workerCard()))
                .taskBinding(new A2aTaskBinding(space))
                .operatorToken("tok-2")) {
            int gatedPort = gated.start(0);
            String base = "http://localhost:" + gatedPort;

            HttpResponse<String> wellKnown = http.send(HttpRequest.newBuilder(
                    URI.create(base + "/.well-known/agent-card.json")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(wellKnown.statusCode()).isEqualTo(200);
            HttpResponse<String> agents = http.send(HttpRequest.newBuilder(
                    URI.create(base + "/agents/worker")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(agents.statusCode()).isEqualTo(200);
            assertThat(json.readTree(agents.body()).path("name").asText()).isEqualTo("worker");

            HttpResponse<String> refused = http.send(HttpRequest.newBuilder(
                    URI.create(base + "/agents/worker"))
                    .POST(HttpRequest.BodyPublishers.ofString("""
                            {"jsonrpc":"2.0","id":1,"method":"tasks/get","params":{"id":"x"}}"""))
                    .build(), HttpResponse.BodyHandlers.ofString());
            assertThat(refused.statusCode()).isEqualTo(401);
            assertThat(refused.headers().firstValue("WWW-Authenticate")).hasValue("Bearer");
            assertThat(json.readTree(refused.body()).path("error").asText())
                    .contains("bearer token");
        }
    }

    /** ASF-007: IPv6 unique-local, link-local and IPv4 carrier-grade-NAT webhooks are denied too. */
    @Test
    void ipv6UniqueLocalAndCgnWebhooksAreRefused() throws Exception {
        String taskId = rpc("""
                {"jsonrpc":"2.0","id":1,"method":"message/send","params":{"message":
                {"role":"user","parts":[{"kind":"text","text":"ssrf probe 2"}],
                "messageId":"m-ssrf2"}}}""")
                .path("result").path("id").asText();
        for (String url : java.util.List.of(
                "http://[fd00::1]/hook",
                "http://[fe80::1]/hook",
                "http://100.64.0.1/hook",
                "https://[::1]/hook")) {
            JsonNode refused = rpc("""
                    {"jsonrpc":"2.0","id":9,"method":"tasks/pushNotificationConfig/set",
                    "params":{"taskId":"%s","pushNotificationConfig":
                    {"url":"%s"}}}""".formatted(taskId, url));
            assertThat(refused.path("error").path("code").asInt()).as(url).isEqualTo(-32602);
            assertThat(refused.path("error").path("message").asText()).as(url)
                    .contains("not a permitted webhook target");
        }
    }
    /**
     * SPEC §12 / TODO-EFG §5: an identity-provider JWT gates the RPC surface by
     * the {@code aspace:a2a:client} scope; discovery stays open.
     */
    @Test
    void aJwtBearerGatesTheRpcSurface() throws Exception {
        String issuer = "https://idp.example.com";
        String audience = "agentspaces-a2a";
        RSAKey idpKey = new RSAKeyGenerator(2048).keyID("idp-key").generate();
        java.util.function.BiFunction<String, String, String> jwt = (subject, scope) -> {
            try {
                SignedJWT token = new SignedJWT(
                        new JWSHeader.Builder(JWSAlgorithm.RS256)
                                .keyID(idpKey.getKeyID()).build(),
                        new JWTClaimsSet.Builder()
                                .issuer(issuer).audience(audience).subject(subject)
                                .claim("scope", scope)
                                .expirationTime(Date.from(Instant.now().plusSeconds(600)))
                                .build());
                token.sign(new RSASSASigner(idpKey));
                return token.serialize();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        };
        try (A2aGateway gated = new A2aGateway("fleet", "Acme", () -> List.of(workerCard()))
                .taskBinding(new A2aTaskBinding(space))
                .bearer(new BearerJwtValidator(issuer, audience,
                        new ImmutableJWKSet<SecurityContext>(
                                new JWKSet(idpKey.toPublicJWK()))))) {
            int gatedPort = gated.start(0);
            String base = "http://localhost:" + gatedPort;
            URI rpcUrl = URI.create(base + "/agents/worker");
            String body = """
                    {"jsonrpc":"2.0","id":1,"method":"message/send","params":{"message":
                    {"role":"user","parts":[{"kind":"text","text":"hi"}],
                    "messageId":"m-jwt"}}}""";

            // The right scope is accepted.
            HttpResponse<String> accepted = http.send(HttpRequest.newBuilder(rpcUrl)
                    .header("Authorization", "Bearer " + jwt.apply("client-app",
                            "openid aspace:a2a:client"))
                    .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(accepted.statusCode()).isEqualTo(200);
            assertThat(json.readTree(accepted.body()).path("result").path("id").asText())
                    .isNotEmpty();

            // A valid token without the scope is forbidden, not unauthenticated.
            HttpResponse<String> forbidden = http.send(HttpRequest.newBuilder(rpcUrl)
                    .header("Authorization", "Bearer " + jwt.apply("client-app",
                            "openid aspace:console:operate"))
                    .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(forbidden.statusCode()).isEqualTo(403);
            assertThat(json.readTree(forbidden.body()).path("error").asText())
                    .contains("aspace:a2a:client");

            // An invalid token is 401 and names the scheme.
            HttpResponse<String> invalid = http.send(HttpRequest.newBuilder(rpcUrl)
                    .header("Authorization", "Bearer not-a-jwt")
                    .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(invalid.statusCode()).isEqualTo(401);
            assertThat(invalid.headers().firstValue("WWW-Authenticate")).hasValue("Bearer");

            // Cards stay open.
            assertThat(http.send(HttpRequest.newBuilder(
                    URI.create(base + "/.well-known/agent-card.json")).build(),
                    HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(200);
            assertThat(http.send(HttpRequest.newBuilder(rpcUrl).build(),
                    HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(200);
        }
    }
}
