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

import ai.badmonkey.agentspaces.a2a.A2aMessages.A2aTaskEntry;
import ai.badmonkey.agentspaces.a2a.A2aMessages.A2aTaskResult;
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A2A 1.0 at the gateway (a2a-protocol.org v1.0.1): the request's
 * {@code A2A-Version} picks 1.0's methods and shapes at the same URL that
 * still serves 0.3, the card lists both interfaces, and every 1.0 document
 * the gateway writes has no {@code kind} and uses 1.0's enum names.
 */
class A2aVersionOneTest {

    private record Hook(String authorization, String token, String contentType, JsonNode body) {
    }

    private final ObjectMapper json = new ObjectMapper();
    private final HttpClient http = HttpClient.newHttpClient();
    private final List<Hook> hooks = new CopyOnWriteArrayList<>();
    private LocalSpace space;
    private A2aGateway gateway;
    private HttpServer webhook;
    private int port;

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
        space = LocalSpace.builder("a2a-v1", new AgentId(peer, "gateway")).build();
        gateway = new A2aGateway("fleet", "Acme", () -> List.of(workerCard()))
                .taskBinding(new A2aTaskBinding(space))
                .allowWebhookHost("localhost");
        port = gateway.start(0);
        webhook = HttpServer.create(new InetSocketAddress(0), 0);
        webhook.createContext("/hook", exchange -> {
            hooks.add(new Hook(exchange.getRequestHeaders().getFirst("Authorization"),
                    exchange.getRequestHeaders().getFirst("X-A2A-Notification-Token"),
                    exchange.getRequestHeaders().getFirst("Content-Type"),
                    json.readTree(exchange.getRequestBody().readAllBytes())));
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
        });
        webhook.start();
    }

    @AfterEach
    void tearDown() {
        gateway.close();
        webhook.stop(0);
        space.close();
    }

    private String endpoint() {
        return "http://localhost:" + port + "/agents/worker";
    }

    private JsonNode rpc(String version, String body) throws Exception {
        return rpcAt(endpoint(), version, body);
    }

    private JsonNode rpcAt(String url, String version, String body) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(url))
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (version != null) {
            request.header("A2A-Version", version);
        }
        return json.readTree(http.send(request.build(), HttpResponse.BodyHandlers.ofString()).body());
    }

    private static String send(String method, String text) {
        return """
                {"jsonrpc":"2.0","id":1,"method":"%s","params":{"message":
                {"role":"ROLE_USER","parts":[{"text":"%s"}],"messageId":"m1"}}}"""
                .formatted(method, text);
    }

    private static String call(String method, String params) {
        return """
                {"jsonrpc":"2.0","id":7,"method":"%s","params":%s}""".formatted(method, params);
    }

    /** Takes the one task entry and completes it with {@code answer}. */
    private void work(String answer, long waitMillis, long holdMillis) {
        Thread.ofVirtual().start(() -> {
            try {
                Thread.sleep(waitMillis);
                var taken = space.take(Template.of(A2aTaskEntry.class),
                        Lease.of(Duration.ofMinutes(1)), Duration.ofSeconds(20)).orElseThrow();
                Thread.sleep(holdMillis);
                space.complete(taken, new A2aTaskResult(taken.entry().taskId(), "worker", answer),
                        Lease.of(Duration.ofHours(1)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    private static void assertNoKind(JsonNode node) {
        assertThat(node.findValues("kind")).as("1.0 has no kind discriminators: " + node).isEmpty();
        assertThat(node.findValues("final")).as("1.0 has no final flag: " + node).isEmpty();
    }

    @Test
    void theCardOffersOnePointZeroFirstAndZeroPointThreeAtTheSameUrl() throws Exception {
        JsonNode card = json.readTree(http.send(HttpRequest.newBuilder(URI.create(endpoint())).build(),
                HttpResponse.BodyHandlers.ofString()).body());
        JsonNode interfaces = card.get("supportedInterfaces");
        assertThat(interfaces).hasSize(2);
        String advertised = interfaces.get(0).get("url").asText();
        assertThat(advertised).endsWith(":" + port + "/agents/worker");
        assertThat(interfaces.get(0).get("protocolBinding").asText()).isEqualTo("JSONRPC");
        assertThat(interfaces.get(0).get("protocolVersion").asText()).isEqualTo("1.0");
        assertThat(interfaces.get(1).get("url").asText()).isEqualTo(advertised);
        assertThat(interfaces.get(1).get("protocolVersion").asText()).isEqualTo("0.3");
        // 0.3's top-level fields describe the 0.3 interface; a 1.0 client ignores them
        assertThat(card.get("url").asText()).isEqualTo(advertised);
        assertThat(card.get("protocolVersion").asText()).isEqualTo("0.3");
        assertThat(card.get("preferredTransport").asText()).isEqualTo("JSONRPC");
        assertThat(card.get("capabilities").get("streaming").asBoolean()).isTrue();
        assertThat(card.get("capabilities").get("pushNotifications").asBoolean()).isTrue();
        assertThat(card.get("capabilities").get("extendedAgentCard").asBoolean()).isFalse();
        assertThat(card.has("metadata")).isFalse();

        // the record reads its own JSON back, as the platform's import does a fetched card
        A2aAgentCard parsed = json.treeToValue(card, A2aAgentCard.class);
        assertThat(parsed.select()).hasValueSatisfying(i -> {
            assertThat(i.version()).contains(A2aVersion.V1_0);
            assertThat(i.url()).isEqualTo(advertised);
        });
        assertThat(parsed.metadata()).containsKey("agentspaces.agent");
    }

    @Test
    @Timeout(30)
    void sendMessageAndGetTaskSpeakOnePointZero() throws Exception {
        JsonNode sent = rpc("1.0", send("SendMessage", "hi"));
        assertThat(sent.has("error")).as(sent.toString()).isFalse();
        JsonNode task = sent.path("result").path("task");
        assertThat(task.path("status").path("state").asText()).isEqualTo("TASK_STATE_SUBMITTED");
        assertThat(task.path("history").get(0).path("role").asText()).isEqualTo("ROLE_USER");
        assertThat(task.path("history").get(0).path("parts").get(0).path("text").asText()).isEqualTo("hi");
        assertNoKind(sent);
        String taskId = task.path("id").asText();

        work("hello back", 0, 0);
        JsonNode done = null;
        for (int i = 0; i < 200; i++) {
            done = rpc("1.0", call("GetTask", "{\"id\":\"" + taskId + "\"}"));
            if ("TASK_STATE_COMPLETED".equals(done.path("result").path("status").path("state").asText())) {
                break;
            }
            Thread.sleep(50);
        }
        assertThat(done.path("result").path("status").path("state").asText()).isEqualTo("TASK_STATE_COMPLETED");
        assertThat(done.path("result").path("artifacts").get(0).path("parts").get(0).path("text").asText())
                .isEqualTo("hello back");
        assertThat(done.path("result").path("history")).hasSize(2);
        assertNoKind(done);

        // §3.2.4: historyLength 0 omits the history, n keeps the n most recent
        JsonNode none = rpc("1.0", call("GetTask", "{\"id\":\"" + taskId + "\",\"historyLength\":0}"));
        assertThat(none.path("result").has("history")).isFalse();
        JsonNode last = rpc("1.0", call("GetTask", "{\"id\":\"" + taskId + "\",\"historyLength\":1}"));
        assertThat(last.path("result").path("history")).hasSize(1);
        assertThat(last.path("result").path("history").get(0).path("role").asText()).isEqualTo("ROLE_AGENT");
    }

    @Test
    void theVersionDecidesTheDialect() throws Exception {
        // no header is 0.3 (§3.6.2): 1.0's names are unknown there, and 0.3's under 1.0
        assertThat(rpc(null, send("SendMessage", "x")).path("error").path("code").asInt()).isEqualTo(-32601);
        assertThat(rpc("1.0", """
                {"jsonrpc":"2.0","id":1,"method":"message/send","params":{"message":
                {"role":"user","parts":[{"kind":"text","text":"x"}],"messageId":"m1"}}}""")
                .path("error").path("code").asInt()).isEqualTo(-32601);
        // 0.3 by header, and with no header, as before
        assertThat(rpc("0.3", """
                {"jsonrpc":"2.0","id":1,"method":"message/send","params":{"message":
                {"role":"user","parts":[{"kind":"text","text":"x"}],"messageId":"m1"}}}""")
                .path("result").path("status").path("state").asText()).isEqualTo("submitted");
        // a patch number is ignored, and the query parameter stands in for the header
        assertThat(rpc("1.0.1", send("SendMessage", "x")).path("result").path("task").path("status")
                .path("state").asText()).isEqualTo("TASK_STATE_SUBMITTED");
        assertThat(rpcAt(endpoint() + "?A2A-Version=1.0", null, send("SendMessage", "x"))
                .path("result").path("task").path("id").asText()).isNotBlank();
        // any other version is VersionNotSupportedError, with its ErrorInfo
        JsonNode refused = rpc("0.5", send("SendMessage", "x"));
        assertThat(refused.path("error").path("code").asInt()).isEqualTo(-32009);
        JsonNode info = refused.path("error").path("data").get(0);
        assertThat(info.path("@type").asText()).isEqualTo("type.googleapis.com/google.rpc.ErrorInfo");
        assertThat(info.path("reason").asText()).isEqualTo("VERSION_NOT_SUPPORTED");
        assertThat(info.path("domain").asText()).isEqualTo("a2a-protocol.org");
        assertThat(info.path("metadata").path("supportedVersions").asText()).isEqualTo("1.0,0.3");
    }

    @Test
    void cancelTaskFollowsOnePointZerosErrors() throws Exception {
        String taskId = rpc("1.0", send("SendMessage", "x")).path("result").path("task").path("id").asText();
        JsonNode canceled = rpc("1.0", call("CancelTask", "{\"id\":\"" + taskId + "\"}"));
        assertThat(canceled.path("result").path("status").path("state").asText()).isEqualTo("TASK_STATE_CANCELED");
        JsonNode again = rpc("1.0", call("CancelTask", "{\"id\":\"" + taskId + "\"}"));
        assertThat(again.path("error").path("code").asInt()).as("TaskNotCancelableError").isEqualTo(-32002);
        assertThat(again.path("error").path("data").get(0).path("reason").asText()).isEqualTo("TASK_NOT_CANCELABLE");
        JsonNode unknown = rpc("1.0", call("GetTask", "{\"id\":\"nope\"}"));
        assertThat(unknown.path("error").path("code").asInt()).isEqualTo(-32001);
        assertThat(unknown.path("error").path("data").get(0).path("reason").asText()).isEqualTo("TASK_NOT_FOUND");
    }

    @Test
    @Timeout(60)
    void sendStreamingMessageStreamsResponsesAndClosesAtTheEnd() throws Exception {
        work("streamed answer", 400, 400);
        HttpResponse<java.io.InputStream> response = http.send(HttpRequest.newBuilder(URI.create(endpoint()))
                .header("A2A-Version", "1.0")
                .POST(HttpRequest.BodyPublishers.ofString(send("SendStreamingMessage", "stream me")))
                .build(), HttpResponse.BodyHandlers.ofInputStream());
        assertThat(response.headers().firstValue("Content-Type"))
                .hasValueSatisfying(v -> assertThat(v).startsWith("text/event-stream"));
        List<JsonNode> events = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.body(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("data: ")) {
                    events.add(json.readTree(line.substring("data: ".length())));
                }
            }
        }
        assertThat(events.get(0).path("result").path("task").path("status").path("state").asText())
                .isEqualTo("TASK_STATE_SUBMITTED");
        JsonNode last = events.get(events.size() - 1).path("result");
        assertThat(last.path("statusUpdate").path("status").path("state").asText()).isEqualTo("TASK_STATE_COMPLETED");
        JsonNode artifact = events.get(events.size() - 2).path("result").path("artifactUpdate");
        assertThat(artifact.path("artifact").path("parts").get(0).path("text").asText()).isEqualTo("streamed answer");
        for (JsonNode event : events) {
            assertNoKind(event);
            JsonNode result = event.path("result");
            assertThat(result.size()).as("a StreamResponse holds exactly one member: " + result).isEqualTo(1);
        }
        String taskId = events.get(0).path("result").path("task").path("id").asText();
        // §9.4.6: no subscription to a task in a terminal state
        JsonNode subscribe = rpc("1.0", call("SubscribeToTask", "{\"id\":\"" + taskId + "\"}"));
        assertThat(subscribe.path("error").path("code").asInt()).isEqualTo(-32004);
    }

    @Test
    @Timeout(60)
    void pushConfigsAreCreatedReadListedAndDeletedAndPostStreamResponses() throws Exception {
        String taskId = rpc("1.0", send("SendMessage", "push me")).path("result").path("task").path("id").asText();
        String hook = "http://localhost:" + webhook.getAddress().getPort() + "/hook";
        JsonNode created = rpc("1.0", call("CreateTaskPushNotificationConfig", """
                {"taskId":"%s","url":"%s","token":"tok-1",
                 "authentication":{"scheme":"Bearer","credentials":"s3cret"}}""".formatted(taskId, hook)));
        String configId = created.path("result").path("id").asText();
        assertThat(configId).isNotBlank();
        assertThat(created.path("result").path("url").asText()).isEqualTo(hook);
        assertThat(created.path("result").has("token")).as("the token is never read back").isFalse();
        assertThat(created.path("result").path("authentication").has("credentials")).isFalse();

        JsonNode got = rpc("1.0", call("GetTaskPushNotificationConfig",
                "{\"taskId\":\"" + taskId + "\",\"id\":\"" + configId + "\"}"));
        assertThat(got.path("result").path("id").asText()).isEqualTo(configId);
        JsonNode listed = rpc("1.0", call("ListTaskPushNotificationConfigs", "{\"taskId\":\"" + taskId + "\"}"));
        assertThat(listed.path("result").path("configs")).hasSize(1);

        work("pushed answer", 0, 0);
        for (int i = 0; i < 200 && hooks.stream().noneMatch(h ->
                "TASK_STATE_COMPLETED".equals(h.body().path("task").path("status").path("state").asText())); i++) {
            Thread.sleep(50);
        }
        Hook done = hooks.get(hooks.size() - 1);
        assertThat(done.body().path("task").path("status").path("state").asText()).isEqualTo("TASK_STATE_COMPLETED");
        assertThat(done.authorization()).isEqualTo("Bearer s3cret");
        assertThat(done.token()).isEqualTo("tok-1");
        assertThat(done.contentType()).isEqualTo("application/a2a+json");
        hooks.forEach(h -> assertNoKind(h.body()));

        JsonNode deleted = rpc("1.0", call("DeleteTaskPushNotificationConfig",
                "{\"taskId\":\"" + taskId + "\",\"id\":\"" + configId + "\"}"));
        assertThat(deleted.has("error")).isFalse();
        assertThat(rpc("1.0", call("GetTaskPushNotificationConfig",
                "{\"taskId\":\"" + taskId + "\",\"id\":\"" + configId + "\"}")).path("error").path("code").asInt())
                .isEqualTo(-32001);
    }

    @Test
    void listTasksAndTheExtendedCardAreRefusedByName() throws Exception {
        // tasks are not scoped to a caller, and 1.0 lists only a caller's own
        assertThat(rpc("1.0", call("ListTasks", "{}")).path("error").path("code").asInt()).isEqualTo(-32004);
        assertThat(rpc("1.0", call("GetExtendedAgentCard", "{}")).path("error").path("code").asInt()).isEqualTo(-32007);
    }
}
