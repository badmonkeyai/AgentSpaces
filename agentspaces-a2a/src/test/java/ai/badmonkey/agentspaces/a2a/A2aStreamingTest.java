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
import ai.badmonkey.agentspaces.a2a.A2aMessages.A2aTaskEntry;
import ai.badmonkey.agentspaces.a2a.A2aMessages.A2aTaskResult;
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.InputStreamReader;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A2A streaming (message/stream, tasks/resubscribe): SSE events track the
 * task's life out of space semantics, ending with {@code final: true}.
 */
class A2aStreamingTest {

    private final ObjectMapper json = new ObjectMapper();
    private final HttpClient http = HttpClient.newHttpClient();
    private LocalSpace space;
    private A2aGateway gateway;
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
        space = LocalSpace.builder("a2a-stream", new AgentId(peer, "gateway")).build();
        gateway = new A2aGateway("fleet", "Acme", () -> List.of(workerCard()))
                .taskBinding(new A2aTaskBinding(space));
        port = gateway.start(0);
    }

    @AfterEach
    void tearDown() {
        gateway.close();
        space.close();
    }

    /** Reads SSE data lines off a streaming response until it closes. */
    private List<JsonNode> streamEvents(String body) throws Exception {
        HttpResponse<java.io.InputStream> response = http.send(HttpRequest.newBuilder(
                        URI.create("http://localhost:" + port + "/agents/worker"))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build(), HttpResponse.BodyHandlers.ofInputStream());
        assertThat(response.headers().firstValue("Content-Type"))
                .hasValueSatisfying(v -> assertThat(v).startsWith("text/event-stream"));
        List<JsonNode> events = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(response.body(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("data: ")) {
                    events.add(json.readTree(line.substring("data: ".length())));
                }
            }
        }
        return events;
    }

    @Test
    @Timeout(60)
    void messageStreamFollowsTheTaskToCompletion() throws Exception {
        // A worker that waits before taking (so the snapshot reads submitted)
        // and holds the take briefly (so a working poll lands) before completing.
        Thread worker = Thread.ofVirtual().start(() -> {
            try {
                Thread.sleep(400);
                var taken = space.take(Template.of(A2aTaskEntry.class),
                        Lease.of(Duration.ofMinutes(1)), Duration.ofSeconds(20))
                        .orElseThrow();
                Thread.sleep(400);
                space.complete(taken, new A2aTaskResult(taken.entry().taskId(),
                        "worker", "streamed answer"), Lease.of(Duration.ofHours(1)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        List<JsonNode> events = streamEvents("""
                {"jsonrpc":"2.0","id":7,"method":"message/stream","params":{"message":
                {"role":"user","parts":[{"kind":"text","text":"stream me"}],
                "messageId":"m1"}}}
                """);
        worker.join(Duration.ofSeconds(10));

        // First event: the task snapshot, still unclaimed.
        assertThat(events.get(0).path("result").path("kind").asText()).isEqualTo("task");
        assertThat(events.get(0).path("result").path("status").path("state").asText())
                .isEqualTo("submitted");
        assertThat(events.get(0).path("id").asInt()).isEqualTo(7);

        // Somewhere in the middle: the working status.
        assertThat(events).anySatisfy(event -> {
            assertThat(event.path("result").path("kind").asText())
                    .isEqualTo("status-update");
            assertThat(event.path("result").path("status").path("state").asText())
                    .isEqualTo("working");
            assertThat(event.path("result").path("final").asBoolean()).isFalse();
        });

        // The artifact arrives before the final status closes the stream.
        JsonNode last = events.get(events.size() - 1);
        assertThat(last.path("result").path("kind").asText()).isEqualTo("status-update");
        assertThat(last.path("result").path("status").path("state").asText())
                .isEqualTo("completed");
        assertThat(last.path("result").path("final").asBoolean()).isTrue();
        JsonNode artifact = events.get(events.size() - 2);
        assertThat(artifact.path("result").path("kind").asText())
                .isEqualTo("artifact-update");
        assertThat(artifact.path("result").path("artifact").path("parts").get(0)
                .path("text").asText()).isEqualTo("streamed answer");
    }

    @Test
    @Timeout(60)
    void resubscribingToACompletedTaskClosesImmediately() throws Exception {
        // Complete a task through the plain surface first.
        HttpResponse<String> sent = http.send(HttpRequest.newBuilder(
                        URI.create("http://localhost:" + port + "/agents/worker"))
                .POST(HttpRequest.BodyPublishers.ofString("""
                        {"jsonrpc":"2.0","id":1,"method":"message/send","params":
                        {"message":{"role":"user","parts":[{"kind":"text",
                        "text":"quick"}],"messageId":"m1"}}}
                        """))
                .build(), HttpResponse.BodyHandlers.ofString());
        String taskId = json.readTree(sent.body()).path("result").path("id").asText();
        var taken = space.take(Template.of(A2aTaskEntry.class),
                Lease.of(Duration.ofMinutes(1)), Duration.ofSeconds(5)).orElseThrow();
        space.complete(taken, new A2aTaskResult(taskId, "worker", "already done"),
                Lease.of(Duration.ofHours(1)));

        List<JsonNode> events = streamEvents("""
                {"jsonrpc":"2.0","id":2,"method":"tasks/resubscribe",
                "params":{"id":"%s"}}""".formatted(taskId));

        assertThat(events).hasSize(2);
        assertThat(events.get(0).path("result").path("status").path("state").asText())
                .isEqualTo("completed");
        assertThat(events.get(1).path("result").path("kind").asText())
                .isEqualTo("status-update");
        assertThat(events.get(1).path("result").path("final").asBoolean()).isTrue();
    }

    @Test
    void streamingCapabilityIsAdvertisedWhenTheBindingIsAttached() throws Exception {
        HttpResponse<String> card = http.send(HttpRequest.newBuilder(URI.create(
                        "http://localhost:" + port + "/.well-known/agent-card.json"))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(json.readTree(card.body()).path("capabilities")
                .path("streaming").asBoolean()).isTrue();
    }

    @Test
    void resubscribingToAnUnknownTaskIsAJsonRpcError() throws Exception {
        HttpResponse<String> response = http.send(HttpRequest.newBuilder(
                        URI.create("http://localhost:" + port + "/agents/worker"))
                .POST(HttpRequest.BodyPublishers.ofString("""
                        {"jsonrpc":"2.0","id":3,"method":"tasks/resubscribe",
                        "params":{"id":"nope"}}"""))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(json.readTree(response.body()).path("error").path("code").asInt())
                .isEqualTo(-32001);
    }
}
