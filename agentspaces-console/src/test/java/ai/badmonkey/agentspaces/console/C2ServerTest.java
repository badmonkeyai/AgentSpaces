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
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** The C2 HTTP surface: token gating, directive broadcast, and the worker gate. */
class C2ServerTest {

    public record Job(String topic) {
    }

    private static final String TOKEN = "s3cr3t-operator-token";

    private final ObjectMapper json = new ObjectMapper();
    private final HttpClient http = HttpClient.newHttpClient();
    private final AgentId consoleId =
            new AgentId(PeerIdentity.generate().peerId(), "console");
    private Space tasks;
    private Space control;
    private ConsoleView view;
    private FleetConsoleServer server;
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        tasks = LocalSpace.builder("tasks", consoleId).build();
        control = LocalSpace.builder("fleet-control", consoleId).build();
        view = ConsoleView.builder()
                .space("tasks", tasks, Job.class)
                .space("control", control, Directive.class)
                .build();
        ConsoleCommand dispatch = new ConsoleCommand() {
            @Override
            public String id() {
                return "dispatch-task";
            }

            @Override
            public String title() {
                return "Dispatch a task";
            }

            @Override
            public List<Field> fields() {
                return List.of(Field.text("topic", "Topic"));
            }

            @Override
            public Outcome execute(Map<String, String> args, CommandContext ctx) {
                String id = ctx.dispatch("tasks", new Job(args.get("topic")),
                        Duration.ofMinutes(10));
                return Outcome.dispatched("queued " + args.get("topic"), id);
            }
        };
        FleetCommander commander = FleetCommander.builder(name -> switch (name) {
            case "tasks" -> tasks;
            case "fleet-control" -> control;
            default -> null;
        }).view(view).command(dispatch).build();
        server = FleetConsoleServer.builder(view)
                .fleetName("c2-fleet")
                .commandAndControl(commander, TOKEN)
                .build();
        port = server.start(0);
    }

    @AfterEach
    void tearDown() {
        server.close();
        view.close();
    }

    private HttpResponse<String> post(String path, String token, String body) throws Exception {
        HttpRequest.Builder req = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (token != null) {
            req.header("X-Console-Token", token);
        }
        return http.send(req.build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void theCatalogAdvertisesCommandsAndTheRootLinksThem() throws Exception {
        JsonNode root = json.readTree(get("/api/v1"));
        assertThat(root.get("commandAndControl").asBoolean()).isTrue();
        assertThat(root.get("_links").get("commands").get("href").asText())
                .isEqualTo("/api/v1/commands");

        JsonNode catalog = json.readTree(get("/api/v1/commands"));
        assertThat(catalog.get("enabled").asBoolean()).isTrue();
        assertThat(catalog.get("commands").get(0).get("id").asText()).isEqualTo("dispatch-task");
        assertThat(catalog.get("commands").get(0).get("fields").get(0).get("name").asText())
                .isEqualTo("topic");
    }

    @Test
    void commandsRequireAValidTokenAndThenTakeEffect() throws Exception {
        // No token, and a wrong token, are both refused.
        assertThat(post("/api/v1/commands/dispatch-task", null,
                "{\"args\":{\"topic\":\"x\"}}").statusCode()).isEqualTo(401);
        assertThat(post("/api/v1/commands/dispatch-task", "wrong",
                "{\"args\":{\"topic\":\"x\"}}").statusCode()).isEqualTo(401);

        // With the right token, the task is dispatched into the fabric.
        HttpResponse<String> ok = post("/api/v1/commands/dispatch-task", TOKEN,
                "{\"operator\":\"alice\",\"args\":{\"topic\":\"harden the build\"}}");
        assertThat(ok.statusCode()).isEqualTo(200);
        JsonNode doc = json.readTree(ok.body());
        assertThat(doc.get("ok").asBoolean()).isTrue();
        // The static gate names every holder "operator"; the body's self-declared
        // name is not the attributed identity (TODO-EFG §5).
        assertThat(doc.get("operator").asText()).isEqualTo("operator");
        String entryId = doc.get("entryId").asText();
        assertThat(tasks.readAll(Template.of(Job.class), 10))
                .containsExactly(new Job("harden the build"));

        // And the console can cancel what it dispatched.
        HttpResponse<String> cancelled = post("/api/v1/commands/cancel", TOKEN,
                "{\"operator\":\"alice\",\"entryId\":\"" + entryId + "\"}");
        assertThat(json.readTree(cancelled.body()).get("ok").asBoolean()).isTrue();
        assertThat(tasks.readAll(Template.of(Job.class), 10)).isEmpty();
    }

    @Test
    void aBroadcastDirectivePausesAndResumesAWorkerGate() throws Exception {
        try (DirectiveGate gate = DirectiveGate.attach(control, "worker-a", consoleId.peer())) {
            assertThat(gate.paused()).isFalse();

            HttpResponse<String> paused = post("/api/v1/commands/directive", TOKEN,
                    "{\"operator\":\"alice\",\"action\":\"PAUSE\",\"target\":\"worker-a\"}");
            assertThat(paused.statusCode()).isEqualTo(200);
            assertThat(gate.paused()).isTrue();
            assertThat(gate.draining()).isFalse();

            post("/api/v1/commands/directive", TOKEN,
                    "{\"operator\":\"alice\",\"action\":\"RESUME\",\"target\":\"worker-a\"}");
            assertThat(gate.paused()).isFalse();

            // A wildcard DRAIN reaches this worker and is terminal.
            post("/api/v1/commands/directive", TOKEN,
                    "{\"operator\":\"alice\",\"action\":\"DRAIN\",\"target\":\"*\"}");
            assertThat(gate.draining()).isTrue();
        }
    }

    @Test
    void aReadOnlyConsoleRefusesCommandsEntirely() throws Exception {
        try (ConsoleView plainView = ConsoleView.builder()
                .space("tasks", tasks, Job.class).build();
             FleetConsoleServer plain = FleetConsoleServer.builder(plainView)
                     .fleetName("read-only").build()) {
            int plainPort = plain.start(0);
            HttpResponse<String> response = http.send(HttpRequest.newBuilder()
                            .uri(URI.create("http://127.0.0.1:" + plainPort
                                    + "/api/v1/commands/directive"))
                            .header("X-Console-Token", TOKEN)
                            .POST(HttpRequest.BodyPublishers.ofString(
                                    "{\"action\":\"PAUSE\",\"target\":\"*\"}")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(404); // C2 not enabled
            assertThat(json.readTree(get(plainPort, "/api/v1")).has("commandAndControl")).isTrue();
            assertThat(json.readTree(get(plainPort, "/api/v1"))
                    .get("commandAndControl").asBoolean()).isFalse();
        }
    }

    private String get(String path) throws Exception {
        return get(port, path);
    }

    private String get(int p, String path) throws Exception {
        return http.send(HttpRequest.newBuilder()
                        .uri(URI.create("http://127.0.0.1:" + p + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString()).body();
    }
}
