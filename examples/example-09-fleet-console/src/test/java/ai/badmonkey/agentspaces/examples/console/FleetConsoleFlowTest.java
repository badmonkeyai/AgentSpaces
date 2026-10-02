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
package ai.badmonkey.agentspaces.examples.console;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.console.ConsoleView;
import ai.badmonkey.agentspaces.console.FleetConsoleServer;
import ai.badmonkey.agentspaces.examples.console.FleetConsole.Peer;
import ai.badmonkey.agentspaces.examples.console.FleetConsole.TaskEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The served console over real TCP, crash included: everything the old
 * in-process view proved, now asserted through the HTTP surface an operator
 * (or an external dashboard) would actually consume.
 */
class FleetConsoleFlowTest {

    private final ObjectMapper json = new ObjectMapper();
    private final HttpClient http = HttpClient.newHttpClient();

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private JsonNode get(int port, String path) throws Exception {
        HttpResponse<String> response = http.send(HttpRequest.newBuilder()
                        .uri(URI.create("http://127.0.0.1:" + port + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(path).isEqualTo(200);
        return json.readTree(response.body());
    }

    @Test
    @Timeout(120)
    void theServedConsoleSeesQueueWipCompletionAndRecovery() throws Exception {
        int seedPort = freePort();
        Peer console = FleetConsole.startPeer("console", seedPort, 0);
        Peer workerA = FleetConsole.startPeer("worker-a", freePort(), seedPort);
        Peer workerB = FleetConsole.startPeer("worker-b", freePort(), seedPort);
        List<Peer> fleet = List.of(console, workerA, workerB);
        Thread threadA = null;
        Thread threadB = null;
        try (ConsoleView view = FleetConsole.view(console);
             FleetConsoleServer server = FleetConsoleServer.builder(view)
                     .fleetName("console-fleet")
                     .panel(FleetConsole.storyPanel(view))
                     .build()) {
            int port = server.start(0);
            threadA = FleetConsole.runWorker(workerA, console.identity().peerId(), 0); // dies on its first take
            threadB = FleetConsole.runWorker(workerB, console.identity().peerId(), -1);
            Thread.sleep(1500);

            for (int i = 1; i <= 3; i++) {
                console.tasks().write(new TaskEntry("t-" + i, i),
                        Lease.of(Duration.ofMinutes(10)));
            }

            // Everything completes despite worker-a's crash: its lease lapses
            // and worker-b picks the task back up. Wait on the attributed
            // findings, not just the completions: a result entry is written just
            // after its task completion, so per-worker counts trail completed by
            // a replication hop.
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            JsonNode overview = get(port, "/api/v1/overview");
            while (overview.get("perWorker").path("worker-b").asInt() < 3
                    && System.nanoTime() < deadline) {
                Thread.sleep(250);
                overview = get(port, "/api/v1/overview");
            }
            assertThat(overview.get("completed").asInt()).isEqualTo(3);
            assertThat(overview.get("written").asInt()).isEqualTo(3);
            assertThat(overview.get("queued").asInt()).isZero();
            assertThat(overview.get("inProgress").asInt()).isZero();
            assertThat(overview.get("members").asInt()).isEqualTo(2);
            // The crashed worker completed nothing; the survivor did it all.
            assertThat(overview.get("perWorker").get("worker-b").asInt()).isEqualTo(3);
            assertThat(overview.get("perWorker").has("worker-a")).isFalse();

            // The demo panel serves live data through the panel SPI.
            JsonNode story = get(port, "/api/v1/panels/story/data");
            assertThat(story.get("eventsObserved").asLong()).isGreaterThan(0);

            // The HAL root links the surface an external client would walk.
            assertThat(get(port, "/api/v1").get("_links").get("events")
                    .get("href").asText()).isEqualTo("/api/v1/events");
        } finally {
            if (threadA != null) {
                threadA.interrupt();
            }
            if (threadB != null) {
                threadB.interrupt();
            }
            fleet.forEach(Peer::close);
        }
    }

    @Test
    @Timeout(120)
    void theServedConsoleAcceptsAuthenticatedCommandsAndCommandsWorkers() throws Exception {
        String token = "flow-test-token";
        int seedPort = freePort();
        Peer console = FleetConsole.startPeer("console", seedPort, 0);
        Peer worker = FleetConsole.startPeer("worker-a", freePort(), seedPort);
        List<Peer> fleet = List.of(console, worker);
        ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace audit =
                ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace.builder(
                        console.runtime(), "c2-audit", console.identity(), "console")
                        .settleWindow(Duration.ofMillis(150)).build();
        Thread workerThread = null;
        try (ConsoleView view = FleetConsole.view(console);
             FleetConsoleServer server = FleetConsoleServer.builder(view)
                     .fleetName("console-fleet")
                     .commandAndControl(FleetConsole.commander(console, audit, view), token)
                     .build()) {
            int port = server.start(0);
            workerThread = FleetConsole.runWorker(worker, console.identity().peerId(), -1);
            Thread.sleep(1500);

            // Unauthenticated command is refused.
            assertThat(post(port, "/api/v1/commands/directive", null,
                    "{\"action\":\"PAUSE\",\"target\":\"*\"}").statusCode()).isEqualTo(401);

            // Operator dispatches a task through C2; a worker completes it.
            HttpResponse<String> dispatched = post(port, "/api/v1/commands/dispatch-task", token,
                    "{\"operator\":\"al\",\"args\":{\"topic\":\"c2 job\",\"priority\":\"5\"}}");
            assertThat(dispatched.statusCode()).isEqualTo(200);
            assertThat(json.readTree(dispatched.body()).get("ok").asBoolean()).isTrue();

            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            JsonNode overview = get(port, "/api/v1/overview");
            while (overview.get("completed").asInt() < 1 && System.nanoTime() < deadline) {
                Thread.sleep(250);
                overview = get(port, "/api/v1/overview");
            }
            assertThat(overview.get("completed").asInt()).isEqualTo(1);

            // A command shows in the activity stream, attributed to the
            // authenticated principal: under the static-token gate every holder is
            // "operator" (a JWT gate would attribute the token's subject), and the
            // body's self-declared name is never the attributed identity.
            assertThat(view.eventsAfter(0))
                    .anySatisfy(e -> {
                        assertThat(e.kind()).isEqualTo("command");
                        assertThat(e.worker()).isEqualTo("operator");
                    });

            // The catalog advertises the dispatch command and C2 is on.
            JsonNode catalog = get(port, "/api/v1/commands");
            assertThat(catalog.get("enabled").asBoolean()).isTrue();
            assertThat(catalog.get("commands").get(0).get("id").asText())
                    .isEqualTo("dispatch-task");
        } finally {
            if (workerThread != null) {
                workerThread.interrupt();
            }
            audit.close();
            fleet.forEach(Peer::close);
        }
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
}
