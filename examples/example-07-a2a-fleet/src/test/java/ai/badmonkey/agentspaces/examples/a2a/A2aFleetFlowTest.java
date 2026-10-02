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
package ai.badmonkey.agentspaces.examples.a2a;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ai.badmonkey.agentspaces.a2a.A2aGateway;
import ai.badmonkey.agentspaces.a2a.A2aTaskBinding;
import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.examples.a2a.A2aFleet.Peer;
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

/** An A2A round trip through a real TCP fleet. */
class A2aFleetFlowTest {

    private final ObjectMapper json = new ObjectMapper();
    private final HttpClient http = HttpClient.newHttpClient();

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private JsonNode post(String url, String body) throws Exception {
        return json.readTree(http.send(HttpRequest.newBuilder(URI.create(url))
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString()).body());
    }

    @Test
    @Timeout(90)
    void anA2aClientDrivesTheFleetToCompletion() throws Exception {
        int seedPort = freePort();
        Peer gatewayPeer = A2aFleet.startPeer("gateway", seedPort, 0);
        Peer analyst = A2aFleet.startPeer("analyst", freePort(), seedPort);
        Thread workerThread = A2aFleet.runWorker(analyst);
        try (A2aGateway gateway = new A2aGateway("a2a-fleet", "demo",
                () -> gatewayPeer.discovery().find(AgentCard.class, c -> true))
                .taskBinding(new A2aTaskBinding(gatewayPeer.tasks()))) {
            int port = gateway.start(0);
            String url = "http://localhost:" + port + "/agents/analyst";

            // The analyst's card reaches the gateway's cache over TCP gossip.
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (gatewayPeer.discovery().find(AgentCard.class, c -> true).isEmpty()
                    && System.nanoTime() < deadline) {
                Thread.sleep(100);
            }
            assertThat(gatewayPeer.discovery().find(AgentCard.class, c -> true))
                    .isNotEmpty();

            JsonNode sent = post(url, """
                    {"jsonrpc":"2.0","id":1,"method":"message/send","params":
                    {"message":{"role":"user","parts":[{"kind":"text",
                    "text":"reconcile the ledgers"}],"messageId":"m1"}}}
                    """);
            String taskId = sent.path("result").path("id").asText();
            assertThat(taskId).isNotEmpty();

            JsonNode task = sent;
            deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline) {
                task = post(url, """
                        {"jsonrpc":"2.0","id":2,"method":"tasks/get",
                        "params":{"id":"%s"}}""".formatted(taskId));
                if ("completed".equals(task.path("result").path("status")
                        .path("state").asText())) {
                    break;
                }
                Thread.sleep(200);
            }
            assertThat(task.path("result").path("status").path("state").asText())
                    .isEqualTo("completed");
            assertThat(task.path("result").path("artifacts").path(0)
                    .path("parts").path(0).path("text").asText())
                    .contains("reconcile the ledgers");
        } finally {
            workerThread.interrupt();
            List.of(gatewayPeer, analyst).forEach(Peer::close);
        }
    }
}
