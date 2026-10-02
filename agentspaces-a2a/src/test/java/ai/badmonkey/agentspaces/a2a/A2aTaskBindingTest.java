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
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The full A2A task surface over a real space: message/send writes the entry,
 * a worker takes and completes it, tasks/get reads every state out of space
 * semantics, tasks/cancel withdraws.
 */
class A2aTaskBindingTest {

    private final ObjectMapper json = new ObjectMapper();
    private final HttpClient http = HttpClient.newHttpClient();
    private LocalSpace space;
    private A2aTaskBinding binding;
    private A2aGateway gateway;
    private int port;

    private static AgentCard workerCard() {
        PeerId peer = PeerId.fromPublicKey("worker".getBytes(StandardCharsets.UTF_8));
        GroupId group = GroupId.fromFounding("fleet".getBytes(StandardCharsets.UTF_8));
        return new AgentCard("aspace://" + group.value() + "/agent/worker",
                peer, group, Instant.parse("2026-01-01T00:00:00Z"), Duration.ofMinutes(15),
                new AgentId(peer, "worker"), "Answers questions", List.of("answer"),
                List.of(A2aTaskEntry.class.getName() + "#v1"),
                List.of(A2aTaskResult.class.getName() + "#v1"), Map.of());
    }

    @BeforeEach
    void setUp() throws Exception {
        PeerId peer = PeerId.fromPublicKey("gateway".getBytes(StandardCharsets.UTF_8));
        space = LocalSpace.builder("a2a-tasks", new AgentId(peer, "gateway")).build();
        binding = new A2aTaskBinding(space);
        gateway = new A2aGateway("fleet", "Acme", () -> List.of(workerCard()))
                .taskBinding(binding);
        port = gateway.start(0);
    }

    @AfterEach
    void tearDown() {
        gateway.close();
        space.close();
    }

    private JsonNode rpc(String agent, String body) throws Exception {
        HttpResponse<String> response = http.send(HttpRequest.newBuilder(
                        URI.create("http://localhost:" + port + "/agents/" + agent))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .header("Content-Type", "application/json")
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return json.readTree(response.body());
    }

    private JsonNode send(String agent, String text) throws Exception {
        return rpc(agent, """
                {"jsonrpc":"2.0","id":1,"method":"message/send","params":{"message":
                {"role":"user","parts":[{"kind":"text","text":"%s"}],"messageId":"m1"}}}
                """.formatted(text));
    }

    private JsonNode tasksGet(String taskId) throws Exception {
        return rpc("worker", """
                {"jsonrpc":"2.0","id":2,"method":"tasks/get","params":{"id":"%s"}}
                """.formatted(taskId));
    }

    /** A worker that takes one task and completes it with a reply. */
    private Thread worker(CountDownLatch proceed) {
        Thread thread = Thread.ofVirtual().start(() -> {
            try {
                var taken = space.take(Template.of(A2aTaskEntry.class),
                        Lease.of(Duration.ofMinutes(1)), Duration.ofSeconds(10)).orElseThrow();
                if (proceed != null) {
                    proceed.await(10, TimeUnit.SECONDS);
                }
                space.complete(taken, new A2aTaskResult(taken.entry().taskId(), "worker",
                        "the answer to '" + taken.entry().text() + "'"),
                        binding.resultLease());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        return thread;
    }

    @Test
    @Timeout(30)
    void aSentMessageIsWorkedByTheFleetAndCompletes() throws Exception {
        JsonNode sent = send("worker", "what is the backlog?");
        assertThat(sent.path("result").path("status").path("state").asText())
                .isEqualTo("submitted");
        String taskId = sent.path("result").path("id").asText();

        Thread thread = worker(null);
        JsonNode task = null;
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            task = tasksGet(taskId);
            if ("completed".equals(task.path("result").path("status")
                    .path("state").asText())) {
                break;
            }
            Thread.sleep(100);
        }
        thread.join(Duration.ofSeconds(5));

        assertThat(task.path("result").path("status").path("state").asText())
                .isEqualTo("completed");
        assertThat(task.path("result").path("artifacts").get(0)
                .path("parts").get(0).path("text").asText())
                .isEqualTo("the answer to 'what is the backlog?'");
        assertThat(task.path("result").path("history")).hasSize(2);
        assertThat(task.path("result").path("history").get(1).path("role").asText())
                .isEqualTo("agent");
    }

    @Test
    @Timeout(30)
    void aHeldTaskReportsWorking() throws Exception {
        String taskId = send("worker", "slow question")
                .path("result").path("id").asText();
        CountDownLatch proceed = new CountDownLatch(1);
        Thread thread = worker(proceed);
        try {
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            String state = "";
            while (System.nanoTime() < deadline) {
                state = tasksGet(taskId).path("result").path("status")
                        .path("state").asText();
                if ("working".equals(state)) {
                    break;
                }
                Thread.sleep(50);
            }
            assertThat(state).isEqualTo("working");
        } finally {
            proceed.countDown();
            thread.join(Duration.ofSeconds(10));
        }
    }

    @Test
    @Timeout(30)
    void cancelWithdrawsAnUnworkedTask() throws Exception {
        String taskId = send("worker", "never mind")
                .path("result").path("id").asText();

        JsonNode canceled = rpc("worker", """
                {"jsonrpc":"2.0","id":3,"method":"tasks/cancel","params":{"id":"%s"}}
                """.formatted(taskId));
        assertThat(canceled.path("result").path("status").path("state").asText())
                .isEqualTo("canceled");
        // The entry is gone: no worker can take it any more.
        assertThat(space.read(Template.of(A2aTaskEntry.class))).isEmpty();
    }

    @Test
    void protocolErrorsSpeakJsonRpc() throws Exception {
        assertThat(rpc("worker", "{not json").path("error").path("code").asInt())
                .isEqualTo(-32700);
        assertThat(rpc("worker", """
                {"jsonrpc":"2.0","id":9,"method":"tasks/nope","params":{}}
                """).path("error").path("code").asInt()).isEqualTo(-32601);
        assertThat(tasksGet("no-such-task").path("error").path("code").asInt())
                .isEqualTo(-32001);
        assertThat(send("nobody", "hello").path("error").path("code").asInt())
                .isEqualTo(-32602);
        assertThat(rpc("worker", """
                {"jsonrpc":"2.0","id":9,"method":"message/send","params":{"message":
                {"role":"user","parts":[{"kind":"file","uri":"x"}]}}}
                """).path("error").path("code").asInt()).isEqualTo(-32602);
    }

    /** ASF-007 / TECH-SPEC §9.4: request bodies over 64 KiB and text parts over 16 KiB are refused as JSON-RPC errors. */
    @Test
    void oversizedBodiesAndTextsAreRefused() throws Exception {
        String padding = "x".repeat(64 * 1024 + 1);
        JsonNode tooBig = rpc("worker", """
                {"jsonrpc":"2.0","id":1,"method":"tasks/get","params":{"id":"%s"}}
                """.formatted(padding));
        assertThat(tooBig.path("error").path("code").asInt()).isEqualTo(-32600);
        assertThat(tooBig.path("error").path("message").asText()).contains("65536");

        JsonNode tooLong = send("worker", "y".repeat(16 * 1024 + 1));
        assertThat(tooLong.path("error").path("code").asInt()).isEqualTo(-32602);
        assertThat(space.read(Template.of(A2aTaskEntry.class))).isEmpty();
    }

    /** TECH-SPEC §9.4: cancellation is not retroactive; a completed task stays completed with its artifact. */
    @Test
    @Timeout(30)
    void cancelIsNotRetroactiveOnACompletedTask() throws Exception {
        String taskId = send("worker", "finish first").path("result").path("id").asText();
        Thread thread = worker(null);
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline && !"completed".equals(
                tasksGet(taskId).path("result").path("status").path("state").asText())) {
            Thread.sleep(50);
        }
        thread.join(Duration.ofSeconds(5));

        JsonNode canceled = rpc("worker", """
                {"jsonrpc":"2.0","id":3,"method":"tasks/cancel","params":{"id":"%s"}}
                """.formatted(taskId));
        assertThat(canceled.path("result").path("status").path("state").asText())
                .isEqualTo("completed");
        assertThat(canceled.path("result").path("artifacts").get(0).path("parts").get(0)
                .path("text").asText()).isEqualTo("the answer to 'finish first'");
        assertThat(tasksGet(taskId).path("result").path("status").path("state").asText())
                .isEqualTo("completed");
    }

    /** A task lease short enough to reason about in a test, on a clock the test drives. */
    private static final Duration SHORT_TASK_LEASE = Duration.ofMinutes(1);

    /**
     * A space, binding, and gateway that all read one {@link TestClock}, so a
     * test moves time explicitly instead of sleeping (TECH-SPEC §9.4).
     */
    private record Clocked(TestClock clock, LocalSpace space, A2aGateway gateway, int port)
            implements AutoCloseable {

        static Clocked start(String spaceName) throws java.io.IOException {
            TestClock clock = TestClock.create();
            PeerId peer = PeerId.fromPublicKey(spaceName.getBytes(StandardCharsets.UTF_8));
            LocalSpace space = LocalSpace.builder(spaceName, new AgentId(peer, "gateway"))
                    .clock(clock).build();
            A2aGateway gateway = new A2aGateway("fleet", "Acme", () -> List.of(workerCard()))
                    .taskBinding(new A2aTaskBinding(space, Lease.of(SHORT_TASK_LEASE),
                            Lease.of(Duration.ofHours(1)), clock));
            return new Clocked(clock, space, gateway, gateway.start(0));
        }

        @Override
        public void close() {
            gateway.close();
            space.close();
        }
    }

    private JsonNode rpcAt(int atPort, String body) throws Exception {
        HttpResponse<String> response = http.send(HttpRequest.newBuilder(
                        URI.create("http://localhost:" + atPort + "/agents/worker"))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .header("Content-Type", "application/json")
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return json.readTree(response.body());
    }

    private String sendAt(int atPort, String text) throws Exception {
        return rpcAt(atPort, """
                {"jsonrpc":"2.0","id":1,"method":"message/send","params":{"message":
                {"role":"user","parts":[{"kind":"text","text":"%s"}],"messageId":"m1"}}}
                """.formatted(text)).path("result").path("id").asText();
    }

    private JsonNode statusAt(int atPort, String taskId) throws Exception {
        return rpcAt(atPort, """
                {"jsonrpc":"2.0","id":2,"method":"tasks/get","params":{"id":"%s"}}
                """.formatted(taskId)).path("result").path("status");
    }

    /** SPEC §12 / TECH-SPEC §9.4: an unworked task whose write lease lapsed reads {@code failed}, with a status message saying no worker took it in time. The space and the binding share one test clock, so no sleeping. */
    @Test
    @Timeout(30)
    void aLapsedUnworkedTaskReadsFailed() throws Exception {
        try (Clocked lapsing = Clocked.start("a2a-lapse")) {
            String taskId = sendAt(lapsing.port(), "never taken");
            assertThat(statusAt(lapsing.port(), taskId).path("state").asText())
                    .isEqualTo("submitted");

            lapsing.clock().advance(SHORT_TASK_LEASE.plusMillis(1));
            lapsing.space().sweepNow();
            assertThat(lapsing.space().read(Template.of(A2aTaskEntry.class))).isEmpty();

            JsonNode status = statusAt(lapsing.port(), taskId);
            assertThat(status.path("state").asText()).isEqualTo("failed");
            assertThat(status.path("message").path("role").asText()).isEqualTo("agent");
            assertThat(status.path("message").path("parts").get(0).path("text").asText())
                    .isEqualTo("no worker took the task before its lease expired");
        }
    }

    /** TECH-SPEC §9.4: a taken entry is gone from readers; while the write lease the binding recorded is still live on the shared clock that reads {@code working}, and once the clock passes the deadline it reads {@code failed}. */
    @Test
    @Timeout(30)
    void aTaskStillInsideItsLeaseReadsWorkingOnTheSharedClock() throws Exception {
        try (Clocked held = Clocked.start("a2a-held")) {
            String taskId = sendAt(held.port(), "slow question");
            // A live take with a lease longer than the task lease: the entry is
            // consumed and stays consumed until the write lease drops it.
            var taken = held.space().take(Template.of(A2aTaskEntry.class),
                    Lease.of(SHORT_TASK_LEASE.multipliedBy(5)), Duration.ofSeconds(1))
                    .orElseThrow();
            assertThat(taken.entry().taskId()).isEqualTo(taskId);
            assertThat(held.space().read(Template.of(A2aTaskEntry.class))).isEmpty();

            held.clock().advance(SHORT_TASK_LEASE.minusMillis(1));
            held.space().sweepNow();
            assertThat(statusAt(held.port(), taskId).path("state").asText())
                    .isEqualTo("working");

            held.clock().advance(Duration.ofMillis(2));
            held.space().sweepNow();
            JsonNode status = statusAt(held.port(), taskId);
            assertThat(status.path("state").asText()).isEqualTo("failed");
            assertThat(status.path("message").path("parts").get(0).path("text").asText())
                    .isEqualTo("no worker took the task before its lease expired");
        }
    }
}
