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
package ai.badmonkey.agentspaces.connect;

import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.connect.AssetInvocations.AssetInvocation;
import ai.badmonkey.agentspaces.connect.AssetInvocations.AssetInvocationResult;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A TypeScript connector serves a Java agent (srad/agentspaces-js.md
 * JS-C-01, JS-C-03, JS-C-10, the cross-language half): a Java peer over TCP
 * runs the {@code data} space; the Node worker ({@code @agentspaces/connect}
 * on the TypeScript core) dials it and publishes an {@code orders} AssetCard
 * with one invoke operation, {@code reserve}; the Java agent discovers the
 * card, queries the asset through {@link DataQueryClient}, invokes
 * {@code reserve} through {@link AssetInvocations#invoke}, and retries the
 * invocation under the same id: the target ran exactly once and one result
 * exists per invocation id. An unlisted operation is refused by the
 * connector's {@code invoke-ops} rule. Needs Node and the built package;
 * skipped otherwise unless {@code -Dtsclient.required=true}.
 */
class TypeScriptConnectorTest {

    private static final Path ROOT = Path.of("").toAbsolutePath().getParent().getParent();
    private static final Path NODE_DIR = ROOT.resolve("agentspaces-typescript");
    private static final Path WORKER = NODE_DIR.resolve("packages/connect/build/test/workers/connector-worker.js");
    private static final String FOUNDING = "ts-connector-v1";
    private final CborCodec codec = CborCodec.defaultCodec();
    private PeerNode node;
    private ReplicatedSpace data;
    private Process worker;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (worker != null) {
            worker.destroy();
            worker.waitFor(5, TimeUnit.SECONDS);
            worker.destroyForcibly();
        }
        if (data != null) {
            data.close();
        }
        if (node != null) {
            node.close();
        }
    }

    private static void require(boolean condition, String why) {
        if (Boolean.getBoolean("tsclient.required")) {
            assertThat(condition).as(why).isTrue();
        }
        Assumptions.assumeTrue(condition, why);
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /** The worker's stdout lines, kept and echoed, with a waiter for a line matching a predicate. */
    private static final class Output {
        final List<String> lines = new CopyOnWriteArrayList<>();

        Output(Process process) {
            Thread.ofVirtual().start(() -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        lines.add(line);
                        System.out.println("  [node connector] " + line);
                    }
                } catch (IOException ignored) {
                    // the worker ended
                }
            });
        }

        String await(Predicate<String> wanted, Duration timeout) throws InterruptedException {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                for (String line : lines) {
                    if (wanted.test(line)) {
                        return line;
                    }
                }
                Thread.sleep(100);
            }
            throw new AssertionError("the worker never printed what was awaited; output:\n" + String.join("\n", lines));
        }

        long count(Predicate<String> wanted) {
            return lines.stream().filter(wanted).count();
        }
    }

    @Test
    @Timeout(180)
    void aJavaAgentDiscoversQueriesAndInvokesATypeScriptConnectorsAssetExactlyOnce() throws Exception {
        require(Files.exists(WORKER), "typescript connect package built (npm run build in packages/connect) at " + NODE_DIR);
        GroupId groupId = GroupId.fromFounding(FOUNDING.getBytes(StandardCharsets.UTF_8));
        GroupAdvertisement group = new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(365), FOUNDING,
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
        PeerIdentity identity = PeerIdentity.generate();
        int port = freePort();
        node = PeerNode.builder(identity).build();
        node.listen(new TcpTransport(), "127.0.0.1:" + port);
        GroupRuntime runtime = node.joinGroup(group,
                new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2), List.of());
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, Clock.systemUTC()), codec,
                identity.peerId());
        data = DataSpaces.builder(runtime, DataSpaces.DEFAULT_NAME, identity, "agent")
                .settleWindow(Duration.ofMillis(150)).build();
        node.startTicking(Duration.ofMillis(250));

        worker = new ProcessBuilder("node", WORKER.toString(), "127.0.0.1", String.valueOf(port), FOUNDING, "150")
                .directory(NODE_DIR.toFile()).redirectErrorStream(true).start();
        Output output = new Output(worker);
        String up = output.await(line -> line.contains("\"event\":\"up\""), Duration.ofSeconds(60));
        assertThat(up).contains("\"assets\":[\"orders\"]");

        // The card arrives on the ads stream; the agent discovers the asset as it would a Java connector's.
        Optional<AssetCard> card = Optional.empty();
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (card.isEmpty() && System.nanoTime() < deadline) {
            card = discovery.find(AssetCard.class, c -> c.asset().equals("orders")).stream().findFirst();
            Thread.sleep(200);
        }
        assertThat(card).as("the TypeScript connector's card\n" + String.join("\n", output.lines)).isPresent();
        assertThat(card.get().access()).containsEntry("query", "aspace:cap/data-query")
                .containsEntry("invoke", AssetInvocations.CAPABILITY).containsEntry("invoke-ops", "reserve");
        assertThat(AssetInvocations.invokeOps(card.get())).containsExactly("reserve");

        // A query through the data space, answered by the TypeScript runtime.
        DataQueryClient client = new DataQueryClient(data, "analyst", Set.of(card.get().issuer()));
        Optional<DataQueryClient.Fetched> fetched = client.fetch("orders", Map.of("region", "eu"),
                Duration.ofSeconds(30));
        assertThat(fetched).as("the query was answered\n" + String.join("\n", output.lines)).isPresent();
        assertThat(fetched.get().fromCache()).isFalse();
        assertThat(fetched.get().result().servedBy()).isEqualTo("ts-connector");
        assertThat(fetched.get().result().error()).isNull();
        assertThat(fetched.get().result().rows()).extracting(row -> row.get("id")).containsExactly("1", "3");

        // One invocation, then a forced retry under the same id: the target ran once, one result per id.
        AssetInvocationResult result = AssetInvocations.invoke(data, card.get(), "reserve",
                Map.of("sku", "A-1", "qty", "2"), Duration.ofSeconds(30)).get(40, TimeUnit.SECONDS);
        assertThat(result.ok()).as(result.toString()).isTrue();
        assertThat(result.result()).containsEntry("reserved", "2").containsEntry("invocationId", result.invocationId());
        output.await(line -> line.contains("\"event\":\"reserve\"") && line.contains(result.invocationId()),
                Duration.ofSeconds(10));
        AssetInvocationResult again = AssetInvocations.retry(data, card.get(), new AssetInvocation(
                AssetInvocations.VERSION, "orders", "reserve", Map.of("sku", "A-1", "qty", "2"),
                result.invocationId()), Duration.ofSeconds(30)).get(40, TimeUnit.SECONDS);
        assertThat(again.ok()).isTrue();
        output.await(line -> line.contains("\"event\":\"stats\"") && line.contains("\"repeated\":1"),
                Duration.ofSeconds(30));
        assertThat(output.count(line -> line.contains("\"event\":\"reserve\""))).as("the target ran exactly once")
                .isEqualTo(1);
        assertThat(data.readAllIssued(Template.of(AssetInvocationResult.class)
                .where("invocationId", eq(result.invocationId())), 16)).as("one result per invocationId").hasSize(1);

        // An operation the card does not list is refused by the connector and never run.
        AssetInvocationResult refused = AssetInvocations.invoke(data, card.get(), "drop", Map.of(),
                Duration.ofSeconds(30)).get(40, TimeUnit.SECONDS);
        assertThat(refused.ok()).isFalse();
        assertThat(refused.error()).isEqualTo("unknown-operation");
        assertThat(refused.result()).containsEntry("rule", "invoke-ops");
        assertThat(output.count(line -> line.contains("\"event\":\"reserve\""))).isEqualTo(1);

        try (Writer stdin = new OutputStreamWriter(worker.getOutputStream(), StandardCharsets.UTF_8)) {
            stdin.write("stop\n");
        }
        assertThat(worker.waitFor(30, TimeUnit.SECONDS)).isTrue();
        assertThat(worker.exitValue()).as(String.join("\n", output.lines)).isZero();
        assertThat(output.await(line -> line.contains("\"event\":\"done\""), Duration.ofSeconds(5)))
                .contains("\"runs\":1").contains("\"repeated\":1");
    }
}
