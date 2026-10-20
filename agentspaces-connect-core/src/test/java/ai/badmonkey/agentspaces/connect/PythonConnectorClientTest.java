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
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.connect.AssetInvocations.AssetInvocationResult;
import ai.badmonkey.agentspaces.connect.DataQueryEntries.DataQuery;
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
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A Python agent queries and invokes a Java connector's asset
 * (srad/agentspaces-js.md JS-C-14, the cross-language half; the mirror of
 * {@link TypeScriptConnectorTest}): a Java peer over TCP runs the {@code data}
 * space and a {@link ConnectorRuntime} serving an {@code orders} table with
 * one invoke operation, {@code reserve}; the Python worker
 * ({@code agentspaces-python/tests/workers/connect_worker.py}) dials it,
 * finds the AssetCard, queries through its {@code DataQueryClient} (the
 * identical query served from the space), invokes {@code reserve}, retries it
 * under the same invocation id, and asks for an unlisted operation: the
 * target ran exactly once, one result exists per invocation id, and the
 * unlisted operation was refused naming the rule. Needs the Python client's
 * virtualenv ({@code PYTHON}, or {@code /tmp/aspace-venv4/bin/python}) with
 * the {@code cryptography} package; skipped otherwise unless
 * {@code -Dtsclient.required=true}.
 */
class PythonConnectorClientTest {

    private static final Path ROOT = Path.of("").toAbsolutePath().getParent().getParent();
    private static final Path PYTHON_DIR = ROOT.resolve("agentspaces-python");
    private static final Path WORKER = PYTHON_DIR.resolve("tests/workers/connect_worker.py");
    private static final String PYTHON = System.getenv().getOrDefault("PYTHON", "/tmp/aspace-venv4/bin/python");
    private static final String FOUNDING = "py-connector-client-v1";
    private final CborCodec codec = CborCodec.defaultCodec();
    private PeerNode node;
    private ReplicatedSpace data;
    private ConnectorRuntime connector;
    private Process worker;

    /** The orders table with one allowlisted operation, {@code reserve}, recording every run by invocation id. */
    private static final class OrdersProvider implements AssetProvider {
        final TableAssetProvider table = new TableAssetProvider("orders", "postgres://ops/public.orders",
                "Customer order history with line items and settlement status", "com.example.OrderRow#v1",
                Duration.ofMinutes(5),
                List.of(Map.of("id", "1", "region", "eu", "amount", "120.00"),
                        Map.of("id", "2", "region", "us", "amount", "80.00"),
                        Map.of("id", "3", "region", "eu", "amount", "45.50")));
        final List<String> reserved = new CopyOnWriteArrayList<>();

        @Override
        public List<AssetCard> describeAssets(GroupId group, PeerId issuer, Instant now) {
            return table.describeAssets(group, issuer, now).stream().map(card -> {
                Map<String, String> access = new HashMap<>(card.access());
                access.put("mode", "read-only,invoke");
                access.put("invoke", AssetInvocations.CAPABILITY);
                access.put(AssetInvocations.INVOKE_OPS, "reserve");
                return new AssetCard(card.id(), card.issuer(), card.group(), card.issued(), card.ttl(), card.asset(),
                        card.uri(), card.description(), card.shape(), card.freshness(), card.costHints(),
                        Map.copyOf(access));
            }).toList();
        }

        @Override
        public QueryResult query(String asset, Map<String, String> parameters) {
            return table.query(asset, parameters);
        }

        @Override
        public InvokeResult invoke(String asset, String operation, Map<String, String> params, String invocationId) {
            if (!operation.equals("reserve")) {
                return InvokeResult.failed("bad-params", "provider", operation + " is not implemented");
            }
            reserved.add(invocationId);
            return InvokeResult.of(Map.of("reserved", params.getOrDefault("qty", "0"), "invocationId", invocationId));
        }
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (worker != null) {
            worker.destroy();
            worker.waitFor(5, TimeUnit.SECONDS);
            worker.destroyForcibly();
        }
        if (connector != null) {
            connector.close();
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

    private static boolean runs(List<String> command, Path directory) {
        try {
            Process process = new ProcessBuilder(command).directory(directory.toFile())
                    .redirectErrorStream(true).start();
            process.getInputStream().readAllBytes();
            return process.waitFor(30, TimeUnit.SECONDS) && process.exitValue() == 0;
        } catch (IOException | InterruptedException e) {
            return false;
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static String field(String json, String name) {
        Matcher matcher = Pattern.compile("\"" + name + "\":\"([^\"]+)\"").matcher(json);
        assertThat(matcher.find()).as(name + " in " + json).isTrue();
        return matcher.group(1);
    }

    @Test
    @Timeout(180)
    void aPythonAgentQueriesAndInvokesAJavaConnectorsAssetExactlyOnce() throws Exception {
        require(Files.exists(WORKER), "python connect worker at " + WORKER);
        require(runs(List.of(PYTHON, "-c", "import cryptography"), PYTHON_DIR),
                PYTHON + " with the cryptography package (set PYTHON to another interpreter)");
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
        data = DataSpaces.builder(runtime, DataSpaces.DEFAULT_NAME, identity, "connector")
                .settleWindow(Duration.ofMillis(150)).build();
        OrdersProvider provider = new OrdersProvider();
        connector = new ConnectorRuntime(data, discovery, identity, "java-connector", provider, groupId,
                Clock.systemUTC(), Duration.ofMinutes(1));
        connector.start();
        node.startTicking(Duration.ofMillis(250));

        worker = new ProcessBuilder(PYTHON, WORKER.toString(), "127.0.0.1", String.valueOf(port), FOUNDING, "90")
                .directory(PYTHON_DIR.toFile()).redirectErrorStream(true).start();
        StringBuilder output = new StringBuilder();
        Thread pump = Thread.ofVirtual().start(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(worker.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append('\n');
                    System.out.println("  [python connect] " + line);
                }
            } catch (IOException ignored) {
                // the worker ended
            }
        });
        assertThat(worker.waitFor(150, TimeUnit.SECONDS)).as("the worker finished\n" + output).isTrue();
        pump.join(5_000);
        String report = output.toString();
        assertThat(worker.exitValue()).as(report).isZero();
        assertThat(report).contains("\"ok\":true");

        // The query was answered by the Java runtime and the identical one served from the space.
        assertThat(report).contains("\"rows\":[\"1\",\"3\"]").contains("\"servedBy\":\"java-connector\"")
                .contains("\"fromCache\":false").contains("\"cached\":true")
                .contains("\"cacheHits\":1").contains("\"queriesSent\":1");
        assertThat(field(report, "connector")).isEqualTo(identity.peerId().value());
        assertThat(provider.table.executions()).as("one execution for two fetches").isEqualTo(1);
        assertThat(connector.executed()).isEqualTo(1);
        assertThat(data.readAll(Template.of(DataQuery.class), 16)).as("the query was taken and completed").isEmpty();

        // One invocation, then a forced retry under the same id: the target ran once, one result per id.
        String invocationId = field(report, "invocationId");
        assertThat(report).contains("\"reserved\":\"2\"").contains("\"retryOk\":true");
        assertThat(provider.reserved).as("the target ran exactly once").containsExactly(invocationId);
        assertThat(connector.invoked()).isEqualTo(1);
        assertThat(connector.repeated()).as("the retry was answered from the remembered result").isEqualTo(1);
        assertThat(data.readAllIssued(Template.of(AssetInvocationResult.class)
                .where("invocationId", eq(invocationId)), 16)).as("one result per invocationId").hasSize(1);

        // An operation the card does not list was refused by the connector and never run.
        assertThat(report).contains("\"refused\":\"unknown-operation\"").contains("\"refusedRule\":\"invoke-ops\"");
        assertThat(connector.refused()).isEqualTo(1);
        assertThat(provider.reserved).hasSize(1);
    }
}
