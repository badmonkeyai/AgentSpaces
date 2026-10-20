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
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.transport.http.WebSocketTransport;
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
 * The worked demonstration of srad/celld-assetcard-analysis.md §7 with a
 * Java querier (srad/agentspaces-js.md JS-C-12): a Java peer with a
 * {@code ws} listener runs the {@code data} space; the example Worker of
 * {@code @agentspaces/workers} runs under workerd (the runner
 * {@code workerd-runner.js}) with the {@code Inventory} Durable Object and
 * the connector cell dialing the Java seed; the Java agent discovers the
 * {@code workers://inventory/do/Inventory} card, queries the stock through
 * {@link DataQueryClient}, reserves through {@link AssetInvocations#invoke},
 * and retries under the same id: the object's {@code once} ran the body
 * once and one result exists per id. An unlisted operation and an
 * undeclared parameter are refused naming their rules. Needs Node, workerd,
 * esbuild, and the built package; skipped otherwise unless
 * {@code -Dtsclient.required=true}.
 */
class WorkerdCellConnectorTest {

    private static final Path ROOT = Path.of("").toAbsolutePath().getParent().getParent();
    private static final Path NODE_DIR = ROOT.resolve("agentspaces-typescript");
    private static final Path RUNNER = NODE_DIR.resolve("packages/workers/build/test/workers/workerd-runner.js");
    private static final Path WORKERD = NODE_DIR.resolve("node_modules/.bin/workerd");
    private static final String FOUNDING = "workerd-cell-v1";
    private final CborCodec codec = CborCodec.defaultCodec();
    private PeerNode node;
    private WebSocketTransport transport;
    private ReplicatedSpace data;
    private Process runner;

    @AfterEach
    void tearDown() throws Exception {
        if (runner != null) {
            runner.destroy();
            runner.waitFor(10, TimeUnit.SECONDS);
            runner.destroyForcibly();
        }
        if (data != null) {
            data.close();
        }
        if (node != null) {
            node.close();
        }
        if (transport != null) {
            transport.close();
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

    /** The runner's stdout lines, kept and echoed, with a waiter for a line matching a predicate. */
    private static final class Output {
        final List<String> lines = new CopyOnWriteArrayList<>();

        Output(Process process) {
            Thread.ofVirtual().start(() -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        lines.add(line);
                        System.out.println("  [workerd runner] " + line);
                    }
                } catch (IOException ignored) {
                    // the runner ended
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
            throw new AssertionError("the runner never printed what was awaited; output:\n" + String.join("\n", lines));
        }
    }

    @Test
    @Timeout(240)
    void aJavaAgentDiscoversQueriesAndReservesThroughTheConnectorCellExactlyOnce() throws Exception {
        require(Files.exists(RUNNER), "workers package built (npm run build in packages/workers) at " + NODE_DIR);
        require(Files.exists(WORKERD), "workerd installed (npm install --save-dev workerd) at " + NODE_DIR);
        GroupId groupId = GroupId.fromFounding(FOUNDING.getBytes(StandardCharsets.UTF_8));
        GroupAdvertisement group = new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(365), FOUNDING,
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
        PeerIdentity identity = PeerIdentity.generate();
        int port = freePort();
        node = PeerNode.builder(identity).channelAuth(PeerNode.ChannelAuth.SESSION).build();
        transport = new WebSocketTransport();
        node.listen(transport, "127.0.0.1:" + port);
        GroupRuntime runtime = node.joinGroup(group,
                new GroupMembership.Config(Duration.ofSeconds(30), Duration.ofSeconds(2), 2), List.of());
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, Clock.systemUTC()), codec,
                identity.peerId());
        data = DataSpaces.builder(runtime, DataSpaces.DEFAULT_NAME, identity, "agent")
                .settleWindow(Duration.ofMillis(150)).build();
        node.startTicking(Duration.ofMillis(250));

        runner = new ProcessBuilder("node", RUNNER.toString(), groupId.value(), "ws://127.0.0.1:" + port + "/aspace", "200")
                .directory(NODE_DIR.toFile()).redirectErrorStream(true).start();
        Output output = new Output(runner);
        String up = output.await(line -> line.contains("\"event\":\"up\"") || line.contains("\"event\":\"error\""),
                Duration.ofSeconds(120));
        assertThat(up).as("the cell came up and connected").contains("\"event\":\"up\"").contains("\"connected\":true");

        // The cards arrive on the ads stream: the class card and the two listed instances (JS-C-08).
        Optional<AssetCard> card = Optional.empty();
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (card.isEmpty() && System.nanoTime() < deadline) {
            card = discovery.find(AssetCard.class, c -> c.asset().equals("do/Inventory")).stream().findFirst();
            Thread.sleep(200);
        }
        assertThat(card).as("the connector cell's card\n" + String.join("\n", output.lines)).isPresent();
        assertThat(card.get().id()).isEqualTo("inventory:do/Inventory");
        assertThat(card.get().uri()).isEqualTo("workers://inventory/do/Inventory");
        assertThat(card.get().access()).containsEntry("query", "aspace:cap/data-query")
                .containsEntry("invoke", AssetInvocations.CAPABILITY).containsEntry("read-ops", "getInventory")
                .containsEntry("invoke-ops", "reserveStock,releaseStock").containsEntry("consistency", "single-writer")
                .containsEntry("object-param", "object").containsEntry("runtime", "workerd").containsEntry("connector", "cell");
        assertThat(AssetInvocations.invokeOps(card.get())).containsExactly("reserveStock", "releaseStock");
        assertThat(discovery.find(AssetCard.class, c -> c.asset().equals("do/Inventory/warehouse-7")))
                .as("the listed instance's card").isNotEmpty();

        // The read through the data space, answered by the cell from the Durable Object.
        DataQueryClient client = new DataQueryClient(data, "planner", Set.of(card.get().issuer()));
        Optional<DataQueryClient.Fetched> fetched = client.fetch("do/Inventory",
                Map.of("op", "getInventory", "object", "warehouse-7"), Duration.ofSeconds(30));
        assertThat(fetched).as("the query was answered\n" + String.join("\n", output.lines)).isPresent();
        assertThat(fetched.get().fromCache()).isFalse();
        assertThat(fetched.get().result().servedBy()).isEqualTo("connector");
        assertThat(fetched.get().result().error()).isNull();
        assertThat(fetched.get().result().rows()).containsExactly(Map.of("sku", "A-100", "qty", "10"));

        // One reservation, then a forced retry under the same id: the body ran once, one result per id.
        Map<String, String> params = Map.of("object", "warehouse-7", "sku", "A-100", "qty", "3");
        AssetInvocationResult result = AssetInvocations.invoke(data, card.get(), "reserveStock", params,
                Duration.ofSeconds(30)).get(40, TimeUnit.SECONDS);
        assertThat(result.ok()).as(result.toString()).isTrue();
        assertThat(result.result()).containsEntry("sku", "A-100").containsEntry("reserved", "3")
                .containsEntry("remaining", "7");
        output.await(line -> line.contains("\"event\":\"runs\"") && line.contains("\"durable\":1"), Duration.ofSeconds(10));
        AssetInvocationResult again = AssetInvocations.retry(data, card.get(), new AssetInvocation(
                AssetInvocations.VERSION, "do/Inventory", "reserveStock", params, result.invocationId()),
                Duration.ofSeconds(30)).get(40, TimeUnit.SECONDS);
        assertThat(again.ok()).isTrue();
        assertThat(again.result()).isEqualTo(result.result());
        assertThat(data.readAllIssued(Template.of(AssetInvocationResult.class)
                .where("invocationId", eq(result.invocationId())), 16)).as("one result per invocationId").hasSize(1);

        // JS-C-07: an operation the card does not list, and a parameter the operation does not declare.
        AssetInvocationResult refused = AssetInvocations.invoke(data, card.get(), "dropTable",
                Map.of("object", "warehouse-7"), Duration.ofSeconds(30)).get(40, TimeUnit.SECONDS);
        assertThat(refused.ok()).isFalse();
        assertThat(refused.error()).isEqualTo("unknown-operation");
        assertThat(refused.result()).containsEntry("rule", "invoke-ops");
        AssetInvocationResult badParam = AssetInvocations.invoke(data, card.get(), "releaseStock",
                Map.of("object", "warehouse-7", "sku", "A-100", "qty", "1", "sql", "x"), Duration.ofSeconds(30))
                .get(40, TimeUnit.SECONDS);
        assertThat(badParam.ok()).isFalse();
        assertThat(badParam.error()).isEqualTo("bad-params");
        assertThat(badParam.result()).containsEntry("rule", "params");

        // The stock after everything: reserved once. The instance card's canonical hash differs from the
        // first query's, so this one runs against the object rather than the still-fresh result.
        Optional<DataQueryClient.Fetched> after = client.fetch("do/Inventory/warehouse-7",
                Map.of("op", "getInventory"), Duration.ofSeconds(30));
        assertThat(after).isPresent();
        assertThat(after.get().result().rows()).containsExactly(Map.of("sku", "A-100", "qty", "7"));

        try (Writer stdin = new OutputStreamWriter(runner.getOutputStream(), StandardCharsets.UTF_8)) {
            stdin.write("stop\n");
        }
        assertThat(runner.waitFor(60, TimeUnit.SECONDS)).isTrue();
        assertThat(runner.exitValue()).as(String.join("\n", output.lines)).isZero();
        assertThat(output.await(line -> line.contains("\"event\":\"done\""), Duration.ofSeconds(5)))
                .as("the body ran exactly once").contains("\"durable\":1");
    }
}
