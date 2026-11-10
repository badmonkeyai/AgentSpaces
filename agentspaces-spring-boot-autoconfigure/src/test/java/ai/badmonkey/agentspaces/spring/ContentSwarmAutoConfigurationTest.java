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
package ai.badmonkey.agentspaces.spring;

import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code agentspaces.content.*} (srad/large-networks-bulk-data.md §7.2): the
 * starter attaches the content swarm per group, escalates application-space
 * payloads above the threshold through it, and a second node reads the entry
 * back over real sockets after the swarm.
 */
class ContentSwarmAutoConfigurationTest {

    public record Blob(String name, byte[] bytes) {
    }

    private final AgentSpacesAutoConfiguration autoConfig = new AgentSpacesAutoConfiguration();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    private record Wired(PeerIdentity identity, PeerNode node, AgentSpaces spaces, AgentSpacesLifecycle lifecycle) {
        ReplicatedSpace tasks() {
            return (ReplicatedSpace) spaces.group("fleet").space("tasks");
        }

        Content content() {
            return (Content) spaces.group("fleet").provider(Content.TYPE).orElseThrow();
        }
    }

    @AfterEach
    void tearDown() {
        for (AutoCloseable closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // Best effort.
            }
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress())) {
            return socket.getLocalPort();
        }
    }

    private AgentSpacesProperties properties(int port, int seedPort, Path store) {
        AgentSpacesProperties properties = new AgentSpacesProperties();
        properties.setBind("127.0.0.1:" + port);
        properties.setTickMillis(100);
        AgentSpacesProperties.SpaceDef tasks = new AgentSpacesProperties.SpaceDef();
        tasks.setName("tasks");
        tasks.setSettleWindowMillis(50);
        AgentSpacesProperties.Group group = new AgentSpacesProperties.Group();
        group.setName("fleet");
        group.setFounding("spring-content-test-v1");
        if (seedPort > 0) {
            group.setSeeds(List.of("127.0.0.1:" + seedPort));
        }
        group.setSpaces(List.of(tasks));
        properties.setGroups(List.of(group));
        properties.getContent().setEnabled(true);
        properties.getContent().setEscalateAbove(256 * 1024);
        properties.getContent().setPieceSize(64 * 1024);
        properties.getContent().getStore().setBackends(List.of("file"));
        properties.getContent().getStore().setPath(store.toString());
        return properties;
    }

    private Wired wire(AgentSpacesProperties properties) {
        PeerIdentity identity = autoConfig.agentSpacesIdentity(properties);
        PeerNode node = autoConfig.agentSpacesNode(properties, identity);
        Authorizers authorizers = autoConfig.agentSpacesAuthorizers(properties, identity);
        AgentSpaces spaces = autoConfig.agentSpaces(properties, node, identity, authorizers,
                autoConfig.agentSpacesEmbedder());
        AgentSpacesLifecycle lifecycle = autoConfig.agentSpacesLifecycle(properties, node, spaces, authorizers);
        closeables.add(lifecycle::stop);
        closeables.add(node::close);
        return new Wired(identity, node, spaces, lifecycle);
    }

    private static boolean await(Duration limit, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + limit.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(50);
        }
        return condition.getAsBoolean();
    }

    @Test
    void aLargeEntryEscalatesThroughTheSwarmAndASecondNodeReadsIt(@TempDir Path dir) throws Exception {
        int portA = freePort();
        Wired a = wire(properties(portA, 0, dir.resolve("a")));
        a.lifecycle().start();
        Wired b = wire(properties(freePort(), portA, dir.resolve("b")));
        b.lifecycle().start();
        assertThat(a.content().capabilityType()).isEqualTo(Content.TYPE);
        assertThat(a.content().store().kind()).isEqualTo("file");
        assertThat(a.content().publishOptions().pieceSize()).as("piece-size reaches a publish").isEqualTo(64 * 1024);

        byte[] bytes = new byte[1024 * 1024 + 7];
        new Random(3).nextBytes(bytes);
        var handle = a.tasks().write(new Blob("model", bytes), Lease.of(Duration.ofHours(1)));
        assertThat(a.tasks().recordOf(handle.entryId()).orElseThrow().payloadRef()).startsWith("bafkrei");

        assertThat(await(Duration.ofSeconds(30), () -> b.tasks().read(Template.of(Blob.class)).isPresent()))
                .as("b: " + b.content().stats() + " a: " + a.content().stats()).isTrue();
        assertThat(b.tasks().read(Template.of(Blob.class)))
                .hasValueSatisfying(blob -> assertThat(blob.bytes()).isEqualTo(bytes));
        assertThat(b.content().store().roots()).hasSize(1);
    }

    /**
     * SPEC §6.1a.5 and §6.3 (v0.1.32; AppCard origin and formats, 2026-11-09): a group with a content swarm spills its
     * large cards through it, and {@code agentspaces.discovery.*} sets the cache's two bounds and the spill fetch bound;
     * the rendezvous multiplier applies to the defaults alone.
     */
    @Test
    void theSwarmServesTheCardSpillAndTheDiscoveryBoundsBind(@TempDir Path dir) throws Exception {
        AgentSpacesProperties properties = properties(freePort(), 0, dir.resolve("a"));
        properties.setRoles(List.of("RENDEZVOUS"));
        properties.getDiscovery().getCache().setMaxBytes(8L * 1024 * 1024);
        properties.getDiscovery().getSpill().setMaxFetchBytes(2L * 1024 * 1024);
        Wired a = wire(properties);
        ai.badmonkey.agentspaces.discovery.DiscoveryService discovery = a.spaces().group("fleet").discovery();
        assertThat(discovery.spiller().store()).isInstanceOf(ai.badmonkey.agentspaces.content.ContentSpillStore.class);
        assertThat(discovery.cache().maxBytes()).as("an explicit setting applies as given").isEqualTo(8L * 1024 * 1024);
        assertThat(discovery.cache().maxEntries()).as("the default, times four for a rendezvous")
                .isEqualTo(4 * ai.badmonkey.agentspaces.discovery.AdCache.DEFAULT_MAX_ENTRIES);
        assertThat(discovery.maxSpillFetchBytes()).isEqualTo(2L * 1024 * 1024);
        AgentSpacesProperties.Discovery defaults = new AgentSpacesProperties.Discovery();
        assertThat(defaults.getCache().getMaxEntries()).isNull();
        assertThat(defaults.getCache().getMaxBytes()).isNull();
        assertThat(defaults.getSpill().getMaxFetchBytes())
                .isEqualTo(ai.badmonkey.agentspaces.api.ad.CardSpill.DEFAULT_MAX_FETCH_BYTES);
    }

    /**
     * PDT 2 phase 2, the starter half of P2-H-04: {@code agentspaces.content.store.backends} selects the hash-keyed
     * piece store under its own directory beside the file store's, refuses it with encrypt-at-rest (its pieces are
     * named by their plaintext hashes), and the metadata lists the value.
     */
    @Test
    void theHashBackendIsSelectableAndTheMetadataNamesIt(@TempDir Path dir) throws Exception {
        AgentSpacesProperties properties = properties(freePort(), 0, dir.resolve("h"));
        properties.getContent().getStore().setBackends(List.of("hash"));
        Wired a = wire(properties);
        assertThat(a.content().store()).isInstanceOf(ai.badmonkey.agentspaces.content.HashPieceStore.class);
        assertThat(a.content().store().kind()).isEqualTo("hash");
        assertThat(dir.resolve("h").resolve("fleet.hash").resolve(ai.badmonkey.agentspaces.content.HashPieceStore.PIECES))
                .as("a directory of its own, beside the file store's").isDirectory();

        AgentSpacesProperties sealed = properties(freePort(), 0, dir.resolve("s"));
        sealed.getContent().getStore().setBackends(List.of("hash"));
        sealed.getContent().getStore().setEncryptAtRest(true);
        assertThatThrownBy(() -> wire(sealed)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("agentspaces.content.store.encrypt-at-rest");

        try (java.io.InputStream in = getClass().getResourceAsStream("/META-INF/additional-spring-configuration-metadata.json")) {
            com.fasterxml.jackson.databind.JsonNode hints = new com.fasterxml.jackson.databind.ObjectMapper().readTree(in).get("hints");
            java.util.List<String> values = new java.util.ArrayList<>();
            for (com.fasterxml.jackson.databind.JsonNode hint : hints) {
                if (hint.get("name").asText().equals("agentspaces.content.store.backends")) {
                    hint.get("values").forEach(v -> values.add(v.get("value").asText()));
                }
            }
            assertThat(values).containsExactly("file", "hash", "memory", "in-place");
        }
    }

    @Test
    void theSwarmNeedsTheCapabilityRuntimeAndAKnownBackend(@TempDir Path dir) throws Exception {
        AgentSpacesProperties noCaps = properties(freePort(), 0, dir.resolve("x"));
        noCaps.getCapabilities().setEnabled(false);
        assertThatThrownBy(() -> wire(noCaps)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("agentspaces.capabilities.enabled");

        AgentSpacesProperties badBackend = properties(freePort(), 0, dir.resolve("y"));
        badBackend.getContent().getStore().setBackends(List.of("s3"));
        assertThatThrownBy(() -> wire(badBackend)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("agentspaces.content.store.backends");
    }
}
