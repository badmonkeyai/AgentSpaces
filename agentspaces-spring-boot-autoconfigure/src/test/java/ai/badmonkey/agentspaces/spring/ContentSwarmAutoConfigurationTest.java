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
