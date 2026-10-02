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
import ai.badmonkey.agentspaces.common.crypto.GroupKey;
import ai.badmonkey.agentspaces.common.crypto.GroupKeyRing;
import ai.badmonkey.agentspaces.identity.FileKeystore;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SPEC §11a.3 v0.1.13 (TODO-9-10-11 D7): the starter's content-key rotation.
 * A group with a {@code content-key} gets one ring shared by its spaces; the
 * key-wrap capability serves and follows it; a granted rotator rotates on
 * demand ({@code rotateContentKey}) or on {@code rotate-every}; the ring
 * persists under the keystore; and misconfigurations fail at startup.
 */
class ContentKeyRotationAutoConfigurationTest {

    private final AgentSpacesAutoConfiguration autoConfig = new AgentSpacesAutoConfiguration();
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final String contentKey = Base64.getEncoder().encodeToString(GroupKey.generate().rawBytes());

    private record Wired(PeerIdentity identity, PeerNode node, AgentSpaces spaces,
                         AgentSpacesLifecycle lifecycle) {
        ReplicatedSpace tasks() {
            return (ReplicatedSpace) spaces.group("fleet").space("tasks");
        }

        GroupKeyRing ring() {
            return tasks().keyRing().orElseThrow();
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
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private AgentSpacesProperties properties(int port, int seedPort, Path keystore) {
        AgentSpacesProperties properties = new AgentSpacesProperties();
        properties.setBind("127.0.0.1:" + port);
        properties.setTickMillis(100);
        if (keystore != null) {
            properties.setKeystore(keystore.toString());
        }
        AgentSpacesProperties.SpaceDef tasks = new AgentSpacesProperties.SpaceDef();
        tasks.setName("tasks");
        tasks.setSettleWindowMillis(50);
        AgentSpacesProperties.SpaceDef findings = new AgentSpacesProperties.SpaceDef();
        findings.setName("findings");
        findings.setSettleWindowMillis(50);
        AgentSpacesProperties.Group group = new AgentSpacesProperties.Group();
        group.setName("fleet");
        group.setFounding("spring-rotation-test-v1");
        group.setContentKey(contentKey);
        if (seedPort > 0) {
            group.setSeeds(List.of("127.0.0.1:" + seedPort));
        }
        group.setSpaces(List.of(tasks, findings));
        properties.setGroups(List.of(group));
        return properties;
    }

    /** A keystore whose peer holds the key-rotator grant. */
    private AgentSpacesProperties rotatorProperties(int port, int seedPort, Path keystore) {
        AgentSpacesProperties properties = properties(port, seedPort, keystore);
        PeerIdentity identity = FileKeystore.loadOrCreate(keystore);
        properties.getSecurity().getGrants().setKeyRotator(List.of(identity.peerId().value()));
        return properties;
    }

    private Wired wire(AgentSpacesProperties properties) {
        PeerIdentity identity = autoConfig.agentSpacesIdentity(properties);
        PeerNode node = autoConfig.agentSpacesNode(properties, identity);
        Authorizers authorizers = autoConfig.agentSpacesAuthorizers(properties, identity);
        AgentSpaces spaces = autoConfig.agentSpaces(properties, node, identity, authorizers,
                autoConfig.agentSpacesEmbedder());
        AgentSpacesLifecycle lifecycle =
                autoConfig.agentSpacesLifecycle(properties, node, spaces, authorizers);
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
    void aGroupsSpacesShareOneRingAndARotationReachesTheFleet(@TempDir Path dir) throws Exception {
        int portA = freePort();
        Wired a = wire(rotatorProperties(portA, 0, dir.resolve("a")));
        a.lifecycle().start();
        // Every member is configured with the same grants: b vouches only for
        // epochs minted by a peer its own configuration names a rotator.
        AgentSpacesProperties bProps = properties(freePort(), portA, dir.resolve("b"));
        bProps.getSecurity().getGrants().setKeyRotator(List.of(a.identity().peerId().value()));
        Wired b = wire(bProps);
        b.lifecycle().start();
        assertThat(((ReplicatedSpace) a.spaces().group("fleet").space("findings")).keyRing())
                .as("one ring per group, shared by its spaces").containsSame(a.ring());

        a.tasks().write(new TaskEntry("before", 1), Lease.of(Duration.ofHours(1)));
        assertThat(await(Duration.ofSeconds(10),
                () -> b.tasks().read(Template.of(TaskEntry.class)).isPresent())).isTrue();

        long epoch = a.spaces().group("fleet").rotateContentKey(Duration.ZERO);
        assertThat(epoch).isEqualTo(1);
        assertThat(await(Duration.ofSeconds(10), () -> b.ring().epochs().contains(1L)))
                .as("b followed the rotation").isTrue();
        var after = b.tasks().write(new TaskEntry("after", 2), Lease.of(Duration.ofHours(1)));
        assertThat(b.tasks().signedState(after.entryId()).orElseThrow().record().keyEpoch())
                .isEqualTo(1L);
        assertThat(await(Duration.ofSeconds(10),
                () -> a.tasks().readAll(Template.of(TaskEntry.class), 10).size() == 2))
                .as("a opens both epochs").isTrue();
        assertThatThrownBy(() -> b.spaces().group("fleet").rotateContentKey(Duration.ZERO))
                .as("b holds no key-rotator grant").isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rotatedEpochsPersistUnderTheKeystoreAcrossARestart(@TempDir Path dir) throws Exception {
        Path keystore = dir.resolve("a");
        Wired first = wire(rotatorProperties(freePort(), 0, keystore));
        first.lifecycle().start();
        first.spaces().group("fleet").rotateContentKey(Duration.ZERO);
        first.spaces().group("fleet").rotateContentKey(Duration.ZERO);
        assertThat(first.ring().epochs()).containsExactly(0L, 1L, 2L);
        byte[] epochTwo = first.ring().openingKeys(2).get(0).rawBytes();
        first.lifecycle().stop();
        first.node().close();
        assertThat(Files.list(keystore.resolve("content-keys")).toList()).hasSize(1);

        Wired second = wire(rotatorProperties(freePort(), 0, keystore));
        assertThat(second.ring().epochs()).as("restored from the keystore").containsExactly(0L, 1L, 2L);
        assertThat(second.ring().openingKeys(2).get(0).rawBytes())
                .isEqualTo(epochTwo);
    }

    /** A member not configured with the rotator's grant never installs its epochs. */
    @Test
    void aMemberWithoutTheRotatorsGrantRefusesItsEpochs(@TempDir Path dir) throws Exception {
        int portA = freePort();
        Wired a = wire(rotatorProperties(portA, 0, dir.resolve("a")));
        a.lifecycle().start();
        Wired b = wire(properties(freePort(), portA, dir.resolve("b")));
        b.lifecycle().start();
        assertThat(await(Duration.ofSeconds(10), () -> b.node().group(
                a.spaces().group("fleet").id()).orElseThrow().membership()
                .member(a.identity().peerId()).isPresent())).isTrue();
        a.spaces().group("fleet").rotateContentKey(Duration.ZERO);
        Thread.sleep(2000);
        assertThat(b.ring().epochs()).containsExactly(0L);
    }

    @Test
    void rotateEveryRotatesOnSchedule(@TempDir Path dir) throws Exception {
        AgentSpacesProperties properties = rotatorProperties(freePort(), 0, dir.resolve("a"));
        AgentSpacesProperties.ContentKeyRotation rotation =
                properties.getGroups().get(0).getContentKeyRotation();
        rotation.setRotateEvery(Duration.ofMillis(500));
        rotation.setCutoverDelay(Duration.ZERO);
        Wired a = wire(properties);
        a.lifecycle().start();
        assertThat(await(Duration.ofSeconds(10), () -> a.ring().newestEpoch() >= 2))
                .as("rotated twice on its own").isTrue();
    }

    @Test
    void misconfiguredRotationFailsAtStartup(@TempDir Path dir) throws Exception {
        AgentSpacesProperties ungranted = properties(freePort(), 0, dir.resolve("u"));
        ungranted.getGroups().get(0).getContentKeyRotation().setRotateEvery(Duration.ofHours(1));
        assertThatThrownBy(() -> wire(ungranted)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("may not rotate");

        AgentSpacesProperties keyless = rotatorProperties(freePort(), 0, dir.resolve("k"));
        keyless.getGroups().get(0).setContentKey(null);
        keyless.getGroups().get(0).getContentKeyRotation().setRotateEvery(Duration.ofHours(1));
        assertThatThrownBy(() -> wire(keyless)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no content-key");

        AgentSpacesProperties noWrap = rotatorProperties(freePort(), 0, dir.resolve("w"));
        noWrap.getCapabilities().setKeyWrap(false);
        noWrap.getGroups().get(0).getContentKeyRotation().setRotateEvery(Duration.ofHours(1));
        assertThatThrownBy(() -> wire(noWrap)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("key-wrap capability is off");
    }
}
