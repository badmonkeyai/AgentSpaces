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
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.pdt.Pdt;
import ai.badmonkey.agentspaces.pdt.values.Codec;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.planner.StoreChooser;
import ai.badmonkey.agentspaces.store.ContentStore;
import ai.badmonkey.agentspaces.store.StorePolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code agentspaces.store.*} (PDT 2 phase 3, P2-S-10, P2-Q-MNT-01; srad/agentspaces-pdt2.md §7.2, appendix C):
 * the starter turns a peer into a content store over its group's facade and swarm, which then advertises the PDT
 * feature level 2; a member pins a tree on it over real sockets; a store without the swarm or the facade refuses
 * to start naming the keys; the keys reach the policy, a value out of range names its key, and a
 * {@code StorePolicy} bean replaces the keys.
 */
class StoreAutoConfigurationTest {

    private static final Codec<String> STR = Codec.of(String.class);

    private final AgentSpacesAutoConfiguration autoConfig = new AgentSpacesAutoConfiguration();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    private record Wired(AgentSpaces spaces, AgentSpacesLifecycle lifecycle, Pdts pdts) {
        Content content() {
            return (Content) spaces.group("fleet").provider(Content.TYPE).orElseThrow();
        }

        Pdt pdt() {
            return pdts.forGroup("fleet");
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

    private static AgentSpacesProperties properties(int port, int seedPort, Path dir, boolean store) {
        AgentSpacesProperties properties = new AgentSpacesProperties();
        properties.setBind("127.0.0.1:" + port);
        properties.setTickMillis(100);
        AgentSpacesProperties.SpaceDef tasks = new AgentSpacesProperties.SpaceDef();
        tasks.setName("tasks");
        tasks.setSettleWindowMillis(50);
        AgentSpacesProperties.Group group = new AgentSpacesProperties.Group();
        group.setName("fleet");
        group.setFounding("spring-store-test-v1");
        if (seedPort > 0) {
            group.setSeeds(List.of("127.0.0.1:" + seedPort));
        }
        group.setSpaces(List.of(tasks));
        properties.setGroups(List.of(group));
        properties.getContent().setEnabled(true);
        properties.getContent().getStore().setBackends(List.of("memory"));
        properties.getContent().getStore().setPath(dir.toString());
        properties.getPdt().setEnabled(true);
        properties.getPdt().getStore().setKind("memory");
        if (store) {
            properties.getStore().setEnabled(true);
            properties.getStore().setName("site-a");
            properties.getStore().setCapacity("1GiB");
            properties.getStore().setAcceptRequesters("open");
        }
        return properties;
    }

    private Wired wire(AgentSpacesProperties properties, Pdts pdts) {
        PeerIdentity identity = autoConfig.agentSpacesIdentity(properties);
        PeerNode node = autoConfig.agentSpacesNode(properties, identity);
        closeables.add(node::close);
        Authorizers authorizers = autoConfig.agentSpacesAuthorizers(properties, identity);
        InstantSource clock = InstantSource.system();
        AgentSpaces spaces = autoConfig.agentSpacesWithRegistry(properties, node, identity, authorizers,
                autoConfig.agentSpacesEmbedder(), clock,
                autoConfig.agentSpacesAgentIdentities(properties, identity, clock), null, List.of(),
                BootstrapChainFactory.defaults(), pdts);
        AgentSpacesLifecycle lifecycle = autoConfig.agentSpacesLifecycle(properties, node, spaces, authorizers);
        closeables.add(lifecycle::stop);
        return new Wired(spaces, lifecycle, pdts);
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
    void aStoreNodeTakesAMembersPinOverTcp(@TempDir Path dir) throws Exception {
        int portA = freePort();
        Wired a = wire(properties(portA, 0, dir.resolve("a"), true), new Pdts());
        a.lifecycle().start();
        Wired b = wire(properties(freePort(), portA, dir.resolve("b"), false), new Pdts());
        b.lifecycle().start();

        ContentStore store = a.pdts().stores().forGroup("fleet");
        assertThat(store.assetName()).isEqualTo("content-store:site-a");
        assertThat(a.content().pdtLevel()).isEqualTo(Content.PDT_LEVEL);
        assertThat(b.content().pdtLevel()).as("a member that is no store stays at level 1").isEqualTo(1);
        assertThat(b.pdts().stores().find("fleet")).isEmpty();

        PMap<String, String> value = b.pdt().mapRef("facts", STR, STR).swap(m -> m.assoc("invoice-1", "paid").assoc("invoice-2", "open"));
        assertThat(await(Duration.ofSeconds(60), () -> {
            try {
                b.pdt().storeFor(value.root(), StoreChooser.Need.of(0, 1), Duration.ofHours(1));
                return true;
            } catch (IllegalStateException noCardYet) {
                return false;
            }
        })).as("the store's card reached the member").isTrue();
        assertThat(await(Duration.ofSeconds(60), () -> store.ledger().holds(value.root()))).as(store.status().toString()).isTrue();
        assertThat(await(Duration.ofSeconds(30), () -> b.pdt().stores().receipts(value.root()).size() == 1)).isTrue();
    }

    @Test
    void aStoreWithoutTheSwarmOrTheFacadeRefusesToStartNamingTheKeys(@TempDir Path dir) throws Exception {
        AgentSpacesProperties noPdt = properties(freePort(), 0, dir.resolve("p"), true);
        noPdt.getPdt().setEnabled(false);
        assertThatThrownBy(() -> wire(noPdt, new Pdts())).hasMessageContaining("agentspaces.store.enabled=true")
                .hasMessageContaining("agentspaces.pdt.enabled=true");
        AgentSpacesProperties noContent = properties(freePort(), 0, dir.resolve("c"), true);
        noContent.getContent().setEnabled(false);
        noContent.getPdt().setEnabled(false);
        assertThatThrownBy(() -> wire(noContent, new Pdts())).hasMessageContaining("agentspaces.content.enabled=true");
    }

    @Test
    void theKeysReachThePolicyAndAStorePolicyBeanWins(@TempDir Path dir) throws Exception {
        AgentSpacesProperties.StorePeer config = new AgentSpacesProperties.StorePeer();
        config.setCapacity("2TiB");
        config.setReserve("10%");
        config.setMaxObject("8GiB");
        config.setLeaseDefault(Duration.ofDays(7));
        config.setLeaseMax(Duration.ofDays(90));
        config.setCopiesMax(2);
        config.setAcceptKinds(List.of("tree"));
        config.setAcceptRequesters("open");
        config.setBlind(true);
        config.setTier("cold");
        config.setPricePerGibMonth(3);
        StorePolicy policy = ContentStores.policy(config);
        assertThat(policy).isEqualTo(new StorePolicy(2L << 40, (2L << 40) / 10, 8L << 30, Duration.ofDays(7),
                Duration.ofDays(90), 2, Set.of("tree"), StorePolicy.Accept.OPEN, true, "cold", 3));

        config.setTier("warm");
        assertThatThrownBy(() -> ContentStores.policy(config)).hasMessageContaining("agentspaces.store.tier");
        config.setTier("hot");
        config.setMaxObject("huge");
        assertThatThrownBy(() -> ContentStores.policy(config)).hasMessageContaining("agentspaces.store.max-object");
        config.setCapacity(null);
        assertThatThrownBy(() -> ContentStores.policy(config)).hasMessageContaining("agentspaces.store.capacity is required");

        Pdts withBean = new Pdts();
        StorePolicy bean = StorePolicy.of(1L << 20).acceptRequesters(StorePolicy.Accept.OPEN);
        withBean.stores().userPolicy(bean);
        Wired a = wire(properties(freePort(), 0, dir.resolve("bean"), true), withBean);
        a.lifecycle().start();
        assertThat(a.pdts().stores().forGroup("fleet").policy()).as("the StorePolicy bean replaces the keys").isSameAs(bean);
    }
}
