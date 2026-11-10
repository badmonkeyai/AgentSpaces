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
import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Matchers;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.Pdt;
import ai.badmonkey.agentspaces.pdt.refs.MergeFunction;
import ai.badmonkey.agentspaces.pdt.refs.PConflict;
import ai.badmonkey.agentspaces.pdt.refs.Ref;
import ai.badmonkey.agentspaces.pdt.store.PackNodeStore;
import ai.badmonkey.agentspaces.pdt.values.Codec;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.pdt.values.PVector;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.ObjectProvider;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code agentspaces.pdt.*} (srad/agentspaces-pdt.md §7.2, PD-C-02,
 * PD-Q-AVL-02): the starter builds one {@code Pdt} per group before the
 * group's content swarm and attaches it after, a swap on one node is deref'd
 * on a second over real sockets, a required durable store without a path
 * refuses to start naming the key, a blank path without {@code require} falls
 * back to memory with a warning, a {@code NodeStore} bean wins over
 * {@code store.kind}, a {@code PdtMergeFunction} bean decides a forked vector
 * ref, every {@code agentspaces.pdt.*} figure reaches the facade (PD-Q-MNT-01),
 * and the {@code agentSpacesPdts} bean method picks both beans up.
 */
class PdtAutoConfigurationTest {

    private static final Codec<String> STR = Codec.of(String.class);

    private final AgentSpacesAutoConfiguration autoConfig = new AgentSpacesAutoConfiguration();
    private final List<AutoCloseable> closeables = new ArrayList<>();

    private record Wired(PeerIdentity identity, PeerNode node, AgentSpaces spaces, AgentSpacesLifecycle lifecycle,
                         Pdts pdts) {
        Content content() {
            return (Content) spaces.group("fleet").provider(Content.TYPE).orElseThrow();
        }

        Pdt pdt() {
            return pdts.forGroup("fleet");
        }

        Ref<PMap<String, String>> facts() {
            return pdt().mapRef("facts", STR, STR);
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

    private AgentSpacesProperties properties(int port, int seedPort, Path contentStore) {
        AgentSpacesProperties properties = new AgentSpacesProperties();
        properties.setBind("127.0.0.1:" + port);
        properties.setTickMillis(100);
        AgentSpacesProperties.SpaceDef tasks = new AgentSpacesProperties.SpaceDef();
        tasks.setName("tasks");
        tasks.setSettleWindowMillis(50);
        AgentSpacesProperties.Group group = new AgentSpacesProperties.Group();
        group.setName("fleet");
        group.setFounding("spring-pdt-test-v1");
        if (seedPort > 0) {
            group.setSeeds(List.of("127.0.0.1:" + seedPort));
        }
        group.setSpaces(List.of(tasks));
        properties.setGroups(List.of(group));
        properties.getContent().setEnabled(true);
        properties.getContent().getStore().setBackends(List.of("memory"));
        properties.getContent().getStore().setPath(contentStore.toString());
        properties.getPdt().setEnabled(true);
        properties.getPdt().getStore().setKind("memory");
        return properties;
    }

    private Wired wire(AgentSpacesProperties properties) {
        return wire(properties, new Pdts());
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
        return new Wired(identity, node, spaces, lifecycle, pdts);
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
    void theFacadeStartsOnTheMemoryStoreAndASwapReachesASecondNodeOverTcp(@TempDir Path dir) throws Exception {
        int portA = freePort();
        Wired a = wire(properties(portA, 0, dir.resolve("a")));
        a.lifecycle().start();
        Wired b = wire(properties(freePort(), portA, dir.resolve("b")));
        b.lifecycle().start();

        // The facade is built before the swarm, the swarm took its tree store, and it attached after.
        assertThat(a.pdts().groupNames()).containsExactly("fleet");
        assertThat(a.pdt().attached()).isTrue();
        assertThat(a.pdt().store().backend().kind()).as("store.kind=memory").isEqualTo("memory");
        assertThat(a.content().treeStore()).as("Content.Builder.treeStore").hasValue(a.pdt().treeStore());
        assertThat(a.content().describe(a.spaces().group("fleet").id()).parameters())
                .as("trees=1 advertised (PD-X-01)").containsEntry(Content.TREES_PARAMETER, Content.TREES_VERSION);
        assertThat(autoConfig.agentSpacesPdt(a.pdts(), a.spaces())).as("the Pdt bean").isSameAs(a.pdt());
        assertThat(a.pdt().refSpace().name()).isEqualTo("pdt");

        a.facts().swap(m -> m.assoc("claim-1", "the sky is blue"));
        assertThat(a.facts().deref().get("claim-1")).contains("the sky is blue");

        assertThat(await(Duration.ofSeconds(30), () -> b.facts().deref().get("claim-1").isPresent()))
                .as("b: " + b.pdt().stats() + " a: " + a.pdt().stats()).isTrue();
        assertThat(b.facts().deref().get("claim-1")).contains("the sky is blue");
        assertThat(b.pdt().stats().deltasInline()).as("the delta rode the head inline (PD-X-07)").isGreaterThanOrEqualTo(1);

        b.facts().swap(m -> m.assoc("claim-2", "water is wet"));
        assertThat(await(Duration.ofSeconds(30), () -> a.facts().deref().get("claim-2").isPresent()))
                .as("a: " + a.pdt().stats() + " b: " + b.pdt().stats()).isTrue();
        assertThat(a.facts().deref().count()).isEqualTo(2);
    }

    /**
     * PD-P-05, PD-C-01: with a content key and {@code agentspaces.pdt.sealing=sealed-nodes}
     * the starter hands the group's ring to the facade, so every node is written as a
     * sealed envelope under the ring's write epoch and a second node opens it with the
     * same key; without the ring the facade could neither write nor read a node.
     */
    @Test
    void aSealedNodesFacadeSealsUnderTheGroupRingAndASecondNodeOpensIt(@TempDir Path dir) throws Exception {
        String contentKey = java.util.Base64.getEncoder().encodeToString(
                ai.badmonkey.agentspaces.common.crypto.GroupKey.generate().rawBytes());
        int portA = freePort();
        AgentSpacesProperties propsA = properties(portA, 0, dir.resolve("a"));
        propsA.getGroups().get(0).setContentKey(contentKey);
        propsA.getPdt().setSealing("sealed-nodes");
        Wired a = wire(propsA);
        a.lifecycle().start();
        AgentSpacesProperties propsB = properties(freePort(), portA, dir.resolve("b"));
        propsB.getGroups().get(0).setContentKey(contentKey);
        propsB.getPdt().setSealing("sealed-nodes");
        Wired b = wire(propsB);
        b.lifecycle().start();

        assertThat(a.pdt().sealing()).isEqualTo(PRef.SEALING_SEALED_NODES);
        assertThat(a.pdt().store()).isInstanceOf(ai.badmonkey.agentspaces.pdt.seal.SealedNodeStore.class);
        assertThat(a.pdt().sealer()).isPresent();
        assertThat(a.pdt().sealer().orElseThrow().canSeal()).as("the facade holds the group's ring").isTrue();
        assertThat(a.content().keyRing()).isPresent();

        PMap<String, String> v1 = a.facts().swap(m -> m.assoc("claim-1", "the sky is blue"));
        assertThat(a.facts().deref().get("claim-1")).contains("the sky is blue");
        byte[] stored = a.pdt().store().stored(v1.root()).orElseThrow();
        assertThat(ai.badmonkey.agentspaces.pdt.seal.NodeSealer.isSealed(stored)).as("the root is an sn envelope").isTrue();
        assertThat(ai.badmonkey.agentspaces.pdt.seal.NodeSealer.decode(stored).epoch()).isZero();
        Template<PRef> template = Template.of(PRef.class).whereTag(PRef.NAME_TAG, Matchers.eq("facts"));
        assertThat(a.pdt().refSpace().readAllEntries(template, 16).get(0).value().sealing())
                .isEqualTo(PRef.SEALING_SEALED_NODES);

        assertThat(await(Duration.ofSeconds(30), () -> b.facts().deref().get("claim-1").isPresent()))
                .as("b: " + b.pdt().stats() + " a: " + a.pdt().stats()).isTrue();
        assertThat(b.pdt().sealing()).isEqualTo(PRef.SEALING_SEALED_NODES);
        assertThat(ai.badmonkey.agentspaces.pdt.seal.NodeSealer.isSealed(b.pdt().store().stored(v1.root()).orElseThrow()))
                .as("b holds the envelope, not the plaintext").isTrue();
        PMap<String, String> v2 = b.facts().swap(m -> m.assoc("claim-2", "water is wet"));
        assertThat(await(Duration.ofSeconds(30), () -> a.facts().deref().get("claim-2").isPresent())).isTrue();
        assertThat(ai.badmonkey.agentspaces.pdt.seal.NodeSealer.isSealed(a.pdt().store().stored(v2.root()).orElseThrow())).isTrue();
    }

    /** A record as the examples declare them: one large map field among small ones. */
    public record Doc(String id, java.util.Map<String, String> fields, int revision) {
    }

    private static java.util.Map<String, String> fields(int n, long seed) {
        java.util.Random random = new java.util.Random(seed);
        java.util.Map<String, String> fields = new java.util.TreeMap<>();
        for (int i = 0; i < n; i++) {
            char[] chars = new char[80];
            for (int c = 0; c < chars.length; c++) {
                chars[c] = (char) ('a' + random.nextInt(26));
            }
            fields.put("field-" + i, new String(chars));
        }
        return fields;
    }

    @Test
    void anOptedInSpaceEscalatesALargeEntryToTheTreesAndASecondWriteShipsItsDelta(@TempDir Path dir) throws Exception {
        int portA = freePort();
        AgentSpacesProperties propsA = properties(portA, 0, dir.resolve("a"));
        AgentSpacesProperties.PersistentTrees.SpaceEscalation tasks = new AgentSpacesProperties.PersistentTrees.SpaceEscalation();
        tasks.setEscalateAbove(64 * 1024);
        propsA.getPdt().getSpaces().put("tasks", tasks);
        Wired a = wire(propsA);
        a.lifecycle().start();
        AgentSpacesProperties propsB = properties(freePort(), portA, dir.resolve("b"));
        propsB.getPdt().getSpaces().put("tasks", tasks);
        Wired b = wire(propsB);
        b.lifecycle().start();
        Space tasksA = a.spaces().group("fleet").space("tasks");
        Space tasksB = b.spaces().group("fleet").space("tasks");
        // the writer escalates through the trees only once every member advertises them (PD-E-06)
        assertThat(await(Duration.ofSeconds(30), () -> a.content().advertisesTrees(b.identity().peerId())
                && b.content().advertisesTrees(a.identity().peerId()))).isTrue();

        java.util.Map<String, String> fields = fields(3_400, 1);
        Doc v1 = new Doc("doc-1", fields, 1);
        tasksA.write(v1, Lease.of(Duration.ofHours(1)));
        ai.badmonkey.agentspaces.pdt.escalation.PdtPayloads.Stats aAfterV1 = a.pdt().payloads().stats();
        assertThat(aAfterV1.published()).as("the 300 KiB entry went to the trees (PD-E-01)").isEqualTo(1);
        assertThat(await(Duration.ofSeconds(30), () -> tasksB.read(Template.of(Doc.class)).isPresent()
                && b.pdt().payloads().stats().fetched() == 1)).as("b: " + b.pdt().payloads()).isTrue();
        assertThat(tasksB.read(Template.of(Doc.class)).orElseThrow()).isEqualTo(v1);
        ai.badmonkey.agentspaces.pdt.escalation.PdtPayloads.Stats bAfterV1 = b.pdt().payloads().stats();

        java.util.Map<String, String> changed = new java.util.TreeMap<>(fields);
        changed.put("field-1500", "changed");
        tasksA.write(new Doc("doc-2", changed, 2), Lease.of(Duration.ofHours(1)));
        assertThat(await(Duration.ofSeconds(30), () -> tasksB.readAll(Template.of(Doc.class), 4).size() == 2
                && b.pdt().payloads().stats().fetched() == 2)).as("b: " + b.pdt().payloads()).isTrue();
        ai.badmonkey.agentspaces.pdt.escalation.PdtPayloads.Stats aAfterV2 = a.pdt().payloads().stats();
        ai.badmonkey.agentspaces.pdt.escalation.PdtPayloads.Stats bAfterV2 = b.pdt().payloads().stats();
        long delta = aAfterV2.bytesWritten() - aAfterV1.bytesWritten();
        long received = bAfterV2.bytesReceived() - bAfterV1.bytesReceived();
        System.out.printf("spring escalation over tcp: v1 tree %d bytes, b received %d; one field changed: delta %d, received %d%n",
                aAfterV1.bytesWritten(), bAfterV1.bytesReceived(), delta, received);
        assertThat(received).as("the second version moves as its delta over TCP (PD-E-01)").isLessThan(16 * 1024);
        assertThat(tasksB.readAll(Template.of(Doc.class), 4)).extracting(Doc::id).containsExactlyInAnyOrder("doc-1", "doc-2");

        // the key is refused without the facade
        AgentSpacesProperties off = properties(freePort(), 0, dir.resolve("c"));
        off.getPdt().setEnabled(false);
        off.getPdt().getSpaces().put("tasks", tasks);
        assertThatThrownBy(() -> wire(off)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("agentspaces.pdt.spaces.tasks.escalate-above")
                .hasMessageContaining("agentspaces.pdt.enabled");
    }

    @Test
    void aRequiredDurableStoreWithoutAPathRefusesToStart(@TempDir Path dir) throws Exception {
        AgentSpacesProperties noPath = properties(freePort(), 0, dir.resolve("x"));
        noPath.getPdt().getStore().setKind("pack");
        noPath.getPdt().getStore().setPath("");
        noPath.getPdt().getStore().setRequire(true);
        assertThatThrownBy(() -> wire(noPath)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("agentspaces.pdt.store.require")
                .hasMessageContaining("agentspaces.pdt.store.path");

        AgentSpacesProperties memoryRequired = properties(freePort(), 0, dir.resolve("y"));
        memoryRequired.getPdt().getStore().setRequire(true);
        assertThatThrownBy(() -> wire(memoryRequired)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("agentspaces.pdt.store.require")
                .hasMessageContaining("agentspaces.pdt.store.kind=memory");

        AgentSpacesProperties noBean = properties(freePort(), 0, dir.resolve("z"));
        noBean.getPdt().getStore().setKind("bean");
        assertThatThrownBy(() -> wire(noBean)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("agentspaces.pdt.store.kind=bean");
    }

    @Test
    void aBlankPackPathWithoutRequireFallsBackToMemoryWithAWarning(@TempDir Path dir) throws Exception {
        List<LogRecord> records = new CopyOnWriteArrayList<>();
        Logger logger = Logger.getLogger(Pdts.class.getName());
        Handler handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                records.add(record);
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        logger.addHandler(handler);
        try {
            AgentSpacesProperties blank = properties(freePort(), 0, dir.resolve("x"));
            blank.getPdt().getStore().setKind("pack");
            blank.getPdt().getStore().setPath("");
            blank.getPdt().getStore().setRequire(false);
            Wired wired = wire(blank);
            assertThat(wired.pdt().store().backend().kind()).as("memory instead of a pack").isEqualTo("memory");
            assertThat(Files.exists(dir.resolve("x").resolve("fleet"))).isFalse();
            assertThat(records).as("a warning names the key").anySatisfy(record -> {
                assertThat(record.getLevel()).isEqualTo(Level.WARNING);
                assertThat(java.text.MessageFormat.format(record.getMessage(), record.getParameters()))
                        .contains("agentspaces.pdt.store.path").contains("memory");
            });
        } finally {
            logger.removeHandler(handler);
        }
    }

    @Test
    void aMergeFunctionBeanDecidesAForkedVectorRef(@TempDir Path dir) throws Exception {
        MergeFunction<PVector<String>> append = (base, earlier, later) -> {
            PVector<String> out = earlier;
            for (long i = base == null ? 0 : base.count(); i < later.count(); i++) {
                out = out.conj(later.nth(i));
            }
            return out;
        };
        Wired wired = wire(properties(freePort(), 0, dir.resolve("x")),
                new Pdts((NodeStore) null, List.of(PdtMergeFunction.of("log", PRef.KIND_VECTOR, append))));
        wired.lifecycle().start();
        Ref<PVector<String>> log = wired.pdt().vectorRef("log", STR);
        PVector<String> v1 = log.swap(v -> v.conj("x"));
        PVector<String> v2a = log.swap(v -> v.conj("a"));
        // a second head off v1, as another member's swap that never saw v2a would write it
        PVector<String> v2b = log.at(v1.root()).conj("b");
        List<byte[]> nodes = new ArrayList<>();
        Set<String> shared = wired.pdt().store().reachable(v1.root());
        for (String cid : wired.pdt().store().reachable(v2b.root())) {
            if (!shared.contains(cid)) {
                nodes.add(wired.pdt().store().get(cid).orElseThrow());
            }
        }
        PRef fork = new PRef(PRef.VERSION, "log", PRef.KIND_VECTOR, v2b.root(), List.of(v1.root()), 2, v2b.count(), nodes,
                PRef.MODE_MULTI, PRef.SEALING_NONE, null, null);
        wired.pdt().refSpace().write(fork, Lease.of(Duration.ofMinutes(10)), fork.tags());
        assertThat(await(Duration.ofSeconds(10), () -> log.heads().heads().size() == 2)).as("two heads").isTrue();
        List<PConflict> conflicts = new CopyOnWriteArrayList<>();
        log.onConflict(conflicts::add);
        assertThat(log.deref().toList()).as("the registered function decided, not the whole-head rule").containsExactly("x", "a", "b");
        assertThat(conflicts).isEmpty();
        assertThat(wired.pdts().mergeFunctions()).hasSize(1);
        // the same name as a map is refused, since the bean named the vector kind
        assertThatThrownBy(() -> wired.pdt().mapRef("log", STR, STR)).isInstanceOf(IllegalArgumentException.class);
        // the bean method of the starter carries both beans into the holder through the providers
        NodeStore mine = NodeStore.inMemory();
        PdtMergeFunction bean = PdtMergeFunction.of("log", PRef.KIND_VECTOR, append);
        Pdts fromBeans = autoConfig.agentSpacesPdts(provider(mine), provider(bean), provider(null));
        assertThat(fromBeans.userStore()).contains(mine);
        assertThat(fromBeans.mergeFunctions()).containsExactly(bean);
        assertThat(autoConfig.agentSpacesPdts(provider(null), provider(null), provider(null)).mergeFunctions()).isEmpty();
    }

    private static <T> ObjectProvider<T> provider(T value) {
        return new ObjectProvider<>() {
            @Override
            public T getObject() {
                if (value == null) {
                    throw new IllegalStateException("no bean");
                }
                return value;
            }

            @Override
            public T getIfAvailable() {
                return value;
            }
        };
    }

    @Test
    void everyFigureOfTheBlockReachesTheFacade(@TempDir Path dir) throws Exception {
        AgentSpacesProperties properties = properties(freePort(), 0, dir.resolve("x"));
        properties.getPdt().getRetain().setVersions(8);
        properties.getPdt().getRetain().setAge(Duration.ofMinutes(20));
        properties.getPdt().setLoadTimeout(Duration.ofSeconds(3));
        properties.getPdt().setInlineDelta(1024);
        properties.getPdt().setFollow("lazy");
        properties.getPdt().setSealing("sealed-nodes");
        AgentSpacesProperties.PersistentTrees.RefDef linear = new AgentSpacesProperties.PersistentTrees.RefDef();
        linear.setMode("linear");
        properties.getPdt().getRefs().put("ledger", linear);
        Wired wired = wire(properties);
        wired.lifecycle().start();
        Pdt pdt = wired.pdt();
        assertThat(pdt.retention().versions()).isEqualTo(8);
        assertThat(pdt.retention().age()).isEqualTo(Duration.ofMinutes(20));
        assertThat(pdt.refs().config().loadTimeout()).isEqualTo(Duration.ofSeconds(3));
        assertThat(pdt.refs().config().inlineDelta()).isEqualTo(1024);
        assertThat(pdt.refs().config().followFull()).as("follow: lazy").isFalse();
        // the group has no content key, so the configured sealing drops to none, and that is what every head names
        assertThat(pdt.refs().config().sealing()).isEqualTo(PRef.SEALING_NONE);
        assertThat(pdt.mapRef("ledger", STR, STR).mode()).isEqualTo(PRef.MODE_LINEAR);
        Ref<PMap<String, String>> facts = wired.facts();
        facts.swap(m -> m.assoc("k", "v"));
        Template<PRef> template = Template.of(PRef.class).whereTag(PRef.NAME_TAG, Matchers.eq("facts"));
        List<Space.Entry<PRef>> heads = pdt.refSpace().readAllEntries(template, 16);
        assertThat(heads).hasSize(1);
        assertThat(heads.get(0).value().sealing()).isEqualTo(pdt.refs().config().sealing());
        assertThat(heads.get(0).value().inlineBytes()).isLessThanOrEqualTo(1024);
    }

    /** A note a task carries, for the verb whose trailing parameter takes a ref. */
    public record Note(String id, String text) {
    }

    /** A bean with a {@code @PdtRef} field and a verb whose trailing parameter is one (PDT 2 phase 7, P2-X-03). */
    @ai.badmonkey.agentspaces.agent.annotation.AgentSpec(name = "noter", description = "Notes facts", goals = {"note"})
    public static class Noter {
        @ai.badmonkey.agentspaces.agent.annotation.PdtRef("facts")
        Ref<PMap<String, String>> facts;

        @ai.badmonkey.agentspaces.agent.annotation.SpaceTake(space = "tasks", pollTimeout = "PT0.2S")
        public void note(Note note,
                         @ai.badmonkey.agentspaces.agent.annotation.PdtRef("notes") Ref<PMap<String, String>> notes) {
            notes.swap(m -> m.assoc(note.id(), note.text()));
        }
    }

    @Test
    void aBoundBeanGetsTheGroupFacadesRefsInAFieldAndAVerbParameter(@TempDir Path dir) throws Exception {
        Wired wired = wire(properties(freePort(), 0, dir.resolve("x")));
        wired.lifecycle().start();
        Noter noter = new Noter();

        wired.spaces().bind(noter);
        noter.facts.swap(m -> m.assoc("claim-1", "the sky is blue"));
        wired.spaces().group("fleet").space("tasks").write(new Note("n-1", "water is wet"), Lease.of(Duration.ofMinutes(5)));

        assertThat(wired.facts().deref().get("claim-1")).as("the field is the facade's ref").contains("the sky is blue");
        Ref<PMap<String, String>> notes = wired.pdt().mapRef("notes", STR, STR);
        assertThat(await(Duration.ofSeconds(10), () -> notes.deref().get("n-1").isPresent())).isTrue();

        AgentSpacesProperties off = properties(freePort(), 0, dir.resolve("y"));
        off.getPdt().setEnabled(false);
        Wired plain = wire(off);
        assertThatThrownBy(() -> plain.spaces().bind(new Noter())).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("agentspaces.pdt.enabled");
    }

    @Test
    void aRefsFollowAndTheStoreQuotaApplyToTheFacade(@TempDir Path dir) throws Exception {
        AgentSpacesProperties properties = properties(freePort(), 0, dir.resolve("x"));
        AgentSpacesProperties.PersistentTrees.RefDef cold = new AgentSpacesProperties.PersistentTrees.RefDef();
        cold.setFollow("lazy");
        properties.getPdt().getRefs().put("archive", cold);
        properties.getPdt().getStore().setQuota(1024 * 1024);
        Wired wired = wire(properties);

        // PDT 2 phase 7, P2-X-08: the per-ref follow and the quota behind the bridge, in place of the logged notes
        assertThat(wired.pdt().refs().config().followsFully("archive")).isFalse();
        assertThat(wired.pdt().refs().config().followsFully("facts")).isTrue();
        assertThat(wired.content().treeStore()).hasValue(wired.pdt().treeStore());
        assertThat(((ai.badmonkey.agentspaces.pdt.swarm.NodeStoreTreeStore) wired.pdt().treeStore()).quota())
                .isEqualTo(1024 * 1024);
    }

    @Test
    void theTreesNeedTheContentSwarm(@TempDir Path dir) throws Exception {
        AgentSpacesProperties noContent = properties(freePort(), 0, dir.resolve("x"));
        noContent.getContent().setEnabled(false);
        assertThatThrownBy(() -> wire(noContent)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("agentspaces.pdt.enabled")
                .hasMessageContaining("agentspaces.content.enabled");

        AgentSpacesProperties off = properties(freePort(), 0, dir.resolve("y"));
        off.getPdt().setEnabled(false);
        Wired plain = wire(off);
        assertThat(plain.pdts().isEmpty()).isTrue();
        assertThatThrownBy(() -> plain.pdts().forGroup("fleet")).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("agentspaces.pdt.enabled");
        assertThat(plain.content().treeStore()).as("no trees without the facade").isEmpty();
    }

    @Test
    void aUserNodeStoreBeanWinsOverTheStoreKind(@TempDir Path dir) throws Exception {
        NodeStore mine = NodeStore.inMemory();
        AgentSpacesProperties properties = properties(freePort(), 0, dir.resolve("x"));
        properties.getPdt().getStore().setKind("pack");
        properties.getPdt().getStore().setPath(dir.resolve("packs").toString());
        properties.getPdt().getStore().setRequire(true);
        Wired wired = wire(properties, new Pdts(mine, List.of()));
        assertThat(wired.pdt().store()).isSameAs(mine);
        assertThat(wired.pdts().userStore()).contains(mine);
        assertThat(Files.exists(dir.resolve("packs"))).as("no pack opened when a bean wins").isFalse();
    }

    @Test
    void aPackStoreLivesUnderThePathPerGroupAndClosesWithTheGroup(@TempDir Path dir) throws Exception {
        AgentSpacesProperties properties = properties(freePort(), 0, dir.resolve("x"));
        properties.getPdt().getStore().setKind("pack");
        properties.getPdt().getStore().setPath(dir.resolve("packs").toString());
        properties.getPdt().getStore().setRequire(true);
        Wired wired = wire(properties);
        wired.lifecycle().start();
        assertThat(wired.pdt().store().backend()).isInstanceOf(PackNodeStore.class);
        assertThat(((PackNodeStore) wired.pdt().store().backend()).dir()).isEqualTo(dir.resolve("packs").resolve("fleet"));
        wired.facts().swap(m -> m.assoc("k", "v"));
        assertThat(wired.pdt().stats().nodesHeld()).isGreaterThanOrEqualTo(1);

        wired.lifecycle().stop();
        assertThat(Files.exists(dir.resolve("packs").resolve("fleet").resolve(PackNodeStore.INDEX_FILE)))
                .as("the pack index is written on close (PD-S-02)").isTrue();
    }
}
