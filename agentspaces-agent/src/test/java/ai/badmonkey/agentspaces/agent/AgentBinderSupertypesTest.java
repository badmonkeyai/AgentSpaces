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
package ai.badmonkey.agentspaces.agent;

import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.SpaceNotify;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The binder reads binding annotations through supertypes (SPEC §10.3,
 * `L3L4-COVERAGE.md` §6.1): a {@code java.lang.reflect.Proxy}, a LangChain4j
 * {@code AiServices} proxy, a Clojure {@code reify}, or a Kotlin object
 * implements an annotated interface and inherits nothing, so the interface
 * is where the fleet's view of the agent is declared. An object's own
 * annotation still wins over an inherited one.
 */
class AgentBinderSupertypesTest {

    private static final Lease HOUR = Lease.of(Duration.ofHours(1));

    public record Task(String topic, int priority) {
    }

    public record Finding(String topic, String summary) {
    }

    public record Audit(String topic) {
    }

    /** The fleet's view of a researcher: declared once, on the interface, as a LangChain4j service would be. */
    @AgentSpec(name = "proxy-researcher", description = "Researches through a proxy", goals = {"research"})
    public interface Researcher {
        @SpaceTake(space = "tasks", pollTimeout = "PT0.2S", resultSpace = "findings")
        Finding research(Task task);
    }

    /** An abstract base that carries the annotation; the bound object is an anonymous subclass. */
    @AgentSpec(name = "auditor", description = "Audits findings", goals = {"audit"})
    public abstract static class AuditorBase {
        @SpaceNotify(space = "findings", resultSpace = "audits")
        public abstract Audit audit(Finding finding);
    }

    /** An override that carries its own annotation, which must win over the interface's. */
    @AgentSpec(name = "own-researcher", description = "Overrides", goals = {"research"})
    public static class OwnResearcher implements Researcher {
        @Override
        @SpaceTake(space = "other", pollTimeout = "PT0.2S")
        public Finding research(Task task) {
            return new Finding(task.topic(), "own");
        }
    }

    private final PeerIdentity identity = PeerIdentity.generate();
    private final LocalSpace tasks = LocalSpace.builder("tasks", identity.agent("host")).build();
    private final LocalSpace findings = LocalSpace.builder("findings", identity.agent("host")).build();
    private final LocalSpace audits = LocalSpace.builder("audits", identity.agent("host")).build();
    private final AgentBinder binder = new AgentBinder(identity, GroupId.of("zSuper"), null, InstantSource.system())
            .space("tasks", tasks).space("findings", findings).space("audits", audits);

    @AfterEach
    void tearDown() {
        binder.close();
        tasks.close();
        findings.close();
        audits.close();
    }

    @Test
    @Timeout(30)
    void aProxyOverAnAnnotatedInterfaceBindsAsTheInterfaceDeclares() throws Exception {
        List<Task> handled = new CopyOnWriteArrayList<>();
        InvocationHandler handler = (proxy, method, args) -> {
            if (method.getName().equals("research")) {
                Task task = (Task) args[0];
                handled.add(task);
                return new Finding(task.topic(), "researched by a proxy");
            }
            if (method.getDeclaringClass() == Object.class) {
                return switch (method.getName()) {
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> "proxy";
                };
            }
            return null;
        };
        Researcher researcher = (Researcher) Proxy.newProxyInstance(Researcher.class.getClassLoader(),
                new Class<?>[] {Researcher.class}, handler);
        assertThat(AgentBinder.isAgentType(researcher.getClass())).as("the spec is read from the interface").isTrue();
        assertThat(AgentBinder.hasBindings(researcher.getClass())).isTrue();
        assertThat(binder.satisfies(researcher.getClass())).isTrue();

        try (AgentBinder.Bound bound = binder.bind(researcher)) {
            assertThat(bound.card().agent().localName()).isEqualTo("proxy-researcher");
            assertThat(bound.card().consumes()).containsExactly(Task.class.getName() + "#v1");
            assertThat(bound.card().produces()).containsExactly(Finding.class.getName() + "#v1");
            tasks.write(new Task("supertypes", 1), HOUR);
            await(() -> !findings.readAll(Template.of(Finding.class), 10).isEmpty());
            assertThat(findings.readAll(Template.of(Finding.class), 10)).singleElement()
                    .satisfies(f -> assertThat(f.summary()).isEqualTo("researched by a proxy"));
            assertThat(handled).hasSize(1);
        }
    }

    @Test
    @Timeout(30)
    void anAnonymousSubclassBindsThroughItsSuperclassAnnotations() throws Exception {
        AuditorBase auditor = new AuditorBase() {
            @Override
            public Audit audit(Finding finding) {
                return new Audit(finding.topic());
            }
        };
        try (AgentBinder.Bound bound = binder.bind(auditor)) {
            assertThat(bound.card().agent().localName()).isEqualTo("auditor");
            findings.write(new Finding("ledger", "done"), HOUR);
            await(() -> !audits.readAll(Template.of(Audit.class), 10).isEmpty());
            assertThat(audits.readAll(Template.of(Audit.class), 10)).singleElement()
                    .satisfies(a -> assertThat(a.topic()).isEqualTo("ledger"));
        }
    }

    @Test
    void anObjectsOwnAnnotationWinsOverTheInterfaces() {
        // The override names "other", which this binder does not register: the own annotation is the one read.
        assertThat(binder.satisfies(OwnResearcher.class)).isFalse();
        AgentBinder other = new AgentBinder(identity, GroupId.of("zSuper"), null, InstantSource.system())
                .space("other", LocalSpace.builder("other", identity.agent("host")).build());
        assertThat(other.satisfies(OwnResearcher.class)).isTrue();
        assertThat(AgentBinder.supertypes(OwnResearcher.class))
                .containsExactly(OwnResearcher.class, Researcher.class);
        other.close();
    }

    private static void await(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }
}
