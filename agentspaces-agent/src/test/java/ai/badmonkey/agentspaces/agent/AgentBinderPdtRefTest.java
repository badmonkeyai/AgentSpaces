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
import ai.badmonkey.agentspaces.agent.annotation.PdtRef;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.agent.pdt.RefResolver;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code @PdtRef} (PDT 2 phase 7, P2-X-03, PD-R-11), written before the annotation existed: the binder injects
 * the handle its {@link RefResolver} gives for the name and the declared generic type, into a field at bind time
 * and into a verb method's trailing parameter at every call, resolved once at bind.
 */
class AgentBinderPdtRefTest {

    /** What the resolver hands out in place of agentspaces-pdt's {@code Ref}, which this module cannot see. */
    public record Handle<T>(String name, Type type) {
    }

    /** A resolver that records every site it resolved. */
    static final class Recording implements RefResolver {
        final List<String> resolved = new CopyOnWriteArrayList<>();

        @Override
        public Object resolve(String name, Type type) {
            resolved.add(name + " " + type.getTypeName());
            if (!(type instanceof ParameterizedType parameterized) || parameterized.getRawType() != Handle.class) {
                throw new IllegalArgumentException("not a Handle: " + type.getTypeName());
            }
            return new Handle<>(name, parameterized.getActualTypeArguments()[0]);
        }
    }

    @AgentSpec(name = "keeper", description = "Keeps facts", goals = {"keep"})
    public static class Keeper {
        @PdtRef("facts")
        Handle<Map<String, Integer>> facts;
    }

    @AgentSpec(name = "worker", description = "Works with a ref", goals = {"work"})
    public static class Worker {
        final List<Handle<?>> received = new CopyOnWriteArrayList<>();

        @SpaceTake(space = "tasks", lease = "PT10M", pollTimeout = "PT0.2S")
        public void work(TaskEntry task, @PdtRef("ledger") Handle<List<String>> ledger) {
            received.add(ledger);
        }
    }

    @AgentSpec(name = "backwards", description = "Puts the ref first", goals = {"fail"})
    public static class Backwards {
        @SpaceTake(space = "tasks")
        public void work(@PdtRef("ledger") Handle<List<String>> ledger, TaskEntry task) {
        }
    }

    @AgentSpec(name = "elsewhere", description = "A ref of another group", goals = {"skip"})
    public static class Elsewhere {
        @PdtRef(value = "facts", group = "other")
        Handle<Map<String, Integer>> facts;
    }

    /** A class whose only binding is a ref field: a scheduled method beside it writes through the handle. */
    public static class RefOnly {
        @PdtRef("facts")
        Handle<Map<String, Integer>> facts;
    }

    private final PeerIdentity identity = PeerIdentity.generate();
    private final LocalSpace tasks = LocalSpace.builder("tasks", identity.agent("host"))
            .sweepEvery(Duration.ofMillis(50))
            .build();
    private final AgentBinder binder = new AgentBinder(identity, GroupId.of("zPdtRef"), null, InstantSource.system());

    @AfterEach
    void tearDown() {
        binder.close();
        tasks.close();
    }

    @Test
    void aFieldGetsTheHandleOfItsNameAndDeclaredType() {
        Recording resolver = new Recording();
        binder.refs(resolver);
        Keeper keeper = new Keeper();

        binder.bind(keeper);

        assertThat(keeper.facts).isNotNull();
        assertThat(keeper.facts.name()).isEqualTo("facts");
        assertThat(keeper.facts.type().getTypeName()).isEqualTo("java.util.Map<java.lang.String, java.lang.Integer>");
        assertThat(resolver.resolved).containsExactly("facts " + AgentBinderPdtRefTest.class.getName()
                + "$Handle<java.util.Map<java.lang.String, java.lang.Integer>>");
    }

    @Test
    @Timeout(30)
    void aTrailingParameterGetsTheHandleOnEveryCallResolvedOnceAtBind() throws Exception {
        Recording resolver = new Recording();
        binder.refs(resolver).space("tasks", tasks);
        Worker worker = new Worker();

        binder.bind(worker);
        tasks.write(new TaskEntry("a", 1), Lease.of(Duration.ofMinutes(5)));
        tasks.write(new TaskEntry("b", 2), Lease.of(Duration.ofMinutes(5)));
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (worker.received.size() < 2 && System.nanoTime() < deadline) {
            Thread.sleep(25);
        }

        assertThat(worker.received).hasSize(2).allMatch(h -> h.name().equals("ledger"));
        assertThat(worker.received.get(0)).isSameAs(worker.received.get(1));
        assertThat(resolver.resolved).as("resolved once, at bind").hasSize(1);
    }

    @Test
    void aRefParameterBeforeTheMethodsOwnParametersIsRefusedByName() {
        binder.refs(new Recording()).space("tasks", tasks);

        assertThatThrownBy(() -> binder.bind(new Backwards()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("@PdtRef parameter")
                .hasMessageContaining("work");
    }

    @Test
    void aRefWithoutAResolverFailsAtBindNamingTheFieldAndTheSwitch() {
        assertThatThrownBy(() -> binder.bind(new Keeper()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("@PdtRef")
                .hasMessageContaining("facts")
                .hasMessageContaining("agentspaces.pdt.enabled");
    }

    @Test
    void aResolverThatRefusesTheTypeFailsAtBindNamingTheField() {
        binder.refs((name, type) -> {
            throw new IllegalArgumentException("a ref holds a PMap, PSet, PVector, or PBlob");
        });

        assertThatThrownBy(() -> binder.bind(new Keeper()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Keeper.facts")
                .hasMessageContaining("a ref holds");
    }

    @Test
    void aHandleOfTheWrongClassIsRefusedNamingBothTypes() {
        binder.refs((name, type) -> "a string, not a handle");

        assertThatThrownBy(() -> binder.bind(new Keeper()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("java.lang.String")
                .hasMessageContaining(Handle.class.getName());
    }

    @Test
    void aFieldOfAnotherGroupIsSkipped() {
        Recording resolver = new Recording();
        binder.refs(resolver);
        Elsewhere elsewhere = new Elsewhere();

        assertThat(binder.satisfies(Elsewhere.class)).as("no binding addresses this group").isFalse();
        binder.bind(elsewhere);

        assertThat(elsewhere.facts).isNull();
        assertThat(resolver.resolved).isEmpty();
    }

    @Test
    void theFacadeSaysARefSiteNeedsAGroupWithPersistentTrees() {
        AgentSpaces spaces = new AgentSpaces(identity, InstantSource.system());
        try {
            spaces.register("fleet", GroupId.of("zPdtRefFacade"), null, null);

            assertThatThrownBy(() -> spaces.bind(new Keeper()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("@PdtRef")
                    .hasMessageContaining("agentspaces.pdt.enabled");

            spaces.group("fleet").binder().refs(new Recording());
            Keeper keeper = new Keeper();
            assertThat(spaces.bind(keeper)).hasSize(1);
            assertThat(keeper.facts.name()).isEqualTo("facts");
        } finally {
            spaces.close();
        }
    }

    @Test
    void aRefFieldIsABindingThatOnlyABinderWithAResolverSatisfies() {
        assertThat(AgentBinder.hasBindings(RefOnly.class)).isTrue();
        assertThat(binder.satisfies(RefOnly.class)).as("no persistent trees in this group").isFalse();

        binder.refs(new Recording());

        assertThat(binder.satisfies(RefOnly.class)).isTrue();
        RefOnly bean = new RefOnly();
        binder.bind(bean);
        assertThat(bean.facts.name()).isEqualTo("facts");
    }
}
