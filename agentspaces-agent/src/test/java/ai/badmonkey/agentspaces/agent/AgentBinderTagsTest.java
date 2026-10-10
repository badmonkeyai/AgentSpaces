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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.SpaceNotify;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.agent.capability.Contribution;
import ai.badmonkey.agentspaces.api.ad.CardAction;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tags on the binding annotations and the {@link Tagged} return (SPEC §7.2,
 * §10.3; issue #16): a filtered take or subscription sees only matching
 * entries, judged before decode, and a tagged return is written with its tags,
 * atomically with the completion for a same-space take result.
 */
class AgentBinderTagsTest {

    private static final Lease HOUR = Lease.of(Duration.ofHours(1));

    private final PeerIdentity identity = PeerIdentity.generate();
    private final LocalSpace tasks = LocalSpace.builder("tasks", identity.agent("host"))
            .sweepEvery(Duration.ofMillis(50))
            .build();
    private final LocalSpace results = LocalSpace.builder("results", identity.agent("host"))
            .sweepEvery(Duration.ofMillis(50))
            .build();
    private final AgentBinder binder = new AgentBinder(identity, GroupId.of("zTags"),
            null, InstantSource.system()).space("tasks", tasks).space("results", results);

    @AfterEach
    void tearDown() {
        binder.close();
        results.close();
        tasks.close();
    }

    public record Task(String id) {
    }

    public record Done(String id, String by) {
    }

    @AgentSpec(name = "eu-worker", description = "Works EU tasks only", goals = {"work"})
    public static class EuWorker {
        @SpaceTake(space = "tasks", tags = "region=eu", pollTimeout = "50ms")
        public Done work(Task task) {
            return new Done(task.id(), "eu");
        }
    }

    @Test
    void aFilteredTakeClaimsOnlyEntriesWithTheTag() throws Exception {
        binder.bind(new EuWorker());
        tasks.write(new Task("t-eu"), HOUR, Map.of("region", "eu"));
        tasks.write(new Task("t-us"), HOUR, Map.of("region", "us"));
        tasks.write(new Task("t-none"), HOUR);
        await(() -> tasks.readAll(Template.of(Done.class), 10).size() == 1, Duration.ofSeconds(5));
        Thread.sleep(200);
        assertThat(tasks.readAll(Template.of(Done.class), 10)).extracting(Done::id).containsExactly("t-eu");
        assertThat(tasks.readAll(Template.of(Task.class), 10)).as("the others were never claimed")
                .extracting(Task::id).containsExactlyInAnyOrder("t-us", "t-none");
    }

    @AgentSpec(name = "typed-watcher", description = "Reacts to typed entries", goals = {"watch"})
    public static class TypedWatcher {
        final List<String> seen = new CopyOnWriteArrayList<>();

        @SpaceNotify(space = "tasks", tags = "rdf:type")
        public void watch(Task task) {
            seen.add(task.id());
        }
    }

    @Test
    void aFilteredSubscriptionFiresOnlyForEntriesCarryingTheTag() throws Exception {
        TypedWatcher watcher = new TypedWatcher();
        binder.bind(watcher);
        tasks.write(new Task("typed"), HOUR, Map.of("rdf:type", "https://example.org/x#Task"));
        tasks.write(new Task("plain"), HOUR);
        await(() -> watcher.seen.size() == 1, Duration.ofSeconds(5));
        Thread.sleep(200);
        assertThat(watcher.seen).containsExactly("typed");
    }

    public static class BadFilter {
        @SpaceTake(space = "tasks", tags = "=eu")
        public void work(Task task) {
        }
    }

    @Test
    void aMalformedFilterIsRefusedAtBindTimeNamingTheMethod() {
        assertThatThrownBy(() -> binder.bind(new BadFilter()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("=eu")
                .hasMessageContaining("work");
    }

    @AgentSpec(name = "labeller", description = "Labels what it writes", goals = {"label"})
    public static class Labeller {
        @SpaceTake(space = "tasks", pollTimeout = "50ms")
        public Tagged<Done> work(Task task) {
            return Tagged.of(new Done(task.id(), "labeller"), "rdf:type", "Done", "id", task.id());
        }

        @SpaceNotify(space = "tasks", resultSpace = "results")
        public Tagged<Done> echo(Done done) {
            return Tagged.of(new Done(done.id(), "echo"), Map.of("via", "notify"));
        }
    }

    @Test
    void aTaggedReturnIsWrittenWithItsTagsOnEveryPath() throws Exception {
        AgentBinder.Bound bound = binder.bind(new Labeller());
        tasks.write(new Task("t1"), HOUR);
        await(() -> !results.readAll(Template.of(Done.class), 10).isEmpty(), Duration.ofSeconds(5));
        // Same-space take result: completed with the result and its tags, atomically.
        List<Space.Entry<Done>> done = tasks.readAllEntries(Template.of(Done.class), 10);
        assertThat(done).hasSize(1);
        assertThat(done.get(0).value().by()).isEqualTo("labeller");
        assertThat(done.get(0).tags()).containsEntry("rdf:type", "Done").containsEntry("id", "t1");
        assertThat(tasks.readAll(Template.of(Task.class), 10)).isEmpty();
        // Cross-space notify result: written with its tags.
        List<Space.Entry<Done>> echoed = results.readAllEntries(Template.of(Done.class), 10);
        assertThat(echoed).hasSize(1);
        assertThat(echoed.get(0).value().by()).isEqualTo("echo");
        assertThat(echoed.get(0).tags()).containsExactlyEntriesOf(Map.of("via", "notify"));
        // The card declares the wrapped type, never Tagged itself.
        assertThat(bound.card().produces()).containsExactly(Done.class.getName() + "#v1");
        List<CardAction> actions = bound.card().actions();
        assertThat(actions).extracting(CardAction::produces)
                .allSatisfy(p -> assertThat(p).containsExactly(Done.class.getName() + "#v1"));
    }

    @AgentSpec(name = "confused", description = "Tags a contribution", goals = {"confuse"})
    public static class Confused {
        final List<String> seen = new CopyOnWriteArrayList<>();

        @SpaceNotify(space = "tasks")
        public Tagged<Contribution> watch(Task task) {
            seen.add(task.id());
            return Tagged.of(Contribution.to("epoch", 1.0), "k", "v");
        }
    }

    @Test
    void aTaggedContributionIsRefusedAtDispatchAndTheReactionKeepsRunning() throws Exception {
        Confused confused = new Confused();
        binder.bind(confused);
        tasks.write(new Task("t1"), HOUR);
        tasks.write(new Task("t2"), HOUR);
        await(() -> confused.seen.size() == 2, Duration.ofSeconds(5));
        Thread.sleep(100);
        assertThat(tasks.readAll(Template.of(Contribution.class), 10)).isEmpty();
    }

    private static void await(BooleanSupplier condition, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within " + timeout);
            }
            Thread.sleep(20);
        }
    }
}
