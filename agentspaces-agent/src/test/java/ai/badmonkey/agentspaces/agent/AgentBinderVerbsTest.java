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
import ai.badmonkey.agentspaces.agent.capability.Motion;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.vote.VoteCapability;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The workflow verbs (ISSUE-WorkflowVerbs): the fork as an {@link Entries}
 * return, field filters with {@code where}, a sealed return declared on the
 * card, and leases as timers through {@code @SpaceNotify(on = EXPIRED)}.
 */
class AgentBinderVerbsTest {

    private static final Lease HOUR = Lease.of(Duration.ofHours(1));

    private final PeerIdentity identity = PeerIdentity.generate();
    private final LocalSpace tasks = LocalSpace.builder("tasks", identity.agent("host"))
            .sweepEvery(Duration.ofMillis(50)).build();
    private final LocalSpace votes = LocalSpace.builder("votes", identity.agent("host")).build();
    private final VoteCapability vote = new VoteCapability(votes, identity.agent("host"),
            identity.peerId(), InstantSource.system());
    private final AgentBinder binder = new AgentBinder(identity, GroupId.of("zVerbs"), null,
            InstantSource.system()).space("tasks", tasks).space("votes", votes).vote("votes", vote);

    @AfterEach
    void tearDown() {
        binder.close();
        votes.close();
        tasks.close();
    }

    public record Brief(String id, String severity) {
    }

    public record QuoteRequest(String id, int expected) {
    }

    public record QuoteTask(String id, String shop) {
    }

    // ------------------------------------------------------------------ fork

    @AgentSpec(name = "forker", description = "Forks a brief into a request and tasks", goals = {"fork"})
    public static class Forker {
        @SpaceNotify(space = "tasks", produces = {QuoteRequest.class, QuoteTask.class})
        public Entries fork(Brief brief) {
            return Entries.of(new QuoteRequest(brief.id(), 2),
                    Tagged.of(new QuoteTask(brief.id(), "north"), "shop", "north"),
                    new QuoteTask(brief.id(), "east"),
                    Motion.of("fork:" + brief.id(), "quote " + brief.id() + "?", List.of("yes", "no"), 1, HOUR));
        }
    }

    @Test
    void aForkWritesEachElementAsIfReturnedAloneAndDeclaresWhatItProduces() throws Exception {
        AgentBinder.Bound bound = binder.bind(new Forker());
        tasks.write(new Brief("b1", "LOW"), HOUR);
        await(() -> tasks.readAll(Template.of(QuoteTask.class), 10).size() == 2, Duration.ofSeconds(5));
        await(() -> vote.proposal("fork:b1").isPresent(), Duration.ofSeconds(5));
        assertThat(tasks.readAll(Template.of(QuoteRequest.class), 10)).hasSize(1);
        assertThat(tasks.readAllEntries(Template.of(QuoteTask.class).whereTag("shop", ai.badmonkey.agentspaces.api.space.Matchers.eq("north")), 10))
                .as("a Tagged element keeps its tags").hasSize(1);
        assertThat(bound.card().produces()).containsExactlyInAnyOrder(
                QuoteRequest.class.getName() + "#v1", QuoteTask.class.getName() + "#v1");
        assertThat(bound.card().actions().get(0).produces()).containsExactlyInAnyOrder(
                QuoteRequest.class.getName() + "#v1", QuoteTask.class.getName() + "#v1");
    }

    @AgentSpec(name = "take-forker", description = "Forks from a take", goals = {"fork"})
    public static class TakeForker {
        @SpaceTake(space = "tasks", pollTimeout = "50ms", produces = QuoteTask.class)
        public Entries fork(Brief brief) {
            return Entries.of(new QuoteTask(brief.id(), "a"), new QuoteTask(brief.id(), "b"));
        }
    }

    @Test
    void aForkFromATakeCompletesTheTakeOnceAndThenWritesEachElement() throws Exception {
        binder.bind(new TakeForker());
        tasks.write(new Brief("b2", "LOW"), HOUR);
        await(() -> tasks.readAll(Template.of(QuoteTask.class), 10).size() == 2, Duration.ofSeconds(5));
        assertThat(tasks.readAll(Template.of(Brief.class), 10)).as("the take completed").isEmpty();
    }

    // ------------------------------------------------------------------ where

    @AgentSpec(name = "severe-only", description = "Takes HIGH briefs only", goals = {"route"})
    public static class SevereOnly {
        @SpaceTake(space = "tasks", where = "severity=HIGH", pollTimeout = "50ms")
        public QuoteTask take(Brief brief) {
            return new QuoteTask(brief.id(), "senior");
        }
    }

    @AgentSpec(name = "not-low", description = "Reacts to everything but LOW", goals = {"route"})
    public static class NotLow {
        final List<String> seen = new CopyOnWriteArrayList<>();

        @SpaceNotify(space = "tasks", where = "severity!=LOW")
        public void see(Brief brief) {
            seen.add(brief.id());
        }
    }

    @Test
    void fieldFiltersRouteByValueBeforeTheMethodIsInvoked() throws Exception {
        NotLow notLow = new NotLow();
        binder.bind(new SevereOnly());
        binder.bind(notLow);
        tasks.write(new Brief("h", "HIGH"), HOUR);
        tasks.write(new Brief("m", "MEDIUM"), HOUR);
        tasks.write(new Brief("l", "LOW"), HOUR);
        await(() -> tasks.readAll(Template.of(QuoteTask.class), 10).size() == 1, Duration.ofSeconds(5));
        Thread.sleep(200);
        assertThat(tasks.readAll(Template.of(QuoteTask.class), 10).get(0).id()).isEqualTo("h");
        assertThat(tasks.readAll(Template.of(Brief.class), 10)).extracting(Brief::id)
                .as("the others were never claimed").containsExactlyInAnyOrder("m", "l");
        assertThat(notLow.seen).containsExactlyInAnyOrder("h", "m");
    }

    public static class NoSuchField {
        @SpaceTake(space = "tasks", where = "urgency=HIGH")
        public void take(Brief brief) {
        }
    }

    public static class Malformed {
        @SpaceNotify(space = "tasks", where = "=HIGH")
        public void see(Brief brief) {
        }
    }

    @Test
    void fieldFiltersAreValidatedAtBindTime() {
        assertThatThrownBy(() -> binder.bind(new NoSuchField()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("urgency")
                .hasMessageContaining("severity");
        assertThatThrownBy(() -> binder.bind(new Malformed()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("=HIGH");
    }

    // ------------------------------------------------------------------ sealed returns

    public sealed interface Next permits Accepted, Declined {
    }

    public record Accepted(String id) implements Next {
    }

    public record Declined(String id, String why) implements Next {
    }

    @AgentSpec(name = "gate", description = "Accepts or declines", goals = {"branch"})
    public static class Gate {
        @SpaceTake(space = "tasks", pollTimeout = "50ms")
        public Next decide(Brief brief) {
            return brief.severity().equals("HIGH") ? new Declined(brief.id(), "too risky") : new Accepted(brief.id());
        }
    }

    @Test
    void aSealedReturnDeclaresEachBranchOnTheCardAndWritesTheConcreteOne() throws Exception {
        AgentBinder.Bound bound = binder.bind(new Gate());
        tasks.write(new Brief("x", "HIGH"), HOUR);
        tasks.write(new Brief("y", "LOW"), HOUR);
        await(() -> !tasks.readAll(Template.of(Declined.class), 10).isEmpty()
                && !tasks.readAll(Template.of(Accepted.class), 10).isEmpty(), Duration.ofSeconds(5));
        assertThat(bound.card().produces()).containsExactlyInAnyOrder(
                Accepted.class.getName() + "#v1", Declined.class.getName() + "#v1");
        assertThat(tasks.readAll(Template.of(Declined.class), 10).get(0).why()).isEqualTo("too risky");
    }

    // ------------------------------------------------------------------ leases as timers

    public record Reminder(String id) {
    }

    public record Escalation(String id) {
    }

    @AgentSpec(name = "escalator", description = "Escalates lapsed reminders", goals = {"deadline"})
    public static class Escalator {
        final List<String> kinds = new CopyOnWriteArrayList<>();

        @SpaceNotify(space = "tasks", on = SpaceEvent.Kind.EXPIRED)
        public Escalation escalate(Reminder reminder) {
            kinds.add("expired:" + reminder.id());
            return new Escalation(reminder.id());
        }
    }

    @AgentSpec(name = "both", description = "Sees a write and its lapse", goals = {"watch"})
    public static class Both {
        final List<String> kinds = new CopyOnWriteArrayList<>();

        @SpaceNotify(space = "tasks", on = {SpaceEvent.Kind.WRITTEN, SpaceEvent.Kind.EXPIRED})
        public void see(Reminder reminder) {
            kinds.add(reminder.id());
        }
    }

    @Test
    void aLeasedEntryIsATimerWhenAReactionListensForItsExpiry() throws Exception {
        Escalator escalator = new Escalator();
        Both both = new Both();
        binder.bind(escalator);
        binder.bind(both);
        tasks.write(new Reminder("r1"), Lease.of(Duration.ofMillis(200)));
        tasks.write(new Reminder("r2"), HOUR);
        await(() -> !tasks.readAll(Template.of(Escalation.class), 10).isEmpty(), Duration.ofSeconds(5));
        Thread.sleep(300);
        assertThat(tasks.readAll(Template.of(Escalation.class), 10)).extracting(Escalation::id)
                .containsExactly("r1");
        assertThat(escalator.kinds).as("the write itself did not fire the expiry reaction")
                .containsExactly("expired:r1");
        assertThat(both.kinds).as("one entry, two kinds, two deliveries; the live one once")
                .containsExactlyInAnyOrder("r1", "r1", "r2");
    }

    public static class Deaf {
        @SpaceNotify(space = "tasks", on = {})
        public void see(Reminder reminder) {
        }
    }

    @Test
    void aReactionToNoEventKindIsRefusedAtBindTime() {
        assertThatThrownBy(() -> binder.bind(new Deaf()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("no event kind");
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
