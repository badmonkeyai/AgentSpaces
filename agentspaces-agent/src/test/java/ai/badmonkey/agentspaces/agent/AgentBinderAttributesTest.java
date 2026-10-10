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
import ai.badmonkey.agentspaces.agent.annotation.Ballot;
import ai.badmonkey.agentspaces.agent.annotation.OnDecision;
import ai.badmonkey.agentspaces.agent.annotation.Part;
import ai.badmonkey.agentspaces.agent.annotation.Propose;
import ai.badmonkey.agentspaces.agent.annotation.SpaceJoin;
import ai.badmonkey.agentspaces.agent.annotation.SpaceNotify;
import ai.badmonkey.agentspaces.agent.annotation.SpaceReduce;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.agent.join.JoinTicket;
import ai.badmonkey.agentspaces.agent.join.Joined;
import ai.badmonkey.agentspaces.agent.reduce.Reductions;
import ai.badmonkey.agentspaces.api.ad.CardAction;
import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.vote.VoteCapability;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import ai.badmonkey.agentspaces.test.TestClock;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The annotation attributes that had no test of their own (the 2026-10-09
 * audit), over a {@link LocalSpace} under a {@link TestClock}: result and
 * ballot leases read back from the written entry, subscription and ticket
 * leases recorded as the binder hands them to the space, descriptions on the
 * card, the field and tag filters of the fan-in and fold annotations, the
 * fold's bound on remembered keys, the null and fork returns of the reacting
 * bindings, and the bind-time refusal of a malformed field filter.
 */
class AgentBinderAttributesTest {

    private static final Lease HOUR = Lease.of(Duration.ofHours(1));

    public record Task(String topic) {
    }

    public record Finding(String topic, String summary) {
    }

    public record Verdict(String id, String winner) {
    }

    public record Note(String id) {
    }

    public record Payment(String account, long cents) {
    }

    public record Total(long cents, int steps) {
    }

    /**
     * A space that remembers the lease of every subscription and every write
     * the binder hands it, forwarding everything to the local space beneath.
     */
    static final class Recorded {
        final Space space;
        final List<Lease> subscriptionLeases = new CopyOnWriteArrayList<>();
        final Map<Class<?>, Lease> writeLeases = new ConcurrentHashMap<>();

        Recorded(Space target) {
            space = (Space) Proxy.newProxyInstance(Space.class.getClassLoader(), new Class<?>[] {Space.class},
                    (proxy, method, args) -> {
                        if (method.getName().equals("notify")) {
                            subscriptionLeases.add((Lease) args[2]);
                        } else if (method.getName().equals("write")) {
                            writeLeases.put(args[0].getClass(), (Lease) args[1]);
                        } else if (method.getName().equals("complete") && args.length >= 3) {
                            writeLeases.put(args[1].getClass(), (Lease) args[2]);
                        }
                        try {
                            return method.invoke(target, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
        }
    }

    private final PeerIdentity identity = PeerIdentity.generate();
    private final TestClock clock = TestClock.create();
    private final LocalSpace tasksLocal = LocalSpace.builder("tasks", identity.agent("host")).clock(clock).build();
    private final LocalSpace votesLocal = LocalSpace.builder("votes", identity.agent("host")).clock(clock).build();
    private final LocalSpace out = LocalSpace.builder("out", identity.agent("host")).clock(clock).build();
    private final Recorded tasks = new Recorded(tasksLocal);
    private final Recorded votes = new Recorded(votesLocal);
    private final VoteCapability vote = new VoteCapability(votes.space, identity.agent("host"),
            identity.peerId(), clock);
    private final AgentBinder binder = new AgentBinder(identity, GroupId.of("zAttrs"), null, clock)
            .space("tasks", tasks.space).space("votes", votes.space).space("out", out).vote("votes", vote);

    @AfterEach
    void tearDown() {
        binder.close();
        out.close();
        votesLocal.close();
        tasksLocal.close();
    }

    private long now() {
        return clock.instant().toEpochMilli();
    }

    private long expiryOf(Space space, Class<?> type) {
        List<? extends Space.Entry<?>> entries = space.readAllEntries(Template.of(type), 10);
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).lease().kind()).isEqualTo(LeaseKind.WRITE);
        return entries.get(0).lease().expiresAtMillis();
    }

    private CardAction action(AgentBinder.Bound bound, String name) {
        return bound.card().actions().stream().filter(a -> a.name().equals(name)).findFirst().orElseThrow();
    }

    // ------------------------------------------------------------------ @SpaceTake

    @AgentSpec(name = "worker", description = "Takes tasks", goals = {"work"})
    public static class Worker {
        final List<String> taken = new CopyOnWriteArrayList<>();

        @SpaceTake(space = "tasks", pollTimeout = "50ms", resultLease = "2h")
        public Finding work(Task task) {
            taken.add(task.topic());
            return task.topic().startsWith("quiet") ? null : new Finding(task.topic(), "done");
        }
    }

    @Test
    void aTakeWritesItsResultUnderTheDeclaredResultLeaseAndANullReturnCompletesWithoutWriting() throws Exception {
        Worker worker = new Worker();
        binder.bind(worker);
        tasks.space.write(new Task("t1"), HOUR);
        await(() -> !tasks.space.readAll(Template.of(Finding.class), 10).isEmpty(), Duration.ofSeconds(5));
        assertThat(expiryOf(tasks.space, Finding.class)).isEqualTo(now() + Duration.ofHours(2).toMillis());

        tasks.space.write(new Task("quiet-1"), HOUR);
        await(() -> worker.taken.contains("quiet-1"), Duration.ofSeconds(5));
        Thread.sleep(100);
        clock.advance(Duration.ofMinutes(11));             // past the default take lease
        tasksLocal.sweepNow();                             // a merely claimed task would reappear here
        assertThat(tasks.space.readAll(Template.of(Task.class), 10)).as("the take completed").isEmpty();
        assertThat(tasks.space.readAll(Template.of(Finding.class), 10)).as("nothing written for the null").hasSize(1);
    }

    // ------------------------------------------------------------------ @SpaceNotify

    @AgentSpec(name = "reactor", description = "Reacts to tasks", goals = {"react"})
    public static class Reactor {
        @SpaceNotify(space = "tasks", resultLease = "3h", description = "Reacts to a task")
        public Finding react(Task task) {
            return new Finding("re:" + task.topic(), "seen");
        }
    }

    @Test
    void aReactionWritesItsResultUnderTheDeclaredResultLeaseAndDescribesItselfOnTheCard() throws Exception {
        AgentBinder.Bound bound = binder.bind(new Reactor());
        assertThat(action(bound, "react").description()).isEqualTo("Reacts to a task");
        tasks.space.write(new Task("t1"), HOUR);
        await(() -> !tasks.space.readAll(Template.of(Finding.class), 10).isEmpty(), Duration.ofSeconds(5));
        assertThat(expiryOf(tasks.space, Finding.class)).isEqualTo(now() + Duration.ofHours(3).toMillis());
    }

    // ------------------------------------------------------------------ @Ballot and @OnDecision

    @AgentSpec(name = "panel", description = "Votes and records", goals = {"decide"})
    public static class Panel {
        final List<String> decided = new CopyOnWriteArrayList<>();

        @Ballot(space = "votes", lease = "6h")
        public String judge(VoteCapability.Proposal proposal) {
            return "yes";
        }

        @OnDecision(space = "votes", lease = "5h", resultSpace = "out", resultLease = "4h")
        public Object record(VoteCapability.Decision decision) {
            decided.add(decision.proposalId());
            if (decision.proposalId().startsWith("quiet")) {
                return null;
            }
            if (decision.proposalId().startsWith("fork")) {
                return Entries.of(new Verdict(decision.proposalId(), decision.winner()),
                        Tagged.of(new Note(decision.proposalId()), "kind", "audit"));
            }
            return new Verdict(decision.proposalId(), decision.winner());
        }
    }

    @Test
    void ballotAndDecisionLeasesAreTheDeclaredOnesAndADecisionMayReturnNullOrAFork() throws Exception {
        Panel panel = new Panel();
        binder.bind(panel);
        assertThat(votes.subscriptionLeases).as("the decision subscriptions carry @OnDecision.lease")
                .contains(Lease.of(Duration.ofHours(5)));

        vote.propose("p1", "ship?", List.of("yes", "no"), 1, HOUR);
        await(() -> !out.readAll(Template.of(Verdict.class), 10).isEmpty(), Duration.ofSeconds(5));
        assertThat(expiryOf(votes.space, VoteCapability.Ballot.class)).as("@Ballot.lease")
                .isEqualTo(now() + Duration.ofHours(6).toMillis());
        assertThat(expiryOf(out, Verdict.class)).as("@OnDecision.resultLease")
                .isEqualTo(now() + Duration.ofHours(4).toMillis());

        vote.propose("quiet-1", "hush?", List.of("yes", "no"), 1, HOUR);
        await(() -> panel.decided.contains("quiet-1"), Duration.ofSeconds(5));
        Thread.sleep(150);
        assertThat(out.readAll(Template.of(Verdict.class), 10)).as("a null return writes nothing").hasSize(1);

        vote.propose("fork-1", "both?", List.of("yes", "no"), 1, HOUR);
        await(() -> !out.readAll(Template.of(Note.class), 10).isEmpty(), Duration.ofSeconds(5));
        assertThat(out.readAll(Template.of(Verdict.class), 10)).extracting(Verdict::id)
                .containsExactlyInAnyOrder("p1", "fork-1");
        assertThat(out.readAllEntries(Template.of(Note.class), 10)).singleElement()
                .satisfies(note -> assertThat(note.tags()).containsEntry("kind", "audit"));
    }

    // ------------------------------------------------------------------ @Propose

    @AgentSpec(name = "asker", description = "Asks about tasks", goals = {"ask"})
    public static class Asker {
        @Propose(space = "tasks", vote = "votes", prefix = "ask:", key = "topic", where = "topic!=noise",
                options = {"yes", "no"}, quorum = 1, subscriptionLease = "45m", description = "Asks about a task")
        public String ask(Task task) {
            return "about " + task.topic();
        }
    }

    @Test
    void aProposeFiltersItsCuesByFieldWatchesUnderItsSubscriptionLeaseAndDescribesItself() throws Exception {
        AgentBinder.Bound bound = binder.bind(new Asker());
        assertThat(action(bound, "ask").description()).isEqualTo("Asks about a task");
        assertThat(tasks.subscriptionLeases).containsExactly(Lease.of(Duration.ofMinutes(45)));
        tasks.space.write(new Task("noise"), HOUR);
        tasks.space.write(new Task("signal"), HOUR);
        await(() -> vote.proposal("ask:signal").isPresent(), Duration.ofSeconds(5));
        Thread.sleep(150);
        assertThat(vote.proposal("ask:noise")).as("the field filter kept the cue out").isEmpty();
    }

    // ------------------------------------------------------------------ @SpaceJoin and @Part

    @AgentSpec(name = "joiner", description = "Joins a task with its finding", goals = {"join"})
    public static class Joiner {
        final List<String> fired = new CopyOnWriteArrayList<>();

        @SpaceJoin(space = "tasks", key = "topic", mode = SpaceJoin.Mode.LEASED, pollTimeout = "100ms",
                lease = "7h", ticketLease = "8h", resultSpace = "out", resultLease = "9h",
                parts = {@Part(Task.class), @Part(value = Finding.class, where = "summary!=draft")},
                produces = {Verdict.class, Note.class}, description = "Joins a task with its finding")
        public Entries join(Joined joined) {
            fired.add(joined.key());
            if (joined.key().startsWith("quiet")) {
                return null;
            }
            return Entries.of(new Verdict(joined.key(), "joined"), new Note(joined.key()));
        }
    }

    @Test
    void aJoinHonoursItsPartFilterLeasesProducesAndDescriptionAndMayReturnNullOrAFork() throws Exception {
        Joiner joiner = new Joiner();
        AgentBinder.Bound bound = binder.bind(joiner);
        CardAction join = action(bound, "join");
        assertThat(join.description()).isEqualTo("Joins a task with its finding");
        assertThat(join.produces()).containsExactly(Verdict.class.getName() + "#v1", Note.class.getName() + "#v1");
        assertThat(tasks.subscriptionLeases).as("each part subscription carries @SpaceJoin.lease")
                .filteredOn(l -> l.equals(Lease.of(Duration.ofHours(7)))).hasSize(2);

        tasks.space.write(new Task("j1"), HOUR);
        tasks.space.write(new Finding("j1", "draft"), HOUR);
        Thread.sleep(200);
        assertThat(joiner.fired).as("a draft does not satisfy the part's field filter").isEmpty();
        tasks.space.write(new Finding("j1", "final"), HOUR);
        await(() -> !out.readAll(Template.of(Note.class), 10).isEmpty(), Duration.ofSeconds(5));
        assertThat(tasks.writeLeases.get(JoinTicket.class)).as("@SpaceJoin.ticketLease")
                .isEqualTo(Lease.of(Duration.ofHours(8)));
        assertThat(expiryOf(out, Verdict.class)).as("@SpaceJoin.resultLease on each forked entry")
                .isEqualTo(now() + Duration.ofHours(9).toMillis());
        assertThat(expiryOf(out, Note.class)).isEqualTo(now() + Duration.ofHours(9).toMillis());

        tasks.space.write(new Task("quiet-1"), HOUR);
        tasks.space.write(new Finding("quiet-1", "final"), HOUR);
        await(() -> joiner.fired.contains("quiet-1"), Duration.ofSeconds(5));
        Thread.sleep(150);
        assertThat(out.readAll(Template.of(Verdict.class), 10)).as("a null return writes nothing").hasSize(1);
    }

    // ------------------------------------------------------------------ @SpaceReduce

    @AgentSpec(name = "totals", description = "Totals payments per account tag", goals = {"total"})
    public static class Totals {
        @SpaceReduce(space = "tasks", keyTag = "acct", tags = "kind=pay", where = "cents!=0", lease = "2s",
                pollTimeout = "100ms", subscriptionLease = "30m", maxOpen = 1, description = "Totals an account")
        public Total fold(Total total, Payment payment) {
            return new Total((total == null ? 0 : total.cents()) + payment.cents(),
                    (total == null ? 0 : total.steps()) + 1);
        }
    }

    private Map<String, String> pay(String account) {
        return Map.of("acct", account, "kind", "pay");
    }

    private boolean totalIs(String key, Total expected) {
        return Reductions.current(tasks.space, Total.class, "totals.fold", key)
                .map(e -> e.value().equals(expected)).orElse(false);
    }

    @Test
    void aReduceKeysByTagFiltersByTagAndFieldWatchesUnderItsSubscriptionLeaseAndSurvivesItsOpenBound()
            throws Exception {
        AgentBinder.Bound bound = binder.bind(new Totals());
        assertThat(action(bound, "fold").description()).isEqualTo("Totals an account");
        assertThat(tasks.subscriptionLeases).as("the element and ticket subscriptions carry subscriptionLease")
                .containsOnly(Lease.of(Duration.ofMinutes(30)));
        tasks.space.write(new Payment("a", 5), HOUR, pay("a"));
        tasks.space.write(new Payment("b", 7), HOUR, pay("b"));
        tasks.space.write(new Payment("a", 0), HOUR, pay("a"));                          // where: cents!=0
        tasks.space.write(new Payment("a", 9), HOUR, Map.of("acct", "a", "kind", "refund")); // tags: kind=pay
        tasks.space.write(new Payment("c", 11), HOUR);                                   // no tags at all
        await(() -> totalIs("a", new Total(5, 1)) && totalIs("b", new Total(7, 1)), Duration.ofSeconds(10));
        // A second round per key: with maxOpen = 1 the fold has forgotten key a by the
        // time b was written, and still folds it from the space.
        tasks.space.write(new Payment("a", 1), HOUR, pay("a"));
        tasks.space.write(new Payment("b", 2), HOUR, pay("b"));
        await(() -> totalIs("a", new Total(6, 2)) && totalIs("b", new Total(9, 2)), Duration.ofSeconds(10));
        assertThat(tasks.space.readAll(Template.of(Payment.class), 20)).extracting(Payment::cents)
                .as("the filtered-out elements were never folded").containsExactlyInAnyOrder(0L, 9L, 11L);
        assertThat(tasks.space.readAll(Template.of(Total.class), 20)).as("one accumulator per key").hasSize(2);
    }

    // ------------------------------------------------------------------ malformed field filters

    public static class BadPartFilter {
        @SpaceJoin(space = "tasks", key = "topic",
                parts = {@Part(value = Task.class, where = "=x"), @Part(Finding.class)})
        public void join(Joined joined) {
        }
    }

    public static class BadReduceFilter {
        @SpaceReduce(space = "tasks", key = "topic", where = "!=x")
        public Total fold(Total total, Task task) {
            return null;
        }
    }

    public static class UnknownReduceField {
        @SpaceReduce(space = "tasks", key = "topic", where = "nope=1")
        public Total fold(Total total, Task task) {
            return null;
        }
    }

    @Test
    void aMalformedOrUnknownFieldFilterOnAPartOrAReduceIsRefusedAtBindTime() {
        assertThatThrownBy(() -> binder.bind(new BadPartFilter()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("field filter '=x'");
        assertThatThrownBy(() -> binder.bind(new BadReduceFilter()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("field filter '!=x'");
        assertThatThrownBy(() -> binder.bind(new UnknownReduceField()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("nope");
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
