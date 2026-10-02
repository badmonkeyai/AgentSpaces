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
package ai.badmonkey.agentspaces.space.local;

import ai.badmonkey.agentspaces.api.error.LeaseExpiredException;
import ai.badmonkey.agentspaces.api.error.SpaceClosedException;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.Subscription;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.test.Fixtures;
import ai.badmonkey.agentspaces.test.Fixtures.FindingEntry;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;
import static ai.badmonkey.agentspaces.api.space.Matchers.gte;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalSpaceTest {

    private TestClock clock;
    private LocalSpace space;

    @BeforeEach
    void setUp() {
        clock = TestClock.create();
        space = LocalSpace.builder("tasks", Fixtures.agent("coordinator"))
                .clock(clock)
                .build();
    }

    @AfterEach
    void tearDown() {
        space.close();
    }

    private static final Lease MINUTES_30 = Lease.of(Duration.ofMinutes(30));
    private static final Lease MINUTES_10 = Lease.of(Duration.ofMinutes(10));

    @Test
    void writeThenReadThenReadAll() {
        space.write(new TaskEntry("gossip", 3), MINUTES_30);
        space.write(new TaskEntry("crdts", 5), MINUTES_30);

        Optional<TaskEntry> highPriority =
                space.read(Template.of(TaskEntry.class).where("priority", gte(5)));
        assertThat(highPriority).contains(new TaskEntry("crdts", 5));

        List<TaskEntry> all = space.readAll(Template.of(TaskEntry.class), 10);
        assertThat(all).hasSize(2);
        assertThat(space.readAll(Template.of(TaskEntry.class), 1)).hasSize(1);
    }

    @Test
    void takeIsExclusiveAndCompleteConsumes() {
        space.write(new TaskEntry("gossip", 3), MINUTES_30);

        Optional<TakenEntry<TaskEntry>> taken =
                space.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(taken).isPresent();

        // While held, the entry is invisible to readers and other takers.
        assertThat(space.read(Template.of(TaskEntry.class))).isEmpty();
        assertThat(space.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO)).isEmpty();

        space.complete(taken.get());
        assertThat(space.read(Template.of(TaskEntry.class))).isEmpty();
    }

    @Test
    void completeWithResultWritesAtomically() {
        space.write(new TaskEntry("gossip", 3), MINUTES_30);
        TakenEntry<TaskEntry> taken =
                space.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO).orElseThrow();

        EntryHandle resultHandle = space.complete(taken,
                new FindingEntry("gossip", "epidemic broadcast works"), MINUTES_30);

        assertThat(resultHandle).isNotNull();
        assertThat(space.read(Template.of(FindingEntry.class).where("topic", eq("gossip"))))
                .contains(new FindingEntry("gossip", "epidemic broadcast works"));
    }

    @Test
    void writeLeaseExpiryRemovesTheEntry() {
        space.write(new TaskEntry("gossip", 3), Lease.of(Duration.ofMinutes(1)));

        clock.advance(Duration.ofMinutes(2));

        assertThat(space.read(Template.of(TaskEntry.class))).isEmpty();
    }

    @Test
    void lapsedTakeLeaseMakesTheEntryReappear() {
        space.write(new TaskEntry("gossip", 3), MINUTES_30);
        TakenEntry<TaskEntry> taken = space.take(Template.of(TaskEntry.class),
                Lease.of(Duration.ofMinutes(1)), Duration.ZERO).orElseThrow();

        clock.advance(Duration.ofMinutes(2));

        // The entry reappears for another taker; the stale take is dead.
        Optional<TakenEntry<TaskEntry>> second =
                space.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO);
        assertThat(second).isPresent();
        assertThatThrownBy(() -> space.complete(taken)).isInstanceOf(LeaseExpiredException.class);
        space.complete(second.get());
    }

    @Test
    void renewalsExtendLeases() {
        EntryHandle handle = space.write(new TaskEntry("gossip", 3), Lease.of(Duration.ofMinutes(1)));
        clock.advance(Duration.ofSeconds(50));
        handle.renew(Duration.ofMinutes(5));
        clock.advance(Duration.ofMinutes(2));
        assertThat(space.read(Template.of(TaskEntry.class))).isPresent();

        TakenEntry<TaskEntry> taken = space.take(Template.of(TaskEntry.class),
                Lease.of(Duration.ofMinutes(1)), Duration.ZERO).orElseThrow();
        clock.advance(Duration.ofSeconds(50));
        taken.renew(Duration.ofMinutes(5));
        clock.advance(Duration.ofMinutes(2));
        space.complete(taken);
        assertThat(space.read(Template.of(TaskEntry.class))).isEmpty();
    }

    @Test
    void expiredRenewalThrows() {
        EntryHandle handle = space.write(new TaskEntry("gossip", 3), Lease.of(Duration.ofMinutes(1)));
        clock.advance(Duration.ofMinutes(2));

        assertThatThrownBy(() -> handle.renew(Duration.ofMinutes(5)))
                .isInstanceOf(LeaseExpiredException.class);
    }

    @Test
    void cancelWithdrawsTheEntry() {
        EntryHandle handle = space.write(new TaskEntry("gossip", 3), MINUTES_30);

        handle.cancel();
        handle.cancel();

        assertThat(space.read(Template.of(TaskEntry.class))).isEmpty();
    }

    @Test
    void notifyDeliversLifecycleEvents() {
        List<SpaceEvent<TaskEntry>> received = new CopyOnWriteArrayList<>();
        space.notify(Template.of(TaskEntry.class), received::add, MINUTES_30);

        space.write(new TaskEntry("gossip", 3), Lease.of(Duration.ofMinutes(5)));
        TakenEntry<TaskEntry> taken = space.take(Template.of(TaskEntry.class),
                Lease.of(Duration.ofMinutes(1)), Duration.ZERO).orElseThrow();
        clock.advance(Duration.ofMinutes(2));
        space.sweepNow();                                  // take lease lapses -> REAPPEARED
        TakenEntry<TaskEntry> again = space.take(Template.of(TaskEntry.class),
                MINUTES_10, Duration.ZERO).orElseThrow();
        space.complete(again);

        assertThat(received).extracting(SpaceEvent::kind).containsExactly(
                SpaceEvent.Kind.WRITTEN, SpaceEvent.Kind.TAKEN, SpaceEvent.Kind.REAPPEARED,
                SpaceEvent.Kind.TAKEN, SpaceEvent.Kind.COMPLETED);
        assertThat(taken.entryId()).isEqualTo(again.entryId());
    }

    @Test
    void expiredEntriesFireExpiredEvents() {
        List<SpaceEvent<TaskEntry>> received = new CopyOnWriteArrayList<>();
        space.notify(Template.of(TaskEntry.class), received::add, MINUTES_30);

        space.write(new TaskEntry("gossip", 3), Lease.of(Duration.ofMinutes(1)));
        clock.advance(Duration.ofMinutes(2));
        space.sweepNow();

        assertThat(received).extracting(SpaceEvent::kind)
                .containsExactly(SpaceEvent.Kind.WRITTEN, SpaceEvent.Kind.EXPIRED);
    }

    @Test
    void closedSubscriptionStopsReceiving() {
        List<SpaceEvent<TaskEntry>> received = new CopyOnWriteArrayList<>();
        Subscription subscription =
                space.notify(Template.of(TaskEntry.class), received::add, MINUTES_30);

        subscription.close();
        space.write(new TaskEntry("gossip", 3), MINUTES_30);

        assertThat(received).isEmpty();
    }

    @Test
    void lapsedSubscriptionStopsReceiving() {
        List<SpaceEvent<TaskEntry>> received = new CopyOnWriteArrayList<>();
        space.notify(Template.of(TaskEntry.class), received::add, Lease.of(Duration.ofMinutes(1)));

        clock.advance(Duration.ofMinutes(2));
        space.write(new TaskEntry("gossip", 3), MINUTES_30);

        assertThat(received).isEmpty();
    }

    @Test
    void blockingTakeWaitsForAWriter() throws Exception {
        Thread writer = Thread.ofVirtual().start(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            space.write(new TaskEntry("late", 1), MINUTES_30);
        });

        Optional<TakenEntry<TaskEntry>> taken =
                space.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ofSeconds(5));

        writer.join();
        assertThat(taken).isPresent();
        assertThat(taken.get().entry().topic()).isEqualTo("late");
    }

    @Test
    void blockingReadTimesOutEmpty() {
        assertThat(space.read(Template.of(TaskEntry.class), Duration.ofMillis(80))).isEmpty();
    }

    @Test
    void closedSpaceRefusesOperations() {
        space.close();

        assertThatThrownBy(() -> space.write(new TaskEntry("x", 1), MINUTES_30))
                .isInstanceOf(SpaceClosedException.class);
    }

    @Test
    void templatesSelectByFieldOnTake() {
        space.write(new TaskEntry("low", 1), MINUTES_30);
        space.write(new TaskEntry("high", 9), MINUTES_30);

        TakenEntry<TaskEntry> taken = space.take(
                Template.of(TaskEntry.class).where("priority", gte(5)),
                MINUTES_10, Duration.ZERO).orElseThrow();

        assertThat(taken.entry().topic()).isEqualTo("high");
        space.complete(taken);
        assertThat(space.read(Template.of(TaskEntry.class)).orElseThrow().topic()).isEqualTo("low");
    }

    /** A result Jackson cannot serialize: its only accessor throws. */
    public static class PoisonResult {
        public String getBoom() {
            throw new IllegalStateException("boom");
        }
    }

    /** Spec §7.2: a result that cannot be written never leaves the entry completed-but-resultless. */
    @Test
    void aResultThatCannotBeSerializedDoesNotConsumeTheTake() {
        List<SpaceEvent<TaskEntry>> received = new CopyOnWriteArrayList<>();
        space.notify(Template.of(TaskEntry.class), received::add, MINUTES_30);
        space.write(new TaskEntry("job", 1), MINUTES_30);
        TakenEntry<TaskEntry> taken =
                space.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO).orElseThrow();

        assertThatThrownBy(() -> space.complete(taken, new PoisonResult(), MINUTES_30))
                .isInstanceOf(RuntimeException.class)
                .isNotInstanceOf(LeaseExpiredException.class);

        // Nothing committed: no COMPLETED event, and the holder still owns the take.
        assertThat(received).extracting(SpaceEvent::kind)
                .containsExactly(SpaceEvent.Kind.WRITTEN, SpaceEvent.Kind.TAKEN);
        space.complete(taken, new FindingEntry("job", "done"), MINUTES_30);
        assertThat(received).extracting(SpaceEvent::kind)
                .containsExactly(SpaceEvent.Kind.WRITTEN, SpaceEvent.Kind.TAKEN,
                        SpaceEvent.Kind.COMPLETED);
        assertThat(space.read(Template.of(TaskEntry.class))).isEmpty();
        assertThat(space.read(Template.of(FindingEntry.class)))
                .contains(new FindingEntry("job", "done"));
    }

    /** Spec §7.2 (v0.1.7 matching order): read and take prefer the oldest entry of the type. */
    @Test
    void readAndTakePreferTheOldestMatchingEntryOfTheType() {
        space.write(new TaskEntry("first", 1), MINUTES_30);
        space.write(new TaskEntry("second", 1), MINUTES_30);
        space.write(new TaskEntry("third", 1), MINUTES_30);

        assertThat(space.read(Template.of(TaskEntry.class)).orElseThrow().topic()).isEqualTo("first");
        assertThat(space.readAll(Template.of(TaskEntry.class), 10))
                .extracting(TaskEntry::topic).containsExactly("first", "second", "third");

        List<String> takenOrder = new java.util.ArrayList<>();
        for (int i = 0; i < 3; i++) {
            TakenEntry<TaskEntry> taken =
                    space.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO).orElseThrow();
            takenOrder.add(taken.entry().topic());
            space.complete(taken);
        }
        assertThat(takenOrder).containsExactly("first", "second", "third");
    }

    /** Spec §7.2: a held entry is skipped, and a reappeared entry keeps its FIFO position. */
    @Test
    void reappearedEntryKeepsItsFifoPosition() {
        space.write(new TaskEntry("first", 1), MINUTES_30);
        space.write(new TaskEntry("second", 1), MINUTES_30);

        TakenEntry<TaskEntry> held = space.take(Template.of(TaskEntry.class),
                Lease.of(Duration.ofMinutes(1)), Duration.ZERO).orElseThrow();
        assertThat(held.entry().topic()).isEqualTo("first");
        assertThat(space.read(Template.of(TaskEntry.class)).orElseThrow().topic()).isEqualTo("second");

        clock.advance(Duration.ofMinutes(2));
        space.sweepNow();
        assertThat(space.read(Template.of(TaskEntry.class)).orElseThrow().topic()).isEqualTo("first");
        assertThat(space.readAll(Template.of(TaskEntry.class), 10))
                .extracting(TaskEntry::topic).containsExactly("first", "second");
    }

    /** Spec §7.2: matching is partitioned by type; a template never matches another type. */
    @Test
    void templatesNeverMatchAnotherType() {
        space.write(new TaskEntry("task", 1), MINUTES_30);
        space.write(new FindingEntry("finding", "summary"), MINUTES_30);

        assertThat(space.readAll(Template.of(TaskEntry.class), 10))
                .containsExactly(new TaskEntry("task", 1));
        assertThat(space.readAll(Template.of(FindingEntry.class), 10))
                .containsExactly(new FindingEntry("finding", "summary"));

        TakenEntry<FindingEntry> taken = space.take(Template.of(FindingEntry.class),
                MINUTES_10, Duration.ZERO).orElseThrow();
        assertThat(taken.entry()).isEqualTo(new FindingEntry("finding", "summary"));
        assertThat(space.read(Template.of(TaskEntry.class))).contains(new TaskEntry("task", 1));
    }

    interface Job {
    }

    public record AlphaJob(String id) implements Job {
    }

    public record BetaJob(String id) implements Job {
    }

    /** Spec §7.2: a supertype template matches every assignable type bucket. */
    @Test
    void templateOnASupertypeMatchesAllAssignableBuckets() {
        space.write(new AlphaJob("a1"), MINUTES_30);
        space.write(new BetaJob("b1"), MINUTES_30);
        space.write(new TaskEntry("unrelated", 1), MINUTES_30);

        assertThat(space.readAll(Template.of(Job.class), 10))
                .containsExactlyInAnyOrder(new AlphaJob("a1"), new BetaJob("b1"));
        assertThat(space.readAll(Template.of(AlphaJob.class), 10))
                .containsExactly(new AlphaJob("a1"));

        TakenEntry<Job> taken = space.take(Template.of(Job.class), MINUTES_10, Duration.ZERO)
                .orElseThrow();
        space.complete(taken);
        assertThat(space.readAll(Template.of(Job.class), 10)).hasSize(1);
    }

    /** SPEC §4.2 v0.1.13 (TODO-9-10-11 B6): a local space seen as another agent of the same peer attributes that agent's writes and results to it, attested when the agent has a key of its own, as a replicated space's views do. */
    @Test
    void anAgentViewAttributesWritesAndResultsToItsAgent() {
        ai.badmonkey.agentspaces.identity.PeerIdentity peer = ai.badmonkey.agentspaces.identity.PeerIdentity.generate();
        LocalSpace shared = LocalSpace.builder("shared", peer.agent("host")).clock(TestClock.create()).build();
        try {
            ai.badmonkey.agentspaces.api.space.Space auditor = shared.as(
                    peer.renewingSubordinate("auditor", Duration.ofHours(1), TestClock.create()));
            ai.badmonkey.agentspaces.api.space.Space clerk = shared.as(peer.agentIdentity("clerk"));
            auditor.write(new TaskEntry("by auditor", 1), Lease.of(Duration.ofMinutes(5)));
            clerk.write(new TaskEntry("by clerk", 2), Lease.of(Duration.ofMinutes(5)));
            assertThat(auditor.writer()).contains(peer.agent("auditor"));
            var issued = shared.readAllIssued(Template.of(TaskEntry.class), 10);
            assertThat(issued).extracting(i -> i.entry().topic() + "|" + i.issuer().localName()
                    + "|" + i.attestation()).containsExactlyInAnyOrder(
                    "by auditor|auditor|AGENT_ATTESTED", "by clerk|clerk|PEER_ASSERTED");

            TakenEntry<TaskEntry> taken = auditor.take(Template.of(TaskEntry.class).where("topic",
                    eq("by clerk")), Lease.of(Duration.ofMinutes(1)), Duration.ZERO).orElseThrow();
            auditor.complete(taken, new FindingEntry("by clerk", "audited"), Lease.of(Duration.ofMinutes(5)));
            assertThat(shared.readAllIssued(Template.of(FindingEntry.class), 10)).singleElement()
                    .satisfies(i -> {
                        assertThat(i.issuer()).isEqualTo(peer.agent("auditor"));
                        assertThat(i.attestation()).isEqualTo(
                                ai.badmonkey.agentspaces.api.space.Space.Attestation.AGENT_ATTESTED);
                    });
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> shared.as(
                            ai.badmonkey.agentspaces.identity.PeerIdentity.generate().agentIdentity("x")))
                    .isInstanceOf(IllegalArgumentException.class);
        } finally {
            shared.close();
        }
    }
}
