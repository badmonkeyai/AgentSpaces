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

import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.api.error.LeaseExpiredException;
import ai.badmonkey.agentspaces.api.error.SpaceClosedException;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.Subscription;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.test.Fixtures;
import ai.badmonkey.agentspaces.test.Fixtures.FindingEntry;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
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

    /**
     * RE-H-01 and RE-H-02 (srad/replication-by-entry.md): a renewal for less than the lease has left is counted, write
     * and take alike, and logged once for the space, and it keeps the longer lease, since a renewal never shortens.
     */
    @Test
    void aShorteningRenewalIsCountedLoggedOnceAndKeepsTheLongerLease() {
        List<java.util.logging.LogRecord> logged = new CopyOnWriteArrayList<>();
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger(LocalSpace.class.getName());
        java.util.logging.Handler capture = new java.util.logging.Handler() {
            @Override public void publish(java.util.logging.LogRecord record) { logged.add(record); }
            @Override public void flush() { }
            @Override public void close() { }
        };
        logger.addHandler(capture);
        try {
            EntryHandle handle = space.write(new TaskEntry("gossip", 3), MINUTES_10);
            handle.renew(Duration.ofMinutes(20));
            assertThat(space.renewShortenings()).isZero();
            handle.renew(Duration.ofMinutes(1));
            handle.renew(Duration.ofSeconds(30));
            assertThat(space.renewShortenings()).isEqualTo(2);
            assertThat(logged).hasSize(1);
            assertThat(logged.get(0).getMessage()).contains("space tasks", handle.entryId().value(), "write lease",
                    "kept the longer lease");

            TakenEntry<TaskEntry> taken = space.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO)
                    .orElseThrow();
            taken.renew(Duration.ofMinutes(1));
            assertThat(space.renewShortenings()).isEqualTo(3);
            assertThat(logged).as("still once for the space").hasSize(1);
            clock.advance(Duration.ofMinutes(2));
            assertThat(space.read(Template.of(TaskEntry.class))).as("the take kept its ten minutes, so the entry is "
                    + "still held").isEmpty();
            space.complete(taken);
        } finally {
            logger.removeHandler(capture);
        }
    }

    /**
     * RE-H-02's clamp on a local space, the same rule as a replicated one: a shorter extension keeps the longer lease,
     * a longer one extends it, for the write lease, the take lease and a subscription.
     */
    @Test
    void aRenewalSetsTheLaterOfTheCurrentExpiryAndNowPlusTheExtension() {
        EntryHandle handle = space.write(new TaskEntry("clamp", 1), MINUTES_10);
        clock.advance(Duration.ofMinutes(1));
        handle.renew(Duration.ofMinutes(2)); // nine minutes were left; two would shorten it
        clock.advance(Duration.ofMinutes(8));
        assertThat(space.read(Template.of(TaskEntry.class))).as("still the ten-minute lease, at minute nine")
                .isPresent();
        handle.renew(Duration.ofMinutes(5)); // one minute left: five extends it
        clock.advance(Duration.ofMinutes(4));
        assertThat(space.read(Template.of(TaskEntry.class))).as("extended to minute fourteen").isPresent();
        clock.advance(Duration.ofMinutes(2));
        assertThat(space.read(Template.of(TaskEntry.class))).as("lapsed at minute fourteen").isEmpty();

        space.write(new TaskEntry("held", 2), Lease.of(Duration.ofHours(1)));
        TakenEntry<TaskEntry> taken = space.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO)
                .orElseThrow();
        taken.renew(Duration.ofSeconds(30));
        clock.advance(Duration.ofMinutes(9));
        assertThat(space.take(Template.of(TaskEntry.class), MINUTES_10, Duration.ZERO))
                .as("the take kept its ten minutes").isEmpty();
        taken.renew(Duration.ofMinutes(10));
        clock.advance(Duration.ofMinutes(5));
        space.complete(taken); // still held at minute fourteen: the longer renewal extended it

        List<SpaceEvent<TaskEntry>> received = new CopyOnWriteArrayList<>();
        Subscription subscription = space.notify(Template.of(TaskEntry.class), received::add, MINUTES_10);
        subscription.renew(Duration.ofSeconds(1));
        clock.advance(Duration.ofMinutes(9));
        space.write(new TaskEntry("heard", 3), MINUTES_10);
        assertThat(received).as("the subscription kept its ten minutes").hasSize(1);
        subscription.close();
    }

    /**
     * RE-H-02's preconditions on a local space: renewal needs the entry present (a cancelled or completed one is gone),
     * unexpired, and renewed by its issuer; each refusal is the exception the API documents.
     */
    @Test
    void aRenewalNeedsTheEntryPresentUnexpiredAndItsIssuer() {
        EntryHandle cancelled = space.write(new TaskEntry("cancelled", 1), MINUTES_10);
        cancelled.cancel();
        assertThatThrownBy(() -> cancelled.renew(Duration.ofMinutes(5))).isInstanceOf(LeaseExpiredException.class);

        EntryHandle completed = space.write(new TaskEntry("completed", 2), MINUTES_10);
        space.complete(space.take(Template.of(TaskEntry.class).where("topic", eq("completed")), MINUTES_10,
                Duration.ZERO).orElseThrow());
        assertThatThrownBy(() -> completed.renew(Duration.ofMinutes(5))).isInstanceOf(LeaseExpiredException.class);

        EntryHandle lapsed = space.write(new TaskEntry("lapsed", 3), Lease.of(Duration.ofMinutes(1)));
        clock.advance(Duration.ofMinutes(2));
        assertThatThrownBy(() -> lapsed.renew(Duration.ofMinutes(5))).isInstanceOf(LeaseExpiredException.class);

        EntryHandle owned = space.write(new TaskEntry("owned", 4), MINUTES_10);
        EntryHandle other = space.handleAs(owned.entryId(), Fixtures.agent("someone-else"));
        assertThatThrownBy(() -> other.renew(Duration.ofMinutes(20))).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot act on");
        owned.renew(Duration.ofMinutes(20)); // the issuer's own handle still renews
    }

    /**
     * The lease a reader sees moves with a renewal (RESUME-RBE core item a): readAllEntries reported the lease the
     * entry was written with after renew, because renew moved the expiry the sweep uses but not the record the views
     * are built from. A shortening renewal leaves the reported lease where it was, as it leaves the lease.
     */
    @Test
    void readAllEntriesReportsTheRenewedWriteLease() {
        long writtenAt = clock.millis();
        EntryHandle handle = space.write(new TaskEntry("renewed", 1), MINUTES_10);
        assertThat(onlyEntry().lease().expiresAtMillis()).isEqualTo(writtenAt + MINUTES_10.duration().toMillis());

        clock.advance(Duration.ofMinutes(1));
        handle.renew(Duration.ofMinutes(30));
        Space.Entry<TaskEntry> renewed = onlyEntry();
        assertThat(renewed.lease().expiresAtMillis()).as("now plus the extension, not the written ten minutes")
                .isEqualTo(clock.millis() + Duration.ofMinutes(30).toMillis());
        assertThat(renewed.lease().kind()).isEqualTo(LeaseKind.WRITE);
        assertThat(renewed.lease().holder()).isEqualTo(Fixtures.agent("coordinator"));

        long expected = renewed.lease().expiresAtMillis();
        handle.renew(Duration.ofMinutes(1));
        assertThat(onlyEntry().lease().expiresAtMillis()).as("a shorter renewal changes nothing").isEqualTo(expected);

        // Renewed while taken, the write lease shows once the take lapses and the entry reappears.
        TakenEntry<TaskEntry> taken = space.take(Template.of(TaskEntry.class), Lease.of(Duration.ofMinutes(1)),
                Duration.ZERO).orElseThrow();
        assertThat(space.readAllEntries(Template.of(TaskEntry.class), 10)).isEmpty();
        handle.renew(Duration.ofHours(2));
        long renewedWhileTaken = clock.millis() + Duration.ofHours(2).toMillis();
        clock.advance(Duration.ofMinutes(2));
        assertThat(onlyEntry().lease().expiresAtMillis()).isEqualTo(renewedWhileTaken);
        assertThat(onlyEntry().lease().kind()).isEqualTo(LeaseKind.WRITE);
        assertThat(taken.entry().topic()).isEqualTo("renewed");
    }

    /**
     * A take that lapses gives the write lease back to the entry's writer: the record names the writer as the
     * holder, so an entry an agent view wrote keeps that agent as the holder, not the space's own identity.
     */
    @Test
    void aLapsedTakeRestoresTheWritersLease() {
        AgentId worker = new AgentId(Fixtures.agent("coordinator").peer(), "worker");
        Space asWorker = space.as(new ai.badmonkey.agentspaces.api.spi.AgentIdentity() {
            @Override public AgentId id() { return worker; }
            @Override public byte[] publicKey() { return new byte[32]; }
            @Override public byte[] sign(byte[] bytes) { throw new UnsupportedOperationException("unsigned space"); }
            @Override public Optional<ai.badmonkey.agentspaces.api.security.AgentCertificate> certificate() {
                return Optional.empty();
            }
        });
        asWorker.write(new TaskEntry("worker's", 1), MINUTES_10);
        assertThat(onlyEntry().lease().holder()).isEqualTo(worker);

        space.take(Template.of(TaskEntry.class), Lease.of(Duration.ofMinutes(1)), Duration.ZERO).orElseThrow();
        clock.advance(Duration.ofMinutes(2));
        Space.Entry<TaskEntry> back = onlyEntry();
        assertThat(back.lease().holder()).isEqualTo(worker);
        assertThat(back.lease().kind()).isEqualTo(LeaseKind.WRITE);
        assertThat(back.issuer()).isEqualTo(worker);
    }

    private Space.Entry<TaskEntry> onlyEntry() {
        List<Space.Entry<TaskEntry>> entries = space.readAllEntries(Template.of(TaskEntry.class), 10);
        assertThat(entries).hasSize(1);
        return entries.get(0);
    }

    /** {@code agentspaces.space.renew-shortening-log=false}: the count runs and nothing is logged. */
    @Test
    void theShorteningLogCanBeTurnedOffAndTheCountStillRuns() {
        List<java.util.logging.LogRecord> logged = new CopyOnWriteArrayList<>();
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger(LocalSpace.class.getName());
        java.util.logging.Handler capture = new java.util.logging.Handler() {
            @Override public void publish(java.util.logging.LogRecord record) { logged.add(record); }
            @Override public void flush() { }
            @Override public void close() { }
        };
        logger.addHandler(capture);
        try (LocalSpace quiet = LocalSpace.builder("quiet", Fixtures.agent("coordinator")).clock(clock)
                .renewShorteningLog(false).build()) {
            quiet.write(new TaskEntry("x", 1), MINUTES_10).renew(Duration.ofMinutes(1));
            assertThat(quiet.renewShortenings()).isEqualTo(1);
            assertThat(logged).isEmpty();
        } finally {
            logger.removeHandler(capture);
        }
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

    // ------------------------------------------- issue #16 §9.2: tags on templates, reads, events

    /** Issue #16 §9.2: read, readAll, take and the blocking read apply tag conditions before the value is matched. */
    @Test
    void tagConditionsSelectOnReadReadAllAndTake() {
        space.write(new TaskEntry("eu-task", 5), MINUTES_30, Map.of("region", "eu"));
        space.write(new TaskEntry("us-task", 5), MINUTES_30, Map.of("region", "us"));
        space.write(new TaskEntry("untagged", 5), MINUTES_30);

        assertThat(space.read(Template.of(TaskEntry.class).whereTag("region", eq("eu"))))
                .contains(new TaskEntry("eu-task", 5));
        assertThat(space.read(Template.of(TaskEntry.class).whereTag("region", eq("apac"))))
                .isEmpty();
        assertThat(space.read(Template.of(TaskEntry.class).whereTag("region", eq("apac")),
                Duration.ofMillis(60))).isEmpty();
        assertThat(space.readAll(Template.of(TaskEntry.class).hasTag("region"), 10))
                .extracting(TaskEntry::topic).containsExactly("eu-task", "us-task");
        assertThat(space.readAll(Template.of(TaskEntry.class)
                .where("priority", gte(5)).whereTag("region", eq("us")), 10))
                .extracting(TaskEntry::topic).containsExactly("us-task");
        assertThat(space.readAllIssued(Template.of(TaskEntry.class).hasTag("region"), 10))
                .extracting(i -> i.entry().topic()).containsExactly("eu-task", "us-task");

        TakenEntry<TaskEntry> taken = space.take(
                Template.of(TaskEntry.class).whereTag("region", eq("us")),
                MINUTES_10, Duration.ZERO).orElseThrow();
        assertThat(taken.entry().topic()).isEqualTo("us-task");
        assertThat(space.take(Template.of(TaskEntry.class).whereTag("region", eq("us")),
                MINUTES_10, Duration.ZERO)).isEmpty();
        assertThat(space.readAll(Template.of(TaskEntry.class).hasTag("region"), 10))
                .extracting(TaskEntry::topic).containsExactly("eu-task");
        // An untagged entry is still what a plain template sees first (FIFO among matches).
        assertThat(space.readAll(Template.of(TaskEntry.class), 10)).hasSize(2);
    }

    /** Issue #16 §9.2: a subscription with tag conditions sees only matching entries' events. */
    @Test
    void notifyHonorsTagConditionsAndWrittenEventsCarryDetails() {
        List<SpaceEvent<TaskEntry>> euOnly = new CopyOnWriteArrayList<>();
        List<SpaceEvent<TaskEntry>> all = new CopyOnWriteArrayList<>();
        space.notify(Template.of(TaskEntry.class).whereTag("region", eq("eu")), euOnly::add,
                MINUTES_30);
        space.notify(Template.of(TaskEntry.class), all::add, MINUTES_30);

        EntryHandle eu = space.write(new TaskEntry("eu-task", 1), MINUTES_30,
                Map.of("region", "eu", "tier", "gold"));
        space.write(new TaskEntry("us-task", 1), MINUTES_30, Map.of("region", "us"));
        space.write(new TaskEntry("untagged", 1), MINUTES_30);

        assertThat(all).hasSize(3);
        assertThat(euOnly).singleElement().satisfies(event -> {
            assertThat(event.kind()).isEqualTo(SpaceEvent.Kind.WRITTEN);
            assertThat(event.entryId()).isEqualTo(eu.entryId());
            assertThat(event.tags()).containsExactlyInAnyOrderEntriesOf(
                    Map.of("region", "eu", "tier", "gold"));
            assertThat(event.details()).isNotNull();
            assertThat(event.details().value()).isEqualTo(new TaskEntry("eu-task", 1));
            assertThat(event.details().entryId()).isEqualTo(eu.entryId());
            assertThat(event.details().issuer()).isEqualTo(Fixtures.agent("coordinator"));
            assertThat(event.details().lease().kind()).isEqualTo(LeaseKind.WRITE);
            assertThat(event.details().issued()).isNotNull();
        });
        assertThat(all.get(2).details()).isNotNull();
        assertThat(all.get(2).tags()).isEmpty();

        // The tag filter applies to every kind: the us-task's lifecycle never reaches euOnly.
        TakenEntry<TaskEntry> taken = space.take(
                Template.of(TaskEntry.class).whereTag("region", eq("us")),
                Lease.of(Duration.ofMinutes(1)), Duration.ZERO).orElseThrow();
        clock.advance(Duration.ofMinutes(2));
        space.sweepNow();
        assertThat(euOnly).hasSize(1);
        assertThat(all).extracting(SpaceEvent::kind).containsExactly(SpaceEvent.Kind.WRITTEN,
                SpaceEvent.Kind.WRITTEN, SpaceEvent.Kind.WRITTEN, SpaceEvent.Kind.TAKEN,
                SpaceEvent.Kind.REAPPEARED);
        SpaceEvent<TaskEntry> reappeared = all.get(4);
        assertThat(reappeared.entryId()).isEqualTo(taken.entryId());
        assertThat(reappeared.details()).isNotNull();
        assertThat(reappeared.tags()).containsExactly(Map.entry("region", "us"));
        assertThat(reappeared.details().lease().kind()).isEqualTo(LeaseKind.WRITE);
    }

    /** Issue #16 §9.2: readAllEntries returns the record's metadata; readAllIssued is unchanged. */
    @Test
    void readAllEntriesReturnsTheRecordsMetadata() {
        ai.badmonkey.agentspaces.identity.PeerIdentity peer =
                ai.badmonkey.agentspaces.identity.PeerIdentity.generate();
        LocalSpace shared = LocalSpace.builder("shared", peer.agent("host")).clock(clock).build();
        try {
            long before = clock.instant().toEpochMilli();
            EntryHandle plain = shared.write(new TaskEntry("plain", 1), MINUTES_30,
                    Map.of("region", "eu"));
            ai.badmonkey.agentspaces.api.space.Space auditor = shared.as(
                    peer.renewingSubordinate("auditor", Duration.ofHours(1), clock));
            EntryHandle attested = auditor.write(new TaskEntry("attested", 2), MINUTES_10,
                    Map.of("region", "us"));

            List<Space.Entry<TaskEntry>> entries =
                    shared.readAllEntries(Template.of(TaskEntry.class), 10);
            assertThat(entries).hasSize(2);
            Space.Entry<TaskEntry> first = entries.get(0);
            assertThat(first.value()).isEqualTo(new TaskEntry("plain", 1));
            assertThat(first.entryId()).isEqualTo(plain.entryId());
            assertThat(first.issuer()).isEqualTo(peer.agent("host"));
            assertThat(first.attestation()).isEqualTo(Space.Attestation.PEER_ASSERTED);
            assertThat(first.tags()).containsExactly(Map.entry("region", "eu"));
            assertThat(first.lease().kind()).isEqualTo(LeaseKind.WRITE);
            assertThat(first.lease().holder()).isEqualTo(peer.agent("host"));
            assertThat(first.lease().expiresAtMillis())
                    .isEqualTo(before + MINUTES_30.duration().toMillis());
            assertThat(first.issued().physical()).isGreaterThanOrEqualTo(before);
            Space.Entry<TaskEntry> second = entries.get(1);
            assertThat(second.entryId()).isEqualTo(attested.entryId());
            assertThat(second.issuer()).isEqualTo(peer.agent("auditor"));
            assertThat(second.attestation()).isEqualTo(Space.Attestation.AGENT_ATTESTED);
            assertThat(second.tags()).containsExactly(Map.entry("region", "us"));
            assertThat(second.lease().expiresAtMillis())
                    .isEqualTo(before + MINUTES_10.duration().toMillis());

            assertThat(shared.readAllEntries(Template.of(TaskEntry.class).hasTag("region"), 1))
                    .hasSize(1);
            assertThat(shared.readAllEntries(Template.of(TaskEntry.class)
                    .whereTag("region", eq("us")), 10))
                    .extracting(e -> e.value().topic()).containsExactly("attested");
            assertThat(auditor.readAllEntries(Template.of(TaskEntry.class), 10)).hasSize(2);
            assertThat(shared.readAllIssued(Template.of(TaskEntry.class), 10))
                    .containsExactly(
                            new Space.Issued<>(new TaskEntry("plain", 1), peer.agent("host")),
                            new Space.Issued<>(new TaskEntry("attested", 2), peer.agent("auditor"),
                                    Space.Attestation.AGENT_ATTESTED));

            // A held entry is not visible, as for readAll.
            TakenEntry<TaskEntry> taken = shared.take(Template.of(TaskEntry.class), MINUTES_10,
                    Duration.ZERO).orElseThrow();
            assertThat(shared.readAllEntries(Template.of(TaskEntry.class), 10))
                    .extracting(e -> e.value().topic()).containsExactly("attested");
            shared.complete(taken);
            assertThatThrownBy(() -> shared.readAllEntries(Template.of(TaskEntry.class), 0))
                    .isInstanceOf(IllegalArgumentException.class);
        } finally {
            shared.close();
        }
    }

    /** Issue #16 §9.2: the four-argument complete writes the result with its tags, atomically; the three-argument form writes none. */
    @Test
    void completeWithTagsWritesTheTaggedResultAtomically() {
        List<SpaceEvent<FindingEntry>> findings = new CopyOnWriteArrayList<>();
        List<SpaceEvent<TaskEntry>> tasks = new CopyOnWriteArrayList<>();
        space.notify(Template.of(FindingEntry.class), findings::add, MINUTES_30);
        space.notify(Template.of(TaskEntry.class), tasks::add, MINUTES_30);
        space.write(new TaskEntry("tagged", 1), MINUTES_30);
        space.write(new TaskEntry("plain", 2), MINUTES_30);

        TakenEntry<TaskEntry> first = space.take(Template.of(TaskEntry.class)
                .where("topic", eq("tagged")), MINUTES_10, Duration.ZERO).orElseThrow();
        EntryHandle taggedResult = space.complete(first, new FindingEntry("tagged", "done"),
                MINUTES_30, Map.of("rdf:type", "ex:Finding", "region", "eu"));
        TakenEntry<TaskEntry> second = space.take(Template.of(TaskEntry.class)
                .where("topic", eq("plain")), MINUTES_10, Duration.ZERO).orElseThrow();
        EntryHandle plainResult = space.complete(second, new FindingEntry("plain", "done"),
                MINUTES_30);

        assertThat(space.read(Template.of(TaskEntry.class))).isEmpty();
        List<Space.Entry<FindingEntry>> results =
                space.readAllEntries(Template.of(FindingEntry.class), 10);
        assertThat(results).hasSize(2);
        assertThat(results.get(0).entryId()).isEqualTo(taggedResult.entryId());
        assertThat(results.get(0).tags()).containsExactlyInAnyOrderEntriesOf(
                Map.of("rdf:type", "ex:Finding", "region", "eu"));
        assertThat(results.get(1).entryId()).isEqualTo(plainResult.entryId());
        assertThat(results.get(1).tags()).isEmpty();
        assertThat(space.read(Template.of(FindingEntry.class).whereTag("rdf:type",
                eq("ex:Finding")))).contains(new FindingEntry("tagged", "done"));

        // The completion and the tagged result land together, in that order.
        assertThat(tasks).extracting(SpaceEvent::kind).containsExactly(SpaceEvent.Kind.WRITTEN,
                SpaceEvent.Kind.WRITTEN, SpaceEvent.Kind.TAKEN, SpaceEvent.Kind.COMPLETED,
                SpaceEvent.Kind.TAKEN, SpaceEvent.Kind.COMPLETED);
        assertThat(findings).extracting(SpaceEvent::kind)
                .containsExactly(SpaceEvent.Kind.WRITTEN, SpaceEvent.Kind.WRITTEN);
        assertThat(findings.get(0).tags()).containsEntry("region", "eu");
        assertThat(findings.get(1).tags()).isEmpty();

        // A view completes with tags as its own agent.
        ai.badmonkey.agentspaces.identity.PeerIdentity peer =
                ai.badmonkey.agentspaces.identity.PeerIdentity.generate();
        LocalSpace shared = LocalSpace.builder("shared", peer.agent("host")).clock(clock).build();
        try {
            ai.badmonkey.agentspaces.api.space.Space clerk = shared.as(peer.agentIdentity("clerk"));
            shared.write(new TaskEntry("job", 1), MINUTES_30);
            TakenEntry<TaskEntry> job = clerk.take(Template.of(TaskEntry.class), MINUTES_10,
                    Duration.ZERO).orElseThrow();
            clerk.complete(job, new FindingEntry("job", "filed"), MINUTES_30, Map.of("by", "clerk"));
            assertThat(shared.readAllEntries(Template.of(FindingEntry.class), 10)).singleElement()
                    .satisfies(e -> {
                        assertThat(e.issuer()).isEqualTo(peer.agent("clerk"));
                        assertThat(e.tags()).containsExactly(Map.entry("by", "clerk"));
                    });
        } finally {
            shared.close();
        }
    }

    /** Issue #16 §10.2: the schema name a type is registered under, for the binder's bind-time check. */
    @Test
    void schemaNameOfRegistersAndNamesTheType() {
        assertThat(space.schemaNameOf(TaskEntry.class))
                .isEqualTo(new SimpleSchemaRegistry().register(TaskEntry.class));
        assertThat(space.schemaNameOf(TaskEntry.class)).isEqualTo(space.schemaNameOf(TaskEntry.class));
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
