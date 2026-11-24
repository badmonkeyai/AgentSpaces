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
package ai.badmonkey.agentspaces.test;

import ai.badmonkey.agentspaces.common.time.TimeSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link VirtualTime} (srad/agentspaces-event-loop.md §3.1 TS-1 to TS-3, TS-5, WD-7): time moves only on an advance,
 * due work runs in deadline order on settled worlds, sleepers are released on time, and a task that never settles
 * fails the advance by name. The class-level timeout is a hang guard only.
 */
@Timeout(60)
class VirtualTimeTest {

    private static final long MS = 1_000_000L;

    @Test
    void nothingRunsAndNoTimePassesWithoutAnAdvance() {
        VirtualTime time = VirtualTime.create();
        long start = time.monoNanos();
        Instant wall = time.instant();
        AtomicBoolean ran = new AtomicBoolean();
        time.schedule(Duration.ZERO, () -> ran.set(true));
        assertThat(ran).as("a task due now waits for the next advance").isFalse();
        assertThat(time.monoNanos()).isEqualTo(start);
        assertThat(time.instant()).isEqualTo(wall);
        time.runDue();
        assertThat(ran).as("an advance of zero runs what is due").isTrue();
        assertThat(time.monoNanos()).isEqualTo(start);
        assertThat(time.virtual()).isTrue();
    }

    @Test
    void anAdvanceRunsDueTasksInDeadlineOrderTiesInScheduleOrderEachAtItsDeadline() {
        VirtualTime time = VirtualTime.create();
        long start = time.monoNanos();
        List<String> order = new CopyOnWriteArrayList<>();
        time.schedule(Duration.ofMillis(30), () -> order.add("c@" + (time.monoNanos() - start) / MS));
        time.schedule(Duration.ofMillis(10), () -> order.add("a1@" + (time.monoNanos() - start) / MS));
        time.schedule(Duration.ofMillis(10), () -> order.add("a2@" + (time.monoNanos() - start) / MS));
        time.schedule(Duration.ofMillis(20), () -> order.add("b@" + (time.monoNanos() - start) / MS));
        time.schedule(Duration.ofMillis(10), () -> order.add("a3@" + (time.monoNanos() - start) / MS));
        time.schedule(Duration.ofMillis(31), () -> order.add("late"));
        time.advance(Duration.ofMillis(30));
        assertThat(order).as("TS-3 and TS-5").containsExactly("a1@10", "a2@10", "a3@10", "b@20", "c@30");
        assertThat(time.monoNanos() - start).isEqualTo(30 * MS);
        assertThat(time.pending()).as("the task past the span waits").isEqualTo(1);
        time.advance(Duration.ofMillis(1));
        assertThat(order).endsWith("late");
    }

    @Test
    void wallTimeMovesWithMonotonicTimeAndAStepMovesWallAlone() {
        Instant epoch = Instant.parse("2026-11-21T00:00:00Z");
        VirtualTime time = VirtualTime.startingAt(epoch);
        long start = time.monoNanos();
        time.advance(Duration.ofSeconds(5));
        assertThat(time.instant()).isEqualTo(epoch.plusSeconds(5));
        assertThat(time.millis()).isEqualTo(epoch.plusSeconds(5).toEpochMilli());
        time.stepWall(Duration.ofSeconds(-30));
        assertThat(time.instant()).as("TS-1: wall time may step backwards").isEqualTo(epoch.minusSeconds(25));
        assertThat(time.monoNanos() - start).as("a wall step leaves monotonic time alone")
                .isEqualTo(Duration.ofSeconds(5).toNanos());
        TimeSource skewed = time.offset(Duration.ofSeconds(2));
        assertThat(skewed.instant()).isEqualTo(time.instant().plusSeconds(2));
        assertThat(skewed.monoNanos()).isEqualTo(time.monoNanos());
        assertThat(skewed.virtual()).isTrue();
    }

    @Test
    void aTaskScheduledByATaskWithinTheSpanRunsInTheSameAdvance() {
        VirtualTime time = VirtualTime.create();
        long start = time.monoNanos();
        List<Long> at = new CopyOnWriteArrayList<>();
        time.schedule(Duration.ofMillis(10), () -> {
            at.add((time.monoNanos() - start) / MS);
            time.schedule(Duration.ofMillis(15), () -> at.add((time.monoNanos() - start) / MS));
            time.schedule(Duration.ZERO, () -> at.add((time.monoNanos() - start) / MS));
        });
        time.advance(Duration.ofMillis(100));
        assertThat(at).containsExactly(10L, 10L, 25L);
    }

    @Test
    void sleepersAreReleasedAtTheirDeadlinesAndTheAdvanceWaitsForThemToParkAgain() {
        VirtualTime time = VirtualTime.create();
        long start = time.monoNanos();
        List<Long> wakes = new CopyOnWriteArrayList<>();
        time.schedule(Duration.ZERO, () -> {
            try {
                for (int i = 0; i < 10; i++) {
                    time.sleep(Duration.ofMillis(100));
                    wakes.add((time.monoNanos() - start) / MS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        time.advance(Duration.ofMillis(450));
        assertThat(wakes).as("released in order, each settled before the advance went on")
                .containsExactly(100L, 200L, 300L, 400L);
        time.advance(Duration.ofSeconds(1));
        assertThat(wakes).hasSize(10).endsWith(1000L);
    }

    @Test
    void aCancelledTaskNeverRunsAndCancelAfterARunIsFalse() {
        VirtualTime time = VirtualTime.create();
        AtomicBoolean ran = new AtomicBoolean();
        TimeSource.Scheduled cancelled = time.schedule(Duration.ofMillis(5), () -> ran.set(true));
        TimeSource.Scheduled kept = time.schedule(Duration.ofMillis(5), () -> { });
        assertThat(cancelled.cancel()).isTrue();
        assertThat(cancelled.cancel()).isFalse();
        assertThat(time.pending()).as("a cancelled task leaves the queue").isEqualTo(1);
        time.advance(Duration.ofMillis(10));
        assertThat(ran).as("TS-2").isFalse();
        assertThat(kept.cancel()).as("TS-2: cancel after the run returns false").isFalse();
        TimeSource.Scheduled never = time.scheduleAt(Long.MAX_VALUE, () -> ran.set(true));
        assertThat(time.pending()).as("a task at NEVER is held by nobody").isZero();
        assertThat(never.cancel()).isTrue();
    }

    @Test
    void scheduledWorkRunsOnVirtualThreadsTheSourceStarted() {
        VirtualTime time = VirtualTime.create();
        AtomicReference<Thread> thread = new AtomicReference<>();
        time.schedule(Duration.ofMillis(1), () -> thread.set(Thread.currentThread()));
        time.advance(Duration.ofMillis(1));
        assertThat(thread.get()).isNotNull();
        assertThat(thread.get().isVirtual()).as("TS-2").isTrue();
        assertThat(thread.get()).isNotEqualTo(Thread.currentThread());
        assertThat(time.tasksRun()).isEqualTo(1);
    }

    @Test
    void aTaskThatThrowsFailsTheAdvanceAfterItSettles() {
        VirtualTime time = VirtualTime.create();
        AtomicBoolean after = new AtomicBoolean();
        time.schedule(Duration.ofMillis(1), () -> {
            throw new IllegalStateException("expected by the test");
        });
        time.schedule(Duration.ofMillis(2), () -> after.set(true));
        assertThatThrownBy(() -> time.advance(Duration.ofMillis(5)))
                .isInstanceOf(AssertionError.class)
                .hasRootCauseMessage("expected by the test");
        assertThat(after).as("the advance still ran the rest of its span").isTrue();
        time.advance(Duration.ofMillis(5)); // the failure was reported once
    }

    @Test
    void aTaskThatNeverSettlesFailsTheAdvanceByNameWithinTheQuiescenceBound() throws Exception {
        VirtualTime time = VirtualTime.create().quiescenceBound(Duration.ofMillis(200));
        CountDownLatch hold = new CountDownLatch(1);
        time.schedule(Duration.ofMillis(1), () -> {
            try {
                hold.await(); // blocked on something virtual time cannot release (WD-7)
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        try {
            assertThatThrownBy(() -> time.advance(Duration.ofMillis(1)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("did not settle")
                    .hasMessageContaining("virtual-time-task-");
        } finally {
            hold.countDown();
        }
    }

    @Test
    void anAdvanceFromInsideATaskIsRefused() {
        VirtualTime time = VirtualTime.create();
        AtomicReference<Throwable> refused = new AtomicReference<>();
        time.schedule(Duration.ZERO, () -> {
            try {
                time.advance(Duration.ofMillis(1));
            } catch (IllegalStateException e) {
                refused.set(e);
            }
        });
        time.runDue();
        assertThat(refused.get()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void aSleeperOnAThreadTheSourceDidNotStartIsReleasedToo() throws Exception {
        VirtualTime time = VirtualTime.create();
        CountDownLatch woke = new CountDownLatch(1);
        CountDownLatch parked = new CountDownLatch(1);
        List<Throwable> errors = new ArrayList<>();
        Thread outside = Thread.ofVirtual().start(() -> {
            try {
                parked.countDown();
                time.sleep(Duration.ofSeconds(3));
                woke.countDown();
            } catch (InterruptedException e) {
                errors.add(e);
            }
        });
        parked.await();
        while (time.pending() == 0) {
            Thread.onSpinWait(); // the sleeper registers itself just after the latch
        }
        time.advance(Duration.ofSeconds(3));
        woke.await();
        outside.join();
        assertThat(errors).isEmpty();
    }
}
