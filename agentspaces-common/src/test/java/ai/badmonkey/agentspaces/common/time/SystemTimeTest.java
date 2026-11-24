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
package ai.badmonkey.agentspaces.common.time;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link SystemTime}'s invariants that hold without reading any timing (srad/agentspaces-event-loop.md §3.1 TS-1,
 * TS-2, TS-4). No test here asserts how long anything took; each waits on the task it scheduled, and the class-level
 * timeout is a hang guard only.
 */
@Timeout(60)
class SystemTimeTest {

    @Test
    void aTaskRunsOnAVirtualThreadAndNeverOnTheTimerThread() throws Exception {
        SystemTime time = new SystemTime("test-timer-ts2");
        CountDownLatch ran = new CountDownLatch(2);
        AtomicReference<Thread> delayed = new AtomicReference<>();
        AtomicReference<Thread> immediate = new AtomicReference<>();
        time.schedule(Duration.ofMillis(1), () -> {
            delayed.set(Thread.currentThread());
            ran.countDown();
        });
        time.schedule(Duration.ZERO, () -> {
            immediate.set(Thread.currentThread());
            ran.countDown();
        });
        ran.await();
        assertThat(delayed.get().isVirtual()).as("TS-2: a delayed task runs on a virtual thread").isTrue();
        assertThat(delayed.get().getName()).isNotEqualTo("test-timer-ts2");
        assertThat(immediate.get().isVirtual()).as("a task with no delay also runs on a virtual thread").isTrue();
    }

    @Test
    void theTimerThreadStartsOnlyWhenATaskHasToWait() throws Exception {
        SystemTime time = new SystemTime("test-timer-ts4");
        assertThat(time.timerStarted()).as("TS-4: a source that scheduled nothing has no timer thread").isFalse();
        CountDownLatch ran = new CountDownLatch(1);
        time.schedule(Duration.ZERO, ran::countDown);
        ran.await();
        assertThat(time.timerStarted()).as("a task due at once skips the timer thread").isFalse();
        TimeSource.Scheduled far = time.schedule(Duration.ofHours(1), () -> { });
        assertThat(time.timerStarted()).isTrue();
        Thread timer = Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getName().equals("test-timer-ts4")).findFirst().orElseThrow();
        assertThat(timer.isDaemon()).as("the timer never holds the JVM open").isTrue();
        assertThat(timer.isVirtual()).as("the one allowlisted platform timer (§3.8)").isFalse();
        assertThat(far.cancel()).isTrue();
    }

    @Test
    void aCancelledTaskNeverRunsAndCancelAfterARunIsFalse() throws Exception {
        SystemTime time = new SystemTime("test-timer-cancel");
        AtomicBoolean ranFar = new AtomicBoolean();
        TimeSource.Scheduled far = time.schedule(Duration.ofHours(1), () -> ranFar.set(true));
        assertThat(time.queued()).isEqualTo(1);
        assertThat(far.cancel()).as("TS-2: cancel before the run stops it").isTrue();
        assertThat(far.cancel()).as("a second cancel finds nothing to stop").isFalse();
        assertThat(time.queued()).as("a cancelled task leaves the delay queue").isZero();

        CountDownLatch ran = new CountDownLatch(1);
        TimeSource.Scheduled soon = time.schedule(Duration.ofMillis(1), ran::countDown);
        ran.await();
        assertThat(soon.cancel()).as("TS-2: cancel after the run returns false").isFalse();
        assertThat(ranFar).isFalse();
    }

    @Test
    void aTaskAtNeverIsHeldByNobodyAndNeverRuns() {
        SystemTime time = new SystemTime("test-timer-never");
        TimeSource.Scheduled never = time.scheduleAt(Long.MAX_VALUE, () -> {
            throw new AssertionError("ran");
        });
        assertThat(never.dueNanos()).isEqualTo(Long.MAX_VALUE);
        assertThat(time.queued()).isZero();
        assertThat(time.timerStarted()).isFalse();
        assertThat(never.cancel()).isTrue();
        // An overflowing delay saturates to NEVER rather than wrapping into the past.
        TimeSource.Scheduled huge = time.schedule(Duration.ofSeconds(Long.MAX_VALUE / 2), () -> {
            throw new AssertionError("ran");
        });
        assertThat(huge.dueNanos()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void manyTasksAtOneDeadlineAllRunOnce() throws Exception {
        SystemTime time = new SystemTime("test-timer-many");
        int n = 500;
        CountDownLatch ran = new CountDownLatch(n);
        java.util.concurrent.atomic.AtomicIntegerArray runs = new java.util.concurrent.atomic.AtomicIntegerArray(n);
        long at = time.monoNanos() + Duration.ofMillis(2).toNanos();
        for (int i = 0; i < n; i++) {
            int index = i;
            time.scheduleAt(at, () -> {
                runs.incrementAndGet(index);
                ran.countDown();
            });
        }
        ran.await();
        for (int i = 0; i < n; i++) {
            assertThat(runs.get(i)).as("task %d ran exactly once", i).isEqualTo(1);
        }
        assertThat(time.dispatched()).isEqualTo(n);
    }

    @Test
    void monotonicTimeNeverGoesBackwards() {
        TimeSource time = TimeSource.system();
        long last = time.monoNanos();
        for (int i = 0; i < 10_000; i++) {
            long now = time.monoNanos();
            assertThat(now - last).as("TS-1").isGreaterThanOrEqualTo(0);
            last = now;
        }
        assertThat(time.virtual()).isFalse();
    }

    @Test
    void aNodeRunOrFlushRunsOnTheLoopPoolAndNeverOnACarrierOrTheTimerThread() throws Exception {
        SystemTime time = new SystemTime("test-timer-loop");
        AutoCloseable open = time.loopPool().open();
        try {
            CountDownLatch ran = new CountDownLatch(2);
            AtomicReference<Thread> delayed = new AtomicReference<>();
            AtomicReference<Thread> immediate = new AtomicReference<>();
            time.scheduleOnLoop(System.nanoTime() + 2_000_000L, () -> {
                delayed.set(Thread.currentThread());
                ran.countDown();
            });
            time.scheduleOnLoop(System.nanoTime() - 1, () -> {
                immediate.set(Thread.currentThread());
                ran.countDown();
            });
            ran.await();
            for (Thread t : java.util.List.of(delayed.get(), immediate.get())) {
                assertThat(t.isVirtual()).as("decision 2: off the application's carriers").isFalse();
                assertThat(t.getName()).as("on the pool named after the timer").startsWith("test-timer-loop-loop-");
            }
            assertThat(time.dispatchLatency().total()).as("EL-X-10: the timed one's dispatch latency is recorded")
                    .isEqualTo(1);
            assertThat(TimeSource.system().loopPool()).as("the process's pool").isNotNull();
        } finally {
            open.close();
        }
    }

    @Test
    void aWrappedSourceSchedulesOnItsBasesLoopPool() throws Exception {
        SystemTime time = new SystemTime("test-timer-wrapped");
        TimeSource wrapped = TimeSources.withWall(java.time.Clock.systemUTC(), time);
        assertThat(wrapped.loopPool()).isSameAs(time.loopPool());
        CountDownLatch ran = new CountDownLatch(1);
        AtomicReference<Thread> on = new AtomicReference<>();
        wrapped.scheduleOnLoop(wrapped.monoNanos() + 1_000_000L, () -> {
            on.set(Thread.currentThread());
            ran.countDown();
        });
        ran.await();
        assertThat(on.get().getName()).startsWith("test-timer-wrapped-loop-");
        TimeSource following = TimeSources.followingWall(java.time.Clock.systemUTC(), time);
        CountDownLatch ran2 = new CountDownLatch(1);
        AtomicReference<Thread> on2 = new AtomicReference<>();
        following.scheduleOnLoop(following.monoNanos() + 1_000_000L, () -> {
            on2.set(Thread.currentThread());
            ran2.countDown();
        });
        ran2.await();
        assertThat(on2.get().getName()).startsWith("test-timer-wrapped-loop-");
    }
}
