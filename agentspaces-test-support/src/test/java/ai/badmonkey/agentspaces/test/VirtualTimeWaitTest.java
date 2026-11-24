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

import ai.badmonkey.agentspaces.common.time.ChangeSignal;
import ai.badmonkey.agentspaces.common.time.Periodic;
import ai.badmonkey.agentspaces.common.time.Waits;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The waits of group W on virtual time (srad/agentspaces-event-loop.md EL-W-01 to EL-W-03, §10.17): {@code waitOn}, the
 * change signal, the renewer as a task, and {@link VirtualTime#run}. Every bound here is exact, in virtual time.
 */
@Timeout(30)
class VirtualTimeWaitTest {

    @Test
    void aWaitOnAMonitorIsReleasedByTheAdvanceThatPassesItsDeadlineAndNotBefore() {
        VirtualTime time = VirtualTime.create();
        Object monitor = new Object();
        long start = time.monoNanos();
        VirtualTime.Outcome<Boolean> outcome = time.run(Duration.ofMillis(100), Duration.ofSeconds(5), () -> {
            synchronized (monitor) {
                return time.waitOn(monitor, start + Duration.ofMillis(750).toNanos());
            }
        });
        assertThat(outcome.value()).as("the deadline passed: no notify").isFalse();
        // released by the advance that crossed 750 ms, at the deadline itself, on the task's own clock
        assertThat(outcome.took()).isEqualTo(Duration.ofMillis(750));
    }

    @Test
    void aNotifyEndsTheWaitBeforeItsDeadline() throws Exception {
        VirtualTime time = VirtualTime.create();
        Object monitor = new Object();
        java.util.concurrent.CompletableFuture<Boolean> answer = new java.util.concurrent.CompletableFuture<>();
        time.schedule(Duration.ZERO, () -> {
            synchronized (monitor) {
                try {
                    answer.complete(time.waitOn(monitor, time.monoNanos() + Duration.ofSeconds(10).toNanos()));
                } catch (InterruptedException e) {
                    answer.completeExceptionally(e);
                }
            }
        });
        time.runDue();
        time.schedule(Duration.ofMillis(300), () -> {
            synchronized (monitor) {
                monitor.notifyAll();
            }
        });
        time.advance(Duration.ofMillis(300));
        // A wake by another thread's notify is settled only once the waiter holds the monitor again (the class
        // comment), so the answer is read from the waiter itself.
        assertThat(answer.get(5, java.util.concurrent.TimeUnit.SECONDS)).as("returned before its deadline").isTrue();
        time.advance(Duration.ofSeconds(10)); // the deadline's task was cancelled: nothing more runs
        assertThat(time.pending()).isZero();
    }

    @Test
    void aWaitOnASuspendedNodesClockIsHeldUntilTheResume() {
        VirtualTime time = VirtualTime.create();
        VirtualTime.NodeTime node = time.unwatchedNode();
        Object monitor = new Object();
        AtomicBoolean done = new AtomicBoolean();
        long deadline = node.monoNanos() + Duration.ofSeconds(2).toNanos();
        time.schedule(Duration.ZERO, () -> {
            synchronized (monitor) {
                try {
                    node.waitOn(monitor, deadline);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            done.set(true);
        });
        time.runDue();
        time.advance(Duration.ofSeconds(1));
        node.suspend();
        time.advance(Duration.ofMinutes(5));
        assertThat(done).as("the node's clock stood still: its deadline did not pass").isFalse();
        node.resume();
        time.advance(Duration.ofMillis(999));
        assertThat(done).isFalse();
        time.advance(Duration.ofMillis(1));
        assertThat(done).as("one more second on the node's own clock").isTrue();
    }

    @Test
    void aChangeSignalWakesItsWaiterAndAChangeBeforeTheWaitIsNotLost() throws Exception {
        VirtualTime time = VirtualTime.create();
        ChangeSignal signal = new ChangeSignal(time);
        long seen = signal.seen();
        signal.run(); // between the look and the wait
        assertThat(signal.await(seen, time.monoNanos() + 1)).as("returns at once, no advance needed").isTrue();

        long before = signal.seen();
        time.schedule(Duration.ofMillis(40), signal);
        VirtualTime.Outcome<Boolean> woke = time.run(Duration.ofMillis(10), Duration.ofSeconds(1),
                () -> signal.await(before, time.monoNanos() + Duration.ofSeconds(30).toNanos()));
        assertThat(woke.value()).isTrue();
        assertThat(woke.took()).as("woken by the change at 40 ms, not by the 30 s deadline").isEqualTo(Duration.ofMillis(40));

        long quiet = signal.seen();
        VirtualTime.Outcome<Boolean> timedOut = time.run(Duration.ofSeconds(1), Duration.ofMinutes(1),
                () -> signal.await(quiet, time.monoNanos() + Waits.BACKSTOP.toNanos()));
        assertThat(timedOut.value()).isFalse();
        assertThat(timedOut.took()).isEqualTo(Waits.BACKSTOP);
    }

    @Test
    void aRenewerRunsOncePerPeriodAfterTheLastRunAndStopsOnCloseOrWhenItsBodySaysSo() {
        VirtualTime time = VirtualTime.create();
        List<Long> at = new CopyOnWriteArrayList<>();
        long start = time.monoNanos();
        Periodic renewer = Periodic.every(time, Duration.ofSeconds(15), "renew", () -> {
            at.add((time.monoNanos() - start) / 1_000_000L);
            return true;
        });
        time.advance(Duration.ofSeconds(14));
        assertThat(at).isEmpty();
        time.advance(Duration.ofSeconds(31));
        assertThat(at).containsExactly(15_000L, 30_000L, 45_000L);
        renewer.close();
        time.advance(Duration.ofMinutes(2));
        assertThat(at).hasSize(3);
        assertThat(renewer.runs()).isEqualTo(3);
        assertThat(time.pending()).as("nothing left armed").isZero();

        AtomicInteger runs = new AtomicInteger();
        Periodic twice = Periodic.every(time, Duration.ofSeconds(1), "twice", () -> runs.incrementAndGet() < 2);
        time.advance(Duration.ofSeconds(10));
        assertThat(runs).hasValue(2);
        assertThat(twice.closed()).isTrue();
    }

    /**
     * A renewer whose body throws an {@code Error} stops, as one that throws a runtime exception does: it reads closed,
     * and nothing is left armed (srad/agentspaces-event-loop.md §10.17 carried, phase 6). It used to stay open with
     * nothing armed, a renewer that reads alive and never runs again.
     */
    @Test
    void aRenewerWhoseBodyThrowsAnErrorReadsClosed() {
        VirtualTime time = VirtualTime.create();
        Periodic renewer = Periodic.every(time, Duration.ofSeconds(1), "fatal", () -> {
            throw new AssertionError("an Error from the body");
        });
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> time.advance(Duration.ofSeconds(2)))
                .isInstanceOf(AssertionError.class);
        assertThat(renewer.closed()).as("an Error stops it like any throw").isTrue();
        assertThat(time.pending()).isZero();
    }
}
