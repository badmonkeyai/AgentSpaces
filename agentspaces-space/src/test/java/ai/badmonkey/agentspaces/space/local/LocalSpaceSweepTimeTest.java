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

import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.test.Fixtures;
import ai.badmonkey.agentspaces.test.Fixtures.TaskEntry;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * srad/agentspaces-event-loop.md EL-T-12 and EL-T-13: a {@code LocalSpace} built with {@code sweepEvery} sweeps on a
 * one-shot of its time source, re-armed from the end of each sweep, with no thread of its own. On {@link VirtualTime}
 * (taken from the clock) the sweep runs only when time is advanced, exactly once per interval, and close stops it.
 */
class LocalSpaceSweepTimeTest {

    @Test
    void theSweepIsOneOneShotPerIntervalOnTheClocksTimeSourceAndCloseStopsIt() {
        VirtualTime time = VirtualTime.create();
        LocalSpace space = LocalSpace.builder("swept", Fixtures.agent("sweeper"))
                .clock(time).sweepEvery(Duration.ofSeconds(1)).build();
        List<SpaceEvent.Kind> received = new CopyOnWriteArrayList<>();
        space.notify(Template.of(TaskEntry.class), e -> received.add(e.kind()), Lease.of(Duration.ofHours(1)));
        assertThat(time.pending()).as("one sweep armed, nothing at a fixed rate").isEqualTo(1);

        space.write(new TaskEntry("gossip", 3), Lease.of(Duration.ofSeconds(5)));
        time.advance(Duration.ofMillis(4_500));
        assertThat(received).containsExactly(SpaceEvent.Kind.WRITTEN);
        long before = time.tasksRun();
        time.advance(Duration.ofMillis(1_000)); // past the 5 s lease: the 5 s sweep finds it
        assertThat(received).containsExactly(SpaceEvent.Kind.WRITTEN, SpaceEvent.Kind.EXPIRED);
        assertThat(time.tasksRun() - before).as("the one sweep at 5 s").isEqualTo(1);

        // A minute of time: one sweep a second, re-armed from completion, never more than one waiting.
        before = time.tasksRun();
        for (int i = 0; i < 60; i++) {
            time.advance(Duration.ofSeconds(1));
            assertThat(time.pending()).isEqualTo(1);
        }
        assertThat(time.tasksRun() - before).isEqualTo(60);

        space.close();
        assertThat(time.pending()).as("close cancels the armed sweep").isZero();
        before = time.tasksRun();
        time.advance(Duration.ofMinutes(1));
        assertThat(time.tasksRun()).isEqualTo(before);
    }

    /**
     * srad/agentspaces-event-loop.md group W, EL-C-08 carried: a waiting take waits on its space's time source, so on
     * a virtual one it times out when an advance passes its deadline and returns as soon as a write lands, in no real
     * time; it used to wait in real time on the system's source, which an advance cannot release.
     */
    @Test
    void aWaitingTakeWaitsOnTheSpacesTimeSource() throws Exception {
        VirtualTime time = VirtualTime.create();
        LocalSpace space = LocalSpace.builder("waits", Fixtures.agent("taker")).clock(time).build();
        VirtualTime.Outcome<java.util.Optional<TakenEntry<TaskEntry>>> none = time.run(Duration.ofSeconds(1),
                Duration.ofMinutes(2), () -> space.take(Template.of(TaskEntry.class), Lease.of(Duration.ofMinutes(1)),
                        Duration.ofSeconds(60)));
        assertThat(none.value()).isEmpty();
        assertThat(none.took()).as("the timeout on the space's source").isBetween(Duration.ofSeconds(60),
                Duration.ofMillis(60_100));

        java.util.concurrent.CompletableFuture<VirtualTime.Outcome<java.util.Optional<TakenEntry<TaskEntry>>>> waiting =
                time.start(() -> space.take(Template.of(TaskEntry.class), Lease.of(Duration.ofMinutes(1)),
                        Duration.ofSeconds(60)));
        time.advance(Duration.ofSeconds(5));
        assertThat(waiting).isNotDone();
        space.write(new TaskEntry("late", 1), Lease.of(Duration.ofMinutes(1)));
        time.advance(Duration.ofMillis(50));
        // the write's notify wakes the waiter on the writer's thread, which an advance does not wait for: a hang guard
        VirtualTime.Outcome<java.util.Optional<TakenEntry<TaskEntry>>> got = waiting.get(10, TimeUnit.SECONDS);
        assertThat(got.value()).isPresent();
        assertThat(got.took()).as("woken by the write, not the deadline").isLessThan(Duration.ofSeconds(6));
        space.close();
    }

    /**
     * DW-2 for the local space (srad/agentspaces-event-loop.md, carried to phase 6): a {@code LocalSpace} keeps a change
     * counter and tells its change listeners, so due work that reads it wakes on a write instead of its backstop, and
     * {@code awaitChange} returns on the change, on the space's time source. It kept the API's defaults: no counter,
     * no listener ever called, and a wait that slept its whole span.
     */
    @Test
    void aLocalSpaceSignalsItsChangesToListenersAndWaiters() throws Exception {
        VirtualTime time = VirtualTime.create();
        LocalSpace space = LocalSpace.builder("signals", Fixtures.agent("signaller")).clock(time).build();
        java.util.concurrent.atomic.AtomicInteger heard = new java.util.concurrent.atomic.AtomicInteger();
        AutoCloseable handle = space.onChangeSignal(heard::incrementAndGet);
        long seen = space.changeSignals();
        space.write(new TaskEntry("one", 1), Lease.of(Duration.ofMinutes(1)));
        assertThat(heard.get()).as("the listener heard the write").isEqualTo(1);
        assertThat(space.changeSignals()).isGreaterThan(seen);

        long now = space.changeSignals();
        java.util.concurrent.CompletableFuture<VirtualTime.Outcome<Boolean>> waiting = time.start(() -> {
            space.awaitChange(now, Duration.ofSeconds(30));
            return true;
        });
        time.advance(Duration.ofSeconds(2));
        assertThat(waiting).isNotDone();
        space.write(new TaskEntry("two", 2), Lease.of(Duration.ofMinutes(1)));
        time.advance(Duration.ofMillis(1));
        assertThat(waiting.get(10, TimeUnit.SECONDS).took()).as("woken by the write, not the 30 s span")
                .isLessThan(Duration.ofSeconds(3));
        handle.close();
        space.write(new TaskEntry("three", 3), Lease.of(Duration.ofMinutes(1)));
        assertThat(heard.get()).as("a closed handle hears nothing more").isEqualTo(2);
        space.close();
    }
}
