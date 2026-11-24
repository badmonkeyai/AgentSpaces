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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The wall-step signal and the node views of {@link VirtualTime} (srad/agentspaces-event-loop.md TS-5, TS-6): a step
 * is told at the next advance, a node's own step only to that node, and a suspended node's clock and tasks stop while
 * the fleet's time moves on. The class timeout is a hang guard only.
 */
@Timeout(30)
class VirtualTimeWallStepTest {

    @Test
    void aWallStepIsToldAtTheNextAdvanceAndASmallOneIsNot() {
        VirtualTime time = VirtualTime.create();
        List<Duration> told = new CopyOnWriteArrayList<>();
        time.onWallStep(told::add);
        long mono = time.monoNanos();
        time.stepWall(Duration.ofMinutes(-5));
        assertThat(told).as("nothing runs between advances").isEmpty();
        time.runDue();
        assertThat(told).containsExactly(Duration.ofMinutes(-5));
        time.stepWall(Duration.ofMillis(900)); // a slew's worth: not a step
        time.advance(Duration.ofSeconds(1));
        assertThat(told).hasSize(1);
        assertThat(time.monoNanos() - mono).as("a step moves no monotonic time").isEqualTo(1_000_000_000L);
        assertThat(time.wallSteps()).isEqualTo(1);
    }

    @Test
    void aNodesOwnStepIsToldOnlyToItAndAFleetStepToEveryNode() {
        VirtualTime time = VirtualTime.create();
        VirtualTime.NodeTime a = time.node();
        VirtualTime.NodeTime b = time.offset(Duration.ofSeconds(3));
        List<String> told = new CopyOnWriteArrayList<>();
        a.onWallStep(step -> told.add("a " + step.toSeconds()));
        b.onWallStep(step -> told.add("b " + step.toSeconds()));
        assertThat(b.millis() - a.millis()).isEqualTo(3_000);
        a.stepWall(Duration.ofMinutes(4));
        time.runDue();
        assertThat(told).containsExactly("a 240");
        assertThat(a.millis() - time.millis()).isEqualTo(240_000);
        assertThat(a.monoNanos()).isEqualTo(time.monoNanos());
        time.stepWall(Duration.ofSeconds(-2));
        time.runDue();
        assertThat(told).containsExactlyInAnyOrder("a 240", "a -2", "b -2");
    }

    @Test
    void aSuspendedNodesClockAndTasksStopAndItsResumeRaisesTheStep() {
        VirtualTime time = VirtualTime.create();
        VirtualTime.NodeTime node = time.unwatchedNode();
        List<String> ran = new CopyOnWriteArrayList<>();
        List<Duration> steps = new CopyOnWriteArrayList<>();
        node.onWallStep(steps::add);
        long start = node.monoNanos();
        node.schedule(Duration.ofSeconds(1), () -> ran.add("1s at " + (node.monoNanos() - start) / 1_000_000));
        node.schedule(Duration.ofSeconds(5), () -> ran.add("5s at " + (node.monoNanos() - start) / 1_000_000));
        time.schedule(Duration.ofSeconds(3), () -> ran.add("fleet at 3s"));
        time.advance(Duration.ofMillis(500));
        node.suspend();
        long wallAtSuspend = node.millis();
        time.advance(Duration.ofHours(2));
        assertThat(ran).as("the fleet moves on; the node's task waits").containsExactly("fleet at 3s");
        assertThat(node.monoNanos() - start).isEqualTo(500_000_000L);
        assertThat(node.millis() - wallAtSuspend).as("its wall clock moves on").isEqualTo(7_200_000);
        assertThat(node.resume()).isEqualTo(Duration.ofHours(2));
        time.runDue();
        assertThat(steps).containsExactly(Duration.ofHours(2));
        assertThat(ran).as("the node's clock stopped at 500 ms, so its 1 s task is half a second away")
                .containsExactly("fleet at 3s");
        time.advance(Duration.ofMillis(500));
        assertThat(ran).containsExactly("fleet at 3s", "1s at 1000");
        time.advance(Duration.ofSeconds(4));
        assertThat(ran).containsExactly("fleet at 3s", "1s at 1000", "5s at 5000");
    }

    @Test
    void aFrozenNodesClockCountsOnAndItsOverdueTasksRunOnceAtTheThaw() {
        VirtualTime time = VirtualTime.create();
        VirtualTime.NodeTime node = time.unwatchedNode();
        List<String> ran = new CopyOnWriteArrayList<>();
        List<Duration> steps = new CopyOnWriteArrayList<>();
        node.onWallStep(steps::add);
        long start = node.monoNanos();
        node.schedule(Duration.ofSeconds(1), () -> ran.add("1s at " + (node.monoNanos() - start) / 1_000_000));
        node.schedule(Duration.ofSeconds(3), () -> ran.add("3s at " + (node.monoNanos() - start) / 1_000_000));
        node.schedule(Duration.ofSeconds(9), () -> ran.add("9s at " + (node.monoNanos() - start) / 1_000_000));
        node.freeze();
        time.advance(Duration.ofSeconds(5));
        assertThat(ran).isEmpty();
        assertThat(node.monoNanos() - start).isEqualTo(5_000_000_000L);
        assertThat(node.resume()).isEqualTo(Duration.ofSeconds(5));
        time.runDue();
        assertThat(ran).containsExactly("1s at 5000", "3s at 5000");
        assertThat(steps).as("wall and monotonic time moved together").isEmpty();
        time.advance(Duration.ofSeconds(4));
        assertThat(ran).containsExactly("1s at 5000", "3s at 5000", "9s at 9000");
    }

    @Test
    void aCancelledTaskOfASuspendedNodeNeverRunsAndAWatchedNodeCannotBeSuspended() {
        VirtualTime time = VirtualTime.create();
        VirtualTime.NodeTime node = time.unwatchedNode();
        List<String> ran = new ArrayList<>();
        var handle = node.schedule(Duration.ofSeconds(1), () -> ran.add("ran"));
        node.suspend();
        time.advance(Duration.ofSeconds(5));
        assertThat(handle.cancel()).isTrue();
        node.resume();
        time.advance(Duration.ofSeconds(5));
        assertThat(ran).isEmpty();
        assertThatThrownBy(() -> time.node().suspend()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void aSleeperOfASuspendedNodeWakesOnlyAfterTheResume() throws Exception {
        VirtualTime time = VirtualTime.create();
        VirtualTime.NodeTime node = time.unwatchedNode();
        List<Long> woke = new CopyOnWriteArrayList<>();
        long start = node.monoNanos();
        time.schedule(Duration.ZERO, () -> {
            try {
                node.sleep(Duration.ofSeconds(2));
                woke.add((node.monoNanos() - start) / 1_000_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        time.runDue();
        time.advance(Duration.ofSeconds(1));
        node.suspend();
        time.advance(Duration.ofSeconds(30));
        assertThat(woke).isEmpty();
        node.resume();
        time.advance(Duration.ofSeconds(2));
        assertThat(woke).containsExactly(2_000L);
    }
}
