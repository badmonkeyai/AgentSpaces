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
package ai.badmonkey.agentspaces.spring;

import ai.badmonkey.agentspaces.peering.node.NodeLoop;
import org.junit.jupiter.api.Test;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.LivenessState;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Liveness turns BROKEN once when the watchdog escalates the node's loop, and only then (EL-H-08, decision 4 i). */
class LoopLivenessTest {

    private final List<Object> published = new ArrayList<>();
    private final LoopLiveness liveness = new LoopLiveness(published::add);

    @Test
    void degradedLeavesLivenessAloneAndDownBreaksItOnce() {
        liveness.onHealth(new NodeLoop.Health(NodeLoop.HealthState.DEGRADED, List.of("g/x backed off")));
        liveness.onHealth(new NodeLoop.Health(NodeLoop.HealthState.OK, List.of()));
        assertThat(published).isEmpty();
        liveness.onHealth(new NodeLoop.Health(NodeLoop.HealthState.DOWN, List.of("3 re-initialisations")));
        liveness.onHealth(new NodeLoop.Health(NodeLoop.HealthState.DOWN, List.of("3 re-initialisations")));
        assertThat(published).singleElement().satisfies(e -> assertThat(((AvailabilityChangeEvent<?>) e).getState())
                .isEqualTo(LivenessState.BROKEN));
        assertThat(liveness.broken()).isTrue();
    }
}
