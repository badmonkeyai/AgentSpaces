/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.badmonkey.agentspaces.constraints;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The reserved entries a planner writes and the refusal a gated result raises (ISSUE-Constraints FR-1, FR-10). */
class ReservedEntriesTest {

    @Test
    void aPlanStepNormalisesItsCollections() {
        PlanStep step = new PlanStep(PlanStep.VERSION, "settle-claim", "claim-7", 3, 2, "pay", "Payer", null, null);
        assertThat(step.inputs()).isEmpty();
        assertThat(step.dependsOn()).isEmpty();
        PlanStep second = new PlanStep(PlanStep.VERSION, "settle-claim", "claim-7", 3, 3, "notify", "Notifier", Map.of("amount", 10), List.of(2));
        assertThat(second.dependsOn()).containsExactly(2);
        assertThatThrownBy(() -> second.inputs().put("x", 1)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void aRefusalNamesTheUnitTheKeyAndTheReason() {
        ConstraintRefusedException e = new ConstraintRefusedException("u1", "claim-7", "no HumanApproval before the PaymentOrder");
        assertThat(e.unit()).isEqualTo("u1");
        assertThat(e.key()).isEqualTo("claim-7");
        assertThat(e.getMessage()).isEqualTo("constraint u1 refuses the result for key claim-7: no HumanApproval before the PaymentOrder");
        assertThatThrownBy(() -> {
            throw e;
        }).isInstanceOf(ConstraintRefusedException.class).hasMessageContaining("u1");
    }
}
