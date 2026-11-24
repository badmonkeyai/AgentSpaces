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

import ai.badmonkey.agentspaces.agent.annotation.OnEstimate;
import ai.badmonkey.agentspaces.capabilities.aggregate.PushSumAggregate;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code @OnEstimate(settle)} (srad/agentspaces-event-loop.md EL-T-06; owner answer 1, 2026-11-22): the duration form,
 * the 8 s default, the deprecated {@code settleTicks} read as rounds of {@code ROUND_PERIOD} (the same rule in time),
 * and the start-up refusal when both are given.
 */
class OnEstimateSettleTest {

    @SuppressWarnings("unused")
    static final class Agent {
        @OnEstimate(settle = "PT40S")
        public void duration(PushSumAggregate.Estimate e) {
        }

        @OnEstimate(settle = "12s", tolerance = 0.01)
        public void simple(PushSumAggregate.Estimate e) {
        }

        @OnEstimate
        public void byDefault(PushSumAggregate.Estimate e) {
        }

        @SuppressWarnings("deprecation")
        @OnEstimate(settleTicks = 40)
        public void ticks(PushSumAggregate.Estimate e) {
        }

        @SuppressWarnings("deprecation")
        @OnEstimate(settleTicks = 0)
        public void first(PushSumAggregate.Estimate e) {
        }

        @SuppressWarnings("deprecation")
        @OnEstimate(settle = "PT8S", settleTicks = 8)
        public void both(PushSumAggregate.Estimate e) {
        }

        @OnEstimate(settle = "soon")
        public void garbled(PushSumAggregate.Estimate e) {
        }
    }

    private static PushSumAggregate.Settle settle(String name) throws Exception {
        Method m = Agent.class.getMethod(name, PushSumAggregate.Estimate.class);
        return AgentBinder.settleOf(m.getAnnotation(OnEstimate.class), m);
    }

    @Test
    void theDurationFormTheDefaultAndTheDeprecatedCountAreOneRuleInTime() throws Exception {
        assertThat(settle("duration").ticks()).as("40 s is 40 rounds of 1 s").isEqualTo(40);
        assertThat(settle("simple")).isEqualTo(new PushSumAggregate.Settle(12, 0.01));
        assertThat(settle("byDefault")).as("8 s, today's default of 8 rounds").isEqualTo(new PushSumAggregate.Settle(8, 0.001));
        assertThat(settle("ticks")).as("the deprecated count, read as rounds: equal to settle = PT40S")
                .isEqualTo(settle("duration"));
        assertThat(settle("first").ticks()).isZero();
        assertThat(PushSumAggregate.Settle.after(Duration.ofMillis(1_500), 0.0).ticks()).as("rounded up").isEqualTo(2);
    }

    @Test
    void bothFormsOrAGarbledSpanAreRefusedAtStartUpNamingTheMethod() {
        assertThatThrownBy(() -> settle("both")).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("both settle and settleTicks").hasMessageContaining("both(");
        assertThatThrownBy(() -> settle("garbled")).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("garbled(");
    }
}
