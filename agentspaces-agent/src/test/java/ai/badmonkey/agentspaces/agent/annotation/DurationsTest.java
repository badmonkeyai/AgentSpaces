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
package ai.badmonkey.agentspaces.agent.annotation;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Both duration spellings parse; nonsense fails with a helpful message. */
class DurationsTest {

    @Test
    void simpleStyleAndIsoBothParse() {
        assertThat(Durations.parse("10m")).isEqualTo(Duration.ofMinutes(10));
        assertThat(Durations.parse("500ms")).isEqualTo(Duration.ofMillis(500));
        assertThat(Durations.parse("30s")).isEqualTo(Duration.ofSeconds(30));
        assertThat(Durations.parse("2h")).isEqualTo(Duration.ofHours(2));
        assertThat(Durations.parse("1d")).isEqualTo(Duration.ofDays(1));
        assertThat(Durations.parse("2H")).isEqualTo(Duration.ofHours(2));
        assertThat(Durations.parse(" 10m ")).isEqualTo(Duration.ofMinutes(10));
        assertThat(Durations.parse("PT10M")).isEqualTo(Duration.ofMinutes(10));
        assertThat(Durations.parse("PT0.2S")).isEqualTo(Duration.ofMillis(200));
    }

    @Test
    void nonsenseFailsWithBothSpellingsNamed() {
        assertThatThrownBy(() -> Durations.parse("soon"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("10m")
                .hasMessageContaining("ISO-8601");
    }
}
