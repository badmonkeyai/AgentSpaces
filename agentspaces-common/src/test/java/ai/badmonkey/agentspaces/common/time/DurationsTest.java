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

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The one duration parser, per its contract: the simple form in each unit, ISO-8601, bare
 * milliseconds only when the caller asks for them, and a refusal that names both spellings.
 */
class DurationsTest {

    @Test
    void eachSimpleUnitParses() {
        assertThat(Durations.parse("500ms", false)).isEqualTo(Duration.ofMillis(500));
        assertThat(Durations.parse("30s", false)).isEqualTo(Duration.ofSeconds(30));
        assertThat(Durations.parse("10m", false)).isEqualTo(Duration.ofMinutes(10));
        assertThat(Durations.parse("2h", false)).isEqualTo(Duration.ofHours(2));
        assertThat(Durations.parse("7d", false)).isEqualTo(Duration.ofDays(7));
        assertThat(Durations.parse("0s", false)).isEqualTo(Duration.ZERO);
    }

    @Test
    void theSimpleFormIgnoresCaseSurroundingSpaceAndAPlusSign() {
        assertThat(Durations.parse("2H", false)).isEqualTo(Duration.ofHours(2));
        assertThat(Durations.parse("500MS", false)).isEqualTo(Duration.ofMillis(500));
        assertThat(Durations.parse("  10m\t", false)).isEqualTo(Duration.ofMinutes(10));
        assertThat(Durations.parse("10 m", false)).isEqualTo(Duration.ofMinutes(10));
        assertThat(Durations.parse("+30s", false)).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void msIsMillisecondsNotMinutes() {
        // "m" and "ms" share a prefix; the longer unit must win.
        assertThat(Durations.parse("5ms", false)).isEqualTo(Duration.ofMillis(5));
        assertThat(Durations.parse("5m", false)).isEqualTo(Duration.ofMinutes(5));
    }

    @Test
    void isoEightSixOOneParsesInEitherCase() {
        assertThat(Durations.parse("PT10M", false)).isEqualTo(Duration.ofMinutes(10));
        assertThat(Durations.parse("PT0.2S", false)).isEqualTo(Duration.ofMillis(200));
        assertThat(Durations.parse("P1DT2H", false)).isEqualTo(Duration.ofHours(26));
        assertThat(Durations.parse("pt10m", false)).isEqualTo(Duration.ofMinutes(10));
        assertThat(Durations.parse("-PT5S", false)).isEqualTo(Duration.ofSeconds(-5));
    }

    @Test
    void aBareNumberIsMillisecondsOnlyWhenTheCallerSaysSo() {
        assertThat(Durations.parse("250", true)).isEqualTo(Duration.ofMillis(250));
        assertThat(Durations.parse("+250", true)).isEqualTo(Duration.ofMillis(250));
        assertThat(Durations.parse("2h", true)).isEqualTo(Duration.ofHours(2));
        assertThatThrownBy(() -> Durations.parse("250", false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'250'");
    }

    @Test
    void nonsenseIsRefusedWithBothSpellingsNamed() {
        for (String value : new String[]{"soon", "10w", "ms", "1.5s", "-5s", "10m30s"}) {
            assertThatThrownBy(() -> Durations.parse(value, true)).as(value)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("10m")
                    .hasMessageContaining("ISO-8601")
                    .hasMessageContaining("'" + value + "'");
        }
    }

    @Test
    void emptyIsRefusedAndNullIsAnError() {
        assertThatThrownBy(() -> Durations.parse("", false))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("empty");
        assertThatThrownBy(() -> Durations.parse("   ", true))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("empty");
        assertThatThrownBy(() -> Durations.parse(null, false)).isInstanceOf(NullPointerException.class);
    }
}
