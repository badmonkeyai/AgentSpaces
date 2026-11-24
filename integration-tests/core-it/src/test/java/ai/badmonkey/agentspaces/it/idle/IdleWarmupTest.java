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
package ai.badmonkey.agentspaces.it.idle;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The warm-up rule of {@link IdleCostIT} (EL-Q-PRF-01, EL-Q-PRF-02; owner decision 2026-11-23): the compile count read
 * from {@code jstat -compiler}, the rates a minute, and the end of the warm-up. Pure functions; no JVM is forked.
 */
class IdleWarmupTest {

    @Test
    void theCompiledColumnIsReadFromJstatsOutput() {
        String out = "Compiled Failed Invalid   Time   FailedType FailedMethod\n    1207      0       0     0.29          0\n";
        assertThat(IdleCostIT.compiledCount(out)).isEqualTo(1207);
        assertThat(IdleCostIT.compiledCount("12345 not found\n")).isEqualTo(-1);
        assertThat(IdleCostIT.compiledCount("")).isEqualTo(-1);
    }

    @Test
    void ratesAreAMinutesCompilationsAndLeaveOutAnUnreadMember() {
        Map<String, Long> before = new LinkedHashMap<>(Map.of("m0", 1000L, "m1", 2000L, "m2", -1L));
        Map<String, Long> after = new LinkedHashMap<>(Map.of("m0", 1010L, "m1", 2003L, "m2", 50L));
        Map<String, Double> perMin = IdleCostIT.compileRates(before, after, 2.0);
        assertThat(perMin).containsOnly(Map.entry("m0", 5.0), Map.entry("m1", 1.5));
    }

    @Test
    void theWarmUpEndsOnlyWhenEveryMemberIsReadAndQuiet() {
        assertThat(IdleCostIT.quiet(Map.of("m0", 5.0, "m1", 0.0), 2, 5)).isTrue();
        assertThat(IdleCostIT.quiet(Map.of("m0", 5.5, "m1", 0.0), 2, 5)).isFalse();
        assertThat(IdleCostIT.quiet(Map.of("m0", 0.0), 2, 5)).as("a member unread").isFalse();
    }
}
