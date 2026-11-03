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
package ai.badmonkey.agentspaces.api.space;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Numbers match by value whatever their boxed class
 * (review-110226-deadcode-parity C6): a filter written with an int literal
 * matches a long or a double field, as the Clojure DSL already arranged by
 * coercing on its side.
 */
class MatchersNumericTest {

    @Test
    void comparisonsCrossBoxedClasses() {
        assertThat(Matchers.gte(2).matches(2L)).isTrue();
        assertThat(Matchers.gt(2).matches(2.5)).isTrue();
        assertThat(Matchers.lt(3).matches(2L)).isTrue();
        assertThat(Matchers.lte(new BigDecimal("2.0")).matches(2)).isTrue();
        assertThat(Matchers.gt(2).matches(1L)).isFalse();
        assertThat(Matchers.gte(2.5).matches(2)).isFalse();
    }

    @Test
    void equalityAndMembershipCrossBoxedClasses() {
        assertThat(Matchers.eq(2).matches(2L)).isTrue();
        assertThat(Matchers.eq(2).matches(2.0)).isTrue();
        assertThat(Matchers.ne(2).matches(2L)).isFalse();
        assertThat(Matchers.in(1, 2, 3).matches(2L)).isTrue();
        assertThat(Matchers.in(1, 2, 3).matches(4L)).isFalse();
        assertThat(Matchers.eq(2).matches("2")).as("a string is not a number").isFalse();
    }

    @Test
    void nonNumbersKeepTheirRules() {
        assertThat(Matchers.gt("b").matches("c")).isTrue();
        assertThat(Matchers.gt("b").matches(3)).as("a mismatched class never matches").isFalse();
        assertThat(Matchers.eq("x").matches("x")).isTrue();
        assertThat(Matchers.eq(null).matches(null)).isTrue();
    }
}
