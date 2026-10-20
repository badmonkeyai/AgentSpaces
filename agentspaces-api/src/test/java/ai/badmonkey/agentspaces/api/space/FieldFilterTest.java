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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The portable field filter grammar (SPEC §7.2, JS-V-03): parsing and the comparison rules. */
class FieldFilterTest {

    @Test
    void parsesEveryOperatorLongestFirst() {
        assertThat(FieldFilter.parse("priority>=3")).isEqualTo(new FieldFilter("priority", ">=", "3"));
        assertThat(FieldFilter.parse("priority <= 3")).isEqualTo(new FieldFilter("priority", "<=", "3"));
        assertThat(FieldFilter.parse("topic!=x")).isEqualTo(new FieldFilter("topic", "!=", "x"));
        assertThat(FieldFilter.parse("topic=a=b")).isEqualTo(new FieldFilter("topic", "=", "a=b"));
        assertThat(FieldFilter.parse("region in eu, us")).isEqualTo(new FieldFilter("region", "in", "eu, us"));
        assertThat(FieldFilter.parse("level<2")).isEqualTo(new FieldFilter("level", "<", "2"));
        assertThatThrownBy(() -> FieldFilter.parse("nonsense")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> FieldFilter.parse("=3")).isInstanceOf(IllegalArgumentException.class);
        // the earliest operator wins: "!=x" is malformed, not the field "!" compared with "x"
        for (String clause : new String[] {"!=x", "<x", " in a,b", "topic!", ""}) {
            assertThatThrownBy(() -> FieldFilter.parse(clause)).as(clause)
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("malformed field filter");
        }
        assertThat(FieldFilter.parse("a<b<=c")).isEqualTo(new FieldFilter("a", "<", "b<=c"));
        assertThat(FieldFilter.parse("topic=a in b")).isEqualTo(new FieldFilter("topic", "=", "a in b"));
        assertThatThrownBy(() -> new FieldFilter("f", "~", "x")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void comparesNumbersNumericallyAndOtherValuesAsStrings() {
        assertThat(FieldFilter.parse("priority>=3").matches(3)).isTrue();
        assertThat(FieldFilter.parse("priority>=3").matches(2.5)).isFalse();
        assertThat(FieldFilter.parse("priority=3").matches(3.0)).isTrue();
        assertThat(FieldFilter.parse("priority<10").matches(9)).isTrue();
        assertThat(FieldFilter.parse("priority<10").matches("9")).isTrue();
        assertThat(FieldFilter.parse("priority<10").matches("abc")).isFalse();
        assertThat(FieldFilter.parse("topic=index").matches("index")).isTrue();
        assertThat(FieldFilter.parse("topic!=index").matches("other")).isTrue();
        assertThat(FieldFilter.parse("topic!=index").matches(null)).isTrue();
        assertThat(FieldFilter.parse("topic=index").matches(null)).isFalse();
        assertThat(FieldFilter.parse("name<m").matches("alpha")).isTrue();
        assertThat(FieldFilter.parse("name>m").matches("alpha")).isFalse();
        assertThat(FieldFilter.parse("region in eu, us").matches("us")).isTrue();
        assertThat(FieldFilter.parse("region in eu, us").matches("ap")).isFalse();
        assertThat(FieldFilter.parse("n in 1,2,3").matches(2.0)).isTrue();
        assertThat(FieldFilter.parse("flag=true").matches(true)).isTrue();
        assertThat(FieldFilter.parse("priority>=3").matcher().matches(4)).isTrue();
    }
}
