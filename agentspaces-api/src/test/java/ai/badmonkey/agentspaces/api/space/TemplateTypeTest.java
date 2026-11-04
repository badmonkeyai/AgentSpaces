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


package ai.badmonkey.agentspaces.api.space;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** v0.1.24: the type-name condition is judged on the record's schema name, by wire or simple name. */
class TemplateTypeTest {

    @Test
    void matchesByWireOrSimpleName() {
        Template<Object> any = Template.of(Object.class);
        assertThat(any.typeName()).isNull();
        assertThat(any.matchesType("a.b.Claim#v1")).isTrue();
        Template<Object> claims = any.whereType("Claim");
        assertThat(claims.matchesType("a.b.Claim#v1")).isTrue();
        assertThat(claims.matchesType("a.b.Outer$Claim#v2")).isTrue();
        assertThat(claims.matchesType("a.b.Payment#v1")).isFalse();
        assertThat(claims.matchesType(null)).isFalse();
        assertThat(any.whereType("a.b.Claim#v1").matchesType("a.b.Claim#v1")).isTrue();
        assertThat(any.whereType("a.b.Claim#v1").matchesType("c.d.Claim#v1")).as("a wire name narrows to its own simple name").isTrue();
        assertThat(claims.whereTag("key", Matchers.eq("c-1")).typeName()).isEqualTo("Claim");
        assertThat(claims.toString()).contains("type=Claim");
    }
}
