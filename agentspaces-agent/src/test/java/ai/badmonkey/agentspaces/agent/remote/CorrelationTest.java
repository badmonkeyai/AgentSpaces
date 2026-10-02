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
package ai.badmonkey.agentspaces.agent.remote;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The default shared-field correlation and its escape hatch (spec §10.6). */
class CorrelationTest {

    record Task(String topic, int priority) {
    }

    record Finding(String topic, String summary) {
    }

    record Unrelated(String subject) {
    }

    /** SPEC §10.6: record components with the same name must hold equal values. */
    @Test
    void sharedFieldsMustBeEqual() {
        Correlation shared = Correlation.sharedFields();
        assertThat(shared.matches(new Task("memory", 3), new Finding("memory", "ok"))).isTrue();
        assertThat(shared.matches(new Task("memory", 3), new Finding("robotics", "ok"))).isFalse();
    }

    /** SPEC §10.6: types sharing no component names, or non-records, fall back to any(). */
    @Test
    void typesSharingNothingCorrelateWithAnything() {
        Correlation shared = Correlation.sharedFields();
        assertThat(shared.matches(new Task("memory", 3), new Unrelated("x"))).isTrue();
        assertThat(shared.matches("not a record", new Finding("memory", "ok"))).isTrue();
        assertThat(shared.matches(new Task("memory", 3), "not a record")).isTrue();
    }

    /** SPEC §10.6: the correlation is replaceable; any() accepts the first result observed. */
    @Test
    void anyAcceptsEverything() {
        assertThat(Correlation.any().matches(new Task("a", 1), new Finding("b", "c"))).isTrue();
    }
}
