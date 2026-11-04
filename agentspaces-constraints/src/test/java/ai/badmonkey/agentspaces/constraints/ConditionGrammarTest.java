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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** FR-5 over {@code constraints_grammar_cases.txt}: every predicate, the operators, and/or/not, malformed text refused. */
class ConditionGrammarTest {

    static final Instant NOW = Instant.parse("2026-11-04T12:00:00Z");
    static final List<Event> EVENTS = List.of(
            new Event("Claim", "acme.Claim#v1", "e1", NOW.minusSeconds(7200), Map.of("claimId", "c-1", "amount", 60000, "status", "open"), Map.of("claimId", "c-1"), Event.Kind.WRITTEN),
            new Event("Claim", "acme.Claim#v1", "e2", NOW.minusSeconds(60), Map.of("claimId", "c-1", "amount", 70000, "status", "closed"), Map.of("claimId", "c-1"), Event.Kind.WRITTEN));

    static String resource(String name) throws IOException {
        try (var in = Objects.requireNonNull(ConditionGrammarTest.class.getResourceAsStream("/" + name))) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Test
    void everyCaseEvaluatesAsRecorded() throws IOException {
        int n = 0;
        for (String line : resource("constraints_grammar_cases.txt").split("\n")) {
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            String[] parts = line.split("\\|");
            String text = parts[0].trim();
            String expected = parts[1].trim();
            n++;
            if (expected.equals("refused")) {
                assertThatThrownBy(() -> ConditionGrammar.parse(text)).as(text).isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining(text.substring(0, Math.min(6, text.length())));
            } else {
                assertThat(ConditionGrammar.parse(text).evaluate(EVENTS, NOW)).as(text).isEqualTo(Boolean.parseBoolean(expected));
            }
        }
        assertThat(n).isGreaterThanOrEqualTo(20);
        assertThat(ConditionGrammar.evaluateAll(Map.of("c1", "exists(Claim)", "bad", "exists("), EVENTS, NOW))
                .containsEntry("c1", true).containsEntry("bad", false);
    }
}
