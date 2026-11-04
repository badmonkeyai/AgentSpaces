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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** FR-4, FR-6 over {@code constraints_marking_cases.txt}: each template, order and tie rules, determinism, guards, the jury read. */
class MarkingEngineTest {

    static final Instant NOW = Instant.parse("2026-11-04T12:00:00Z");

    static Event event(String spec, int i) {
        String[] tk = spec.split("@");
        String[] oc = tk[1].split(":");
        return new Event(tk[0], "acme." + tk[0] + "#v1", "e" + i, NOW.minusSeconds(400000).plusSeconds(Long.parseLong(oc[0])),
                Map.of("claimId", "c-1", "amount", 60000), Map.of("claimId", "c-1"),
                oc.length > 1 && oc[1].equals("claim") ? Event.Kind.CLAIMED : Event.Kind.WRITTEN);
    }

    @Test
    void everyTemplateMarksAsRecorded() throws Exception {
        int n = 0;
        for (String line : ConditionGrammarTest.resource("constraints_marking_cases.txt").split("\n")) {
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            String[] parts = line.split("\\|");
            String[] head = parts[0].trim().split("\\s+");
            Constraint c = new Constraint(1, "rb", 1, "u1", head[0], head[1], List.of(), head[2], List.of(), Integer.parseInt(head[3]),
                    head[4].equals("-") ? "" : head[4], "field:claimId", List.of("gate", "monitor"), Map.of());
            List<Event> events = new ArrayList<>();
            int i = 0;
            for (String spec : parts[1].trim().isEmpty() ? new String[0] : parts[1].trim().split(",")) {
                events.add(event(spec.trim(), i++));
            }
            Version v = new Version(new Vocabulary(1, "rb", 1, "h1", Map.of()), List.of(c));
            Marking m = MarkingEngine.mark(v, "c-1", events, NOW.minusSeconds(400000).plusSeconds(10));
            String expectedEnabled = parts[2].trim();
            String expectedRefused = parts[3].trim();
            assertThat(m.enabled()).as(line + " enabled").containsExactlyInAnyOrderElementsOf(expectedEnabled.isEmpty() ? List.of() : List.of(expectedEnabled.split(",")));
            assertThat(m.refused()).as(line + " refused").containsExactlyInAnyOrderElementsOf(expectedRefused.isEmpty() ? List.of() : List.of(expectedRefused.split(",")));
            assertThat(m.violations()).as(line + " violations").hasSize(Integer.parseInt(parts[4].trim()));
            // determinism: shuffled input gives the same marking
            List<Event> shuffled = new ArrayList<>(events);
            Collections.shuffle(shuffled, new java.util.Random(7));
            assertThat(MarkingEngine.mark(v, "c-1", shuffled, NOW.minusSeconds(400000).plusSeconds(10))).isEqualTo(m);
            n++;
        }
        assertThat(n).isEqualTo(15);
    }

    @Test
    void guardsNarrowAndAJuriedUnitAwaitsADecision() {
        Constraint guarded = new Constraint(1, "rb", 1, "k1", Constraint.PRECEDENCE, "HumanApproval", List.of(), "PaymentOrder",
                List.of("amount>50000"), 0, "", "field:claimId", List.of("gate"), Map.of());
        Version v = new Version(new Vocabulary(1, "rb", 1, "h", Map.of("big", "exists(PaymentOrder where amount > 50000)")), List.of(guarded));
        Event small = new Event("PaymentOrder", "acme.PaymentOrder#v1", "p1", NOW, Map.of("claimId", "c-1", "amount", 100), Map.of(), Event.Kind.WRITTEN);
        Marking m = MarkingEngine.mark(v, "c-1", List.of(small), NOW);
        assertThat(m.violations()).as("a small order is outside the guard").isEmpty();
        assertThat(m.conditions()).containsEntry("big", false);
        Event big = new Event("PaymentOrder", "acme.PaymentOrder#v1", "p2", NOW, Map.of("claimId", "c-1", "amount", 60000), Map.of(), Event.Kind.WRITTEN);
        assertThat(MarkingEngine.mark(v, "c-1", List.of(big), NOW).violations()).hasSize(1);
        Constraint juried = new Constraint(1, "rb", 1, "k2", Constraint.PRECEDENCE, "DamageCheck", List.of(), "PaymentOrder", List.of(), 0, "",
                "field:claimId", List.of("jury"), Map.of("quorum", 2));
        Version jv = new Version(new Vocabulary(1, "rb", 1, "h", Map.of()), List.of(juried));
        Event check = new Event("DamageCheck", "acme.DamageCheck#v1", "d1", NOW, Map.of("claimId", "c-1"), Map.of(), Event.Kind.WRITTEN);
        assertThat(MarkingEngine.mark(jv, "c-1", List.of(check), NOW).allows("PaymentOrder")).as("awaits a jury").isFalse();
        JuryDecision allow = new JuryDecision(1, "rb", 1, "k2", "c-1", "m-1", JuryDecision.ALLOW, List.of(), NOW);
        assertThat(MarkingEngine.mark(jv, "c-1", List.of(check), NOW, List.of(allow)).allows("PaymentOrder")).isTrue();
        assertThat(MarkingEngine.mark(jv, "c-1", List.of(check), NOW).explain("PaymentOrder")).contains("awaits a jury");
    }
}
