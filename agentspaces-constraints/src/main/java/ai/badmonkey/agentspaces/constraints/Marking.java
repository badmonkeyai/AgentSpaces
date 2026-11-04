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

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The marking of one key under one version (§9.3): the state per unit, the types the constraints enable next,
 * the types they refuse, the units awaiting a jury, and the violations the events already show.
 */
public record Marking(Map<String, String> states, Set<String> enabled, Set<String> refused, Set<String> awaitingJury,
                      List<Violation> violations, Map<String, Boolean> conditions) {

    /** A unit's refusal or observed violation on the key. */
    public record Violation(String unit, String template, String reason, String entryId) {
    }

    public Marking {
        states = states == null ? Map.of() : Map.copyOf(states);
        enabled = enabled == null ? Set.of() : Set.copyOf(enabled);
        refused = refused == null ? Set.of() : Set.copyOf(refused);
        awaitingJury = awaitingJury == null ? Set.of() : Set.copyOf(awaitingJury);
        violations = violations == null ? List.of() : List.copyOf(violations);
        conditions = conditions == null ? Map.of() : Map.copyOf(conditions);
    }

    /** Whether an action on the type is allowed: not refused and not awaiting a jury. */
    public boolean allows(String type) {
        String simple = Event.simpleName(type);
        return !refused.contains(simple) && !awaitingJury.contains(simple);
    }

    /** The sentence-shaped reason a type is refused, or an empty string. */
    public String explain(String type) {
        String simple = Event.simpleName(type);
        StringBuilder b = new StringBuilder();
        for (Violation v : violations) {
            if (v.reason().contains(simple)) {
                if (b.length() > 0) {
                    b.append("; ");
                }
                b.append(v.unit()).append(": ").append(v.reason());
            }
        }
        if (awaitingJury.contains(simple)) {
            if (b.length() > 0) {
                b.append("; ");
            }
            b.append(simple).append(" awaits a jury decision");
        }
        return b.toString();
    }
}
