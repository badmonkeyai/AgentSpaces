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

/**
 * One unit of a published version (ISSUE-Constraints FR-2, FR-4): a template over the types {@code a} and
 * {@code b} (wire or simple names) narrowed by guards (field filter clauses), with {@code n} for cardinality,
 * {@code within} (an ISO duration) for response, the {@code key} ({@code field:<name>} or {@code tag:<name>})
 * that groups entries, and the enforcement list ({@code gate}, {@code monitor}, {@code jury}, {@code approval},
 * {@code planner}). Reserved entry type {@code ...constraints.Constraint#v1}; tagged {@code charter},
 * {@code charterVersion}, and {@code unit}.
 */
public record Constraint(int v, String book, long version, String unit, String template, String a,
                         List<String> aGuard, String b, List<String> bGuard, int n, String within, String key,
                         List<String> enforcement, Map<String, Object> jury) {

    public static final int VERSION = 1;
    public static final String PRECEDENCE = "precedence";
    public static final String NOT_COEXISTENCE = "notCoexistence";
    public static final String RESPONSE = "response";
    public static final String CARDINALITY = "cardinality";
    public static final List<String> TEMPLATES = List.of(PRECEDENCE, NOT_COEXISTENCE, RESPONSE, CARDINALITY);
    public static final String GATE = "gate";
    public static final String MONITOR = "monitor";
    public static final String JURY = "jury";

    public Constraint {
        aGuard = aGuard == null ? List.of() : List.copyOf(aGuard);
        bGuard = bGuard == null ? List.of() : List.copyOf(bGuard);
        enforcement = enforcement == null ? List.of(GATE, MONITOR) : List.copyOf(enforcement);
        jury = jury == null ? Map.of() : Map.copyOf(jury);
        b = b == null ? "" : b;
        within = within == null ? "" : within;
        key = key == null ? "" : key;
    }

    public Map<String, String> tags(String hash) {
        return Map.of(RuleTags.CHARTER, book, RuleTags.CHARTER_VERSION, hash, RuleTags.UNIT, RuleTags.unit(unit, hash));
    }

    public boolean gated() {
        return enforcement.contains(GATE) || enforcement.contains(JURY);
    }

    public boolean juried() {
        return enforcement.contains(JURY);
    }
}
