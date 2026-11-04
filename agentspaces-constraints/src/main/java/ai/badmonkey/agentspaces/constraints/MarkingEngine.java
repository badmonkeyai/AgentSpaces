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

import ai.badmonkey.agentspaces.api.space.FieldFilter;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The marking engine (ISSUE-Constraints FR-4, FR-6): pure and deterministic over one key's events in issue
 * order. Each template is a small state machine:
 * <ul>
 * <li>{@code precedence(A, B)}: B is enabled only after an A; a B before any A is a violation;</li>
 * <li>{@code notCoexistence(A, B)}: once A occurred B is refused and once B occurred A is refused; both is a
 * violation;</li>
 * <li>{@code response(A, B, within)}: after an A the unit is pending until a B; pending past {@code within} is a
 * violation; nothing is refused (the monitor reports an absence);</li>
 * <li>{@code cardinality(A, n)}: A is enabled while fewer than n occurred; the n+1th is a violation.</li>
 * </ul>
 * Guards narrow which entries count as A or B. The enabled set is what every constraint allows; a juried unit
 * puts its constrained type on the awaiting list unless a {@link JuryDecision} allows the key.
 */
public final class MarkingEngine {

    private MarkingEngine() {
    }

    public static Marking mark(Version version, String key, List<Event> events, Instant now) {
        return mark(version, key, events, now, List.of());
    }

    /** Marks with the jury decisions of the key (FR-11). */
    public static Marking mark(Version version, String key, List<Event> events, Instant now, List<JuryDecision> decisions) {
        List<Event> ordered = Event.ordered(events);
        Map<String, String> states = new LinkedHashMap<>();
        Set<String> allEnabled = new LinkedHashSet<>();
        Set<String> allRefused = new LinkedHashSet<>();
        Set<String> awaiting = new LinkedHashSet<>();
        List<Marking.Violation> violations = new ArrayList<>();
        Map<String, Boolean> conditions = ConditionGrammar.evaluateAll(version.vocabulary().conditions(), ordered, now);
        for (Constraint c : version.constraints()) {
            String a = Event.simpleName(c.a());
            String b = Event.simpleName(c.b());
            List<FieldFilter> aGuard = Guards.parse(c.aGuard());
            List<FieldFilter> bGuard = Guards.parse(c.bGuard());
            // a juried unit's template still marks (the monitor reads it) but the jury decides the enablement (FR-11)
            Set<String> enabled = c.juried() ? new LinkedHashSet<>() : allEnabled;
            Set<String> refused = c.juried() ? new LinkedHashSet<>() : allRefused;
            switch (c.template()) {
                case Constraint.PRECEDENCE -> {
                    boolean seenA = false;
                    String state = "waiting-for-" + a;
                    for (Event e : ordered) {
                        if (Guards.matches(e, a, aGuard)) {
                            seenA = true;
                            state = "satisfied";
                        } else if (Guards.matches(e, b, bGuard) && !seenA) {
                            violations.add(new Marking.Violation(c.unit(), c.template(), b + " before any " + a, e.entryId()));
                            state = "violated";
                        }
                    }
                    states.put(c.unit(), state);
                    enabled.add(a);
                    if (seenA) {
                        enabled.add(b);
                    } else {
                        refused.add(b);
                    }
                }
                case Constraint.NOT_COEXISTENCE -> {
                    boolean seenA = false;
                    boolean seenB = false;
                    for (Event e : ordered) {
                        if (Guards.matches(e, a, aGuard)) {
                            if (seenB) {
                                violations.add(new Marking.Violation(c.unit(), c.template(), a + " and " + b + " for one key", e.entryId()));
                            }
                            seenA = true;
                        } else if (Guards.matches(e, b, bGuard)) {
                            if (seenA) {
                                violations.add(new Marking.Violation(c.unit(), c.template(), b + " and " + a + " for one key", e.entryId()));
                            }
                            seenB = true;
                        }
                    }
                    states.put(c.unit(), seenA && seenB ? "violated" : seenA ? a + "-chosen" : seenB ? b + "-chosen" : "open");
                    if (seenA) {
                        refused.add(b);
                        enabled.add(a);
                    } else if (seenB) {
                        refused.add(a);
                        enabled.add(b);
                    } else {
                        enabled.add(a);
                        enabled.add(b);
                    }
                }
                case Constraint.RESPONSE -> {
                    Duration within = c.within().isBlank() ? null : Duration.parse(c.within());
                    Instant pendingSince = null;
                    String state = "idle";
                    for (Event e : ordered) {
                        if (Guards.matches(e, a, aGuard)) {
                            if (pendingSince == null) {
                                pendingSince = e.issued();
                            }
                            state = "pending";
                        } else if (Guards.matches(e, b, bGuard) && pendingSince != null) {
                            if (within != null && Duration.between(pendingSince, e.issued()).compareTo(within) > 0) {
                                violations.add(new Marking.Violation(c.unit(), c.template(), b + " after " + within + " from " + a, e.entryId()));
                            }
                            pendingSince = null;
                            state = "satisfied";
                        }
                    }
                    if (pendingSince != null && within != null && Duration.between(pendingSince, now).compareTo(within) > 0) {
                        violations.add(new Marking.Violation(c.unit(), c.template(), "no " + b + " within " + within + " of " + a, ""));
                        state = "overdue";
                    }
                    states.put(c.unit(), state);
                    enabled.add(a);
                    enabled.add(b);
                }
                case Constraint.CARDINALITY -> {
                    long count = 0;
                    for (Event e : ordered) {
                        if (Guards.matches(e, a, aGuard)) {
                            count++;
                            if (count > c.n()) {
                                violations.add(new Marking.Violation(c.unit(), c.template(), "more than " + c.n() + " " + a, e.entryId()));
                            }
                        }
                    }
                    states.put(c.unit(), count + " of " + c.n());
                    if (count < c.n()) {
                        enabled.add(a);
                    } else {
                        refused.add(a);
                    }
                }
                default -> states.put(c.unit(), "unknown template " + c.template());
            }
            if (c.juried()) {
                boolean allowed = decisions.stream().anyMatch(d -> d.unit().equals(c.unit()) && JuryDecision.ALLOW.equals(d.verdict()));
                if (allowed) {
                    allEnabled.add(a);
                    if (!b.isBlank()) {
                        allEnabled.add(b);
                    }
                } else {
                    awaiting.add(a);
                    if (!b.isBlank()) {
                        awaiting.add(b);
                    }
                }
            }
        }
        allEnabled.removeAll(allRefused);
        return new Marking(states, allEnabled, allRefused, awaiting, violations, conditions);
    }
}
