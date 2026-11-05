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
package ai.badmonkey.agentspaces.planner;

import ai.badmonkey.agentspaces.constraints.Constraint;

import java.util.ArrayList;
import java.util.List;

/** The constraint templates read as a plan filter: a plan that would violate one is pruned. */
final class Constraints {

    private Constraints() {
    }

    /** Null when the plan is admitted; else the unit that forbids it. */
    static String violated(PlanningState start, List<PlannedStep> steps, List<Constraint> constraints) {
        List<String> produced = new ArrayList<>();
        for (PlannedStep s : steps) {
            produced.addAll(s.produces());
        }
        for (Constraint c : constraints) {
            String a = PlanAction.simpleName(c.a());
            String b = c.b() == null ? "" : PlanAction.simpleName(c.b());
            boolean aHeld = start.has(a) || produced.contains(a);
            boolean bHeld = !b.isEmpty() && (start.has(b) || produced.contains(b));
            switch (c.template()) {
                case Constraint.NOT_COEXISTENCE -> {
                    if (aHeld && bHeld) {
                        return c.unit();
                    }
                }
                case Constraint.CARDINALITY -> {
                    long n = produced.stream().filter(a::equals).count() + (start.has(a) ? 1 : 0);
                    if (n > c.n()) {
                        return c.unit();
                    }
                }
                case Constraint.PRECEDENCE -> {
                    // b only after a: a plan that produces b must have a already, or produce it in an earlier step
                    if (!b.isEmpty() && !start.has(b)) {
                        int aAt = firstProducing(steps, a);
                        int bAt = firstProducing(steps, b);
                        if (bAt >= 0 && !start.has(a) && (aAt < 0 || aAt > bAt)) {
                            return c.unit();
                        }
                    }
                }
                default -> {
                    // response and the condition templates do not forbid a route
                }
            }
        }
        return null;
    }

    private static int firstProducing(List<PlannedStep> steps, String term) {
        for (int i = 0; i < steps.size(); i++) {
            if (steps.get(i).produces().contains(term)) {
                return i;
            }
        }
        return -1;
    }
}
