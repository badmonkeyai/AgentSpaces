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

import java.util.List;

/**
 * The enablement planner (strategy B1 read as a planner): one step, the enabled action whose effects reach the
 * goal; stuck otherwise, naming what is missing. The cheapest planner, and the right one when the fleet's cards
 * already cover the goal in one action.
 */
public final class EnablementPlanner implements Planner {

    public static final String NAME = "enablement";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Plan plan(String goal, PlanningState state, List<PlanAction> actions, List<Constraint> constraints, PlanBudget budget) {
        String g = PlanAction.simpleName(goal);
        if (state.has(g)) {
            return new Plan(g, List.of(), NAME, "the goal already holds");
        }
        PlanAction reaching = null;
        List<PlanAction> enabled = new java.util.ArrayList<>();
        for (PlanAction a : actions) {
            if (!a.effects().contains(g)) {
                continue;
            }
            reaching = reaching == null ? a : reaching;
            if (a.applicable(state)) {
                enabled.add(a);
            }
        }
        if (reaching == null) {
            return Plan.stuck(g, NAME, "no action produces " + g);
        }
        // the cheapest enabled action first, then by name, so the answer is reproducible
        enabled.sort((x, y) -> {
            int c = Double.compare(SearchPlanner.costOf(x), SearchPlanner.costOf(y));
            return c != 0 ? c : x.name().compareTo(y.name());
        });
        String forbidden = null;
        for (PlanAction a : enabled) {
            PlannedStep step = new PlannedStep(1, a.name(), a.card(), a.consumes(), List.copyOf(a.effects()), a.space(), List.of());
            String unit = Constraints.violated(state, List.of(step), constraints);
            if (unit == null) {
                return new Plan(g, List.of(step), NAME, "one enabled action reaches the goal");
            }
            forbidden = "constraint " + unit + " forbids " + a.name();
        }
        if (forbidden != null) {
            return Plan.stuck(g, NAME, forbidden);
        }
        return Plan.stuck(g, NAME, "no enabled action produces " + g + "; " + reaching.name() + " needs " + missing(reaching, state));
    }

    static String missing(PlanAction a, PlanningState state) {
        StringBuilder b = new StringBuilder();
        for (String t : a.consumes()) {
            if (!state.has(t)) {
                b.append(b.isEmpty() ? "" : ", ").append(t);
            }
        }
        for (String t : a.pre()) {
            if (!state.has(t)) {
                b.append(b.isEmpty() ? "" : ", ").append(t);
            }
        }
        return b.isEmpty() ? "nothing" : b.toString();
    }
}
