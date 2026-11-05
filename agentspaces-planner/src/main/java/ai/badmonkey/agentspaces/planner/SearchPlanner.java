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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The bounded search planner (strategy B2's JDK-only adapter): a breadth-first forward search over the actions'
 * contracts from the key's state, the shortest admitted plan first, pruned by the constraints and bounded by
 * the budget's depth and width. Among plans of one length the cheaper (by cost hint, then name) comes first,
 * so a plan is reproducible for one fleet and one state.
 */
public final class SearchPlanner implements Planner {

    public static final String NAME = "search";

    @Override
    public String name() {
        return NAME;
    }

    private record Node(PlanningState state, List<PlannedStep> steps) {
    }

    @Override
    public Plan plan(String goal, PlanningState state, List<PlanAction> actions, List<Constraint> constraints, PlanBudget budget) {
        String g = PlanAction.simpleName(goal);
        if (state.has(g)) {
            return new Plan(g, List.of(), NAME, "the goal already holds");
        }
        if (actions.stream().noneMatch(a -> a.effects().contains(g))) {
            return Plan.stuck(g, NAME, "no action produces " + g);
        }
        List<PlanAction> ordered = new ArrayList<>(actions);
        ordered.sort((x, y) -> {
            int c = Double.compare(costOf(x), costOf(y));
            return c != 0 ? c : x.name().compareTo(y.name());
        });
        Deque<Node> frontier = new ArrayDeque<>();
        frontier.add(new Node(state, List.of()));
        Set<Set<String>> seen = new HashSet<>();
        seen.add(key(state));
        String forbidden = null;
        int kept = 1;
        while (!frontier.isEmpty()) {
            Node n = frontier.poll();
            if (n.steps().size() >= budget.depth()) {
                continue;
            }
            for (PlanAction a : ordered) {
                if (!a.applicable(n.state()) || n.state().holds(a.effects())) {
                    continue;   // not enabled, or brings nothing new
                }
                List<PlannedStep> steps = new ArrayList<>(n.steps());
                steps.add(new PlannedStep(steps.size() + 1, a.name(), a.card(), a.consumes(), List.copyOf(a.effects()), a.space(),
                        steps.isEmpty() ? List.of() : List.of(steps.size())));
                String unit = Constraints.violated(state, steps, constraints);
                if (unit != null) {
                    forbidden = unit;
                    continue;
                }
                PlanningState next = n.state().after(a);
                if (next.has(g)) {
                    return new Plan(g, steps, NAME, steps.size() + " step(s) by forward search");
                }
                Set<String> k = key(next);
                if (seen.add(k)) {
                    if (++kept > budget.width()) {
                        return Plan.stuck(g, NAME, "the search budget (" + budget.width() + " states) ran out before " + g);
                    }
                    frontier.add(new Node(next, steps));
                }
            }
        }
        if (forbidden != null) {
            return Plan.stuck(g, NAME, "every route to " + g + " within " + budget.depth() + " steps violates constraint " + forbidden);
        }
        return Plan.stuck(g, NAME, "no route to " + g + " within " + budget.depth() + " steps; " + nearest(g, actions, state));
    }

    private static Set<String> key(PlanningState s) {
        Set<String> k = new HashSet<>(s.types());
        for (String x : s.situations()) {
            k.add("@" + x);
        }
        return k;
    }

    private static String nearest(String goal, List<PlanAction> actions, PlanningState state) {
        for (PlanAction a : actions) {
            if (a.effects().contains(goal)) {
                return a.name() + " needs " + EnablementPlanner.missing(a, state);
            }
        }
        return "nothing produces it";
    }

    static double costOf(PlanAction a) {
        String c = a.cost();
        if (c.isBlank()) {
            return 1.0;
        }
        try {
            return Double.parseDouble(c);
        } catch (NumberFormatException e) {
            try {
                return java.time.Duration.parse(c).toMinutes();
            } catch (RuntimeException ignored) {
                return switch (c.toLowerCase(java.util.Locale.ROOT)) {
                    case "low" -> 0.5;
                    case "high" -> 2.0;
                    default -> 1.0;
                };
            }
        }
    }
}
