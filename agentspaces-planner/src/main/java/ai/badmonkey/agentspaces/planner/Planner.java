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
 * The planner SPI (ISSUE-CardContract, strategy B2): pure over a goal term, a key's state, the cards' actions
 * with contracts, the constraints in force, and a budget. {@link EnablementPlanner} answers with the next enabled
 * action whose effects reach the goal; {@link SearchPlanner} searches forward over contracts to the budget's
 * depth. Adapters for other planners (Embabel's GOAP, a model proposer under a checker, PDDL) implement this
 * interface and nothing else.
 */
public interface Planner {

    /** The planner's name, written on the plan. */
    String name();

    /**
     * A plan from the state to the goal.
     *
     * @param goal        the term to reach: a type's simple name or a situation
     * @param state       what holds for the key now
     * @param actions     the actions the fleet offers
     * @param constraints the constraints in force; a plan that would violate one is not returned
     * @param budget      the depth and width
     * @return the plan, empty with a reason when none is found
     */
    Plan plan(String goal, PlanningState state, List<PlanAction> actions, List<Constraint> constraints, PlanBudget budget);

    /**
     * A new plan from a step that failed: the steps before it are kept as done; the state is what holds now.
     *
     * @param previous the plan being replaced
     * @param fromStep the step that failed (from one)
     * @param state    what holds for the key now
     */
    default Plan replan(Plan previous, int fromStep, PlanningState state, List<PlanAction> actions, List<Constraint> constraints, PlanBudget budget) {
        Plan next = plan(previous.goal(), state, actions, constraints, budget);
        return new Plan(next.goal(), next.steps(), name(), next.stuck() ? next.reason() : "replanned from step " + fromStep + ": " + next.reason());
    }
}
