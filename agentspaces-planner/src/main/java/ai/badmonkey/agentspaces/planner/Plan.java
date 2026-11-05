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

import java.util.List;

/**
 * A planner's answer: the steps that reach the goal, or none with the reason the planner found (the missing
 * type or situation, the constraint that forbids every route, the budget).
 *
 * @param goal    the term the plan reaches
 * @param steps   the steps in order; empty when stuck
 * @param planner the planner's name
 * @param reason  why the plan is empty, or how it was found
 */
public record Plan(String goal, List<PlannedStep> steps, String planner, String reason) {

    public Plan {
        steps = steps == null ? List.of() : List.copyOf(steps);
        reason = reason == null ? "" : reason;
    }

    public boolean stuck() {
        return steps.isEmpty();
    }

    public static Plan stuck(String goal, String planner, String reason) {
        return new Plan(goal, List.of(), planner, reason);
    }
}
