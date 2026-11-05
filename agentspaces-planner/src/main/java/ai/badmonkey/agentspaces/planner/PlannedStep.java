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
 * One step of a plan: which action, on which card, after which steps.
 *
 * @param step      the step's number, from one
 * @param action    the action's name
 * @param card      the card's agent name
 * @param consumes  the input the step needs (simple names)
 * @param produces  what the step brings (simple names)
 * @param space     the space the action takes its input in
 * @param dependsOn the steps that must finish first
 */
public record PlannedStep(int step, String action, String card, List<String> consumes, List<String> produces, String space, List<Integer> dependsOn) {

    public PlannedStep {
        consumes = consumes == null ? List.of() : List.copyOf(consumes);
        produces = produces == null ? List.of() : List.copyOf(produces);
        space = space == null ? "" : space;
        dependsOn = dependsOn == null ? List.of() : List.copyOf(dependsOn);
    }
}
