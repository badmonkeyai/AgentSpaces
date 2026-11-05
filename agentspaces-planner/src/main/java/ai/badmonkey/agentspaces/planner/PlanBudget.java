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

/**
 * How far a planner searches: the longest plan it returns and how many partial plans it keeps (SRAD §6.8
 * decision 5: eight steps by default, a per-goal override).
 *
 * @param depth the most steps in a plan
 * @param width the most partial plans kept while searching
 */
public record PlanBudget(int depth, int width) {

    public static final PlanBudget DEFAULT = new PlanBudget(8, 10_000);

    public PlanBudget {
        if (depth < 1 || width < 1) {
            throw new IllegalArgumentException("a budget needs a positive depth and width");
        }
    }
}
