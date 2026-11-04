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

/** Reserved for the planner (a later spiral): one step of a plan toward a goal. Nothing writes it today. */
public record PlanStep(int v, String goal, String key, long version, int step, String action, String card,
                       Map<String, Object> inputs, List<Integer> dependsOn) {

    public static final int VERSION = 1;

    public PlanStep {
        inputs = inputs == null ? Map.of() : Map.copyOf(inputs);
        dependsOn = dependsOn == null ? List.of() : List.copyOf(dependsOn);
    }
}
