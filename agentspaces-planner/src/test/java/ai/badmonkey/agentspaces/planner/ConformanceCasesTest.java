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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ISSUE-CardContract FR-6: the conformance cases under {@code src/test/resources/cases}, one JSON file each,
 * name a goal, a state, the actions with contracts, the constraints, a budget, and what each planner answers.
 * Another planner adapter runs the same cases.
 */
class ConformanceCasesTest {

    static final ObjectMapper JSON = new ObjectMapper();

    @TestFactory
    Stream<DynamicTest> everyCaseHolds() throws IOException {
        List<Path> files;
        try (Stream<Path> s = Files.list(Path.of("src/test/resources/cases"))) {
            files = s.filter(p -> p.toString().endsWith(".json")).sorted().toList();
        }
        assertThat(files).hasSizeGreaterThanOrEqualTo(9);
        return files.stream().map(f -> DynamicTest.dynamicTest(f.getFileName().toString(), () -> run(JSON.readTree(f.toFile()))));
    }

    static void run(JsonNode c) {
        PlanningState state = PlanningState.of(strings(c.path("state").path("types")), strings(c.path("state").path("situations")));
        List<PlanAction> actions = new ArrayList<>();
        for (JsonNode a : c.path("actions")) {
            actions.add(new PlanAction(a.path("name").asText(), a.path("card").asText(), strings(a.path("consumes")), strings(a.path("produces")),
                    strings(a.path("pre")), strings(a.path("post")), a.path("cost").asText(""), a.path("space").asText("")));
        }
        List<Constraint> constraints = new ArrayList<>();
        for (JsonNode k : c.path("constraints")) {
            constraints.add(new Constraint(1, "rb", 1, k.path("unit").asText(), k.path("template").asText(), k.path("a").asText(), List.of(),
                    k.path("b").asText(""), List.of(), k.path("n").asInt(0), "", "field:key", List.of("gate"), java.util.Map.of()));
        }
        PlanBudget budget = c.has("budget") ? new PlanBudget(c.path("budget").path("depth").asInt(8), c.path("budget").path("width").asInt(10_000)) : PlanBudget.DEFAULT;
        JsonNode expect = c.path("expect");
        check(new EnablementPlanner(), c, state, actions, constraints, budget, expect.path("enablement"), expect.path("enablementReason"));
        check(new SearchPlanner(), c, state, actions, constraints, budget, expect.path("search"), expect.path("searchReason"));
    }

    static void check(Planner planner, JsonNode c, PlanningState state, List<PlanAction> actions, List<Constraint> constraints, PlanBudget budget,
                      JsonNode expectedSteps, JsonNode expectedReason) {
        Plan plan = planner.plan(c.path("goal").asText(), state, actions, constraints, budget);
        assertThat(plan.steps()).as(c.path("name").asText() + " (" + planner.name() + "): " + plan.reason())
                .extracting(PlannedStep::action).containsExactlyElementsOf(strings(expectedSteps));
        assertThat(plan.planner()).isEqualTo(planner.name());
        if (!expectedReason.isMissingNode()) {
            assertThat(plan.reason()).as(planner.name()).contains(expectedReason.asText());
        }
        for (int i = 0; i < plan.steps().size(); i++) {
            assertThat(plan.steps().get(i).step()).isEqualTo(i + 1);
        }
        if (!plan.stuck()) {
            Plan again = planner.replan(plan, 1, state, actions, constraints, budget);
            assertThat(again.steps()).extracting(PlannedStep::action).containsExactlyElementsOf(strings(expectedSteps));
            assertThat(again.reason()).contains("replanned from step 1");
        }
    }

    static List<String> strings(JsonNode n) {
        List<String> out = new ArrayList<>();
        for (JsonNode x : n) {
            out.add(x.asText());
        }
        return out;
    }
}
