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
package ai.badmonkey.agentspaces.it;

import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.console.ConsolePanel;
import ai.badmonkey.agentspaces.spring.ConsoleViewCustomizer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * The integration-test application: an ordinary Spring Boot app whose only
 * AgentSpaces knowledge is one starter dependency, some properties, and one
 * annotated bean. Everything else, identity, node, group join, spaces, worker
 * loop, and card publication, must arrive through the autoconfiguration
 * running under real Spring Boot.
 */
@SpringBootApplication
public class FleetItApp {

    /** The task entry the fleet works. */
    public record ItTask(String topic, int priority) {
    }

    /** The finding a worker produces. */
    public record ItFinding(String topic, String summary) {
    }

    /** The worker bean: the post-processor must enroll it automatically. */
    @Component
    @AgentSpec(name = "it-researcher", description = "Researches integration topics",
            goals = {"research"})
    public static class Researcher {

        /** Topics this instance worked, for test assertions. */
        public final CopyOnWriteArrayList<String> worked = new CopyOnWriteArrayList<>();

        /**
         * Takes a task and produces a finding.
         *
         * @param task the task
         * @return the finding
         */
        @SpaceTake(space = "tasks", lease = "PT10M", pollTimeout = "PT0.2S",
                resultSpace = "findings")
        public ItFinding research(ItTask task) {
            worked.add(task.topic());
            return new ItFinding(task.topic(), "done: " + task.topic());
        }
    }

    /**
     * Tells the console what an {@code ItFinding} means: worker attribution
     * for the served view. Collected by the console autoconfiguration only
     * when {@code agentspaces.console.enabled=true}; a plain bean otherwise.
     *
     * @return the customizer
     */
    @Bean
    public ConsoleViewCustomizer itConsoleCustomizer() {
        return (builder, spaces) -> builder.results("findings",
                ItFinding.class, finding -> "it-researcher");
    }

    /**
     * One application-defined console panel, proving panel beans are
     * collected under real Spring.
     *
     * @return the panel
     */
    @Bean
    public ConsolePanel itPanel() {
        return new ConsolePanel() {
            @Override
            public String id() {
                return "it-panel";
            }

            @Override
            public String title() {
                return "Integration";
            }

            @Override
            public Object data() {
                return Map.of("suite", "spring-boot-it");
            }
        };
    }

    /**
     * Boots the app.
     *
     * @param args command-line arguments
     */
    public static void main(String[] args) {
        SpringApplication.run(FleetItApp.class, args);
    }
}
