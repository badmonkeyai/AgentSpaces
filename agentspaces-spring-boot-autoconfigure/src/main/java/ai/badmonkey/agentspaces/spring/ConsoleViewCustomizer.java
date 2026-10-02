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
package ai.badmonkey.agentspaces.spring;

import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.console.ConsoleView;

/**
 * Refines the auto-configured console view. The starter's default watches
 * every configured space generically; a customizer bean is where an
 * application tells the console what its entries mean, in the Spring Boot
 * customizer idiom:
 *
 * <pre>{@code
 * @Bean
 * ConsoleViewCustomizer researchConsole() {
 *     return (builder, spaces) -> builder
 *             .results("tasks", Finding.class, f -> ((Finding) f).worker());
 * }
 * }</pre>
 *
 * <p>Every customizer bean in the context runs, in bean order, after the
 * default registration and before the view is built.
 */
@FunctionalInterface
public interface ConsoleViewCustomizer {

    /**
     * Adjusts the view under construction.
     *
     * @param builder the view builder, defaults already applied
     * @param spaces  the fabric facade, for navigating groups and spaces
     */
    void customize(ConsoleView.Builder builder, AgentSpaces spaces);
}
