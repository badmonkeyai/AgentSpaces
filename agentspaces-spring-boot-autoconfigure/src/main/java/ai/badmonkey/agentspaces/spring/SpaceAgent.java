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

import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import org.springframework.stereotype.Component;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The one-annotation fleet agent, Spring-style: a stereotype that is both a
 * {@code @Component} (so component scanning registers the bean) and an
 * {@link AgentSpec} (so the starter's post-processor enrolls it in the fleet,
 * starting its worker loops and publishing its AgentCard). Drop it on a POJO
 * and move on:
 *
 * <pre>{@code
 * @SpaceAgent(description = "Researches topics from the shared task space")
 * public class Researcher {
 *
 *     @SpaceTake                                  // sole space: inferred
 *     public Finding research(ResearchTask task) {
 *         return new Finding(task.topic(), summarize(task));
 *     }
 * }
 * }</pre>
 *
 * <p>The binder reads {@code name}, {@code description}, and {@code goals}
 * from any annotation meta-annotated with {@code @AgentSpec}, so applications
 * can compose their own stereotypes the same way.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
@AgentSpec
public @interface SpaceAgent {

    /**
     * The agent's local name; defaults to the decapitalized class name.
     *
     * @return the agent name
     */
    String name() default "";

    /**
     * What this agent does, for humans and semantic discovery.
     *
     * @return the description
     */
    String description() default "";

    /**
     * Goals the agent can pursue.
     *
     * @return the goals
     */
    String[] goals() default {};
}
