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
package ai.badmonkey.agentspaces.agent.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Describes an agent class for the fleet: the human- and LLM-readable identity
 * that becomes its AgentCard (spec §6.1). The Embabel adapter maps
 * {@code @Agent} metadata onto this; plain applications annotate directly.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface AgentSpec {

    /** The agent's local name; defaults to the decapitalized class name. */
    String name() default "";

    /** What this agent does, for humans and semantic discovery. */
    String description() default "";

    /** Goals the agent can pursue. */
    String[] goals() default {};
}
