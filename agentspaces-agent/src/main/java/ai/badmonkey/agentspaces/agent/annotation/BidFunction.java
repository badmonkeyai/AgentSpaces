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
 * The cost function for an {@code AUCTION} space (spec §7.4, plan §10.3): given
 * a candidate entry, return this agent's cost; the lowest bid across the fleet
 * wins the entry. The method takes exactly one parameter (the entry) and returns
 * {@code double}. Bid policies are plain functions, so they can be learned.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface BidFunction {

    /**
     * The AUCTION space this bid function prices; empty means the binder's
     * sole registered space (an error when several are registered).
     */
    String space() default "";

    /**
     * The group this binding belongs to, by the application-facing group name
     * or the GroupId value; empty means "any group whose spaces satisfy it".
     * A binder for a different group treats the method as not its own and
     * skips it, so one bean can bind methods into several groups by name
     * (spec §10.3), and the {@code AgentSpaces} facade's {@code bind(...)}
     * routes the bean to every group its annotations name.
     */
    String group() default "";
}
