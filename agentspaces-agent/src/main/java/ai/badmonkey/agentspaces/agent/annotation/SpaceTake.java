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
 * The replicated-worker idiom as one annotation (plan §10.3): the method's
 * parameter type becomes a take template on the named space, the binder performs
 * the leased take, invokes the method, and on normal return completes the take,
 * writing a non-null result back as an entry. An exception or a crash simply
 * lets the TAKE lease lapse, and the task reappears for another worker.
 *
 * <p>The method takes exactly one parameter (the entry) and may return a result
 * entry or {@code void}. Durations accept the Spring-style simple form
 * ({@code "10m"}, {@code "500ms"}) as well as ISO-8601 ({@code "PT10M"}).
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface SpaceTake {

    /**
     * The space to take from; empty means the binder's sole registered space
     * (an error when several are registered).
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

    /** The TAKE lease; renew via long-running work patterns. */
    String lease() default "10m";

    /** How long each poll waits for a matching entry. */
    String pollTimeout() default "1s";

    /** The write lease for a returned result entry. */
    String resultLease() default "1h";

    /**
     * The space the result entry is written into; empty means the same space the
     * task was taken from (atomically with the completion).
     */
    String resultSpace() default "";

    /**
     * What this action does, published as the action's description on the
     * agent's card (SPEC §6.1, v0.1.13) for planners, semantic discovery, and
     * A2A skills; empty means the card's agent description stands in.
     */
    String description() default "";
}
