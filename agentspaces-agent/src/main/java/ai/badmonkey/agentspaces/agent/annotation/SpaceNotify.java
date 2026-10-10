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
 * The choreography sibling of {@link SpaceTake}: the method is invoked for
 * every matching entry written to the space, without consuming it, and a
 * non-void return value is written back as the next entry in the flow — react
 * to X, produce Y, and Y is some other agent's cue. The binder carries the
 * whole choreography discipline so the method body does not have to: delivery
 * is deduplicated per entry (at-least-once becomes effectively-once per bound
 * agent), the method runs on its own virtual thread so slow work never holds
 * the fabric's delivery thread, and a {@code null} return simply writes
 * nothing.
 *
 * <pre>{@code
 * @SpaceNotify
 * public Triage triage(VisitRequest request) {   // react to X…
 *     return new Triage(request.requestId(), acuityOf(request));  // …produce Y
 * }
 * }</pre>
 *
 * <p>The method takes exactly one parameter (the entry). Durations accept the
 * Spring-style simple form ({@code "10m"}) as well as ISO-8601.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface SpaceNotify {

    /**
     * The space to watch; empty means the binder's sole registered space (an
     * error when several are registered).
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

    /**
     * The subscription lease. The binder renews it at half-lease intervals for
     * as long as the agent stays bound, so the value bounds how long a stale
     * subscription can outlive a crashed agent, not how long reactions last.
     */
    String lease() default "1h";

    /**
     * The space a returned entry is written into; empty means the watched
     * space, matching {@link SpaceTake}'s own result-space default.
     */
    String resultSpace() default "";

    /** The write lease for a returned entry. */
    String resultLease() default "1h";

    /**
     * What this action does, published as the action's description on the
     * agent's card (SPEC §6.1, v0.1.13) for planners, semantic discovery, and
     * A2A skills; empty means the card's agent description stands in.
     */
    String description() default "";

    /**
     * Tag filters an entry must satisfy to be seen by this method, each
     * {@code "key=value"} (the tag equals the value) or {@code "key"} (the tag
     * is present), compiled onto the method's template so they are judged
     * before the payload is decoded (SPEC §7.2, issue #16).
     */
    String[] tags() default {};

    /**
     * Field filters an entry must satisfy to be seen by this method, each
     * {@code "field=value"} or {@code "field!=value"} compared on the field's
     * string form, compiled onto the method's template (ISSUE-WorkflowVerbs);
     * an unknown field is refused at bind time.
     */
    String[] where() default {};

    /**
     * What this method produces when its return type does not say: the entry
     * types inside an {@code Entries} fork or an {@code Object} return, declared
     * on the card (ISSUE-WorkflowVerbs). A sealed return type declares its
     * permitted subclasses without this.
     */
    Class<?>[] produces() default {};

    /**
     * The event kinds this method reacts to; {@code WRITTEN} by default.
     * {@code EXPIRED} turns a leased entry into a timer, {@code REAPPEARED}
     * into a dead-worker signal, {@code COMPLETED} into "done elsewhere"
     * (ISSUE-WorkflowVerbs). Each entry is delivered once per kind.
     */
    ai.badmonkey.agentspaces.api.space.SpaceEvent.Kind[] on() default {
            ai.badmonkey.agentspaces.api.space.SpaceEvent.Kind.WRITTEN};
}
