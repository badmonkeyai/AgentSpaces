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
 * The exactly-once worker as one annotation (SPEC §7.4 ORDERED, §8, §10.3):
 * {@link SpaceTake}'s contract — take, act, complete-or-lapse — with the take
 * routed through the space's ordered-log coordinator instead of the space's own
 * claim race, so the log's commit order decides every take identically on every
 * member and each entry is completed at most once, fleet-wide. The binder owns
 * the loop the coordinator asks of a caller: a take lost to a leader election is
 * simply resubmitted on the next poll.
 *
 * <pre>{@code
 * @OrderedTake(space = "payments", lease = "30s")
 * public PaymentReceipt confirm(PaymentOrder order) { ... }   // completed with the receipt, once
 * }</pre>
 *
 * <p>The space must have a coordinator registered ({@code group.ordered(space,
 * coordinator)} or {@code binder.ordered(...)}), whose holder is the space
 * handle's writer; binding fails fast otherwise, naming the wiring. An attested
 * (subordinate-keyed) agent takes and completes as itself: its coordinator must
 * be built over the agent's own identity ({@code OrderedTakes.over(..., agentIdentity,
 * ...)}), and completions are signed with the agent's key.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface OrderedTake {
    /** The coordinated space; empty means the binder's sole coordinated space. */
    String space() default "";

    /** The group this binding belongs to; empty means any group whose spaces satisfy it. */
    String group() default "";

    /** The TAKE lease; the action must complete within it or the entry reappears. */
    String lease() default "30s";

    /** How long one poll of the coordinator waits for an entry before looping. */
    String pollTimeout() default "2s";

    /** The write lease for a returned result. */
    String resultLease() default "1h";

    /** The space a returned result is written into; empty means the take space. */
    String resultSpace() default "";

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
}
