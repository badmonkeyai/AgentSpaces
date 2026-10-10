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
 * The persistent accumulator (SPEC §10.3, ISSUE-SpaceReduce): a fold over an
 * unbounded stream of elements per key, whose state is an entry in the
 * elements' space. The method takes the current accumulator ({@code null} on
 * a key's first step) and one element and returns the new accumulator; the
 * binder takes the element under a lease, invokes the method, and completes
 * the element with the new accumulator as the result, in one atomic step, so
 * an element is never folded twice. A {@link ai.badmonkey.agentspaces.agent.join.JoinTicket}
 * per key serializes the steps of one key, so no update is lost, and the
 * accumulator's lease is the key's idle timeout: every step rewrites it, a
 * quiet key lapses, and the lapse is an {@code EXPIRED} event.
 *
 * <pre>{@code
 * @SpaceReduce(space = "payments", key = "claimant", accumulatorLease = "24h")
 * public ClaimantExposure expose(ClaimantExposure exposure, Payment payment) {
 *     long paid = (exposure == null ? 0 : exposure.paidCents()) + payment.cents();
 *     return new ClaimantExposure(payment.claimant(), paid);
 * }
 * }</pre>
 *
 * <p>The return is the accumulator, a {@link ai.badmonkey.agentspaces.agent.Tagged}
 * accumulator, or an {@link ai.badmonkey.agentspaces.agent.Entries} fork holding
 * exactly one accumulator and any side outputs, which are written after it;
 * {@code null} consumes the element and leaves the accumulator as it was. The
 * accumulator carries the tags {@code reduce}, {@code key}, and {@code steps},
 * and {@link ai.badmonkey.agentspaces.agent.reduce.Reductions#current} reads a
 * key's newest one. {@link Mode#LEASED} takes the key's ticket through the
 * space's claim race; {@link Mode#ORDERED} takes it, the elements, and the
 * retired accumulators through the space's ordered-log coordinator, so two
 * reducers never fold one key at once.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface SpaceReduce {

    /** How a key's steps are serialized across the fleet. */
    enum Mode {
        /** The key's ticket is taken under a lease through the space: a duplicate drain needs a partition. */
        LEASED,
        /** The ticket and every take go through the ordered log: one reducer per key, fleet-wide. */
        ORDERED
    }

    /** The space the elements, the accumulators, and the tickets live in; empty means the sole registered space. */
    String space() default "";

    /** The group this binding belongs to; empty means any group whose spaces satisfy it. */
    String group() default "";

    /** The name the tickets and the accumulator tags carry; empty means {@code <agent>.<method>}. */
    String name() default "";

    /** The element's field carrying the key; empty with an empty {@link #keyTag()} folds every element into one accumulator. */
    String key() default "";

    /** Or the element's tag carrying the key; exclusive with {@link #key()}. */
    String keyTag() default "";

    /** Tag filters on the element, {@code "key=value"} or {@code "key"}, as on {@code @SpaceTake}. */
    String[] tags() default {};

    /** Field filters on the element, {@code "field=value"} or {@code "field!=value"}, as on {@code @SpaceTake}. */
    String[] where() default {};

    /** How the steps of one key are serialized. */
    Mode mode() default Mode.LEASED;

    /** The take lease on an element and on the key's ticket. */
    String lease() default "30s";

    /** How long a take waits for an element, and how often untaken elements are swept. */
    String pollTimeout() default "1s";

    /** The accumulator's and the ticket's write lease: the key's idle timeout. */
    String accumulatorLease() default "24h";

    /** The element subscription's lease, renewed at half-lease while bound. */
    String subscriptionLease() default "1h";

    /** The bound on remembered keys. */
    int maxOpen() default 10_000;

    /** The side-output types of an {@link ai.badmonkey.agentspaces.agent.Entries} return, for the card. */
    Class<?>[] produces() default {};

    /** The card action's description. */
    String description() default "";
}
