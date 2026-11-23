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
 * Reacts once per push-sum epoch when this replica's estimate has settled
 * (SPEC §8 aggregate, §10.3; ISSUE-OnEstimate): the read side of the
 * {@link ai.badmonkey.agentspaces.agent.capability.Contribution} return. The
 * method takes the {@code PushSumAggregate.Estimate} and returns the next act
 * as any bound method does (an entry, a {@code Motion}, a {@code Contribution},
 * or {@code null}). An estimate has settled when it has changed by no more than
 * {@link #tolerance()} (relative) for {@link #settle()} (by default 8 s: eight
 * gossip rounds of about a second); {@code settle = "PT0S"} fires on the first
 * estimate present. The capability evaluates the rule after each round and at
 * the estimate's last move plus the span, so no thread polls
 * (srad/agentspaces-event-loop.md EL-T-06, EL-T-16).
 *
 * <pre>{@code
 * @OnEstimate(prefix = "reserve:", settle = "PT8S")
 * public ReserveReport report(PushSumAggregate.Estimate reserve) {
 *     return new ReserveReport(reserve.epochId(), reserve.value(), name);
 * }
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface OnEstimate {

    /** Only epochs whose id starts with this prefix; empty means every epoch. */
    String prefix() default "";

    /**
     * How long the estimate must stay within {@link #tolerance()} to count as settled: an ISO-8601 or simple-style
     * duration ({@code "PT8S"}, {@code "8s"}); empty means the default of 8 s, or {@link #settleTicks()} when given.
     * Giving both is refused at start-up (srad/agentspaces-event-loop.md EL-T-06).
     */
    String settle() default "";

    /**
     * Settle steps (gossip rounds of about a second) the estimate must stay within {@link #tolerance()}; -1 means not
     * given. Read as n × {@code ROUND_PERIOD} (1 s; owner answer 1, 2026-11-22).
     *
     * @deprecated give {@link #settle()} as a duration
     */
    @Deprecated(since = "0.7.0")
    int settleTicks() default -1;

    /** The relative change (of the larger of 1 and the last value) that still counts as unchanged. */
    double tolerance() default 0.001;

    /** The group this binding belongs to; empty means any group whose aggregate satisfies it. */
    String group() default "";

    /** The space a returned entry is written into; empty means the binder's sole registered space. */
    String resultSpace() default "";

    /** The write lease for a returned entry. */
    String resultLease() default "1h";

    /** Published as the action's description on the agent's card (SPEC §6.1). */
    String description() default "";
}
