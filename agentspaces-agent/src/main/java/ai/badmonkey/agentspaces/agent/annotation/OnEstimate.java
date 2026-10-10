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
 * {@link #tolerance()} (relative) for {@link #settleTicks()} consecutive
 * protocol ticks; {@code settleTicks = 0} fires on the first estimate present.
 * The capability evaluates the rule on its own tick, so no thread polls.
 *
 * <pre>{@code
 * @OnEstimate(prefix = "reserve:", settleTicks = 8)
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

    /** Consecutive ticks the estimate must stay within {@link #tolerance()} to count as settled. */
    int settleTicks() default 8;

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
