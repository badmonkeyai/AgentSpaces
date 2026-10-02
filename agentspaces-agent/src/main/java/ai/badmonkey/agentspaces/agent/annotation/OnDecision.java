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
 * Reacts once when a vote closes (SPEC §8 QUORUM, §10.3). The method takes the
 * {@code VoteCapability.Decision} and is invoked exactly once per proposal, the
 * first time this replica's tally meets the quorum; a non-null return value is
 * written as the next entry in the flow, like {@link SpaceNotify}. Underneath,
 * the binder watches ballots and proposals in the vote space and recomputes the
 * decision, so a replica that learns of a vote late still fires once.
 *
 * <pre>{@code
 * @OnDecision(space = "votes", prefix = "safety:", resultSpace = "trip")
 * public TravelAdvisory publish(VoteCapability.Decision decision) {
 *     return advisoryFrom(decision);   // decision.granularity() says how it was counted
 * }
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface OnDecision {
    /** The vote space; empty means the binder's sole registered vote space. */
    String space() default "";

    /** The group this binding belongs to; empty means any group whose spaces satisfy it. */
    String group() default "";

    /** Only proposals whose id starts with this prefix; empty means every proposal. */
    String prefix() default "";

    /** The subscription lease; renewed at half-lease while the agent stays bound. */
    String lease() default "1h";

    /** The space a returned entry is written into; empty means the vote space. */
    String resultSpace() default "";

    /** The write lease for a returned entry. */
    String resultLease() default "1h";
}
