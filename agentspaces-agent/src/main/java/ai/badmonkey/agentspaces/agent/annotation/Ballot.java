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
 * Casts this agent's ballot whenever a proposal appears in a vote space (SPEC
 * §8 QUORUM, §10.3). The method takes the {@code VoteCapability.Proposal} and
 * returns the option it votes for, or {@code null} to abstain; the binder casts
 * the ballot through the group's vote capability, as this agent, once per
 * proposal. Because the cue <em>is</em> the proposal entry, the ballot can never
 * be refused as "unknown proposal": the two-entry race every hand-written
 * panelist had to defend against does not exist here.
 *
 * <pre>{@code
 * @Ballot(space = "votes", prefix = "safety:")
 * public String judge(VoteCapability.Proposal proposal) {
 *     return risky(proposal) ? "AVOID" : "GO";
 * }
 * }</pre>
 *
 * <p>Under a subordinate identity factory the ballot is the agent's own attested
 * record, so several agents on one peer are several voters wherever the
 * authorizer counts per agent.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Ballot {
    /** The vote space; empty means the binder's sole registered vote space. */
    String space() default "";

    /** The group this binding belongs to; empty means any group whose spaces satisfy it. */
    String group() default "";

    /** Only proposals whose id starts with this prefix; empty means every proposal. */
    String prefix() default "";

    /** The ballot's write lease; outlive the proposal's decision window. */
    String lease() default "1h";
}
