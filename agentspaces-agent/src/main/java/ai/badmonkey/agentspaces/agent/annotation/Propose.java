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
 * Opens a vote for each cue (SPEC §8 QUORUM, §10.3; ISSUE-Propose): the
 * annotation form of opening, for a lead whose options, quorum, and lease are
 * fixed and whose cue decides only the question. The method takes the cue and
 * returns the question as a {@code String}, or {@code null} to ask nothing; it
 * may return a {@link ai.badmonkey.agentspaces.agent.capability.Motion} when
 * one cue needs a shape of its own. The proposal id is {@link #prefix()} plus
 * the cue's key: the {@link #key()} fields joined by {@code :}, or the
 * {@link #keyTag()} tag. The binder subscribes to the cue as
 * {@link SpaceNotify} does, invokes once per proposal id per bound agent
 * (several cues may map to one id: every channel's assessment of one wave
 * opens one safety vote), opens once per replica, as the bound agent, and
 * declares the action on the card.
 *
 * <pre>{@code
 * @Propose(space = "evidence", vote = "reviews", prefix = "remediation:", key = "incidentId",
 *         options = {"approve", "reject"}, quorum = 3, lease = "12h")
 * public String open(RemediationProposal p) {
 *     return "Approve remediation for " + p.incidentId() + ": " + p.action();
 * }
 * }</pre>
 *
 * <p>A class may both propose and vote on the same space; attribution and the
 * authorizer decide what counts.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Propose {

    /** The cue's space; empty means the binder's sole registered space. */
    String space() default "";

    /** The group this binding belongs to; empty means any group whose spaces satisfy it. */
    String group() default "";

    /** The vote space; empty means the binder's sole registered vote space. */
    String vote() default "";

    /** The proposal id's prefix, which {@code @Ballot} and {@code @OnDecision} filter on. */
    String prefix() default "";

    /** The cue fields whose values, joined by {@code :}, complete the proposal id. */
    String[] key() default {};

    /** The cue tag that completes the proposal id instead of fields; exclusive with {@link #key()}. */
    String keyTag() default "";

    /** The options, two or more. */
    String[] options();

    /** The ballots that close the vote, positive. */
    int quorum();

    /** The proposal's write lease; ballots must outlive it. */
    String lease() default "1h";

    /** The cue subscription's lease, renewed at half-lease while the agent stays bound. */
    String subscriptionLease() default "1h";

    /** Tag filters on the cue, {@code "key=value"} or {@code "key"}, judged before decode. */
    String[] tags() default {};

    /** Field filters on the cue, {@code "field=value"} or {@code "field!=value"}. */
    String[] where() default {};

    /**
     * What this action does, published as the action's description on the
     * agent's card (SPEC §6.1); empty means the card's agent description stands in.
     */
    String description() default "";
}
