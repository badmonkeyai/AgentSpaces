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
package ai.badmonkey.agentspaces.agent.capability;

import ai.badmonkey.agentspaces.api.space.Lease;
import java.util.List;
import java.util.Objects;

/**
 * A vote to open, returned from a bound method (SPEC §8 QUORUM, §10.3;
 * ISSUE-Motion): the vote-side sibling of {@link Contribution}. The binder
 * opens the proposal on the vote capability registered for {@link #space()}
 * (empty means the sole registered vote space), as the bound agent, once per
 * proposal id per replica: a motion whose proposal is already visible is
 * skipped. For a take, the taken entry is completed before the motion is
 * opened, so a failed open never makes a finished task reappear.
 *
 * <pre>{@code
 * @SpaceNotify(space = "ideas")
 * public Motion open(NextDestinationIdeas ideas) {
 *     return Motion.of("next:" + ideas.tripId(), "Where next after this trip?",
 *             ideas.names(), partySize, Lease.of(Duration.ofHours(12)));
 * }
 * }</pre>
 *
 * @param space      the vote space; empty means the sole registered one
 * @param proposalId the proposal's id, the key ballots and decisions carry
 * @param question   what is put to the vote
 * @param options    the options, two or more
 * @param quorum     the ballots that close the vote, positive
 * @param lease      the proposal's write lease; ballots must outlive it
 */
public record Motion(String space, String proposalId, String question, List<String> options,
                     int quorum, Lease lease) {

    public Motion {
        Objects.requireNonNull(proposalId, "proposalId");
        Objects.requireNonNull(question, "question");
        options = List.copyOf(Objects.requireNonNull(options, "options"));
        Objects.requireNonNull(lease, "lease");
        if (options.size() < 2) {
            throw new IllegalArgumentException("a motion needs at least two options");
        }
        if (quorum <= 0) {
            throw new IllegalArgumentException("quorum must be positive: " + quorum);
        }
        space = space == null ? "" : space;
    }

    /** A motion on the sole registered vote space. */
    public static Motion of(String proposalId, String question, List<String> options, int quorum,
                            Lease lease) {
        return new Motion("", proposalId, question, options, quorum, lease);
    }

    /** A motion on a named vote space. */
    public static Motion in(String space, String proposalId, String question, List<String> options,
                            int quorum, Lease lease) {
        return new Motion(Objects.requireNonNull(space, "space"), proposalId, question, options,
                quorum, lease);
    }
}
