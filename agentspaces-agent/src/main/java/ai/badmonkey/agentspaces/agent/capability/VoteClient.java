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

import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.capabilities.vote.VoteCapability;
import ai.badmonkey.agentspaces.common.id.AgentId;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The typed client for {@code aspace:cap/vote} (spec §8, §10.5): a thin
 * wrapper over a {@link VoteCapability} bound to this group's vote space, so
 * application code proposes and votes through
 * {@code spaces.group("g").capability(VoteClient.class)} and never touches the
 * capability wiring. Every operation is space-mediated: proposals, ballots, and
 * the recomputable tally are ordinary leased entries in the shared vote space.
 *
 * <p>Resolution, by the {@link Factory}: a {@code VoteCapability} registered
 * locally through {@code GroupContext.provide(...)} (or bound as a
 * {@code @ProvidesCapability} bean) backs the client directly; otherwise a
 * discovered {@code CapabilityAdvertisement} for the vote capability names the
 * space it is bound to ({@code space:<name>}), and the client is built over
 * this peer's replica of that space, which must have been registered with its
 * writer id ({@code group.space(name, space, writer)}) so ballots carry the
 * authenticated voter the tally checks.
 */
public final class VoteClient {

    private final VoteCapability vote;
    private final AgentSpaces.GroupContext group;

    VoteClient(VoteCapability vote, AgentSpaces.GroupContext group) {
        this.vote = Objects.requireNonNull(vote, "vote");
        this.group = Objects.requireNonNull(group, "group");
    }

    /**
     * Puts a proposal to the group.
     *
     * @param proposalId unique proposal identifier
     * @param question   the question
     * @param options    the allowed options; at least two
     * @param quorum     distinct voters needed to close; positive
     * @param lease      how long the proposal stays open
     * @return this client
     */
    public VoteClient propose(String proposalId, String question, List<String> options,
                              int quorum, Lease lease) {
        vote.propose(proposalId, question, options, quorum, lease);
        return this;
    }

    /**
     * Casts this peer's ballot.
     *
     * @param proposalId the proposal
     * @param option     the chosen option
     * @param lease      the ballot's lease
     * @return this client
     */
    public VoteClient castBallot(String proposalId, String option, Lease lease) {
        vote.castBallot(proposalId, option, lease);
        return this;
    }

    /**
     * Returns the proposal, when known to this replica.
     *
     * @param proposalId the proposal
     * @return the proposal
     */
    public Optional<VoteCapability.Proposal> proposal(String proposalId) {
        return vote.proposal(proposalId);
    }

    /**
     * Recomputes the tally from the space.
     *
     * @param proposalId the proposal
     * @return votes per option
     */
    public Map<String, Integer> tally(String proposalId) {
        return vote.tally(proposalId);
    }

    /**
     * Returns the decision once the quorum is reached.
     *
     * @param proposalId the proposal
     * @return the decision, or empty while the quorum is short
     */
    public Optional<VoteCapability.Decision> decision(String proposalId) {
        return vote.decision(proposalId);
    }

    /**
     * Returns the current, unexpired advertisements of the vote capability in
     * this group: who offers it and which space each is bound to.
     *
     * @return the advertisements
     */
    public List<CapabilityAdvertisement> providers() {
        return group.discovery().find(CapabilityAdvertisement.class,
                ad -> VoteCapability.TYPE.equals(ad.capabilityType()));
    }

    /** Returns the underlying capability, for operations this client does not wrap. */
    public VoteCapability capability() {
        return vote;
    }

    /** Resolves {@link VoteClient}s; registered through {@code ServiceLoader}. */
    public static final class Factory implements CapabilityClientFactory<VoteClient> {

        @Override
        public Class<VoteClient> clientType() {
            return VoteClient.class;
        }

        @Override
        public VoteClient create(AgentSpaces.GroupContext group) {
            Objects.requireNonNull(group, "group");
            Optional<VoteCapability> local = group.provider(VoteCapability.TYPE)
                    .filter(VoteCapability.class::isInstance)
                    .map(VoteCapability.class::cast);
            if (local.isPresent()) {
                return new VoteClient(local.get(), group);
            }
            List<CapabilityAdvertisement> ads = group.discovery().find(
                    CapabilityAdvertisement.class,
                    ad -> VoteCapability.TYPE.equals(ad.capabilityType()));
            for (CapabilityAdvertisement ad : ads) {
                Optional<String> spaceName = boundSpace(ad.binding());
                if (spaceName.isEmpty() || !group.spaceNames().contains(spaceName.get())) {
                    continue;
                }
                Optional<AgentId> writer = group.writer(spaceName.get());
                if (writer.isEmpty()) {
                    throw new IllegalStateException("group '" + group.name()
                            + "' registers the vote space '" + spaceName.get()
                            + "' without its writer id; register it as space(name, space,"
                            + " writer) so ballots carry the authenticated voter, or"
                            + " provide(new VoteCapability(...)) locally");
                }
                Space space = group.space(spaceName.get());
                return new VoteClient(new VoteCapability(space, writer.get(),
                        group.identity().peerId(), group.clock()), group);
            }
            throw new IllegalStateException("no vote capability resolves in group '"
                    + group.name() + "': no local VoteCapability provider and "
                    + (ads.isEmpty() ? "no advertisement of " + VoteCapability.TYPE
                    : "no advertised binding names a registered space (advertised: "
                    + ads.stream().map(CapabilityAdvertisement::binding).toList()
                    + ", registered: " + group.spaceNames() + ")"));
        }

        /** The space name of a {@code space:<name>} binding. */
        static Optional<String> boundSpace(String binding) {
            return binding != null && binding.startsWith("space:") && binding.length() > 6
                    ? Optional.of(binding.substring(6)) : Optional.empty();
        }
    }
}
