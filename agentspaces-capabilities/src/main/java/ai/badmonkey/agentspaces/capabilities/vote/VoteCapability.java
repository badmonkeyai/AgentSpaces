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
package ai.badmonkey.agentspaces.capabilities.vote;

import ai.badmonkey.agentspaces.api.ad.AgentCard;
import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.api.spi.CapabilityProvider;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import java.util.function.Consumer;
import ai.badmonkey.agentspaces.api.space.Subscription;
import ai.badmonkey.agentspaces.capabilities.aggregate.PushSumAggregate;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;

import java.time.Duration;
import java.time.InstantSource;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;

/**
 * The {@code aspace:cap/vote} capability (spec §8): collective decisions over a
 * shared vote space. {@code QUORUM} mode is the canonical, auditable form:
 * proposals and ballots are ordinary leased entries in a replicated space, every
 * ballot is signed by its writer through the space machinery, one ballot counts
 * per voter, and the tally is recomputable by any member from the space itself.
 * {@code MAJORITY_GOSSIP} mode gives a cheap approximate signal by push-sum
 * preference averaging; use it for temperature checks and QUORUM for decisions.
 *
 * <p>Electorate (spec §8: "a configurable quorum of members with fresh
 * AgentCards"): an optional {@code Predicate<AgentId>} decides whose ballots
 * count. {@link #freshAgentCards(DiscoveryService)} is the spec's rule: a ballot
 * counts only while its authenticated issuer has an unexpired {@link AgentCard}
 * in the group's ad-cache, so the quorum is relative to the live electorate and
 * a decision recomputes to open again when the cards behind it lapse. The
 * default electorate is {@link #ANY_AUTHENTICATED_ISSUER}, the pre-electorate
 * behaviour for groups that do not publish cards.
 */
public final class VoteCapability implements CapabilityProvider {

    /** The capability type URI. */
    public static final String TYPE = "aspace:cap/vote";

    /**
     * A proposal put to the group.
     *
     * @param proposalId unique proposal identifier
     * @param question   the question being decided
     * @param options    the allowed options
     * @param quorum     how many distinct voters close the decision
     */
    public record Proposal(String proposalId, String question, List<String> options, int quorum) {
    }

    /**
     * One agent's ballot.
     *
     * @param proposalId the proposal voted on
     * @param option     the chosen option
     * @param voter      the voting agent's encoded id
     */
    public record Ballot(String proposalId, String option, String voter) {
    }

    /**
     * A closed decision.
     *
     * @param proposalId the proposal
     * @param winner     the winning option
     * @param tally      votes per option
     */
    public record Decision(String proposalId, String winner, Map<String, Integer> tally,
                           Authorizer.Granularity granularity) {

        /** A decision counted per peer, the rule every pre-phase-2 caller assumed. */
        public Decision(String proposalId, String winner, Map<String, Integer> tally) {
            this(proposalId, winner, tally, Authorizer.Granularity.PEER);
        }
    }

    /**
     * The default vote authorizer: the electorate decides alone, which is the
     * behaviour of every constructor that predates gate2-review G2-3.
     */
    private static final Authorizer PERMIT_ALL = (peer, operation, scope) -> true;

    /** The default electorate: every authenticated ballot issuer counts. */
    public static final Predicate<AgentId> ANY_AUTHENTICATED_ISSUER = agent -> true;

    private final Space voteSpace;
    private final AgentId self;
    private final PeerId selfPeer;
    private final InstantSource clock;
    private final Predicate<AgentId> electorate;
    private final Authorizer authorizer;
    /** Revoked voters, applied at every recount (v0.1.13, review M-13). */
    private volatile ai.badmonkey.agentspaces.api.security.RevocationView revocations;

    /**
     * Creates the capability over a vote space with the default electorate
     * (every authenticated issuer counts).
     *
     * @param voteSpace the shared space proposals and ballots live in
     * @param self      the local voting agent
     * @param selfPeer  the local peer (used in the advertisement)
     * @param clock     the time source for advertisement freshness
     */
    public VoteCapability(Space voteSpace, AgentId self, PeerId selfPeer, InstantSource clock) {
        this(voteSpace, self, selfPeer, clock, ANY_AUTHENTICATED_ISSUER);
    }

    /**
     * Creates the capability with an explicit electorate: only ballots whose
     * authenticated issuer passes the predicate count toward the tally and the
     * quorum. The predicate is evaluated at every tally, so a decision is a
     * function of the space and the electorate at the moment of reading.
     *
     * @param voteSpace  the shared space proposals and ballots live in
     * @param self       the local voting agent
     * @param selfPeer   the local peer (used in the advertisement)
     * @param clock      the time source for advertisement freshness
     * @param electorate who may vote; see {@link #freshAgentCards(DiscoveryService)}
     */
    public VoteCapability(Space voteSpace, AgentId self, PeerId selfPeer, InstantSource clock,
                          Predicate<AgentId> electorate) {
        this(voteSpace, self, selfPeer, clock, electorate, PERMIT_ALL);
    }

    /**
     * Creates the capability whose electorate is narrowed further by an
     * {@link Authorizer} (gate2-review G2-3): a ballot counts only when its
     * authenticated issuer passes the electorate <em>and</em>
     * {@code permits(issuer.peer(), VOTE, voteSpace.name())}, and this agent
     * refuses to cast a ballot the same check would discard. Under the
     * membership profiles the authorizer answers from the group's admitted
     * members, and under the identity-provider profiles it answers from the
     * {@code aspace:vote} scopes in each peer's token, so an organization can
     * name the electorate where it names its Raft voters and directive
     * issuers.
     *
     * <p>Like the electorate, the authorizer is consulted at every tally
     * against this replica's own view, so a decision is recomputable by any
     * member that shares that view. Both inputs converge through the same
     * gossip the ballots travel on.
     *
     * @param voteSpace  the shared space proposals and ballots live in
     * @param self       the local voting agent
     * @param selfPeer   the local peer (used in the advertisement)
     * @param clock      the time source for advertisement freshness
     * @param electorate who may vote; see {@link #freshAgentCards(DiscoveryService)}
     * @param authorizer decides {@code VOTE} in the vote space's scope
     */
    public VoteCapability(Space voteSpace, AgentId self, PeerId selfPeer, InstantSource clock,
                          Predicate<AgentId> electorate, Authorizer authorizer) {
        this.voteSpace = Objects.requireNonNull(voteSpace, "voteSpace");
        this.self = Objects.requireNonNull(self, "self");
        this.selfPeer = Objects.requireNonNull(selfPeer, "selfPeer");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.electorate = Objects.requireNonNull(electorate, "electorate");
        this.authorizer = Objects.requireNonNull(authorizer, "authorizer");
        this.revocations = ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace.revocationViewOf(voteSpace);
        // QA4 A4-4: a ballot names its voter, and the tally drops any ballot
        // whose declared voter is not the entry's authenticated writer. If this
        // capability could never write as `self`, every ballot it casts would be
        // discarded as forged -- at its own replica too -- and the quorum would
        // simply never close, with nothing logged. Refuse now, loudly, instead of
        // handing back an object that cannot vote.
        voteSpace.writer().ifPresent(writer -> {
            if (!writer.equals(self)) {
                throw new IllegalArgumentException(
                        "this capability would vote as '" + self.encoded() + "' but space '"
                                + voteSpace.name() + "' attributes its writes to '"
                                + writer.encoded() + "', so every ballot it cast would be"
                                + " dropped as forged; hand it a view of the space as that"
                                + " agent (ReplicatedSpace.as(identity)), give the voter its own"
                                + " space handle (AgentSpaces.GroupContext.space(name, space,"
                                + " writer)), or construct it with the space's own agent");
            }
        });
    }

    /**
     * Creates the capability whose electorate is the group's members with fresh
     * AgentCards (spec §8 QUORUM).
     *
     * @param voteSpace the shared space proposals and ballots live in
     * @param self      the local voting agent
     * @param selfPeer  the local peer (used in the advertisement)
     * @param clock     the time source for advertisement freshness
     * @param discovery the group's discovery service, whose ad-cache holds the cards
     */
    public VoteCapability(Space voteSpace, AgentId self, PeerId selfPeer, InstantSource clock,
                          DiscoveryService discovery) {
        this(voteSpace, self, selfPeer, clock, freshAgentCards(discovery));
    }

    /**
     * The spec §8 electorate rule: an agent may vote while an unexpired
     * {@link AgentCard} naming it is in the ad-cache. The cache evicts lapsed
     * cards on every lookup, so freshness is judged against the cache's clock
     * at tally time.
     *
     * @param discovery the group's discovery service
     * @return the electorate predicate
     */
    /**
     * The same capability voting as another agent of this peer, over that agent's
     * view of the vote space (QA4 A4-7 phase 3): electorate, authorizer, and clock
     * are shared; only who signs and who is named as voter differ.
     *
     * @param voteView the vote space as the agent ({@code space.as(identity)})
     * @param voter    the agent
     * @return the capability for that voter
     */
    public VoteCapability as(Space voteView, AgentId voter) {
        return new VoteCapability(voteView, voter, selfPeer, clock, electorate, authorizer);
    }

    /** The name of the vote space this capability votes in. */
    public String spaceName() {
        return voteSpace.name();
    }

    /** The agent this capability votes as. */
    public AgentId voter() {
        return self;
    }

    /**
     * Runs {@code listener} once per proposal, the first time this replica's
     * tally meets the proposal's quorum. Underneath, the vote space's ballot and
     * proposal writes are watched and the decision recomputed on each, so a
     * replica that learns of a vote late still fires exactly once, on its first
     * qualifying ballot. The returned subscription is leased like any other; renew
     * it for as long as the listener should keep running.
     *
     * @param proposals which proposal ids to watch (e.g. a prefix test)
     * @param listener  receives each decision once
     * @param lease     the subscription lease
     * @return the subscription; close it to stop
     */
    public Subscription onDecision(Predicate<String> proposals, Consumer<Decision> listener,
                                   Lease lease) {
        Objects.requireNonNull(proposals, "proposals");
        Objects.requireNonNull(listener, "listener");
        Objects.requireNonNull(lease, "lease");
        Set<String> decided = java.util.concurrent.ConcurrentHashMap.newKeySet();
        Consumer<String> check = proposalId -> {
            if (!proposals.test(proposalId) || decided.contains(proposalId)) {
                return;
            }
            decision(proposalId).ifPresent(decision -> {
                if (decided.add(proposalId)) {
                    listener.accept(decision);
                }
            });
        };
        Subscription ballots = voteSpace.notify(Template.of(Ballot.class), event -> {
            if (event.kind() == ai.badmonkey.agentspaces.api.space.SpaceEvent.Kind.WRITTEN) {
                check.accept(event.entry().proposalId());
            }
        }, lease);
        // A proposal can land after ballots already cast for it (two entries,
        // no ordering between them): its arrival is a cue too.
        Subscription proposalsSub = voteSpace.notify(Template.of(Proposal.class), event -> {
            if (event.kind() == ai.badmonkey.agentspaces.api.space.SpaceEvent.Kind.WRITTEN) {
                check.accept(event.entry().proposalId());
            }
        }, lease);
        return new Subscription() {
            @Override
            public void renew(java.time.Duration extension) {
                ballots.renew(extension);
                proposalsSub.renew(extension);
            }

            @Override
            public void close() {
                ballots.close();
                proposalsSub.close();
            }
        };
    }

    public static Predicate<AgentId> freshAgentCards(DiscoveryService discovery) {
        Objects.requireNonNull(discovery, "discovery");
        return agent -> !discovery.find(AgentCard.class, card -> agent.equals(card.agent()))
                .isEmpty();
    }

    @Override
    public String capabilityType() {
        return TYPE;
    }

    @Override
    public CapabilityAdvertisement describe(GroupId group) {
        return new CapabilityAdvertisement(
                // Scoped by space: a peer offering votes over two spaces publishes
                // two advertisements, not one that the second overwrites.
                "aspace://" + group.value() + "/cap/vote/" + selfPeer.value() + "/"
                        + voteSpace.name(),
                selfPeer, group, clock.instant(), Duration.ofMinutes(15),
                TYPE, "0.1", "space:" + voteSpace.name(),
                Map.of("modes", "QUORUM,MAJORITY_GOSSIP"), Map.of());
    }

    /**
     * Puts a proposal to the group.
     *
     * @param proposalId unique proposal identifier
     * @param question   the question
     * @param options    the allowed options; at least two
     * @param quorum     distinct voters needed to close; positive
     * @param lease      how long the proposal stays open
     */
    public void propose(String proposalId, String question, List<String> options,
                        int quorum, Lease lease) {
        if (options.size() < 2) {
            throw new IllegalArgumentException("a proposal needs at least two options");
        }
        if (quorum <= 0) {
            throw new IllegalArgumentException("quorum must be positive: " + quorum);
        }
        voteSpace.write(new Proposal(proposalId, question, List.copyOf(options), quorum), lease);
    }

    /**
     * Casts this agent's ballot. A voter's first ballot counts; later ballots by
     * the same voter are ignored at tally time, so re-casting is harmless.
     *
     * @param proposalId the proposal
     * @param option     the chosen option; must be one of the proposal's options
     * @param lease      the ballot's lease (outlive the proposal's decision window)
     * @throws IllegalArgumentException if the proposal is unknown here or the
     *                                  option is not on the ballot
     */
    public void castBallot(String proposalId, String option, Lease lease) {
        Proposal proposal = proposal(proposalId).orElseThrow(() ->
                new IllegalArgumentException("unknown proposal: " + proposalId));
        if (!proposal.options().contains(option)) {
            throw new IllegalArgumentException(
                    "option '" + option + "' is not on the ballot for " + proposalId);
        }
        if (!authorizer.permits(self, Authorizer.Operation.VOTE, voteSpace.name())) {
            // Fail fast rather than write a ballot every tally discards (G2-3);
            // asked per agent, which is per peer unless the authorizer holds
            // agent-level grants (QA4 A4-7 phase 2).
            throw new IllegalStateException("agent " + self.encoded()
                    + " is not permitted VOTE in space '" + voteSpace.name()
                    + "'; its ballot would count for nothing");
        }
        voteSpace.write(new Ballot(proposalId, option, self.encoded()), lease);
    }

    /**
     * Returns the proposal, when known to this replica.
     *
     * @param proposalId the proposal
     * @return the proposal
     */
    public Optional<Proposal> proposal(String proposalId) {
        return voteSpace.read(Template.of(Proposal.class)
                .where("proposalId", eq(proposalId)));
    }

    /**
     * Recomputes the tally from the space: one ballot per distinct voter, first
     * ballot read wins for that voter. The voter identity is the ballot entry's
     * authenticated issuer, not the ballot's self-declared {@code voter} field:
     * a ballot whose declared voter differs from the writer that signed it is
     * an impersonation attempt and is discarded (ASF-006). Ballot stuffing under
     * invented voter ids fails the same check, so the tally stays recomputable
     * and auditable by any member from the space alone. A ballot naming an
     * option that is not on the proposal is likewise ignored: {@link #castBallot}
     * refuses such options locally, so one written straight to the space is a
     * spoiled ballot and must neither count toward the quorum nor invent an
     * option that could win. A ballot whose issuer is outside the configured
     * electorate (no fresh AgentCard, under the spec §8 rule) is skipped: it
     * neither counts toward the quorum nor toward any option, and it is
     * re-examined at every tally, so the tally tracks the live electorate.
     *
     * <p>Counting follows the authorizer's granularity (QA4 A4-7 phase 2), so
     * counting and authorization can never disagree. Under {@code PEER} the
     * counted identity is the issuer's peer: a peer with ten agents casts one
     * counted ballot however it is seated, which is what closes the sybil hole
     * of a self-asserted {@code localName}. Under {@code AGENT} each permitted
     * agent counts once, and only when the record is {@code AGENT_ATTESTED},
     * because a peer-asserted name is exactly what the granularity exists to
     * distrust. The electorate predicate is the liveness filter it always was
     * and is not an eligibility check.
     *
     * @param proposalId the proposal
     * @return votes per option, zero-filled for unvoted options when the proposal
     *         is known
     */
    public Map<String, Integer> tally(String proposalId) {
        return tallyWith(proposalId);
    }

    /**
     * Sets the revocations the tally applies (SPEC §6.1, v0.1.13); defaults to
     * the vote space's own when it is a replicated space.
     *
     * @param revocations the group's revocation view
     * @return this capability
     */
    public VoteCapability revocations(ai.badmonkey.agentspaces.api.security.RevocationView revocations) {
        this.revocations = Objects.requireNonNull(revocations, "revocations");
        return this;
    }

    private Map<String, Integer> tallyWith(String proposalId) {
        Map<String, Integer> tally = new TreeMap<>();
        Optional<Proposal> proposal = proposal(proposalId);
        proposal.ifPresent(p -> p.options().forEach(o -> tally.put(o, 0)));
        Authorizer.Granularity granularity = granularity();
        Set<String> counted = new HashSet<>();
        for (Space.Issued<Ballot> issued : voteSpace.readAllIssued(
                Template.of(Ballot.class).where("proposalId", eq(proposalId)), 10_000)) {
            Ballot ballot = issued.entry();
            if (!ballot.voter().equals(issued.issuer().encoded())) {
                continue; // declared voter is not the authenticated writer: forged
            }
            if (proposal.isPresent() && !proposal.get().options().contains(ballot.option())) {
                continue; // option is not on the ballot: spoiled, counts for nothing
            }
            if (!electorate.test(issued.issuer())) {
                continue; // outside the electorate (no fresh AgentCard): does not count
            }
            if (granularity == Authorizer.Granularity.AGENT
                    && issued.attestation() != Space.Attestation.AGENT_ATTESTED) {
                continue; // per-agent counting trusts only the agent's own key
            }
            if (!authorizer.permits(issued.issuer(), Authorizer.Operation.VOTE,
                    voteSpace.name())) {
                continue; // the profile's authorizer refuses this voter (G2-3)
            }
            if (revocations.revoked(issued.issuer(), null)) {
                // v0.1.13 (M-13): a revoked agent's ballot stops counting at the
                // next recount, so a decision can reopen (SPEC §8).
                continue;
            }
            String key = granularity == Authorizer.Granularity.AGENT
                    ? issued.issuer().encoded() : issued.issuer().peer().value();
            if (counted.add(key)) {
                tally.merge(ballot.option(), 1, Integer::sum);
            }
        }
        return tally;
    }

    /**
     * The granularity this capability counts at: the authorizer's, for
     * {@code VOTE} in the vote space's scope.
     *
     * @return {@code PEER} (one counted ballot per peer) or {@code AGENT}
     */
    public Authorizer.Granularity granularity() {
        return authorizer.granularity(Authorizer.Operation.VOTE, voteSpace.name());
    }

    /**
     * Returns the decision once the quorum of distinct voters is reached: the
     * option with the most votes, ties broken lexicographically so every replica
     * agrees.
     *
     * @param proposalId the proposal
     * @return the decision, or empty while the quorum is short
     */
    public Optional<Decision> decision(String proposalId) {
        Optional<Proposal> proposal = proposal(proposalId);
        if (proposal.isEmpty()) {
            return Optional.empty();
        }
        Map<String, Integer> tally = tally(proposalId);
        int voters = tally.values().stream().mapToInt(Integer::intValue).sum();
        if (voters < proposal.get().quorum()) {
            return Optional.empty();
        }
        String winner = tally.entrySet().stream()
                .max(Map.Entry.<String, Integer>comparingByValue()
                        .thenComparing(Map.Entry.comparingByKey(
                                java.util.Comparator.reverseOrder())))
                .map(Map.Entry::getKey)
                .orElseThrow();
        return Optional.of(new Decision(proposalId, winner, new HashMap<>(tally), granularity()));
    }

    // ------------------------------------------------------- MAJORITY_GOSSIP mode

    /**
     * Casts an approximate preference through push-sum: contributes 1 to the
     * chosen option's average and 0 to the others. All participants must cast
     * over the same aggregator and then tick it to convergence.
     *
     * @param aggregate  the group's push-sum aggregator
     * @param voteId     the vote identifier
     * @param options    the options
     * @param preference this agent's preferred option
     */
    public static void castPreference(PushSumAggregate aggregate, String voteId,
                                      List<String> options, String preference) {
        for (String option : options) {
            aggregate.start(epoch(voteId, option), option.equals(preference) ? 1.0 : 0.0);
        }
    }

    /**
     * Reads the current approximate leader from the push-sum estimates.
     *
     * @param aggregate the group's push-sum aggregator
     * @param voteId    the vote identifier
     * @param options   the options
     * @return the option with the highest estimated preference share
     */
    public static Optional<String> leader(PushSumAggregate aggregate, String voteId,
                                          List<String> options) {
        String best = null;
        double bestShare = -1;
        for (String option : options) {
            var estimate = aggregate.estimate(epoch(voteId, option));
            if (estimate.isPresent() && estimate.getAsDouble() > bestShare) {
                bestShare = estimate.getAsDouble();
                best = option;
            }
        }
        return Optional.ofNullable(best);
    }

    private static String epoch(String voteId, String option) {
        return "vote:" + voteId + ":" + option;
    }
}
