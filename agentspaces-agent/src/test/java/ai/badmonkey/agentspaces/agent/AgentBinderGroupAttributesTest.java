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
package ai.badmonkey.agentspaces.agent;

import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.Ballot;
import ai.badmonkey.agentspaces.agent.annotation.BidFunction;
import ai.badmonkey.agentspaces.agent.annotation.CapabilityRef;
import ai.badmonkey.agentspaces.agent.annotation.OnDecision;
import ai.badmonkey.agentspaces.agent.annotation.OnEstimate;
import ai.badmonkey.agentspaces.agent.annotation.OrderedTake;
import ai.badmonkey.agentspaces.agent.annotation.Part;
import ai.badmonkey.agentspaces.agent.annotation.Propose;
import ai.badmonkey.agentspaces.agent.annotation.ProvidesCapability;
import ai.badmonkey.agentspaces.agent.annotation.SpaceJoin;
import ai.badmonkey.agentspaces.agent.annotation.SpaceNotify;
import ai.badmonkey.agentspaces.agent.annotation.SpaceReduce;
import ai.badmonkey.agentspaces.agent.annotation.SpaceRef;
import ai.badmonkey.agentspaces.agent.join.Joined;
import ai.badmonkey.agentspaces.agent.reduce.Reductions;
import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.ad.CardAction;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.CapabilityProvider;
import ai.badmonkey.agentspaces.capabilities.aggregate.PushSumAggregate;
import ai.badmonkey.agentspaces.capabilities.vote.VoteCapability;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code group} attribute on every binding annotation other than
 * {@code @SpaceTake} (QA-SPEC-COVERAGE §7 claimed it; only the take had a
 * test): a bean whose methods name groups binds each into that group only, a
 * binder of another group skips a method naming one without touching the
 * coordinator, aggregate, or client it would need, and a group nobody joined
 * fails at bind through the facade. {@code @OrderedTake} and {@code @OnEstimate}
 * bind by name where their coordinator and aggregate exist, in
 * {@link AgentBinderOrderedAttributesClusterTest}; here they are skipped and
 * refused like the rest.
 */
class AgentBinderGroupAttributesTest {

    private static final Lease HOUR = Lease.of(Duration.ofHours(1));

    public record Verdict(String proposalId, String winner) {
    }

    public record Note(String text) {
    }

    private final PeerIdentity identity = PeerIdentity.generate();
    private final AgentSpaces spaces = new AgentSpaces(identity, InstantSource.system());
    private final LocalSpace alphaVotes = LocalSpace.builder("votes", identity.agent("host")).build();
    private final LocalSpace betaVotes = LocalSpace.builder("votes", identity.agent("host")).build();
    private final LocalSpace alphaNotes = LocalSpace.builder("notes", identity.agent("host")).build();
    private final LocalSpace betaNotes = LocalSpace.builder("notes", identity.agent("host")).build();
    private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable c : closeables) {
            c.close();
        }
        spaces.close();
        alphaVotes.close();
        betaVotes.close();
        alphaNotes.close();
        betaNotes.close();
    }

    private VoteCapability vote(Space space) {
        return new VoteCapability(space, identity.agent("host"), identity.peerId(), InstantSource.system());
    }

    /** Votes in alpha, records decisions in beta, hears notes in beta, holds refs per group. */
    @AgentSpec(name = "split", description = "Binds into two groups by name", goals = {"split"})
    public static class Split {
        final List<String> judged = new CopyOnWriteArrayList<>();
        final List<String> decided = new CopyOnWriteArrayList<>();
        final List<String> noted = new CopyOnWriteArrayList<>();
        @SpaceRef(value = "notes", group = "beta")
        Space betaNotesRef;

        @Ballot(space = "votes", group = "alpha")
        public String judge(VoteCapability.Proposal proposal) {
            judged.add(proposal.proposalId());
            return "yes";
        }

        @OnDecision(space = "votes", group = "beta")
        public Verdict record(VoteCapability.Decision decision) {
            decided.add(decision.proposalId());
            return new Verdict(decision.proposalId(), decision.winner());
        }

        @SpaceNotify(space = "notes", group = "beta")
        public Void hear(Note note) {
            noted.add(note.text());
            return null;
        }
    }

    @Test
    @Timeout(30)
    void eachMethodBindsIntoTheGroupItNames() throws Exception {
        AgentSpaces.GroupContext alpha = spaces.register("alpha", GroupId.of("zGrpAlpha"), null, null);
        AgentSpaces.GroupContext beta = spaces.register("beta", GroupId.of("zGrpBeta"), null, null);
        VoteCapability alphaVote = vote(alphaVotes);
        alpha.space("votes", alphaVotes, identity.agent("host")).space("notes", alphaNotes).vote("votes", alphaVote);
        beta.space("votes", betaVotes, identity.agent("host")).space("notes", betaNotes).vote("votes", vote(betaVotes));
        Split split = new Split();
        List<AgentBinder.Bound> bound = spaces.bind(split);
        closeables.addAll(bound);
        assertThat(bound).hasSize(2);
        assertThat(split.betaNotesRef).isSameAs(betaNotes);

        // A proposal in alpha is judged (the ballot is alpha's); the same in beta is not.
        vote(alphaVotes).propose("p-alpha", "ship?", List.of("yes", "no"), 1, HOUR);
        vote(betaVotes).propose("p-beta", "ship?", List.of("yes", "no"), 1, HOUR);
        await(() -> split.judged.contains("p-alpha"));
        // Beta's decision closes when beta's vote capability gets a ballot; alpha's never reaches the recorder.
        vote(betaVotes).castBallot("p-beta", "no", HOUR);
        await(() -> split.decided.contains("p-beta"));
        assertThat(betaVotes.readAll(Template.of(Verdict.class), 10)).singleElement()
                .satisfies(v -> assertThat(v.winner()).isEqualTo("no"));
        alphaNotes.write(new Note("alpha"), HOUR);
        betaNotes.write(new Note("beta"), HOUR);
        await(() -> split.noted.contains("beta"));
        Thread.sleep(300);
        assertThat(split.judged).containsExactly("p-alpha");
        assertThat(split.decided).containsExactly("p-beta");
        assertThat(split.noted).containsExactly("beta");
    }

    @Test
    void aGroupNobodyJoinedFailsAtBindForEveryAnnotation() {
        spaces.register("alpha", GroupId.of("zGrpAlpha"), null, null)
                .space("votes", alphaVotes, identity.agent("host")).space("notes", alphaNotes).vote("votes", vote(alphaVotes));
        assertThatThrownBy(() -> spaces.bind(new Object() {
            @Ballot(space = "votes", group = "gamma")
            public String judge(VoteCapability.Proposal p) {
                return null;
            }
        })).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> spaces.bind(new Object() {
            @BidFunction(space = "notes", group = "gamma")
            public double bid(Note n) {
                return 1.0;
            }
        })).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> spaces.bind(new Object() {
            @OnDecision(space = "votes", group = "gamma")
            public Verdict record(VoteCapability.Decision d) {
                return null;
            }
        })).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> spaces.bind(new Object() {
            @OrderedTake(space = "notes", group = "gamma")
            public void take(Note n) {
            }
        })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("no joined group");
        assertThatThrownBy(() -> spaces.bind(new Object() {
            @Propose(space = "notes", vote = "votes", group = "gamma", key = "text", options = {"a", "b"}, quorum = 1)
            public String ask(Note n) {
                return null;
            }
        })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("gamma");
        assertThatThrownBy(() -> spaces.bind(new Object() {
            @SpaceJoin(space = "notes", group = "gamma", key = "caseId", parts = {@Part(Left.class), @Part(Right.class)})
            public void join(Joined j) {
            }
        })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("gamma");
        assertThatThrownBy(() -> spaces.bind(new Object() {
            @SpaceReduce(space = "notes", group = "gamma", key = "account")
            public Balance fold(Balance b, Payment p) {
                return null;
            }
        })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("no joined group");
        assertThatThrownBy(() -> spaces.bind(new Object() {
            @OnEstimate(group = "gamma")
            public void report(PushSumAggregate.Estimate e) {
            }
        })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("gamma");
        assertThatThrownBy(() -> spaces.bind(new GammaProbe()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("gamma");
    }

    @ProvidesCapability(value = "aspace:cap/probe", group = "gamma")
    public static class GammaProbe implements CapabilityProvider {
        @Override
        public String capabilityType() {
            return "aspace:cap/probe";
        }

        @Override
        public CapabilityAdvertisement describe(GroupId group) {
            throw new UnsupportedOperationException("never advertised: its group is not joined");
        }
    }

    // ------------------------------------------------------------ a binder of another group skips

    static final class Gauge {
    }

    /** One owned reaction beside five Layer 4 bindings and a client ref for another group. */
    @AgentSpec(name = "mixed", description = "Binds one method here and the rest elsewhere", goals = {"mix"})
    public static class Mixed {
        @CapabilityRef(group = "alpha")
        Gauge alphaGauge;
        @CapabilityRef(group = "beta")
        Gauge betaGauge;

        @SpaceNotify(space = "notes", group = "alpha")
        public void hear(Note note) {
        }

        @OrderedTake(space = "notes", group = "beta")
        public void take(Note note) {
        }

        @Propose(space = "notes", vote = "votes", group = "beta", key = "text", options = {"a", "b"}, quorum = 1)
        public String ask(Note note) {
            return null;
        }

        @SpaceJoin(space = "notes", group = "beta", key = "caseId", parts = {@Part(Left.class), @Part(Right.class)})
        public void join(Joined joined) {
        }

        @SpaceReduce(space = "notes", group = "beta", key = "account")
        public Balance fold(Balance balance, Payment payment) {
            return null;
        }

        @OnEstimate(group = "beta")
        public void report(PushSumAggregate.Estimate estimate) {
        }
    }

    /**
     * The binder's {@code owns(group)} rule for the Layer 4 annotations: a method
     * naming another group is skipped before the coordinator, aggregate, or vote
     * it would need is looked up, and a {@code @CapabilityRef} naming another
     * group stays null while the one naming this group is injected.
     */
    @Test
    void aBinderOfAnotherGroupSkipsTheMethodsAndFieldsNamingIt() {
        Gauge gauge = new Gauge();
        AgentBinder alpha = new AgentBinder(identity, GroupId.of("zGrpAlpha"), null, InstantSource.system(), "alpha")
                .space("notes", alphaNotes).space("votes", alphaVotes).vote("votes", vote(alphaVotes))
                .client(Gauge.class, gauge);
        closeables.add(alpha);
        Mixed mixed = new Mixed();
        AgentBinder.Bound bound = alpha.bind(mixed);
        assertThat(bound.card().actions()).extracting(CardAction::name).containsExactly("hear");
        assertThat(mixed.alphaGauge).isSameAs(gauge);
        assertThat(mixed.betaGauge).as("a ref for another group is not this binder's to inject").isNull();
    }

    // ------------------------------------------------------------ Layer 4 methods bind by name

    public record Left(String caseId) {
    }

    public record Right(String caseId) {
    }

    public record Pair(String caseId) {
    }

    public record Payment(String account, long cents) {
    }

    public record Balance(String account, long cents) {
    }

    /** Proposes in alpha; joins and folds in beta. */
    @AgentSpec(name = "split4", description = "Binds Layer 4 methods into two groups by name", goals = {"split"})
    public static class SplitLayer4 {
        @Propose(space = "notes", vote = "votes", group = "alpha", prefix = "ask:", key = "text",
                options = {"yes", "no"}, quorum = 1)
        public String ask(Note note) {
            return "about " + note.text();
        }

        @SpaceJoin(space = "notes", group = "beta", key = "caseId", parts = {@Part(Left.class), @Part(Right.class)})
        public Pair join(Joined joined) {
            return new Pair(joined.key());
        }

        @SpaceReduce(space = "notes", group = "beta", key = "account", lease = "2s", pollTimeout = "100ms")
        public Balance fold(Balance balance, Payment payment) {
            return new Balance(payment.account(), (balance == null ? 0 : balance.cents()) + payment.cents());
        }
    }

    @Test
    @Timeout(30)
    void layerFourMethodsBindIntoTheGroupTheyName() throws Exception {
        AgentSpaces.GroupContext alpha = spaces.register("alpha", GroupId.of("zGrpAlpha"), null, null);
        AgentSpaces.GroupContext beta = spaces.register("beta", GroupId.of("zGrpBeta"), null, null);
        VoteCapability alphaVote = vote(alphaVotes);
        VoteCapability betaVote = vote(betaVotes);
        alpha.space("votes", alphaVotes, identity.agent("host")).space("notes", alphaNotes).vote("votes", alphaVote);
        beta.space("votes", betaVotes, identity.agent("host")).space("notes", betaNotes).vote("votes", betaVote);
        List<AgentBinder.Bound> bound = spaces.bind(new SplitLayer4());
        closeables.addAll(bound);
        assertThat(bound).hasSize(2);

        alphaNotes.write(new Note("n1"), HOUR);
        betaNotes.write(new Note("n2"), HOUR);
        await(() -> alphaVote.proposal("ask:n1").isPresent());
        betaNotes.write(new Left("c1"), HOUR);
        betaNotes.write(new Right("c1"), HOUR);
        alphaNotes.write(new Left("c2"), HOUR);
        alphaNotes.write(new Right("c2"), HOUR);
        await(() -> !betaNotes.readAll(Template.of(Pair.class), 10).isEmpty());
        betaNotes.write(new Payment("acc", 5), HOUR);
        alphaNotes.write(new Payment("acc", 5), HOUR);
        await(() -> Reductions.current(betaNotes, Balance.class, "split4.fold", "acc").isPresent());
        Thread.sleep(300);
        assertThat(betaVote.proposal("ask:n2")).as("the propose is alpha's").isEmpty();
        assertThat(alphaNotes.readAll(Template.of(Pair.class), 10)).as("the join is beta's").isEmpty();
        assertThat(alphaNotes.readAll(Template.of(Payment.class), 10)).as("the reduce is beta's").hasSize(1);
        assertThat(alphaNotes.readAll(Template.of(Balance.class), 10)).isEmpty();
    }

    private static void await(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }
}
