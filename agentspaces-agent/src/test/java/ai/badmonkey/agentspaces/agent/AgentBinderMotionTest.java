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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.badmonkey.agentspaces.agent.annotation.AgentSpec;
import ai.badmonkey.agentspaces.agent.annotation.SpaceNotify;
import ai.badmonkey.agentspaces.agent.annotation.SpaceTake;
import ai.badmonkey.agentspaces.agent.capability.Contribution;
import ai.badmonkey.agentspaces.agent.capability.Motion;
import ai.badmonkey.agentspaces.api.ad.CardAction;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.vote.VoteCapability;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The {@link Motion} return (ISSUE-Motion): a bound method returns the vote to
 * open, the binder opens it once per proposal id per replica, a take is
 * completed first, and a declared {@code Motion} or {@code Contribution}
 * return fails at bind time when its capability is missing.
 */
class AgentBinderMotionTest {

    private static final Lease HOUR = Lease.of(Duration.ofHours(1));

    private final PeerIdentity identity = PeerIdentity.generate();
    private final LocalSpace tasks = LocalSpace.builder("tasks", identity.agent("host"))
            .sweepEvery(Duration.ofMillis(50)).build();
    private final LocalSpace votes = LocalSpace.builder("votes", identity.agent("host")).build();
    private final VoteCapability vote = new VoteCapability(votes, identity.agent("host"),
            identity.peerId(), InstantSource.system());
    private final AgentBinder binder = new AgentBinder(identity, GroupId.of("zMotion"), null,
            InstantSource.system()).space("tasks", tasks).space("votes", votes).vote("votes", vote);

    @AfterEach
    void tearDown() {
        binder.close();
        votes.close();
        tasks.close();
    }

    public record Cue(String caseId, String text) {
    }

    @AgentSpec(name = "lead", description = "Opens a vote per case", goals = {"open"})
    public static class NotifyLead {
        final AtomicInteger invocations = new AtomicInteger();

        @SpaceNotify(space = "tasks")
        public Motion open(Cue cue) {
            invocations.incrementAndGet();
            return Motion.of("case:" + cue.caseId(), cue.text(), List.of("approve", "deny"), 2,
                    Lease.of(Duration.ofHours(12)));
        }
    }

    @Test
    void aMotionReturnOpensTheVoteOnceAndWritesNoEntry() throws Exception {
        NotifyLead lead = new NotifyLead();
        AgentBinder.Bound bound = binder.bind(lead);
        tasks.write(new Cue("c1", "approve c1?"), HOUR);
        await(() -> vote.proposal("case:c1").isPresent(), Duration.ofSeconds(5));
        VoteCapability.Proposal proposal = vote.proposal("case:c1").orElseThrow();
        assertThat(proposal.question()).isEqualTo("approve c1?");
        assertThat(proposal.options()).containsExactly("approve", "deny");
        assertThat(proposal.quorum()).isEqualTo(2);
        // A second cue for the same case: the method runs (it is a notify), the
        // motion is skipped because the proposal is already open here.
        tasks.write(new Cue("c1", "approve c1 again?"), HOUR);
        await(() -> lead.invocations.get() == 2, Duration.ofSeconds(5));
        Thread.sleep(150);
        assertThat(votes.readAll(Template.of(VoteCapability.Proposal.class), 10)).hasSize(1);
        assertThat(vote.proposal("case:c1").orElseThrow().question()).isEqualTo("approve c1?");
        assertThat(tasks.readAll(Template.of(Motion.class), 10)).as("a motion is never an entry").isEmpty();
        // The card: a declared Motion return produces a Proposal into the vote space.
        assertThat(bound.card().produces()).contains(VoteCapability.Proposal.class.getName() + "#v1");
        assertThat(bound.card().spaceBindings())
                .containsEntry(VoteCapability.Proposal.class.getName() + "#v1", "votes");
        CardAction action = bound.card().actions().get(0);
        assertThat(action.produces()).containsExactly(VoteCapability.Proposal.class.getName() + "#v1");
    }

    @AgentSpec(name = "taker", description = "Opens a vote per taken task", goals = {"open"})
    public static class TakeLead {
        @SpaceTake(space = "tasks", pollTimeout = "50ms")
        public Motion open(Cue cue) {
            return Motion.in("votes", "case:" + cue.caseId(), cue.text(), List.of("yes", "no"), 1, HOUR);
        }
    }

    @Test
    void aMotionFromATakeCompletesTheTakeAndThenOpens() throws Exception {
        binder.bind(new TakeLead());
        tasks.write(new Cue("c2", "take c2?"), HOUR);
        await(() -> vote.proposal("case:c2").isPresent(), Duration.ofSeconds(5));
        assertThat(tasks.readAll(Template.of(Cue.class), 10)).as("the take completed").isEmpty();
        assertThat(vote.proposal("case:c2").orElseThrow().options()).containsExactly("yes", "no");
    }

    public static class VotelessLead {
        @SpaceNotify(space = "tasks")
        public Motion open(Cue cue) {
            return null;
        }
    }

    public static class AggregatelessSensor {
        @SpaceNotify(space = "tasks")
        public Contribution feel(Cue cue) {
            return null;
        }
    }

    @Test
    void declaredMotionAndContributionReturnsRefuseToBindWithoutTheirCapability() {
        AgentBinder bare = new AgentBinder(identity, GroupId.of("zMotion"), null, InstantSource.system())
                .space("tasks", tasks);
        try {
            assertThatThrownBy(() -> bare.bind(new VotelessLead()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Motion").hasMessageContaining("group.provide(vote)");
            assertThatThrownBy(() -> bare.bind(new AggregatelessSensor()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Contribution").hasMessageContaining("group.provide(aggregate)");
        } finally {
            bare.close();
        }
    }

    private static void await(BooleanSupplier condition, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within " + timeout);
            }
            Thread.sleep(20);
        }
    }
}
