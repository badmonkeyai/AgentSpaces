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
import ai.badmonkey.agentspaces.agent.annotation.Ballot;
import ai.badmonkey.agentspaces.agent.annotation.Propose;
import ai.badmonkey.agentspaces.agent.capability.Motion;
import ai.badmonkey.agentspaces.api.ad.CardAction;
import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.vote.VoteCapability;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.space.local.LocalSpace;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * {@code @Propose} (ISSUE-Propose): the cue opens a vote with the shape on the
 * annotation, once per proposal id per bound agent and per replica, with a
 * composite or tag key, a {@code null} that asks nothing, a {@code Motion} for
 * the odd cue, bind-time validation, a card action of kind {@code propose},
 * and a class that both proposes and votes.
 */
class AgentBinderProposeTest {

    private static final Lease HOUR = Lease.of(Duration.ofHours(1));

    private final PeerIdentity identity = PeerIdentity.generate();
    private final LocalSpace cues = LocalSpace.builder("cues", identity.agent("host")).build();
    private final LocalSpace votes = LocalSpace.builder("votes", identity.agent("host")).build();
    private final VoteCapability vote = new VoteCapability(votes, identity.agent("host"),
            identity.peerId(), InstantSource.system());
    private final AgentBinder binder = new AgentBinder(identity, GroupId.of("zPropose"), null,
            InstantSource.system()).space("cues", cues).space("votes", votes).vote("votes", vote);

    @AfterEach
    void tearDown() {
        binder.close();
        votes.close();
        cues.close();
    }

    public record Assessment(String tripId, String location, String wave, String channel, int danger) {
    }

    @AgentSpec(name = "safety-lead", description = "Opens an advisory vote per wave", goals = {"advise"})
    public static class SafetyLead {
        final AtomicInteger invocations = new AtomicInteger();

        @Propose(space = "cues", vote = "votes", prefix = "advisory:", key = {"tripId", "location", "wave"},
                options = {"GO", "CAUTION", "AVOID"}, quorum = 3, lease = "12h")
        public String open(Assessment a) {
            invocations.incrementAndGet();
            return "Travel advisory for " + a.location() + " (" + a.wave() + ")";
        }
    }

    @Test
    void aCueOpensOneProposalWithTheAnnotationsShapeAndACompositeKey() throws Exception {
        SafetyLead lead = new SafetyLead();
        binder.bind(lead);
        cues.write(new Assessment("t1", "Lisbon", "baseline", "news", 10), HOUR);
        await(() -> vote.proposal("advisory:t1:Lisbon:baseline").isPresent(), Duration.ofSeconds(5));
        VoteCapability.Proposal proposal = vote.proposal("advisory:t1:Lisbon:baseline").orElseThrow();
        assertThat(proposal.question()).isEqualTo("Travel advisory for Lisbon (baseline)");
        assertThat(proposal.options()).containsExactly("GO", "CAUTION", "AVOID");
        assertThat(proposal.quorum()).isEqualTo(3);
        Space.Entry<VoteCapability.Proposal> entry = votes
                .readAllEntries(Template.of(VoteCapability.Proposal.class), 10).get(0);
        assertThat(entry.lease().kind()).isEqualTo(LeaseKind.WRITE);
        assertThat(entry.lease().expiresAtMillis())
                .isGreaterThan(System.currentTimeMillis() + Duration.ofHours(11).toMillis());
        assertThat(entry.issuer()).isEqualTo(identity.agent("host"));
    }

    @Test
    void severalCuesForOneIdOpenOnceAndInvokeTheMethodOnce() throws Exception {
        SafetyLead lead = new SafetyLead();
        binder.bind(lead);
        for (String channel : List.of("news", "video", "social")) {
            cues.write(new Assessment("t1", "Lisbon", "baseline", channel, 10), HOUR);
        }
        cues.write(new Assessment("t1", "Porto", "baseline", "news", 5), HOUR);     // another wave
        await(() -> vote.proposal("advisory:t1:Porto:baseline").isPresent(), Duration.ofSeconds(5));
        Thread.sleep(200);
        assertThat(votes.readAll(Template.of(VoteCapability.Proposal.class), 10)).hasSize(2);
        assertThat(lead.invocations.get()).as("one invocation per proposal id").isEqualTo(2);
    }

    @Test
    void aProposalAlreadyInTheReplicaOpensNothingAndSkipsTheMethod() throws Exception {
        vote.propose("advisory:t1:Lisbon:baseline", "already asked", List.of("GO", "AVOID"), 1, HOUR);
        SafetyLead lead = new SafetyLead();
        binder.bind(lead);
        cues.write(new Assessment("t1", "Lisbon", "baseline", "news", 10), HOUR);
        Thread.sleep(300);
        assertThat(votes.readAll(Template.of(VoteCapability.Proposal.class), 10)).hasSize(1);
        assertThat(vote.proposal("advisory:t1:Lisbon:baseline").orElseThrow().question())
                .isEqualTo("already asked");
        assertThat(lead.invocations.get()).isZero();
    }

    public record Remediation(String incidentId, String action, boolean urgent) {
    }

    @AgentSpec(name = "risk-lead", description = "Puts urgent remediations to the panel", goals = {"decide"})
    public static class RiskLead {
        final AtomicInteger invocations = new AtomicInteger();
        final AtomicInteger failuresLeft = new AtomicInteger();

        @Propose(space = "cues", vote = "votes", prefix = "remediation:", key = "incidentId",
                options = {"approve", "reject"}, quorum = 2)
        public String open(Remediation r) {
            invocations.incrementAndGet();
            if (failuresLeft.getAndDecrement() > 0) {
                throw new IllegalStateException("simulated failure");
            }
            return r.urgent() ? "Approve " + r.action() + " for " + r.incidentId() + "?" : null;
        }
    }

    @Test
    void aNullReturnAsksNothingAndTheNextCueForTheIdMayAsk() throws Exception {
        RiskLead lead = new RiskLead();
        binder.bind(lead);
        cues.write(new Remediation("inc-1", "restart the pod", false), HOUR);
        await(() -> lead.invocations.get() == 1, Duration.ofSeconds(5));
        Thread.sleep(150);
        assertThat(vote.proposal("remediation:inc-1")).isEmpty();
        cues.write(new Remediation("inc-1", "restart the pod", true), HOUR);
        await(() -> vote.proposal("remediation:inc-1").isPresent(), Duration.ofSeconds(5));
        assertThat(lead.invocations.get()).isEqualTo(2);
    }

    @Test
    void aFailedOpenReleasesTheIdSoTheNextCueRetries() throws Exception {
        RiskLead lead = new RiskLead();
        lead.failuresLeft.set(1);
        binder.bind(lead);
        cues.write(new Remediation("inc-2", "rotate the key", true), HOUR);
        await(() -> lead.invocations.get() == 1, Duration.ofSeconds(5));
        Thread.sleep(150);
        assertThat(vote.proposal("remediation:inc-2")).as("the first attempt failed").isEmpty();
        cues.write(new Remediation("inc-2", "rotate the key", true), HOUR);
        await(() -> vote.proposal("remediation:inc-2").isPresent(), Duration.ofSeconds(5));
        assertThat(lead.invocations.get()).isEqualTo(2);
    }

    public record Signal(String note) {
    }

    @AgentSpec(name = "tag-lead", description = "Keys its votes by a tag", goals = {"decide"})
    public static class TagLead {
        @Propose(space = "cues", vote = "votes", prefix = "sig:", keyTag = "case", tags = "kind=alert",
                options = {"ack", "ignore"}, quorum = 1)
        public String open(Signal s) {
            return s.note();
        }
    }

    @Test
    void aTagKeyedCueIsFilteredAndKeyedByItsTags() throws Exception {
        binder.bind(new TagLead());
        cues.write(new Signal("disk full"), HOUR, Map.of("case", "c-9", "kind", "alert"));
        cues.write(new Signal("just info"), HOUR, Map.of("case", "c-10", "kind", "info"));
        cues.write(new Signal("no case"), HOUR, Map.of("kind", "alert"));
        await(() -> vote.proposal("sig:c-9").isPresent(), Duration.ofSeconds(5));
        Thread.sleep(200);
        assertThat(votes.readAll(Template.of(VoteCapability.Proposal.class), 10)).hasSize(1);
        assertThat(vote.proposal("sig:c-9").orElseThrow().question()).isEqualTo("disk full");
    }

    @AgentSpec(name = "odd-lead", description = "Shapes each vote itself", goals = {"decide"})
    public static class MotionLead {
        @Propose(space = "cues", vote = "votes", options = {"a", "b"}, quorum = 9)
        public Motion open(Remediation r) {
            return Motion.of("own:" + r.incidentId(), r.action(), List.of("now", "later", "never"), 1, HOUR);
        }
    }

    @Test
    void aMotionReturnOpensItsOwnShapeOnTheAnnotationsVoteSpace() throws Exception {
        binder.bind(new MotionLead());
        cues.write(new Remediation("inc-3", "patch", true), HOUR);
        await(() -> vote.proposal("own:inc-3").isPresent(), Duration.ofSeconds(5));
        VoteCapability.Proposal proposal = vote.proposal("own:inc-3").orElseThrow();
        assertThat(proposal.options()).containsExactly("now", "later", "never");
        assertThat(proposal.quorum()).isEqualTo(1);
    }

    @AgentSpec(name = "self-judge", description = "Proposes and votes", goals = {"decide"})
    public static class ProposerAndVoter {
        @Propose(space = "cues", vote = "votes", prefix = "self:", key = "incidentId",
                options = {"yes", "no"}, quorum = 1)
        public String open(Remediation r) {
            return "Do " + r.action() + "?";
        }

        @Ballot(space = "votes", prefix = "self:")
        public String judge(VoteCapability.Proposal proposal) {
            return "yes";
        }
    }

    @Test
    void aClassMayBothProposeAndVoteOnTheSameSpace() throws Exception {
        AgentBinder.Bound bound = binder.bind(new ProposerAndVoter());
        cues.write(new Remediation("inc-4", "reboot", true), HOUR);
        await(() -> vote.decision("self:inc-4").isPresent(), Duration.ofSeconds(5));
        assertThat(vote.decision("self:inc-4").orElseThrow().winner()).isEqualTo("yes");
        assertThat(bound.card().actions()).extracting(CardAction::kind)
                .containsExactlyInAnyOrder(CardAction.PROPOSE, CardAction.BALLOT);
    }

    @Test
    void theCardDeclaresTheCueAndTheProposalAndTheActionIsNotInvocable() {
        AgentBinder.Bound bound = binder.bind(new RiskLead());
        String cue = Remediation.class.getName() + "#v1";
        String proposal = VoteCapability.Proposal.class.getName() + "#v1";
        assertThat(bound.card().consumes()).containsExactly(cue);
        assertThat(bound.card().produces()).containsExactly(proposal);
        assertThat(bound.card().spaceBindings()).containsEntry(cue, "cues").containsEntry(proposal, "votes");
        CardAction action = bound.card().actions().get(0);
        assertThat(action.kind()).isEqualTo(CardAction.PROPOSE);
        assertThat(action.invocable()).isFalse();
        assertThat(action.consumes()).containsExactly(cue);
        assertThat(action.produces()).containsExactly(proposal);
        assertThat(action.space()).isEqualTo("cues");
    }

    public static class OneOption {
        @Propose(space = "cues", vote = "votes", key = "incidentId", options = {"only"}, quorum = 1)
        public String open(Remediation r) {
            return "?";
        }
    }

    public static class ZeroQuorum {
        @Propose(space = "cues", vote = "votes", key = "incidentId", options = {"a", "b"}, quorum = 0)
        public String open(Remediation r) {
            return "?";
        }
    }

    public static class BothKeys {
        @Propose(space = "cues", vote = "votes", key = "incidentId", keyTag = "case", options = {"a", "b"},
                quorum = 1)
        public String open(Remediation r) {
            return "?";
        }
    }

    public static class NoKey {
        @Propose(space = "cues", vote = "votes", options = {"a", "b"}, quorum = 1)
        public String open(Remediation r) {
            return "?";
        }
    }

    public static class NoSuchField {
        @Propose(space = "cues", vote = "votes", key = "caseId", options = {"a", "b"}, quorum = 1)
        public String open(Remediation r) {
            return "?";
        }
    }

    public static class BadFilter {
        @Propose(space = "cues", vote = "votes", key = "incidentId", tags = "=x", options = {"a", "b"},
                quorum = 1)
        public String open(Remediation r) {
            return "?";
        }
    }

    public static class WrongReturn {
        @Propose(space = "cues", vote = "votes", key = "incidentId", options = {"a", "b"}, quorum = 1)
        public Remediation open(Remediation r) {
            return r;
        }
    }

    @Test
    void proposesAreValidatedAtBindTime() {
        assertThatThrownBy(() -> binder.bind(new OneOption()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("two options");
        assertThatThrownBy(() -> binder.bind(new ZeroQuorum()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("quorum");
        assertThatThrownBy(() -> binder.bind(new BothKeys()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("keyTag");
        assertThatThrownBy(() -> binder.bind(new NoKey()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("no key or keyTag");
        assertThatThrownBy(() -> binder.bind(new NoSuchField()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("caseId");
        assertThatThrownBy(() -> binder.bind(new BadFilter()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("=x");
        assertThatThrownBy(() -> binder.bind(new WrongReturn()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("String");
        AgentBinder voteless = new AgentBinder(identity, GroupId.of("zPropose"), null, InstantSource.system())
                .space("cues", cues);
        try {
            assertThatThrownBy(() -> voteless.bind(new RiskLead()))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("vote");
        } finally {
            voteless.close();
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
