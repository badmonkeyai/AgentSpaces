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
package ai.badmonkey.agentspaces.peering.membership;

import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.spi.Authorizer.Granularity;
import ai.badmonkey.agentspaces.api.spi.Authorizer.Operation;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The default authorization posture: admission is authority, with optional
 * explicit grants narrowing individual operations to named peers.
 */
class MembershipAuthorizerTest {

    private final TestClock clock = TestClock.create();
    private final PeerId self = PeerIdentity.generate().peerId();
    private final PeerId member = PeerIdentity.generate().peerId();
    private final PeerId console = PeerIdentity.generate().peerId();
    private final PeerId stranger = PeerIdentity.generate().peerId();
    private final GroupId group = GroupId.of("zAuthz");
    private final GroupMembership membership = new GroupMembership(
            self, clock, new Random(7), GroupMembership.Config.defaults());

    private void admit(PeerId peer) {
        membership.onPeerAdvertisement(new PeerAdvertisement(
                "aspace://" + group.value() + "/peer/" + peer.value(), peer, group,
                clock.instant(), Duration.ofMinutes(10),
                List.of(new PeerAdvertisement.Endpoint("mem", "x", 0)),
                Set.of(), Map.of()));
    }

    @Test
    void admittedMembersMayActAndStrangersMayNot() {
        admit(member);
        MembershipAuthorizer authorizer = new MembershipAuthorizer(membership, self);

        assertThat(authorizer.permits(member, Operation.RAFT_VOTER, "")).isTrue();
        assertThat(authorizer.permits(self, Operation.RAFT_VOTER, "")).isTrue();
        assertThat(authorizer.permits(stranger, Operation.RAFT_VOTER, "")).isFalse();
    }

    @Test
    void explicitGrantsNarrowAnOperationToNamedAdmittedPeers() {
        admit(member);
        admit(console);
        MembershipAuthorizer authorizer = new MembershipAuthorizer(membership, self,
                Map.of(Operation.DIRECTIVE_ISSUER, Set.of(console)));

        assertThat(authorizer.permits(console, Operation.DIRECTIVE_ISSUER, "")).isTrue();
        assertThat(authorizer.permits(member, Operation.DIRECTIVE_ISSUER, ""))
                .as("admitted but not granted").isFalse();
        assertThat(authorizer.permits(member, Operation.RAFT_VOTER, ""))
                .as("ungranted operations stay membership-wide").isTrue();
        assertThat(authorizer.permits(stranger, Operation.DIRECTIVE_ISSUER, ""))
                .as("a grant never outlives admission").isFalse();
    }

    /** Spec §11 / v0.1.9: a granted peer loses its grants with its membership when evicted. */
    @Test
    void aGrantedPeerLosesItsGrantsWhenEvicted() {
        admit(console);
        MembershipAuthorizer authorizer = new MembershipAuthorizer(membership, self,
                Map.of(Operation.DIRECTIVE_ISSUER, Set.of(console)));
        assertThat(authorizer.permits(console, Operation.DIRECTIVE_ISSUER, "")).isTrue();

        membership.evict(console);

        assertThat(authorizer.permits(console, Operation.DIRECTIVE_ISSUER, ""))
                .as("the explicit grant is void without admission").isFalse();
        assertThat(authorizer.permits(console, Operation.RAFT_VOTER, ""))
                .as("and so is every membership-wide default").isFalse();

        admit(console);
        assertThat(authorizer.permits(console, Operation.DIRECTIVE_ISSUER, ""))
                .as("re-admission restores the standing grant").isTrue();
    }

    // ------------------------------------------------ QA4 A4-7 phase 2: agent grants

    /** Without agent grants the authorizer speaks per peer and delegates the agent question to the peer. */
    @Test
    void peerGrantsAnswerPerPeerAndAgentsInheritTheirPeer() {
        admit(member);
        MembershipAuthorizer authorizer = new MembershipAuthorizer(membership, self,
                Map.of(Operation.VOTE, Set.of(member)));
        assertThat(authorizer.granularity(Operation.VOTE, "votes")).isEqualTo(Granularity.PEER);
        assertThat(authorizer.permits(new AgentId(member, "any"), Operation.VOTE, "votes")).isTrue();
        assertThat(authorizer.permits(new AgentId(console, "any"), Operation.VOTE, "votes")).isFalse();
    }

    /** An AgentId grant names one agent, switches the operation to AGENT granularity, and permits its peer at peer level. */
    @Test
    void agentGrantsNameOneAgentAndSwitchTheOperationToAgentGranularity() {
        admit(member);
        AgentId named = new AgentId(member, "auditor");
        MembershipAuthorizer authorizer = new MembershipAuthorizer(membership, self,
                Map.of(Operation.VOTE, Set.of()), Map.of(Operation.VOTE, Set.of(named)));
        assertThat(authorizer.granularity(Operation.VOTE, "votes")).isEqualTo(Granularity.AGENT);
        assertThat(authorizer.granularity(Operation.RAFT_VOTER, "")).isEqualTo(Granularity.PEER);
        assertThat(authorizer.permits(named, Operation.VOTE, "votes")).isTrue();
        assertThat(authorizer.permits(new AgentId(member, "clerk"), Operation.VOTE, "votes"))
                .as("a sibling on the same peer is not named").isFalse();
        assertThat(authorizer.permits(member, Operation.VOTE, "votes"))
                .as("the peer hosting a granted agent passes the peer-level question").isTrue();
        assertThat(authorizer.permits(console, Operation.VOTE, "votes")).isFalse();
        membership.evict(member);
        assertThat(authorizer.permits(named, Operation.VOTE, "votes"))
                .as("an agent grant never outlives its peer's admission").isFalse();
    }

    /** Grant lists mix bare PeerIds and peer/localName AgentIds; a bare PeerId admits every agent on it. */
    @Test
    void parsingAcceptsPeerIdsAndAgentIdsAlike() {
        admit(member);
        admit(console);
        AgentId named = new AgentId(member, "auditor");
        MembershipAuthorizer authorizer = MembershipAuthorizer.parsing(membership, self,
                Map.of(Operation.VOTE, List.of(console.value(), " " + named.encoded() + " ", "")));
        assertThat(authorizer.granularity(Operation.VOTE, "votes")).isEqualTo(Granularity.AGENT);
        assertThat(authorizer.permits(named, Operation.VOTE, "votes")).isTrue();
        assertThat(authorizer.permits(new AgentId(console, "whoever"), Operation.VOTE, "votes"))
                .as("a bare PeerId grant admits every agent on that peer").isTrue();
        assertThat(authorizer.permits(new AgentId(member, "clerk"), Operation.VOTE, "votes")).isFalse();
        assertThatThrownBy(() -> MembershipAuthorizer.parsing(membership, self,
                Map.of(Operation.VOTE, List.of(member.value() + "/"))))
                .as("a slash makes it an AgentId, and an AgentId needs a name")
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * SPEC §11 v0.1.13: KEY_ROTATE needs an explicit grant at agent level as at
     * peer level, and an operation granted only to agents (through the
     * constructor, not just parsing) stays narrowed for every other agent and peer.
     */
    @Test
    void keyRotateNeedsAGrantAndAgentOnlyGrantsNarrowTheOperation() {
        admit(member);
        admit(console);
        MembershipAuthorizer open = new MembershipAuthorizer(membership, self);
        assertThat(open.permits(member, Operation.KEY_ROTATE, "g")).isFalse();
        assertThat(open.permits(new AgentId(member, "clerk"), Operation.KEY_ROTATE, "g"))
                .as("no grant at agent level either").isFalse();
        assertThat(open.permits(new AgentId(member, "clerk"), Operation.VOTE, "g"))
                .as("other operations stay membership-wide").isTrue();

        AgentId auditor = new AgentId(member, "auditor");
        MembershipAuthorizer agentOnly = new MembershipAuthorizer(membership, self, Map.of(),
                Map.of(Operation.VOTE, Set.of(auditor)));
        assertThat(agentOnly.permits(auditor, Operation.VOTE, "votes")).isTrue();
        assertThat(agentOnly.permits(new AgentId(member, "clerk"), Operation.VOTE, "votes"))
                .as("a sibling of the granted agent").isFalse();
        assertThat(agentOnly.permits(new AgentId(console, "x"), Operation.VOTE, "votes")).isFalse();
        assertThat(agentOnly.permits(console, Operation.VOTE, "votes"))
                .as("a peer hosting no granted agent").isFalse();
        assertThat(agentOnly.permits(member, Operation.VOTE, "votes"))
                .as("the granted agent's peer, at peer level").isTrue();
    }
}
