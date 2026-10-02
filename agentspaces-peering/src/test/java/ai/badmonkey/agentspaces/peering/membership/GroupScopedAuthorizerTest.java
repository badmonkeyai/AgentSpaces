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

import ai.badmonkey.agentspaces.api.ad.CredentialRevocation;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SPEC §11 v0.1.13 (review M-4): an authorizer shared across groups, such as
 * the identity provider's, is scoped per group. It refuses peers and agents the
 * group revoked and, when membership is required, peers outside the group,
 * however permissive the wrapped authorizer is.
 */
class GroupScopedAuthorizerTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final PeerIdentity founderId = PeerIdentity.generate();
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zScoped",
            founderId.peerId(), GroupId.of("zScoped"), java.time.Instant.EPOCH, Duration.ofDays(1),
            "scoped", GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    /** An identity provider that permits every token holder everything, in every group. */
    private final Authorizer permissive = (peer, operation, scope) -> true;

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private GroupRuntime join(PeerIdentity identity, String address, String... seeds) throws IOException {
        PeerNode node = PeerNode.builder(identity).clock(clock).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String s : seeds) {
            endpoints.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        nodes.add(node);
        return node.joinGroup(groupAd, GroupMembership.Config.defaults(), endpoints);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    @Test
    void aRevokedPeerOrAgentAndANonMemberAreRefusedWhateverTheTokenSays() throws Exception {
        GroupRuntime founder = join(founderId, "f");
        PeerIdentity memberId = PeerIdentity.generate();
        join(memberId, "m", "f");
        PeerIdentity victimId = PeerIdentity.generate();
        join(victimId, "v", "f");
        tickAll(6);
        Authorizer scoped = new GroupScopedAuthorizer(permissive, founder.revocationView(),
                founder.membership(), founderId.peerId());
        PeerId outsider = PeerIdentity.generate().peerId();

        assertThat(scoped.permits(memberId.peerId(), Authorizer.Operation.VOTE, "votes")).isTrue();
        assertThat(scoped.permits(founderId.peerId(), Authorizer.Operation.VOTE, "votes"))
                .as("this node is a member of its own group").isTrue();
        assertThat(scoped.permits(outsider, Authorizer.Operation.VOTE, "votes"))
                .as("a valid token from outside the group grants nothing here").isFalse();

        founder.revoke(victimId.peerId(), "compromised");
        founder.revokeAgent(memberId.agent("rogue"), CredentialRevocation.PRIVILEGE_WITHDRAWN);
        assertThat(scoped.permits(victimId.peerId(), Authorizer.Operation.VOTE, "votes")).isFalse();
        assertThat(scoped.permits(memberId.agent("rogue"), Authorizer.Operation.VOTE, "votes")).isFalse();
        assertThat(scoped.permits(memberId.agent("honest"), Authorizer.Operation.VOTE, "votes")).isTrue();

        Authorizer withoutMembership = new GroupScopedAuthorizer(permissive, founder.revocationView(),
                null, founderId.peerId());
        assertThat(withoutMembership.permits(outsider, Authorizer.Operation.VOTE, "votes"))
                .as("membership is the wrapped authorizer's call when not required").isTrue();
        assertThat(withoutMembership.permits(victimId.peerId(), Authorizer.Operation.VOTE, "votes")).isFalse();
    }
}
