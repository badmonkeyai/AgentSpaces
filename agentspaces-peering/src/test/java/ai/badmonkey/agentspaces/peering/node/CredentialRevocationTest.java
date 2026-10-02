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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.api.ad.CredentialRevocation;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SPEC §5.6 v0.1.13 (TODO-9-10-11 C2–C4): credential revocations across a
 * fleet. An agent revoked by its own peer is refused everywhere, including by
 * a late joiner; a stranger cannot revoke it; and a revoked join credential
 * evicts the member it admitted and admits nobody again.
 */
class CredentialRevocationTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final PeerIdentity founderId = PeerIdentity.generate();

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private GroupAdvertisement group(String name, GroupAdvertisement.MembershipPolicy policy) {
        return new GroupAdvertisement("aspace://" + name, founderId.peerId(), GroupId.of(name),
                java.time.Instant.EPOCH, Duration.ofDays(1), name, policy,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
    }

    private GroupRuntime join(PeerIdentity identity, String address, long seed, GroupAdvertisement groupAd,
                              Map<String, String> hints, String... seedAddresses) throws IOException {
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        nodes.add(node);
        return node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds, hints, null);
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    @Test
    void anAgentRevokedByItsOwnPeerIsRefusedFleetWideAndByLateJoiners() throws Exception {
        GroupAdvertisement open = group("zAgentRevoke", GroupAdvertisement.MembershipPolicy.OPEN);
        PeerIdentity hostId = PeerIdentity.generate();
        GroupRuntime founder = join(founderId, "f", 1, open, Map.of());
        GroupRuntime host = join(hostId, "h", 2, open, Map.of(), "f");
        GroupRuntime stranger = join(PeerIdentity.generate(), "s", 3, open, Map.of(), "f");
        tickAll(6);
        AgentId planner = hostId.agent("planner");
        AgentId sibling = hostId.agent("sibling");

        assertThat(stranger.revokeAgent(planner, CredentialRevocation.RETIRED))
                .as("a stranger has no authority over another peer's agent").isEmpty();
        assertThat(host.revokeAgent(planner, CredentialRevocation.KEY_COMPROMISE)).isPresent();
        tickAll(4);
        for (GroupRuntime member : List.of(founder, host, stranger)) {
            assertThat(member.revocationView().refuses(planner, null, clock.instant())).isTrue();
            assertThat(member.revocationView().refuses(sibling, null, clock.instant())).isFalse();
            assertThat(member.revocationView().revoked(hostId.peerId()))
                    .as("revoking an agent leaves its peer alone").isFalse();
        }
        GroupRuntime late = join(PeerIdentity.generate(), "l", 4, open, Map.of(), "f");
        tickAll(8);
        assertThat(late.revocationView().refuses(planner, null, clock.instant()))
                .as("anti-entropy delivered the agent revocation").isTrue();
        assertThat(founder.membership().member(hostId.peerId())).isPresent();
    }

    @Test
    void aRevokedJoinCredentialEvictsItsMemberAndAdmitsNobodyAgain() throws Exception {
        GroupAdvertisement invite = group("zInviteRevoke", GroupAdvertisement.MembershipPolicy.INVITE);
        PeerIdentity guestId = PeerIdentity.generate();
        PeerIdentity keptId = PeerIdentity.generate();
        String guestCredential = JoinCredentials.issue(founderId, invite.group(), guestId.peerId());
        GroupRuntime founder = join(founderId, "f", 1, invite, Map.of());
        GroupRuntime kept = join(keptId, "k", 2, invite, Map.of(JoinCredentials.HINT_KEY,
                JoinCredentials.issue(founderId, invite.group(), keptId.peerId())), "f");
        join(guestId, "g", 3, invite, Map.of(JoinCredentials.HINT_KEY, guestCredential), "f");
        tickAll(6);
        assertThat(founder.membership().member(guestId.peerId())).isPresent();
        assertThat(kept.membership().member(guestId.peerId())).isPresent();

        assertThat(kept.revokeJoinCredential(guestCredential, CredentialRevocation.PRIVILEGE_WITHDRAWN))
                .as("only the founder revokes a join credential").isEmpty();
        assertThat(founder.revokeJoinCredential(guestCredential, CredentialRevocation.PRIVILEGE_WITHDRAWN))
                .isPresent();
        tickAll(10);
        assertThat(founder.membership().member(guestId.peerId())).as("evicted at the founder").isEmpty();
        assertThat(nodes.get(0).connectedTo(guestId.peerId())).as("and its link cut").isFalse();
        assertThat(nodes.get(0).connectedTo(keptId.peerId())).as("other links stay").isTrue();
        assertThat(kept.membership().member(guestId.peerId())).as("evicted at the other member").isEmpty();
        assertThat(founder.membership().member(keptId.peerId())).as("other invitees stay").isPresent();
        assertThat(founder.revocationView().revoked(guestId.peerId()))
                .as("the peer itself is not revoked: it may be re-invited").isFalse();
    }

    @Test
    void aRevocationCannotTakeEffectInTheFuture() throws Exception {
        GroupAdvertisement open = group("zFutureRevoke", GroupAdvertisement.MembershipPolicy.OPEN);
        GroupRuntime founder = join(founderId, "f", 1, open, Map.of());
        assertThatThrownBy(() -> founder.revoke(CredentialRevocation.Target.agent(founderId.agent("x")),
                CredentialRevocation.RETIRED, clock.instant().plusSeconds(60)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** ASF-047 / SPEC §6.1: a node forwards a revoked peer's verifiable self-ad no further, and a live peer's as before. */
    @Test
    void aRevokedPeersAdIsNotForwarded() throws Exception {
        GroupAdvertisement open = group("zForwardFilter", GroupAdvertisement.MembershipPolicy.OPEN);
        GroupRuntime founder = join(founderId, "f", 1, open, Map.of());
        PeerIdentity victim = PeerIdentity.generate();
        PeerIdentity bystander = PeerIdentity.generate();
        ai.badmonkey.agentspaces.common.codec.CborCodec codec =
                ai.badmonkey.agentspaces.common.codec.CborCodec.defaultCodec();
        java.util.function.Function<PeerIdentity, byte[]> selfAd = peer -> {
            byte[] adBytes = codec.toBytes(new ai.badmonkey.agentspaces.api.ad.PeerAdvertisement(
                    "aspace://" + open.group().value() + "/peer/" + peer.peerId().value(),
                    peer.peerId(), open.group(), clock.instant(), Duration.ofMinutes(10),
                    List.of(new ai.badmonkey.agentspaces.api.ad.PeerAdvertisement.Endpoint("mem", "x", 0)),
                    java.util.Set.of(), Map.of()));
            return codec.toBytes(new PeerNode.SignedPeerAd(adBytes, peer.rawPublicKey(), peer.sign(adBytes)));
        };
        PeerNode node = nodes.get(0);
        assertThat(node.forwardsPeerAd(founder.revocations(), selfAd.apply(victim))).isTrue();

        assertThat(founder.revoke(victim.peerId(), "compromised")).isPresent();
        assertThat(node.forwardsPeerAd(founder.revocations(), selfAd.apply(victim)))
                .as("the revoked peer's ad stops here").isFalse();
        assertThat(node.forwardsPeerAd(founder.revocations(), selfAd.apply(bystander))).isTrue();
    }
}
