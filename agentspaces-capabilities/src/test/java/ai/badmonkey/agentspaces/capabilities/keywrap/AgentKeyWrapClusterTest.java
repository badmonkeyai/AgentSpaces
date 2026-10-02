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
package ai.badmonkey.agentspaces.capabilities.keywrap;

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.spi.AgentIdentity;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.crypto.GroupKey;
import ai.badmonkey.agentspaces.common.crypto.GroupKeyRing;
import ai.badmonkey.agentspaces.common.crypto.X25519;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SPEC §11a.2 v0.1.13 (TODO-9-10-11 D5): per-agent key wrap. An agent asks
 * for itself with a certificate that certifies its X25519 key; the holder
 * judges the agent (not merely its peer) and seals to the certified key. Here
 * the requesting peer is deliberately not a key holder, so anything it
 * receives was granted at agent granularity.
 */
class AgentKeyWrapClusterTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final PeerIdentity founderId = PeerIdentity.generate();
    private final PeerIdentity requesterId = PeerIdentity.generate();
    private final GroupId groupId = GroupId.of("zAgentWrap");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zAgentWrap", founderId.peerId(), groupId, java.time.Instant.EPOCH,
            Duration.ofDays(1), "agent wrap", GroupAdvertisement.MembershipPolicy.OPEN,
            ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
    private final Set<String> granted = ConcurrentHashMap.newKeySet();
    private final Set<AgentId> revoked = ConcurrentHashMap.newKeySet();

    private record Member(PeerIdentity identity, GroupKeyDistributor keys, GroupKeyRing ring,
                          GroupRuntime runtime) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private Member member(PeerIdentity identity, String address, long seed, GroupKey epochZero,
                          Predicate<ai.badmonkey.agentspaces.common.id.PeerId> peers,
                          String... seedAddresses) throws IOException {
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        DiscoveryService discovery = DiscoveryService.create(runtime, codec, identity.peerId(),
                clock, Set.of());
        GroupKeyRing ring = GroupKeyRing.of(epochZero);
        GroupKeyDistributor keys = new GroupKeyDistributor(new CapabilityPipes(runtime, codec),
                identity.peerId(), codec, clock);
        keys.serve(ring, peers);
        keys.agentsAllowed(agent -> granted.contains(agent.localName()));
        keys.refuseAgents(revoked::contains);
        keys.follow(ring, discovery);
        new CapabilityRuntime(runtime, discovery, identity).register(keys);
        nodes.add(node);
        return new Member(identity, keys, ring, runtime);
    }

    private void tickAll(int rounds) throws InterruptedException {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
            Thread.sleep(20);
        }
    }

    private AgentIdentity agentOf(PeerIdentity peer, String name) {
        return peer.renewingSubordinate(name, Ed25519.generate(), X25519.generate(),
                Duration.ofDays(1), clock);
    }

    /** A founder holding epoch 1, and a requester whose peer the founder will not serve. */
    private Member[] fleet() throws Exception {
        GroupKey epochZero = GroupKey.generate();
        Member founder = member(founderId, "f", 1, epochZero, peer -> false);
        Member requester = member(requesterId, "r", 2, epochZero, peer -> false, "f");
        tickAll(4);
        assertThat(founder.keys().rotate(founderId, clock.instant())).isEqualTo(1);
        tickAll(4);
        assertThat(requester.ring().epochs()).as("the peer itself is no key holder").containsExactly(0L);
        return new Member[] {founder, requester};
    }

    @Test
    void aGrantedAgentIsServedUnderItsOwnKeyAndTheEpochIsInstalled() throws Exception {
        Member[] fleet = fleet();
        granted.add("planner");
        AgentIdentity planner = agentOf(requesterId, "planner");
        List<GroupKeyRing.Held> received = fleet[1].keys().requestAs(requesterId, planner,
                founderId.peerId(), List.of(1L), Duration.ofSeconds(2));
        assertThat(received).extracting(GroupKeyRing.Held::epoch).containsExactly(1L);
        assertThat(received.get(0).key().rawBytes()).as("the founder's epoch-1 key")
                .isEqualTo(fleet[0].ring().openingKeys(1).get(0).rawBytes());
        assertThat(fleet[1].ring().epochs()).containsExactly(0L, 1L);
    }

    @Test
    void anUngrantedSiblingAgentIsRefused() throws Exception {
        Member[] fleet = fleet();
        granted.add("planner");
        AgentIdentity sibling = agentOf(requesterId, "intern");
        assertThat(fleet[1].keys().requestAs(requesterId, sibling, founderId.peerId(),
                List.of(1L), Duration.ofSeconds(2))).isEmpty();
        assertThat(fleet[1].ring().epochs()).containsExactly(0L);
    }

    @Test
    void anAgentCertifiedByAnotherPeerIsRefused() throws Exception {
        Member[] fleet = fleet();
        granted.add("planner");
        // A granted name, but certified by a peer other than the sender: the
        // certificate cannot be judged against the authenticated sender's key.
        AgentIdentity foreign = agentOf(PeerIdentity.generate(), "planner");
        assertThat(fleet[1].keys().requestAs(requesterId, foreign, founderId.peerId(),
                List.of(1L), Duration.ofSeconds(2))).isEmpty();
    }

    @Test
    void anAgentWithoutACertifiedEncryptionKeyCannotAsk() throws Exception {
        Member[] fleet = fleet();
        AgentIdentity signingOnly = requesterId.renewingSubordinate("planner", Duration.ofDays(1), clock);
        assertThatThrownBy(() -> fleet[1].keys().requestAs(requesterId, signingOnly,
                founderId.peerId(), List.of(1L), Duration.ofSeconds(2)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("encryption key");
    }

    /** SPEC §5.6 v0.1.13 (C3): a group revocation of the agent, by its own peer, reaches the holder's check. */
    @Test
    void anAgentRevokedInTheGroupIsServedNothing() throws Exception {
        Member[] fleet = fleet();
        granted.add("planner");
        AgentIdentity planner = agentOf(requesterId, "planner");
        assertThat(fleet[1].runtime().revokeAgent(planner.id(),
                ai.badmonkey.agentspaces.api.ad.CredentialRevocation.KEY_COMPROMISE)).isPresent();
        tickAll(4);
        assertThat(fleet[0].runtime().revocationView().revoked(planner.id(), null)).isTrue();
        assertThat(fleet[1].keys().requestAs(requesterId, planner, founderId.peerId(),
                List.of(1L), Duration.ofSeconds(2))).isEmpty();
    }

    @Test
    void aRevokedAgentIsCutOffFromEpochsMintedAfterItsRevocation() throws Exception {
        Member[] fleet = fleet();
        granted.add("planner");
        AgentIdentity planner = agentOf(requesterId, "planner");
        assertThat(fleet[1].keys().requestAs(requesterId, planner, founderId.peerId(),
                List.of(1L), Duration.ofSeconds(2))).hasSize(1);

        revoked.add(planner.id());
        assertThat(fleet[0].keys().rotate(founderId, clock.instant())).isEqualTo(2);
        tickAll(4);
        assertThat(fleet[1].keys().requestAs(requesterId, planner, founderId.peerId(),
                List.of(2L), Duration.ofSeconds(2))).as("revoked, whatever the grant").isEmpty();
        assertThat(fleet[1].ring().epochs()).as("keeps what it had, gains nothing new")
                .containsExactly(0L, 1L);
    }
}
