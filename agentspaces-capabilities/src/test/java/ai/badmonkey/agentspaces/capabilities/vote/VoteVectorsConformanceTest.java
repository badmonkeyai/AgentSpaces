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

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.membership.MembershipAuthorizer;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The sybil ballot vectors through a real replica and a real tally (SPEC §8
 * QUORUM, QA4 A4-7 phase 2): three peer-signed ballots under three names from
 * one peer count as one voter at PEER granularity and as none at AGENT
 * granularity. The Python and TypeScript clients tally the same bytes to the
 * same numbers in their golden suites.
 */
class VoteVectorsConformanceTest {

    private static final List<Path> GOLDEN_PATHS = List.of(
            Path.of("..", "tools", "golden", "golden.json"),
            Path.of("..", "..", "agentspaces-spec", "golden.json"));
    private static final boolean GOLDEN_REQUIRED = Boolean.getBoolean("golden.required");
    private static Map<String, Object> golden;

    private TestClock clock;
    private final SimNetwork network = new SimNetwork();
    private final List<PeerNode> nodes = new ArrayList<>();
    private GroupId groupId;
    private PeerIdentity receiving;
    private GroupRuntime receiverRuntime;
    private GroupRuntime sender;
    private ReplicatedSpace receiver;

    @BeforeAll
    @SuppressWarnings("unchecked")
    static void loadVectors() throws Exception {
        Path found = GOLDEN_PATHS.stream().filter(Files::exists).findFirst().orElse(null);
        if (GOLDEN_REQUIRED) {
            assertThat(found).as("golden.json is required (golden.required=true) but none of "
                    + GOLDEN_PATHS + " exists").isNotNull();
        }
        assumeTrue(found != null, "golden.json not present");
        golden = new ObjectMapper().readValue(found.toFile(), Map.class);
    }

    @BeforeEach
    void cluster() throws IOException {
        clock = TestClock.startingAt(Instant.parse((String) golden.get("certificate_verify_at")));
        groupId = GroupId.of((String) golden.get("group_id"));
        GroupAdvertisement groupAd = new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1),
                "golden", GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
        receiving = PeerIdentity.generate();
        PeerNode a = node(receiving, 1, "a");
        receiverRuntime = a.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        receiver = ReplicatedSpace.builder(receiverRuntime, "tasks", receiving, "reader")
                .clock(clock).build();
        PeerNode b = node(PeerIdentity.generate(), 2, "b");
        sender = b.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", "a", 0)));
        tickAll(4);
        for (int i = 1; i <= 3; i++) {
            String vector = "vote_three_names_one_peer_delta_" + i;
            sender.gossip().publish("space:tasks", vector,
                    HexFormat.of().parseHex((String) golden.get(vector)));
        }
        tickAll(2);
        assertThat(receiver.knownEntries()).as("all three ballots landed").isEqualTo(3);
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode node(PeerIdentity identity, long seed, String address) throws IOException {
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        nodes.add(node);
        return node;
    }

    private void tickAll(int rounds) {
        for (int i = 0; i < rounds; i++) {
            nodes.forEach(PeerNode::tick);
            clock.advance(Duration.ofSeconds(1));
        }
    }

    @Test
    void threeNamesFromOnePeerTallyAsOneVoterAtPeerGranularity() {
        VoteCapability vote = new VoteCapability(receiver, receiving.agent("reader"),
                receiving.peerId(), clock);
        assertThat(vote.granularity()).isEqualTo(Authorizer.Granularity.PEER);
        assertThat(vote.tally((String) golden.get("vote_three_names_one_peer_proposal")))
                .containsExactly(Map.entry("approve", 1));
    }

    @Test
    void peerAssertedNamesTallyAsNobodyAtAgentGranularity() {
        PeerId goldenPeer = PeerId.of((String) golden.get("peer_id"));
        Set<AgentId> granted = Set.of(new AgentId(goldenPeer, "one"), new AgentId(goldenPeer, "two"),
                new AgentId(goldenPeer, "three"));
        // The golden peer is not a member here, and even admitted its ballots are
        // peer-asserted: under agent granularity they would count for nothing.
        MembershipAuthorizer authorizer = new MembershipAuthorizer(receiverRuntime.membership(),
                receiving.peerId(), Map.of(Authorizer.Operation.VOTE, Set.of()),
                Map.of(Authorizer.Operation.VOTE, granted));
        VoteCapability vote = new VoteCapability(receiver, receiving.agent("reader"),
                receiving.peerId(), clock, VoteCapability.ANY_AUTHENTICATED_ISSUER, authorizer);
        assertThat(vote.granularity()).isEqualTo(Authorizer.Granularity.AGENT);
        assertThat(vote.tally((String) golden.get("vote_three_names_one_peer_proposal"))).isEmpty();
    }
}
