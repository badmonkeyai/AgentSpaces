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

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.capabilities.orderedlog.RaftLog;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.spoilers.Action;
import ai.badmonkey.agentspaces.spoilers.Script;
import ai.badmonkey.agentspaces.spoilers.ScriptRunner;
import ai.badmonkey.agentspaces.spoilers.Trigger;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * C08 of EL-A-03 (srad/agentspaces-event-loop.md §10.33, §10.37): a newcomer admitted, and a stretch applied, while a
 * Raft election runs. Four members on their own loops on one virtual time, three of them Raft voters. The leader
 * falls silent on the log's pipes (EL-SP-10's fault); at that instant both followers' loops are stretched x4 and a
 * fourth member joins through a follower. PREVENTED: a new leader within the election band (the election timer is a
 * fixed period, never stretched, EL-O-08), at most one leader per term, the newcomer in every view within its
 * newcomer round, and no member dropped.
 */
@Timeout(300)
class RaftElectionCombinationSpoilerTest {

    private static final Duration TICK = Duration.ofMillis(250);
    private static final Duration PERIOD = Duration.ofSeconds(1);

    private final SimNetwork network = new SimNetwork();
    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-23T00:00:00Z"));
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zRaftCombo");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zRaftCombo",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "raft-combo",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final List<PeerNode> nodes = new ArrayList<>();

    private record Member(String name, PeerNode node, GroupRuntime runtime, RaftLog raft) {
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(PeerNode::close);
    }

    private PeerNode node(PeerIdentity identity, String name, long seed) throws Exception {
        PeerNode node = PeerNode.builder(identity).clock(time).time(time).randomSeed(seed).build();
        node.listen(network.register(name), name);
        nodes.add(node);
        return node;
    }

    @Test
    void c08ANewcomerAndAStretchDuringARaftElectionElectOneLeaderAndDropNobody() throws Exception {
        List<PeerIdentity> identities = List.of(PeerIdentity.generate(), PeerIdentity.generate(),
                PeerIdentity.generate());
        List<PeerId> ids = identities.stream().map(PeerIdentity::peerId).toList();
        List<Member> members = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            String name = "n" + i;
            PeerNode node = node(identities.get(i), name, 300 + i);
            GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(),
                    i == 0 ? List.of() : List.of(new PeerAdvertisement.Endpoint("mem", "n0", 0)));
            RaftLog raft = new RaftLog(new CapabilityPipes(runtime, codec), codec, ids.get(i), ids, time, 2000 + i,
                    (command, index) -> { });
            DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, time), codec, ids.get(i));
            CapabilityRuntime capabilities = new CapabilityRuntime(runtime, discovery, identities.get(i));
            members.add(new Member(name, node, runtime, raft));
            node.startTicking(TICK);
            capabilities.register(raft);
        }
        for (int i = 0; i < 600 && !members.stream().allMatch(m -> m.runtime().membership().size() == 2); i++) {
            time.advance(TICK);
        }
        Member leader = null;
        for (int i = 0; i < 2_000 && leader == null; i++) {
            time.advance(Duration.ofMillis(10));
            leader = members.stream().filter(m -> m.raft().isLeader()).findFirst().orElse(null);
        }
        assertThat(leader).as("a leader").isNotNull();
        time.advance(Duration.ofSeconds(30));
        assertThat(leader.raft().isLeader()).isTrue();
        long term = leader.raft().term();
        Member silent = leader;
        List<Member> followers = members.stream().filter(m -> m != silent).toList();

        Script script = Script.of(leader.name()).seed(808)
                .rule("leader-silent").when(Trigger.frame(Envelope.Kind.PIPE_DATA).from(leader.name()))
                .act(Action.drop()).always()
                .build();
        ScriptRunner runner = ScriptRunner.attach(network, time, PERIOD, PeerNode.ChannelAuth.SIGNED, script);
        for (Member f : followers) {
            f.node().loop().requestStretch(4); // the stretch lands with the election
        }
        PeerNode newcomer = node(PeerIdentity.generate(), "n3", 399);
        GroupRuntime newRuntime = newcomer.joinGroup(groupAd, GroupMembership.Config.defaults(),
                List.of(new PeerAdvertisement.Endpoint("mem", followers.get(0).name(), 0)));
        newcomer.startTicking(TICK);
        long start = time.millis();
        Map<Long, Set<PeerId>> leadersByTerm = new HashMap<>();
        long elected = -1;
        long admitted = -1;
        int stretched = 0;
        for (int step = 0; step < 3_000; step++) { // 30 s in 10 ms steps
            time.advance(Duration.ofMillis(10));
            if (step % 100 == 99) {
                runner.tick();
            }
            for (Member m : followers) {
                if (m.raft().isLeader()) {
                    leadersByTerm.computeIfAbsent(m.raft().term(), k -> new HashSet<>()).add(m.node().peerId());
                    if (elected < 0 && m.raft().term() > term) {
                        elected = time.millis() - start;
                        stretched = followers.stream().mapToInt(f -> f.node().loop().stretch()).min().orElse(0);
                    }
                }
            }
            if (admitted < 0 && newRuntime.membership().size() == 3
                    && members.stream().allMatch(m -> m.runtime().membership().member(newcomer.peerId()).isPresent())) {
                admitted = time.millis() - start;
            }
        }
        runner.finish();
        System.out.printf("C08: new leader %d ms after the old fell silent, newcomer in every view after %d ms,"
                + " followers stretched x%d, leaders by term %s%n", elected, admitted, stretched, leadersByTerm);
        assertThat(stretched).as("both followers were stretched x4 when the new leader was elected").isEqualTo(4);
        assertThat(elected).as("a new leader within the election band (1.25 to 2.25 s, then a round or two), the"
                + " stretch notwithstanding").isBetween(0L, 6_000L);
        assertThat(leadersByTerm.values()).as("at most one leader per term").allSatisfy(s -> assertThat(s).hasSize(1));
        assertThat(admitted).as("the newcomer in every view within 10 s").isBetween(0L, 10_000L);
        for (Member m : members) {
            assertThat(m.runtime().membership().size()).as("%s dropped nobody", m.name()).isEqualTo(3);
        }
    }
}
