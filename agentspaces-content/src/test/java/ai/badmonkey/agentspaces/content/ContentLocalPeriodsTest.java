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
package ai.badmonkey.agentspaces.content;

import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.tree.MemoryTreeStore;
import ai.badmonkey.agentspaces.content.tree.NodeFixtures;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.VirtualTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * srad/agentspaces-event-loop.md EL-C-04 (§10.35): a challenge's deadline and a tree queue's idle bound are local
 * periods, on the monotonic base, so a wall step back of an hour holds neither. On the wall clock the challenge stayed
 * open, and the queue held, until the wall caught up an hour later.
 */
@Timeout(120)
class ContentLocalPeriodsTest {

    private final VirtualTime time = VirtualTime.startingAt(Instant.parse("2026-11-23T00:00:00Z"));
    private final SimNetwork network = new SimNetwork();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zLocalPeriods");
    private final GroupAdvertisement groupAd = new GroupAdvertisement("aspace://zLocalPeriods",
            PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "content",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final Map<PeerId, CapabilityAdvertisement> ads = new ConcurrentHashMap<>();
    private final List<PeerNode> nodes = new ArrayList<>();
    private final List<Content> contents = new ArrayList<>();

    @AfterEach
    void tearDown() {
        contents.forEach(Content::close);
        nodes.forEach(PeerNode::close);
    }

    private record Peer(PeerNode node, Content content, MemoryTreeStore trees) {
    }

    private Peer peer(String address, long seed, boolean levelTwo, String... seedAddresses) throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(time).time(time).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        BlockExchange blocks = new BlockExchange(runtime, codec, time, 0, BlockExchange.DEFAULT_MAX_BLOCK_BYTES);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "content", identity, "content")
                .clock(time).settleWindow(Duration.ZERO).blocks(blocks).build();
        MemoryTreeStore trees = new MemoryTreeStore();
        Content.Builder b = Content.builder(new CapabilityPipes(runtime, codec), runtime, space, identity.peerId())
                .codec(codec).clock(time).store(new MemoryPieceStore(time, 0)).blocks(blocks).random(new Random(seed))
                .treeStore(trees).advertisements(p -> Optional.ofNullable(ads.get(p)));
        Content content = (levelTwo ? b.pdtLevel(Content.PDT_LEVEL) : b).build();
        ads.put(identity.peerId(), content.describe(groupId));
        content.joinLoop(runtime);
        content.start();
        node.startTicking(Duration.ofMillis(250));
        nodes.add(node);
        contents.add(content);
        return new Peer(node, content, trees);
    }

    @Test
    void aWallStepBackHoldsNeitherAChallengeDeadlineNorATreeQueuesIdleBound() throws Exception {
        Peer store = peer("store", 1, true);
        Peer requester = peer("requester", 2, false, "store");
        time.advance(Duration.ofSeconds(5));
        NodeFixtures.Tree tree = NodeFixtures.tree(40, 2, 3, 200);
        store.trees().putAll(tree);
        List<String> cids = new ArrayList<>(tree.nodes().keySet());
        java.util.Collections.shuffle(cids, new Random(4));
        List<Messages.Pick> picks = cids.subList(0, 3).stream().map(Messages.Pick::node).toList();

        // a tree queue: the store's answer (tag 9, a tree frame) opens one at the requester; then the wall goes back
        CompletableFuture<Content.ChallengeResult> first = requester.content().challenge(store.node().peerId(),
                tree.root(), picks, Duration.ofSeconds(10));
        long guard = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!first.isDone()) {
            assertThat(System.nanoTime() - guard).as("the challenge is answered").isNegative();
            time.advance(Duration.ofMillis(10));
        }
        assertThat(first.get().passed()).as("answered in time: %s", first.get().reason()).isTrue();
        assertThat(requester.content().treeQueuesHeld()).as("the answer opened a queue at the requester").isEqualTo(1);
        time.stepWall(Duration.ofHours(-1)); // both peers' wall clocks go back an hour
        time.advance(Duration.ofSeconds(61));
        assertThat(requester.content().treeQueuesHeld()).as("the idle queue is dropped a minute on, not an hour")
                .isZero();

        // a challenge's deadline: the answer waits on the link, and the wall goes back again
        network.hold("store", "requester");
        CompletableFuture<Content.ChallengeResult> answer = requester.content().challenge(store.node().peerId(),
                tree.root(), picks, Duration.ofSeconds(10));
        time.advance(Duration.ofSeconds(1));
        time.stepWall(Duration.ofHours(-1));
        time.advance(Duration.ofSeconds(10));
        assertThat(answer).as("the 10 s deadline passed on the monotonic base").isDone();
        assertThat(answer.get().passed()).isFalse();
        assertThat(answer.get().reason()).contains("deadline");
        assertThat(answer.get().elapsed()).as("elapsed on the monotonic base").isBetween(10_000L, 11_500L);
        assertThat(store.node().loopStats().backstopFoundWork()).isZero();
        assertThat(requester.node().loopStats().backstopFoundWork()).isZero();
    }
}
