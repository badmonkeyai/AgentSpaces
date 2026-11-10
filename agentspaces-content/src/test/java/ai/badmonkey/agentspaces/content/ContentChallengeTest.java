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

import ai.badmonkey.agentspaces.agent.content.Manifest;
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
import ai.badmonkey.agentspaces.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Possession challenges between two content swarms (PDT 2 phase 3, P2-S-07, P2-F-05; srad/agentspaces-pdt2.md
 * §3.2.4, §6.10.2): the feature level travels on the advertisement, tag 12 goes only to a level-2 member, the
 * challenger verifies each answered pick by its CID within the deadline, and a holder that lost nodes, holds
 * none, or answers late fails.
 */
class ContentChallengeTest {

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<Peer> peers = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zChallenge");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zChallenge", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "content",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final Map<PeerId, CapabilityAdvertisement> ads = new ConcurrentHashMap<>();

    private record Peer(String name, PeerNode node, Content content, MemoryTreeStore trees) {
        PeerId id() {
            return node.peerId();
        }
    }

    @AfterEach
    void tearDown() {
        peers.forEach(p -> {
            p.content().close();
            p.node().close();
        });
    }

    private Peer newPeer(String address, long seed, UnaryOperator<Content.Builder> customise, String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        BlockExchange blocks = new BlockExchange(runtime, codec, clock, 0, BlockExchange.DEFAULT_MAX_BLOCK_BYTES);
        ReplicatedSpace space = ReplicatedSpace.builder(runtime, "content", identity, "content")
                .clock(clock).settleWindow(Duration.ZERO).blocks(blocks).build();
        MemoryTreeStore trees = new MemoryTreeStore();
        Content content = customise.apply(Content.builder(new CapabilityPipes(runtime, codec), runtime, space, identity.peerId())
                .codec(codec).clock(clock).store(new MemoryPieceStore(clock, 0)).blocks(blocks).random(new Random(seed))
                .treeStore(trees).advertisements(peer -> Optional.ofNullable(ads.get(peer)))).build();
        ads.put(identity.peerId(), content.describe(groupId));
        runtime.onTick(content::tick);
        content.start();
        Peer peer = new Peer(address, node, content, trees);
        peers.add(peer);
        return peer;
    }

    private void tickAll() {
        peers.forEach(p -> p.node().tick());
        clock.advance(Duration.ofMillis(250));
    }

    private boolean runUntil(Duration max, BooleanSupplier until) throws InterruptedException {
        long deadline = System.nanoTime() + max.toNanos();
        while (!until.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            tickAll();
            Thread.sleep(2);
        }
        return true;
    }

    private Content.ChallengeResult await(CompletableFuture<Content.ChallengeResult> answer) throws Exception {
        assertThat(runUntil(Duration.ofSeconds(20), answer::isDone)).as("the challenge settles").isTrue();
        return answer.get();
    }

    private static List<Messages.Pick> picks(NodeFixtures.Tree tree, int n, long seed) {
        List<String> cids = new ArrayList<>(tree.nodes().keySet());
        java.util.Collections.shuffle(cids, new Random(seed));
        return cids.subList(0, n).stream().map(Messages.Pick::node).toList();
    }

    @Test
    void theFeatureLevelTravelsOnTheAdvertisementAndALevelTwoHolderProvesEveryPick() throws Exception {
        Peer store = newPeer("store", 1, b -> b.pdtLevel(Content.PDT_LEVEL));
        Peer requester = newPeer("requester", 2, b -> b, "store");
        settle();
        assertThat(store.content().describe(groupId).parameters())
                .containsEntry(Content.PDT_PARAMETER, "2").containsEntry(Content.TREES_PARAMETER, "1");
        assertThat(requester.content().describe(groupId).parameters()).as("level 1 carries no pdt parameter")
                .doesNotContainKey(Content.PDT_PARAMETER);
        assertThat(requester.content().pdtLevel(store.id())).isEqualTo(2);
        assertThat(store.content().pdtLevel(requester.id())).isEqualTo(1);

        NodeFixtures.Tree tree = NodeFixtures.tree(31, 3, 4, 500);
        store.trees().putAll(tree);
        Content.ChallengeResult result = await(requester.content().challenge(store.id(), tree.root(),
                picks(tree, Messages.MAX_PICKS, 7), Duration.ofSeconds(2)));
        assertThat(result.passed()).as(result.toString()).isTrue();
        assertThat(result.proven()).isEqualTo(Messages.MAX_PICKS);
        assertThat(result.reason()).isNull();
        assertThat(store.content().challengesAnswered(tree.root())).isEqualTo(1);
    }

    @Test
    void aHolderThatLostAPickOrHoldsNoneFailsAndALevelOneMemberIsNeverAsked() throws Exception {
        Peer store = newPeer("store", 3, b -> b.pdtLevel(Content.PDT_LEVEL));
        Peer old = newPeer("old", 4, b -> b, "store");
        Peer requester = newPeer("requester", 5, b -> b, "store");
        settle();
        NodeFixtures.Tree tree = NodeFixtures.tree(32, 3, 4, 500);
        store.trees().putAll(tree);
        List<Messages.Pick> chosen = picks(tree, 8, 9);
        store.trees().remove(chosen.get(3).cid());
        Content.ChallengeResult lost = await(requester.content().challenge(store.id(), tree.root(), chosen, Duration.ofSeconds(2)));
        assertThat(lost.passed()).isFalse();
        assertThat(lost.reason()).contains("not held");
        assertThat(lost.proven()).isEqualTo(7);
        assertThat(store.content().challengesAnswered(tree.root())).as("a partial answer is no pass in the rows").isZero();

        NodeFixtures.Tree other = NodeFixtures.tree(33, 2, 2, 100);
        Content.ChallengeResult none = await(requester.content().challenge(store.id(), other.root(), picks(other, 2, 1),
                Duration.ofSeconds(2)));
        assertThat(none.passed()).isFalse();
        assertThat(none.reason()).contains("REJECT reason 1");

        old.trees().putAll(tree);
        Content.ChallengeResult levelOne = requester.content().challenge(old.id(), tree.root(), chosen, Duration.ofSeconds(2)).get();
        assertThat(levelOne.passed()).isFalse();
        assertThat(levelOne.reason()).as("tag 12 goes only to a level-2 member (rule 15)").contains("level 1");
    }

    @Test
    void anAnswerAfterTheDeadlineFails() throws Exception {
        Peer store = newPeer("store", 6, b -> b.pdtLevel(Content.PDT_LEVEL));
        Peer requester = newPeer("requester", 7, b -> b, "store");
        settle();
        NodeFixtures.Tree tree = NodeFixtures.tree(34, 2, 3, 200);
        store.trees().putAll(tree);
        network.hold("store", "requester");
        CompletableFuture<Content.ChallengeResult> answer = requester.content().challenge(store.id(), tree.root(),
                picks(tree, 4, 3), Duration.ofSeconds(2));
        for (int i = 0; i < 12 && !answer.isDone(); i++) {
            tickAll();
            Thread.sleep(5);
        }
        network.release("store", "requester", SimNetwork.Order.FIFO, 1);
        Content.ChallengeResult late = await(answer);
        assertThat(late.passed()).isFalse();
        assertThat(late.reason()).contains("deadline");
    }

    @Test
    void anAnswerThatArrivesAfterTheDeadlineButBeforeTheTickFails() throws Exception {
        // a store that fetches the picks on demand answers late; the challenger's tick has not run yet
        Peer store = newPeer("store", 10, b -> b.pdtLevel(Content.PDT_LEVEL));
        Peer requester = newPeer("requester", 11, b -> b, "store");
        settle();
        NodeFixtures.Tree tree = NodeFixtures.tree(35, 2, 3, 200);
        store.trees().putAll(tree);
        network.hold("store", "requester");
        CompletableFuture<Content.ChallengeResult> answer = requester.content().challenge(store.id(), tree.root(),
                picks(tree, 4, 5), Duration.ofSeconds(2));
        for (int i = 0; i < 8 && network.heldCount("store", "requester") == 0; i++) {
            peers.forEach(p -> p.node().tick()); // the clock stands still, so the deadline does not pass here
            Thread.sleep(5);
        }
        assertThat(network.heldCount("store", "requester")).as("the answer waits on the link").isPositive();
        assertThat(answer).isNotDone();
        clock.advance(Duration.ofSeconds(3));
        network.release("store", "requester", SimNetwork.Order.FIFO, 1);
        long until = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!answer.isDone() && System.nanoTime() < until) {
            Thread.sleep(5); // the frames arrive on the receiver's thread; no peer ticks here
        }
        assertThat(answer).as("the answer settles the challenge without a tick").isDone();
        assertThat(answer.get().passed()).isFalse();
        assertThat(answer.get().reason()).contains("deadline");
    }

    @Test
    void anArtifactIsChallengedByThePiecesOfItsTable() throws Exception {
        Peer store = newPeer("store", 8, b -> b.pdtLevel(Content.PDT_LEVEL));
        Peer requester = newPeer("requester", 9, b -> b, "store");
        settle();
        byte[] bytes = new byte[5 * 65_536 + 17];
        new Random(5).nextBytes(bytes);
        Manifest manifest = store.content().publish(bytes, PublishOptions.defaults().withPieceSize(65_536));
        assertThat(runUntil(Duration.ofSeconds(10), () -> requester.content().spaces().manifest(manifest.root()).isPresent())).isTrue();
        List<String> pieces = requester.content().pieceCids(manifest.root());
        assertThat(pieces).hasSize(6);
        Content.ChallengeResult result = await(requester.content().challenge(store.id(), manifest.root(),
                List.of(Messages.Pick.node(pieces.get(1)), Messages.Pick.node(pieces.get(5))), Duration.ofSeconds(2)));
        assertThat(result.passed()).as(result.toString()).isTrue();
        assertThat(result.proven()).isEqualTo(2);
    }

    private void settle() {
        for (int i = 0; i < 6; i++) {
            tickAll();
        }
    }
}
