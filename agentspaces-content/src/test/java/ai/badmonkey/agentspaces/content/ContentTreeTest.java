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

import ai.badmonkey.agentspaces.agent.content.Holding;
import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.GroupKey;
import ai.badmonkey.agentspaces.common.crypto.GroupKeyRing;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.tree.MemoryTreeStore;
import ai.badmonkey.agentspaces.content.tree.NodeFixtures;
import ai.badmonkey.agentspaces.content.tree.TreeFetch;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The tree exchange over the in-memory fabric (srad/agentspaces-pdt.md §3.5,
 * scenarios 2 and 4 of §2.2, S2, S3, and S7 of §6.4). Nodes use the test format
 * of {@code NodeFixtures}; each peer serves and fetches through a
 * {@code MemoryTreeStore}, and the members' advertisements come from one shared
 * lookup, so a 0.5.0 shape without {@code trees} is one entry of it.
 */
class ContentTreeTest {

    private static final int LEAF = 2_000;
    private static final int DEPTH = 4;
    private static final int FANOUT = 4;

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<Peer> peers = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zTreeContent");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zTreeContent", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "content",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final Map<PeerId, CapabilityAdvertisement> ads = new ConcurrentHashMap<>();
    private final Set<String> dead = new HashSet<>();

    private record Peer(String name, PeerNode node, GroupRuntime runtime, ReplicatedSpace space,
                        Content content, MemoryTreeStore trees) {
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

    private Peer newPeer(String address, long seed, String... seedAddresses) throws IOException {
        return newPeer(address, seed, b -> b, false, seedAddresses);
    }

    private Peer newPeer(String address, long seed, java.util.function.UnaryOperator<Content.Builder> customise,
                         boolean legacy, String... seedAddresses) throws IOException {
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
        CapabilityAdvertisement ad = content.describe(groupId);
        if (legacy) {
            Map<String, String> without = new HashMap<>(ad.parameters());
            without.remove(Content.TREES_PARAMETER);
            ad = new CapabilityAdvertisement(ad.id(), ad.issuer(), ad.group(), ad.issued(), ad.ttl(), ad.capabilityType(),
                    ad.version(), ad.binding(), Map.copyOf(without), ad.costHints());
        }
        ads.put(identity.peerId(), ad);
        runtime.onTick(content::tick);
        content.start();
        Peer peer = new Peer(address, node, runtime, space, content, trees);
        peers.add(peer);
        return peer;
    }

    private void kill(Peer peer) {
        dead.add(peer.name());
        peer.content().close();
        peer.node().close();
    }

    private void tickAll() {
        for (Peer p : peers) {
            if (!dead.contains(p.name())) {
                p.node().tick();
            }
        }
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

    private static NodeFixtures.Tree v0() {
        return NodeFixtures.tree(11, DEPTH, FANOUT, LEAF);
    }

    /** v0 with its first thirteen leaves changed: 20 of 341 nodes differ, so 94 % are shared. */
    private static NodeFixtures.Tree v1() {
        Set<Integer> changed = new HashSet<>();
        for (int i = 0; i < 13; i++) {
            changed.add(i);
        }
        return NodeFixtures.tree(11, DEPTH, FANOUT, LEAF, changed, 99);
    }

    private static int holdersComplete(Peer at, String root) {
        int n = 0;
        for (Holding h : at.content().spaces().holdings(root)) {
            if (h.complete()) {
                n++;
            }
        }
        return n;
    }

    // ------------------------------------------------------------ scenarios

    @Test
    void aHeldBaseMakesTheFetchCostItsDeltaAndTheHoldingSaysComplete() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        NodeFixtures.Tree v0 = v0();
        NodeFixtures.Tree v1 = v1();
        assertThat(v1.nodesNotIn(v0)).isEqualTo(20);
        a.trees().putAll(v0).putAll(v1);
        a.content().holdTree(v0.root());
        a.content().holdTree(v1.root());
        b.trees().putAll(v0);

        // PD-S-06: the holding reaches b's replica with have null and complete true; no manifest exists
        assertThat(runUntil(Duration.ofSeconds(10), () -> holdersComplete(b, v1.root()) == 1)).isTrue();
        Holding held = b.content().spaces().holdings(v1.root()).get(0);
        assertThat(held.holder()).isEqualTo(a.id());
        assertThat(held.have()).isNull();
        assertThat(held.complete()).isTrue();
        assertThat(held.count()).isEqualTo(v1.nodes().size());
        assertThat(b.content().describe(v1.root())).as("a tree root has no manifest").isEmpty();
        assertThat(b.content().list(10)).isEmpty();

        TreeFetch fetch = b.content().fetchTree(v1.root(), List.of(v0.root()), FetchOptions.defaults());
        assertThat(runUntil(Duration.ofSeconds(20), fetch::complete)).as(fetch.trace().toString()).isTrue();
        assertThat(b.trees().complete(v1.root())).isTrue();
        long delta = v1.bytesNotIn(v0);
        TreeFetch.Progress p = fetch.progress();
        System.out.printf("PD-Q-REL-03 shape: %d bytes received for a delta of %d bytes (%d nodes of %d), %d requests, %d round trips%n",
                p.bytesReceived(), delta, v1.nodesNotIn(v0), v1.nodes().size(), p.requestsSent(), p.roundTrips());
        assertThat(p.bytesReceived()).isGreaterThanOrEqualTo(delta).isLessThanOrEqualTo((long) (1.5 * delta));
        assertThat(p.roundTrips()).as("PD-Q-PRF-06: the root, then one walk per child").isLessThanOrEqualTo(2);
        assertThat(p.bytesBySource()).containsKey(a.id());
        assertThat(a.content().stats().nodesServed()).isGreaterThanOrEqualTo(20);
        assertThat(b.content().stats().nodesReceived()).isGreaterThanOrEqualTo(20);

        // b completed, so it holds and serves v1 now
        assertThat(runUntil(Duration.ofSeconds(10), () -> holdersComplete(a, v1.root()) == 2)).isTrue();
        assertThat(b.content().treeRoots()).contains(v1.root());
        b.content().releaseTree(v1.root());
        assertThat(b.content().treeRoots()).doesNotContain(v1.root());
    }

    @Test
    void withoutAHeldBaseTheHolderSendsTopLevelsAndTheFetchFinishesInDepthPlusOneRoundTrips() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        NodeFixtures.Tree v0 = v0();
        NodeFixtures.Tree v1 = v1();
        a.trees().putAll(v1);
        a.content().holdTree(v1.root());
        assertThat(runUntil(Duration.ofSeconds(10), () -> holdersComplete(b, v1.root()) == 1)).isTrue();

        TreeFetch fetch = b.content().fetchTree(v1.root(), List.of(v0.root()), FetchOptions.defaults());
        assertThat(runUntil(Duration.ofSeconds(20), fetch::complete)).as(fetch.trace().toString()).isTrue();
        assertThat(b.trees().complete(v1.root())).isTrue();
        assertThat(b.trees().size()).isEqualTo(v1.nodes().size());
        TreeFetch.Progress p = fetch.progress();
        System.out.printf("PD-Q-PRF-06: depth %d tree of %d nodes without a held base: %d round trips, %d requests%n",
                DEPTH, v1.nodes().size(), p.roundTrips(), p.requestsSent());
        assertThat(p.roundTrips()).isLessThanOrEqualTo(DEPTH + 1);
        assertThat(p.nodesVerified()).isEqualTo(v1.nodes().size());
        assertThat(fetch.trace()).anySatisfy(line -> assertThat(line).contains("ended (3)"));
    }

    @Test
    void aFetchFromThreeHoldersCompletesWhenOneDiesPartWay() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        Peer c = newPeer("c", 3, "a");
        Peer d = newPeer("d", 4, "a");
        for (int i = 0; i < 6; i++) {
            tickAll();
        }
        NodeFixtures.Tree v1 = v1();
        for (Peer holder : List.of(a, b, c)) {
            holder.trees().putAll(v1);
            holder.content().holdTree(v1.root());
        }
        assertThat(runUntil(Duration.ofSeconds(10), () -> holdersComplete(d, v1.root()) == 3)).isTrue();
        // PD-Q-REL-04: a is the preferred holder, so it answers the root and walks two of the four subtrees.
        // Its frames to d queue in the fabric; the test lets them through one at a time until a has
        // delivered part of its walk, then a dies with the rest of its stream undelivered.
        network.hold("a", "d");
        TreeFetch fetch = d.content().fetchTree(v1.root(), List.of(), FetchOptions.defaults().prefer(a.id()));
        long target = v1.bytes() / 8;
        assertThat(runUntil(Duration.ofSeconds(20), () -> {
            if (fetch.progress().bytesBySource().getOrDefault(a.id(), 0L) >= target) {
                return true;
            }
            network.releaseOne("a", "d");
            return false;
        })).as(fetch.trace().toString()).isTrue();
        assertThat(fetch.complete()).isFalse();
        int verifiedAtDeath = fetch.progress().nodesVerified();
        kill(a);
        assertThat(runUntil(Duration.ofSeconds(30), fetch::complete)).as(fetch.trace().toString()).isTrue();
        assertThat(d.trees().complete(v1.root())).isTrue();
        TreeFetch.Progress p = fetch.progress();
        long fromA = p.bytesBySource().getOrDefault(a.id(), 0L);
        long fromOthers = p.bytesBySource().getOrDefault(b.id(), 0L) + p.bytesBySource().getOrDefault(c.id(), 0L);
        assertThat(fromA).as("a served part of its walk before it died").isGreaterThan(0L).isLessThan(v1.bytes());
        assertThat(fromOthers).as("b and c served the rest").isGreaterThan(0L);
        assertThat(fromA + fromOthers).isEqualTo(v1.bytes());
        assertThat(p.bytesReceived()).as("a's released subtree was not shipped twice").isEqualTo(v1.bytes());
        System.out.printf("PD-Q-REL-04 shape: a died after %d of %d nodes (%d bytes of %d from a); b and c served %d bytes, %d requests%n",
                verifiedAtDeath, v1.nodes().size(), fromA, v1.bytes(), fromOthers, p.requestsSent());
    }

    @Test
    void aFetchStartedBeforeAnyHolderExistsWaitsAndCompletesWhenAHoldingAppears() throws Exception {
        // PD-X-08, the restart half: the fetch is live with no source, and the holding that appears restarts it
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        NodeFixtures.Tree v1 = v1();
        assertThat(b.content().spaces().holdings(v1.root())).isEmpty();
        TreeFetch fetch = b.content().fetchTree(v1.root(), List.of(), FetchOptions.defaults());
        for (int i = 0; i < 30; i++) {
            tickAll(); // past HOLDINGS_REREAD_MILLIS
        }
        assertThat(fetch.isDoneOrFailed()).isFalse();
        assertThat(fetch.progress().waiting()).isTrue();
        assertThat(fetch.progress().requestsSent()).isZero();
        assertThat(b.content().activeTreeFetches()).containsExactly(fetch);

        a.trees().putAll(v1);
        a.content().holdTree(v1.root());
        assertThat(runUntil(Duration.ofSeconds(20), fetch::complete)).as(fetch.trace().toString()).isTrue();
        assertThat(b.trees().complete(v1.root())).isTrue();
        assertThat(fetch.progress().waiting()).isFalse();
        assertThat(fetch.progress().bytesBySource().get(a.id())).isEqualTo(v1.bytes());
        assertThat(b.content().fetchTree(v1.root(), List.of(), FetchOptions.defaults()).complete())
                .as("the store holds it: a new fetch completes at once").isTrue();
    }

    @Test
    void anIncompleteRootsHoldingSaysCompleteFalseAndCompleteOnlyPublishesNone() throws Exception {
        // PD-S-06: the holding of a partial tree carries complete false and the held count; COMPLETE_ONLY withholds it
        ContentPolicy completeOnly = new ContentPolicy() {
            @Override
            public SeedMode seedMode() {
                return SeedMode.COMPLETE_ONLY;
            }
        };
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        Peer c = newPeer("c", 3, bb -> bb.policy(completeOnly), false, "a");
        for (int i = 0; i < 6; i++) {
            tickAll();
        }
        NodeFixtures.Tree v1 = v1();
        String child = NodeFixtures.linksOf(v1.nodes().get(v1.root())).get(0);
        String grandchild = NodeFixtures.linksOf(v1.nodes().get(child)).get(0);
        Set<String> gap = new MemoryTreeStore().putAll(v1).reachable(grandchild);
        for (Peer holder : List.of(a, c)) {
            holder.trees().putAll(v1);
            for (String cid : gap) {
                holder.trees().remove(cid);
            }
            assertThat(holder.trees().complete(v1.root())).isFalse();
            holder.content().holdTree(v1.root());
        }
        assertThat(runUntil(Duration.ofSeconds(10), () -> b.content().spaces().holdings(v1.root()).size() == 1)).isTrue();
        for (int i = 0; i < 8; i++) {
            tickAll();
        }
        List<Holding> seen = b.content().spaces().holdings(v1.root());
        assertThat(seen).as("c publishes nothing under COMPLETE_ONLY").hasSize(1);
        Holding held = seen.get(0);
        assertThat(held.holder()).isEqualTo(a.id());
        assertThat(held.complete()).isFalse();
        assertThat(held.count()).isEqualTo(v1.nodes().size() - gap.size()).isLessThan(v1.nodes().size());
        assertThat(holdersComplete(b, v1.root())).isZero();
        assertThat(c.content().treeRoots()).contains(v1.root());

        // once the gap is filled the next forced refresh says complete, and c publishes
        for (Peer holder : List.of(a, c)) {
            holder.trees().putAll(v1);
            holder.content().holdTree(v1.root());
        }
        assertThat(runUntil(Duration.ofSeconds(10), () -> holdersComplete(b, v1.root()) == 2)).isTrue();
        for (Holding h : b.content().spaces().holdings(v1.root())) {
            assertThat(h.count()).isEqualTo(v1.nodes().size());
        }
    }

    @Test
    void aPartitionedMemberCatchesUpByDeltaAfterTheHeal() throws Exception {
        // PD-Q-REL-03: b holds v0; a publishes v1 while the two cannot hear each other; after the heal b
        // fetches v1 against v0 and pays for the changed nodes only
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        NodeFixtures.Tree v0 = v0();
        NodeFixtures.Tree v1 = v1();
        a.trees().putAll(v0);
        a.content().holdTree(v0.root());
        assertThat(runUntil(Duration.ofSeconds(10), () -> holdersComplete(b, v0.root()) == 1)).isTrue();
        TreeFetch base = b.content().fetchTree(v0.root(), List.of(), FetchOptions.defaults());
        assertThat(runUntil(Duration.ofSeconds(20), base::complete)).as(base.trace().toString()).isTrue();

        network.hold("a", "b");
        network.hold("b", "a");
        a.trees().putAll(v1);
        a.content().holdTree(v1.root());
        for (int i = 0; i < 20; i++) {
            tickAll();
        }
        assertThat(b.content().spaces().holdings(v1.root())).as("nothing crosses the partition").isEmpty();
        network.release("a", "b", SimNetwork.Order.FIFO, 0);
        network.release("b", "a", SimNetwork.Order.FIFO, 0);
        assertThat(runUntil(Duration.ofSeconds(10), () -> holdersComplete(b, v1.root()) == 1)).isTrue();

        TreeFetch fetch = b.content().fetchTree(v1.root(), List.of(v0.root()), FetchOptions.defaults());
        assertThat(runUntil(Duration.ofSeconds(20), fetch::complete)).as(fetch.trace().toString()).isTrue();
        assertThat(b.trees().complete(v1.root())).isTrue();
        long delta = v1.bytesNotIn(v0);
        TreeFetch.Progress p = fetch.progress();
        System.out.printf("PD-Q-REL-03: after the heal %d bytes received for a delta of %d bytes (%d nodes changed during the partition), %d round trips%n",
                p.bytesReceived(), delta, v1.nodesNotIn(v0), p.roundTrips());
        assertThat(p.bytesReceived()).isGreaterThanOrEqualTo(delta).isLessThanOrEqualTo((long) (1.5 * delta));
        assertThat(p.roundTrips()).isLessThanOrEqualTo(2);
    }

    @Test
    void aPolicyThatReadsTheManifestRefusesATreeRequestAndTheHolderKeepsServing() throws Exception {
        // PD-P-01: a tree root has no manifest, so a policy written for pieces alone throws on a tree request;
        // the holder refuses with reason 3, counts it, and its tree handler survives
        ContentPolicy piecesOnly = new ContentPolicy() {
            @Override
            public boolean mayServe(GroupId group, PeerId requester, Manifest manifest) {
                return manifest.pieces() > 0;
            }
        };
        Peer a = newPeer("a", 1, b -> b.policy(piecesOnly), false);
        Peer b = newPeer("b", 2, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        NodeFixtures.Tree v1 = v1();
        a.trees().putAll(v1);
        a.content().holdTree(v1.root());
        assertThat(runUntil(Duration.ofSeconds(10), () -> holdersComplete(b, v1.root()) == 1)).isTrue();

        TreeFetch fetch = b.content().fetchTree(v1.root(), List.of(), FetchOptions.defaults());
        assertThat(runUntil(Duration.ofSeconds(2), fetch::complete)).isFalse();
        assertThat(fetch.progress().nodesVerified()).isZero();
        assertThat(a.content().stats().treeRequestsRejected()).isGreaterThanOrEqualTo(1);
        assertThat(a.content().stats().treeMessagesDropped()).as("the policy's exception counts as a refusal").isZero();
        assertThat(a.content().stats().nodesServed()).isZero();
        assertThat(fetch.trace()).anySatisfy(line -> assertThat(line).contains("rejected (3)"));

        // a NODE_WANT runs the same check per CID and is refused the same way
        TreeFetch batch = b.content().fetchNodes(List.of(v1.root()), FetchOptions.defaults().prefer(a.id()));
        assertThat(runUntil(Duration.ofSeconds(2), batch::complete)).isFalse();
        assertThat(a.content().stats().treeRequestsRejected()).isGreaterThanOrEqualTo(2);
    }

    @Test
    void aKeylessHolderRefusesWhenTheGroupSealsWhateverTheAskerAdvertises() throws Exception {
        // PD-P-03: a keyless holder that knows its group seals (any member's advertisement says group-key)
        // refuses even a keyless asker whose own advertisement says none; without that signal two keyless
        // members exchange plaintext nodes, which the CIDs verify
        Peer sealed = newPeer("s", 1, b -> b.groupSeals(() -> true), false);
        Peer open = newPeer("o", 2, "s");
        Peer asker = newPeer("j", 3, "s");
        for (int i = 0; i < 6; i++) {
            tickAll();
        }
        assertThat(ads.get(asker.id()).parameters().get("sealing")).isEqualTo(Manifest.SEALING_NONE);
        NodeFixtures.Tree v2 = NodeFixtures.tree(5, 2, 3, 100);
        NodeFixtures.Tree v3 = NodeFixtures.tree(6, 2, 3, 100);
        sealed.trees().putAll(v2);
        sealed.content().holdTree(v2.root());
        open.trees().putAll(v3);
        open.content().holdTree(v3.root());
        assertThat(runUntil(Duration.ofSeconds(10), () -> holdersComplete(asker, v2.root()) == 1
                && holdersComplete(asker, v3.root()) == 1)).isTrue();

        TreeFetch refused = asker.content().fetchTree(v2.root(), List.of(), FetchOptions.defaults());
        assertThat(runUntil(Duration.ofSeconds(2), refused::complete)).isFalse();
        assertThat(refused.progress().nodesVerified()).isZero();
        assertThat(sealed.content().stats().treeRequestsRejected()).isGreaterThanOrEqualTo(1);
        assertThat(sealed.content().stats().nodesServed()).isZero();
        assertThat(refused.trace()).anySatisfy(line -> assertThat(line).contains("rejected (3)"));

        TreeFetch served = asker.content().fetchTree(v3.root(), List.of(), FetchOptions.defaults());
        assertThat(runUntil(Duration.ofSeconds(20), served::complete)).as(served.trace().toString()).isTrue();
        assertThat(asker.trees().complete(v3.root())).isTrue();
        assertThat(open.content().stats().treeRequestsRejected()).isZero();
    }

    @Test
    void aNodeThatDoesNotHashToItsCidStrikesTheHolderAndTheFetchCompletesElsewhere() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        Peer c = newPeer("c", 3, "a");
        for (int i = 0; i < 6; i++) {
            tickAll();
        }
        NodeFixtures.Tree v1 = v1();
        a.trees().putAll(v1);
        b.trees().putAll(v1);
        String child = NodeFixtures.linksOf(v1.nodes().get(v1.root())).get(0);
        byte[] altered = NodeFixtures.node(NodeFixtures.linksOf(v1.nodes().get(child)), new byte[]{(byte) 'x'});
        a.trees().corrupt(child, altered);
        a.content().holdTree(v1.root());
        b.content().holdTree(v1.root());
        assertThat(runUntil(Duration.ofSeconds(10), () -> holdersComplete(c, v1.root()) == 2)).isTrue();

        TreeFetch fetch = c.content().fetchTree(v1.root(), List.of(), FetchOptions.defaults().prefer(a.id()));
        assertThat(runUntil(Duration.ofSeconds(20), fetch::complete)).as(fetch.trace().toString()).isTrue();
        assertThat(c.trees().complete(v1.root())).isTrue();
        assertThat(c.content().stats().piecesStruck()).isGreaterThanOrEqualTo(1);
        assertThat(fetch.trace()).anySatisfy(line -> assertThat(line).contains("struck " + a.id().display()));
        assertThat(fetch.progress().bytesBySource().get(b.id())).isGreaterThan(0L);
        for (Map.Entry<String, byte[]> e : v1.nodes().entrySet()) {
            assertThat(c.trees().get(e.getKey())).contains(e.getValue());
        }
    }

    @Test
    void aGrantRefusalAnswersRejectThreeAndNoNodeCrosses() throws Exception {
        Authorizer none = new Authorizer() {
            @Override
            public boolean permits(PeerId peer, Operation operation, String scope) {
                return operation != Operation.CONTENT_FETCH;
            }
        };
        Peer a = newPeer("a", 1, b -> b.authorizer(none), false);
        Peer b = newPeer("b", 2, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        NodeFixtures.Tree v1 = v1();
        a.trees().putAll(v1);
        a.content().holdTree(v1.root());
        assertThat(runUntil(Duration.ofSeconds(10), () -> holdersComplete(b, v1.root()) == 1)).isTrue();

        TreeFetch fetch = b.content().fetchTree(v1.root(), List.of(), FetchOptions.defaults());
        assertThat(runUntil(Duration.ofSeconds(2), fetch::complete)).isFalse();
        assertThat(fetch.progress().nodesVerified()).isZero();
        assertThat(b.trees().size()).isZero();
        assertThat(a.content().stats().treeRequestsRejected()).isGreaterThanOrEqualTo(1);
        assertThat(a.content().stats().nodesServed()).isZero();
        assertThat(fetch.trace()).anySatisfy(line -> assertThat(line).contains("rejected (3)"));
    }

    @Test
    void nodesTravelSealedUnderTheGroupKeyAndAKeylessHolderRefuses() throws Exception {
        GroupKey key = GroupKey.generate();
        Peer a = newPeer("a", 1, b -> b.keyRing(GroupKeyRing.of(key)), false);
        Peer b = newPeer("b", 2, bb -> bb.keyRing(GroupKeyRing.of(key)), false, "a");
        Peer k = newPeer("k", 3, "a"); // a member with no content key
        for (int i = 0; i < 6; i++) {
            tickAll();
        }
        NodeFixtures.Tree v1 = v1();
        a.trees().putAll(v1);
        a.content().holdTree(v1.root());
        assertThat(runUntil(Duration.ofSeconds(10), () -> holdersComplete(b, v1.root()) == 1)).isTrue();

        List<byte[]> onTheWire = new java.util.concurrent.CopyOnWriteArrayList<>();
        network.tap("a", "b", onTheWire::add);
        TreeFetch fetch = b.content().fetchTree(v1.root(), List.of(), FetchOptions.defaults());
        assertThat(runUntil(Duration.ofSeconds(20), fetch::complete)).as(fetch.trace().toString()).isTrue();
        network.untap("a", "b");
        assertThat(b.trees().complete(v1.root())).isTrue();
        int leavesChecked = 0;
        for (byte[] node : v1.nodes().values()) {
            if (NodeFixtures.linksOf(node).isEmpty()) {
                byte[] tail = java.util.Arrays.copyOfRange(node, node.length - 64, node.length);
                for (byte[] frame : onTheWire) {
                    assertThat(indexOf(frame, tail)).as("no plaintext node crosses the wire").isEqualTo(-1);
                }
                if (++leavesChecked == 8) {
                    break;
                }
            }
        }
        assertThat(onTheWire).isNotEmpty();

        // PD-P-03: the keyless member holds another tree, and refuses a member whose content is sealed
        NodeFixtures.Tree v2 = NodeFixtures.tree(5, 2, 3, 100);
        k.trees().putAll(v2);
        k.content().holdTree(v2.root());
        assertThat(runUntil(Duration.ofSeconds(10), () -> holdersComplete(b, v2.root()) == 1)).isTrue();
        TreeFetch refused = b.content().fetchTree(v2.root(), List.of(), FetchOptions.defaults());
        assertThat(runUntil(Duration.ofSeconds(2), refused::complete)).isFalse();
        assertThat(refused.progress().nodesVerified()).isZero();
        assertThat(k.content().stats().treeRequestsRejected()).isGreaterThanOrEqualTo(1);
        assertThat(refused.trace()).anySatisfy(line -> assertThat(line).contains("rejected (3)"));
    }

    @Test
    void aMemberWithoutTheTreesParameterNeverReceivesATreeMessage() throws Exception {
        Peer legacy = newPeer("l", 1, b -> b, true);
        Peer b = newPeer("b", 2, "l");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        NodeFixtures.Tree v1 = v1();
        legacy.trees().putAll(v1);
        legacy.content().holdTree(v1.root());
        assertThat(runUntil(Duration.ofSeconds(10), () -> holdersComplete(b, v1.root()) == 1)).isTrue();
        assertThat(b.content().advertisesTrees(legacy.id())).isFalse();

        TreeFetch fetch = b.content().fetchTree(v1.root(), List.of(), FetchOptions.defaults().withTimeout(Duration.ofSeconds(5)));
        assertThat(runUntil(Duration.ofSeconds(10), fetch::isDoneOrFailed)).as(fetch.trace().toString()).isTrue();
        assertThat(fetch.complete()).isFalse();
        assertThat(fetch.failure()).isPresent();
        assertThat(fetch.failure().get().getMessage()).contains(v1.root());
        assertThat(fetch.progress().requestsSent()).isZero();
        Content.Stats stats = legacy.content().stats();
        assertThat(stats.nodesServed()).isZero();
        assertThat(stats.treeRequestsRejected()).isZero();
        assertThat(stats.treeMessagesDropped()).isZero();
        assertThat(b.content().activeTreeFetches()).isEmpty();
    }

    @Test
    void aBareNodeBatchFetchesByCidFromThePreferredHolderAndFollowsNoLink() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        NodeFixtures.Tree v1 = v1();
        a.trees().putAll(v1);
        List<String> wanted = new ArrayList<>();
        wanted.add(v1.root());
        for (String cid : v1.nodes().keySet()) {
            if (wanted.size() < 10 && !cid.equals(v1.root())) {
                wanted.add(cid);
            }
        }
        TreeFetch batch = b.content().fetchNodes(wanted, FetchOptions.defaults().prefer(a.id()));
        assertThat(runUntil(Duration.ofSeconds(20), batch::complete)).as(batch.trace().toString()).isTrue();
        assertThat(b.trees().size()).isEqualTo(10);
        assertThat(batch.progress().nodesVerified()).isEqualTo(10);
        assertThat(batch.progress().requestsSent()).isEqualTo(1);
        for (String cid : wanted) {
            assertThat(b.trees().has(cid)).isTrue();
        }
        assertThat(b.content().treeRoots()).as("a batch publishes no holding").isEmpty();
        TreeFetch again = b.content().fetchNodes(wanted, FetchOptions.defaults());
        assertThat(again.complete()).as("already held: complete at once").isTrue();
    }

    private static int indexOf(byte[] haystack, byte[] needle) {
        outer:
        for (int i = 0; i + needle.length <= haystack.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }
}
