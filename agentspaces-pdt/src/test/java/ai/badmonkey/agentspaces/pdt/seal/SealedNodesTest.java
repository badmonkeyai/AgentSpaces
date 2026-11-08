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
package ai.badmonkey.agentspaces.pdt.seal;

import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.GroupKey;
import ai.badmonkey.agentspaces.common.crypto.GroupKeyRing;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.FetchOptions;
import ai.badmonkey.agentspaces.content.MemoryPieceStore;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.content.tree.TreeFetch;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.pdt.Kind;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeFormatException;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.Pdt;
import ai.badmonkey.agentspaces.pdt.blob.PBlob;
import ai.badmonkey.agentspaces.pdt.golden.PdtVectors;
import ai.badmonkey.agentspaces.pdt.refs.Ref;
import ai.badmonkey.agentspaces.pdt.values.Codec;
import ai.badmonkey.agentspaces.pdt.values.Editor;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.blocks.MemoryBlockStore;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The sealed-nodes mode of srad/agentspaces-pdt.md §3.8 (PD-P-05, PD-P-06, PD-Q-SEC-05):
 * equal plaintext in one epoch seals to equal bytes and one CID; a keyless peer on the
 * sim fleet of {@code PdtSwarmTest} fetches, holds, and serves a sealed tree it cannot
 * read; a key holder opens it; and an epoch rotation keeps the old nodes readable
 * through the ring's opening keys while new nodes seal under the new epoch.
 */
class SealedNodesTest {

    private static final Codec<String> STR = Codec.of(String.class);
    private static final Duration DEADLINE = Duration.ofSeconds(30);
    private static final int ENTRIES = 2_000;

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<Peer> peers = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zPdtSeal");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zPdtSeal", PeerIdentity.generate().peerId(), groupId,
            Instant.EPOCH, Duration.ofDays(1), "content",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final Map<PeerId, CapabilityAdvertisement> ads = new ConcurrentHashMap<>();
    private final Set<String> dead = new HashSet<>();
    private final GroupKey groupKey = GroupKey.fromBytes(PdtVectors.SEALED_TEST_KEY);

    private record Peer(String name, PeerNode node, GroupRuntime runtime, Content content, SealedNodeStore store, Pdt pdt,
                        ReplicatedSpace refs) {
        PeerId id() {
            return node.peerId();
        }
    }

    @AfterEach
    void tearDown() {
        peers.forEach(p -> {
            if (!dead.contains(p.name())) {
                p.pdt().close();
                p.content().close();
                p.node().close();
            }
        });
    }

    private GroupKeyRing ringOf(GroupKey key) {
        return key == null ? GroupKeyRing.empty() : GroupKeyRing.of(key);
    }

    /** A peer in the sealed-nodes mode; {@code key} null makes it keyless. */
    private Peer newPeer(String address, long seed, GroupKey key, String... seedAddresses) throws IOException {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        BlockExchange blocks = new BlockExchange(runtime, codec, clock, 0, BlockExchange.DEFAULT_MAX_BLOCK_BYTES);
        ReplicatedSpace contentSpace = ReplicatedSpace.builder(runtime, "content", identity, "content")
                .clock(clock).settleWindow(Duration.ZERO).blocks(blocks).build();
        ReplicatedSpace refSpace = ReplicatedSpace.builder(runtime, "refs", identity, "refs")
                .clock(clock).settleWindow(Duration.ZERO).build();
        NodeStore plain = new NodeStore(new MemoryBlockStore(), clock);
        Pdt pdt = Pdt.builder(refSpace).store(plain).clock(clock).loadTimeout(DEADLINE).collectEvery(Duration.ZERO)
                .sealing(PRef.SEALING_SEALED_NODES).keyRing(ringOf(key)).build();
        assertThat(pdt.store()).isInstanceOf(SealedNodeStore.class);
        Content content = Content.builder(new CapabilityPipes(runtime, codec), runtime, contentSpace, identity.peerId())
                .codec(codec).clock(clock).store(new MemoryPieceStore(clock, 0)).blocks(blocks).random(new Random(seed))
                .treeStore(pdt.treeStore()).advertisements(peer -> Optional.ofNullable(ads.get(peer))).build();
        ads.put(identity.peerId(), content.describe(groupId));
        runtime.onTick(content::tick);
        content.start();
        pdt.attach(content).start();
        runtime.onTick(pdt::tick);
        Peer peer = new Peer(address, node, runtime, content, (SealedNodeStore) pdt.store(), pdt, refSpace);
        peers.add(peer);
        return peer;
    }

    private void kill(Peer peer) {
        dead.add(peer.name());
        peer.pdt().close();
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

    private static int holdersComplete(Peer at, String root) {
        int n = 0;
        for (var h : at.content().spaces().holdings(root)) {
            if (h.complete()) {
                n++;
            }
        }
        return n;
    }

    private static String value(int i, Random random) {
        char[] chars = new char[64];
        for (int c = 0; c < chars.length; c++) {
            chars[c] = (char) ('a' + random.nextInt(26));
        }
        return i + ":" + new String(chars);
    }

    private static PMap<String, String> buildMap(NodeStore store, long seed) {
        Random random = new Random(seed);
        Editor<String, String> editor = PMap.empty(store, STR, STR).editor();
        for (int i = 0; i < ENTRIES; i++) {
            editor.assoc("key-" + i, value(i, random));
        }
        return editor.done().value();
    }

    private static SealedNodeStore sealedStore(TestClock clock, GroupKeyRing ring) {
        return new SealedNodeStore(new MemoryBlockStore(), clock, ring);
    }

    private static long epochOf(NodeStore store, String cid) {
        return NodeSealer.decode(store.stored(cid).orElseThrow()).epoch();
    }

    // ------------------------------------------------------------ the format

    @Test
    void equalPlaintextInOneEpochSealsToEqualBytesAndOneCid() {
        byte[] nodeKey = NodeSealer.nodeKey(groupKey);
        byte[] plain = PdtVectors.mapDeep();
        byte[] first = NodeSealer.seal(nodeKey, 3, plain, Cids.DAG_CBOR);
        byte[] again = NodeSealer.seal(nodeKey, 3, plain, Cids.DAG_CBOR);
        assertThat(again).isEqualTo(first);
        assertThat(Cids.of(again, Cids.DAG_CBOR)).isEqualTo(Cids.of(first, Cids.DAG_CBOR));
        assertThat(NodeSealer.seal(nodeKey, 4, plain, Cids.DAG_CBOR)).as("another epoch, another seal").isNotEqualTo(first);
        assertThat(NodeSealer.seal(nodeKey, 3, PdtVectors.mapSmall(), Cids.DAG_CBOR)).isNotEqualTo(first);

        NodeSealer.Envelope envelope = NodeSealer.decode(first);
        assertThat(envelope.links()).as("the links of the plaintext travel in the clear").isEqualTo(NodeCodec.links(plain));
        assertThat(NodeCodec.links(first)).as("a keyless walker reads the same links through the codec").isEqualTo(NodeCodec.links(plain));
        assertThat(NodeSealer.open(nodeKey, envelope)).contains(plain);

        // a tampered ciphertext, a tampered nonce, and a lie about the links are each refused
        byte[] tampered = envelope.ciphertext().clone();
        tampered[5] ^= 1;
        assertThat(NodeSealer.open(nodeKey, new NodeSealer.Envelope(3, envelope.nonce(), tampered, envelope.links()))).isEmpty();
        byte[] nonce = envelope.nonce().clone();
        nonce[0] ^= 1;
        assertThat(NodeSealer.open(nodeKey, new NodeSealer.Envelope(3, nonce, envelope.ciphertext(), envelope.links()))).isEmpty();
        List<String> fewer = envelope.links().subList(0, envelope.links().size() - 1);
        assertThatThrownBy(() -> NodeSealer.open(nodeKey, new NodeSealer.Envelope(3, envelope.nonce(), envelope.ciphertext(), fewer)))
                .isInstanceOf(NodeFormatException.class).hasMessageContaining("links in the clear");

        // a raw chunk seals with no links and opens under the raw codec
        byte[] chunk = new byte[20_000];
        new Random(1).nextBytes(chunk);
        byte[] sealedChunk = NodeSealer.seal(nodeKey, 3, chunk, Cids.RAW);
        assertThat(NodeSealer.decode(sealedChunk).links()).isEmpty();
        assertThat(NodeSealer.open(nodeKey, NodeSealer.decode(sealedChunk), Cids.RAW)).contains(chunk);
        assertThat(NodeCodec.links(Cids.of(sealedChunk, Cids.RAW), sealedChunk)).isEmpty();
    }

    @Test
    void aSealedStoreWritesEnvelopesAKeyHolderOpensAndAKeylessStoreHoldsWalksAndRefusesToRead() {
        GroupKeyRing ring = GroupKeyRing.of(groupKey);
        SealedNodeStore writer = sealedStore(clock, ring);
        PMap<String, String> map = buildMap(writer, 7);
        String root = map.root();
        assertThat(writer.complete(root)).isTrue();
        for (String cid : writer.reachable(root)) {
            byte[] stored = writer.stored(cid).orElseThrow();
            assertThat(NodeCodec.kindOf(stored)).as(cid).isEqualTo(Kind.SN);
            assertThat(Cids.of(stored, Cids.parse(cid).codec())).isEqualTo(cid);
            assertThat(NodeCodec.kindOf(writer.get(cid).orElseThrow())).as("opened to a map node").isIn(Kind.M, Kind.MC);
        }
        assertThat(map.get("key-1234")).contains(expected(7).get(1234));

        // the same value in a second store under the same key and epoch has the same root and the same nodes
        SealedNodeStore twin = sealedStore(clock, GroupKeyRing.of(groupKey));
        assertThat(buildMap(twin, 7).root()).isEqualTo(root);
        assertThat(twin.reachable(root)).containsExactlyInAnyOrderElementsOf(writer.reachable(root));

        // a keyless store takes every envelope as it is, walks the tree, and refuses to open any node
        SealedNodeStore keyless = sealedStore(clock, GroupKeyRing.empty());
        for (String cid : writer.reachable(root)) {
            keyless.putVerified(cid, writer.stored(cid).orElseThrow());
        }
        assertThat(keyless.complete(root)).isTrue();
        assertThat(keyless.missing(root)).isEmpty();
        assertThat(keyless.reachable(root)).hasSameSizeAs(writer.reachable(root));
        assertThatThrownBy(() -> keyless.get(root)).isInstanceOf(SealedNodeException.class)
                .satisfies(e -> assertThat(((SealedNodeException) e).epoch()).isZero())
                .hasMessageContaining("does not hold");
        assertThatThrownBy(() -> PMap.load(keyless, root, STR, STR).get("key-1")).isInstanceOf(SealedNodeException.class);
        assertThatThrownBy(() -> keyless.put(PdtVectors.mapSmall(), Cids.DAG_CBOR)).isInstanceOf(SealedNodeException.class)
                .hasMessageContaining("no node can be sealed");

        // a key holder over the keyless store's backend reads everything
        SealedNodeStore reader = new SealedNodeStore(keyless.backend(), clock, GroupKeyRing.of(groupKey));
        PMap<String, String> onReader = PMap.load(reader, root, STR, STR);
        assertThat(onReader.count()).isEqualTo(ENTRIES);
        assertThat(onReader.get("key-1999")).contains(expected(7).get(1999));

        // a blob seals chunk by chunk: the chunk CIDs stay raw-codec and open to the bytes
        byte[] bytes = new byte[300 * 1024];
        new Random(3).nextBytes(bytes);
        PBlob blob = PBlob.of(bytes, writer);
        assertThat(blob.chunks().size()).isGreaterThan(1);
        for (var chunk : blob.chunks()) {
            assertThat(Cids.parse(chunk.cid()).codec()).isEqualTo(Cids.RAW);
            assertThat(NodeCodec.kindOf(writer.stored(chunk.cid()).orElseThrow())).isEqualTo(Kind.SN);
        }
        assertThat(PBlob.load(writer, blob.root()).read(100_000, 16)).isEqualTo(java.util.Arrays.copyOfRange(bytes, 100_000, 100_016));
        assertThat(writer.complete(blob.root())).isTrue();
    }

    private static Map<Integer, String> expected(long seed) {
        Random random = new Random(seed);
        Map<Integer, String> map = new java.util.HashMap<>();
        for (int i = 0; i < ENTRIES; i++) {
            map.put(i, value(i, random));
        }
        return map;
    }

    // ------------------------------------------------------------ the fleet

    @Test
    void aKeylessPeerFetchesHoldsAndServesASealedTreeItCannotReadAndAKeyHolderOpensIt() throws Exception {
        Peer a = newPeer("a", 1, groupKey);
        Peer c = newPeer("c", 3, null, "a");
        Peer b = newPeer("b", 2, groupKey, "a");
        for (int i = 0; i < 6; i++) {
            tickAll();
        }
        PMap<String, String> v0 = buildMap(a.store(), 7);
        String root0 = v0.root();
        int nodes = a.store().reachable(root0).size();
        a.pdt().swarm().hold(root0);
        assertThat(runUntil(Duration.ofSeconds(10), () -> holdersComplete(c, root0) == 1)).isTrue();

        // c, keyless, fetches the whole tree through the links in the clear, holds it, and serves it
        TreeFetch toC = c.pdt().swarm().fetchTree(root0);
        assertThat(runUntil(Duration.ofSeconds(30), toC::complete)).as(toC.trace().toString()).isTrue();
        assertThat(c.store().complete(root0)).isTrue();
        assertThat(c.store().reachable(root0)).hasSize(nodes);
        assertThat(toC.progress().nodesVerified()).isEqualTo(nodes);
        assertThatThrownBy(() -> c.store().get(root0)).isInstanceOf(SealedNodeException.class);
        assertThatThrownBy(() -> PMap.load(c.store(), c.pdt().swarm().loader(), root0, STR, STR).get("key-1"))
                .isInstanceOf(SealedNodeException.class);
        c.pdt().swarm().hold(root0);
        assertThat(runUntil(Duration.ofSeconds(10), () -> holdersComplete(b, root0) == 2)).isTrue();

        // a dies; b fetches from c alone and opens every node with its key
        kill(a);
        TreeFetch toB = b.pdt().swarm().fetchTree(root0, List.of(), FetchOptions.defaults().prefer(c.id()));
        assertThat(runUntil(Duration.ofSeconds(30), toB::complete)).as(toB.trace().toString()).isTrue();
        assertThat(toB.progress().bytesBySource().getOrDefault(c.id(), 0L)).as("every byte came from the keyless holder")
                .isEqualTo(toB.progress().bytesReceived());
        PMap<String, String> onB = PMap.load(b.store(), root0, STR, STR);
        assertThat(onB.count()).isEqualTo(ENTRIES);
        assertThat(onB.get("key-1234")).contains(v0.get("key-1234").orElseThrow());
        for (String cid : b.store().reachable(root0)) {
            assertThat(NodeCodec.kindOf(b.store().stored(cid).orElseThrow())).isEqualTo(Kind.SN);
        }

        // a one-key change on b shares every unchanged sealed node: the delta c then fetches is a path
        PMap<String, String> v1 = onB.assoc("key-1234", "changed");
        String root1 = v1.root();
        long delta = 0;
        Set<String> shared = b.store().reachable(root0);
        for (String cid : b.store().reachable(root1)) {
            if (!shared.contains(cid)) {
                delta += b.store().stored(cid).orElseThrow().length;
            }
        }
        b.pdt().swarm().hold(root1);
        assertThat(runUntil(Duration.ofSeconds(10), () -> holdersComplete(c, root1) == 1)).isTrue();
        TreeFetch deltaToC = c.pdt().swarm().fetchTree(root1, List.of(root0));
        assertThat(runUntil(Duration.ofSeconds(30), deltaToC::complete)).as(deltaToC.trace().toString()).isTrue();
        assertThat(deltaToC.progress().bytesReceived()).isEqualTo(delta).isLessThanOrEqualTo(8 * 1024);
        System.out.printf("PD-P-05: %d sealed nodes fetched by a keyless peer and opened by a key holder; a one-key change shipped %d bytes%n",
                nodes, deltaToC.progress().bytesReceived());
    }

    @Test
    void aRefHeadInSealedNodesModeNamesTheModeAndTheEpochAndAKeyHolderDerefsIt() throws Exception {
        Peer a = newPeer("a", 1, groupKey);
        Peer b = newPeer("b", 2, groupKey, "a");
        for (int i = 0; i < 6; i++) {
            tickAll();
        }
        Ref<PMap<String, String>> factsA = a.pdt().mapRef("facts", STR, STR);
        Ref<PMap<String, String>> factsB = b.pdt().mapRef("facts", STR, STR);
        factsA.swap(m -> m.assoc("k", "v"));
        assertThat(runUntil(Duration.ofSeconds(20), () -> factsB.deref().get("k").isPresent())).isTrue();
        PRef head = a.refs().readAllEntries(ai.badmonkey.agentspaces.api.space.Template.of(PRef.class), 8).get(0).value();
        assertThat(head.sealing()).isEqualTo(PRef.SEALING_SEALED_NODES);
        assertThat(head.keyEpoch()).isEqualTo(0L);
        assertThat(a.pdt().sealing()).isEqualTo(PRef.SEALING_SEALED_NODES);
        assertThat(a.pdt().sealer()).isPresent();
        assertThat(NodeCodec.kindOf(b.store().stored(head.root()).orElseThrow())).as("the inline delta carried envelopes").isEqualTo(Kind.SN);
        assertThat(factsB.deref().get("k")).contains("v");
    }

    // ------------------------------------------------------------ rotation

    @Test
    void anEpochRotationKeepsOldNodesReadableThroughOpeningKeysAndSealsNewOnesUnderTheNewEpoch() {
        GroupKeyRing ring = GroupKeyRing.of(groupKey);
        SealedNodeStore store = sealedStore(clock, ring);
        PMap<String, String> v0 = buildMap(store, 7);
        String root0 = v0.root();
        for (String cid : store.reachable(root0)) {
            assertThat(epochOf(store, cid)).isZero();
        }

        GroupKey second = GroupKey.generate();
        clock.advance(Duration.ofMinutes(1));
        ring.install(1, "rotator", second, clock.instant());
        assertThat(store.sealer().writeEpoch()).contains(1L);
        PMap<String, String> v1 = v0.assoc("key-1234", "changed").assoc("key-new", "new");
        String root1 = v1.root();
        Set<String> old = store.reachable(root0);
        int fresh = 0;
        for (String cid : store.reachable(root1)) {
            long epoch = epochOf(store, cid);
            if (old.contains(cid)) {
                assertThat(epoch).as("a shared node keeps its epoch 0 envelope").isZero();
            } else {
                assertThat(epoch).as("a new node seals under epoch 1").isEqualTo(1L);
                fresh++;
            }
        }
        assertThat(fresh).isGreaterThan(0).isLessThan(old.size() / 10);
        assertThat(store.complete(root1)).isTrue();
        PMap<String, String> reread = PMap.load(store, root1, STR, STR);
        assertThat(reread.get("key-1234")).contains("changed");
        assertThat(reread.get("key-7")).contains(v0.get("key-7").orElseThrow());
        assertThat(reread.count()).isEqualTo(ENTRIES + 1);

        // the same plaintext in the two epochs is two different envelopes with two CIDs
        byte[] plain = PdtVectors.mapSmall();
        byte[] inZero = NodeSealer.seal(NodeSealer.nodeKey(groupKey), 0, plain, Cids.DAG_CBOR);
        byte[] inOne = NodeSealer.seal(NodeSealer.nodeKey(second), 1, plain, Cids.DAG_CBOR);
        assertThat(Cids.of(inOne, Cids.DAG_CBOR)).isNotEqualTo(Cids.of(inZero, Cids.DAG_CBOR));

        // a member that holds only epoch 1 reads the new nodes and fails on an old one, naming the epoch it lacks
        GroupKeyRing lateRing = GroupKeyRing.empty();
        lateRing.install(1, "rotator", second, clock.instant());
        SealedNodeStore late = new SealedNodeStore(store.backend(), clock, lateRing);
        assertThat(late.get(root1)).as("the new root is an epoch 1 node").isPresent();
        String oldChild = old.stream().filter(cid -> !cid.equals(root0) && store.reachable(root1).contains(cid)).findFirst().orElseThrow();
        assertThatThrownBy(() -> late.get(oldChild)).isInstanceOf(SealedNodeException.class)
                .satisfies(e -> assertThat(((SealedNodeException) e).epoch()).isZero());
        assertThat(late.complete(root1)).as("completeness needs no key").isTrue();
    }

    @Test
    void theBuilderRefusesASwarmBuiltOverAnUnsealedStoreAndAdoptsASealedOne() throws Exception {
        PeerIdentity identity = PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(9).build();
        node.listen(network.register("x"), "x");
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), List.of());
        BlockExchange blocks = new BlockExchange(runtime, codec, clock, 0, BlockExchange.DEFAULT_MAX_BLOCK_BYTES);
        ReplicatedSpace contentSpace = ReplicatedSpace.builder(runtime, "content", identity, "content")
                .clock(clock).settleWindow(Duration.ZERO).blocks(blocks).build();
        ReplicatedSpace refSpace = ReplicatedSpace.builder(runtime, "refs", identity, "refs").clock(clock).build();
        try {
            NodeStore plain = new NodeStore(new MemoryBlockStore(), clock);
            Content unsealed = Content.builder(new CapabilityPipes(runtime, codec), runtime, contentSpace, identity.peerId())
                    .codec(codec).clock(clock).store(new MemoryPieceStore(clock, 0)).blocks(blocks)
                    .treeStore(ai.badmonkey.agentspaces.pdt.swarm.PdtSwarm.treeStore(plain)).build();
            assertThatThrownBy(() -> Pdt.builder(unsealed, refSpace).sealing(PRef.SEALING_SEALED_NODES).keyRing(GroupKeyRing.of(groupKey)).build())
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("SealedNodeStore");
            unsealed.close();

            SealedNodeStore sealed = sealedStore(clock, GroupKeyRing.of(groupKey));
            Content over = Content.builder(new CapabilityPipes(runtime, codec), runtime, contentSpace, identity.peerId())
                    .codec(codec).clock(clock).store(new MemoryPieceStore(clock, 0)).blocks(blocks)
                    .treeStore(ai.badmonkey.agentspaces.pdt.swarm.PdtSwarm.treeStore(sealed)).build();
            Pdt pdt = Pdt.builder(over, refSpace).sealing(PRef.SEALING_SEALED_NODES).build();
            assertThat(pdt.store()).isSameAs(sealed);
            assertThat(pdt.sealer()).contains(sealed.sealer());
            assertThatThrownBy(() -> Pdt.builder(refSpace).sealing("sealed")).isInstanceOf(IllegalArgumentException.class);
            pdt.close();
            over.close();
        } finally {
            node.close();
        }
    }
}
