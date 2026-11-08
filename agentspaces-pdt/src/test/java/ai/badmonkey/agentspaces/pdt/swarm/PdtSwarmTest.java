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
package ai.badmonkey.agentspaces.pdt.swarm;

import ai.badmonkey.agentspaces.agent.content.Holding;
import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.FetchOptions;
import ai.badmonkey.agentspaces.content.MemoryPieceStore;
import ai.badmonkey.agentspaces.content.tree.TreeFetch;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.blob.BlobTree;
import ai.badmonkey.agentspaces.pdt.blob.PBlob;
import ai.badmonkey.agentspaces.pdt.values.Codec;
import ai.badmonkey.agentspaces.pdt.values.Editor;
import ai.badmonkey.agentspaces.pdt.values.NodeLoader;
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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Persistent values over the content swarm's tree exchange, end to end
 * (srad/agentspaces-pdt.md §3.5, §6.4 S2 and S3, PD-Q-PRF-01, PD-Q-PRF-02,
 * PD-Q-PRF-06, PD-Q-REL-04, PD-V-06). Each peer is a {@code PeerNode} on the
 * in-memory fabric with a {@code NodeStore} over a {@code MemoryBlockStore},
 * shown to its {@code Content} through {@link NodeStoreTreeStore} and attached
 * through {@link PdtSwarm}; the members' advertisements come from one shared
 * lookup as in {@code ContentTreeTest}. The fleet helper is the one of
 * {@code ContentSwarmTest}: frames travel inline, ticks run on the test thread,
 * and timeouts run on the test clock.
 */
class PdtSwarmTest {

    private static final int ENTRIES = 10_000;
    private static final int VALUE_CHARS = 190;
    private static final int BLOB_BYTES = 32 * 1024 * 1024;
    private static final Codec<String> STR = Codec.of(String.class);
    private static final Duration DEADLINE = Duration.ofSeconds(30);

    private final TestClock clock = TestClock.create();
    private final SimNetwork network = new SimNetwork();
    private final List<Peer> peers = new ArrayList<>();
    private final CborCodec codec = CborCodec.defaultCodec();
    private final GroupId groupId = GroupId.of("zPdtSwarm");
    private final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zPdtSwarm", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "content",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    private final Map<PeerId, CapabilityAdvertisement> ads = new ConcurrentHashMap<>();
    private final Set<String> dead = new HashSet<>();

    private record Peer(String name, PeerNode node, GroupRuntime runtime, ReplicatedSpace space,
                        Content content, NodeStore store, PdtSwarm swarm) {
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
        NodeStore store = new NodeStore(new MemoryBlockStore(), clock);
        Content content = Content.builder(new CapabilityPipes(runtime, codec), runtime, space, identity.peerId())
                .codec(codec).clock(clock).store(new MemoryPieceStore(clock, 0)).blocks(blocks).random(new Random(seed))
                .treeStore(PdtSwarm.treeStore(store)).advertisements(peer -> Optional.ofNullable(ads.get(peer))).build();
        ads.put(identity.peerId(), content.describe(groupId));
        runtime.onTick(content::tick);
        content.start();
        PdtSwarm swarm = PdtSwarm.attach(content, store, DEADLINE);
        Peer peer = new Peer(address, node, runtime, space, content, store, swarm);
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

    private static int holdersComplete(Peer at, String root) {
        int n = 0;
        for (Holding h : at.content().spaces().holdings(root)) {
            if (h.complete()) {
                n++;
            }
        }
        return n;
    }

    // ------------------------------------------------------------ fixtures

    private static String key(int i) {
        return "key-" + i;
    }

    private static String value(int i, Random random) {
        char[] chars = new char[VALUE_CHARS];
        for (int c = 0; c < chars.length; c++) {
            chars[c] = (char) ('a' + random.nextInt(26));
        }
        return i + ":" + new String(chars);
    }

    /** A map of 10,000 entries in a peer's store; its values are reproducible from the seed. */
    private static PMap<String, String> buildMap(NodeStore store, long seed) {
        Random random = new Random(seed);
        Editor<String, String> editor = PMap.empty(store, STR, STR).editor();
        for (int i = 0; i < ENTRIES; i++) {
            editor.assoc(key(i), value(i, random));
        }
        return editor.done().value();
    }

    private static Map<String, String> expected(long seed) {
        Random random = new Random(seed);
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < ENTRIES; i++) {
            map.put(key(i), value(i, random));
        }
        return map;
    }

    private static byte[] seededBytes(int length, long seed) {
        byte[] bytes = new byte[length];
        new Random(seed).nextBytes(bytes);
        return bytes;
    }

    /** Levels below the root: 0 for a single node, as {@code NodeFixtures.Tree.depth} counts. */
    private static int levelsBelowRoot(NodeStore store, String root) {
        Deque<String> queue = new ArrayDeque<>();
        Deque<Integer> depths = new ArrayDeque<>();
        Set<String> seen = new HashSet<>();
        queue.add(root);
        depths.add(0);
        seen.add(root);
        int deepest = 0;
        while (!queue.isEmpty()) {
            String cid = queue.poll();
            int depth = depths.poll();
            deepest = Math.max(deepest, depth);
            byte[] bytes = store.get(cid).orElseThrow();
            for (String link : NodeCodec.links(cid, bytes)) {
                if (seen.add(link)) {
                    queue.add(link);
                    depths.add(depth + 1);
                }
            }
        }
        return deepest;
    }

    private static long bytesUnder(NodeStore store, String root) {
        long total = 0;
        for (String cid : store.reachable(root)) {
            total += store.get(cid).orElseThrow().length;
        }
        return total;
    }

    private static long bytesNotIn(NodeStore store, String root, String base) {
        Set<String> shared = store.reachable(base);
        long total = 0;
        for (String cid : store.reachable(root)) {
            if (!shared.contains(cid)) {
                total += store.get(cid).orElseThrow().length;
            }
        }
        return total;
    }

    /** The chunk of a blob that covers an offset. */
    private static BlobTree.Entry chunkAt(PBlob blob, long offset) {
        long start = 0;
        for (BlobTree.Entry entry : blob.chunks()) {
            if (offset < start + entry.length()) {
                return entry;
            }
            start += entry.length();
        }
        throw new IllegalArgumentException("offset " + offset + " past the end");
    }

    // ------------------------------------------------------------ scenarios

    @Test
    void aOneKeyChangeToATenThousandEntryMapShipsUnderEightKiB() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        PMap<String, String> v0 = buildMap(a.store(), 7);
        String root0 = v0.root();
        long mapBytes = bytesUnder(a.store(), root0);
        a.swarm().hold(root0);
        assertThat(a.store().pinCount(root0)).isEqualTo(1);
        assertThat(runUntil(Duration.ofSeconds(10), () -> holdersComplete(b, root0) == 1)).isTrue();

        TreeFetch full = b.swarm().fetchTree(root0);
        assertThat(runUntil(Duration.ofSeconds(30), full::complete)).as(full.trace().toString()).isTrue();
        assertThat(b.store().complete(root0)).isTrue();
        assertThat(full.progress().bytesReceived()).as(full.trace().toString()).isEqualTo(mapBytes);
        b.swarm().hold(root0);
        assertThat(runUntil(Duration.ofSeconds(10), () -> holdersComplete(a, root0) == 2)).isTrue();

        PMap<String, String> v1 = v0.assoc(key(1234), "changed");
        String root1 = v1.root();
        long delta = bytesNotIn(a.store(), root1, root0);
        a.swarm().hold(root1);
        assertThat(runUntil(Duration.ofSeconds(10), () -> holdersComplete(b, root1) == 1)).isTrue();

        TreeFetch fetch = b.swarm().fetchTree(root1, List.of(root0));
        assertThat(runUntil(Duration.ofSeconds(30), fetch::complete)).as(fetch.trace().toString()).isTrue();
        assertThat(b.store().complete(root1)).isTrue();
        TreeFetch.Progress p = fetch.progress();
        System.out.printf("PD-Q-PRF-01: %d entries in %d bytes (%d nodes, %d levels below the root); one assoc shipped %d bytes"
                        + " for a delta of %d bytes in %d nodes, %d requests, %d round trips%n",
                ENTRIES, mapBytes, a.store().reachable(root0).size(), levelsBelowRoot(a.store(), root0),
                p.bytesReceived(), delta, p.nodesVerified(), p.requestsSent(), p.roundTrips());
        assertThat(p.bytesReceived()).as(fetch.trace().toString()).isEqualTo(delta).isLessThanOrEqualTo(8 * 1024);
        assertThat(p.roundTrips()).as("PD-Q-PRF-06 with the base held").isLessThanOrEqualTo(2);

        PMap<String, String> onB = PMap.load(b.store(), b.swarm().loader(), root1, STR, STR);
        assertThat(onB.count()).isEqualTo(ENTRIES);
        assertThat(onB.get(key(1234))).contains("changed");
        Map<String, String> expected = expected(7);
        assertThat(onB.get(key(7))).contains(expected.get(key(7)));
        assertThat(onB.get(key(9999))).contains(expected.get(key(9999)));
        assertThat(b.swarm().fetcher().inFlight()).isEmpty();
        assertThat(b.content().treeRoots()).contains(root0, root1);
        assertThat(b.swarm().release(root0)).isZero();
        assertThat(b.content().treeRoots()).doesNotContain(root0);
    }

    @Test
    void aOneByteEditOfAThirtyTwoMiBBlobShipsTheEditedChunkAndItsPath() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        byte[] bytes = seededBytes(BLOB_BYTES, 42);
        PBlob b0 = PBlob.of(bytes, a.store());
        String root0 = b0.root();
        long blobBytes = bytesUnder(a.store(), root0);
        a.swarm().hold(root0);
        assertThat(runUntil(Duration.ofSeconds(10), () -> holdersComplete(b, root0) == 1)).isTrue();

        long started = System.nanoTime();
        TreeFetch full = b.swarm().fetchTree(root0);
        assertThat(runUntil(Duration.ofSeconds(60), full::complete)).as(full.trace().toString()).isTrue();
        long fullMillis = (System.nanoTime() - started) / 1_000_000;
        assertThat(b.store().complete(root0)).isTrue();
        assertThat(full.progress().bytesReceived()).isEqualTo(blobBytes);
        b.swarm().hold(root0);

        long offset = 17_000_001L;
        byte edited = (byte) (bytes[(int) offset] ^ 0x5a);
        PBlob b1 = b0.splice(offset, 1, new byte[]{edited});
        String root1 = b1.root();
        assertThat(root1).isNotEqualTo(root0);
        BlobTree.Entry chunk = chunkAt(b1, offset);
        long delta = bytesNotIn(a.store(), root1, root0);
        a.swarm().hold(root1);
        assertThat(runUntil(Duration.ofSeconds(10), () -> holdersComplete(b, root1) == 1)).isTrue();

        TreeFetch fetch = b.swarm().fetchTree(root1, List.of(root0));
        assertThat(runUntil(Duration.ofSeconds(30), fetch::complete)).as(fetch.trace().toString()).isTrue();
        assertThat(b.store().complete(root1)).isTrue();
        TreeFetch.Progress p = fetch.progress();
        System.out.printf("PD-Q-PRF-02: %d MiB blob of %d chunks (%d bytes, height %d) fetched whole in %d ms, %d requests, %d round trips;"
                        + " a one-byte edit shipped %d bytes for a delta of %d bytes in %d nodes (edited chunk %d bytes), %d requests, %d round trips%n",
                BLOB_BYTES >> 20, b0.chunks().size(), blobBytes, b0.tree().height(), fullMillis,
                full.progress().requestsSent(), full.progress().roundTrips(),
                p.bytesReceived(), delta, p.nodesVerified(), chunk.length(), p.requestsSent(), p.roundTrips());
        assertThat(p.bytesReceived()).isEqualTo(delta).isLessThanOrEqualTo(chunk.length() + 16 * 1024);
        assertThat(p.roundTrips()).as("PD-Q-PRF-06 with the base held").isLessThanOrEqualTo(2);

        PBlob onB = PBlob.load(b.store(), root1);
        assertThat(onB.size()).isEqualTo(BLOB_BYTES);
        assertThat(onB.read(offset, 1)[0]).isEqualTo(edited);
        assertThat(onB.read(offset - 1, 1)[0]).isEqualTo(bytes[(int) offset - 1]);
        assertThat(onB.read(offset + 1, 1)[0]).isEqualTo(bytes[(int) offset + 1]);
    }

    @Test
    void aPeerWithoutTheBaseCatchesUpWithinDepthPlusOneRoundTrips() throws Exception {
        Peer a = newPeer("a", 1);
        Peer c = newPeer("c", 3, "a");
        for (int i = 0; i < 4; i++) {
            tickAll();
        }
        PMap<String, String> v0 = buildMap(a.store(), 7);
        String root0 = v0.root();
        PMap<String, String> v1 = v0.assoc(key(1234), "changed");
        String root1 = v1.root();
        a.swarm().hold(root0);
        a.swarm().hold(root1);
        assertThat(runUntil(Duration.ofSeconds(10), () -> holdersComplete(c, root1) == 1 && holdersComplete(c, root0) == 1)).isTrue();
        assertThat(c.store().size()).isZero();

        // scenario 4: c names a base it does not hold; the holder prunes against it and c walks its own frontier
        TreeFetch fetch = c.swarm().fetchTree(root1, List.of(root0));
        assertThat(runUntil(Duration.ofSeconds(30), fetch::complete)).as(fetch.trace().toString()).isTrue();
        assertThat(c.store().complete(root1)).isTrue();
        int depth = levelsBelowRoot(a.store(), root1);
        TreeFetch.Progress p = fetch.progress();
        System.out.printf("PD-Q-PRF-06: catch-up of %d nodes (%d levels below the root) without the base: %d round trips, %d requests, %d bytes%n",
                p.nodesVerified(), depth, p.roundTrips(), p.requestsSent(), p.bytesReceived());
        assertThat(p.roundTrips()).isLessThanOrEqualTo(depth + 1);
        assertThat(p.bytesReceived()).isEqualTo(bytesUnder(a.store(), root1));
        PMap<String, String> onC = PMap.load(c.store(), root1, STR, STR);
        assertThat(onC.get(key(1234))).contains("changed");
        assertThat(onC.count()).isEqualTo(ENTRIES);
    }

    @Test
    void aLazyMapReadsTwentyKeysThroughTheLoaderAndStoresOnlyTheirPaths() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        Peer d = newPeer("d", 4, "a");
        for (int i = 0; i < 6; i++) {
            tickAll();
        }
        PMap<String, String> v0 = buildMap(a.store(), 7);
        PMap<String, String> v1 = v0.assoc(key(1234), "changed");
        String root1 = v1.root();
        a.swarm().hold(root1);
        TreeFetch toB = b.swarm().fetchTree(root1);
        assertThat(runUntil(Duration.ofSeconds(30), toB::complete)).as(toB.trace().toString()).isTrue();
        b.swarm().hold(root1);
        assertThat(runUntil(Duration.ofSeconds(10), () -> holdersComplete(d, root1) == 2)).isTrue();
        assertThat(d.store().size()).isZero();

        // d opens the map with the loader alone; the gets run off the test thread, which keeps ticking
        d.swarm().fetcher().follow(root1);
        AtomicInteger loads = new AtomicInteger();
        NodeLoader counting = cid -> {
            loads.incrementAndGet();
            return d.swarm().loader().load(cid);
        };
        Map<String, String> expected = expected(7);
        expected.put(key(1234), "changed");
        Random random = new Random(99);
        List<String> keys = new ArrayList<>();
        keys.add(key(1234));
        while (keys.size() < 20) {
            keys.add(key(random.nextInt(ENTRIES)));
        }
        CompletableFuture<Map<String, Optional<String>>> reads = CompletableFuture.supplyAsync(() -> {
            PMap<String, String> lazy = PMap.load(d.store(), counting, root1, STR, STR);
            Map<String, Optional<String>> got = new HashMap<>();
            for (String key : keys) {
                got.put(key, lazy.get(key));
            }
            got.put("#count", Optional.of(Long.toString(lazy.count())));
            return got;
        }, r -> Thread.ofVirtual().start(r));
        assertThat(runUntil(Duration.ofSeconds(60), reads::isDone)).as("lazy reads finished").isTrue();
        Map<String, Optional<String>> got = reads.get();
        for (String key : keys) {
            assertThat(got.get(key)).as(key).contains(expected.get(key));
        }
        assertThat(got.get("#count")).contains(Long.toString(ENTRIES));

        int depth = levelsBelowRoot(a.store(), root1);
        int pathNodes = depth + 1;
        System.out.printf("PD-V-06 end to end: 20 lazy gets loaded %d nodes over the swarm (%d bytes); the store holds %d of %d nodes, path length %d%n",
                loads.get(), d.store().bytesUsed(), d.store().size(), a.store().reachable(root1).size(), pathNodes);
        assertThat(d.store().size()).isLessThanOrEqualTo(loads.get()).isLessThanOrEqualTo(20 * pathNodes);
        assertThat(d.store().complete(root1)).isFalse();
        assertThat(d.swarm().fetcher().inFlight()).isEmpty();
        assertThat(d.content().treeRoots()).as("a bare batch publishes no holding").isEmpty();
    }

    @Test
    void aBlobFetchFromThreeHoldersCompletesWhenOneDiesPartWay() throws Exception {
        Peer a = newPeer("a", 1);
        Peer b = newPeer("b", 2, "a");
        Peer c = newPeer("c", 3, "a");
        Peer d = newPeer("d", 4, "a");
        for (int i = 0; i < 6; i++) {
            tickAll();
        }
        byte[] bytes = seededBytes(BLOB_BYTES, 42);
        String root = null;
        for (Peer holder : List.of(a, b, c)) {
            PBlob blob = PBlob.of(bytes, holder.store());
            root = blob.root();
            holder.swarm().hold(root);
        }
        String blobRoot = root;
        long blobBytes = bytesUnder(a.store(), blobRoot);
        int nodes = a.store().reachable(blobRoot).size();
        assertThat(runUntil(Duration.ofSeconds(10), () -> holdersComplete(d, blobRoot) == 3)).isTrue();
        // PD-Q-REL-04: a is the preferred holder, so it answers the root and walks subtrees; its frames to d
        // queue in the fabric and the test lets them through one at a time until a has delivered a quarter
        // of the blob, then a dies with the rest of its walk undelivered
        network.hold("a", "d");
        TreeFetch fetch = d.swarm().fetchTree(blobRoot, List.of(), FetchOptions.defaults().prefer(a.id()));
        long target = blobBytes / 4;
        assertThat(runUntil(Duration.ofSeconds(60), () -> {
            if (fetch.progress().bytesBySource().getOrDefault(a.id(), 0L) >= target) {
                return true;
            }
            network.releaseOne("a", "d");
            return false;
        })).as(fetch.trace().toString()).isTrue();
        assertThat(fetch.complete()).isFalse();
        int verifiedAtDeath = fetch.progress().nodesVerified();
        kill(a);
        assertThat(runUntil(Duration.ofSeconds(60), fetch::complete)).as(fetch.trace().toString()).isTrue();
        assertThat(d.store().complete(blobRoot)).isTrue();
        TreeFetch.Progress p = fetch.progress();
        long fromA = p.bytesBySource().getOrDefault(a.id(), 0L);
        long fromOthers = p.bytesBySource().getOrDefault(b.id(), 0L) + p.bytesBySource().getOrDefault(c.id(), 0L);
        assertThat(fromA).as("a served part of its walk before it died").isGreaterThanOrEqualTo(target).isLessThan(blobBytes);
        assertThat(fromOthers).as("b and c served the rest").isGreaterThan(0L);
        assertThat(fromA + fromOthers).isEqualTo(blobBytes);
        assertThat(p.bytesReceived()).as("the part a delivered was not shipped again: " + fetch.trace())
                .isLessThanOrEqualTo((long) (1.1 * blobBytes));
        System.out.printf("PD-Q-REL-04 shape: a died after %d of %d nodes (%d of %d bytes from a); b and c served %d bytes,"
                        + " %d bytes received in all, %d requests, %d round trips%n",
                verifiedAtDeath, nodes, fromA, blobBytes, fromOthers, p.bytesReceived(), p.requestsSent(), p.roundTrips());
        assertThat(PBlob.load(d.store(), blobRoot).read(BLOB_BYTES - 16, 16)).isEqualTo(java.util.Arrays.copyOfRange(bytes, BLOB_BYTES - 16, BLOB_BYTES));
    }

    @Test
    void attachRefusesASwarmBuiltOverAnotherStore() throws Exception {
        Peer a = newPeer("a", 1);
        NodeStore other = NodeStore.inMemory();
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> PdtSwarm.attach(a.content(), other, DEADLINE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("NodeStoreTreeStore");
        assertThat(a.content().treeStore()).containsInstanceOf(NodeStoreTreeStore.class);
    }
}
