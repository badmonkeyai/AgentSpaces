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
package ai.badmonkey.agentspaces.pdt.refs;

import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Matchers;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.MemoryPieceStore;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.Pdt;
import ai.badmonkey.agentspaces.pdt.values.Codec;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.blocks.MemoryBlockStore;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.test.SimNetwork;
import ai.badmonkey.agentspaces.test.TestClock;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * The sim fleet of the ref tests: the {@code newPeer}, {@code tickAll}, and
 * {@code runUntil} helper of {@code PdtSwarmTest}, with a second replicated
 * space per peer for the refs and a {@link Pdt} facade built before the content
 * swarm (so the swarm takes {@code pdt.treeStore()}) and attached after. Frames
 * travel inline, ticks run on the test thread, and leases run on the test clock.
 */
class RefFleet {

    static final Codec<String> STR = Codec.of(String.class);
    static final Duration DEADLINE = Duration.ofSeconds(30);
    static final String REF_SPACE = "refs";

    final TestClock clock = TestClock.create();
    final SimNetwork network = new SimNetwork();
    final List<Peer> peers = new ArrayList<>();
    final CborCodec codec = CborCodec.defaultCodec();
    final GroupId groupId = GroupId.of("zPdtRefs");
    final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zPdtRefs", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "content",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    final Map<PeerId, CapabilityAdvertisement> ads = new ConcurrentHashMap<>();
    final Set<String> dead = new HashSet<>();
    final Map<String, java.util.concurrent.atomic.AtomicLong> nodeTickNanos = new ConcurrentHashMap<>();
    final java.util.concurrent.atomic.AtomicLong ticks = new java.util.concurrent.atomic.AtomicLong();

    record Peer(String name, PeerIdentity identity, PeerNode node, GroupRuntime runtime, ReplicatedSpace contentSpace,
                ReplicatedSpace refSpace, Content content, NodeStore store, Pdt pdt, java.util.concurrent.atomic.AtomicLong pdtTickNanos,
                java.util.concurrent.atomic.AtomicLong contentTickNanos) {
        PeerId id() {
            return node.peerId();
        }
    }

    /** Per-peer choices; {@code store} reuses a node store, the shape of a restart over a durable store. */
    record Spec(Consumer<Pdt.Builder> pdt, Consumer<ReplicatedSpace.Builder> refSpace, NodeStore store) {
        static Spec defaults() {
            return new Spec(b -> { }, b -> { }, null);
        }

        Spec pdt(Consumer<Pdt.Builder> value) {
            return new Spec(value, refSpace, store);
        }

        Spec refSpace(Consumer<ReplicatedSpace.Builder> value) {
            return new Spec(pdt, value, store);
        }

        Spec store(NodeStore value) {
            return new Spec(pdt, refSpace, value);
        }
    }

    void close() {
        peers.forEach(p -> {
            if (!dead.contains(p.name())) {
                p.pdt().close();
                p.content().close();
                p.node().close();
            }
        });
    }

    Peer newPeer(String address, long seed, String... seedAddresses) throws IOException {
        return newPeer(address, seed, Spec.defaults(), seedAddresses);
    }

    Peer newPeer(String address, long seed, Spec spec, String... seedAddresses) throws IOException {
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
        ReplicatedSpace.Builder refBuilder = ReplicatedSpace.builder(runtime, REF_SPACE, identity, "refs")
                .clock(clock).settleWindow(Duration.ZERO);
        spec.refSpace().accept(refBuilder);
        ReplicatedSpace refSpace = refBuilder.build();
        NodeStore store = spec.store() != null ? spec.store() : new NodeStore(new MemoryBlockStore(), clock);
        Pdt.Builder pdtBuilder = Pdt.builder(refSpace).store(store).clock(clock).loadTimeout(DEADLINE)
                .collectEvery(Duration.ZERO);
        spec.pdt().accept(pdtBuilder);
        Pdt pdt = pdtBuilder.build();
        Content content = Content.builder(new CapabilityPipes(runtime, codec), runtime, contentSpace, identity.peerId())
                .codec(codec).clock(clock).store(new MemoryPieceStore(clock, 0)).blocks(blocks).random(new Random(seed))
                .treeStore(pdt.treeStore()).advertisements(peer -> Optional.ofNullable(ads.get(peer))).build();
        ads.put(identity.peerId(), content.describe(groupId));
        java.util.concurrent.atomic.AtomicLong pdtTickNanos = new java.util.concurrent.atomic.AtomicLong();
        java.util.concurrent.atomic.AtomicLong contentTickNanos = new java.util.concurrent.atomic.AtomicLong();
        runtime.onTick(() -> {
            long t = System.nanoTime();
            content.tick();
            contentTickNanos.addAndGet(System.nanoTime() - t);
        });
        content.start();
        pdt.attach(content).start();
        runtime.onTick(() -> {
            long t = System.nanoTime();
            pdt.tick();
            pdtTickNanos.addAndGet(System.nanoTime() - t);
        });
        Peer peer = new Peer(address, identity, node, runtime, contentSpace, refSpace, content, store, pdt, pdtTickNanos, contentTickNanos);
        peers.add(peer);
        return peer;
    }

    void kill(Peer peer) {
        dead.add(peer.name());
        peer.pdt().close();
        peer.content().close();
        peer.node().close();
    }

    void tickAll() {
        for (Peer p : peers) {
            if (!dead.contains(p.name())) {
                long t = System.nanoTime();
                p.node().tick();
                nodeTickNanos.computeIfAbsent(p.name(), k -> new java.util.concurrent.atomic.AtomicLong()).addAndGet(System.nanoTime() - t);
            }
        }
        ticks.incrementAndGet();
        clock.advance(Duration.ofMillis(250));
    }

    void settle(int ticks) {
        for (int i = 0; i < ticks; i++) {
            tickAll();
        }
    }

    boolean runUntil(Duration max, BooleanSupplier until) throws InterruptedException {
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

    /** Every live peer's store holds the root complete. */
    boolean allHold(String root) {
        for (Peer p : peers) {
            if (!dead.contains(p.name()) && !p.store().complete(root)) {
                return false;
            }
        }
        return true;
    }

    /** The live head entries of a name in a peer's ref space, as the space holds them. */
    static List<Space.Entry<PRef>> headEntries(Peer peer, String name) {
        return peer.refSpace().readAllEntries(Template.of(PRef.class).whereTag(PRef.NAME_TAG, Matchers.eq(name)), 1024);
    }

    static long bytesNotIn(NodeStore store, String root, String base) {
        Set<String> shared = base == null ? Set.of() : store.reachable(base);
        long total = 0;
        for (String cid : store.reachable(root)) {
            if (!shared.contains(cid)) {
                total += store.get(cid).orElseThrow().length;
            }
        }
        return total;
    }

    /** Writes a head for a value by hand, with the nodes not under {@code baseRoot} inline, as a forking writer would. */
    static PRef writeHead(Peer peer, String name, String kind, String root, String baseRoot, long seq, long size, Duration lease) {
        return writeHead(peer, name, kind, root, baseRoot, seq, size, lease, null);
    }

    /** As {@link #writeHead(Peer, String, String, String, String, long, long, Duration)}, with the given attrs. */
    static PRef writeHead(Peer peer, String name, String kind, String root, String baseRoot, long seq, long size,
                          Duration lease, java.util.Map<String, String> attrs) {
        Set<String> shared = baseRoot == null ? Set.of() : peer.store().reachable(baseRoot);
        List<byte[]> nodes = new ArrayList<>();
        for (String cid : peer.store().reachable(root)) {
            if (!shared.contains(cid)) {
                nodes.add(peer.store().get(cid).orElseThrow());
            }
        }
        PRef head = new PRef(PRef.VERSION, name, kind, root, baseRoot == null ? List.of() : List.of(baseRoot), seq, size,
                nodes.isEmpty() ? null : nodes, PRef.MODE_MULTI, PRef.SEALING_NONE, null, attrs);
        if (head.violation().isPresent()) {
            throw new IllegalStateException(head.violation().get());
        }
        peer.refSpace().write(head, Lease.of(lease), head.tags());
        return head;
    }

    static String key(int i) {
        return "key-" + i;
    }

    static String value(int chars, Random random) {
        char[] out = new char[chars];
        for (int c = 0; c < out.length; c++) {
            out[c] = (char) ('a' + random.nextInt(26));
        }
        return new String(out);
    }
}
