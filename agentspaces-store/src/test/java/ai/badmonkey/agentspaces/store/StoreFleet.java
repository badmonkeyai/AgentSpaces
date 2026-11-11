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
package ai.badmonkey.agentspaces.store;

import ai.badmonkey.agentspaces.agent.content.SignedPin;
import ai.badmonkey.agentspaces.agent.content.StorePin;
import ai.badmonkey.agentspaces.agent.content.StoreReceipt;
import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.api.ad.CapabilityAdvertisement;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.GroupKey;
import ai.badmonkey.agentspaces.common.crypto.GroupKeyRing;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.MemoryPieceStore;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.Pdt;
import ai.badmonkey.agentspaces.pdt.values.Codec;
import ai.badmonkey.agentspaces.pdt.values.PMap;
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
import java.util.function.UnaryOperator;

/**
 * The sim fleet of the store tests (PDT 2 phase 3): peers on a {@link SimNetwork} with a content space, a ref
 * space, discovery, a {@link Pdt} facade built before its swarm, and, for a store, a {@link ContentStore} over
 * them, ticked with the group. Frames travel inline, ticks run on the test thread, and leases run on the test
 * clock; the stores' take loops run on their own threads, as in a host.
 */
final class StoreFleet implements AutoCloseable {

    static final Codec<String> STR = Codec.of(String.class);
    static final Duration HOLDING = Duration.ofSeconds(2);
    /*
     * The product default. Each tick moves the test clock 250 ms while the stores' fetches run on their own
     * threads, so on a loaded machine the clock outruns them: a 60 s timeout was 240 ticks, under a second of
     * real time, and a starved fetch was abandoned though nothing was wrong (the winner of a pin race then
     * held its take with nothing fetched). Ten minutes still abandons a truly stuck fetch inside a test's
     * real-time wait; tests that probe the timeout set their own (PinProgressTest).
     */
    static final Duration FETCH_TIMEOUT = Duration.ofMinutes(10);

    final TestClock clock = TestClock.create();
    final SimNetwork network = new SimNetwork();
    final CborCodec codec = CborCodec.defaultCodec();
    final GroupId groupId = GroupId.of("zStores");
    final GroupAdvertisement groupAd = new GroupAdvertisement(
            "aspace://zStores", PeerIdentity.generate().peerId(), groupId,
            java.time.Instant.EPOCH, Duration.ofDays(1), "content",
            GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
            GroupAdvertisement.GossipParameters.defaults());
    final Map<PeerId, CapabilityAdvertisement> ads = new ConcurrentHashMap<>();
    final List<Peer> peers = new ArrayList<>();
    final Set<String> dead = new HashSet<>();

    record Peer(String name, PeerIdentity identity, PeerNode node, GroupRuntime runtime, ReplicatedSpace contentSpace,
                DiscoveryService discovery, Content content, NodeStore store, Pdt pdt, ContentStore contentStore) {
        PeerId id() {
            return node.peerId();
        }
    }

    /**
     * Per-peer choices.
     *
     * @param store    a store policy makes the peer a content store; null for a plain member
     * @param nodes    a node store to reuse, the shape of a restart over a durable store; null for a fresh one
     * @param identity an identity to reuse; null for a fresh one
     * @param sealing  the facade's sealing, {@code none} or {@code sealed-nodes}
     * @param key      the group key of a sealed-nodes member; null for a keyless one
     * @param store2   further store builder settings
     * @param content  further swarm builder settings
     * @param granted  whether a store starts with an authorizer that permits every requester; a grant store
     *                 without one waits (F2)
     */
    record Spec(StorePolicy store, NodeStore nodes, PeerIdentity identity, String sealing, GroupKey key,
                UnaryOperator<ContentStore.Builder> store2, UnaryOperator<Content.Builder> content, boolean granted) {
        static Spec member() {
            return new Spec(null, null, null, PRef.SEALING_NONE, null, b -> b, b -> b, true);
        }

        static Spec store(StorePolicy policy) {
            return member().policy(policy);
        }

        Spec policy(StorePolicy value) {
            return new Spec(value, nodes, identity, sealing, key, store2, content, granted);
        }

        Spec nodes(NodeStore value) {
            return new Spec(store, value, identity, sealing, key, store2, content, granted);
        }

        Spec identity(PeerIdentity value) {
            return new Spec(store, nodes, value, sealing, key, store2, content, granted);
        }

        Spec sealedNodes(GroupKey value) {
            return new Spec(store, nodes, identity, PRef.SEALING_SEALED_NODES, value, store2, content, granted);
        }

        Spec storeBuilder(UnaryOperator<ContentStore.Builder> value) {
            return new Spec(store, nodes, identity, sealing, key, value, content, granted);
        }

        Spec contentBuilder(UnaryOperator<Content.Builder> value) {
            return new Spec(store, nodes, identity, sealing, key, store2, value, granted);
        }

        /** A store built without an authorizer (F2). */
        Spec withoutAuthorizer() {
            return new Spec(store, nodes, identity, sealing, key, store2, content, false);
        }
    }

    Peer newPeer(String address, long seed, Spec spec, String... seedAddresses) throws IOException {
        PeerIdentity identity = spec.identity() != null ? spec.identity() : PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).clock(clock).randomSeed(seed).build();
        node.listen(network.register(address), address);
        List<PeerAdvertisement.Endpoint> seeds = new ArrayList<>();
        for (String s : seedAddresses) {
            seeds.add(new PeerAdvertisement.Endpoint("mem", s, 0));
        }
        GroupRuntime runtime = node.joinGroup(groupAd, GroupMembership.Config.defaults(), seeds);
        BlockExchange blocks = new BlockExchange(runtime, codec, clock, 0, BlockExchange.DEFAULT_MAX_BLOCK_BYTES);
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(codec, clock), codec, identity.peerId());
        ReplicatedSpace contentSpace = ReplicatedSpace.builder(runtime, "content", identity, "content")
                .clock(clock).settleWindow(Duration.ZERO).blocks(blocks).build();
        ReplicatedSpace refSpace = ReplicatedSpace.builder(runtime, "refs", identity, "refs")
                .clock(clock).settleWindow(Duration.ZERO).build();
        NodeStore nodes = spec.nodes() != null ? spec.nodes() : new NodeStore(new MemoryBlockStore(), clock);
        Pdt.Builder pdtBuilder = Pdt.builder(refSpace).store(nodes).clock(clock).loadTimeout(Duration.ofSeconds(30))
                .collectEvery(Duration.ZERO).discovery(discovery).identity(identity).sealing(spec.sealing());
        if (PRef.SEALING_SEALED_NODES.equals(spec.sealing())) {
            pdtBuilder.keyRing(spec.key() == null ? GroupKeyRing.empty() : GroupKeyRing.of(spec.key()));
        }
        Pdt pdt = pdtBuilder.build();
        Content.Builder contentBuilder = Content.builder(new CapabilityPipes(runtime, codec), runtime, contentSpace, identity.peerId())
                .codec(codec).clock(clock).store(new MemoryPieceStore(clock, 0)).blocks(blocks).random(new Random(seed))
                .treeStore(pdt.treeStore()).advertisements(peer -> Optional.ofNullable(ads.get(peer)));
        if (spec.store() != null) {
            contentBuilder.pdtLevel(Content.PDT_LEVEL);
        }
        Content content = spec.content().apply(contentBuilder).build();
        ads.put(identity.peerId(), content.describe(groupId));
        runtime.onTick(content::tick);
        content.start();
        pdt.attach(content).start();
        runtime.onTick(pdt::tick);
        ContentStore contentStore = null;
        if (spec.store() != null) {
            ContentStore.Builder storeBuilder = ContentStore.builder(pdt, discovery, identity).name(address)
                    .policy(spec.store()).clock(clock).holdingInterval(HOLDING).takeLease(Duration.ofSeconds(20))
                    .fetchTimeout(FETCH_TIMEOUT);
            if (spec.granted()) {
                storeBuilder.authorizer(GRANT_ALL);
            }
            contentStore = spec.store2().apply(storeBuilder).build();
            ContentStore started = contentStore;
            runtime.onTick(started::tick);
            started.start();
        }
        Peer peer = new Peer(address, identity, node, runtime, contentSpace, discovery, content, pdt.store(), pdt, contentStore);
        peers.add(peer);
        return peer;
    }

    void kill(Peer peer) {
        dead.add(peer.name() + "@" + System.identityHashCode(peer));
        if (peer.contentStore() != null) {
            peer.contentStore().close();
        }
        peer.pdt().close();
        peer.content().close();
        peer.node().close();
    }

    boolean alive(Peer peer) {
        return !dead.contains(peer.name() + "@" + System.identityHashCode(peer));
    }

    void tickAll() {
        for (Peer p : List.copyOf(peers)) {
            if (alive(p)) {
                p.node().tick();
            }
        }
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
            Thread.sleep(3);
        }
        return true;
    }

    /** Ticks the fleet for a stretch of real time, so the stores' take loops scan the pins many times. */
    void runFor(Duration real) throws InterruptedException {
        runUntil(real, () -> false);
    }

    /** A map value of {@code entries} keys on a peer's facade, swapped into a ref; its root is complete in the peer's store. */
    static PMap<String, String> facts(Peer peer, String ref, int entries, long seed) {
        Random random = new Random(seed);
        return peer.pdt().mapRef(ref, STR, STR).swap(m -> {
            PMap<String, String> out = m;
            for (int i = 0; i < entries; i++) {
                out = out.assoc("key-" + i, "secret-value-" + i + "-" + random.nextInt(1_000_000));
            }
            return out;
        });
    }

    /** The live receipts of a root, by holder, as one member's replica shows them. */
    static List<PeerId> receiptHolders(Peer peer, String root) {
        return peer.pdt().stores().receipts(root).stream().map(StoreReceipt::holder).toList();
    }

    /** The authorizer a store starts with unless its spec says otherwise: every requester may fetch every root. */
    static final Authorizer GRANT_ALL = (peer, operation, scope) -> true;

    /**
     * Writes a pin entry as a requester does: the terms under the writer's signature (F1), leased for {@code lease}
     * and good until then.
     */
    EntryHandle writePin(Peer writer, StorePin pin, Duration lease) {
        SignedPin signed = SignedPin.sign(pin, groupId, clock.millis() + lease.toMillis(), writer.identity());
        return writer.contentSpace().write(signed, Lease.of(lease), signed.tags());
    }

    /** An authorizer that permits everything except {@code CONTENT_FETCH} for the refused peers. */
    static Authorizer refusing(Set<PeerId> refused) {
        return (peer, operation, scope) -> operation != Authorizer.Operation.CONTENT_FETCH || !refused.contains(peer);
    }

    @Override
    public void close() {
        for (Peer p : peers) {
            if (alive(p)) {
                kill(p);
            }
        }
    }
}
