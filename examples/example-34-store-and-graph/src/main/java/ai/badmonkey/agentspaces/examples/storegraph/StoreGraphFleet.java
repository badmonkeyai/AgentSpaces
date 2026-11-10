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
package ai.badmonkey.agentspaces.examples.storegraph;

import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityPipes;
import ai.badmonkey.agentspaces.capabilities.runtime.CapabilityRuntime;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.GroupKey;
import ai.badmonkey.agentspaces.common.crypto.GroupKeyRing;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.FilePieceStore;
import ai.badmonkey.agentspaces.discovery.AdCache;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.Pdt;
import ai.badmonkey.agentspaces.pdt.store.PackNodeStore;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.transport.TcpTransport;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;
import ai.badmonkey.agentspaces.store.ContentStore;
import ai.badmonkey.agentspaces.store.StorePolicy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * The peers of example 34 (srad/agentspaces-pdt2.md §7.3, P2-O-03). Every peer joins one group, founded by
 * text, with the content swarm at PDT feature level 2, a replica of the ref space {@value #REF_SPACE}, and a
 * {@link Pdt} facade over a pack node store on disk. A peer whose {@link Spec} names a {@link StorePolicy} also
 * runs a {@link ContentStore}: it publishes a {@code content-store} card, takes signed pins, holds their roots,
 * answers challenges, and reconciles with the other stores.
 *
 * <p>A {@link Spec} in the sealed-nodes mode seals every node this peer writes under the group's content key, and a
 * keyless peer in that mode (the blind store) stores, serves, and walks the sealed nodes without reading them. A
 * store follows the ref space lazily: it holds what a requester pinned on it and nothing a ref names.
 *
 * <p>The demo runs every peer on {@code tcp}; a spec's listeners map a scheme ({@code tcp}, {@code tls},
 * {@code dtls}) to a loopback port, and a seed is {@code host:port} for tcp or {@code scheme://host:port}.
 */
public final class StoreGraphFleet {

    /** The space that carries the ref heads ({@code agentspaces.pdt.space}). */
    public static final String REF_SPACE = "pdt";

    /** The content swarm's space. */
    public static final String CONTENT_SPACE = "content";

    /** How long a read waits for a node it lacks. */
    static final Duration LOAD_TIMEOUT = Duration.ofSeconds(120);

    /** How often a store refreshes its card and walks its holdings; a host's default is 10 s. */
    static final Duration HOLDING_INTERVAL = Duration.ofSeconds(2);

    static final CborCodec CODEC = CborCodec.defaultCodec();

    private StoreGraphFleet() {
    }

    /**
     * How one peer starts.
     *
     * @param founding     the group's founding text
     * @param listeners    the bindings to listen on, scheme to loopback port; empty for a dial-only peer
     * @param seeds        the seeds
     * @param dir          the directory of the pack node store and the piece store
     * @param identity     the identity, or null for a fresh one; a store restarts under its own
     * @param sealing      {@code none} or {@code sealed-nodes}
     * @param key          the group's content key in the sealed-nodes mode, or null for a keyless peer
     * @param store        the store policy of a content store, or null for a plain member
     * @param storeName    the store's name, which keys its ledger across a restart
     * @param maxPackBytes the size at which a pack file closes, so a restart can lose one pack in the middle
     * @param roles        the peer's roles: {@code RELAY} for the peer a dial-only client seeds at, so the client
     *                     reaches the other members through it
     */
    public record Spec(String founding, Map<String, Integer> listeners, List<String> seeds, Path dir, PeerIdentity identity,
                       String sealing, GroupKey key, StorePolicy store, String storeName, long maxPackBytes,
                       Set<PeerAdvertisement.PeerRole> roles) {

        public Spec {
            Objects.requireNonNull(founding, "founding");
            listeners = Map.copyOf(listeners);
            seeds = List.copyOf(seeds);
            Objects.requireNonNull(dir, "dir");
            Objects.requireNonNull(sealing, "sealing");
            roles = Set.copyOf(roles);
        }

        /** A plain member on tcp. */
        public static Spec member(String founding, int port, List<String> seeds, Path dir) {
            return new Spec(founding, Map.of("tcp", port), seeds, dir, null, PRef.SEALING_NONE, null, null, null,
                    PackNodeStore.DEFAULT_MAX_PACK_BYTES, Set.of());
        }

        public Spec listeners(Map<String, Integer> value) {
            return new Spec(founding, value, seeds, dir, identity, sealing, key, store, storeName, maxPackBytes, roles);
        }

        public Spec seeds(List<String> value) {
            return new Spec(founding, listeners, value, dir, identity, sealing, key, store, storeName, maxPackBytes, roles);
        }

        public Spec identity(PeerIdentity value) {
            return new Spec(founding, listeners, seeds, dir, value, sealing, key, store, storeName, maxPackBytes, roles);
        }

        /** The sealed-nodes mode under a key, or keyless when the key is null. */
        public Spec sealedNodes(GroupKey value) {
            return new Spec(founding, listeners, seeds, dir, identity, PRef.SEALING_SEALED_NODES, value, store, storeName,
                    maxPackBytes, roles);
        }

        public Spec store(String name, StorePolicy policy) {
            return new Spec(founding, listeners, seeds, dir, identity, sealing, key, policy, name, maxPackBytes, roles);
        }

        public Spec roles(Set<PeerAdvertisement.PeerRole> value) {
            return new Spec(founding, listeners, seeds, dir, identity, sealing, key, store, storeName, maxPackBytes, value);
        }

        public Spec maxPackBytes(long value) {
            return new Spec(founding, listeners, seeds, dir, identity, sealing, key, store, storeName, value, roles);
        }
    }

    /** One running peer. */
    public static final class Peer implements AutoCloseable {
        private final String name;
        private final PeerIdentity identity;
        private final PeerNode node;
        private final GroupRuntime runtime;
        private final DiscoveryService discovery;
        private final ReplicatedSpace contentSpace;
        private final ReplicatedSpace refSpace;
        private final CapabilityRuntime capabilities;
        private final Content content;
        private final PackNodeStore pack;
        private final Pdt pdt;
        private final ContentStore contentStore;
        private final ScheduledExecutorService ticker;
        private final List<AutoCloseable> transports;
        private volatile boolean closed;

        private Peer(String name, PeerIdentity identity, PeerNode node, GroupRuntime runtime, DiscoveryService discovery,
                     ReplicatedSpace contentSpace, ReplicatedSpace refSpace, CapabilityRuntime capabilities, Content content,
                     PackNodeStore pack, Pdt pdt, ContentStore contentStore, ScheduledExecutorService ticker,
                     List<AutoCloseable> transports) {
            this.name = name;
            this.identity = identity;
            this.node = node;
            this.runtime = runtime;
            this.discovery = discovery;
            this.contentSpace = contentSpace;
            this.refSpace = refSpace;
            this.capabilities = capabilities;
            this.content = content;
            this.pack = pack;
            this.pdt = pdt;
            this.contentStore = contentStore;
            this.ticker = ticker;
            this.transports = transports;
        }

        public String name() {
            return name;
        }

        public PeerIdentity identity() {
            return identity;
        }

        public PeerId id() {
            return node.peerId();
        }

        public PeerNode node() {
            return node;
        }

        public GroupRuntime runtime() {
            return runtime;
        }

        public DiscoveryService discovery() {
            return discovery;
        }

        public Content content() {
            return content;
        }

        public Pdt pdt() {
            return pdt;
        }

        /** The facade's node store: a sealed store in the sealed-nodes mode. */
        public NodeStore store() {
            return pdt.store();
        }

        /** The pack node store under the facade. */
        public PackNodeStore pack() {
            return pack;
        }

        /** The content store, or null for a plain member. */
        public ContentStore contentStore() {
            return contentStore;
        }

        public boolean closed() {
            return closed;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            ticker.shutdownNow();
            if (contentStore != null) {
                quietly(contentStore::close);
            }
            quietly(pdt::close);
            quietly(capabilities::close);
            quietly(content::close);
            quietly(refSpace::close);
            quietly(contentSpace::close);
            quietly(node::close);
            for (AutoCloseable transport : transports) {
                quietly(transport);
            }
            quietly(pack::close);
        }

        private static void quietly(AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // closing something that is already down
            }
        }
    }

    /**
     * Starts a peer.
     *
     * @param name a label for the log
     * @param spec how it starts
     * @return the running peer
     * @throws IOException when a port cannot be bound or the store cannot open
     */
    public static Peer start(String name, Spec spec) throws IOException {
        PeerIdentity identity = spec.identity() != null ? spec.identity() : PeerIdentity.generate();
        PeerNode node = PeerNode.builder(identity).roles(spec.roles()).build();
        List<AutoCloseable> transports = new ArrayList<>();
        for (Map.Entry<String, Integer> listener : new TreeMap<>(spec.listeners()).entrySet()) {
            Transport transport = transportFor(listener.getKey(), identity);
            if (transport instanceof AutoCloseable closeable) {
                transports.add(closeable);
            }
            node.listen(transport, "127.0.0.1:" + listener.getValue());
        }
        Set<String> dialSchemes = new TreeSet<>();
        for (String seed : spec.seeds()) {
            int at = seed.indexOf("://");
            dialSchemes.add(at < 0 ? "tcp" : seed.substring(0, at));
        }
        dialSchemes.removeAll(spec.listeners().keySet());
        for (String scheme : dialSchemes) {
            Transport transport = transportFor(scheme, identity);
            if (transport instanceof AutoCloseable closeable) {
                transports.add(closeable);
            }
            node.transport(transport);
        }
        GroupId groupId = GroupId.fromFounding(spec.founding().getBytes(StandardCharsets.UTF_8));
        GroupAdvertisement ad = new GroupAdvertisement("aspace://" + groupId.value(), identity.peerId(), groupId,
                Instant.EPOCH, Duration.ofDays(365), spec.founding(), GroupAdvertisement.MembershipPolicy.OPEN,
                ConflictStrategyType.LEASE_RACE, GroupAdvertisement.GossipParameters.defaults());
        List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
        for (String seed : spec.seeds()) {
            int at = seed.indexOf("://");
            endpoints.add(at < 0 ? new PeerAdvertisement.Endpoint("tcp", seed, 0)
                    : new PeerAdvertisement.Endpoint(seed.substring(0, at), seed.substring(at + 3), 0));
        }
        GroupRuntime runtime = node.joinGroup(ad, GroupMembership.Config.defaults(), endpoints);
        BlockExchange blocks = new BlockExchange(runtime, CODEC);
        ReplicatedSpace contentSpace = ReplicatedSpace.builder(runtime, CONTENT_SPACE, identity, CONTENT_SPACE)
                .blocks(blocks).build();
        ReplicatedSpace refSpace = ReplicatedSpace.builder(runtime, REF_SPACE, identity, "refs").build();
        Files.createDirectories(spec.dir());
        PackNodeStore pack = PackNodeStore.open(spec.dir().resolve("nodes"),
                PackNodeStore.Options.defaults().withMaxPackBytes(spec.maxPackBytes()));
        DiscoveryService discovery = new DiscoveryService(runtime, new AdCache(CODEC, InstantSource.system()), CODEC,
                identity.peerId());
        CapabilityRuntime capabilities = new CapabilityRuntime(runtime, discovery, identity);
        Pdt.Builder pdtBuilder = Pdt.builder(refSpace).store(new NodeStore(pack, InstantSource.system()))
                .loadTimeout(LOAD_TIMEOUT).collectEvery(Duration.ZERO).discovery(discovery).identity(identity)
                .sealing(spec.sealing());
        if (PRef.SEALING_SEALED_NODES.equals(spec.sealing())) {
            pdtBuilder.keyRing(spec.key() == null ? GroupKeyRing.empty() : GroupKeyRing.of(spec.key()));
        }
        if (spec.store() != null) {
            // a store holds what a requester pinned on it, never what a ref happens to name
            pdtBuilder.follow(Pdt.FOLLOW_LAZY);
        }
        Pdt pdt = pdtBuilder.build();
        Content content = Content.builder(new CapabilityPipes(runtime, CODEC), runtime, contentSpace, identity.peerId())
                .codec(CODEC).store(new FilePieceStore(spec.dir().resolve("pieces"), CODEC, InstantSource.system(), 0))
                .blocks(blocks).random(new Random(name.hashCode())).discovery(discovery).treeStore(pdt.treeStore())
                .pdtLevel(Content.PDT_LEVEL).build();
        capabilities.register(content);
        pdt.attach(content).start();
        runtime.onTick(pdt::tick);
        ContentStore contentStore = null;
        if (spec.store() != null) {
            contentStore = ContentStore.builder(pdt, discovery, identity).name(spec.storeName()).policy(spec.store())
                    .holdingInterval(HOLDING_INTERVAL).fetchTimeout(LOAD_TIMEOUT).build();
            runtime.onTick(contentStore::tick);
            contentStore.start();
        }
        ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "tick-" + name);
            t.setDaemon(true);
            return t;
        });
        ticker.scheduleAtFixedRate(() -> {
            try {
                node.tick();
            } catch (RuntimeException e) {
                System.err.println(name + " tick failed: " + e.getMessage());
            }
        }, 0, 100, TimeUnit.MILLISECONDS);
        capabilities.autoRefresh(Duration.ofSeconds(30), ticker);
        return new Peer(name, identity, node, runtime, discovery, contentSpace, refSpace, capabilities, content, pack, pdt,
                contentStore, ticker, transports);
    }

    /** The transport for a scheme; a binding with its own channel certificate is made for the peer's identity. */
    static Transport transportFor(String scheme, PeerIdentity identity) throws IOException {
        try {
            return switch (scheme) {
                case "tcp" -> new TcpTransport();
                case "tls" -> new ai.badmonkey.agentspaces.peering.transport.TlsTcpTransport(identity);
                case "dtls" -> new ai.badmonkey.agentspaces.transport.dtls.DtlsTransport(identity);
                default -> throw new IllegalArgumentException("no binding " + scheme + " (tcp, tls, dtls)");
            };
        } catch (java.security.GeneralSecurityException e) {
            throw new IOException("cannot make the " + scheme + " binding: " + e.getMessage(), e);
        }
    }

    // ------------------------------------------------------------ waits

    /** Polls until a condition holds or the time runs out. */
    public static boolean await(Duration max, BooleanSupplier until) throws InterruptedException {
        long deadline = System.nanoTime() + max.toNanos();
        while (!until.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            Thread.sleep(20);
        }
        return true;
    }

    /**
     * Waits until every peer sees every other and reads it at PDT feature level 2, the level of the store's card,
     * challenges, and reconciles (P2-F-05).
     */
    public static boolean awaitFleet(List<Peer> peers, Duration max) throws InterruptedException {
        return await(max, () -> {
            for (Peer from : peers) {
                if (from.runtime().membership().allMembers().size() < peers.size() - 1) {
                    return false;
                }
                for (Peer to : peers) {
                    if (from != to && from.content().pdtLevel(to.id()) < Content.PDT_LEVEL) {
                        return false;
                    }
                }
            }
            return true;
        });
    }
}
