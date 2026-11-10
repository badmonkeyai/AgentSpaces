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
package ai.badmonkey.agentspaces.pdt;

import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.common.crypto.GroupKeyRing;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.tree.TreeStore;
import ai.badmonkey.agentspaces.pdt.blob.FileEntry;
import ai.badmonkey.agentspaces.pdt.blob.PBlob;
import ai.badmonkey.agentspaces.pdt.escalation.PdtPayloads;
import ai.badmonkey.agentspaces.pdt.refs.DbMerge;
import ai.badmonkey.agentspaces.pdt.refs.MergeFunction;
import ai.badmonkey.agentspaces.pdt.refs.Ref;
import ai.badmonkey.agentspaces.pdt.refs.Refs;
import ai.badmonkey.agentspaces.pdt.seal.NodeSealer;
import ai.badmonkey.agentspaces.pdt.seal.SealedNodeStore;
import ai.badmonkey.agentspaces.pdt.store.MetaStore;
import ai.badmonkey.agentspaces.pdt.store.NodeCollector;
import ai.badmonkey.agentspaces.pdt.store.Retention;
import ai.badmonkey.agentspaces.pdt.store.StoreThenFetchLoader;
import ai.badmonkey.agentspaces.pdt.swarm.NodeStoreTreeStore;
import ai.badmonkey.agentspaces.pdt.swarm.PdtSwarm;
import ai.badmonkey.agentspaces.pdt.values.Codec;
import ai.badmonkey.agentspaces.pdt.values.PDb;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.pdt.values.PSet;
import ai.badmonkey.agentspaces.pdt.values.PVector;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.InstantSource;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The facade a host builds once per group (srad/agentspaces-pdt.md §6.2, §7.2,
 * PD-C-02): it owns the {@link NodeStore}, the {@link Retention} of delta bases,
 * the {@link NodeCollector} that runs on the tick, the {@link PdtSwarm} over the
 * group's {@link Content} (whose loader is the {@link StoreThenFetchLoader} of
 * PD-V-06), and the {@link Refs} engine over the ref space, and it hands out
 * {@link Ref} handles by name and kind.
 *
 * <p>Two ways to wire it. When the content swarm already exists, built with
 * {@code Content.Builder.treeStore(PdtSwarm.treeStore(store))} over the node
 * store, {@code Pdt.builder(content, space).store(store).build()} attaches to
 * it (a builder with no store adopts the swarm's). When the facade must exist
 * first, {@code Pdt.builder(space).store(store).build()} gives the host
 * {@link #treeStore()} for {@code Content.Builder.treeStore}, and
 * {@link #attach(Content)} joins the built swarm before {@link #start()}.
 * {@link #start}, {@link #tick}, and {@link #close} are wired like the swarm's.
 *
 * <p>Sealing (§3.8). {@code sealing("sealed-nodes")} with {@code keyRing(ring)} makes
 * the facade's store a {@link SealedNodeStore} over the given store's backend (or
 * adopts a sealed store the host built), so every node this peer writes is sealed
 * under the ring's write epoch, every head carries {@code sealing = sealed-nodes}
 * and the epoch in {@code keyEpoch}, and the tree store handed to the swarm serves
 * sealed envelopes to every member, keys or not (PD-P-05). A keyless member runs
 * the same mode over {@link GroupKeyRing#empty()}: it holds and serves what it
 * cannot read. The swarm built after the facade must take {@link #treeStore()},
 * which is why a sealed facade refuses a swarm built over an unsealed store.
 *
 * <pre>{@code
 * Pdt pdt = Pdt.builder(space).store(store).retain(64, Duration.ofHours(1)).build();
 * Content content = Content.builder(pipes, runtime, contentSpace, self).treeStore(pdt.treeStore()).build();
 * pdt.attach(content).start();
 * Ref<PMap<String, Fact>> facts = pdt.ref("facts", PMap.class, Codec.of(String.class), Codec.of(Fact.class));
 * facts.swap(m -> m.assoc(fact.id(), fact));
 * }</pre>
 */
public final class Pdt implements AutoCloseable {

    /** Follow mode: a reader fetches each new head's tree as the head arrives. */
    public static final String FOLLOW_FULL = "full";

    /** Follow mode: a reader fetches nothing on a head and loads nodes on get (PD-V-06). */
    public static final String FOLLOW_LAZY = "lazy";

    /**
     * The facade's counters (PD-C-08).
     *
     * @param refs          handles created
     * @param heads         heads across every ref as last read
     * @param deltasInline  heads whose delta arrived inline (PD-X-07)
     * @param deltasFetched heads whose delta a tree fetch brought
     * @param conflicts     merge decisions of §3.6.1 this reader made
     * @param bytesReceived node bytes received inline or by fetch
     * @param headsIgnored  heads ignored for a fault: malformed, another kind, or no ref at all (S7, PD-P-04)
     * @param nodesDropped  inline nodes dropped for failing verification or reachability (PD-X-07)
     * @param headsRewritten lapsed heads this peer rewrote (PD-R-08)
     * @param basesFetched  bases fetched to keep the version graph held for the common-base walk (PD-R-06)
     * @param nodesHeld     nodes in the store
     * @param bytesHeld     bytes in the store
     * @param entriesTruncated reads of a name that filled {@code Refs.MAX_ENTRIES}, so the heads read then are untrustworthy
     * @param mergeFunctionFailures merge function calls that threw, after which the rule of §3.6.1 decided (PD-R-05)
     * @param headsLate     heads left out of reads while their roots were still loading, a late fetch or a failed one
     *                      that the tick retries ({@code Refs.headsLate()} gives the causes; swarm retries)
     */
    public record Stats(int refs, int heads, long deltasInline, long deltasFetched, long conflicts, long bytesReceived,
                        long headsIgnored, long nodesDropped, long headsRewritten, long basesFetched, int nodesHeld, long bytesHeld,
                        long entriesTruncated, long mergeFunctionFailures, long headsLate) {

        /** The twelve-figure form, kept for callers that predate the truncation, merge-failure, and late-head counters. */
        public Stats(int refs, int heads, long deltasInline, long deltasFetched, long conflicts, long bytesReceived,
                     long headsIgnored, long nodesDropped, long headsRewritten, long basesFetched, int nodesHeld, long bytesHeld) {
            this(refs, heads, deltasInline, deltasFetched, conflicts, bytesReceived, headsIgnored, nodesDropped, headsRewritten,
                    basesFetched, nodesHeld, bytesHeld, 0, 0, 0);
        }
    }

    private final Space refSpace;
    private final NodeStore store;
    private final NodeStoreTreeStore treeStore;
    private final Retention retention;
    private final InstantSource clock;
    private final Duration loadTimeout;
    private final Refs.Config refsConfig;
    private final Duration collectEvery;
    private final int collectorVisits;
    private volatile PdtSwarm swarm;
    private volatile PdtPayloads payloads;
    private volatile Refs refs;
    private volatile NodeCollector collector;
    private volatile ai.badmonkey.agentspaces.pdt.stores.Stores stores;
    private final ai.badmonkey.agentspaces.discovery.DiscoveryService discovery;
    private final Duration challengeDeadline;
    private final ai.badmonkey.agentspaces.pdt.refs.PushPublisher.Config push;
    private final ai.badmonkey.agentspaces.identity.PeerIdentity identity;
    private volatile boolean started;
    private volatile boolean closed;
    private long lastCollectMillis = Long.MIN_VALUE;

    private Pdt(Builder b) {
        this.refSpace = b.refSpace;
        this.clock = b.clock;
        NodeStore chosen = b.store;
        if (chosen == null && b.content != null) {
            chosen = b.content.treeStore()
                    .filter(t -> t instanceof NodeStoreTreeStore)
                    .map(t -> ((NodeStoreTreeStore) t).store())
                    .orElse(null);
        }
        if (chosen == null) {
            chosen = new NodeStore(new ai.badmonkey.agentspaces.peering.blocks.MemoryBlockStore(), clock);
        }
        if (PRef.SEALING_SEALED_NODES.equals(b.sealing) && !(chosen instanceof SealedNodeStore)) {
            if (b.store == null && b.content != null) {
                throw new IllegalStateException("sealing sealed-nodes needs a SealedNodeStore, but the content swarm was built"
                        + " over an unsealed store; build the facade first and give the swarm pdt.treeStore()");
            }
            chosen = new SealedNodeStore(chosen.backend(), clock, new NodeSealer(b.keyRing == null ? GroupKeyRing.empty() : b.keyRing, clock));
        }
        this.store = chosen;
        // PDT 2 phase 4, P2-Z-02: the store's table of intra-chunk deltas follows the builder's options
        store.deltas().retention(b.deltaRetention);
        if (b.deltas != null) {
            store.deltas().enabled(b.deltas && !(store instanceof SealedNodeStore));
        }
        this.treeStore = b.content != null && b.content.treeStore().orElse(null) instanceof NodeStoreTreeStore adapter
                && adapter.store() == store ? adapter : new NodeStoreTreeStore(store, b.quota);
        // over a pack store the retained versions persist beside the pins (PDT 2 phase 1, P2-V-03)
        this.retention = new Retention(b.retainVersions, b.retainAge, clock,
                store.backend() instanceof MetaStore meta ? meta : null);
        this.loadTimeout = b.loadTimeout;
        this.refsConfig = new Refs.Config(b.loadTimeout, b.inlineDelta, FOLLOW_FULL.equals(b.follow), b.sealing,
                b.headLease, b.takeLease, b.takeTimeout, b.takeRetries, b.modes, b.follows, b.cancelGrace);
        this.collectEvery = b.collectEvery;
        this.collectorVisits = b.collectorVisits;
        this.discovery = b.discovery;
        this.challengeDeadline = b.challengeDeadline;
        this.push = b.push;
        this.identity = b.identity;
        if (b.content != null) {
            attach(b.content);
        }
    }

    /** A builder over a ref space; the content swarm attaches later through {@link #attach}. */
    public static Builder builder(Space refSpace) {
        return new Builder(null, refSpace);
    }

    /**
     * A builder over an existing content swarm and a ref space (§7.2). The swarm
     * must have been built with a tree store from {@link PdtSwarm#treeStore} over
     * the node store the builder is given, or over the store the builder adopts
     * from it when none is given.
     */
    public static Builder builder(Content content, Space refSpace) {
        return new Builder(Objects.requireNonNull(content, "content"), refSpace);
    }

    /** The builder of §7.2. */
    public static final class Builder {
        private final Content content;
        private final Space refSpace;
        private NodeStore store;
        private int retainVersions = Retention.DEFAULT_VERSIONS;
        private Duration retainAge = Retention.DEFAULT_AGE;
        private Duration loadTimeout = StoreThenFetchLoader.DEFAULT_DEADLINE;
        private int inlineDelta = PRef.INLINE_DELTA_BYTES;
        private String follow = FOLLOW_FULL;
        private String sealing = PRef.SEALING_NONE;
        private GroupKeyRing keyRing;
        private InstantSource clock = InstantSource.system();
        private Duration headLease = Refs.DEFAULT_HEAD_LEASE;
        private Duration takeLease = Duration.ofSeconds(30);
        private Duration takeTimeout = Duration.ofSeconds(5);
        private int takeRetries = 3;
        private Duration cancelGrace = Duration.ofMillis(Refs.CANCEL_GRACE_MILLIS);
        private Duration collectEvery = Duration.ofMinutes(1);
        private int collectorVisits = NodeCollector.DEFAULT_VISITS_PER_TICK;
        private final Map<String, String> modes = new HashMap<>();
        private final Map<String, Boolean> follows = new HashMap<>();
        private long quota = ai.badmonkey.agentspaces.pdt.store.QuotaNodeStore.UNBOUNDED;
        private ai.badmonkey.agentspaces.discovery.DiscoveryService discovery;
        private Duration challengeDeadline = ai.badmonkey.agentspaces.pdt.stores.Stores.DEFAULT_DEADLINE;
        private ai.badmonkey.agentspaces.pdt.refs.PushPublisher.Config push = ai.badmonkey.agentspaces.pdt.refs.PushPublisher.Config.defaults();
        private ai.badmonkey.agentspaces.identity.PeerIdentity identity;
        private Boolean deltas;
        private Duration deltaRetention = ai.badmonkey.agentspaces.pdt.blob.ChunkDeltas.DEFAULT_RETENTION;

        private Builder(Content content, Space refSpace) {
            this.content = content;
            this.refSpace = Objects.requireNonNull(refSpace, "refSpace");
        }

        /** The node store ({@code agentspaces.pdt.store}); a memory store when absent and no swarm supplies one. */
        public Builder store(NodeStore value) {
            this.store = Objects.requireNonNull(value, "store");
            return this;
        }

        /** The retained versions per ref and the retained age ({@code agentspaces.pdt.retain}, PD-S-03). */
        public Builder retain(int versions, Duration age) {
            if (versions < 0) {
                throw new IllegalArgumentException("versions must not be negative");
            }
            this.retainVersions = versions;
            this.retainAge = Objects.requireNonNull(age, "age");
            return this;
        }

        /** How long a lazy load or a deref waits for a node or a delta ({@code agentspaces.pdt.load-timeout}, PD-V-06). */
        public Builder loadTimeout(Duration value) {
            this.loadTimeout = Objects.requireNonNull(value, "loadTimeout");
            return this;
        }

        /** The largest delta carried on a head, in bytes, at most {@link PRef#INLINE_DELTA_BYTES} ({@code agentspaces.pdt.inline-delta}). */
        public Builder inlineDelta(int bytes) {
            this.inlineDelta = bytes;
            return this;
        }

        /** {@link #FOLLOW_FULL} or {@link #FOLLOW_LAZY} ({@code agentspaces.pdt.follow}). */
        public Builder follow(String value) {
            if (!FOLLOW_FULL.equals(value) && !FOLLOW_LAZY.equals(value)) {
                throw new IllegalArgumentException("follow is full or lazy, got " + value);
            }
            this.follow = value;
            return this;
        }

        /**
         * The follow of one ref ({@code agentspaces.pdt.refs.<name>.follow}; PDT 2 phase 7, P2-X-08):
         * {@link #FOLLOW_FULL} or {@link #FOLLOW_LAZY} for that name, the facade's follow for the rest.
         */
        public Builder follow(String name, String value) {
            Objects.requireNonNull(name, "name");
            if (!FOLLOW_FULL.equals(value) && !FOLLOW_LAZY.equals(value)) {
                throw new IllegalArgumentException("the follow of ref " + name + " is full or lazy, got " + value);
            }
            follows.put(name, FOLLOW_FULL.equals(value));
            return this;
        }

        /**
         * The sealing every head names ({@code agentspaces.pdt.sealing}, §3.8):
         * {@code none}, {@code group-key}, or {@code sealed-nodes}. The last wraps the
         * store in a {@link SealedNodeStore} at build (PD-P-05).
         */
        public Builder sealing(String value) {
            Objects.requireNonNull(value, "sealing");
            if (!PRef.SEALING_NONE.equals(value) && !PRef.SEALING_GROUP_KEY.equals(value) && !PRef.SEALING_SEALED_NODES.equals(value)) {
                throw new IllegalArgumentException("sealing is none, group-key, or sealed-nodes, got " + value);
            }
            this.sealing = value;
            return this;
        }

        /**
         * The group's content key ring, which the sealed-nodes mode seals under and
         * opens with (SPEC §11a.3); absent, a sealed facade is keyless and only holds
         * and serves.
         */
        public Builder keyRing(GroupKeyRing value) {
            this.keyRing = Objects.requireNonNull(value, "keyRing");
            return this;
        }

        /** The clock of leases, retention, and grace periods. */
        public Builder clock(InstantSource value) {
            this.clock = Objects.requireNonNull(value, "clock");
            return this;
        }

        /** The write lease of a head, renewed at half-life (PD-R-08); ten minutes by default. */
        public Builder headLease(Duration value) {
            this.headLease = Objects.requireNonNull(value, "headLease");
            return this;
        }

        /**
         * The quota of the store behind the tree store this facade hands the swarm ({@code agentspaces.pdt.store.quota};
         * PDT 2 phase 7, P2-X-08): a received node that would carry the store past it is refused and ends its fetch.
         * A facade that adopts the tree store of a swarm built before it takes that tree store's quota.
         *
         * @param bytes the quota in bytes, or {@code QuotaNodeStore.UNBOUNDED}
         * @return this builder
         */
        public Builder quota(long bytes) {
            if (bytes <= 0) {
                throw new IllegalArgumentException("a quota is a positive number of bytes, got " + bytes);
            }
            this.quota = bytes;
            return this;
        }

        /** The mode of a ref ({@code agentspaces.pdt.refs.<name>.mode}): {@code multi} or {@code linear}. */
        public Builder mode(String name, String mode) {
            Objects.requireNonNull(name, "name");
            if (!PRef.MODE_MULTI.equals(mode) && !PRef.MODE_LINEAR.equals(mode)) {
                throw new IllegalArgumentException("mode is multi or linear, got " + mode);
            }
            modes.put(name, mode);
            return this;
        }

        /** The take lease, the wait for the head, and the retries of a linear swap (PD-R-04). */
        public Builder take(Duration lease, Duration timeout, int retries) {
            this.takeLease = Objects.requireNonNull(lease, "lease");
            this.takeTimeout = Objects.requireNonNull(timeout, "timeout");
            if (retries < 0) {
                throw new IllegalArgumentException("retries must not be negative");
            }
            this.takeRetries = retries;
            return this;
        }

        /** How long a writer keeps a superseded head before cancelling it: two gossip rounds, at least 2 s (PD-R-03). */
        public Builder cancelGrace(Duration value) {
            this.cancelGrace = Objects.requireNonNull(value, "cancelGrace");
            return this;
        }

        /** How often a collection cycle starts on the tick (PD-S-04); zero disables collection. */
        public Builder collectEvery(Duration value) {
            this.collectEvery = Objects.requireNonNull(value, "collectEvery");
            return this;
        }

        /** The collector's visit budget per tick. */
        public Builder collectorVisits(int value) {
            if (value < 1) {
                throw new IllegalArgumentException("collectorVisits must be at least 1");
            }
            this.collectorVisits = value;
            return this;
        }

        /** The group's discovery, where {@link Pdt#storeFor} finds the content stores' cards (PDT 2 phase 3, P2-S-11). */
        public Builder discovery(ai.badmonkey.agentspaces.discovery.DiscoveryService value) {
            this.discovery = Objects.requireNonNull(value, "discovery");
            return this;
        }

        /**
         * This peer's identity, which signs the pins {@link Pdt#storeFor} writes, so a store charges this peer only
         * for what it signed (PDT 2 follow-ups, 2026-11-09, F1).
         */
        public Builder identity(ai.badmonkey.agentspaces.identity.PeerIdentity value) {
            this.identity = Objects.requireNonNull(value, "identity");
            return this;
        }

        /** How long a challenged store has to answer ({@code agentspaces.store.challenge-deadline}, P2-S-07); 2 s by default. */
        public Builder challengeDeadline(Duration value) {
            this.challengeDeadline = Objects.requireNonNull(value, "challengeDeadline");
            return this;
        }

        /**
         * Eager push ({@code agentspaces.pdt.push.*}, PDT 2 phase 5, P2-P-01, P2-Q-MNT-01): off by default. With it on,
         * this peer pushes each delta above the inline bound down its ref's stream, within the budget, and follows the
         * streams of the refs it follows fully.
         */
        public Builder push(ai.badmonkey.agentspaces.pdt.refs.PushPublisher.Config value) {
            this.push = Objects.requireNonNull(value, "push");
            return this;
        }

        /**
         * Whether blob edits keep intra-chunk delta frames ({@code agentspaces.pdt.blobs.deltas}, PDT 2 phase 4,
         * P2-Z-02, P2-Q-MNT-01): on by default, except over a sealed store, which keeps none. With it on, an edit
         * keeps frames only while this peer and another member advertise level 2 (PDT 2 phase 4 measures, §10.14).
         */
        public Builder deltas(boolean value) {
            this.deltas = value;
            return this;
        }

        /** How long a kept frame stays ({@code agentspaces.pdt.blobs.delta-retention}, P2-Z-02); one hour by default. */
        public Builder deltaRetention(Duration value) {
            this.deltaRetention = Objects.requireNonNull(value, "deltaRetention");
            return this;
        }

        public Pdt build() {
            return new Pdt(this);
        }
    }

    /** The tree store to give {@code Content.Builder.treeStore} when the swarm is built after this facade. */
    public TreeStore treeStore() {
        return treeStore;
    }

    /**
     * Joins the content swarm built over this facade's tree store.
     *
     * @param content the swarm
     * @return this
     * @throws IllegalStateException when the swarm's tree store is not over this facade's node store, or a swarm is attached already
     */
    public Pdt attach(Content content) {
        Objects.requireNonNull(content, "content");
        if (swarm != null) {
            throw new IllegalStateException("a content swarm is attached already");
        }
        PdtSwarm attached = PdtSwarm.attach(content, store, loadTimeout);
        Refs engine = new Refs(refSpace, attached, retention, clock, refsConfig, push);
        this.collector = new NodeCollector(store, engine::liveRoots, retention, collectorVisits);
        this.refs = engine;
        this.swarm = attached;
        this.stores = new ai.badmonkey.agentspaces.pdt.stores.Stores(content, store, discovery, identity, clock, challengeDeadline,
                new java.security.SecureRandom());
        return this;
    }

    /** Whether {@link #attach} ran. */
    public boolean attached() {
        return swarm != null;
    }

    /** Starts the facade; the swarm must be attached. */
    public void start() {
        if (swarm == null) {
            throw new IllegalStateException("attach the content swarm before starting");
        }
        started = true;
        lastCollectMillis = clock.millis();
    }

    /** The periodic work: the ref engine's tick and, every {@code collectEvery}, a collection tick (PD-S-04). */
    public void tick() {
        if (closed || !started) {
            return;
        }
        refs.tick();
        flushMeta();
        if (!collectEvery.isZero()) {
            long now = clock.millis();
            NodeCollector c = collector;
            NodeCollector.Tick result = null;
            if (c.phase() != NodeCollector.Phase.IDLE) {
                result = c.tick();
            } else if (now - lastCollectMillis >= collectEvery.toMillis()) {
                lastCollectMillis = now;
                result = c.tick();
            }
            if (result != null && result.cycleComplete() && result.removed() > 0) {
                refs.forgetHeld();
            }
        }
    }

    @Override
    public void close() {
        closed = true;
        Refs engine = refs;
        if (engine != null) {
            engine.close();
        }
        flushMeta();
    }

    /** Writes the pins and the retained versions that changed to the store's meta records (P2-V-03). */
    private void flushMeta() {
        store.flushMeta();
        retention.flush();
    }

    // ------------------------------------------------------------ refs

    /**
     * Shares a ref's current version into another group's facade (PDT 2 phase 1, P2-X-02): the
     * other facade writes a head of the same name with the same root, and its swarm holds the
     * root there; {@link Refs#share} gives the rules.
     *
     * @param ref   a handle of this facade
     * @param group the facade of the second group
     * @param <T>   the value type
     * @return the handle of the same ref in the second group
     */
    public <T> Ref<T> share(Ref<T> ref, Pdt group) {
        Objects.requireNonNull(group, "group");
        return engine().share(ref, group.engine());
    }

    /**
     * A ref by name and kind with untyped CBOR codecs: {@code PMap.class},
     * {@code PSet.class}, {@code PVector.class}, or {@code PBlob.class}.
     *
     * @param name the ref name
     * @param kind the value class
     * @param <T>  the value type
     * @return the handle
     */
    @SuppressWarnings("unchecked")
    public <T> Ref<T> ref(String name, Class<? super T> kind) {
        Objects.requireNonNull(kind, "kind");
        Refs engine = engine();
        if (kind == PMap.class) {
            return (Ref<T>) engine.mapRef(name);
        }
        if (kind == PSet.class) {
            return (Ref<T>) engine.setRef(name, Codec.of(Object.class));
        }
        if (kind == PVector.class) {
            return (Ref<T>) engine.vectorRef(name, Codec.of(Object.class));
        }
        if (kind == PBlob.class) {
            return (Ref<T>) engine.blobRef(name);
        }
        throw new IllegalArgumentException("a ref holds a PMap, PSet, PVector, or PBlob, not " + kind.getName());
    }

    /**
     * The handle a {@code @PdtRef} site of a declared type receives (PDT 2 phase 7, P2-X-03, PD-R-11); a host
     * registers {@code pdt::refFor} as each group's {@code RefResolver}. The value type picks the kind:
     * {@code Ref<PMap<K, V>>} a map ref with the canonical codecs of {@code K} and {@code V}
     * ({@link Codec#of}), except {@code Ref<PMap<String, FileEntry>>}, the tree ref of PD-B-05;
     * {@code Ref<PSet<E>>} and {@code Ref<PVector<E>>} with the codec of {@code E}; {@code Ref<PBlob>} a
     * blob ref; and {@code Ref<PDb>} a database ref whose meeting heads keep the later one whole, as
     * {@code dbRef(name, null)}. A raw value class, a wildcard, or a type variable reads untyped CBOR, as
     * {@link #ref(String, Class)} does.
     *
     * @param name the ref name
     * @param type the site's generic type
     * @return the handle
     * @throws IllegalArgumentException when the type is no {@code Ref} of one of those values
     */
    public Ref<?> refFor(String name, Type type) {
        Objects.requireNonNull(type, "type");
        if (!(type instanceof ParameterizedType site) || site.getRawType() != Ref.class) {
            if (type == Ref.class) {
                throw new IllegalArgumentException("declare the value type, for example Ref<PMap<String, Fact>>, not a raw Ref");
            }
            throw new IllegalArgumentException(type.getTypeName() + " is not a Ref; declare Ref<PMap<String, Fact>> or another ref");
        }
        Type value = site.getActualTypeArguments()[0];
        Class<?> kind = value instanceof Class<?> raw ? raw
                : value instanceof ParameterizedType parameterized ? (Class<?>) parameterized.getRawType() : null;
        if (kind == null) {
            throw new IllegalArgumentException(type.getTypeName() + " names no value; declare the value type, for example"
                    + " Ref<PMap<String, Fact>>");
        }
        Type[] args = value instanceof ParameterizedType parameterized ? parameterized.getActualTypeArguments() : null;
        if (kind == PMap.class) {
            if (args == null) {
                return ref(name, PMap.class);
            }
            if (args[0] == String.class && args[1] == FileEntry.class) {
                return treeRef(name);
            }
            return mapRef(name, codecOf(args[0]), codecOf(args[1]));
        }
        if (kind == PSet.class) {
            return setRef(name, args == null ? Codec.of(Object.class) : codecOf(args[0]));
        }
        if (kind == PVector.class) {
            return vectorRef(name, args == null ? Codec.of(Object.class) : codecOf(args[0]));
        }
        if (kind == PBlob.class) {
            return blobRef(name);
        }
        if (kind == PDb.class) {
            return dbRef(name, null);
        }
        throw new IllegalArgumentException("a ref holds a PMap, PSet, PVector, PBlob, or PDb, not " + value.getTypeName());
    }

    /** The canonical codec of a declared key or element type: its raw class, or untyped CBOR for a wildcard or variable. */
    private static Codec<Object> codecOf(Type type) {
        Class<?> raw = type instanceof Class<?> c ? c
                : type instanceof ParameterizedType parameterized ? (Class<?>) parameterized.getRawType() : Object.class;
        @SuppressWarnings("unchecked")
        Codec<Object> codec = (Codec<Object>) Codec.of(raw);
        return codec;
    }

    /** A map ref with typed codecs; {@code kind} must be {@code PMap.class}. */
    public <K, V> Ref<PMap<K, V>> ref(String name, Class<? super PMap<K, V>> kind, Codec<K> keys, Codec<V> values) {
        if (kind != PMap.class) {
            throw new IllegalArgumentException("two codecs name a PMap ref, not " + kind.getName());
        }
        return engine().mapRef(name, keys, values);
    }

    /**
     * A set or vector ref with a typed codec; {@code kind} is {@code PSet.class} or
     * {@code PVector.class}.
     */
    @SuppressWarnings("unchecked")
    public <T, E> Ref<T> ref(String name, Class<? super T> kind, Codec<E> codec) {
        if (kind == PSet.class) {
            return (Ref<T>) engine().setRef(name, codec);
        }
        if (kind == PVector.class) {
            return (Ref<T>) engine().vectorRef(name, codec);
        }
        throw new IllegalArgumentException("one codec names a PSet or PVector ref, not " + kind.getName());
    }

    /** A map ref with typed codecs. */
    public <K, V> Ref<PMap<K, V>> mapRef(String name, Codec<K> keys, Codec<V> values) {
        return engine().mapRef(name, keys, values);
    }

    /** A set ref. */
    public <E> Ref<PSet<E>> setRef(String name, Codec<E> codec) {
        return engine().setRef(name, codec);
    }

    /** A vector ref. */
    public <E> Ref<PVector<E>> vectorRef(String name, Codec<E> codec) {
        return engine().vectorRef(name, codec);
    }

    /** A blob ref. */
    public Ref<PBlob> blobRef(String name) {
        return engine().blobRef(name);
    }

    /** A tree ref: the map from relative path to {@link FileEntry} that {@code TreePublisher} writes (PD-B-05). */
    public Ref<PMap<String, FileEntry>> treeRef(String name) {
        return engine().treeRef(name);
    }

    /**
     * A DataScript database ref (PDT 2 phase 2, P2-D-03, P2-D-06): version 2 heads of kind
     * {@code db}, linear by default, merged by datoms through {@code merge} when heads meet.
     * {@code agentspaces-datascript} builds its conn on this.
     *
     * @param name  the ref name
     * @param merge the datom merge, or null for the later head whole
     * @return the handle
     */
    public Ref<PDb> dbRef(String name, DbMerge merge) {
        return engine().dbRef(name, merge);
    }

    /**
     * Registers the merge function every reader of a name applies when heads
     * meet (PD-R-05).
     *
     * @param name the ref name
     * @param fn   the function
     * @param <T>  the value type
     * @return this
     */
    public <T> Pdt mergeFor(String name, MergeFunction<T> fn) {
        engine().mergeFor(name, fn);
        return this;
    }

    /**
     * As {@link #mergeFor(String, MergeFunction)} with the kind the function
     * expects ({@code PRef.KIND_MAP}, {@code KIND_SET}, {@code KIND_VECTOR},
     * {@code KIND_BLOB}, or {@code KIND_TREE}): a ref of the name with another
     * kind is refused, so the function never runs over a value it was not
     * written for.
     *
     * @param name the ref name
     * @param kind the kind name
     * @param fn   the function
     * @param <T>  the value type
     * @return this
     */
    public <T> Pdt mergeFor(String name, String kind, MergeFunction<T> fn) {
        engine().mergeFor(name, kind, fn);
        return this;
    }

    private Refs engine() {
        Refs engine = refs;
        if (engine == null) {
            throw new IllegalStateException("attach the content swarm before creating refs");
        }
        return engine;
    }

    // ------------------------------------------------------------ content stores (PDT 2 phase 3)

    /**
     * Asks the group's content stores to hold a root (P2-S-11, P2-S-03): the planner's {@code StoreChooser}
     * picks stores from their cards, and the facade writes one {@code SignedPin} naming them, signed by the
     * identity of {@link Builder#identity}; writing again
     * renews the lease (P2-S-04). {@link ai.badmonkey.agentspaces.pdt.stores.Stores#storeFor} gives the rules.
     *
     * @param root  a tree root this peer holds complete, or an artifact root
     * @param need  the copies, tier, price, and stores to avoid
     * @param lease how long the stores should hold it
     * @return the pin and the chosen cards
     */
    public ai.badmonkey.agentspaces.pdt.stores.Stores.Pinning storeFor(String root,
                                                                        ai.badmonkey.agentspaces.planner.StoreChooser.Need need,
                                                                        Duration lease) {
        return stores().storeFor(root, need, lease);
    }

    /**
     * Challenges a store's possession of a root (P2-S-07); two failures in a row strike the store and stop
     * its receipts counting.
     *
     * @param root  the root
     * @param store the store's peer
     * @return the outcome
     */
    public java.util.concurrent.CompletableFuture<Content.ChallengeResult> challenge(String root,
                                                                                     ai.badmonkey.agentspaces.common.id.PeerId store) {
        return stores().challenge(root, store);
    }

    /** The requester's view of the content stores, or a failure before {@link #attach}. */
    public ai.badmonkey.agentspaces.pdt.stores.Stores stores() {
        ai.badmonkey.agentspaces.pdt.stores.Stores view = stores;
        if (view == null) {
            throw new IllegalStateException("attach the content swarm before asking content stores");
        }
        return view;
    }

    // ------------------------------------------------------------ parts

    /** The node store; a {@link SealedNodeStore} in the sealed-nodes mode. */
    public NodeStore store() {
        return store;
    }

    /** The sealing every head names: {@code none}, {@code group-key}, or {@code sealed-nodes}. */
    public String sealing() {
        return refsConfig.sealing();
    }

    /** The sealer of the sealed-nodes mode, when the store seals. */
    public java.util.Optional<NodeSealer> sealer() {
        return store instanceof SealedNodeStore sealed ? java.util.Optional.of(sealed.sealer()) : java.util.Optional.empty();
    }

    /** The retained versions. */
    public Retention retention() {
        return retention;
    }

    /** The attached swarm, or null before {@link #attach}. */
    public PdtSwarm swarm() {
        return swarm;
    }

    /**
     * The bulk-payload seam of §3.7 (PD-E-01): what a space opts into through
     * {@code ReplicatedSpace.Builder.bulk(pdt.payloads(), escalateAbove)} with
     * {@code sealBeforeEscalate(false)}. One seam per facade, over its store,
     * swarm, and retention; built on first use.
     *
     * @return the seam
     * @throws IllegalStateException before {@link #attach}
     */
    public PdtPayloads payloads() {
        PdtPayloads seam = payloads;
        if (seam == null) {
            synchronized (this) {
                seam = payloads;
                if (seam == null) {
                    PdtSwarm attached = swarm;
                    if (attached == null) {
                        throw new IllegalStateException("attach the content swarm before escalating payloads");
                    }
                    seam = new PdtPayloads(store, attached, retention, clock, loadTimeout);
                    payloads = seam;
                }
            }
        }
        return seam;
    }

    /** The ref engine, or null before {@link #attach}. */
    public Refs refs() {
        return refs;
    }

    /** The collector, or null before {@link #attach}. */
    public NodeCollector collector() {
        return collector;
    }

    /** The ref space. */
    public Space refSpace() {
        return refSpace;
    }

    /** The counters. */
    public Stats stats() {
        Refs engine = refs;
        if (engine == null) {
            return new Stats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, store.size(), store.bytesUsed());
        }
        return new Stats(engine.refCount(), engine.headCount(), engine.deltasInline(), engine.deltasFetched(),
                engine.conflicts(), engine.bytesReceived(), engine.headsIgnored(), engine.nodesDropped(),
                engine.headsRewritten(), engine.basesFetched(), store.size(), store.bytesUsed(),
                engine.entriesTruncated(), engine.mergeFunctionFailures(), engine.headsLate().total());
    }

    @Override
    public String toString() {
        return "Pdt(" + stats() + ")";
    }
}
