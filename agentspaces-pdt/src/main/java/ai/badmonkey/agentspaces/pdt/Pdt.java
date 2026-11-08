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
import ai.badmonkey.agentspaces.pdt.refs.MergeFunction;
import ai.badmonkey.agentspaces.pdt.refs.Ref;
import ai.badmonkey.agentspaces.pdt.refs.Refs;
import ai.badmonkey.agentspaces.pdt.seal.NodeSealer;
import ai.badmonkey.agentspaces.pdt.seal.SealedNodeStore;
import ai.badmonkey.agentspaces.pdt.store.NodeCollector;
import ai.badmonkey.agentspaces.pdt.store.Retention;
import ai.badmonkey.agentspaces.pdt.store.StoreThenFetchLoader;
import ai.badmonkey.agentspaces.pdt.swarm.NodeStoreTreeStore;
import ai.badmonkey.agentspaces.pdt.swarm.PdtSwarm;
import ai.badmonkey.agentspaces.pdt.values.Codec;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.pdt.values.PSet;
import ai.badmonkey.agentspaces.pdt.values.PVector;

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
     * @param headsIgnored  heads ignored: malformed, another kind, or a root that failed to load (S7, PD-P-04)
     * @param nodesDropped  inline nodes dropped for failing verification or reachability (PD-X-07)
     * @param headsRewritten lapsed heads this peer rewrote (PD-R-08)
     * @param basesFetched  bases fetched to keep the version graph held for the common-base walk (PD-R-06)
     * @param nodesHeld     nodes in the store
     * @param bytesHeld     bytes in the store
     * @param entriesTruncated reads of a name that filled {@code Refs.MAX_ENTRIES}, so the heads read then are untrustworthy
     * @param mergeFunctionFailures merge function calls that threw, after which the rule of §3.6.1 decided (PD-R-05)
     */
    public record Stats(int refs, int heads, long deltasInline, long deltasFetched, long conflicts, long bytesReceived,
                        long headsIgnored, long nodesDropped, long headsRewritten, long basesFetched, int nodesHeld, long bytesHeld,
                        long entriesTruncated, long mergeFunctionFailures) {

        /** The twelve-figure form, kept for callers that predate the truncation and merge-failure counters. */
        public Stats(int refs, int heads, long deltasInline, long deltasFetched, long conflicts, long bytesReceived,
                     long headsIgnored, long nodesDropped, long headsRewritten, long basesFetched, int nodesHeld, long bytesHeld) {
            this(refs, heads, deltasInline, deltasFetched, conflicts, bytesReceived, headsIgnored, nodesDropped, headsRewritten,
                    basesFetched, nodesHeld, bytesHeld, 0, 0);
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
        this.treeStore = b.content != null && b.content.treeStore().orElse(null) instanceof NodeStoreTreeStore adapter
                && adapter.store() == store ? adapter : PdtSwarm.treeStore(store);
        this.retention = new Retention(b.retainVersions, b.retainAge, clock);
        this.loadTimeout = b.loadTimeout;
        this.refsConfig = new Refs.Config(b.loadTimeout, b.inlineDelta, FOLLOW_FULL.equals(b.follow), b.sealing,
                b.headLease, b.takeLease, b.takeTimeout, b.takeRetries, b.modes, b.cancelGrace);
        this.collectEvery = b.collectEvery;
        this.collectorVisits = b.collectorVisits;
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
        Refs engine = new Refs(refSpace, attached, retention, clock, refsConfig);
        this.collector = new NodeCollector(store, engine::liveRoots, retention, collectorVisits);
        this.refs = engine;
        this.swarm = attached;
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
    }

    // ------------------------------------------------------------ refs

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
                engine.entriesTruncated(), engine.mergeFunctionFailures());
    }

    @Override
    public String toString() {
        return "Pdt(" + stats() + ")";
    }
}
