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
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.error.LeaseExpiredException;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Matchers;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.Subscription;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.hlc.HybridLogicalClock;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.FetchOptions;
import ai.badmonkey.agentspaces.content.tree.TreeFetch;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.NodeUnavailableException;
import ai.badmonkey.agentspaces.pdt.blob.FileEntry;
import ai.badmonkey.agentspaces.pdt.blob.PBlob;
import ai.badmonkey.agentspaces.pdt.store.Retention;
import ai.badmonkey.agentspaces.pdt.swarm.PdtSwarm;
import ai.badmonkey.agentspaces.pdt.values.Codec;
import ai.badmonkey.agentspaces.pdt.values.Merge;
import ai.badmonkey.agentspaces.pdt.values.NodeLoader;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.pdt.values.PSet;
import ai.badmonkey.agentspaces.pdt.values.PVector;

import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * The ref engine (srad/agentspaces-pdt.md §3.6, §6.4 S1, S4, S5; group R; PD-P-04;
 * PD-X-07): heads, deref, swap in both modes, the inline delta at both ends, the
 * merge of §3.6.1, head leases, and the retained history, over one ref space and
 * one {@link PdtSwarm}.
 *
 * <p>A ref is the set of live {@code PRef} entries with one name in the space;
 * its heads are the entries no other live entry with a larger seq lists in its
 * base, with equal roots folded into one (PD-R-08); a swap that recreates an
 * ancestor's content gets the ancestor's root and stays a head by its seq,
 * because content addressing makes the root the version's name and only the
 * seq tells a revert from the version it reverted to. The engine reads entries through
 * {@code Space.readAllEntries} and hears new ones through {@code Space.notify}
 * on {@code Template.of(PRef.class).whereTag("name", eq(name))} (PD-R-07). The
 * space's own admission decides who may write a head: a refused record never
 * enters the replica, and the engine adds one check of its own, ignoring a head
 * whose root fails to load and verify within the load timeout (PD-P-04, S7). A
 * head whose root or base is not a CID, whose seq is above {@code PRef.MAX_SEQ},
 * or whose record fails any other bound of {@code PRef.violation()} is ignored
 * and counted before anything parses it.
 *
 * <p>Reading. {@code deref} loads the single head, or merges several: maps,
 * sets, and trees key-wise and three-way against the lowest common base found by
 * walking base links over held versions for at most {@value #MAX_BASE_STEPS}
 * steps, both-side conflicts to the head with the later issued HLC (ties by the
 * issuer's PeerId bytes); vectors and blobs to the later head whole, unless
 * {@link #mergeFor} registered a function for the name (PD-R-05, PD-R-06). A
 * reader merges on read and never writes; every member computes the same result
 * from the same heads. A deref with several heads to fetch starts every fetch
 * first and waits for them together against one load timeout, so N unloadable
 * heads cost one timeout, and a merge function that throws or does not fit the
 * ref's kind falls back to the rule with reason {@code MERGE_FUNCTION_FAILED}.
 *
 * <p>Writing. {@code swap} builds the value over a recording store so every node
 * the function creates is captured, flushes the new root, and writes a head whose
 * base lists the heads it read and whose seq is one more than their largest
 * (PD-R-03); when the captured nodes reachable from the new root total at most
 * the inline bound they ride on the head (PD-X-07, S1), else readers fetch the
 * delta with their retained roots as haves (S2). The writer pins and holds the
 * new root, releases the pin of the root it superseded, and cancels its own
 * superseded heads {@value #CANCEL_GRACE_MILLIS} ms later on the tick, two
 * gossip rounds, so a lagging reader still finds a base. An own head counts as
 * superseded only while it lies below the lowest common base of the held heads
 * (the base the merge picks): a head that only an unloadable successor lists
 * stays live, and every version on a path from a head to the common base stays
 * live while the fork lasts, so a reader that joins later walks the same graph
 * to the same base (PD-R-06); a live head this writer has not loaded yet but
 * still expects to bounds that base from below, because the base the fleet
 * picks once the head lands lies no higher than the one it shares with the
 * held heads; a base cancelled before a lagging writer forked off it is
 * fetched by root from the heads' issuers. In linear mode the head is taken under the space's
 * conflict strategy and the successor completes the take (PD-R-04, S5), the
 * {@code @SpaceReduce} pattern; a first head is written only when this engine
 * never saw a version, a take, or a live entry of the name.
 *
 * <p>Leases. A head lease defaults to ten minutes and the writer renews it at
 * half-life on the tick; when a head lapses, a member that holds its root
 * complete rewrites it with the same root, base, and seq under its own signature
 * (PD-R-08), carrying the original issue stamp and issuer in the reserved attrs
 * {@code pdt.issued} and {@code pdt.issuer}, so the merge order of the version
 * does not move with the rewrite.
 *
 * <p>Threading. Space events arrive on the space's delivery thread; deref and
 * swap run on the caller's thread; the tick runs on the host's scheduler. State
 * is guarded by short critical sections and no lock is held across a space call,
 * a fetch wait, or a listener.
 */
public final class Refs implements AutoCloseable {

    /** The default head lease (PD-R-08). */
    public static final Duration DEFAULT_HEAD_LEASE = Duration.ofMinutes(10);

    /** The grace before a superseded own head is cancelled: two gossip rounds (PD-R-03). */
    public static final long CANCEL_GRACE_MILLIS = 2_000;

    /** The most base links a common-base walk follows (PD-R-06). */
    public static final int MAX_BASE_STEPS = 64;

    /** The most entries of one name read at once; a read that fills it counts in {@link #entriesTruncated}. */
    public static final int MAX_ENTRIES = 4096;

    /** The most roots whose completeness is remembered (§6.7 row 4). */
    static final int COMPLETE_CAPACITY = 4096;

    /** How long a root whose fetch failed is left alone before a deref starts its fetch again, as a multiple of the load timeout. */
    static final int UNLOADABLE_BACKOFF = 4;

    /** The common base of heads that are all first versions: the empty value. */
    private static final String EMPTY_BASE = "";

    /** The most haves a delta fetch names: the wire bound of PD-X-02. */
    public static final int MAX_HAVES = 64;

    /** The most versions remembered per ref for base walks. */
    static final int KNOWN_CAPACITY = 4096;

    /** The lease of the engine's notify subscriptions. */
    private static final Duration SUBSCRIPTION_LEASE = Duration.ofHours(24);

    /**
     * The engine's settings.
     *
     * @param loadTimeout how long a deref waits for a head's fetch (PD-V-06)
     * @param inlineDelta the largest delta carried on a head, in bytes (PD-X-07)
     * @param followFull  whether a reader fetches each new head's tree at once; lazy readers load nodes on get
     * @param sealing     the sealing every head names
     * @param headLease   the write lease of a head (PD-R-08)
     * @param takeLease   the take lease of a linear swap
     * @param takeTimeout how long a linear swap waits for the head
     * @param takeRetries how many takes a linear swap attempts after a timeout
     * @param modes       the configured mode per ref name; absent names follow their first head, else multi
     * @param cancelGrace how long a writer keeps a superseded own head before cancelling it: two gossip
     *                    rounds, at least {@value #CANCEL_GRACE_MILLIS} ms (PD-R-03)
     */
    public record Config(Duration loadTimeout, int inlineDelta, boolean followFull, String sealing, Duration headLease,
                         Duration takeLease, Duration takeTimeout, int takeRetries, Map<String, String> modes,
                         Duration cancelGrace) {

        public Config {
            Objects.requireNonNull(loadTimeout, "loadTimeout");
            Objects.requireNonNull(sealing, "sealing");
            Objects.requireNonNull(headLease, "headLease");
            Objects.requireNonNull(takeLease, "takeLease");
            Objects.requireNonNull(takeTimeout, "takeTimeout");
            Objects.requireNonNull(cancelGrace, "cancelGrace");
            if (cancelGrace.toMillis() < CANCEL_GRACE_MILLIS) {
                throw new IllegalArgumentException("cancelGrace is at least " + CANCEL_GRACE_MILLIS + " ms");
            }
            if (inlineDelta < 0 || inlineDelta > PRef.INLINE_DELTA_BYTES) {
                throw new IllegalArgumentException("inlineDelta must be in [0, " + PRef.INLINE_DELTA_BYTES + "], got " + inlineDelta);
            }
            if (takeRetries < 0) {
                throw new IllegalArgumentException("takeRetries must not be negative");
            }
            modes = modes == null ? Map.of() : Map.copyOf(modes);
        }

        /** The defaults of §7.2. */
        public static Config defaults() {
            return new Config(Duration.ofSeconds(10), PRef.INLINE_DELTA_BYTES, true, PRef.SEALING_NONE,
                    DEFAULT_HEAD_LEASE, Duration.ofSeconds(30), Duration.ofSeconds(5), 3, Map.of(),
                    Duration.ofMillis(CANCEL_GRACE_MILLIS));
        }
    }

    /** What a deref computed: the value and the heads it came from. */
    public record Result<T>(T value, Heads heads) {
    }

    /** One head entry this peer wrote, for renewal and cancellation; several may share a root after a revert. */
    private static final class Own {
        final EntryHandle handle;
        final String root;
        long lastRenewMillis;
        Long supersededAtMillis;
        /** The bases the entry names, so a displaced entry goes once the versions it supersedes are gone. */
        final List<String> base;
        /** A later own entry for the same root displaced this one: it goes, after the grace, once none of its bases is live. */
        boolean displaced;

        Own(EntryHandle handle, String root, long nowMillis, List<String> base) {
            this.handle = handle;
            this.root = root;
            this.lastRenewMillis = nowMillis;
            this.base = List.copyOf(base);
        }
    }

    /** A delta fetch started for a head. */
    private static final class Pending {
        final TreeFetch fetch;
        final Heads.Head head;
        final long startedMillis;
        /** Set once a deref waited the load timeout in vain; later derefs skip the head until the tick settles it. */
        volatile boolean timedOut;
        /** A base of a head fetched so the version graph stays held (PD-R-06); not a head, so no watcher hears it. */
        final boolean base;

        Pending(TreeFetch fetch, Heads.Head head, long startedMillis, boolean base) {
            this.fetch = fetch;
            this.head = head;
            this.startedMillis = startedMillis;
            this.base = base;
        }
    }

    /** The engine's state for one ref name. */
    static final class State<T> {
        /** Entries a TAKEN event named and no COMPLETED, EXPIRED, or REAPPEARED event released: a take is under way. */
        final Set<EntryId> takenEntries = ConcurrentHashMap.newKeySet();
        /** The ids of every entry of the name read last, well formed or not, so ignored entries are pruned against them. */
        volatile Set<EntryId> liveIds = Set.of();
        /** Roots whose fetch failed, with the clock time, so a deref does not start and wait for the same fetch again at once. */
        final Map<String, Long> unloadable = new LinkedHashMap<>(16, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
                return size() > MAX_HAVES;
            }
        };
        final String name;
        final RefKind<T> kind;
        final Template<PRef> template;
        final String configuredMode;
        volatile String observedMode;
        final Map<String, Heads.Head> known = new LinkedHashMap<>(64, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Heads.Head> eldest) {
                return size() > KNOWN_CAPACITY;
            }
        };
        /** The head entries this peer wrote, by entry id; a revert within the grace gives one root two entries. */
        final Map<EntryId, Own> own = new LinkedHashMap<>();
        final Set<EntryId> ownEntries = ConcurrentHashMap.newKeySet();
        /** Roots this peer wrote heads for, recorded before the write so the inline event of an own head is known as own. */
        final Set<String> ownRoots = ConcurrentHashMap.newKeySet();
        /** The roots live at the previous tick, so a head that lapsed since is a rewrite candidate. */
        volatile Set<String> previousLive = Set.of();
        final Set<EntryId> ignoredEntries = ConcurrentHashMap.newKeySet();
        final Map<String, Pending> pending = new ConcurrentHashMap<>();
        /**
         * Heads whose root was not complete when their event fired: a gossip batch may deliver a head before its
         * base, so the next tick checks again before it starts a fetch, and a deref meanwhile starts one at once.
         */
        final Map<String, Heads.Head> recheck = new ConcurrentHashMap<>();
        /** The progress of the last settled fetches by root, for the panel and the tests (PD-C-08). */
        final Map<String, TreeFetch.Progress> fetched = new LinkedHashMap<>(16, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, TreeFetch.Progress> eldest) {
                return size() > MAX_HAVES;
            }
        };
        final Set<String> pinned = ConcurrentHashMap.newKeySet();
        /** Pinned roots this peer also advertises a holding for: versions whose delta travels by request. */
        final Set<String> heldRoots = ConcurrentHashMap.newKeySet();
        /** Roots a fetch completed here, which the swarm holds until they stop being live (PD-S-06). */
        final Set<String> fetchedRoots = ConcurrentHashMap.newKeySet();
        final List<Consumer<T>> watchers = new CopyOnWriteArrayList<>();
        final List<Consumer<PConflict>> conflictListeners = new CopyOnWriteArrayList<>();
        volatile Heads lastHeads;
        volatile Set<String> liveRoots = Set.of();
        volatile Subscription subscription;
        volatile long subscriptionRenewedMillis;

        State(String name, RefKind<T> kind, String configuredMode) {
            this.name = name;
            this.kind = kind;
            this.configuredMode = configuredMode;
            this.template = Template.of(PRef.class).whereTag(PRef.NAME_TAG, Matchers.eq(name));
        }

        String mode() {
            if (configuredMode != null) {
                return configuredMode;
            }
            String observed = observedMode;
            return observed == null ? PRef.MODE_MULTI : observed;
        }

        boolean linear() {
            return PRef.MODE_LINEAR.equals(mode());
        }

        Subscription addWatcher(Consumer<T> listener) {
            watchers.add(listener);
            return new Removal(() -> watchers.remove(listener));
        }

        Subscription addConflictListener(Consumer<PConflict> listener) {
            conflictListeners.add(listener);
            return new Removal(() -> conflictListeners.remove(listener));
        }
    }

    /** A subscription that only removes a listener. */
    private static final class Removal implements Subscription {
        private final Runnable remove;

        Removal(Runnable remove) {
            this.remove = remove;
        }

        @Override
        public void renew(Duration extension) {
        }

        @Override
        public void close() {
            remove.run();
        }
    }

    private final Space space;
    private final PdtSwarm swarm;
    private final NodeStore store;
    private final NodeLoader loader;
    private final Retention retention;
    private final InstantSource clock;
    private final Config config;
    private final Map<String, State<?>> states = new ConcurrentHashMap<>();
    private final Map<String, MergeFunction<?>> mergeFunctions = new ConcurrentHashMap<>();
    private final AtomicLong deltasInline = new AtomicLong();
    private final AtomicLong deltasFetched = new AtomicLong();
    private final AtomicLong conflicts = new AtomicLong();
    private final AtomicLong bytesReceived = new AtomicLong();
    private final AtomicLong headsIgnored = new AtomicLong();
    private final AtomicLong nodesDropped = new AtomicLong();
    private final AtomicLong headsRewritten = new AtomicLong();
    private final AtomicLong basesFetched = new AtomicLong();
    private final AtomicLong entriesTruncated = new AtomicLong();
    private final AtomicLong mergeFunctionFailures = new AtomicLong();
    /** The kind a registered merge function was declared for, by name; absent when registered without one. */
    private final Map<String, String> mergeKinds = new ConcurrentHashMap<>();
    /**
     * Roots the store held complete when last asked, the oldest forgotten past
     * {@value #COMPLETE_CAPACITY}; a completeness walk costs the whole tree, so the answer is kept.
     */
    private final Map<String, Boolean> completeRoots = new LinkedHashMap<>(64, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return size() > COMPLETE_CAPACITY;
        }
    };
    private volatile boolean closed;

    /**
     * An engine.
     *
     * @param space     the ref space
     * @param swarm     the attached node store and swarm
     * @param retention the retained versions, fed on every version seen
     * @param clock     the clock of leases and grace periods
     * @param config    the settings
     */
    public Refs(Space space, PdtSwarm swarm, Retention retention, InstantSource clock, Config config) {
        this.space = Objects.requireNonNull(space, "space");
        this.swarm = Objects.requireNonNull(swarm, "swarm");
        this.store = swarm.store();
        this.loader = swarm.loader();
        this.retention = Objects.requireNonNull(retention, "retention");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.config = Objects.requireNonNull(config, "config");
    }

    /**
     * Whether the store holds a version complete, remembered once true until
     * {@link #forgetHeld}; a root that is not a CID is never held, and the
     * question never throws (§6.7 row 6).
     */
    private boolean held(String root) {
        synchronized (completeRoots) {
            if (completeRoots.containsKey(root)) {
                return true;
            }
        }
        boolean complete;
        try {
            complete = store.complete(root);
        } catch (IllegalArgumentException malformed) {
            return false;
        }
        if (complete) {
            synchronized (completeRoots) {
                completeRoots.put(root, Boolean.TRUE);
            }
            return true;
        }
        return false;
    }

    /** Drops the remembered completeness, after a collection cycle removed nodes (PD-S-04). */
    public void forgetHeld() {
        synchronized (completeRoots) {
            completeRoots.clear();
        }
    }

    /** The settings. */
    public Config config() {
        return config;
    }

    /** The ref space. */
    public Space space() {
        return space;
    }

    // ------------------------------------------------------------ refs

    /** A map ref over untyped CBOR keys and values. */
    public Ref<PMap<Object, Object>> mapRef(String name) {
        return ref(name, RefKind.map());
    }

    /** A map ref with typed codecs. */
    public <K, V> Ref<PMap<K, V>> mapRef(String name, Codec<K> keys, Codec<V> values) {
        return ref(name, RefKind.map(PRef.KIND_MAP, keys, values));
    }

    /** A set ref. */
    public <E> Ref<PSet<E>> setRef(String name, Codec<E> codec) {
        return ref(name, RefKind.set(codec));
    }

    /** A vector ref. */
    public <E> Ref<PVector<E>> vectorRef(String name, Codec<E> codec) {
        return ref(name, RefKind.vector(codec));
    }

    /** A blob ref. */
    public Ref<PBlob> blobRef(String name) {
        return ref(name, RefKind.blob());
    }

    /** A tree ref: the map from relative path to file entry of PD-B-05. */
    public Ref<PMap<String, FileEntry>> treeRef(String name) {
        return ref(name, RefKind.tree());
    }

    @SuppressWarnings("unchecked")
    private <T> Ref<T> ref(String name, RefKind<T> kind) {
        Objects.requireNonNull(name, "name");
        if (name.isEmpty() || name.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > PRef.MAX_NAME_BYTES) {
            throw new IllegalArgumentException("a ref name is 1 to " + PRef.MAX_NAME_BYTES + " bytes");
        }
        State<?> existing = states.get(name);
        if (existing != null) {
            if (!existing.kind.name().equals(kind.name())) {
                throw new IllegalArgumentException("ref " + name + " is a " + existing.kind.name() + ", not a " + kind.name());
            }
            return new Ref<>(this, (State<T>) existing);
        }
        String mergeKind = mergeKinds.get(name);
        if (mergeKind != null && !mergeKind.equals(kind.name())) {
            throw new IllegalArgumentException("ref " + name + " is a " + kind.name() + ", but its merge function was registered for a " + mergeKind);
        }
        State<T> state = new State<>(name, kind, config.modes().get(name));
        State<?> raced = states.putIfAbsent(name, state);
        if (raced != null) {
            return new Ref<>(this, (State<T>) raced);
        }
        subscribe(state);
        return new Ref<>(this, state);
    }

    private <T> void subscribe(State<T> state) {
        state.subscription = space.notify(state.template, event -> onEvent(state, event), Lease.of(SUBSCRIPTION_LEASE));
        state.subscriptionRenewedMillis = clock.millis();
    }

    /**
     * Registers the merge function every reader of a name applies when two heads
     * meet (PD-R-05), in place of the rule of §3.6.1.
     *
     * @param name the ref name
     * @param fn   the function, or null to remove
     * @param <T>  the value type
     * @return this
     */
    public <T> Refs mergeFor(String name, MergeFunction<T> fn) {
        return mergeFor(name, null, fn);
    }

    /**
     * As {@link #mergeFor(String, MergeFunction)} with the kind the function
     * expects ({@code PRef.KIND_*}): a ref of the name with another kind refuses
     * the registration, and a later handle of another kind is refused too, so a
     * function meant for one group's vector never runs over another's map.
     *
     * @param name the ref name
     * @param kind the kind name, or null for any
     * @param fn   the function, or null to remove
     * @param <T>  the value type
     * @return this
     */
    public <T> Refs mergeFor(String name, String kind, MergeFunction<T> fn) {
        Objects.requireNonNull(name, "name");
        if (fn == null) {
            mergeFunctions.remove(name);
            mergeKinds.remove(name);
            return this;
        }
        if (kind != null) {
            if (!PRef.KINDS.contains(kind)) {
                throw new IllegalArgumentException("unknown kind " + kind);
            }
            State<?> existing = states.get(name);
            if (existing != null && !existing.kind.name().equals(kind)) {
                throw new IllegalArgumentException("ref " + name + " is a " + existing.kind.name() + ", not a " + kind);
            }
            mergeKinds.put(name, kind);
        } else {
            mergeKinds.remove(name);
        }
        mergeFunctions.put(name, fn);
        return this;
    }

    /**
     * The progress of the fetch that brought a version of a name, kept for the
     * last {@value #MAX_HAVES} fetched roots; empty for a version that arrived
     * inline or was never fetched here.
     *
     * @param name the ref name
     * @param root the version's root
     * @return the progress
     */
    public Optional<TreeFetch.Progress> fetched(String name, String root) {
        State<?> state = states.get(name);
        if (state == null) {
            return Optional.empty();
        }
        synchronized (state.fetched) {
            return Optional.ofNullable(state.fetched.get(root));
        }
    }

    /**
     * The version record this engine knows for a root of a name: the head as it
     * was last seen, whether or not it is live now.
     *
     * @param name the ref name
     * @param root the version's root
     * @return the version, or empty when this engine never saw its head
     */
    public Optional<Heads.Head> version(String name, String root) {
        State<?> state = states.get(name);
        if (state == null) {
            return Optional.empty();
        }
        synchronized (state.known) {
            return Optional.ofNullable(state.known.get(root));
        }
    }

    /** How many versions of a name this engine has seen heads for. */
    public int versionsKnown(String name) {
        State<?> state = states.get(name);
        if (state == null) {
            return 0;
        }
        synchronized (state.known) {
            return state.known.size();
        }
    }

    /** The ref names this engine has handles for. */
    public Set<String> names() {
        return Set.copyOf(states.keySet());
    }

    // ------------------------------------------------------------ reading

    /**
     * The heads of a name as this reader sees them now.
     *
     * @param name the ref name
     * @return the heads; empty when the name has no handle or no version
     */
    public Heads heads(String name) {
        State<?> state = states.get(name);
        return state == null ? new Heads(name, List.of()) : heads(state, false);
    }

    <T> Heads heads(State<T> state, boolean wait) {
        List<Heads.Head> live = liveEntries(state);
        List<Heads.Head> loadable = new ArrayList<>();
        List<Heads.Head> waiting = new ArrayList<>();
        // every fetch starts before any wait, so several unloadable heads cost one load timeout together
        for (Heads.Head head : live) {
            if (loadable(state, head)) {
                loadable.add(head);
            } else {
                waiting.add(head);
            }
        }
        if (wait && !waiting.isEmpty()) {
            awaitFetches(state, waiting);
        }
        for (Heads.Head head : waiting) {
            if (loadable(state, head)) {
                loadable.add(head);
                continue;
            }
            Pending pending = state.pending.get(head.root());
            if (wait && pending != null && !pending.fetch.isDoneOrFailed()) {
                pending.timedOut = true;
            }
            boolean settled = pending == null || pending.timedOut || pending.fetch.isDoneOrFailed();
            if (settled && state.ignoredEntries.add(head.entryId())) {
                headsIgnored.incrementAndGet();
            }
        }
        return new Heads(state.name, headsAmong(loadable));
    }

    /**
     * The entries no other entry supersedes, equal roots folded. A base link
     * supersedes only the versions below it: seq grows along every base link,
     * so the entry that lists a root in its base supersedes the entries of that
     * root with a smaller seq (and an entry at {@code PRef.MAX_SEQ} whatever it
     * lists). A version that recreates an ancestor's content, and with it the
     * ancestor's root, therefore stays a head over the entries it was computed
     * from, where a fold by root alone left the ref with no head at all. Equal
     * roots fold to the entry with the largest seq, the earlier issued first
     * among equals.
     */
    private static List<Heads.Head> headsAmong(List<Heads.Head> entries) {
        Map<String, Long> supersededBelow = new HashMap<>();
        for (Heads.Head head : entries) {
            long by = head.seq() >= PRef.MAX_SEQ ? Long.MAX_VALUE : head.seq();
            for (String base : head.base()) {
                supersededBelow.merge(base, by, Math::max);
            }
        }
        Map<String, Heads.Head> byRoot = new LinkedHashMap<>();
        List<Heads.Head> ordered = new ArrayList<>(entries);
        ordered.sort(Heads.MERGE_ORDER);
        for (Heads.Head head : ordered) {
            Long by = supersededBelow.get(head.root());
            if (by != null && by > head.seq()) {
                continue;
            }
            byRoot.merge(head.root(), head, (kept, next) -> next.seq() > kept.seq() ? next : kept);
        }
        return new ArrayList<>(byRoot.values());
    }

    <T> Result<T> deref(State<T> state) {
        Heads heads = heads(state, true);
        if (heads.isEmpty() && state.linear()) {
            // a take hides the head from readAllEntries; the taken version is still the ref's value (PD-R-04)
            Heads last = state.lastHeads;
            if (last != null && !last.isEmpty() && allHeld(last)) {
                heads = last;
            } else {
                Heads.Head latest = latestHeldVersion(state);
                if (latest != null) {
                    heads = new Heads(state.name, List.of(latest));
                }
            }
        }
        if (heads.isEmpty()) {
            return new Result<>(state.kind.empty(store, loader), heads);
        }
        state.lastHeads = heads;
        return new Result<>(materialize(state, heads, store), heads);
    }

    /** The held version with the largest seq this engine knows, for a linear ref whose head is taken. */
    private Heads.Head latestHeldVersion(State<?> state) {
        List<Heads.Head> known;
        synchronized (state.known) {
            known = new ArrayList<>(state.known.values());
        }
        Heads.Head best = null;
        for (Heads.Head version : known) {
            if (best != null && (version.seq() < best.seq()
                    || (version.seq() == best.seq() && Heads.MERGE_ORDER.compare(version, best) <= 0))) {
                continue;
            }
            if (held(version.root())) {
                best = version;
            }
        }
        return best;
    }

    private boolean allHeld(Heads heads) {
        for (Heads.Head head : heads.heads()) {
            if (!held(head.root())) {
                return false;
            }
        }
        return true;
    }

    /** The value of the heads over a store: the single head loaded, or the merge of §3.6.1. */
    @SuppressWarnings("unchecked")
    private <T> T materialize(State<T> state, Heads heads, NodeStore over) {
        List<Heads.Head> list = heads.heads();
        if (list.size() == 1) {
            return state.kind.load(over, loader, list.get(0).root());
        }
        MergeFunction<T> fn = (MergeFunction<T>) mergeFunctions.get(state.name);
        String declaredKind = mergeKinds.get(state.name);
        if (fn != null && declaredKind != null && !declaredKind.equals(state.kind.name())) {
            fn = null;
        }
        Heads.Head latest = heads.latest();
        if (fn == null && !mergesByKey(state)) {
            // the whole-head rule needs no base, so no base fetch and no wait precede it
            for (Heads.Head other : list) {
                if (other != latest) {
                    conflict(state, new PConflict(state.name, null, latest.root(), other.root(), PConflict.WHOLE_HEAD));
                }
            }
            return state.kind.load(over, loader, latest.root());
        }
        String baseRoot = commonBase(state, list);
        if (baseRoot == null && fn == null) {
            for (Heads.Head other : list) {
                if (other != latest) {
                    conflict(state, new PConflict(state.name, null, latest.root(), other.root(), PConflict.NO_COMMON_BASE));
                }
            }
            return state.kind.load(over, loader, latest.root());
        }
        T base = baseRoot == null ? null : EMPTY_BASE.equals(baseRoot) ? state.kind.empty(over, loader) : state.kind.load(over, loader, baseRoot);
        T acc = state.kind.load(over, loader, list.get(0).root());
        for (int i = 1; i < list.size(); i++) {
            Heads.Head later = list.get(i);
            Heads.Head earlier = list.get(i - 1);
            T laterValue = state.kind.load(over, loader, later.root());
            if (fn != null) {
                try {
                    acc = Objects.requireNonNull(fn.merge(base, acc, laterValue), "the merge function returned null");
                    continue;
                } catch (RuntimeException failed) {
                    // the function's failure is its own: the rule of §3.6.1 decides, and the event says so
                    mergeFunctionFailures.incrementAndGet();
                    conflict(state, new PConflict(state.name, null, later.root(), earlier.root(), PConflict.MERGE_FUNCTION_FAILED));
                    if (!mergesByKey(state) || baseRoot == null) {
                        acc = laterValue;
                        continue;
                    }
                }
            }
            Merge.Resolver<Object, Object> resolver = (key, b, mine, theirs) -> {
                conflict(state, new PConflict(state.name, String.valueOf(key), later.root(), earlier.root(), PConflict.LATER_HLC));
                return theirs;
            };
            acc = state.kind.merge3(acc, base, laterValue, resolver);
        }
        return acc;
    }

    private static boolean mergesByKey(State<?> state) {
        String kind = state.kind.name();
        return PRef.KIND_MAP.equals(kind) || PRef.KIND_SET.equals(kind) || PRef.KIND_TREE.equals(kind);
    }

    /**
     * The lowest common base of the heads (PD-R-06): the first root in the
     * breadth-first base walk of the latest head, at most {@value #MAX_BASE_STEPS}
     * steps deep, that every other head's walk reaches and the store holds
     * complete; null when there is none.
     */
    private String commonBase(State<?> state, List<Heads.Head> heads) {
        boolean allFirst = true;
        for (Heads.Head head : heads) {
            allFirst &= head.base().isEmpty();
        }
        if (allFirst) {
            // every head is a first version (two writers started the ref at once): the empty value is their base
            return EMPTY_BASE;
        }
        List<Set<String>> walks = new ArrayList<>();
        for (Heads.Head head : heads) {
            walks.add(ancestors(state, head.root()));
        }
        Set<String> latest = walks.get(walks.size() - 1);
        for (String candidate : latest) {
            boolean common = true;
            for (int i = 0; i < walks.size() - 1 && common; i++) {
                common = walks.get(i).contains(candidate);
            }
            if (!common) {
                continue;
            }
            // the base is chosen by the graph alone, so every reader picks the same one; a reader that does
            // not hold it fetches it from its issuer, and only when that fails does the later head win whole
            if (held(candidate)) {
                return candidate;
            }
            Heads.Head version;
            synchronized (state.known) {
                version = state.known.get(candidate);
            }
            if (version == null) {
                // the heads list the base but this engine never saw its record (a late joiner, or a restart):
                // fetch it by root from the heads' issuers, who retain it (PD-R-06)
                version = syntheticVersion(state, candidate, heads);
            }
            List<String> haves = new ArrayList<>();
            for (Heads.Head head : heads) {
                if (held(head.root()) && haves.size() < MAX_HAVES) {
                    haves.add(head.root());
                }
            }
            Pending pending = startFetch(state, version, haves, true, issuersOf(heads));
            if (pending != null) {
                awaitFetch(pending);
            }
            if (held(candidate)) {
                settle(state, candidate);
                return candidate;
            }
            return null;
        }
        return null;
    }

    /** A version record for a root the heads list as a base and this engine never saw a head of: the latest head's stamp and issuer. */
    private static Heads.Head syntheticVersion(State<?> state, String root, List<Heads.Head> heads) {
        Heads.Head latest = heads.get(heads.size() - 1);
        return new Heads.Head(root, List.of(), 0, 0, state.kind.name(), latest.mode(), latest.issuer(), latest.issued(),
                latest.entryId(), latest.expiresAtMillis());
    }

    private static List<PeerId> issuersOf(List<Heads.Head> heads) {
        List<PeerId> peers = new ArrayList<>();
        for (Heads.Head head : heads) {
            if (!peers.contains(head.issuer().peer())) {
                peers.add(head.issuer().peer());
            }
        }
        return peers;
    }

    /** The roots reachable from a root over base links, breadth first, the root itself first. */
    private Set<String> ancestors(State<?> state, String root) {
        Set<String> seen = new LinkedHashSet<>();
        Deque<String> frontier = new ArrayDeque<>();
        Deque<Integer> depths = new ArrayDeque<>();
        frontier.add(root);
        depths.add(0);
        while (!frontier.isEmpty()) {
            String cid = frontier.poll();
            int depth = depths.poll();
            if (!seen.add(cid) || depth >= MAX_BASE_STEPS) {
                continue;
            }
            Heads.Head version;
            synchronized (state.known) {
                version = state.known.get(cid);
            }
            if (version == null) {
                continue;
            }
            for (String base : version.base()) {
                if (!seen.contains(base)) {
                    frontier.add(base);
                    depths.add(depth + 1);
                }
            }
        }
        return seen;
    }

    private void conflict(State<?> state, PConflict event) {
        conflicts.incrementAndGet();
        for (Consumer<PConflict> listener : state.conflictListeners) {
            try {
                listener.accept(event);
            } catch (RuntimeException ignored) {
                // a listener's failure is its own
            }
        }
    }

    <T> List<Heads.Head> history(State<T> state, int n) {
        if (n <= 0) {
            return List.of();
        }
        Heads heads = state.lastHeads;
        if (heads == null || heads.isEmpty()) {
            heads = heads(state, false);
        }
        Set<String> exclude = new HashSet<>(heads.roots());
        List<Heads.Head> out = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        Deque<String> frontier = new ArrayDeque<>();
        for (Heads.Head head : heads.heads()) {
            frontier.add(head.root());
        }
        while (!frontier.isEmpty() && out.size() < n) {
            String cid = frontier.poll();
            if (!seen.add(cid)) {
                continue;
            }
            Heads.Head version;
            synchronized (state.known) {
                version = state.known.get(cid);
            }
            if (version == null) {
                continue;
            }
            if (!exclude.contains(cid) && held(cid)) {
                out.add(version);
            }
            frontier.addAll(version.base());
        }
        return out;
    }

    <T> T at(State<T> state, String root) {
        if (held(root) || state.kind.lazy()) {
            return state.kind.load(store, loader, root);
        }
        throw new NodeUnavailableException(root, List.of(), "version " + root + " of " + state.name + " is not held");
    }

    // ------------------------------------------------------------ entries

    /** The live entries of the name as heads, refreshing the known versions and the live roots. */
    private <T> List<Heads.Head> liveEntries(State<T> state) {
        List<Space.Entry<PRef>> entries = space.readAllEntries(state.template, MAX_ENTRIES);
        if (entries.size() >= MAX_ENTRIES) {
            // the space answers an arbitrary subset past the cap, so the heads are not trustworthy; counted, never silent
            entriesTruncated.incrementAndGet();
        }
        List<Heads.Head> live = new ArrayList<>(entries.size());
        Set<String> roots = new HashSet<>();
        Set<EntryId> ids = new HashSet<>();
        for (Space.Entry<PRef> entry : entries) {
            ids.add(entry.entryId());
            Heads.Head head = toHead(state, entry.value(), entry.issuer(), entry.issued(), entry.entryId(),
                    entry.lease() == null ? 0 : entry.lease().expiresAtMillis());
            if (head == null) {
                continue;
            }
            live.add(head);
            roots.add(head.root());
        }
        state.liveRoots = roots;
        state.liveIds = ids;
        return live;
    }

    /** A head from an entry, recorded among the known versions; null when the entry is not a head of this ref. */
    private <T> Heads.Head toHead(State<T> state, PRef record, AgentId issuer, HlcTimestamp issued,
                                  EntryId entryId, long expiresAtMillis) {
        return toHead(state, record, issuer, issued, entryId, expiresAtMillis, false);
    }

    /**
     * As {@link #toHead}; with {@code onlyIfAbsent} the known versions gain the
     * record only when they lack it, and with no expiry, so a taken entry's claim
     * never makes its version a rewrite candidate.
     */
    private <T> Heads.Head toHead(State<T> state, PRef record, AgentId issuer, HlcTimestamp issued,
                                  EntryId entryId, long expiresAtMillis, boolean onlyIfAbsent) {
        if (record == null || issuer == null || issued == null) {
            return null;
        }
        if (record.violation().isPresent() || !record.name().equals(state.name) || !record.kind().equals(state.kind.name())) {
            if (state.ignoredEntries.add(entryId)) {
                headsIgnored.incrementAndGet();
            }
            return null;
        }
        if (state.observedMode == null && state.configuredMode == null) {
            state.observedMode = record.mode();
        }
        // a rewrite carries the original head's stamp and issuer (PD-R-08), so the merge order stays put
        if (record.issuedAttr() != null) {
            try {
                HlcTimestamp original = HlcTimestamp.parse(record.issuedAttr());
                // HLC canonical stamps, 2026-11-08: a counter above the limit orders nothing, like any
                // stamp a merge point refuses; parse already refuses text that is not canonical
                if (HybridLogicalClock.isAcceptable(original)) {
                    AgentId originalIssuer = record.issuerAttr() == null ? issuer : AgentId.parse(record.issuerAttr());
                    issued = original;
                    issuer = originalIssuer;
                }
            } catch (IllegalArgumentException malformed) {
                // the reserved attrs are advisory: a malformed pair leaves the entry's own stamp
            }
        }
        Heads.Head head = new Heads.Head(record.root(), record.base(), record.seq(), record.size(), record.kind(),
                record.mode(), issuer, issued, entryId, expiresAtMillis);
        synchronized (state.known) {
            Heads.Head known = state.known.get(head.root());
            if (onlyIfAbsent) {
                if (known == null) {
                    state.known.put(head.root(), head.seenAs(entryId, Long.MAX_VALUE));
                }
            } else if (known == null) {
                state.known.put(head.root(), head);
            } else {
                // a record with a larger seq for a known root is a later version that recreated the root (a
                // revert); the version keeps the union of the bases every record of it named, so the base walks
                // of PD-R-06 and the cancellation rule still reach the versions the earlier record was computed from
                boolean newer = head.seq() > known.seq();
                List<String> bases = new ArrayList<>(newer ? head.base() : known.base());
                for (String base : newer ? known.base() : head.base()) {
                    if (!bases.contains(base)) {
                        bases.add(base);
                    }
                }
                boolean sameEntry = known.entryId().equals(entryId) && known.expiresAtMillis() == expiresAtMillis;
                if (newer || !sameEntry || bases.size() != known.base().size()) {
                    Heads.Head kept = newer ? head : known.seenAs(entryId, expiresAtMillis);
                    state.known.put(head.root(), bases.equals(kept.base()) ? kept : kept.withBase(bases));
                }
            }
        }
        return head;
    }

    /**
     * Whether a head's value can be read now: held complete, or readable lazily
     * through the loader; otherwise its fetch is started (or found) and the
     * caller may wait for it with {@link #awaitFetches}. Never throws for a root
     * that is not a CID: the head is simply not loadable.
     */
    private <T> boolean loadable(State<T> state, Heads.Head head) {
        String root = head.root();
        if (held(root)) {
            settle(state, root);
            return true;
        }
        if (!PRef.isCid(root)) {
            return false;
        }
        Pending pending = state.pending.get(root);
        if (pending == null && (config.followFull() || !state.kind.lazy()) && !backingOff(state, root)) {
            state.recheck.remove(root);
            pending = startFetch(state, head);
        }
        if (pending != null) {
            if (held(root)) {
                settle(state, root);
                return true;
            }
            if (pending.fetch.isDoneOrFailed() && !pending.fetch.complete()) {
                state.pending.remove(root, pending);
                markUnloadable(state, root);
            }
            return false;
        }
        // lazy follow: the holders of the root and its issuer serve the bare batches of each get (PD-X-05),
        // and the root node answers whether the version can be read at all
        swarm.fetcher().follow(root).prefer(head.issuer().peer());
        try {
            loader.load(root);
            return true;
        } catch (NodeUnavailableException | IllegalArgumentException unavailable) {
            return false;
        }
    }

    /** Whether a root's fetch failed recently enough that no new fetch starts for it yet. */
    private boolean backingOff(State<?> state, String root) {
        Long failedAt;
        synchronized (state.unloadable) {
            failedAt = state.unloadable.get(root);
        }
        return failedAt != null && clock.millis() - failedAt < config.loadTimeout().toMillis() * UNLOADABLE_BACKOFF;
    }

    private void markUnloadable(State<?> state, String root) {
        synchronized (state.unloadable) {
            state.unloadable.put(root, clock.millis());
        }
    }

    /** Waits for the fetches of several heads together, against one load timeout; heads already timed out are not waited for. */
    private <T> void awaitFetches(State<T> state, List<Heads.Head> heads) {
        List<Pending> pendings = new ArrayList<>();
        for (Heads.Head head : heads) {
            Pending pending = state.pending.get(head.root());
            if (pending != null && !pending.timedOut) {
                pendings.add(pending);
            }
        }
        if (pendings.isEmpty()) {
            return;
        }
        long deadline = System.nanoTime() + config.loadTimeout().toNanos();
        while (System.nanoTime() < deadline) {
            boolean outstanding = false;
            for (Pending pending : pendings) {
                if (!held(pending.head.root()) && !pending.fetch.isDoneOrFailed()) {
                    outstanding = true;
                    break;
                }
            }
            if (!outstanding) {
                return;
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void awaitFetch(Pending pending) {
        long deadline = System.nanoTime() + config.loadTimeout().toNanos();
        while (!held(pending.head.root()) && !pending.fetch.isDoneOrFailed()) {
            if (System.nanoTime() >= deadline) {
                return;
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private <T> Pending startFetch(State<T> state, Heads.Head head) {
        return startFetch(state, head, haves(state, head), false, List.of(head.issuer().peer()));
    }

    /**
     * Starts the fetch of a version unless one is under way; the session fails
     * after the load timeout without a reachable holder (PD-X-08), so the tick
     * reaps it and nothing waits forever.
     */
    private <T> Pending startFetch(State<T> state, Heads.Head head, List<String> haves, boolean base, List<PeerId> prefer) {
        Pending existing = state.pending.get(head.root());
        if (existing != null) {
            return existing;
        }
        FetchOptions options = new FetchOptions(config.loadTimeout(), Set.copyOf(prefer), Set.of());
        TreeFetch fetch;
        try {
            fetch = swarm.fetchTree(head.root(), haves, options);
        } catch (IllegalArgumentException malformed) {
            return null;
        }
        Pending pending = new Pending(fetch, head, clock.millis(), base);
        Pending raced = state.pending.putIfAbsent(head.root(), pending);
        if (raced != null) {
            fetch.cancel();
            return raced;
        }
        return pending;
    }

    /**
     * Fetches the bases of a complete head that this peer knows but does not hold,
     * with the head as a have, so the version graph every reader walks for the
     * common base of PD-R-06 is held here too; a base that arrived out of order
     * or was only ever referenced is completed for the cost of the nodes the
     * head replaced.
     */
    private <T> void ensureBases(State<T> state, Heads.Head head) {
        for (String base : head.base()) {
            if (held(base) || state.pending.containsKey(base)) {
                continue;
            }
            if (!PRef.isCid(base)) {
                continue;
            }
            Heads.Head version;
            synchronized (state.known) {
                version = state.known.get(base);
            }
            if (version == null) {
                // never seen as a head here: the merge fetches it by root when it needs it as the common base
                continue;
            }
            List<String> haves = new ArrayList<>();
            haves.add(head.root());
            for (String have : haves(state, version)) {
                if (haves.size() >= MAX_HAVES) {
                    break;
                }
                if (!haves.contains(have)) {
                    haves.add(have);
                }
            }
            startFetch(state, version, haves, true, List.of(head.issuer().peer()));
        }
    }

    /** The roots this ref holds complete, the head's bases first, at most {@value #MAX_HAVES}. */
    private <T> List<String> haves(State<T> state, Heads.Head head) {
        LinkedHashSet<String> haves = new LinkedHashSet<>();
        for (String base : head.base()) {
            if (held(base)) {
                haves.add(base);
            }
        }
        for (Retention.Version version : retention.versions(state.name)) {
            if (haves.size() >= MAX_HAVES) {
                break;
            }
            if (!version.root().equals(head.root()) && held(version.root())) {
                haves.add(version.root());
            }
        }
        List<String> out = new ArrayList<>(haves);
        return out.size() > MAX_HAVES ? out.subList(0, MAX_HAVES) : out;
    }

    /** Closes a pending fetch whose root is complete: counts it, records the version, and tells the watchers. */
    private <T> boolean settle(State<T> state, String root) {
        Pending pending = state.pending.remove(root);
        if (pending == null) {
            return false;
        }
        TreeFetch.Progress progress = pending.fetch.progress();
        synchronized (state.fetched) {
            state.fetched.put(root, progress);
        }
        bytesReceived.addAndGet(progress.bytesReceived());
        state.fetchedRoots.add(root);
        retention.recordVersion(state.name, root, clock.millis());
        if (pending.base) {
            basesFetched.incrementAndGet();
            return true;
        }
        deltasFetched.incrementAndGet();
        ensureBases(state, pending.head);
        deliver(state, pending.head);
        return true;
    }

    private <T> void deliver(State<T> state, Heads.Head head) {
        if (state.watchers.isEmpty()) {
            return;
        }
        T value;
        try {
            value = state.kind.load(store, loader, head.root());
        } catch (RuntimeException unavailable) {
            return;
        }
        for (Consumer<T> watcher : state.watchers) {
            try {
                watcher.accept(value);
            } catch (RuntimeException ignored) {
                // a watcher's failure is its own
            }
        }
    }

    // ------------------------------------------------------------ events

    private <T> void onEvent(State<T> state, SpaceEvent<PRef> event) {
        if (closed) {
            return;
        }
        if (event.kind() == SpaceEvent.Kind.TAKEN) {
            // a linear swap is under way somewhere: the version it took is known, and no first head is written meanwhile
            state.takenEntries.add(event.entryId());
            Space.Entry<PRef> taken = event.details();
            if (taken != null) {
                toHead(state, event.entry(), event.issuer(), taken.issued(), event.entryId(), 0, true);
            }
            return;
        }
        if (event.kind() == SpaceEvent.Kind.COMPLETED || event.kind() == SpaceEvent.Kind.EXPIRED) {
            state.takenEntries.remove(event.entryId());
            return;
        }
        if (event.kind() == SpaceEvent.Kind.REAPPEARED) {
            state.takenEntries.remove(event.entryId());
        }
        if (event.kind() != SpaceEvent.Kind.WRITTEN && event.kind() != SpaceEvent.Kind.REAPPEARED) {
            return;
        }
        Space.Entry<PRef> details = event.details();
        Heads.Head head;
        if (details != null) {
            head = toHead(state, event.entry(), event.issuer(), details.issued(), event.entryId(),
                    details.lease() == null ? 0 : details.lease().expiresAtMillis());
        } else {
            head = null;
            for (Space.Entry<PRef> entry : space.readAllEntries(state.template, MAX_ENTRIES)) {
                if (entry.entryId().equals(event.entryId())) {
                    head = toHead(state, entry.value(), entry.issuer(), entry.issued(), entry.entryId(),
                            entry.lease() == null ? 0 : entry.lease().expiresAtMillis());
                    break;
                }
            }
        }
        if (head == null) {
            return;
        }
        PRef record = event.entry();
        boolean own = state.ownEntries.contains(event.entryId()) || state.ownRoots.contains(record.root());
        if (!own && record.nodes() != null && !record.nodes().isEmpty()) {
            InlineDelta.Accepted accepted = InlineDelta.accept(store, record.root(), record.nodes());
            if (accepted.stored() > 0) {
                deltasInline.incrementAndGet();
                bytesReceived.addAndGet(accepted.bytes());
            }
            if (accepted.dropped() > 0) {
                nodesDropped.addAndGet(accepted.dropped());
            }
        }
        if (held(record.root())) {
            arrived(state, head, own);
            return;
        }
        if (config.followFull() || !state.kind.lazy()) {
            state.recheck.put(record.root(), head);
            return;
        }
        swarm.fetcher().follow(record.root()).prefer(head.issuer().peer());
        deliver(state, head);
    }

    /** A head whose root is held: the version is recorded, its bases kept, and the watchers told. */
    private <T> void arrived(State<T> state, Heads.Head head, boolean own) {
        if (!own) {
            retention.recordVersion(state.name, head.root(), clock.millis());
            ensureBases(state, head);
        }
        if (!settle(state, head.root())) {
            deliver(state, head);
        }
    }

    // ------------------------------------------------------------ writing

    <T> T swap(State<T> state, UnaryOperator<T> f) {
        if (closed) {
            throw new IllegalStateException("the refs engine is closed");
        }
        return state.linear() ? swapLinear(state, f) : swapMulti(state, f);
    }

    private <T> T swapMulti(State<T> state, UnaryOperator<T> f) {
        InlineDelta.Recording recording = InlineDelta.recording(store);
        Heads heads = heads(state, true);
        T current = heads.isEmpty() ? state.kind.empty(recording.store(), loader) : materialize(state, heads, recording.store());
        if (!heads.isEmpty()) {
            state.lastHeads = heads;
        }
        T next = Objects.requireNonNull(f.apply(current), "the swap function returned null");
        String newRoot = state.kind.root(next);
        if (heads.heads().size() == 1 && heads.heads().get(0).root().equals(newRoot)) {
            return next;
        }
        PRef head = head(state, newRoot, next, heads, recording, state.mode());
        write(state, head, heads);
        return next;
    }

    /** Whether this engine never saw a version, a take, or a live entry of the name: only then is a first linear head written. */
    private boolean neverVersioned(State<?> state) {
        if (!liveEntries(state).isEmpty() || !state.takenEntries.isEmpty()) {
            return false;
        }
        Heads last = state.lastHeads;
        if (last != null && !last.isEmpty()) {
            return false;
        }
        synchronized (state.known) {
            if (!state.known.isEmpty()) {
                return false;
            }
        }
        return retention.versions(state.name).isEmpty();
    }

    private <T> T swapLinear(State<T> state, UnaryOperator<T> f) {
        for (int attempt = 0; attempt <= config.takeRetries(); attempt++) {
            liveEntries(state);
            Optional<TakenEntry<PRef>> taken = space.take(state.template, Lease.of(config.takeLease()), config.takeTimeout());
            if (taken.isEmpty()) {
                // a take may be outstanding elsewhere (the taken entry is hidden): a first head is written only when
                // nothing of the name was ever seen here, else the take is tried again and the swap fails past the retries
                if (attempt >= Math.min(1, config.takeRetries()) && neverVersioned(state)) {
                    return swapFirstLinear(state, f);
                }
                continue;
            }
            TakenEntry<PRef> take = taken.get();
            PRef record = take.entry();
            Heads.Head takenHead;
            synchronized (state.known) {
                takenHead = state.known.get(record.root());
            }
            if (takenHead == null) {
                throw new IllegalStateException("took a head of " + state.name + " this engine never read: " + record);
            }
            if (!loadable(state, takenHead)) {
                awaitFetches(state, List.of(takenHead));
            }
            if (!loadable(state, takenHead)) {
                throw new NodeUnavailableException(record.root(), List.of(),
                        "the taken head of " + state.name + " could not be loaded; its take lease will lapse");
            }
            List<Heads.Head> others = new ArrayList<>();
            List<Heads.Head> candidates = new ArrayList<>();
            for (Heads.Head other : liveEntries(state)) {
                if (!other.entryId().equals(take.entryId())) {
                    candidates.add(other);
                }
            }
            awaitFetches(state, candidates);
            for (Heads.Head other : candidates) {
                if (loadable(state, other)) {
                    others.add(other);
                }
            }
            others.add(takenHead);
            Heads heads = new Heads(state.name, headsAmong(others));
            if (!heads.roots().contains(takenHead.root())) {
                heads = new Heads(state.name, List.of(takenHead));
            }
            state.lastHeads = heads;
            InlineDelta.Recording recording = InlineDelta.recording(store);
            T current = materialize(state, heads, recording.store());
            T next = Objects.requireNonNull(f.apply(current), "the swap function returned null");
            String newRoot = state.kind.root(next);
            PRef head = newRoot.equals(takenHead.root()) && heads.heads().size() == 1
                    ? record.withoutNodes()
                    : head(state, newRoot, next, heads, recording, PRef.MODE_LINEAR);
            checkOwn(state, head);
            pin(state, head);
            state.ownRoots.add(newRoot);
            EntryHandle handle = space.complete(take, head, Lease.of(config.headLease()), head.tags());
            state.ownEntries.add(handle.entryId());
            state.ownEntries.remove(take.entryId());
            long now = clock.millis();
            synchronized (state.own) {
                // the take consumed the entry this peer may have owned; an older own entry for the new root goes after the grace
                state.own.remove(take.entryId());
                for (Own existing : state.own.values()) {
                    if (existing.root.equals(newRoot)) {
                        existing.displaced = true;
                    }
                }
                state.own.put(handle.entryId(), new Own(handle, newRoot, now, head.base()));
            }
            releaseSuperseded(state, newRoot, heads);
            retention.recordVersion(state.name, newRoot, now);
            liveEntries(state);
            rememberWritten(state, newRoot);
            return next;
        }
        throw new IllegalStateException("could not take the head of " + state.name + " in "
                + (config.takeRetries() + 1) + " attempts of " + config.takeTimeout().toMillis() + " ms");
    }

    /** The first version of a linear ref, when no head exists to take. */
    private <T> T swapFirstLinear(State<T> state, UnaryOperator<T> f) {
        InlineDelta.Recording recording = InlineDelta.recording(store);
        T next = Objects.requireNonNull(f.apply(state.kind.empty(recording.store(), loader)), "the swap function returned null");
        String newRoot = state.kind.root(next);
        Heads none = new Heads(state.name, List.of());
        PRef head = head(state, newRoot, next, none, recording, PRef.MODE_LINEAR);
        write(state, head, none);
        return next;
    }

    /** The head record of a new version, with its delta inline when it fits (PD-X-07). */
    private <T> PRef head(State<T> state, String newRoot, T next, Heads heads, InlineDelta.Recording recording, String mode) {
        List<Map.Entry<String, byte[]>> delta = recording.delta(newRoot);
        long bytes = 0;
        List<byte[]> inline = new ArrayList<>(delta.size());
        for (Map.Entry<String, byte[]> node : delta) {
            bytes += node.getValue().length;
            inline.add(node.getValue());
        }
        List<byte[]> nodes = !inline.isEmpty() && bytes <= config.inlineDelta() ? inline : null;
        List<String> base = heads.roots();
        if (base.size() > PRef.MAX_BASE) {
            base = base.subList(base.size() - PRef.MAX_BASE, base.size());
        }
        // seq is advisory and saturates at PRef.MAX_SEQ, so a head at the ceiling never makes its successors negative
        long seq = Math.min(heads.maxSeq(), PRef.MAX_SEQ - 1) + 1;
        return new PRef(PRef.VERSION, state.name, state.kind.name(), newRoot, base, seq,
                state.kind.size(next), nodes, mode, config.sealing(), keyEpoch(), null);
    }

    /**
     * The epoch a head names: the sealed store's write epoch in the sealed-nodes mode
     * of §3.8 (every node under the root carries its own epoch; the head's is the
     * writer's at the time), and absent otherwise.
     */
    private Long keyEpoch() {
        if (store instanceof ai.badmonkey.agentspaces.pdt.seal.SealedNodeStore sealed) {
            return sealed.sealer().writeEpoch().orElse(null);
        }
        return null;
    }

    /** A head this writer is about to publish must pass its own bounds; a head every reader would drop is not written. */
    private static void checkOwn(State<?> state, PRef head) {
        Optional<String> violation = head.violation();
        if (violation.isPresent()) {
            throw new IllegalStateException("the new head of " + state.name + " violates a bound and would be ignored by every reader: " + violation.get());
        }
    }

    /**
     * Pins a new version's root; when its delta travels by request the root is
     * also held, so the holding names this writer as a source (PD-S-06). An inline
     * delta needs no holding: readers prefer the issuer when they must fetch, and
     * a holding per swap would churn the content space.
     */
    private <T> void pin(State<T> state, PRef head) {
        if (!state.pinned.add(head.root())) {
            // the same version written again (a revert to a root this writer still pins): one pin per root
            return;
        }
        if (head.nodes() == null) {
            state.heldRoots.add(head.root());
            swarm.hold(head.root());
        } else {
            store.pin(head.root());
        }
    }

    private <T> void unpin(State<T> state, String root) {
        if (state.heldRoots.remove(root)) {
            swarm.release(root);
        } else {
            store.unpin(root);
        }
    }

    /** Pins and holds the root, writes the head, and books the write. */
    private <T> EntryHandle write(State<T> state, PRef head, Heads superseded) {
        checkOwn(state, head);
        pin(state, head);
        state.ownRoots.add(head.root());
        EntryHandle handle = space.write(head, Lease.of(config.headLease()), head.tags());
        state.ownEntries.add(handle.entryId());
        long now = clock.millis();
        synchronized (state.own) {
            // an older own entry for the same root (a revert within the grace) is displaced: it goes, after the
            // grace, once the versions it supersedes are gone, so they never become heads again meanwhile
            for (Own existing : state.own.values()) {
                if (existing.root.equals(head.root())) {
                    existing.displaced = true;
                }
            }
            state.own.put(handle.entryId(), new Own(handle, head.root(), now, head.base()));
            Set<String> oldRoots = new HashSet<>(superseded.roots());
            for (Own own : state.own.values()) {
                if (oldRoots.contains(own.root) && own.supersededAtMillis == null) {
                    own.supersededAtMillis = now;
                }
            }
        }
        releaseSuperseded(state, head.root(), superseded);
        retention.recordVersion(state.name, head.root(), now);
        liveEntries(state);
        rememberWritten(state, head.root());
        return handle;
    }

    /** Whether this peer has a live own entry for the root; called under the lock of {@code state.own}. */
    private static boolean ownsRoot(State<?> state, String root) {
        for (Own own : state.own.values()) {
            if (own.root.equals(root)) {
                return true;
            }
        }
        return false;
    }

    /** After a write, the heads this peer last read are the head it wrote, so history walks from it. */
    private <T> void rememberWritten(State<T> state, String root) {
        Heads.Head mine;
        synchronized (state.known) {
            mine = state.known.get(root);
        }
        if (mine != null) {
            state.lastHeads = new Heads(state.name, List.of(mine));
        }
    }

    private <T> void releaseSuperseded(State<T> state, String newRoot, Heads superseded) {
        for (Heads.Head old : superseded.heads()) {
            if (!old.root().equals(newRoot) && state.pinned.remove(old.root())) {
                unpin(state, old.root());
            }
        }
    }

    // ------------------------------------------------------------ tick

    /**
     * The engine's periodic work: settles finished fetches, renews own heads at
     * half-life, cancels own superseded heads after the grace, rewrites lapsed
     * heads this peer holds (PD-R-08), and keeps the notify subscriptions alive.
     */
    public void tick() {
        if (closed) {
            return;
        }
        long now = clock.millis();
        for (State<?> state : states.values()) {
            tick(state, now);
        }
    }

    private <T> void tick(State<T> state, long now) {
        List<Heads.Head> live = liveEntries(state);
        Set<String> liveRoots = new HashSet<>();
        Set<EntryId> liveIds = state.liveIds;
        Set<String> basesInUse = new HashSet<>();
        for (Heads.Head head : live) {
            liveRoots.add(head.root());
            basesInUse.addAll(head.base());
        }
        for (String root : List.copyOf(state.recheck.keySet())) {
            Heads.Head head = state.recheck.remove(root);
            if (head == null) {
                continue;
            }
            if (held(root)) {
                arrived(state, head, state.ownRoots.contains(root));
            } else if (liveIds.contains(head.entryId())) {
                startFetch(state, head);
            }
        }
        for (String root : List.copyOf(state.pending.keySet())) {
            Pending pending = state.pending.get(root);
            if (pending == null) {
                continue;
            }
            if (held(root)) {
                settle(state, root);
            } else if (pending.fetch.isDoneOrFailed() && !pending.fetch.complete()) {
                state.pending.remove(root, pending);
                markUnloadable(state, root);
                if (!pending.base && state.ignoredEntries.add(pending.head.entryId())) {
                    headsIgnored.incrementAndGet();
                }
            } else if (!pending.base && !liveIds.contains(pending.head.entryId()) && !basesInUse.contains(root)) {
                // the head that asked for this fetch is gone: the session is cancelled rather than kept alive forever
                state.pending.remove(root, pending);
                pending.fetch.cancel();
            }
        }
        // ignored entries and failed roots are remembered only while they are live, so both are bounded by the live entries
        state.ignoredEntries.retainAll(liveIds);
        state.takenEntries.removeIf(liveIds::contains);
        synchronized (state.unloadable) {
            state.unloadable.keySet().removeIf(root -> !liveRoots.contains(root) && !basesInUse.contains(root));
        }
        // a fetched version the swarm holds stops being advertised once it is no longer a live entry; the
        // retained nodes stay for delta bases, the holding and its refresh walk go (PD-S-06)
        for (String root : List.copyOf(state.fetchedRoots)) {
            if (!liveRoots.contains(root) && !state.pinned.contains(root)) {
                state.fetchedRoots.remove(root);
                swarm.release(root);
            }
        }
        long halfLife = config.headLease().toMillis() / 2;
        List<Own> owned;
        synchronized (state.own) {
            owned = new ArrayList<>(state.own.values());
        }
        // an own head is superseded while it lies below the lowest common base of the held heads: not while it is
        // a head itself (its only successors unloadable), and not while a fork still walks through it (PD-R-03, PD-R-06)
        Set<String> cancellable = cancellable(state, live);
        for (Own own : owned) {
            // a displaced entry goes once none of the versions it supersedes is live, so they never become heads again
            boolean superseded = cancellable.contains(own.root)
                    || (own.displaced && java.util.Collections.disjoint(own.base, liveRoots));
            if (own.supersededAtMillis == null && superseded) {
                own.supersededAtMillis = now;
            } else if (own.supersededAtMillis != null && !superseded) {
                own.supersededAtMillis = null;
            }
            if (own.supersededAtMillis != null) {
                if (now - own.supersededAtMillis >= config.cancelGrace().toMillis()) {
                    try {
                        own.handle.cancel();
                    } catch (RuntimeException gone) {
                        // the entry already lapsed or was consumed
                    }
                    boolean rootStillOwn;
                    synchronized (state.own) {
                        state.own.remove(own.handle.entryId(), own);
                        rootStillOwn = ownsRoot(state, own.root);
                    }
                    state.ownEntries.remove(own.handle.entryId());
                    if (!rootStillOwn && state.pinned.remove(own.root)) {
                        unpin(state, own.root);
                    }
                }
                continue;
            }
            if (now - own.lastRenewMillis >= halfLife) {
                try {
                    own.handle.renew(config.headLease());
                    own.lastRenewMillis = now;
                } catch (LeaseExpiredException lapsed) {
                    synchronized (state.own) {
                        state.own.remove(own.handle.entryId(), own);
                    }
                    state.ownEntries.remove(own.handle.entryId());
                } catch (RuntimeException ignored) {
                    // the next tick tries again
                }
            }
        }
        Set<String> candidates = new LinkedHashSet<>(state.previousLive);
        Heads last = state.lastHeads;
        if (last != null) {
            candidates.addAll(last.roots());
        }
        state.previousLive = liveRoots;
        for (String root : candidates) {
            boolean ownRoot;
            synchronized (state.own) {
                ownRoot = ownsRoot(state, root);
            }
            if (ownRoot || liveRoots.contains(root) || basesInUse.contains(root)) {
                continue;
            }
            Heads.Head known;
            synchronized (state.known) {
                known = state.known.get(root);
            }
            if (known == null || known.expiresAtMillis() > now || !held(root)) {
                continue;
            }
            rewrite(state, known);
        }
        if (now - state.subscriptionRenewedMillis >= SUBSCRIPTION_LEASE.toMillis() / 2) {
            try {
                state.subscription.renew(SUBSCRIPTION_LEASE);
                state.subscriptionRenewedMillis = now;
            } catch (RuntimeException lapsed) {
                try {
                    state.subscription.close();
                } catch (RuntimeException ignored) {
                    // already gone
                }
                subscribe(state);
            }
        }
    }

    /**
     * The roots a writer may cancel: the strict ancestors of the lowest common
     * base of the held heads (the base the merge of PD-R-06 picks). Every
     * version on a path from a head to that base stays live, so a reader that
     * arrives later walks the same graph to the same base; a head that only
     * unloadable successors list is a head itself and stays. A live entry this
     * writer has not loaded yet but still expects to (its record is known, its
     * fetch never failed) does not make a head, yet its base walk bounds the
     * base from below: once it lands the fleet merges against a base no higher
     * than the one it shares with the held heads, and a version cancelled on the
     * narrower view of the held heads alone would cut the path a later reader
     * walks (PD-R-06). Empty when no head is held or the heads and the arriving
     * entries share no base within the walk bound.
     */
    private <T> Set<String> cancellable(State<T> state, List<Heads.Head> live) {
        List<Heads.Head> loadable = new ArrayList<>();
        List<Heads.Head> arriving = new ArrayList<>();
        for (Heads.Head head : live) {
            // the tick may read an entry before its event delivered the inline delta, so it starts no fetch here:
            // a head counts once it is held, or once its root node is present under the lazy follow
            if (held(head.root()) || (state.kind.lazy() && !config.followFull() && store.has(head.root()))) {
                loadable.add(head);
            } else if (mayStillLoad(state, head.root())) {
                arriving.add(head);
            }
        }
        List<Heads.Head> heads = headsAmong(loadable);
        if (heads.isEmpty()) {
            return Set.of();
        }
        List<Set<String>> walks = new ArrayList<>();
        for (Heads.Head head : heads) {
            walks.add(ancestors(state, head.root()));
        }
        List<Set<String>> bounds = new ArrayList<>();
        for (Heads.Head head : arriving) {
            bounds.add(ancestors(state, head.root()));
        }
        String lowest = null;
        for (String candidate : walks.get(walks.size() - 1)) {
            boolean common = true;
            for (int i = 0; i < walks.size() - 1 && common; i++) {
                common = walks.get(i).contains(candidate);
            }
            for (int i = 0; i < bounds.size() && common; i++) {
                common = bounds.get(i).contains(candidate);
            }
            if (common) {
                lowest = candidate;
                break;
            }
        }
        if (lowest == null) {
            return Set.of();
        }
        Set<String> below = new HashSet<>(ancestors(state, lowest));
        below.remove(lowest);
        return below;
    }

    /**
     * Whether a live head's root, not held here, is still expected to arrive: a
     * CID the engine fetches in full (every kind under the full follow, the eager
     * kinds under the lazy one) whose fetch never failed. A root whose fetch
     * failed stays out of the cancellation rule even while a retry runs, so a
     * phantom head cannot hold a writer's superseded versions live forever.
     */
    private boolean mayStillLoad(State<?> state, String root) {
        if (!PRef.isCid(root) || (!config.followFull() && state.kind.lazy())) {
            return false;
        }
        synchronized (state.unloadable) {
            return !state.unloadable.containsKey(root);
        }
    }

    /**
     * Rewrites a lapsed head with the same root, base, and seq under this peer's
     * signature (PD-R-08), carrying the original stamp and issuer in the reserved
     * attrs so every reader keeps the merge order of the original head.
     */
    private <T> void rewrite(State<T> state, Heads.Head head) {
        Map<String, String> stamp = Map.of(PRef.ISSUED_ATTR, head.issued().encoded(), PRef.ISSUER_ATTR, head.issuer().encoded());
        PRef record = new PRef(PRef.VERSION, state.name, state.kind.name(), head.root(), head.base(), head.seq(),
                head.size(), null, head.mode(), config.sealing(), keyEpoch(), stamp);
        if (record.violation().isPresent()) {
            return;
        }
        state.pinned.add(head.root());
        state.heldRoots.add(head.root());
        swarm.hold(head.root());
        state.ownRoots.add(head.root());
        EntryHandle handle;
        try {
            handle = space.write(record, Lease.of(config.headLease()), record.tags());
        } catch (RuntimeException refused) {
            if (state.pinned.remove(head.root())) {
                unpin(state, head.root());
            }
            return;
        }
        state.ownEntries.add(handle.entryId());
        synchronized (state.own) {
            state.own.put(handle.entryId(), new Own(handle, head.root(), clock.millis(), head.base()));
        }
        headsRewritten.incrementAndGet();
        liveEntries(state);
    }

    // ------------------------------------------------------------ collection and stats

    /** The roots a collector must keep: live heads, own heads, pending fetches, and the last heads read, of every ref. */
    public Set<String> liveRoots() {
        Set<String> roots = new HashSet<>();
        for (State<?> state : states.values()) {
            roots.addAll(state.liveRoots);
            roots.addAll(state.pending.keySet());
            roots.addAll(state.pinned);
            synchronized (state.own) {
                for (Own own : state.own.values()) {
                    roots.add(own.root);
                }
            }
            Heads last = state.lastHeads;
            if (last != null) {
                roots.addAll(last.roots());
            }
        }
        return roots;
    }

    /** The handles created. */
    public int refCount() {
        return states.size();
    }

    /** The heads across every ref as last read. */
    public int headCount() {
        int n = 0;
        for (State<?> state : states.values()) {
            Heads last = state.lastHeads;
            n += last == null ? 0 : last.heads().size();
        }
        return n;
    }

    /** Heads whose inline delta was stored, at least in part. */
    public long deltasInline() {
        return deltasInline.get();
    }

    /** Heads whose delta a tree fetch brought. */
    public long deltasFetched() {
        return deltasFetched.get();
    }

    /** Conflict decisions this reader made (PD-R-05). */
    public long conflicts() {
        return conflicts.get();
    }

    /** Node bytes received inline or by fetch. */
    public long bytesReceived() {
        return bytesReceived.get();
    }

    /** Heads ignored: malformed, of another kind, or whose root failed to load (S7, PD-P-04). */
    public long headsIgnored() {
        return headsIgnored.get();
    }

    /** Inline nodes dropped because no link from the root reached them or their bytes did not hash to their CID (PD-X-07). */
    public long nodesDropped() {
        return nodesDropped.get();
    }

    /** Lapsed heads this peer rewrote (PD-R-08). */
    public long headsRewritten() {
        return headsRewritten.get();
    }

    /** Bases fetched to keep the version graph held for the common-base walk (PD-R-06). */
    public long basesFetched() {
        return basesFetched.get();
    }

    /** Reads of a name that filled {@value #MAX_ENTRIES} entries, so the heads computed from them are untrustworthy. */
    public long entriesTruncated() {
        return entriesTruncated.get();
    }

    /** Merge function calls that threw, after which the rule of §3.6.1 decided (PD-R-05). */
    public long mergeFunctionFailures() {
        return mergeFunctionFailures.get();
    }

    @Override
    public void close() {
        closed = true;
        for (State<?> state : states.values()) {
            Subscription subscription = state.subscription;
            if (subscription != null) {
                try {
                    subscription.close();
                } catch (RuntimeException ignored) {
                    // already gone
                }
            }
        }
    }
}
