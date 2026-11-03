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
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Subscription;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.api.spi.CapabilityProvider;
import ai.badmonkey.agentspaces.capabilities.runtime.PipeChannel;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.GroupKey;
import ai.badmonkey.agentspaces.common.crypto.GroupKeyRing;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * The content swarm capability, {@code aspace:cap/content-swarm}
 * (srad/large-networks-bulk-data.md). One instance per group runtime. It is the
 * publisher's API ({@link #publish}), the fetcher's API ({@link #fetch}), the
 * holder that answers wants from the store, and the {@link CapabilityProvider}
 * that advertises the capability and refreshes holdings from the group tick.
 *
 * <p>Everything crosses the wire as ordinary entries (the manifest and the
 * holdings, in the content space) and as {@code PIPE_DATA} payloads under this
 * capability type; no frame kind is added, and no transport scheme is named
 * anywhere in this module (principle 3 of the SRAD).
 */
public final class Content implements CapabilityProvider, AutoCloseable {

    /** The capability type. */
    public static final String TYPE = "aspace:cap/content-swarm";

    /** "Never happened" for a millisecond stamp: far enough in the past that {@code now - NEVER} cannot overflow. */
    static final long NEVER = Long.MIN_VALUE / 4;

    /** Advertisement TTL, as every shipped capability uses. */
    static final Duration ADVERTISEMENT_TTL = Duration.ofMinutes(15);

    /** The holding refresh rules (LB-H-02, LB-H-03). */
    static final int PIECES_PER_HOLDING_WRITE = 32;
    static final long HOLDING_WRITE_INTERVAL_MILLIS = 10_000;

    /** Roots whose holdings one tick visits at most (LB-H-05). */
    static final int HOLDINGS_PER_TICK = 64;

    private int holdingCursor;
    static final long MANIFEST_KEEPALIVE_INTERVAL_MILLIS = 30_000;
    static final long TABLE_FETCH_TIMEOUT_MILLIS = 5_000;
    static final long MANIFEST_RETRY_MILLIS = 2_000;

    private static final Logger LOG = Logger.getLogger(Content.class.getName());

    private static final class Holder {
        EntryHandle handle;
        Holding last;
        long writtenAtMillis = NEVER;
        int piecesAtWrite = -1;
        long renewedAtMillis;
    }

    private static final class Pending {
        final FetchSession session;
        final Subscription holdings;
        final List<Runnable> onDone = new java.util.concurrent.CopyOnWriteArrayList<>();
        long manifestAskedAtMillis = NEVER;
        boolean tableFetching;

        Pending(FetchSession session, Subscription holdings) {
            this.session = session;
            this.holdings = holdings;
        }
    }

    /** Counters an operator reads. */
    public record Stats(int fetchesActive, int rootsHeld, long bytesServed, long bytesReceived,
                        long wantsRefused, long piecesStruck) {
    }

    /** What {@link #describe(String)} answers. */
    public record Description(Manifest manifest, int completeHolders, int partialHolders,
                              BitSet held, boolean replicated, boolean advertised) {

        /** The form without the collection flag. */
        public Description(Manifest manifest, int completeHolders, int partialHolders, BitSet held, boolean replicated) {
            this(manifest, completeHolders, partialHolders, held, replicated, false);
        }
    }

    private final PipeChannel pipes;
    private final GroupRuntime runtime;
    private final ContentSpaces spaces;
    private final PeerId self;
    private final CborCodec codec;
    private final InstantSource clock;
    private final PieceStore store;
    private final ContentPolicy policy;
    private final BlockExchange blocks;
    private final GroupKeyRing keyRing;
    private final Authorizer authorizer;
    private final Lease holdingLease;
    private final Random random;
    private final int pieceSize;
    private final AtomicInteger requestIds = new AtomicInteger();
    private final Map<String, Pending> fetches = new ConcurrentHashMap<>();
    private final Deque<Pending> queued = new ArrayDeque<>();
    private final Map<String, Holder> holders = new ConcurrentHashMap<>();
    /** Roots published, shared, or fetched through this group's swarm (LB-K-06): what a collection here may list. */
    private final java.util.Set<String> servedHere = ConcurrentHashMap.newKeySet();
    /** The collections advertised over this swarm (LB-K-07), registered by their providers. */
    private final java.util.List<CollectionAssetProvider> collections = new java.util.concurrent.CopyOnWriteArrayList<>();
    /** In-place pieces verified at their first serve (LB-T-10), as {@code root#index}. */
    private final java.util.Set<String> externalVerified = ConcurrentHashMap.newKeySet();
    /** One lock per piece under first-serve verification, so concurrent wants read it once. */
    private final Map<String, Object> verifying = new ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicLong rootsDroppedOnSample = new java.util.concurrent.atomic.AtomicLong();
    private long downloadDay = NEVER;
    private long downloadedToday;
    private final Map<String, EntryHandle> manifestHandles = new ConcurrentHashMap<>();
    private final AtomicLong bytesServed = new AtomicLong();
    private final AtomicLong wantsRefused = new AtomicLong();
    private final AtomicLong piecesStruck = new AtomicLong();
    /**
     * Content frames leave the transport's reader thread at once: a holder that
     * served on it and a fetcher that absorbed on it would each block writing to
     * a peer blocked writing to them once the windows filled the socket buffers,
     * and the stall would count as a timeout. One virtual thread per frame.
     */
    private final java.util.concurrent.ExecutorService handlers =
            java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
    private long uploadDay = NEVER;
    private long uploadedToday;
    private long manifestsCheckedAtMillis = NEVER;
    private volatile boolean closed;

    private Content(Builder b) {
        this.pieceSize = b.pieceSize;
        this.pipes = Objects.requireNonNull(b.pipes, "pipes");
        this.runtime = Objects.requireNonNull(b.runtime, "runtime");
        this.spaces = new ContentSpaces(Objects.requireNonNull(b.space, "space"));
        this.self = Objects.requireNonNull(b.self, "self");
        this.codec = b.codec == null ? CborCodec.defaultCodec() : b.codec;
        this.clock = b.clock == null ? InstantSource.system() : b.clock;
        this.store = b.store == null ? new MemoryPieceStore(this.clock, 0) : b.store;
        this.policy = b.policy == null ? ContentPolicy.DEFAULT : b.policy;
        this.blocks = b.blocks;
        this.keyRing = b.keyRing;
        this.authorizer = b.authorizer;
        this.holdingLease = b.holdingLease == null ? Lease.of(Duration.ofMinutes(5)) : b.holdingLease;
        this.random = b.random == null ? new Random() : b.random;
        pipes.onCapability(TYPE, (from, payload) -> {
            if (closed) {
                return;
            }
            try {
                handlers.execute(() -> onFrame(from, payload));
            } catch (java.util.concurrent.RejectedExecutionException e) {
                // closing
            }
        });
    }

    /** Starts a builder. */
    public static Builder builder(PipeChannel pipes, GroupRuntime runtime, Space contentSpace, PeerId self) {
        return new Builder(pipes, runtime, contentSpace, self);
    }

    /** Builds a {@link Content}. */
    public static final class Builder {
        private final PipeChannel pipes;
        private final GroupRuntime runtime;
        private final Space space;
        private final PeerId self;
        private CborCodec codec;
        private InstantSource clock;
        private PieceStore store;
        private ContentPolicy policy;
        private BlockExchange blocks;
        private GroupKeyRing keyRing;
        private Authorizer authorizer;
        private Lease holdingLease;
        private Random random;
        private int pieceSize = ContentRoots.DEFAULT_PIECE_SIZE;

        private Builder(PipeChannel pipes, GroupRuntime runtime, Space space, PeerId self) {
            this.pipes = pipes;
            this.runtime = runtime;
            this.space = space;
            this.self = self;
        }

        public Builder codec(CborCodec value) {
            this.codec = value;
            return this;
        }

        public Builder clock(InstantSource value) {
            this.clock = value;
            return this;
        }

        public Builder store(PieceStore value) {
            this.store = value;
            return this;
        }

        public Builder policy(ContentPolicy value) {
            this.policy = value;
            return this;
        }

        /** The group's block exchange, used for piece tables above the inline bound. */
        public Builder blocks(BlockExchange value) {
            this.blocks = value;
            return this;
        }

        /** The group's content key ring: artifacts publish sealed and pieces travel sealed (LB-X-06). */
        public Builder keyRing(GroupKeyRing value) {
            this.keyRing = value;
            return this;
        }

        /** The authorizer consulted for {@code CONTENT_FETCH} before serving (LB-P-02). */
        public Builder authorizer(Authorizer value) {
            this.authorizer = value;
            return this;
        }

        public Builder holdingLease(Lease value) {
            this.holdingLease = value;
            return this;
        }

        /**
         * The piece size a publish without explicit options uses (the host's
         * {@code agentspaces.content.piece-size}); a power of two in [64 KiB, 4 MiB].
         *
         * @param value the piece size in bytes
         * @return this builder
         */
        public Builder pieceSize(int value) {
            this.pieceSize = value;
            return this;
        }

        public Builder random(Random value) {
            this.random = value;
            return this;
        }

        public Content build() {
            return new Content(this);
        }
    }

    // ------------------------------------------------------------ CapabilityProvider

    @Override
    public String capabilityType() {
        return TYPE;
    }

    @Override
    public CapabilityAdvertisement describe(GroupId group) {
        return new CapabilityAdvertisement(
                "aspace://" + group.value() + "/cap/content-swarm/" + self.value(),
                self, group, clock.instant(), ADVERTISEMENT_TTL, TYPE, "0.1", "pipe",
                Map.of("pieceSizes", Manifest.MIN_PIECE_SIZE + "-" + Manifest.MAX_PIECE_SIZE,
                        "sealing", keyRing == null ? Manifest.SEALING_NONE : Manifest.SEALING_GROUP_KEY,
                        "seedMode", policy.seedMode().name().toLowerCase(java.util.Locale.ROOT)),
                Map.of());
    }

    @Override
    public boolean requiresTick() {
        return true;
    }

    /** Publishes holdings for everything the store already holds (LB-T-09). */
    @Override
    public void start() {
        verifySamples();
        for (String root : store.roots()) {
            if (store.have(root).cardinality() > 0) {
                refreshHolding(root, true);
            }
        }
    }

    @Override
    public void tick() {
        if (closed) {
            return;
        }
        long now = clock.millis();
        for (Pending pending : List.copyOf(fetches.values())) {
            drive(pending, now);
        }
        // LB-H-05: a peer holding many roots visits at most HOLDINGS_PER_TICK of them per tick, in
        // turn, so its holding writes stay bounded however large its catalogue; each root still comes
        // round well inside the holding's half-life at any sane tick period.
        List<String> roots = List.copyOf(holders.keySet());
        if (roots.size() <= HOLDINGS_PER_TICK) {
            roots.forEach(root -> refreshHolding(root, false));
        } else {
            int start = Math.floorMod(holdingCursor, roots.size());
            for (int i = 0; i < HOLDINGS_PER_TICK; i++) {
                refreshHolding(roots.get((start + i) % roots.size()), false);
            }
            holdingCursor = start + HOLDINGS_PER_TICK;
        }
        if (now - manifestsCheckedAtMillis >= MANIFEST_KEEPALIVE_INTERVAL_MILLIS) {
            manifestsCheckedAtMillis = now;
            keepManifestsAlive();
        }
    }

    @Override
    public void stop() {
        close();
    }

    @Override
    public void close() {
        closed = true;
        handlers.shutdownNow();
        for (Pending pending : List.copyOf(fetches.values())) {
            pending.session.cancel();
            pending.holdings.close();
        }
        fetches.clear();
        try {
            store.flush(); // the file store persists what it batched (LB-T-01)
        } catch (RuntimeException e) {
            LOG.warning(() -> "store flush on close failed: " + e.getMessage());
        }
    }

    // ------------------------------------------------------------ API

    /** The store. */
    public PieceStore store() {
        return store;
    }

    /** The content space view. */
    public ContentSpaces spaces() {
        return spaces;
    }

    /** The counters. */
    public Stats stats() {
        long received = 0;
        for (Pending p : fetches.values()) {
            received += p.session.bytesReceived();
        }
        return new Stats(fetches.size(), store.roots().size(), bytesServed.get(), received,
                wantsRefused.get(), piecesStruck.get());
    }

    /**
     * Publishes a file: hashes it in pieces, stores it (in place or copied), writes
     * the manifest and this peer's complete holding, and returns the manifest.
     *
     * @param file    the artifact
     * @param options the options
     * @return the manifest
     * @throws IOException on a read failure
     */
    /**
     * Publishes an artifact read from a stream (LB-M-01): spooled to a temporary file, hashed
     * and stored from it, then the file removed, so memory holds one piece at a time. In-place
     * publication needs a file and is refused here.
     *
     * @param in      the artifact; read to its end, not closed
     * @param options the options
     * @return the manifest
     * @throws IOException on a read or spool failure
     */
    public Manifest publish(java.io.InputStream in, PublishOptions options) throws IOException {
        Objects.requireNonNull(in, "in");
        if (options.inPlace()) {
            throw new IllegalArgumentException("an in-place publish needs a file the store can map; pass a Path");
        }
        Path spool = java.nio.file.Files.createTempFile("agentspaces-publish-", ".bin");
        try {
            java.nio.file.Files.copy(in, spool, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            return publish(spool, options);
        } finally {
            java.nio.file.Files.deleteIfExists(spool);
        }
    }

    public Manifest publish(Path file, PublishOptions options) throws IOException {
        Objects.requireNonNull(file, "file");
        if (options.sealedHashing()) {
            if (options.inPlace()) {
                throw new IllegalArgumentException("a sealed-hashing publish stores sealed pieces; it cannot serve a file in place");
            }
            try (java.io.InputStream in = java.nio.file.Files.newInputStream(file)) {
                return publishSealed(in, options);
            }
        }
        ContentRoots.Hashed hashed;
        try (FileChannel in = FileChannel.open(file, StandardOpenOption.READ)) {
            hashed = ContentRoots.hash(in, options.pieceSize());
        }
        Manifest manifest = manifestOf(hashed, options, true);
        if (options.inPlace()) {
            InPlaceStore inPlace = inPlaceBackend();
            if (inPlace == null) {
                throw new IllegalStateException("no in-place backend in the store; add InPlaceStore to the composition");
            }
            inPlace.register(manifest, file);
        } else {
            store.open(manifest);
            try (FileChannel in = FileChannel.open(file, StandardOpenOption.READ)) {
                copyPieces(in, manifest);
            }
        }
        return published(manifest, options);
    }

    /**
     * Publishes an artifact that lies in an external backend, such as an object a remote service
     * serves by range (CC-L-02). The caller hashed the bytes at {@code options.pieceSize()}; this
     * builds and signs the manifest, registers it with the backend under {@code key}, and writes
     * the manifest and a complete holding, as {@link #publish(Path, PublishOptions)} does for a file
     * served in place. The swarm never copies the bytes; each piece is hashed whole at its first
     * serve.
     *
     * @param backend an external backend this swarm's store composes
     * @param key     where the artifact lies, in the backend's terms
     * @param hashed  the artifact's piece table, size, and root at the options' piece size
     * @param options the publish options; sealed hashing is refused
     * @return the manifest
     */
    public Manifest publishExternal(ExternalStore backend, String key, ContentRoots.Hashed hashed, PublishOptions options) {
        Objects.requireNonNull(backend, "backend");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(hashed, "hashed");
        Objects.requireNonNull(options, "options");
        if (options.sealedHashing()) {
            throw new IllegalArgumentException("a sealed-hashing publish stores sealed pieces; it cannot serve an external artifact");
        }
        if (!composes(backend)) {
            throw new IllegalStateException("the " + backend.kind() + " backend is not part of this swarm's store;"
                    + " add it to the composition before the swarm starts");
        }
        if (hashed.table().length != hashed.pieces(options.pieceSize()) * Manifest.HASH_BYTES) {
            throw new IllegalArgumentException("the table was hashed at another piece size than " + options.pieceSize());
        }
        Manifest manifest = manifestOf(hashed, options, true);
        backend.register(manifest, key);
        return published(manifest, options);
    }

    /**
     * Hashes a stream at the options' piece size and publishes it as an external artifact; see
     * {@link #publishExternal(ExternalStore, String, ContentRoots.Hashed, PublishOptions)}.
     *
     * @param backend an external backend this swarm's store composes
     * @param key     where the artifact lies, in the backend's terms
     * @param in      the artifact's bytes, read once and not kept
     * @param options the publish options
     * @return the manifest
     * @throws IOException when the stream fails
     */
    public Manifest publishExternal(ExternalStore backend, String key, java.io.InputStream in, PublishOptions options)
            throws IOException {
        Objects.requireNonNull(in, "in");
        return publishExternal(backend, key, ContentRoots.hash(in, options.pieceSize()), options);
    }

    /** The publish options of this host: its configured piece size, the other defaults. */
    public PublishOptions publishOptions() {
        return PublishOptions.defaults().withPieceSize(pieceSize);
    }

    /** Publishes bytes under {@link #publishOptions()}. */
    public Manifest publish(byte[] bytes) {
        return publish(bytes, publishOptions());
    }

    /** Publishes a file under {@link #publishOptions()}. */
    public Manifest publish(Path file) throws IOException {
        return publish(file, publishOptions());
    }

    /**
     * Publishes bytes held in memory.
     *
     * @param bytes   the artifact
     * @param options the options
     * @return the manifest
     */
    public Manifest publish(byte[] bytes, PublishOptions options) {
        Objects.requireNonNull(bytes, "bytes");
        if (options.sealedHashing()) {
            try {
                return publishSealed(new java.io.ByteArrayInputStream(bytes), options);
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        }
        ContentRoots.Hashed hashed = ContentRoots.hash(bytes, options.pieceSize());
        Manifest manifest = manifestOf(hashed, options, true);
        store.open(manifest);
        for (int i = 0; i < manifest.pieces(); i++) {
            int start = i * manifest.pieceSize();
            store.write(manifest.root(), i, java.util.Arrays.copyOfRange(bytes, start, start + manifest.pieceLength(i)));
        }
        return published(manifest, options);
    }

    /**
     * Publishes bytes a space already sealed (or chose not to seal): the manifest
     * says {@code sealing: none} whatever this capability's key ring, so the
     * pieces travel as the space wrote them and a reader's space opens the whole
     * payload as it would an inline one (group E).
     *
     * @param bytes   the payload as the space produced it
     * @param options the options
     * @return the manifest
     */
    public Manifest publishOpaque(byte[] bytes, PublishOptions options) {
        Objects.requireNonNull(bytes, "bytes");
        ContentRoots.Hashed hashed = ContentRoots.hash(bytes, options.pieceSize());
        Manifest manifest = manifestOf(hashed, options, false);
        store.open(manifest);
        for (int i = 0; i < manifest.pieces(); i++) {
            int start = i * manifest.pieceSize();
            store.write(manifest.root(), i, java.util.Arrays.copyOfRange(bytes, start, start + manifest.pieceLength(i)));
        }
        return published(manifest, options);
    }

    /**
     * Runs a callback when the fetch of a root ends, complete or not; at once
     * when no fetch is active.
     *
     * @param root     the root
     * @param callback what to run
     */
    public void onFetchDone(String root, Runnable callback) {
        Pending pending = fetches.get(root);
        if (pending == null) {
            callback.run();
            return;
        }
        pending.onDone.add(callback);
    }

    /**
     * Fetches an artifact from every reachable holder.
     *
     * @param root    the root
     * @param options the options
     * @return the fetch handle; complete at once when the store already holds the artifact
     */
    public Fetch fetch(String root, FetchOptions options) {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(options, "options");
        if (!ContentRoots.isRoot(root)) {
            throw new IllegalArgumentException("not a content root: " + root);
        }
        Pending existing = fetches.get(root);
        if (existing != null) {
            return existing.session;
        }
        FetchSession session = new FetchSession(root, fabric, store, options, policy.maxSessions(), policy.maxWindow());
        Subscription subscription = spaces.watchHoldings(root, ev -> session.holdingsChanged(), Lease.of(Duration.ofHours(24)));
        Pending pending = new Pending(session, subscription);
        synchronized (queued) {
            if (fetches.size() >= policy.maxFetches()) {
                queued.addLast(pending);
                return session;
            }
        }
        fetches.put(root, pending);
        drive(pending, clock.millis());
        return session;
    }

    /** As {@link #fetch(String, FetchOptions)} with the default options. */
    public Fetch fetch(String root) {
        return fetch(root, FetchOptions.defaults());
    }

    /**
     * Shares an artifact this peer holds in full into this capability's group
     * (LB-G-01): the bridge pattern. A member of two groups holds one store; the
     * first group's capability fetched or published the artifact, and this
     * group's capability writes a manifest with the same root (sealed under this
     * group's key when it has one, since pieces are sealed per range at send)
     * and a complete holding, so members here fetch from this peer without a
     * second copy of the bytes.
     *
     * @param root  the root, complete in the store
     * @param lease the manifest lease in this group
     * @return the manifest written here
     * @throws IllegalStateException when the store does not hold the artifact in full
     */
    public Manifest share(String root, Lease lease) {
        Manifest held = store.manifest(root).orElseThrow(() ->
                new IllegalStateException("root " + root + " is not in this peer's store"));
        if (Manifest.HASHING_SEALED.equals(held.hashing())) {
            throw new IllegalStateException("root " + root + " is hashed sealed and belongs to its group;"
                    + " share(root, lease, source) re-seals it under this group's key as a new root (LB-G-03)");
        }
        if (store.have(root).cardinality() < held.pieces()) {
            throw new IllegalStateException("root " + root + " is incomplete here: "
                    + store.have(root).cardinality() + " of " + held.pieces() + " pieces");
        }
        if (held.tableRef() != null && blocks != null && blocks.local(held.tableRef()).isEmpty()) {
            throw new IllegalStateException("root " + root + ": the piece table block is not held here");
        }
        boolean sealed = keyRing != null;
        Manifest here = new Manifest(Manifest.VERSION, held.root(), held.size(), held.pieceSize(), held.pieces(),
                held.table(), held.tableRef(), held.tableRefs(), held.name(), held.mediaType(),
                sealed ? Manifest.SEALING_GROUP_KEY : Manifest.SEALING_NONE,
                sealed ? keyRing.writeEpoch(clock.instant()) : null,
                held.hashing(), self, clock.millis(), held.attrs());
        store.open(here); // the store keeps the first manifest it saw; the pieces are shared
        return published(here, PublishOptions.defaults().withLease(lease));
    }

    /**
     * Shares a root another group's swarm holds into this group (LB-G-01, LB-G-03). A plain-hashing
     * root keeps its root ({@link #share(String, Lease)}); a sealed-hashing root is opened with the
     * source group's key and re-sealed under this group's, so it travels here as a new root with
     * the same name, media type, attrs, and piece size.
     *
     * @param root   the root in the source group
     * @param lease  the manifest lease here
     * @param source the source group's swarm, which holds the root and its key
     * @return the manifest here; its root differs from {@code root} for a sealed-hashing root
     * @throws IOException when the source cannot open the root
     */
    public Manifest share(String root, Lease lease, Content source) throws IOException {
        Manifest held = source.store().manifest(root).orElseThrow(() ->
                new IllegalStateException("root " + root + " is not in the source's store"));
        if (!Manifest.HASHING_SEALED.equals(held.hashing())) {
            return share(root, lease);
        }
        PublishOptions options = PublishOptions.defaults().withLease(lease).withPieceSize(held.pieceSize())
                .withName(held.name()).withMediaType(held.mediaType()).withAttrs(held.attrs()).withSealedHashing(true);
        try (SeekableByteChannel plain = source.openPlain(root)) {
            return publishSealed(java.nio.channels.Channels.newInputStream(plain), options);
        }
    }

    /**
     * Opens a complete artifact for reading as its plaintext (LB-G-03): a plain-hashing root as
     * stored, a sealed-hashing root decrypted piece by piece with this group's key into a temporary
     * file that closing the channel removes.
     *
     * @param root the root
     * @return a read-only channel over the plaintext
     * @throws IOException when the root is not complete here, or this peer holds no key for it
     */
    public SeekableByteChannel openPlain(String root) throws IOException {
        Optional<Manifest> manifest = store.manifest(root).or(() -> spaces.manifest(root));
        if (manifest.isEmpty() || !Manifest.HASHING_SEALED.equals(manifest.get().hashing())) {
            return store.openArtifact(root);
        }
        Manifest m = manifest.get();
        long epoch = m.keyEpoch() == null ? 0 : m.keyEpoch();
        List<GroupKey> keys = keyRing == null ? List.of() : keyRing.openingKeys(epoch);
        if (keys.isEmpty()) {
            throw new IOException("root " + root + " is sealed for this group and this peer holds no content key for epoch " + epoch);
        }
        Path plain = java.nio.file.Files.createTempFile("agentspaces-plain-", ".bin");
        try (FileChannel out = FileChannel.open(plain, StandardOpenOption.WRITE)) {
            for (int i = 0; i < m.pieces(); i++) {
                byte[] sealed = store.read(root, i, 0, m.pieceLength(i)).orElseThrow(() ->
                        new IOException("root " + root + " is incomplete here"));
                byte[] opened = null;
                for (GroupKey key : keys) {
                    opened = key.decrypt(sealed, sealedAad(epoch, i)).orElse(null);
                    if (opened != null) {
                        break;
                    }
                }
                if (opened == null) {
                    throw new IOException("piece " + i + " of " + root + " does not open under this group's key");
                }
                ByteBuffer buffer = ByteBuffer.wrap(opened);
                while (buffer.hasRemaining()) {
                    out.write(buffer);
                }
            }
        } catch (IOException | RuntimeException e) {
            java.nio.file.Files.deleteIfExists(plain);
            throw e;
        }
        return FileChannel.open(plain, StandardOpenOption.READ, StandardOpenOption.DELETE_ON_CLOSE);
    }

    /**
     * The sealed-hashing publish (LB-G-03): the plaintext is cut into chunks one seal shorter than
     * the piece size, each sealed once under this epoch's key with an AAD of the group, the epoch,
     * and the index, and the sealed pieces are spooled, hashed, stored, and published; the table
     * and root are over the sealed bytes, so every member can verify them without the key.
     */
    private Manifest publishSealed(java.io.InputStream in, PublishOptions options) throws IOException {
        if (keyRing == null) {
            throw new IllegalStateException("a sealed-hashing publish needs the group's content key");
        }
        long epoch = keyRing.writeEpoch(clock.instant());
        GroupKey key = keyRing.sealingKey(epoch).orElseThrow(() ->
                new IllegalStateException("content key epoch " + epoch + " is not held"));
        int overhead = key.encrypt(new byte[0], new byte[0]).length;
        int chunk = options.pieceSize() - overhead;
        Path spool = java.nio.file.Files.createTempFile("agentspaces-sealed-", ".bin");
        try {
            try (FileChannel out = FileChannel.open(spool, StandardOpenOption.WRITE)) {
                for (int index = 0; ; index++) {
                    byte[] plain = in.readNBytes(chunk);
                    if (plain.length == 0) {
                        break;
                    }
                    ByteBuffer buffer = ByteBuffer.wrap(key.encrypt(plain, sealedAad(epoch, index)));
                    while (buffer.hasRemaining()) {
                        out.write(buffer);
                    }
                    if (plain.length < chunk) {
                        break;
                    }
                }
            }
            ContentRoots.Hashed hashed;
            try (FileChannel spooled = FileChannel.open(spool, StandardOpenOption.READ)) {
                hashed = ContentRoots.hash(spooled, options.pieceSize());
            }
            Manifest manifest = manifestOf(hashed, options, true, epoch);
            store.open(manifest);
            try (FileChannel spooled = FileChannel.open(spool, StandardOpenOption.READ)) {
                copyPieces(spooled, manifest);
            }
            return published(manifest, options);
        } finally {
            java.nio.file.Files.deleteIfExists(spool);
        }
    }

    /** The AAD of a sealed-hashing piece: the group, the epoch, and the index, so a piece opens only where it belongs. */
    private byte[] sealedAad(long epoch, int index) {
        return ("aspace:content:sealed|" + runtime.id().value() + "|" + epoch + "|" + index)
                .getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /** Pieces between HAVE pushes to a fetch's sources (LB-H-08). */
    static final int HAVE_EVERY_PIECES = 8;

    /** One in this many pieces is re-hashed when a writable backend starts (LB-T-10). */
    static final int SAMPLE_EVERY = 64;

    /** Registers a collection advertised over this swarm (LB-K-07); its provider does it. */
    void registerCollection(CollectionAssetProvider provider) {
        collections.add(provider);
    }

    /** The collections advertised over this swarm (LB-K-07). */
    public List<CollectionAssetProvider> collections() {
        return List.copyOf(collections);
    }

    /**
     * Whether this group's swarm serves a root (LB-K-06): published, shared, or fetched through it,
     * or announced in its replica. A store shared with another group holds roots this group never
     * saw; a collection here lists only these.
     */
    public boolean servesHere(String root) {
        return servedHere.contains(root) || spaces.manifest(root).isPresent();
    }

    /**
     * Every root {@link #servesHere} would accept among {@code candidates}, reading the replica only
     * when some candidate was not published, shared, or fetched here (a collection query's filter).
     */
    public java.util.Set<String> rootsServedHere(java.util.Collection<String> candidates) {
        java.util.Set<String> roots = new java.util.HashSet<>();
        boolean unknown = false;
        for (String root : candidates) {
            if (servedHere.contains(root)) {
                roots.add(root);
            } else {
                unknown = true;
            }
        }
        if (unknown) {
            java.util.Set<String> wanted = new java.util.HashSet<>(candidates);
            for (Manifest m : spaces.manifests(Integer.MAX_VALUE)) {
                if (wanted.contains(m.root())) {
                    roots.add(m.root());
                }
            }
        }
        return roots;
    }

    /**
     * Mirrors a source swarm's catalogue into several groups (LB-G-06): every complete root of
     * {@code source} whose manifest matches {@code which} is shared into each target, sealed-hashing
     * roots re-sealed per target. A root a target cannot take is skipped and reported.
     *
     * @return per target, the manifests written there
     */
    public static Map<GroupId, List<Manifest>> shareAll(Content source, java.util.Collection<Content> targets,
                                                       java.util.function.Predicate<Manifest> which, Lease lease) {
        Map<GroupId, List<Manifest>> written = new java.util.LinkedHashMap<>();
        for (Content target : targets) {
            List<Manifest> here = new ArrayList<>();
            for (String root : source.store().roots()) {
                Optional<Manifest> m = source.store().manifest(root);
                if (m.isEmpty() || !which.test(m.get()) || source.store().have(root).cardinality() < m.get().pieces()) {
                    continue;
                }
                try {
                    here.add(target.share(root, lease, source));
                } catch (IOException | RuntimeException e) {
                    LOG.warning("shareAll: " + root + " into " + target.group().value() + ": " + e.getMessage());
                }
            }
            written.put(target.group(), here);
        }
        return written;
    }

    private void sendHave(FetchSession session, boolean complete) {
        Manifest m = store.manifest(session.root()).orElse(null);
        if (m == null) {
            return;
        }
        BitSet verified = session.verifiedPieces();
        byte[] have = complete ? null : Holding.encodeHave(verified, m.pieces());
        byte[] message = codec.toBytes(new Messages.Have(session.root(), have, complete));
        for (PeerId source : session.sourceIds()) {
            pipes.send(source, TYPE, message);
        }
    }

    private static BitSet fullSet(int pieces) {
        BitSet all = new BitSet(pieces);
        all.set(0, pieces);
        return all;
    }

    private static BitSet decodeHave(byte[] have, int pieces) {
        // the run-length form of a HAVE is the holding's; a carrier record decodes it the same way
        return new Holding(Holding.VERSION, "b", PeerId.of("z"), false, have, 0, null, null).decodeHave(pieces);
    }

    private synchronized boolean withinDownloadBudget(int bytes) {
        long budget = policy.downloadBytesPerDay();
        if (budget <= 0) {
            return true;
        }
        long day = clock.millis() / 86_400_000L;
        if (day != downloadDay) {
            downloadDay = day;
            downloadedToday = 0;
        }
        if (downloadedToday + bytes > budget) {
            return false;
        }
        downloadedToday += bytes;
        return true;
    }

    /** The piece table of a manifest held here: inline, or its block when held; empty when not here. */
    private Optional<byte[]> tableHere(Manifest m) {
        if (m.table() != null) {
            return Optional.of(m.table());
        }
        return m.tableRef() == null || blocks == null ? Optional.empty() : blocks.local(m.tableRef());
    }

    private static boolean pieceMatches(byte[] table, int index, byte[] piece) {
        byte[] digest = ai.badmonkey.agentspaces.common.crypto.Digests.sha256(piece);
        int at = index * 32;
        return at + 32 <= table.length
                && java.util.Arrays.equals(digest, 0, 32, table, at, at + 32);
    }

    /**
     * LB-T-10: on start, re-hash one in {@link #SAMPLE_EVERY} pieces of every root a writable backend
     * holds (at least one per root), and forget a root whose sample fails, so a damaged disk stops
     * serving rather than serving damage; roots served in place are checked at their first serve.
     */
    private void verifySamples() {
        for (String root : store.roots()) {
            if (externalBackendFor(root) != null) {
                continue;
            }
            Manifest m = store.manifest(root).orElse(null);
            if (m == null || m.pieces() == 0) {
                continue;
            }
            byte[] table = tableHere(m).orElse(null);
            if (table == null) {
                continue;
            }
            BitSet have = store.have(root);
            int first = Math.floorMod(root.hashCode(), Math.min(SAMPLE_EVERY, m.pieces()));
            boolean ok = true;
            for (int i = first; i < m.pieces() && ok; i += SAMPLE_EVERY) {
                if (!have.get(i)) {
                    continue;
                }
                byte[] piece = store.read(root, i, 0, m.pieceLength(i)).orElse(null);
                ok = piece != null && pieceMatches(table, i, piece);
            }
            if (!ok) {
                LOG.warning("content store: root " + root
                        + " failed its sampled verification on start and is forgotten (LB-T-10)");
                rootsDroppedOnSample.incrementAndGet();
                store.forget(root);
            }
        }
    }

    /**
     * LB-T-10: a piece of an external backend (files in place, objects behind a remote service) is
     * hashed whole at its first serve; a mismatch forgets the root in every backend, so a piece a
     * read-through cache took from the external backend is dropped with it.
     */
    private boolean verifiedExternally(Manifest m, int index) {
        PieceStore external = externalBackendFor(m.root());
        String piece = m.root() + "#" + index;
        if (external == null || externalVerified.contains(piece)) {
            return true;
        }
        // The sub-range wants of one piece arrive together; the first reads and hashes it, the
        // others wait and find it verified (a remote backend then serves each piece once).
        Object lock = verifying.computeIfAbsent(piece, k -> new Object());
        try {
            synchronized (lock) {
                if (externalVerified.contains(piece)) {
                    return true;
                }
                return verifyExternalPiece(m, index, external);
            }
        } finally {
            verifying.remove(piece, lock);
        }
    }

    private boolean verifyExternalPiece(Manifest m, int index, PieceStore external) {
        byte[] table = tableHere(m).orElse(null);
        if (table == null || index < 0 || index >= m.pieces()) {
            return true; // nothing to check against; the fetcher verifies regardless
        }
        byte[] piece;
        try {
            piece = store.read(m.root(), index, 0, m.pieceLength(index)).orElse(null);
        } catch (RuntimeException e) {
            LOG.warning("content store: " + external.kind() + " root " + m.root() + " piece " + index
                    + " could not be read: " + e.getMessage());
            return false;
        }
        if (piece != null && pieceMatches(table, index, piece)) {
            externalVerified.add(m.root() + "#" + index);
            return true;
        }
        synchronized (externalVerified) {
            if (external.roots().contains(m.root())) { // two wants in flight count one drop
                LOG.warning("content store: " + external.kind() + " root " + m.root() + " piece " + index
                        + " no longer matches its manifest; the root is forgotten (LB-T-10)");
                rootsDroppedOnSample.incrementAndGet();
                externalVerified.removeIf(k -> k.startsWith(m.root() + "#"));
                store.forget(m.root());
            }
        }
        return false;
    }

    /** The external backend that holds a root, or null when none does. */
    private PieceStore externalBackendFor(String root) {
        if (store instanceof CompositeStore composite) {
            for (PieceStore b : composite.backends()) {
                if (b.external() && b.roots().contains(root)) {
                    return b;
                }
            }
            return null;
        }
        return store.external() && store.roots().contains(root) ? store : null;
    }

    /** Whether a backend is this swarm's store or one of its composed backends. */
    private boolean composes(PieceStore backend) {
        if (store == backend) {
            return true;
        }
        return store instanceof CompositeStore composite && composite.backends().contains(backend);
    }

    /** Roots forgotten because a sample or a first serve did not match (LB-T-10). */
    public long rootsDroppedOnSample() {
        return rootsDroppedOnSample.get();
    }

    /** The fetches in progress, for the console (LB-O-03). */
    public List<Fetch> activeFetches() {
        List<Fetch> active = new ArrayList<>();
        for (Pending p : fetches.values()) {
            active.add(p.session);
        }
        return active;
    }

    /** The group this capability serves. */
    public GroupId group() {
        return runtime.id();
    }

    /** Describes a root without starting a fetch (LB-M-07). */
    public Optional<Description> describe(String root) {
        Optional<Manifest> replicated = spaces.manifest(root);
        Optional<Manifest> manifest = replicated.or(() -> store.manifest(root));
        if (manifest.isEmpty()) {
            return Optional.empty();
        }
        int complete = 0;
        int partial = 0;
        for (Holding h : spaces.holdings(root)) {
            if (h.complete()) {
                complete++;
            } else {
                partial++;
            }
        }
        boolean advertised = collections.stream().anyMatch(c -> c.advertises(manifest.get()));
        return Optional.of(new Description(manifest.get(), complete, partial, store.have(root), replicated.isPresent(),
                advertised));
    }

    /** Every manifest the replica shows (LB-K-01). */
    public List<Manifest> list(int limit) {
        return spaces.manifests(limit);
    }

    /**
     * The manifests matching a template (LB-K-01): field conditions on the manifest and tag
     * conditions on its tags, which carry the root, the name, and the user's attrs (LB-M-09).
     */
    public List<Manifest> list(ai.badmonkey.agentspaces.api.space.Template<Manifest> template, int limit) {
        return spaces.manifests(template, limit);
    }

    /**
     * Finds artifacts the replica knows (LB-K-01) and describes each with its holder counts. Every
     * argument is optional (null): {@code name} matches a substring of the manifest's name, case
     * aside; {@code mediaType} matches exactly; every entry of {@code attrs} must equal the
     * manifest's attr; {@code origin} is the publisher; {@code maxAge} bounds how long ago it was
     * published. Newest first.
     */
    public List<Description> search(String name, String mediaType, Map<String, String> attrs, PeerId origin,
                                    java.time.Duration maxAge, int limit) {
        long now = clock.millis();
        String needle = name == null ? null : name.toLowerCase(java.util.Locale.ROOT);
        List<Description> found = new ArrayList<>();
        List<Manifest> candidates = new ArrayList<>(spaces.manifests(Integer.MAX_VALUE));
        candidates.sort(java.util.Comparator.comparingLong(Manifest::created).reversed());
        for (Manifest m : candidates) {
            if (needle != null && (m.name() == null || !m.name().toLowerCase(java.util.Locale.ROOT).contains(needle))) {
                continue;
            }
            if (mediaType != null && !mediaType.equals(m.mediaType())) {
                continue;
            }
            if (attrs != null && !attrs.isEmpty() && (m.attrs() == null
                    || !attrs.entrySet().stream().allMatch(e -> e.getValue().equals(m.attrs().get(e.getKey()))))) {
                continue;
            }
            if (origin != null && !origin.equals(m.origin())) {
                continue;
            }
            if (maxAge != null && now - m.created() > maxAge.toMillis()) {
                continue;
            }
            describe(m.root()).ifPresent(found::add);
            if (found.size() >= limit) {
                break;
            }
        }
        return found;
    }

    /** Pins a root against eviction. */
    public void pin(String root) {
        store.pin(root);
    }

    /** Unpins a root. */
    public void unpin(String root) {
        store.unpin(root);
    }

    // ------------------------------------------------------------ publishing

    private Manifest manifestOf(ContentRoots.Hashed hashed, PublishOptions options, boolean sealIfKeyed) {
        return manifestOf(hashed, options, sealIfKeyed, null);
    }

    /** With {@code sealedEpoch} the manifest is a sealed-hashing one under that epoch (LB-G-03). */
    private Manifest manifestOf(ContentRoots.Hashed hashed, PublishOptions options, boolean sealIfKeyed, Long sealedEpoch) {
        int pieces = hashed.pieces(options.pieceSize());
        byte[] table = pieces <= Manifest.INLINE_TABLE_PIECES ? hashed.table() : null;
        String tableRef = null;
        if (table == null) {
            if (blocks == null) {
                throw new IllegalStateException("an artifact of " + pieces + " pieces needs the block exchange for its table");
            }
            tableRef = blocks.put(hashed.table());
            blocks.retain(tableRef);
        }
        boolean sealed = sealIfKeyed && keyRing != null;
        String sealing = sealed ? Manifest.SEALING_GROUP_KEY : Manifest.SEALING_NONE;
        Long keyEpoch = sealedEpoch != null ? sealedEpoch : sealed ? keyRing.writeEpoch(clock.instant()) : null;
        return new Manifest(Manifest.VERSION, hashed.root(), hashed.size(), options.pieceSize(), pieces,
                table, tableRef, null, options.name(), options.mediaType(), sealing, keyEpoch,
                sealedEpoch != null ? Manifest.HASHING_SEALED : Manifest.HASHING_PLAIN, self, clock.millis(), options.attrs());
    }

    private void copyPieces(FileChannel in, Manifest manifest) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(manifest.pieceSize());
        for (int i = 0; i < manifest.pieces(); i++) {
            buffer.clear();
            buffer.limit(manifest.pieceLength(i));
            long at = (long) i * manifest.pieceSize();
            while (buffer.hasRemaining()) {
                int n = in.read(buffer, at + buffer.position());
                if (n < 0) {
                    throw new IOException("artifact shorter than hashed");
                }
            }
            store.write(manifest.root(), i, java.util.Arrays.copyOf(buffer.array(), buffer.position()));
        }
    }

    private Manifest published(Manifest manifest, PublishOptions options) {
        servedHere.add(manifest.root());
        store.pin(manifest.root());
        if (options.replicate() && spaces.manifest(manifest.root()).isEmpty()) {
            manifestHandles.put(manifest.root(), spaces.writeManifest(manifest, options.lease()));
        }
        refreshHolding(manifest.root(), true);
        return manifest;
    }

    private InPlaceStore inPlaceBackend() {
        if (store instanceof InPlaceStore in) {
            return in;
        }
        if (store instanceof CompositeStore composite) {
            for (PieceStore b : composite.backends()) {
                if (b instanceof InPlaceStore in) {
                    return in;
                }
            }
        }
        return null;
    }

    // ------------------------------------------------------------ holdings

    /**
     * Writes or renews this peer's holding for a root: a new entry when the
     * bitfield changed and the write interval or the piece step passed, or the
     * root completed; a renewal at half-life otherwise.
     */
    private void refreshHolding(String root, boolean force) {
        if (policy.seedMode() == ContentPolicy.SeedMode.NEVER) {
            return;
        }
        Optional<Manifest> manifest = store.manifest(root);
        if (manifest.isEmpty()) {
            return;
        }
        BitSet have = store.have(root);
        int count = have.cardinality();
        if (count == 0) {
            return;
        }
        boolean complete = count >= manifest.get().pieces();
        if (policy.seedMode() == ContentPolicy.SeedMode.COMPLETE_ONLY && !complete) {
            return;
        }
        Holder holder = holders.computeIfAbsent(root, r -> new Holder());
        long now = clock.millis();
        boolean changed = holder.piecesAtWrite != count;
        boolean due = force || holder.handle == null
                || (changed && (complete || now - holder.writtenAtMillis >= HOLDING_WRITE_INTERVAL_MILLIS
                || count - holder.piecesAtWrite >= PIECES_PER_HOLDING_WRITE));
        if (due) {
            List<PeerId> via = runtime.viaRelays();
            Holding holding = Holding.of(root, self, have, manifest.get().pieces(), via.isEmpty() ? null : via);
            EntryHandle previous = holder.handle;
            try {
                holder.handle = spaces.writeHolding(holding, holdingLease);
            } catch (RuntimeException e) {
                LOG.fine(() -> "holding write deferred for " + root + ": " + e.getMessage());
                return;
            }
            holder.last = holding;
            holder.writtenAtMillis = now;
            holder.renewedAtMillis = now;
            holder.piecesAtWrite = count;
            if (previous != null) {
                try {
                    previous.cancel();
                } catch (RuntimeException ignored) {
                    // the previous holding lapses by lease in any case
                }
            }
        } else if (now - holder.renewedAtMillis >= holdingLease.duration().toMillis() / 2) {
            try {
                holder.handle.renew(holdingLease.duration());
                holder.renewedAtMillis = now;
            } catch (RuntimeException e) {
                holder.handle = null; // lapsed or withdrawn: rewrite next tick
            }
        }
    }

    /** A complete holder rewrites a manifest the replica has lost (the publisher may be gone). */
    private void keepManifestsAlive() {
        for (String root : store.roots()) {
            Optional<Manifest> manifest = store.manifest(root);
            if (manifest.isEmpty() || store.have(root).cardinality() < manifest.get().pieces()) {
                continue;
            }
            EntryHandle handle = manifestHandles.get(root);
            if (handle != null) {
                try {
                    handle.renew(Duration.ofDays(7));
                    continue;
                } catch (RuntimeException e) {
                    manifestHandles.remove(root);
                }
            }
            if (spaces.manifest(root).isEmpty()) {
                try {
                    manifestHandles.put(root, spaces.writeManifest(manifest.get(), Lease.of(Duration.ofDays(7))));
                } catch (RuntimeException e) {
                    LOG.fine(() -> "manifest keepalive deferred for " + root + ": " + e.getMessage());
                }
            }
        }
    }

    // ------------------------------------------------------------ fetching

    private void drive(Pending pending, long now) {
        FetchSession session = pending.session;
        if (session.isDone()) {
            finish(pending);
            return;
        }
        if (!session.hasManifest()) {
            resolveManifest(pending, now);
            return;
        }
        session.tick();
        if (session.isDone()) {
            finish(pending);
        }
    }

    private void resolveManifest(Pending pending, long now) {
        FetchSession session = pending.session;
        String root = session.root();
        Optional<Manifest> manifest = store.manifest(root).or(() -> spaces.manifest(root));
        if (manifest.isEmpty()) {
            if (now - pending.manifestAskedAtMillis >= MANIFEST_RETRY_MILLIS) {
                pending.manifestAskedAtMillis = now;
                for (Holding h : spaces.holdings(root)) {
                    if (!h.holder().equals(self)) {
                        pipes.send(h.holder(), TYPE, codec.toBytes(new Messages.ManifestWant(root)));
                    }
                }
            }
            return;
        }
        Manifest m = manifest.get();
        if (m.table() != null) {
            if (!ContentRoots.rootOf(m.table()).equals(root)) {
                LOG.warning(() -> "manifest for " + root + " carries a table that does not hash to it; ignored");
                return;
            }
            session.start(m, m.table());
            if (session.isDone()) {
                finish(pending);
            }
            return;
        }
        if (blocks == null) {
            LOG.warning(() -> "manifest for " + root + " references a table block but this peer has no block exchange");
            return;
        }
        if (pending.tableFetching) {
            return;
        }
        pending.tableFetching = true;
        List<PeerId> candidates = new ArrayList<>();
        for (Holding h : spaces.holdings(root)) {
            if (!h.holder().equals(self)) {
                candidates.add(h.holder());
            }
        }
        candidates.add(m.origin());
        Thread.ofVirtual().name("content-table-" + root).start(() -> {
            try {
                Optional<byte[]> table = blocks.fetch(m.tableRef(), candidates, TABLE_FETCH_TIMEOUT_MILLIS);
                if (table.isPresent() && ContentRoots.rootOf(table.get()).equals(root)) {
                    blocks.retain(m.tableRef());
                    session.start(m, table.get());
                    if (session.isDone()) {
                        finish(pending);
                    }
                }
            } finally {
                pending.tableFetching = false;
            }
        });
    }

    private void finish(Pending pending) {
        String root = pending.session.root();
        if (fetches.remove(root, pending)) {
            pending.holdings.close();
            for (Runnable callback : pending.onDone) {
                try {
                    callback.run();
                } catch (RuntimeException e) {
                    LOG.warning(() -> "fetch-done callback for " + root + " failed: " + e.getMessage());
                }
            }
            Pending next;
            synchronized (queued) {
                next = queued.pollFirst();
            }
            if (next != null) {
                fetches.put(next.session.root(), next);
                drive(next, clock.millis());
            }
        }
    }

    /** The associated data a sealed range binds: root, piece index, offset (3.3). */
    static byte[] sealingAad(String root, int index, int offset) {
        return (root + "|" + index + "|" + offset).getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private final FetchSession.Fabric fabric = new FetchSession.Fabric() {
        @Override
        public PeerId self() {
            return self;
        }

        @Override
        public long nowMillis() {
            return clock.millis();
        }

        @Override
        public void send(PeerId to, Object message) {
            pipes.send(to, TYPE, codec.toBytes(message));
        }

        @Override
        public int nextRequestId() {
            return requestIds.incrementAndGet();
        }

        @Override
        public List<Holding> holdings(String root) {
            return spaces.holdings(root);
        }

        @Override
        public void strike(PeerId peer, String reason) {
            piecesStruck.incrementAndGet();
            runtime.reportMisbehavior(peer, reason);
        }

        @Override
        public void onPieceVerified(FetchSession session, int index) {
            if (policy.seedMode() == ContentPolicy.SeedMode.AUTO) {
                refreshHolding(session.root(), false);
                if (session.verifiedPieces().cardinality() % HAVE_EVERY_PIECES == 0) {
                    // off the session's lock: a send can wait on a full socket, and the session's
                    // other frames must not wait behind it
                    try {
                        handlers.execute(() -> sendHave(session, false));
                    } catch (java.util.concurrent.RejectedExecutionException e) {
                        // closing
                    }
                }
            }
        }

        @Override
        public void onComplete(FetchSession session) {
            servedHere.add(session.root());
            store.pin(session.root());
            // the last verified piece already wrote the complete holding under AUTO; only a holding
            // still short of complete (COMPLETE_ONLY, or a write deferred) is written here (LB-Q-PRF-02)
            refreshHolding(session.root(), false);
            sendHave(session, true);
        }

        @Override
        public boolean mayRequest(int bytes) {
            return withinDownloadBudget(bytes);
        }

        @Override
        public Random random() {
            return random;
        }

        @Override
        public SeekableByteChannel openArtifact(PieceStore pieces, String root) throws IOException {
            return openPlain(root);
        }

        @Override
        public Optional<byte[]> open(Manifest manifest, int index, int offset, byte[] bytes) {
            if (!Manifest.SEALING_GROUP_KEY.equals(manifest.sealing()) || Manifest.HASHING_SEALED.equals(manifest.hashing())) {
                return Optional.of(bytes); // a sealed-hashing piece is stored sealed and verified as such (LB-T-05)
            }
            if (keyRing == null) {
                return Optional.empty();
            }
            long epoch = manifest.keyEpoch() == null ? 0 : manifest.keyEpoch();
            byte[] aad = sealingAad(manifest.root(), index, offset);
            for (GroupKey key : keyRing.openingKeys(epoch)) {
                Optional<byte[]> plain = key.decrypt(bytes, aad);
                if (plain.isPresent()) {
                    return plain;
                }
            }
            return Optional.empty();
        }

        @Override
        public int hopsTo(PeerId peer) {
            return runtime.hopsTo(peer);
        }

        @Override
        public boolean canOpen(Manifest manifest) {
            return !Manifest.SEALING_GROUP_KEY.equals(manifest.sealing())
                    || Manifest.HASHING_SEALED.equals(manifest.hashing())
                    || (keyRing != null && !keyRing.openingKeys(manifest.keyEpoch() == null ? 0 : manifest.keyEpoch()).isEmpty());
        }
    };

    // ------------------------------------------------------------ the holder side

    private void onFrame(PeerId from, byte[] payload) {
        if (closed) {
            return;
        }
        Optional<Integer> tag = Messages.tagOf(codec, payload);
        if (tag.isEmpty()) {
            return;
        }
        switch (tag.get()) {
            case Messages.PIECE_WANT -> Messages.decode(codec, payload, Messages.PieceWant.class)
                    .ifPresent(want -> serve(from, want));
            case Messages.PIECE -> Messages.decode(codec, payload, Messages.Piece.class)
                    .ifPresent(piece -> {
                        Pending pending = fetches.get(piece.root());
                        if (pending != null) {
                            pending.session.onPiece(from, piece);
                            if (pending.session.isDone()) {
                                finish(pending);
                            }
                        }
                    });
            case Messages.REJECT -> Messages.decode(codec, payload, Messages.Reject.class)
                    .ifPresent(reject -> {
                        Pending pending = fetches.get(reject.root());
                        if (pending != null) {
                            pending.session.onReject(from, reject);
                        }
                    });
            case Messages.CANCEL -> {
                // a holder answers wants one at a time, so a cancel has nothing to withdraw
            }
            case Messages.HAVE -> Messages.decode(codec, payload, Messages.Have.class).ifPresent(have -> {
                // LB-H-08: advisory; the pieces a source verified since its holding, folded into the fetch
                Pending pending = fetches.get(have.root());
                Manifest m = pending == null ? null : store.manifest(have.root()).orElse(null);
                if (pending != null && m != null) {
                    BitSet bits = have.complete() || have.have() == null ? fullSet(m.pieces())
                            : decodeHave(have.have(), m.pieces());
                    pending.session.onHave(from, bits);
                }
            });
            case Messages.MANIFEST_WANT -> Messages.decode(codec, payload, Messages.ManifestWant.class)
                    .ifPresent(want -> store.manifest(want.root()).ifPresent(m ->
                            pipes.send(from, TYPE, codec.toBytes(new Messages.ManifestReply(want.root(), codec.toBytes(m))))));
            case Messages.MANIFEST -> Messages.decode(codec, payload, Messages.ManifestReply.class)
                    .ifPresent(reply -> onManifest(from, reply));
            default -> {
            }
        }
    }

    private void serve(PeerId from, Messages.PieceWant want) {
        Optional<Manifest> manifest = store.manifest(want.root());
        if (manifest.isEmpty()) {
            reject(from, want, Messages.REASON_UNKNOWN_ROOT);
            return;
        }
        if (policy.seedMode() == ContentPolicy.SeedMode.NEVER
                || !policy.mayServe(runtime.id(), from, manifest.get())
                || (authorizer != null && !authorizer.permits(from, Authorizer.Operation.CONTENT_FETCH, want.root()))) {
            reject(from, want, Messages.REASON_POLICY);
            return;
        }
        boolean sealedHashing = Manifest.HASHING_SEALED.equals(manifest.get().hashing());
        if (sealedHashing && spaces.manifest(want.root()).isEmpty() && !manifestHandles.containsKey(want.root())) {
            // LB-T-05: sealed pieces are served only into the group they were sealed for, the one whose
            // replica carries the manifest; the store may be shared with other groups.
            reject(from, want, Messages.REASON_POLICY);
            return;
        }
        GroupKey sealingKey = null;
        if (Manifest.SEALING_GROUP_KEY.equals(manifest.get().sealing()) && !sealedHashing) {
            long epoch = manifest.get().keyEpoch() == null ? 0 : manifest.get().keyEpoch();
            sealingKey = keyRing == null ? null : keyRing.sealingKey(epoch).orElse(null);
            if (sealingKey == null) {
                reject(from, want, Messages.REASON_POLICY); // a keyless holder serves nothing sealed
                return;
            }
        }
        if (want.length() <= 0 || want.length() > Manifest.MAX_PIECE_SIZE) {
            reject(from, want, Messages.REASON_UNKNOWN_ROOT);
            return;
        }
        if (!withinUploadBudget(want.length())) {
            reject(from, want, Messages.REASON_OVER_BUDGET);
            return;
        }
        if (!verifiedExternally(manifest.get(), want.index())) {
            reject(from, want, Messages.REASON_NOT_HELD);
            return;
        }
        Optional<byte[]> bytes = store.read(want.root(), want.index(), want.offset(), want.length());
        if (bytes.isEmpty()) {
            reject(from, want, Messages.REASON_NOT_HELD);
            return;
        }
        byte[] payload = bytes.get();
        if (sealingKey != null) {
            payload = sealingKey.encrypt(payload, sealingAad(want.root(), want.index(), want.offset()));
        }
        bytesServed.addAndGet(bytes.get().length);
        pipes.send(from, TYPE, codec.toBytes(new Messages.Piece(want.root(), want.index(), want.offset(), payload, want.req())));
    }

    private void reject(PeerId from, Messages.PieceWant want, int reason) {
        wantsRefused.incrementAndGet();
        pipes.send(from, TYPE, codec.toBytes(new Messages.Reject(want.root(), want.index(), reason)));
    }

    private synchronized boolean withinUploadBudget(int bytes) {
        long budget = policy.uploadBytesPerDay();
        if (budget <= 0) {
            return true;
        }
        long day = clock.millis() / 86_400_000L;
        if (day != uploadDay) {
            uploadDay = day;
            uploadedToday = 0;
        }
        if (uploadedToday + bytes > budget) {
            return false;
        }
        uploadedToday += bytes;
        return true;
    }

    private void onManifest(PeerId from, Messages.ManifestReply reply) {
        Pending pending = fetches.get(reply.root());
        if (pending == null || pending.session.hasManifest() || reply.record() == null) {
            return;
        }
        Manifest manifest;
        try {
            manifest = codec.fromBytes(reply.record(), Manifest.class);
        } catch (RuntimeException e) {
            return;
        }
        if (manifest == null || !reply.root().equals(manifest.root()) || manifest.violation().isPresent()) {
            return;
        }
        if (manifest.table() != null && !ContentRoots.rootOf(manifest.table()).equals(manifest.root())) {
            runtime.reportMisbehavior(from, "content manifest for " + reply.root() + " does not hash to its root");
            return;
        }
        store.open(manifest);
        drive(pending, clock.millis());
    }
}
