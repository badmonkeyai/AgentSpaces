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
import ai.badmonkey.agentspaces.content.tree.TreeFetch;
import ai.badmonkey.agentspaces.content.tree.TreeFetchSession;
import ai.badmonkey.agentspaces.content.tree.TreeServer;
import ai.badmonkey.agentspaces.content.tree.TreeStore;
import ai.badmonkey.agentspaces.discovery.DiscoveryService;
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
 *
 * <p>The tree exchange of srad/agentspaces-pdt.md §3.5 rides the same capability:
 * with a {@link Builder#treeStore} this peer serves {@code NODE_WANT} and
 * {@code TREE_WANT} through a {@link TreeServer} under the checks a piece want
 * passes, fetches trees with {@link #fetchTree} and node batches with
 * {@link #fetchNodes}, and publishes holdings for tree roots with
 * {@link #holdTree} (PD-S-06). Tags 8 to 11 go only to members whose current
 * advertisement carries {@code trees = "1"} (PD-X-01), and one peer's tree frames
 * are handled in the order they arrived, so a holder's breadth-first stream
 * reaches the fetcher parents first.
 */
public final class Content implements CapabilityProvider, AutoCloseable {

    /** The capability type. */
    public static final String TYPE = "aspace:cap/content-swarm";

    /** "Never happened" for a millisecond stamp: far enough in the past that {@code now - NEVER} cannot overflow. */
    static final long NEVER = Long.MIN_VALUE / 4;

    /**
     * The advertisement parameter that says this peer speaks the tree exchange
     * (srad/agentspaces-pdt.md PD-X-01): tags 8 to 11 go only to a member whose
     * current content-swarm advertisement carries {@code trees = "1"}.
     */
    public static final String TREES_PARAMETER = "trees";

    /** The value of {@link #TREES_PARAMETER} this build advertises. */
    public static final String TREES_VERSION = "1";

    /**
     * The advertisement parameter that carries the PDT feature level (srad/agentspaces-pdt2.md §6.10.2,
     * P2-F-05): a member sends a level-N form only to a member whose current advertisement carries this
     * parameter as the canonical decimal text of N or above, and reads a member without it as level 1.
     */
    public static final String PDT_PARAMETER = "pdt";

    /** The highest feature level this build reads, the one a content store advertises (P2-S-01). */
    public static final int PDT_LEVEL = 2;

    /** Advertisement TTL, as every shipped capability uses. */
    static final Duration ADVERTISEMENT_TTL = Duration.ofMinutes(15);

    /** The holding refresh rules (LB-H-02, LB-H-03). */
    static final int PIECES_PER_HOLDING_WRITE = 32;
    static final long HOLDING_WRITE_INTERVAL_MILLIS = 10_000;

    /** Roots whose holdings one tick visits at most (LB-H-05). */
    static final int HOLDINGS_PER_TICK = 64;
    /** Frames one peer may have queued for the tree exchange. */
    static final int TREE_QUEUE_FRAMES = 256;
    /** Bytes one peer may have queued for the tree exchange. */
    static final long TREE_QUEUE_BYTES = 32L * 1024 * 1024;
    /** An empty tree queue unused this long is forgotten. */
    static final long TREE_QUEUE_IDLE_MILLIS = 60_000;
    /** How often the group-seals signal is re-read from the advertisements. */
    static final long GROUP_SEALS_REREAD_MILLIS = 5_000;

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

    /**
     * Counters an operator reads.
     *
     * @param fetchesActive        piece fetches in progress
     * @param rootsHeld            roots in the piece store
     * @param bytesServed          piece bytes served
     * @param bytesReceived        piece bytes received by the active fetches
     * @param wantsRefused         piece wants refused
     * @param piecesStruck         pieces (and tree nodes) whose sender was struck
     * @param treeFetchesActive    tree fetches and node batches in progress
     * @param nodesServed          nodes served to {@code NODE_WANT} and {@code TREE_WANT}
     * @param nodesReceived        nodes received and verified by tree fetches
     * @param treeRequestsRejected tree requests refused with a {@code REJECT}
     * @param treeMessagesDropped  tree messages dropped for a violated bound, an unknown request, or a broken wire
     *                             schema, and payloads with an unknown tag or none (PD-X-02, PD-X-04; PDT 2
     *                             phase 1, P2-X-09)
     * @param pieceMessagesDropped piece messages (tags 1 to 7) and manifest records dropped for a broken wire
     *                             schema (PDT 2 phase 1, P2-X-09)
     */
    public record Stats(int fetchesActive, int rootsHeld, long bytesServed, long bytesReceived,
                        long wantsRefused, long piecesStruck, int treeFetchesActive, long nodesServed,
                        long nodesReceived, long treeRequestsRejected, long treeMessagesDropped,
                        long pieceMessagesDropped) {

        /** The form of 0.5.0, before the tree exchange. */
        public Stats(int fetchesActive, int rootsHeld, long bytesServed, long bytesReceived,
                     long wantsRefused, long piecesStruck) {
            this(fetchesActive, rootsHeld, bytesServed, bytesReceived, wantsRefused, piecesStruck, 0, 0, 0, 0, 0, 0);
        }
    }

    /** The frames of one peer's tree exchange, drained in order by one task at a time. */
    private final class TreeQueue {
        final PeerId peer;
        final java.util.ArrayDeque<byte[]> frames = new java.util.ArrayDeque<>();
        long bytes;
        boolean draining;
        volatile long lastUsedMillis;

        TreeQueue(PeerId peer) {
            this.peer = peer;
        }

        /** Queues a frame and starts a drain when none runs; false when the queue is full. */
        boolean offer(byte[] payload, long now) {
            boolean start;
            synchronized (this) {
                lastUsedMillis = now;
                if (frames.size() >= TREE_QUEUE_FRAMES || bytes + payload.length > TREE_QUEUE_BYTES) {
                    return false;
                }
                frames.addLast(payload);
                bytes += payload.length;
                start = !draining;
                if (start) {
                    draining = true;
                }
            }
            if (start) {
                handlers.execute(this::drain);
            }
            return true;
        }

        private void drain() {
            while (true) {
                byte[] payload;
                synchronized (this) {
                    payload = frames.pollFirst();
                    if (payload == null) {
                        draining = false;
                        return;
                    }
                    bytes -= payload.length;
                }
                try {
                    onFrame(peer, payload);
                } catch (RuntimeException e) {
                    treeMessagesDropped.incrementAndGet();
                    LOG.warning(() -> "tree frame from " + peer.display() + " failed: " + e);
                }
                lastUsedMillis = clock.millis();
            }
        }

        synchronized boolean idle(long now) {
            return !draining && frames.isEmpty() && now - lastUsedMillis >= TREE_QUEUE_IDLE_MILLIS;
        }
    }

    private static final class TreePending {
        final TreeFetchSession session;
        final Subscription holdings;
        final List<Runnable> onDone = new java.util.concurrent.CopyOnWriteArrayList<>();

        TreePending(TreeFetchSession session, Subscription holdings) {
            this.session = session;
            this.holdings = holdings;
        }
    }

    /**
     * The outcome of one possession challenge (PDT 2 phase 3, P2-S-07).
     *
     * @param holder  the challenged member
     * @param root    the root it claims to hold
     * @param picks   the picks named
     * @param proven  the picks whose bytes arrived and verified by CID
     * @param passed  whether every pick verified within the deadline
     * @param reason  why the challenge failed, or null when it passed
     * @param elapsed milliseconds from the send to the outcome
     */
    public record ChallengeResult(PeerId holder, String root, int picks, int proven, boolean passed, String reason,
                                  long elapsed) {
    }

    /** One challenge awaiting its answer: the picks not yet proven, by CID. */
    private static final class PendingChallenge {
        final PeerId holder;
        final String root;
        final int picks;
        final java.util.Set<String> outstanding;
        final long sentAtMillis;
        final long deadlineMillis;
        final java.util.concurrent.CompletableFuture<ChallengeResult> result = new java.util.concurrent.CompletableFuture<>();

        PendingChallenge(PeerId holder, String root, List<Messages.Pick> picks, long sentAtMillis, long deadlineMillis) {
            this.holder = holder;
            this.root = root;
            this.picks = picks.size();
            this.outstanding = new java.util.HashSet<>();
            picks.forEach(p -> outstanding.add(p.cid()));
            this.sentAtMillis = sentAtMillis;
            this.deadlineMillis = deadlineMillis;
        }

        synchronized void settle(boolean passed, String reason, long now) {
            result.complete(new ChallengeResult(holder, root, picks, picks - outstanding.size(), passed, reason, now - sentAtMillis));
        }
    }

    /**
     * What {@link #describe(String)} answers.
     *
     * @param manifest        the manifest
     * @param completeHolders members whose holding says complete
     * @param partialHolders  members holding part of the root
     * @param held            the pieces this peer holds; empty for a tree root, whose nodes the tree store holds
     * @param replicated      whether the manifest is in the content space
     * @param advertised      whether a collection here lists the root
     * @param nodes           for a {@code layout: tree} manifest, the nodes of the tree as a complete holding
     *                        states them (P2-X-01), 0 while no complete holder is known; 0 for a flat manifest
     */
    public record Description(Manifest manifest, int completeHolders, int partialHolders,
                              BitSet held, boolean replicated, boolean advertised, long nodes) {

        /** The form without the collection flag. */
        public Description(Manifest manifest, int completeHolders, int partialHolders, BitSet held, boolean replicated) {
            this(manifest, completeHolders, partialHolders, held, replicated, false, 0);
        }

        /** The form without the node count, which a flat manifest leaves at 0. */
        public Description(Manifest manifest, int completeHolders, int partialHolders, BitSet held, boolean replicated,
                           boolean advertised) {
            this(manifest, completeHolders, partialHolders, held, replicated, advertised, 0);
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
    /** Where this peer reads another member's current content-swarm advertisement (PD-X-01); may be null. */
    private final java.util.function.Function<PeerId, Optional<CapabilityAdvertisement>> advertisements;
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
    /** The tree exchange (srad/agentspaces-pdt.md): the store this peer serves trees from, or null. */
    private final TreeStore treeStore;
    private final TreeServer treeServer;
    private final Map<String, TreePending> treeFetches = new ConcurrentHashMap<>();
    /** Tree roots this peer publishes holdings for (PD-S-06). */
    private final java.util.Set<String> treeRoots = ConcurrentHashMap.newKeySet();
    private final AtomicLong nodesReceived = new AtomicLong();
    private final AtomicLong treeMessagesDropped = new AtomicLong();
    private final AtomicLong pieceMessagesDropped = new AtomicLong();
    private final AtomicInteger batchIds = new AtomicInteger();
    /** The PDT feature level this peer advertises (P2-F-05). */
    private final int pdtLevel;
    /** The CIDs this peer reconciles (P2-K-03). */
    private final java.util.function.Supplier<? extends java.util.Collection<String>> reconcileSet;
    /** The holder's side of reconciliations, at level 2 (P2-K-02). */
    private final ai.badmonkey.agentspaces.content.tree.sync.SketchServer sketchServer;
    /** The reconciliations this peer asked for and awaits, by request id (P2-K-02). */
    private final Map<Integer, ai.badmonkey.agentspaces.content.tree.sync.SketchSession> sketchSessions = new ConcurrentHashMap<>();
    /** The source of RIBLT session keys (P2-K-05). */
    private final java.security.SecureRandom sketchKeys = new java.security.SecureRandom();
    /** Roots whose answered challenges this peer counts at most. */
    static final int MAX_CHALLENGED_ROOTS = 4096;
    /** Challenges this peer answered with every pick held, by root (P2-S-13). */
    private final Map<String, AtomicLong> challengesAnswered = new ConcurrentHashMap<>();
    /** Possession challenges this peer sent and awaits, by request id (P2-S-07). */
    private final Map<Integer, PendingChallenge> challenges = new ConcurrentHashMap<>();
    /**
     * One serial, bounded queue per peer for tags 8 to 11, so a holder's
     * breadth-first stream is verified in order (a parent before its children, a
     * NODES before its TREE_END); piece frames keep one virtual thread each. A
     * queue holds at most {@link #TREE_QUEUE_FRAMES} frames and
     * {@link #TREE_QUEUE_BYTES} bytes (a frame past either is dropped and counted
     * in {@code treeMessagesDropped}), and an idle queue is forgotten after
     * {@link #TREE_QUEUE_IDLE_MILLIS}, so a flooding or departed member holds
     * nothing here for long (§6.7 row 3).
     */
    private final Map<PeerId, TreeQueue> treeQueues = new ConcurrentHashMap<>();
    /** Whether any member's current advertisement says the group's content travels sealed; may be null. */
    private final java.util.function.BooleanSupplier groupSealsLookup;
    private volatile boolean groupSeals;
    private long groupSealsReadAtMillis = NEVER;
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
        this.advertisements = b.advertisements != null ? b.advertisements
                : b.discovery == null ? null : advertisementsIn(b.discovery, this.clock);
        this.treeStore = b.treeStore;
        this.pdtLevel = b.pdtLevel;
        this.reconcileSet = b.reconcileSet != null ? b.reconcileSet : this::heldRoots;
        this.sketchServer = new ai.badmonkey.agentspaces.content.tree.sync.SketchServer(this.reconcileSet, this.clock::millis);
        this.groupSealsLookup = b.groupSeals != null ? b.groupSeals
                : b.discovery == null ? null : groupSealsIn(b.discovery, this.clock);
        this.treeServer = new TreeServer(this.treeStore, treeHost);
        pipes.onCapability(TYPE, (from, payload) -> {
            if (closed) {
                return;
            }
            try {
                int tag = Messages.peekTag(codec, payload).orElse(0);
                if (tag >= Messages.NODE_WANT && tag <= Messages.TREE_END) {
                    if (!treeQueues.computeIfAbsent(from, TreeQueue::new).offer(payload, clock.millis())) {
                        treeMessagesDropped.incrementAndGet();
                    }
                } else {
                    handlers.execute(() -> onFrame(from, payload));
                }
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
        private java.util.function.Function<PeerId, Optional<CapabilityAdvertisement>> advertisements;
        private DiscoveryService discovery;
        private java.util.function.BooleanSupplier groupSeals;
        private TreeStore treeStore;
        private int pdtLevel = 1;
        private java.util.function.Supplier<? extends java.util.Collection<String>> reconcileSet;

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
         * Where this peer reads a member's current content-swarm advertisement, so it
         * sends the tree exchange only to members that advertise it (PD-X-01). The
         * function answers the member's newest unexpired {@code aspace:cap/content-swarm}
         * advertisement, or empty when none is known. Without one, no member is taken
         * to speak the tree exchange. {@link #discovery} wires the group's cache; an
         * explicit function here takes precedence over it.
         */
        public Builder advertisements(java.util.function.Function<PeerId, Optional<CapabilityAdvertisement>> value) {
            this.advertisements = value;
            return this;
        }

        /**
         * The group's discovery service, read local-first ({@link DiscoveryService#find})
         * for the content-swarm advertisements of members (PD-X-01). The cache evicts
         * expired advertisements, and this lookup also refuses one expired at this
         * peer's clock.
         */
        public Builder discovery(DiscoveryService value) {
            this.discovery = value;
            return this;
        }

        /**
         * Whether the group's content travels sealed, as this peer knows it: true when
         * any member's current content-swarm advertisement carries
         * {@code sealing = group-key}. A holder without a content key ring refuses
         * every tree request while this answers true (PD-P-03), whatever the asker's
         * own advertisement says. {@link #discovery} derives it from the cache;
         * an explicit supplier here takes precedence. Read once per
         * {@link Content#GROUP_SEALS_REREAD_MILLIS} on the tick.
         */
        public Builder groupSeals(java.util.function.BooleanSupplier value) {
            this.groupSeals = value;
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

        /**
         * The node store this peer serves trees from and fetches trees into
         * (srad/agentspaces-pdt.md PD-X-03, PD-X-04). Without one the peer
         * answers every {@code NODE_WANT} and {@code TREE_WANT} with {@code REJECT}
         * reason 3 and {@link Content#fetchTree} refuses to start.
         *
         * @param value the store, or null
         * @return this builder
         */
        public Builder treeStore(TreeStore value) {
            this.treeStore = value;
            return this;
        }

        /**
         * The PDT feature level this peer advertises ({@link #PDT_PARAMETER}, P2-F-05): 1 by default, which
         * adds no parameter; {@link #PDT_LEVEL} makes this peer read the level-2 forms of §6.10.4 and answer
         * tag 12 {@code CHALLENGE}, as a content store does (PDT 2 phase 3, P2-S-01).
         *
         * @param value 1 or {@link #PDT_LEVEL}
         * @return this builder
         */
        public Builder pdtLevel(int value) {
            if (value < 1 || value > PDT_LEVEL) {
                throw new IllegalArgumentException("the pdt level is 1 or " + PDT_LEVEL + ", got " + value);
            }
            this.pdtLevel = value;
            return this;
        }

        /**
         * The CIDs this peer reconciles (the {@code sketch} seam of P2-Q-MNT-01; PDT 2 phase 5, P2-K-02, P2-K-03): the
         * roots it holds complete ({@link Content#heldRoots}) by default. A host that reconciles another set, its pins
         * or the version-graph nodes of a ref (§3.6), gives it here; both sides of a session should name the same
         * kind of set.
         *
         * @param value the set, read once per session and at most every ten seconds when this peer answers
         * @return this builder
         */
        public Builder reconcileSet(java.util.function.Supplier<? extends java.util.Collection<String>> value) {
            this.reconcileSet = Objects.requireNonNull(value, "reconcileSet");
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
        Map<String, String> parameters = new java.util.HashMap<>(Map.of(
                "pieceSizes", Manifest.MIN_PIECE_SIZE + "-" + Manifest.MAX_PIECE_SIZE,
                "sealing", keyRing == null ? Manifest.SEALING_NONE : Manifest.SEALING_GROUP_KEY,
                "seedMode", policy.seedMode().name().toLowerCase(java.util.Locale.ROOT),
                TREES_PARAMETER, TREES_VERSION));
        if (pdtLevel >= PDT_LEVEL) {
            parameters.put(PDT_PARAMETER, Integer.toString(pdtLevel));
        }
        return new CapabilityAdvertisement(
                "aspace://" + group.value() + "/cap/content-swarm/" + self.value(),
                self, group, clock.instant(), ADVERTISEMENT_TTL, TYPE, "0.1", "pipe",
                Map.copyOf(parameters), Map.of());
    }

    /** The PDT feature level this peer advertises (P2-F-05). */
    public int pdtLevel() {
        return pdtLevel;
    }

    /**
     * The PDT feature level of a member (§6.10.2, P2-F-05): the canonical decimal text of the {@code pdt}
     * parameter on its newest unexpired content-swarm advertisement; 1 for a member without one, without
     * the parameter, or with any other text.
     *
     * @param peer the member
     * @return the level, at least 1
     */
    public int pdtLevel(PeerId peer) {
        Objects.requireNonNull(peer, "peer");
        if (advertisements == null) {
            return 1;
        }
        String text = advertisements.apply(peer).map(ad -> ad.parameters().get(PDT_PARAMETER)).orElse(null);
        if (text == null || !text.matches("[1-9][0-9]{0,8}")) {
            return 1;
        }
        return Integer.parseInt(text);
    }

    /**
     * Whether an intra-chunk delta frame this peer keeps could reach a member now (PDT 2 phase 4 measures, owner
     * default of 2026-11-09, srad/agentspaces-pdt2.md §10.14): this peer answers in plaintext at level 2, and some
     * other member of the view advertises level 2, the pair {@link TreeServer} sends frames between (P2-Z-01,
     * P2-F-05). A writer keeps frames only while this holds, so a fleet of level-1 members never compresses one.
     *
     * @return true when some member could receive a frame
     */
    public boolean framesReachAMember() {
        if (pdtLevel < PDT_LEVEL || keyRing != null) {
            return false;
        }
        for (var member : runtime.membership().allMembers()) {
            if (pdtLevel(member.id()) >= PDT_LEVEL) {
                return true;
            }
        }
        return false;
    }

    /**
     * The newest unexpired content-swarm advertisement of each member, read
     * local-first from a discovery cache (PD-X-01); what {@link Builder#discovery}
     * installs.
     *
     * @param discovery the group's discovery service
     * @param clock     the clock expiry is judged by
     * @return the lookup
     */
    /**
     * Whether any member's current content-swarm advertisement in a discovery cache
     * says the group's content travels sealed (PD-P-03): the group-seals signal of
     * {@link Builder#groupSeals} as {@link Builder#discovery} derives it.
     */
    public static java.util.function.BooleanSupplier groupSealsIn(DiscoveryService discovery, InstantSource clock) {
        Objects.requireNonNull(discovery, "discovery");
        Objects.requireNonNull(clock, "clock");
        return () -> !discovery.find(CapabilityAdvertisement.class,
                ad -> TYPE.equals(ad.capabilityType()) && !ad.expired(clock.instant())
                        && Manifest.SEALING_GROUP_KEY.equals(ad.parameters().get("sealing"))).isEmpty();
    }

    public static java.util.function.Function<PeerId, Optional<CapabilityAdvertisement>> advertisementsIn(
            DiscoveryService discovery, InstantSource clock) {
        Objects.requireNonNull(discovery, "discovery");
        Objects.requireNonNull(clock, "clock");
        return peer -> discovery.find(CapabilityAdvertisement.class,
                        ad -> TYPE.equals(ad.capabilityType()) && peer.equals(ad.issuer()) && !ad.expired(clock.instant()))
                .stream().max(java.util.Comparator.comparing(CapabilityAdvertisement::issued));
    }

    /**
     * Whether a member's current content-swarm advertisement carries
     * {@code trees = "1"} (PD-X-01), so this peer may send it tags 8 to 11. A
     * member without a known advertisement, or whose advertisement lacks the
     * parameter (a 0.5.0 peer), does not. Reads the lookup the builder's
     * {@code advertisements} or {@code discovery} gave; without one the answer
     * is always false.
     *
     * @param peer the member
     * @return true when the member advertises the tree exchange
     */
    public boolean advertisesTrees(PeerId peer) {
        Objects.requireNonNull(peer, "peer");
        if (advertisements == null) {
            return false;
        }
        return advertisements.apply(peer)
                .map(ad -> TREES_VERSION.equals(ad.parameters().get(TREES_PARAMETER)))
                .orElse(false);
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
        for (TreePending pending : List.copyOf(treeFetches.values())) {
            pending.session.tick();
            if (pending.session.isDone()) {
                finishTree(pending);
            }
        }
        treeQueues.values().removeIf(q -> q.idle(now));
        expireChallenges(now);
        for (ai.badmonkey.agentspaces.content.tree.sync.SketchSession session : List.copyOf(sketchSessions.values())) {
            session.tick(now);
            if (session.outcome().isDone()) {
                sketchSessions.remove(session.req(), session);
            }
        }
        if (groupSealsLookup != null && now - groupSealsReadAtMillis >= GROUP_SEALS_REREAD_MILLIS) {
            groupSealsReadAtMillis = now;
            groupSeals = groupSealsLookup.getAsBoolean();
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
        treeQueues.clear();
        for (Pending pending : List.copyOf(fetches.values())) {
            pending.session.cancel();
            pending.holdings.close();
        }
        fetches.clear();
        for (TreePending pending : List.copyOf(treeFetches.values())) {
            pending.session.cancel();
            pending.holdings.close();
        }
        treeFetches.clear();
        for (ai.badmonkey.agentspaces.content.tree.sync.SketchSession session : sketchSessions.values()) {
            session.outcome().completeExceptionally(new IllegalStateException("the content swarm closed"));
        }
        sketchSessions.clear();
        try {
            store.flush(); // the file store persists what it batched (LB-T-01)
        } catch (RuntimeException e) {
            LOG.warning(() -> "store flush on close failed: " + e.getMessage());
        }
    }

    // ------------------------------------------------------------ API

    /** This peer. */
    public PeerId self() {
        return self;
    }

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
                wantsRefused.get(), piecesStruck.get(), treeFetches.size(), treeServer.nodesServed(),
                nodesReceived.get(), treeServer.rejected(), treeServer.dropped() + treeMessagesDropped.get(),
                pieceMessagesDropped.get());
    }

    /** The node store this peer serves trees from, when it has one. */
    /**
     * The group's content-key ring this swarm seals under, when the group has one; a host
     * that builds its own {@code Pdt} hands it to {@code Pdt.Builder.keyRing} so the
     * sealed-nodes mode can seal (srad/agentspaces-pdt.md PD-P-05, PD-C-03).
     */
    public Optional<GroupKeyRing> keyRing() {
        return Optional.ofNullable(keyRing);
    }

    public Optional<TreeStore> treeStore() {
        return Optional.ofNullable(treeStore);
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
        try (java.io.InputStream in = java.nio.file.Files.newInputStream(file)) {
            options = withIdentity(options, in);
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
        try {
            options = withIdentity(options, new java.io.ByteArrayInputStream(bytes));
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        Manifest manifest = manifestOf(hashed, options, true);
        store.open(manifest);
        for (int i = 0; i < manifest.pieces(); i++) {
            int start = i * manifest.pieceSize();
            store.write(manifest.root(), i, java.util.Arrays.copyOfRange(bytes, start, start + manifest.pieceLength(i)));
        }
        return published(manifest, options);
    }

    /**
     * The options with a plaintext artifact's identity in their attrs (PDT 2 phase 4, P2-B-06, P2-H-05; rule 11 of
     * srad/agentspaces-pdt2.md §6.10.3): {@code pdt.blake3} and {@code pdt.bao} from the tree store, which keeps and
     * serves the outboard, so identical files published at other piece sizes name one identity. A swarm without a tree
     * store names none, and neither does one that seals with the group key, since the identity would name plaintext.
     * The identity is advisory, so a publish whose own attrs leave no room for it within the manifest's bounds names
     * none and publishes as before (PDT 2 phase 4 gate, P2-Q-REL-08).
     */
    private PublishOptions withIdentity(PublishOptions options, java.io.InputStream content) throws IOException {
        Map<String, String> attrs = new java.util.LinkedHashMap<>(options.attrs() == null ? Map.of() : options.attrs());
        if (treeStore == null || keyRing != null || !identityFits(attrs)) {
            return options;
        }
        Map<String, String> identity = treeStore.outboard(content);
        if (identity.isEmpty()) {
            return options;
        }
        attrs.putAll(identity);
        return options.withAttrs(attrs);
    }

    /**
     * Whether {@code pdt.blake3} and {@code pdt.bao} fit beside a publish's own attrs within the manifest's bounds
     * ({@link Manifest#MAX_ATTRS}, {@link Manifest#MAX_ATTRS_BYTES}); both values are CID texts of a fixed length.
     */
    private static boolean identityFits(Map<String, String> attrs) {
        Map<String, String> probe = new java.util.LinkedHashMap<>(attrs);
        int digest = ai.badmonkey.agentspaces.content.tree.Cids.DIGEST_LENGTH;
        probe.put(Manifest.BLAKE3_ATTR, ai.badmonkey.agentspaces.content.tree.Cids.identity(new byte[digest]));
        probe.put(Manifest.BAO_ATTR, ai.badmonkey.agentspaces.content.tree.Cids.ofDigest(new byte[digest],
                ai.badmonkey.agentspaces.content.tree.Cids.DAG_CBOR));
        if (probe.size() > Manifest.MAX_ATTRS) {
            return false;
        }
        long bytes = 0;
        for (Map.Entry<String, String> e : probe.entrySet()) {
            bytes += e.getKey().getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                    + e.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        }
        return bytes <= Manifest.MAX_ATTRS_BYTES;
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
        if (pending != null) {
            pending.onDone.add(callback);
            return;
        }
        TreePending tree = treeFetches.get(root);
        if (tree != null) {
            tree.onDone.add(callback);
            return;
        }
        callback.run();
    }

    // ------------------------------------------------------------ trees

    /**
     * Fetches every node under a tree root from the holders of the root and of
     * the {@code haves} roots (srad/agentspaces-pdt.md §3.5, S2 and S3 of §6.4,
     * PD-X-04 to PD-X-08). Holders prune the subtrees the haves already give
     * this peer, so a version that shares most of its nodes with a held base
     * costs its delta. On completion the root's holding is written as
     * {@link #holdTree} does and the {@link #onFetchDone} callbacks run.
     *
     * @param root    the root CID
     * @param haves   roots this peer's store holds complete, at most {@link Messages#MAX_HAVES}; may be empty
     * @param options the options; {@code prefer} adds sources, {@code timeout} bounds the wait for one (PD-X-08)
     * @return the handle; complete at once when the store already holds the tree
     * @throws IllegalStateException when this peer has no tree store
     */
    public TreeFetch fetchTree(String root, List<String> haves, FetchOptions options) {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(options, "options");
        if (treeStore == null) {
            throw new IllegalStateException("this peer has no tree store; give the builder one before fetching trees");
        }
        if (!ai.badmonkey.agentspaces.content.tree.Cids.isCid(root)) {
            throw new IllegalArgumentException("not a CID: " + root);
        }
        List<String> bases = haves == null ? List.of() : List.copyOf(haves);
        if (bases.size() > Messages.MAX_HAVES) {
            throw new IllegalArgumentException("at most " + Messages.MAX_HAVES + " haves, got " + bases.size());
        }
        TreePending existing = treeFetches.get(root);
        if (existing != null) {
            return existing.session;
        }
        TreeFetchSession session = new TreeFetchSession(root, bases, List.of(), treeFabric, treeStore, options,
                policy.maxSessions(), policy.maxWindow());
        Subscription subscription = spaces.watchHoldings(root, ev -> session.holdingsChanged(), Lease.of(Duration.ofHours(24)));
        TreePending pending = new TreePending(session, subscription);
        TreePending raced = treeFetches.putIfAbsent(root, pending);
        if (raced != null) {
            subscription.close();
            return raced.session;
        }
        session.start();
        if (session.isDone()) {
            finishTree(pending);
        }
        return session;
    }

    /** As {@link #fetchTree(String, List, FetchOptions)} with no haves and the default options. */
    public TreeFetch fetchTree(String root) {
        return fetchTree(root, List.of(), FetchOptions.defaults());
    }

    /**
     * Fetches a batch of nodes by CID with {@code NODE_WANT} alone, following no
     * links: what a node loader asks for when a value is read lazily (PD-V-06).
     * Sources are the options' preferred peers and any holder of one of the CIDs
     * as a root, so a loader passes the holders of the tree it reads from in
     * {@code prefer}. Complete when every CID is held.
     *
     * @param cids    the CIDs, at most {@link Messages#MAX_WANT_CIDS}
     * @param options the options
     * @return the handle
     * @throws IllegalStateException when this peer has no tree store
     */
    public TreeFetch fetchNodes(List<String> cids, FetchOptions options) {
        Objects.requireNonNull(cids, "cids");
        Objects.requireNonNull(options, "options");
        if (treeStore == null) {
            throw new IllegalStateException("this peer has no tree store; give the builder one before fetching nodes");
        }
        if (cids.isEmpty() || cids.size() > Messages.MAX_WANT_CIDS) {
            throw new IllegalArgumentException("a batch names 1 to " + Messages.MAX_WANT_CIDS + " cids, got " + cids.size());
        }
        for (String cid : cids) {
            if (!ai.badmonkey.agentspaces.content.tree.Cids.isCid(cid)) {
                throw new IllegalArgumentException("not a CID: " + cid);
            }
        }
        TreeFetchSession session = new TreeFetchSession(null, List.of(), cids, treeFabric, treeStore, options,
                policy.maxSessions(), policy.maxWindow());
        String key = "batch#" + batchIds.incrementAndGet() + ":" + cids.get(0);
        Subscription subscription = spaces.watchHoldings(cids.get(0), ev -> session.holdingsChanged(), Lease.of(Duration.ofHours(24)));
        TreePending pending = new TreePending(session, subscription);
        treeFetches.put(key, pending);
        session.start();
        if (session.isDone()) {
            finishTree(pending);
        }
        return session;
    }

    /**
     * Publishes this peer's holding for a tree root (PD-S-06): {@code have} null,
     * {@code complete} as the tree store reports it, {@code count} the nodes held
     * under the root, {@code via} the relays this peer sits behind. The holdings
     * tick refreshes it as it refreshes piece holdings until {@link #releaseTree}.
     * A tree root has no manifest, so {@link #describe} answers empty for it; its
     * holders are read through {@link ContentSpaces#holdings}.
     *
     * @param root the root CID
     * @throws IllegalStateException when this peer has no tree store
     */
    public void holdTree(String root) {
        Objects.requireNonNull(root, "root");
        if (treeStore == null) {
            throw new IllegalStateException("this peer has no tree store");
        }
        treeRoots.add(root);
        refreshHolding(root, true);
    }

    /**
     * Shares a tree root this peer holds complete into this capability's group for a lease (PDT 2
     * phase 1, P2-X-02; PD-R-10): the tree counterpart of {@link #share(String, Lease)}. A member of
     * two groups keeps one node store under both swarms, so the second group's swarm serves the
     * nodes the first one fetched without a second transfer; this writes one holding naming this
     * peer as a complete holder, which lapses with the lease unless {@link #holdTree} keeps it.
     *
     * @param root  the root CID, complete in the tree store
     * @param lease the holding's lease in this group
     * @return the holding written
     * @throws IllegalStateException when this peer has no tree store or does not hold the root complete
     */
    public Holding shareTree(String root, Lease lease) {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(lease, "lease");
        if (treeStore == null) {
            throw new IllegalStateException("this peer has no tree store");
        }
        if (!treeStore.complete(root)) {
            throw new IllegalStateException("tree root " + root + " is not complete in this peer's store");
        }
        List<PeerId> via = runtime.viaRelays();
        Holding holding = new Holding(Holding.VERSION, root, self, true, null, treeStore.reachable(root).size(),
                via.isEmpty() ? null : via, null);
        spaces.writeHolding(holding, lease);
        return holding;
    }

    /**
     * Withdraws this peer's holding for a tree root.
     *
     * @param root the root CID
     */
    public void releaseTree(String root) {
        treeRoots.remove(root);
        Holder holder = holders.remove(root);
        if (holder != null && holder.handle != null) {
            try {
                holder.handle.cancel();
            } catch (RuntimeException ignored) {
                // the holding lapses by lease in any case
            }
        }
    }

    /** The tree roots this peer publishes holdings for. */
    public java.util.Set<String> treeRoots() {
        return java.util.Set.copyOf(treeRoots);
    }

    /** Per-peer tree frame queues held now, idle ones included until the tick forgets them (§6.7 row 3). */
    int treeQueuesHeld() {
        return treeQueues.size();
    }

    /** The tree fetches and node batches in progress, for the console. */
    public List<TreeFetch> activeTreeFetches() {
        List<TreeFetch> active = new ArrayList<>();
        for (TreePending p : treeFetches.values()) {
            active.add(p.session);
        }
        return active;
    }

    private void finishTree(TreePending pending) {
        String key = null;
        for (Map.Entry<String, TreePending> e : treeFetches.entrySet()) {
            if (e.getValue() == pending) {
                key = e.getKey();
                break;
            }
        }
        if (key != null && treeFetches.remove(key, pending)) {
            pending.holdings.close();
            for (Runnable callback : pending.onDone) {
                try {
                    callback.run();
                } catch (RuntimeException e) {
                    LOG.warning(() -> "tree fetch-done callback for " + pending.session.root() + " failed: " + e.getMessage());
                }
            }
        }
    }

    // ------------------------------------------------------------ reconciliation (P2-K-02, P2-K-03)

    /**
     * The outcome of one reconciliation with a member (PDT 2 phase 5, P2-K-03).
     *
     * @param peer     the member
     * @param have     CIDs this peer reconciles and the member lacks
     * @param need     CIDs the member holds and this peer lacks: an artifact root as the raw CID whose manifest the
     *                 content space holds, every other as a dag-cbor CID
     * @param rounds   the rounds the session took
     * @param bytes    the bytes of every want and answer of the session
     * @param encoding {@code negentropy}, {@code riblt}, or {@code riblt,negentropy}
     */
    public record Reconciliation(PeerId peer, List<String> have, List<String> need, int rounds, long bytes, String encoding) {
    }

    /**
     * The roots this peer holds complete and advertises: every root whose last holding here says complete, trees and
     * artifacts alike (P2-K-03). The holdings tick walks a tree at most every ten seconds, so a node lost since then
     * shows at the next walk.
     *
     * @return the roots, a snapshot
     */
    public java.util.Set<String> heldRoots() {
        java.util.Set<String> held = new java.util.HashSet<>();
        holders.forEach((root, holder) -> {
            Holding last = holder.last;
            if (last != null && last.complete()) {
                held.add(root);
            }
        });
        return held;
    }

    /**
     * Reconciles this peer's set (by default {@link #heldRoots}) with a member's by negentropy (PDT 2 phase 5, P2-K-02,
     * S4 of §6.4): rounds of a {@code TREE_WANT} with {@code root ""} and the sketch, each answered by a
     * {@code TREE_END} of reason 5, under one request id, until a reply names no range. Only a member at feature
     * level 2 receives the form (rule 12); the session fails at once against a level-1 member and sends nothing.
     *
     * @param peer the member
     * @return the outcome; fails on a refusal, a timeout of a round, 64 rounds, or an answer outside its grammar
     */
    public java.util.concurrent.CompletableFuture<Reconciliation> reconcileHeld(PeerId peer) {
        return reconcileHeld(peer, 0);
    }

    /**
     * As {@link #reconcileHeld(PeerId)}, by RIBLT first when the expected difference is above 0 (P2-K-05): coded
     * symbols stream until the member decodes, up to twice the expected difference (at least 64), and the session
     * continues by negentropy past that.
     *
     * @param peer     the member
     * @param expected the expected size of the difference, or 0 for negentropy alone
     * @return the outcome
     */
    public java.util.concurrent.CompletableFuture<Reconciliation> reconcileHeld(PeerId peer, int expected) {
        Objects.requireNonNull(peer, "peer");
        if (pdtLevel(peer) < PDT_LEVEL) {
            return java.util.concurrent.CompletableFuture.failedFuture(new IllegalStateException(
                    peer.display() + " is at pdt level 1 and reads no sketch"));
        }
        int req = requestIds.incrementAndGet();
        ai.badmonkey.agentspaces.content.tree.sync.SketchSession session = new ai.badmonkey.agentspaces.content.tree.sync.SketchSession(
                peer, req, ai.badmonkey.agentspaces.content.tree.sync.SketchSet.of(reconcileSet.get()), expected, sketchKeys,
                new ai.badmonkey.agentspaces.content.tree.sync.SketchSession.Fabric() {
                    @Override
                    public long nowMillis() {
                        return clock.millis();
                    }

                    @Override
                    public int send(PeerId to, Messages.TreeWant want) {
                        byte[] bytes = codec.toBytes(want);
                        pipes.send(to, TYPE, bytes);
                        return bytes.length;
                    }
                });
        sketchSessions.put(req, session);
        session.start();
        return session.outcome().thenApply(o -> new Reconciliation(peer, o.have(), o.need().stream().map(this::resolveRoot).toList(),
                o.rounds(), o.bytesSent() + o.bytesReceived(), o.encoding()));
    }

    /** A needed root that came back as a digest: the raw CID when the content space holds its manifest, else dag-cbor. */
    private String resolveRoot(String dagCbor) {
        String raw = ai.badmonkey.agentspaces.content.tree.Cids.ofDigest(
                ai.badmonkey.agentspaces.content.tree.Cids.parse(dagCbor).digest(), ai.badmonkey.agentspaces.content.tree.Cids.RAW);
        return spaces.manifest(raw).isPresent() ? raw : dagCbor;
    }

    private boolean onSketchEnd(PeerId from, Messages.TreeEnd end, int bytes) {
        ai.badmonkey.agentspaces.content.tree.sync.SketchSession session = sketchSessions.get(end.req());
        if (session == null || !session.onEnd(from, end, bytes)) {
            return false;
        }
        if (session.outcome().isDone()) {
            sketchSessions.remove(end.req(), session);
        }
        return true;
    }

    private boolean onSketchRefused(PeerId from, Messages.Reject reject) {
        for (ai.badmonkey.agentspaces.content.tree.sync.SketchSession session : sketchSessions.values()) {
            if (session.onReject(from, reject)) {
                sketchSessions.remove(session.req(), session);
                return true;
            }
        }
        return false;
    }

    // ------------------------------------------------------------ eager push (P2-P-01, P2-P-04)

    /** The group's gossip bus, on which the PDT layer pushes a ref's deltas down the ref's stream (PDT 2 phase 5, P2-P-01). */
    public ai.badmonkey.agentspaces.peering.gossip.GossipBus gossip() {
        return runtime.gossip();
    }

    /**
     * Spends download budget on bytes that arrive outside a fetch, as a pushed bundle does (PDT 2 phase 5, P2-P-04).
     *
     * @param bytes the bytes
     * @return false, spending nothing, when the day's budget cannot take them
     */
    public boolean chargeDownload(int bytes) {
        return withinDownloadBudget(bytes);
    }

    /**
     * Spends upload budget on bytes this peer forwards outside a fetch, as a relayed bundle is (PDT 2 phase 5, P2-P-04).
     *
     * @param bytes the bytes
     * @return false, spending nothing, when the day's budget cannot take them
     */
    public boolean chargeUpload(int bytes) {
        return withinUploadBudget(bytes);
    }

    // ------------------------------------------------------------ possession challenges (P2-S-07)

    /**
     * Challenges a member to prove it holds a root (PDT 2 phase 3, P2-S-07; srad/agentspaces-pdt2.md
     * §3.2.4): sends tag 12 {@code CHALLENGE} with the picks and completes with the outcome. Every pick must
     * come back within the deadline and hash to its CID, so a holder that lost a node, holds none of the
     * picks, or must fetch them first fails. The message goes only to a member at feature level 2 (rule
     * 15); a challenge of a level-1 member fails at once and sends nothing. The deadline runs on this
     * peer's clock and the tick settles it.
     *
     * @param holder   the member that claims to hold the root
     * @param root     a tree root (picks name its nodes) or an artifact root (picks name its pieces by the
     *                 raw CID of their table digests, as {@link #pieceCids} lists them)
     * @param picks    1 to {@link Messages#MAX_PICKS} picks
     * @param deadline how long the holder has to answer
     * @return the outcome
     */
    public java.util.concurrent.CompletableFuture<ChallengeResult> challenge(PeerId holder, String root,
                                                                             List<Messages.Pick> picks, Duration deadline) {
        Objects.requireNonNull(holder, "holder");
        Objects.requireNonNull(deadline, "deadline");
        int req = requestIds.incrementAndGet();
        Messages.Challenge message = new Messages.Challenge(req, root, List.copyOf(picks));
        message.violation().ifPresent(v -> {
            throw new IllegalArgumentException(v);
        });
        long now = clock.millis();
        PendingChallenge pending = new PendingChallenge(holder, root, message.picks(), now, now + deadline.toMillis());
        if (pdtLevel(holder) < PDT_LEVEL) {
            pending.settle(false, holder.display() + " is at pdt level 1 and reads no CHALLENGE", now);
            return pending.result;
        }
        challenges.put(req, pending);
        pipes.send(holder, TYPE, codec.toBytes(message));
        return pending.result;
    }

    /**
     * The pieces of an artifact as challenge picks name them (P2-S-07): the raw CID of each piece's
     * sha2-256 digest in the manifest's table, in piece order.
     *
     * @param root the artifact root, whose manifest and table this peer knows
     * @return the piece CIDs, empty when the manifest or its table is not here
     */
    public List<String> pieceCids(String root) {
        Optional<byte[]> table = store.manifest(root).or(() -> spaces.manifest(root)).flatMap(this::tableHere);
        if (table.isEmpty()) {
            return List.of();
        }
        List<String> cids = new ArrayList<>();
        for (int at = 0; at + 32 <= table.get().length; at += 32) {
            cids.add(ai.badmonkey.agentspaces.content.tree.Cids.ofDigest(
                    java.util.Arrays.copyOfRange(table.get(), at, at + 32), ai.badmonkey.agentspaces.content.tree.Cids.RAW));
        }
        return cids;
    }

    /**
     * How many challenges of a root this peer answered with every pick held (P2-S-13), counted for at most
     * {@link #MAX_CHALLENGED_ROOTS} roots.
     *
     * @param root the root
     * @return the count
     */
    public long challengesAnswered(String root) {
        AtomicLong count = challengesAnswered.get(root);
        return count == null ? 0 : count.get();
    }

    /** Takes the answer to a challenge this peer sent; false when the request is no challenge of this peer's. */
    private boolean onChallengeAnswer(PeerId from, Messages.Nodes nodes) {
        PendingChallenge pending = challenges.get(nodes.req());
        if (pending == null || !pending.holder.equals(from)) {
            return false;
        }
        long now = clock.millis();
        if (now > pending.deadlineMillis) {
            // the tick settles a lapsed challenge only on its next run; an answer in between is still late
            finishChallenge(nodes.req(), pending, false, "no complete answer before the deadline", now);
            return true;
        }
        Optional<List<byte[]>> opened = nodes.violation().isPresent() ? Optional.empty() : treeFabric.open(from, nodes.req(), nodes);
        synchronized (pending) {
            if (opened.isEmpty()) {
                finishChallenge(nodes.req(), pending, false, "an answer that does not open or breaks its bounds", now);
                return true;
            }
            for (byte[] bytes : opened.get()) {
                String matched = null;
                for (String cid : pending.outstanding) {
                    int codecOf = ai.badmonkey.agentspaces.content.tree.Cids.parse(cid).codec();
                    if (cid.equals(ai.badmonkey.agentspaces.content.tree.Cids.of(bytes, codecOf))) {
                        matched = cid;
                        break;
                    }
                }
                if (matched == null) {
                    finishChallenge(nodes.req(), pending, false, "bytes that match no pick", now);
                    return true;
                }
                pending.outstanding.remove(matched);
            }
            if (nodes.missing() != null && nodes.missing().stream().anyMatch(pending.outstanding::contains)) {
                finishChallenge(nodes.req(), pending, false, nodes.missing().size() + " picks not held", now);
            } else if (pending.outstanding.isEmpty()) {
                finishChallenge(nodes.req(), pending, true, null, now);
            }
        }
        return true;
    }

    /** Takes a refusal of a challenge; false when no challenge to that member names the root. */
    private boolean onChallengeRefused(PeerId from, Messages.Reject reject) {
        for (Map.Entry<Integer, PendingChallenge> e : challenges.entrySet()) {
            PendingChallenge pending = e.getValue();
            if (pending.holder.equals(from) && pending.root.equals(reject.root())) {
                finishChallenge(e.getKey(), pending, false, "REJECT reason " + reject.reason(), clock.millis());
                return true;
            }
        }
        return false;
    }

    private void finishChallenge(int req, PendingChallenge pending, boolean passed, String reason, long now) {
        if (challenges.remove(req, pending)) {
            pending.settle(passed, reason, now);
        }
    }

    /** Settles every challenge whose deadline passed (the tick). */
    private void expireChallenges(long now) {
        for (Map.Entry<Integer, PendingChallenge> e : challenges.entrySet()) {
            if (now > e.getValue().deadlineMillis) {
                finishChallenge(e.getKey(), e.getValue(), false, "no complete answer before the deadline", now);
            }
        }
    }

    /** Whether a member's current advertisement says its group content travels sealed. */
    private boolean advertisesSealing(PeerId peer) {
        if (advertisements == null) {
            return false;
        }
        return advertisements.apply(peer)
                .map(ad -> Manifest.SEALING_GROUP_KEY.equals(ad.parameters().get("sealing")))
                .orElse(false);
    }

    /** The AAD a sealed NODES binds (PD-P-03): the recipient and the request. */
    static byte[] treeSealingAad(PeerId recipient, int req) {
        return (Messages.SEALING_AAD_PREFIX + recipient.value() + "|" + req).getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Reads the opened plaintext of a sealed {@code NODES}: the wire encoding of the nodes list, a
     * nested document read through {@link CborCodec#fromWire} (PDT 2 phase 1, P2-X-09).
     *
     * @return the node bytes, or empty when the plaintext is not the list's own encoding
     */
    static Optional<List<byte[]>> sealedNodesList(CborCodec codec, byte[] plain) {
        try {
            return Optional.of(List.of(codec.fromWire(plain, byte[][].class)));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    /**
     * Fetches an artifact from every reachable holder. A flat root (a raw CID over its piece table) fetches by the
     * piece swarm. A blob tree's root, the dag-cbor CID a {@code layout: tree} manifest names, fetches by the tree
     * exchange, as {@link #fetchTree(String, List, FetchOptions)} with no haves does, and never by a piece table,
     * which a tree manifest does not carry (PDT 2 follow-ups, 2026-11-09, F3; P2-X-01). The handle of a tree
     * reports its nodes, reads the manifest once a member writes it, and does not {@link Fetch#open open}: the
     * persistent-tree layer reads the blob from the tree store.
     *
     * @param root    the root
     * @param options the options
     * @return the fetch handle; complete at once when the store already holds the artifact or the tree
     * @throws IllegalStateException when the root names a tree and this peer has no tree store
     */
    public Fetch fetch(String root, FetchOptions options) {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(options, "options");
        if (!ContentRoots.isRoot(root)) {
            throw new IllegalArgumentException("not a content root: " + root);
        }
        if (ai.badmonkey.agentspaces.content.tree.Cids.isCid(root)
                && ai.badmonkey.agentspaces.content.tree.Cids.parse(root).codec() == ai.badmonkey.agentspaces.content.tree.Cids.DAG_CBOR) {
            return new TreeLayoutFetch(fetchTree(root, List.of(), options), () -> spaces.manifest(root));
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

    /**
     * The download budget for node bytes that already arrived (PD-P-02): the bytes
     * are charged whatever the answer, and the answer says whether the fetch may go
     * on; zero bytes asks whether anything is left.
     */
    private synchronized boolean downloadBudgetAllows(long bytes) {
        long budget = policy.downloadBytesPerDay();
        if (budget <= 0) {
            return true;
        }
        long day = clock.millis() / 86_400_000L;
        if (day != downloadDay) {
            downloadDay = day;
            downloadedToday = 0;
        }
        downloadedToday += bytes;
        return downloadedToday < budget;
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
                store.forget(root);
                rootsDroppedOnSample.incrementAndGet(); // counted after the forget, so the count says it is done
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
                externalVerified.removeIf(k -> k.startsWith(m.root() + "#"));
                store.forget(m.root());
                rootsDroppedOnSample.incrementAndGet(); // counted after the forget, so the count says it is done
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

    /**
     * Describes a root without starting a fetch (LB-M-07). A {@code layout: tree} manifest's root counts its
     * holders from the tree holdings and its nodes from the most nodes a complete holding states (P2-X-01).
     */
    public Optional<Description> describe(String root) {
        Optional<Manifest> replicated = spaces.manifest(root);
        Optional<Manifest> manifest = replicated.or(() -> store.manifest(root));
        if (manifest.isEmpty()) {
            return Optional.empty();
        }
        int complete = 0;
        int partial = 0;
        long nodes = 0;
        for (Holding h : spaces.holdings(root)) {
            if (h.complete()) {
                complete++;
                nodes = Math.max(nodes, h.count());
            } else {
                partial++;
            }
        }
        boolean advertised = collections.stream().anyMatch(c -> c.advertises(manifest.get()));
        return Optional.of(new Description(manifest.get(), complete, partial, store.have(root), replicated.isPresent(),
                advertised, manifest.get().treeLayout() ? nodes : 0));
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
        if (treeRoots.contains(root)) {
            refreshTreeHolding(root, force);
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

    /**
     * The holding of a tree root (PD-S-06): {@code have} null, {@code complete} and
     * {@code count} from the tree store. The store is walked at most once per
     * {@link #HOLDING_WRITE_INTERVAL_MILLIS} unless forced; between walks the
     * holding renews at half-life like a piece holding.
     */
    private void refreshTreeHolding(String root, boolean force) {
        if (treeStore == null) {
            return;
        }
        Holder holder = holders.computeIfAbsent(root, r -> new Holder());
        long now = clock.millis();
        boolean walkDue = force || holder.handle == null || now - holder.writtenAtMillis >= HOLDING_WRITE_INTERVAL_MILLIS;
        if (walkDue) {
            int count = treeStore.reachable(root).size();
            if (count == 0) {
                return;
            }
            boolean complete = treeStore.complete(root);
            if (policy.seedMode() == ContentPolicy.SeedMode.COMPLETE_ONLY && !complete) {
                return;
            }
            boolean changed = holder.piecesAtWrite != count || holder.last == null || holder.last.complete() != complete;
            if (force || holder.handle == null || changed) {
                List<PeerId> via = runtime.viaRelays();
                Holding holding = new Holding(Holding.VERSION, root, self, complete, null, count,
                        via.isEmpty() ? null : via, null);
                EntryHandle previous = holder.handle;
                try {
                    holder.handle = spaces.writeHolding(holding, holdingLease);
                } catch (RuntimeException e) {
                    LOG.fine(() -> "tree holding write deferred for " + root + ": " + e.getMessage());
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
                return;
            }
            holder.writtenAtMillis = now; // nothing changed: the next walk waits the interval
        }
        if (holder.handle != null && now - holder.renewedAtMillis >= holdingLease.duration().toMillis() / 2) {
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

    /** The checks and the sealing the tree server runs through (PD-P-01 to PD-P-03). */
    private final TreeServer.Host treeHost = new TreeServer.Host() {
        @Override
        public boolean mayServe(PeerId from, String cid) {
            if (!Content.this.mayServe(from, cid, null)) {
                return false;
            }
            // PD-P-03: a keyless holder in a group that seals serves nothing. The group seals when any
            // member's advertisement says so (this peer's own reading of the cache), or when the asker's
            // does; the asker alone cannot talk a keyless holder into plaintext.
            return keyRing != null || !(groupSeals() || advertisesSealing(from));
        }

        @Override
        public boolean withinUploadBudget(int bytes) {
            return Content.this.withinUploadBudget(bytes);
        }

        @Override
        public long nowMillis() {
            return clock.millis();
        }

        @Override
        public Optional<Messages.Nodes> seal(PeerId to, int req, List<byte[]> nodes, List<String> missing) {
            if (keyRing == null) {
                return Optional.of(Messages.Nodes.plain(req, nodes, missing));
            }
            long epoch;
            GroupKey key;
            try {
                epoch = keyRing.writeEpoch(clock.instant());
                key = keyRing.sealingKey(epoch).orElse(null);
            } catch (IllegalStateException e) {
                return Optional.empty();
            }
            if (key == null) {
                return Optional.empty();
            }
            byte[] sealed = key.encrypt(codec.toBytes(nodes.toArray(new byte[0][])), treeSealingAad(to, req));
            return Optional.of(Messages.Nodes.sealed(req, sealed, epoch, missing));
        }

        @Override
        public void send(PeerId to, Object message) {
            pipes.send(to, TYPE, codec.toBytes(message));
        }

        @Override
        public void served(int nodes, long bytes) {
            bytesServed.addAndGet(bytes);
        }

        @Override
        public boolean levelTwo() {
            return pdtLevel >= PDT_LEVEL;
        }

        @Override
        public boolean readsLevelTwo(PeerId peer) {
            return pdtLevel(peer) >= PDT_LEVEL; // PDT 2 phase 4, P2-Z-01 and P2-F-05
        }

        @Override
        public void challengeAnswered(String root) {
            if (challengesAnswered.size() < MAX_CHALLENGED_ROOTS || challengesAnswered.containsKey(root)) {
                challengesAnswered.computeIfAbsent(root, r -> new AtomicLong()).incrementAndGet();
            }
        }

        @Override
        public Optional<byte[]> piece(String root, String cid) {
            Optional<Manifest> manifest = store.manifest(root);
            if (manifest.isEmpty()) {
                return Optional.empty(); // the server checked the asker's grant on the root
            }
            List<String> pieces = pieceCids(root);
            int index = pieces.indexOf(cid);
            if (index < 0 || !store.have(root).get(index)) {
                return Optional.empty();
            }
            return store.read(root, index, 0, manifest.get().pieceLength(index));
        }

        @Override
        public Optional<ai.badmonkey.agentspaces.content.tree.sync.SketchServer> sketches() {
            return Optional.of(sketchServer); // a level-2 holder reconciles; a level-1 one never gets here
        }
    };

    /** What a tree fetch needs from this capability (PD-X-04 to PD-X-08). */
    private final TreeFetchSession.Fabric treeFabric = new TreeFetchSession.Fabric() {
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
        public int hopsTo(PeerId peer) {
            return runtime.hopsTo(peer);
        }

        @Override
        public boolean advertisesTrees(PeerId peer) {
            return Content.this.advertisesTrees(peer);
        }

        @Override
        public Optional<List<byte[]>> open(PeerId from, int req, Messages.Nodes nodes) {
            if (nodes.plaintext()) {
                return Optional.of(nodes.nodes());
            }
            if (keyRing == null) {
                return Optional.empty();
            }
            byte[] aad = treeSealingAad(self, req);
            for (GroupKey key : keyRing.openingKeys(nodes.epoch())) {
                Optional<byte[]> plain = key.decrypt(nodes.sealed(), aad);
                if (plain.isPresent()) {
                    return sealedNodesList(codec, plain.get());
                }
            }
            return Optional.empty();
        }

        @Override
        public void onNodesVerified(int count, long bytes) {
            nodesReceived.addAndGet(count);
        }

        @Override
        public boolean readsDeltas() {
            return pdtLevel >= PDT_LEVEL; // PDT 2 phase 4, P2-Z-01: a level-1 peer reads no NODES.deltas
        }

        @Override
        public boolean withinDownloadBudget(long bytes) {
            return downloadBudgetAllows(bytes);
        }

        @Override
        public void onComplete(TreeFetchSession session) {
            if (!session.batch() && session.complete() && treeStore != null && treeStore.complete(session.root())
                    && policy.seedMode() != ContentPolicy.SeedMode.NEVER) {
                treeRoots.add(session.root());
                refreshHolding(session.root(), true);
            }
        }
    };

    // ------------------------------------------------------------ the holder side

    /**
     * Dispatches one content-swarm message. Every message reads through {@link Messages#decode}
     * under its wire schema (PDT 2 phase 1, P2-X-09): a piece message (tags 1 to 7) that breaks it
     * counts in {@code pieceMessagesDropped}; a tree message (tags 8 to 11) that breaks it, and a
     * payload with an unknown tag or none, count in {@code treeMessagesDropped}, as the TypeScript
     * port counts them. None of them strikes the sender, since a relabel or a newer tag proves no
     * hostility.
     */
    private void onFrame(PeerId from, byte[] payload) {
        if (closed) {
            return;
        }
        Optional<Integer> tag = Messages.tagOf(codec, payload);
        if (tag.isEmpty()) {
            treeMessagesDropped.incrementAndGet();
            return;
        }
        switch (tag.get()) {
            case Messages.PIECE_WANT -> piece(payload, Messages.PieceWant.class, want -> serve(from, want));
            case Messages.PIECE -> piece(payload, Messages.Piece.class, piece -> {
                Pending pending = fetches.get(piece.root());
                if (pending != null) {
                    pending.session.onPiece(from, piece);
                    if (pending.session.isDone()) {
                        finish(pending);
                    }
                }
            });
            case Messages.REJECT -> piece(payload, Messages.Reject.class, reject -> {
                if (reject.index() == Messages.TREE_INDEX) {
                    if (onChallengeRefused(from, reject) || ("".equals(reject.root()) && onSketchRefused(from, reject))) {
                        return;
                    }
                    for (TreePending pending : treeFetches.values()) {
                        if (pending.session.onReject(from, reject)) {
                            break;
                        }
                    }
                    return;
                }
                Pending pending = fetches.get(reject.root());
                if (pending != null) {
                    pending.session.onReject(from, reject);
                }
            });
            // a holder answers wants one at a time, so a cancel has nothing to withdraw; it is still read
            case Messages.CANCEL -> piece(payload, Messages.Cancel.class, cancel -> { });
            case Messages.HAVE -> piece(payload, Messages.Have.class, have -> {
                // LB-H-08: advisory; the pieces a source verified since its holding, folded into the fetch
                Pending pending = fetches.get(have.root());
                Manifest m = pending == null ? null : store.manifest(have.root()).orElse(null);
                if (pending != null && m != null) {
                    BitSet bits = have.complete() || have.have() == null ? fullSet(m.pieces())
                            : decodeHave(have.have(), m.pieces());
                    pending.session.onHave(from, bits);
                }
            });
            case Messages.MANIFEST_WANT -> piece(payload, Messages.ManifestWant.class, want ->
                    store.manifest(want.root()).ifPresent(m ->
                            pipes.send(from, TYPE, codec.toBytes(new Messages.ManifestReply(want.root(), codec.toBytes(m))))));
            case Messages.MANIFEST -> piece(payload, Messages.ManifestReply.class, reply -> onManifest(from, reply));
            case Messages.NODE_WANT -> Messages.decode(codec, payload, Messages.NodeWant.class)
                    .ifPresentOrElse(want -> treeServer.serve(from, want), treeMessagesDropped::incrementAndGet);
            case Messages.TREE_WANT -> Messages.decode(codec, payload, Messages.TreeWant.class)
                    .ifPresentOrElse(want -> treeServer.serve(from, want), treeMessagesDropped::incrementAndGet);
            case Messages.NODES -> Messages.decode(codec, payload, Messages.Nodes.class)
                    .ifPresentOrElse(nodes -> {
                        if (onChallengeAnswer(from, nodes)) {
                            return;
                        }
                        if (nodes.violation().isPresent()) {
                            treeMessagesDropped.incrementAndGet();
                            // the request it answered is released, so the rest of its stream strikes nobody
                            for (TreePending pending : treeFetches.values()) {
                                if (pending.session.onUnusableNodes(from, nodes.req())) {
                                    break;
                                }
                            }
                            return;
                        }
                        boolean taken = false;
                        for (TreePending pending : treeFetches.values()) {
                            if (pending.session.onNodes(from, nodes)) {
                                taken = true;
                                if (pending.session.isDone()) {
                                    finishTree(pending);
                                }
                                break;
                            }
                        }
                        if (!taken) {
                            treeMessagesDropped.incrementAndGet();
                        }
                    }, treeMessagesDropped::incrementAndGet);
            case Messages.TREE_END -> Messages.decode(codec, payload, Messages.TreeEnd.class)
                    .ifPresentOrElse(end -> {
                        if (end.violation().isPresent()) {
                            treeMessagesDropped.incrementAndGet();
                            return;
                        }
                        if (end.reason() == Messages.END_SKETCH) {
                            if (!onSketchEnd(from, end, payload.length)) {
                                treeMessagesDropped.incrementAndGet();
                            }
                            return;
                        }
                        boolean taken = false;
                        for (TreePending pending : treeFetches.values()) {
                            if (pending.session.onTreeEnd(from, end)) {
                                taken = true;
                                if (pending.session.isDone()) {
                                    finishTree(pending);
                                }
                                break;
                            }
                        }
                        if (!taken) {
                            treeMessagesDropped.incrementAndGet();
                        }
                    }, treeMessagesDropped::incrementAndGet);
            // P2-S-07: a level-1 peer reads no level-2 form, so the server drops and counts tag 12 there
            case Messages.CHALLENGE -> Messages.decode(codec, payload, Messages.Challenge.class)
                    .ifPresentOrElse(challenge -> treeServer.serve(from, challenge), treeMessagesDropped::incrementAndGet);
            default -> treeMessagesDropped.incrementAndGet();
        }
    }

    /** Reads a piece message (tags 1 to 7) under its wire schema and acts on it, or counts it dropped (P2-X-09). */
    private <T> void piece(byte[] payload, Class<T> type, java.util.function.Consumer<T> action) {
        Messages.decode(codec, payload, type).ifPresentOrElse(action, pieceMessagesDropped::incrementAndGet);
    }

    private void serve(PeerId from, Messages.PieceWant want) {
        Optional<Manifest> manifest = store.manifest(want.root());
        if (manifest.isEmpty()) {
            reject(from, want, Messages.REASON_UNKNOWN_ROOT);
            return;
        }
        if (!mayServe(from, want.root(), manifest.get())) {
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

    /**
     * The checks every want passes before this peer serves (LB-P-02, PD-P-01): the
     * seed mode, the policy, and the {@code CONTENT_FETCH} grant. For a piece want
     * the scope is the root of the manifest. A null manifest is a request of the
     * tree exchange, whose scope is the CID asked for (the root of a
     * {@code TREE_WANT}, each CID of a {@code NODE_WANT}), since a tree root has no
     * manifest and a holder cannot bind a node to a root without a walk; an
     * authorizer that scopes a grant to one tree root therefore answers for the
     * CIDs under it as well, for example by consulting its tree store. A policy that
     * throws on a tree request (one that reads the manifest without overriding
     * {@link ContentPolicy#mayServeTree}) refuses it.
     */
    private boolean mayServe(PeerId from, String root, Manifest manifest) {
        if (policy.seedMode() == ContentPolicy.SeedMode.NEVER) {
            return false;
        }
        try {
            if (manifest == null ? !policy.mayServeTree(runtime.id(), from, root) : !policy.mayServe(runtime.id(), from, manifest)) {
                return false;
            }
        } catch (RuntimeException e) {
            LOG.fine(() -> "content policy failed for " + root + " from " + from.display() + ": " + e);
            return false;
        }
        return authorizer == null || authorizer.permits(from, Authorizer.Operation.CONTENT_FETCH, root);
    }

    /**
     * Whether the group's content travels sealed, as the tick last read it (PD-P-03): any member's current
     * advertisement carries {@code sealing = group-key}. A blind content store refuses a tree pin while this
     * holds, since it could open nothing it fetched (PDT 2 phase 3, P2-S-08).
     */
    public boolean groupSeals() {
        if (groupSealsLookup != null && groupSealsReadAtMillis == NEVER) {
            groupSealsReadAtMillis = clock.millis();
            groupSeals = groupSealsLookup.getAsBoolean();
        }
        return groupSeals;
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
        if (pending == null || pending.session.hasManifest()) {
            return;
        }
        Manifest manifest;
        try {
            // the record travels as the manifest's own wire encoding (P2-X-09)
            manifest = codec.fromWire(reply.record(), Manifest.class);
        } catch (RuntimeException e) {
            pieceMessagesDropped.incrementAndGet();
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
