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
package ai.badmonkey.agentspaces.content.tree;

import ai.badmonkey.agentspaces.agent.content.Holding;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.FetchOptions;
import ai.badmonkey.agentspaces.content.Messages;

import java.time.Duration;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One tree fetch (srad/agentspaces-pdt.md §3.5, S2 and S3 of §6.4, PD-X-04 to
 * PD-X-08): the fetcher side of the tree exchange over a {@link TreeStore}.
 *
 * <p>Order of work. The session asks for the root with {@code NODE_WANT} when the
 * store lacks it. Once the root is held it reads the root's children and sends a
 * {@code TREE_WANT} per child whose subtree the store does not hold complete,
 * split across the top two sources of the ranking (one {@code TREE_WANT} for the
 * root itself when it has at most one child). Every verified node adds the links
 * the store lacks to the frontier, and the frontier goes out as {@code NODE_WANT}
 * batches of at most {@link Messages#MAX_WANT_CIDS} across every source, skipping
 * CIDs a source answered {@code missing} for and CIDs under a subtree walk still
 * in flight. A {@code TREE_END} adds what the holder did not send, and the store's
 * own view of the subtree, to the frontier, so a holder without the haves, a
 * budget stop, or a walk cap all continue the same way (PD-X-06). In the endgame
 * (no unasked work left, no subtree walk in flight, and a batch late by more than
 * {@link #ENDGAME_LATE_MILLIS} or twice its source's smoothed round trip) the late
 * batches are duplicated to the other sources once and nothing is cancelled, since
 * a batch is small. The fetch is complete when {@link TreeStore#complete} says so.
 *
 * <p>Verification (PD-X-04). A {@code NODES} is accepted only for a request this
 * session sent to that peer. A node of a {@code NODE_WANT} answer must hash to a
 * CID of the request, taken in order; a node of a {@code TREE_WANT} answer must
 * hash to the subtree root or to a link of a node this fetch already verified.
 * The sha2-256 is recomputed through {@link Cids}; a node that matches nothing
 * strikes its sender through the fabric and drops that source for the fetch.
 * The CIDs a holder names in a {@code TREE_END} frontier or in the {@code missing}
 * list of a {@code TREE_WANT} answer pass the same rule: only the subtree root or
 * a link of a verified node enters the frontier, and a holder that names anything
 * else is struck, so one source can neither steer the fetch into foreign nodes
 * nor stall it on a CID nobody holds. Because a holder walks breadth first and the
 * capability delivers one peer's tree frames in order, a parent always precedes
 * its children; an answer that cannot be opened, or that the capability dropped
 * for a violated bound, releases its request so the rest of that stream cannot
 * strike an honest holder.
 *
 * <p>Sources (PD-X-05) are the holders of the root and of every haves root, plus
 * the options' preferred peers, among members whose advertisement carries
 * {@code trees = "1"}; {@link SourceRanking} orders them and keeps their windows,
 * timeouts, strikes, and back-off. With no usable source, or when every usable
 * source has answered {@code missing} for everything left to ask, the fetch waits,
 * and when the options name a timeout it fails after it with an exception naming
 * the root (PD-X-08); a new holding then starts a new fetch through the capability.
 * Received node bytes count against the download budget of the policy through
 * {@link Fabric#withinDownloadBudget} (PD-P-02): once the day's budget is spent
 * the session sends nothing more until the budget allows again.
 *
 * <p>A session is also the bare {@code NODE_WANT} batch of {@code Content.fetchNodes}:
 * no root, a fixed set of wanted CIDs, links not followed, complete when every
 * CID is held.
 */
public final class TreeFetchSession implements TreeFetch {

    static final long HOLDINGS_REREAD_MILLIS = 5_000;
    static final int TRACE_CAPACITY = 256;
    /** The budget every {@code TREE_WANT} asks for. */
    static final long TREE_BUDGET = Messages.MAX_TREE_BUDGET;
    /** Sources a subtree split spreads over (S2: the top two). */
    static final int SUBTREE_SOURCES = 2;
    /** A batch outstanding this long (or twice its source's smoothed round trip) is late enough to duplicate. */
    static final long ENDGAME_LATE_MILLIS = 1_000;

    /** What the session needs from the capability. */
    public interface Fabric {
        PeerId self();

        long nowMillis();

        void send(PeerId to, Object message);

        int nextRequestId();

        List<Holding> holdings(String root);

        void strike(PeerId peer, String reason);

        /** Relays between this peer and a holder: 0 direct, -1 unreachable. */
        int hopsTo(PeerId peer);

        /** Whether a member's current advertisement carries the tree exchange (PD-X-01). */
        boolean advertisesTrees(PeerId peer);

        /**
         * Opens a {@code NODES} answer: the plaintext nodes as sent, or the sealed
         * list opened with the epoch's opening keys (PD-P-03).
         *
         * @return the nodes, or empty when a sealed answer cannot be opened here
         */
        Optional<List<byte[]>> open(PeerId from, int req, Messages.Nodes nodes);

        /** Nodes verified in this session, for the capability's counters. */
        default void onNodesVerified(int count, long bytes) {
        }

        /**
         * Spends download budget for node bytes that arrived (PD-P-02). Zero bytes
         * asks whether any budget is left without spending it.
         *
         * @param bytes the bytes received, or zero to ask
         * @return false when the day's budget is spent; the session then sends nothing
         *         more until a later call answers true
         */
        default boolean withinDownloadBudget(long bytes) {
            return true;
        }

        /** The session completed. */
        void onComplete(TreeFetchSession session);
    }

    private static final class Source extends SourceRanking.Source {
        /** CIDs this source answered {@code missing} or {@code REJECT} not-held for. */
        final Set<String> lacks = new HashSet<>();

        Source(PeerId id) {
            super(id);
        }
    }

    private static final class Request {
        final int req;
        final Source source;
        final int kind;
        final List<String> cids;
        final String subtree;
        /** One more than the generation of the answer this request needed: the depth of its dependency chain. */
        final int generation;
        long lastActivityMillis;
        int matched;
        boolean duplicated;

        Request(int req, Source source, int kind, List<String> cids, String subtree, int generation, long now) {
            this.req = req;
            this.source = source;
            this.kind = kind;
            this.cids = cids;
            this.subtree = subtree;
            this.generation = generation;
            this.lastActivityMillis = now;
        }
    }

    private final String root;
    private final List<String> haves;
    private final List<String> wanted;
    private final Fabric fabric;
    private final TreeStore store;
    private final FetchOptions options;
    private final SourceRanking ranking;
    private final int maxSessions;
    private final int maxWindow;
    private final ReentrantLock lock = new ReentrantLock();
    private final CompletableFuture<Void> done = new CompletableFuture<>();
    private final Map<PeerId, Source> sources = new LinkedHashMap<>();
    private final Map<Integer, Request> outstanding = new LinkedHashMap<>();
    private final Set<String> expected = new HashSet<>();
    private final Set<String> verified = new HashSet<>();
    private final Set<String> frontier = new LinkedHashSet<>();
    private final Set<String> askedNodes = new HashSet<>();
    private final Map<String, String> subtreeOf = new HashMap<>();
    /** The generation of the answer that revealed a CID, so a request's generation is the chain depth behind it. */
    private final Map<String, Integer> generationOf = new HashMap<>();
    private final Set<String> inFlightSubtrees = new HashSet<>();
    private final Deque<String> pendingSubtrees = new ArrayDeque<>();
    private final Deque<String> trace = new ArrayDeque<>();
    private final long startedAtMillis;

    private boolean started;
    private boolean treePlanned;
    private int nodesVerified;
    private long bytesReceived;
    private int requestsSent;
    private int roundTrips;
    private volatile boolean holdingsDirty = true;
    private long holdingsReadAtMillis = Long.MIN_VALUE / 4;
    private long lastSourceSeenAtMillis;
    private boolean scheduling;
    private boolean rescheduleRequested;
    private volatile boolean cancelled;
    private boolean waiting;
    /** The download budget is spent (PD-P-02); cleared when a tick finds budget again. */
    private boolean budgetSpent;
    /** Frontier CIDs a holder named that no verified node links to (PD-X-04). */
    private long foreignCidsRefused;

    /**
     * A tree fetch.
     *
     * @param root        the root, or null for a bare node batch
     * @param haves       roots the store holds complete that the holders may prune against; at most 64
     * @param wanted      the CIDs of a bare batch, or empty for a tree fetch
     * @param fabric      the capability
     * @param store       the node store
     * @param options     the fetch options
     * @param maxSessions sources at most
     * @param maxWindow   requests in flight per source at most
     */
    public TreeFetchSession(String root, List<String> haves, List<String> wanted, Fabric fabric, TreeStore store,
                            FetchOptions options, int maxSessions, int maxWindow) {
        this.root = root;
        this.haves = haves == null ? List.of() : List.copyOf(haves);
        this.wanted = wanted == null ? List.of() : List.copyOf(wanted);
        this.fabric = Objects.requireNonNull(fabric, "fabric");
        this.store = Objects.requireNonNull(store, "store");
        this.options = Objects.requireNonNull(options, "options");
        this.ranking = new SourceRanking(options.prefer());
        this.maxSessions = Math.max(1, maxSessions);
        this.maxWindow = Math.max(1, maxWindow);
        this.startedAtMillis = fabric.nowMillis();
        this.lastSourceSeenAtMillis = startedAtMillis;
        if (root == null && this.wanted.isEmpty()) {
            throw new IllegalArgumentException("a tree fetch names a root or a batch of cids");
        }
    }

    // ------------------------------------------------------------ TreeFetch

    @Override
    public String root() {
        return root != null ? root : wanted.get(0);
    }

    @Override
    public Progress progress() {
        lock.lock();
        try {
            Map<PeerId, Long> bySource = new LinkedHashMap<>();
            int active = 0;
            for (Source s : sources.values()) {
                bySource.put(s.id, s.bytesVerified);
                if (s.inFlight > 0) {
                    active++;
                }
            }
            return new Progress(nodesVerified, bytesReceived, bySource, active, requestsSent, roundTrips, waiting);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean await(Duration timeout) throws InterruptedException {
        try {
            if (timeout.isZero() || timeout.isNegative()) {
                done.get();
            } else {
                done.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            }
            return true;
        } catch (TimeoutException | java.util.concurrent.ExecutionException e) {
            return false;
        }
    }

    @Override
    public boolean complete() {
        return done.isDone() && !done.isCompletedExceptionally() && !cancelled;
    }

    @Override
    public boolean isDoneOrFailed() {
        return done.isDone();
    }

    @Override
    public Optional<Throwable> failure() {
        if (!done.isCompletedExceptionally()) {
            return Optional.empty();
        }
        try {
            done.join();
            return Optional.empty();
        } catch (java.util.concurrent.CompletionException | java.util.concurrent.CancellationException e) {
            return Optional.of(e.getCause() == null ? e : e.getCause());
        }
    }

    @Override
    public boolean cancelled() {
        return cancelled;
    }

    @Override
    public void cancel() {
        lock.lock();
        try {
            if (done.isDone()) {
                return;
            }
            cancelled = true;
            outstanding.clear();
            note("cancelled");
            done.completeExceptionally(new java.util.concurrent.CancellationException("tree fetch cancelled"));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<String> trace() {
        lock.lock();
        try {
            return List.copyOf(trace);
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------ driving

    /** Whether this session sent the request. */
    public boolean owns(int req) {
        lock.lock();
        try {
            return outstanding.containsKey(req);
        } finally {
            lock.unlock();
        }
    }

    /** Whether this is a bare node batch rather than a tree fetch. */
    public boolean batch() {
        return root == null;
    }

    /** Whether the fetch has ended. */
    public boolean isDone() {
        return done.isDone();
    }

    /** The holders this fetch draws from now. */
    public List<PeerId> sourceIds() {
        lock.lock();
        try {
            return List.copyOf(sources.keySet());
        } finally {
            lock.unlock();
        }
    }

    /** Marks the holdings as changed; lock-free, as the space fires it inline from a replication delta. */
    public void holdingsChanged() {
        holdingsDirty = true;
    }

    /** Reads the store's state and sends the first requests. */
    public void start() {
        lock.lock();
        try {
            if (started || done.isDone()) {
                return;
            }
            started = true;
            if (root != null) {
                if (store.complete(root)) {
                    note("start: the store already holds " + root + " complete");
                    finish();
                    return;
                }
                expected.add(root);
                if (store.has(root)) {
                    frontier.addAll(store.missing(root));
                    note("start: root held, " + frontier.size() + " nodes missing");
                } else {
                    frontier.add(root);
                    note("start: root " + root + " absent, " + haves.size() + " haves");
                }
            } else {
                for (String cid : wanted) {
                    expected.add(cid);
                    if (!store.has(cid)) {
                        frontier.add(cid);
                    }
                }
                note("start: batch of " + wanted.size() + " nodes, " + frontier.size() + " missing");
                if (frontier.isEmpty()) {
                    finish();
                    return;
                }
            }
            holdingsDirty = true;
            refreshSources(fabric.nowMillis());
            schedule();
        } finally {
            lock.unlock();
        }
    }

    /** Called from the group tick: time out requests, re-read holdings, schedule, judge the wait. */
    public void tick() {
        lock.lock();
        try {
            if (done.isDone() || !started) {
                return;
            }
            long now = fabric.nowMillis();
            timeOutRequests(now);
            if (holdingsDirty || now - holdingsReadAtMillis >= HOLDINGS_REREAD_MILLIS) {
                refreshSources(now);
            }
            if (budgetSpent && fabric.withinDownloadBudget(0)) {
                budgetSpent = false;
                note("download budget available again; resuming (PD-P-02)");
            }
            schedule();
            if (waiting && !options.timeout().isZero() && !options.timeout().isNegative()
                    && now - lastSourceSeenAtMillis > options.timeout().toMillis()) {
                note("no reachable tree holder for " + options.timeout() + "; failed (PD-X-08)");
                done.completeExceptionally(new IllegalStateException("no reachable holder of tree " + root()
                        + " after " + options.timeout()));
            }
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------ messages

    /**
     * A {@code NODES} answer.
     *
     * @return false when the answer was not solicited by this session, so the capability counts it
     */
    public boolean onNodes(PeerId from, Messages.Nodes nodes) {
        lock.lock();
        try {
            Request request = outstanding.get(nodes.req());
            if (request == null || !request.source.id.equals(from)) {
                return false;
            }
            if (done.isDone()) {
                return true;
            }
            Source source = request.source;
            long now = fabric.nowMillis();
            Optional<List<byte[]>> opened = fabric.open(from, nodes.req(), nodes);
            if (opened.isEmpty()) {
                note("unopenable NODES from " + source.id.display() + " (epoch " + nodes.epoch() + "); request "
                        + request.req + " released");
                releaseUnusable(request);
                if (source.unopenable()) {
                    releaseAll(source);
                    holdingsDirty = true;
                }
                schedule();
                return true;
            }
            List<byte[]> list = opened.get();
            long bytes = 0;
            for (byte[] node : list) {
                bytes += node.length;
            }
            List<String> missing = nodes.missing() == null ? List.of() : nodes.missing();
            boolean last;
            if (request.kind == Messages.NODE_WANT) {
                // only CIDs of the request may be reported missing; the rest says nothing about the source
                List<String> asked = new ArrayList<>();
                for (String cid : missing) {
                    if (request.cids.contains(cid)) {
                        asked.add(cid);
                    }
                }
                missing = asked;
                source.lacks.addAll(missing);
                last = acceptBatch(request, list, missing);
            } else {
                last = false;
                for (byte[] node : list) {
                    if (!acceptSubtreeNode(request, node)) {
                        break;
                    }
                }
                if (outstanding.containsKey(request.req)) {
                    int added = admitFrontier(request, missing, "missing");
                    if (added < 0) {
                        schedule();
                        return true;
                    }
                    if (!missing.isEmpty()) {
                        note("subtree " + request.subtree + " from " + source.id.display() + ": " + missing.size()
                                + " missing at that holder, " + added + " more to ask elsewhere");
                    }
                }
                source.lacks.addAll(missing);
            }
            if (outstanding.containsKey(request.req)) {
                if (last) {
                    outstanding.remove(request.req);
                    askedNodes.removeAll(request.cids);
                    source.answered(now - request.lastActivityMillis, bytes);
                } else {
                    source.progressed(now - request.lastActivityMillis, bytes);
                    request.lastActivityMillis = now;
                }
                source.grow(maxWindow);
            }
            if (bytes > 0 && !fabric.withinDownloadBudget(bytes) && !budgetSpent) {
                budgetSpent = true;
                note("download budget spent after " + bytesReceived + " bytes; sending nothing more today (PD-P-02)");
            }
            checkComplete();
            schedule();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * An answer the capability dropped for a violated bound: the request is released
     * so a later part of the same stream cannot strike its sender (PD-X-04).
     *
     * @return false when the request is not this session's
     */
    public boolean onUnusableNodes(PeerId from, int req) {
        lock.lock();
        try {
            Request request = outstanding.get(req);
            if (request == null || !request.source.id.equals(from)) {
                return false;
            }
            if (done.isDone()) {
                return true;
            }
            note("NODES for request " + req + " from " + request.source.id.display() + " dropped; request released");
            releaseUnusable(request);
            schedule();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** Takes a request whose answer cannot be used out of flight and queues its work again. */
    private void releaseUnusable(Request request) {
        release(request);
        request.source.inFlight = Math.max(0, request.source.inFlight - 1);
    }

    /**
     * Admits CIDs a holder named (a TREE_END frontier, or the missing list of a
     * TREE_WANT answer) into the frontier when they are the subtree root or a link
     * of a verified node (PD-X-04); a holder that names anything else is struck.
     *
     * @return the CIDs added to the frontier, or -1 when the source was struck
     */
    private int admitFrontier(Request request, List<String> cids, String what) {
        int added = 0;
        for (String cid : cids) {
            if (!expected.contains(cid)) {
                foreignCidsRefused++;
                strike(request.source, "the " + what + " of TREE_WANT " + request.req + " under " + request.subtree
                        + " names " + cid + ", which no verified node links to");
                return -1;
            }
            generationOf.putIfAbsent(cid, request.generation);
            if (!store.has(cid) && !verified.contains(cid) && frontier.add(cid)) {
                added++;
            }
        }
        return added;
    }

    /** Frontier or missing CIDs refused because no verified node linked to them (PD-X-04). */
    public long foreignCidsRefused() {
        lock.lock();
        try {
            return foreignCidsRefused;
        } finally {
            lock.unlock();
        }
    }

    /**
     * A {@code TREE_END}.
     *
     * @return false when it closes no request of this session
     */
    public boolean onTreeEnd(PeerId from, Messages.TreeEnd end) {
        lock.lock();
        try {
            Request request = outstanding.get(end.req());
            if (request == null || request.kind != Messages.TREE_WANT || !request.source.id.equals(from)) {
                return false;
            }
            if (done.isDone()) {
                return true;
            }
            outstanding.remove(end.req());
            inFlightSubtrees.remove(request.subtree);
            Source source = request.source;
            long now = fabric.nowMillis();
            source.answered(now - request.lastActivityMillis, 0);
            int added = admitFrontier(request, end.frontier(), "frontier");
            boolean struck = added < 0;
            added = Math.max(0, added);
            for (String cid : store.missing(request.subtree)) {
                generationOf.putIfAbsent(cid, request.generation);
                if (frontier.add(cid)) {
                    added++;
                }
            }
            if (struck) {
                schedule();
                return true;
            }
            if (end.reason() == Messages.END_HAVES_NOT_HELD) {
                source.lacks.addAll(haves);
            }
            note("subtree " + request.subtree + " from " + source.id.display() + " ended (" + end.reason() + "): "
                    + end.frontier().size() + " frontier cids"
                    + (end.reason() == Messages.END_DONE ? " after pruning under the haves" : "")
                    + ", " + added + " more to ask for");
            checkComplete();
            schedule();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * A {@code REJECT} with {@link Messages#TREE_INDEX}.
     *
     * @return false when it matches no request of this session to that peer
     */
    public boolean onReject(PeerId from, Messages.Reject reject) {
        lock.lock();
        try {
            Source source = sources.get(from);
            if (source == null) {
                return false;
            }
            Request matched = null;
            for (Request r : outstanding.values()) {
                if (r.source == source && (reject.root().equals(r.subtree)
                        || (r.cids != null && !r.cids.isEmpty() && reject.root().equals(r.cids.get(0))))) {
                    matched = r;
                    break;
                }
            }
            if (matched == null) {
                return false;
            }
            if (done.isDone()) {
                return true;
            }
            release(matched);
            source.inFlight = Math.max(0, source.inFlight - 1);
            long now = fabric.nowMillis();
            if (reject.reason() == Messages.REASON_OVER_BUDGET || reject.reason() == Messages.REASON_POLICY) {
                source.backOff(now);
                note("source " + source.id.display() + " rejected (" + reject.reason() + "); dropped for a while");
                releaseAll(source);
            } else {
                source.lacks.add(reject.root());
                holdingsDirty = true;
            }
            schedule();
            return true;
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------ verification

    /** Verifies a NODE_WANT answer's nodes against the request's CIDs in order; true when the request is answered in full. */
    private boolean acceptBatch(Request request, List<byte[]> list, List<String> missing) {
        Source source = request.source;
        for (byte[] node : list) {
            byte[] digest = Digests.sha256(node);
            int at = -1;
            for (int i = request.matched; i < request.cids.size(); i++) {
                if (digestOf(request.cids.get(i)).map(d -> java.util.Arrays.equals(d, digest)).orElse(false)) {
                    at = i;
                    break;
                }
            }
            if (at < 0) {
                strike(source, "a node of NODE_WANT " + request.req + " matches none of the cids asked for");
                return false;
            }
            for (int i = request.matched; i < at; i++) {
                source.lacks.add(request.cids.get(i)); // skipped: the holder lacks it or lists it missing
            }
            request.matched = at + 1;
            accept(request.cids.get(at), node, source, request.subtree, request.generation);
        }
        int lacking = 0;
        for (String cid : request.cids) {
            if (missing.contains(cid)) {
                lacking++;
            }
        }
        return request.matched + lacking >= request.cids.size() || !missing.isEmpty();
    }

    /** Verifies a TREE_WANT answer's node against the expected set; false after a strike. */
    private boolean acceptSubtreeNode(Request request, byte[] node) {
        byte[] digest = Digests.sha256(node);
        String structured = Cids.ofDigest(digest, Cids.DAG_CBOR);
        String raw = Cids.ofDigest(digest, Cids.RAW);
        String cid = expected.contains(structured) ? structured : expected.contains(raw) ? raw : null;
        if (cid == null) {
            strike(request.source, "a node of TREE_WANT " + request.req + " under " + request.subtree
                    + " is neither the subtree root nor a link of a verified node");
            return false;
        }
        accept(cid, node, request.source, request.subtree, request.generation);
        return true;
    }

    private void accept(String cid, byte[] node, Source source, String subtree, int generation) {
        frontier.remove(cid);
        bytesReceived += node.length; // every delivery that verified, a duplicate included, so the cost is visible
        if (!verified.add(cid)) {
            return;
        }
        if (!store.has(cid)) {
            store.putVerified(cid, node);
        }
        nodesVerified++;
        source.bytesVerified += node.length;
        fabric.onNodesVerified(1, node.length);
        if (root == null) {
            return; // a bare batch follows no links
        }
        List<String> links;
        try {
            links = store.links(cid, node);
        } catch (IllegalArgumentException e) {
            links = List.of();
        }
        String under = subtree != null ? subtree : subtreeOf.get(cid);
        for (String link : links) {
            expected.add(link);
            generationOf.putIfAbsent(link, generation);
            if (under != null) {
                subtreeOf.put(link, under);
            }
            if (!store.has(link) && !verified.contains(link)) {
                frontier.add(link);
            }
        }
    }

    private void strike(Source source, String why) {
        note("struck " + source.id.display() + ": " + why);
        fabric.strike(source.id, "tree exchange: " + why);
        releaseAll(source);
        source.dropForever();
        holdingsDirty = true;
    }

    private static Optional<byte[]> digestOf(String cid) {
        try {
            return Optional.of(Cids.parse(cid).digest());
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    // ------------------------------------------------------------ scheduling

    private void refreshSources(long now) {
        holdingsDirty = false;
        holdingsReadAtMillis = now;
        Set<PeerId> seen = new LinkedHashSet<>();
        List<String> lookups = new ArrayList<>();
        if (root != null) {
            lookups.add(root);
        } else {
            lookups.addAll(wanted);
        }
        lookups.addAll(haves);
        for (String r : lookups) {
            for (Holding h : fabric.holdings(r)) {
                seen.add(h.holder());
            }
        }
        seen.addAll(options.prefer());
        Set<PeerId> usable = new HashSet<>();
        for (PeerId id : seen) {
            if (id.equals(fabric.self()) || options.avoid().contains(id) || !fabric.advertisesTrees(id)) {
                continue;
            }
            usable.add(id);
            int hops = fabric.hopsTo(id);
            Source existing = sources.get(id);
            if (existing != null) {
                existing.hops = hops;
            } else if (sources.size() < maxSessions) {
                Source added = new Source(id);
                added.hops = hops;
                sources.put(id, added);
                note("source " + id.display() + (hops > 0 ? " (" + hops + " relay hop" + (hops > 1 ? "s" : "") + ")" : ""));
            }
        }
        sources.values().removeIf(s -> !usable.contains(s.id) && s.inFlight == 0);
    }

    private void timeOutRequests(long now) {
        List<Request> expired = new ArrayList<>();
        for (Request r : outstanding.values()) {
            if (r.source.timedOut(r.lastActivityMillis, now)) {
                expired.add(r);
            }
        }
        for (Request r : expired) {
            release(r);
            if (r.source.timedOut(now)) {
                note("source " + r.source.id.display() + " timed out " + SourceRanking.TIMEOUTS_BEFORE_DROP
                        + " times; dropped for 60 s");
                releaseAll(r.source);
                holdingsDirty = true;
            }
        }
    }

    /**
     * Takes a request out of flight and puts its work back on the queues. A subtree
     * walk that delivered part of its subtree before its source died is not walked
     * again from another holder (that would ship the delivered part twice): the
     * rest goes out as {@code NODE_WANT} batches from the store's own view, which
     * is the PD-X-06 continuation a {@code TREE_END} would have started.
     */
    private void release(Request r) {
        if (outstanding.remove(r.req) == null) {
            return;
        }
        if (r.kind == Messages.TREE_WANT) {
            inFlightSubtrees.remove(r.subtree);
            if (store.complete(r.subtree)) {
                return;
            }
            if (store.has(r.subtree)) {
                for (String cid : store.missing(r.subtree)) {
                    generationOf.putIfAbsent(cid, r.generation);
                    frontier.add(cid);
                }
            } else if (!pendingSubtrees.contains(r.subtree)) {
                pendingSubtrees.addFirst(r.subtree);
            }
        } else {
            askedNodes.removeAll(r.cids);
        }
    }

    private void releaseAll(Source source) {
        for (Request r : List.copyOf(outstanding.values())) {
            if (r.source == source) {
                release(r);
            }
        }
        source.inFlight = 0;
    }

    private void schedule() {
        if (scheduling) {
            rescheduleRequested = true;
            return;
        }
        scheduling = true;
        try {
            do {
                rescheduleRequested = false;
                scheduleOnce();
            } while (rescheduleRequested && !done.isDone());
        } finally {
            scheduling = false;
        }
    }

    private void scheduleOnce() {
        if (done.isDone() || !started) {
            return;
        }
        long now = fabric.nowMillis();
        List<Source> order = ranking.order(sources.values());
        List<Source> usable = new ArrayList<>();
        for (Source s : order) {
            if (!s.dropped(now)) {
                usable.add(s);
            }
        }
        waiting = usable.isEmpty();
        if (waiting) {
            return;
        }
        if (budgetSpent) {
            lastSourceSeenAtMillis = now; // a source exists; the policy, not the swarm, holds the fetch
            return;
        }
        if (root != null && !store.has(root)) {
            if (!askedNodes.contains(root)) {
                for (Source s : usable) {
                    if (s.inFlight < s.window && !s.lacks.contains(root)) {
                        askedNodes.add(root); // once per source: a tick before the answer must not ask again
                        send(s, Messages.NODE_WANT, List.of(root), null, now);
                        break;
                    }
                }
            }
            if (outstanding.isEmpty() && allLack(usable, root)) {
                stalled("the root");
            } else {
                lastSourceSeenAtMillis = now;
            }
            return;
        }
        if (root != null && !treePlanned) {
            planSubtrees();
        }
        // subtrees across the top two sources; a subtree every top source lacks stays queued for a new source
        List<Source> top = usable.subList(0, Math.min(SUBTREE_SOURCES, usable.size()));
        int turn = 0;
        for (String subtree : List.copyOf(pendingSubtrees)) {
            Source chosen = null;
            for (int i = 0; i < top.size() && chosen == null; i++) {
                Source s = top.get((turn + i) % top.size());
                if (s.inFlight < s.window && !s.lacks.contains(subtree)) {
                    chosen = s;
                }
            }
            if (chosen == null) {
                continue;
            }
            pendingSubtrees.remove(subtree);
            inFlightSubtrees.add(subtree);
            subtreeOf.put(subtree, subtree);
            expected.add(subtree);
            send(chosen, Messages.TREE_WANT, null, subtree, now);
            turn++;
        }
        // the frontier in batches across every source
        List<String> candidates = new ArrayList<>();
        for (String cid : frontier) {
            if (askedNodes.contains(cid)) {
                continue;
            }
            String under = subtreeOf.get(cid);
            if (under != null && inFlightSubtrees.contains(under)) {
                continue;
            }
            candidates.add(cid);
        }
        for (Source s : usable) {
            while (s.inFlight < s.window && !candidates.isEmpty()) {
                List<String> batch = new ArrayList<>();
                for (java.util.Iterator<String> it = candidates.iterator(); it.hasNext() && batch.size() < Messages.MAX_WANT_CIDS; ) {
                    String cid = it.next();
                    if (!s.lacks.contains(cid)) {
                        batch.add(cid);
                        it.remove();
                    }
                }
                if (batch.isEmpty()) {
                    break;
                }
                askedNodes.addAll(batch);
                send(s, Messages.NODE_WANT, batch, null, now);
                if (rescheduleRequested) {
                    return;
                }
            }
            if (!frontier.isEmpty()) {
                s.sampleFill();
            }
        }
        if (outstanding.isEmpty() && (!candidates.isEmpty() || !pendingSubtrees.isEmpty())) {
            // nothing in flight and nothing any usable source will answer: no reachable holder of what is left
            if (store.complete(root)) {
                finish();
                return;
            }
            stalled(candidates.size() + " frontier cids and " + pendingSubtrees.size() + " subtrees");
            return;
        }
        lastSourceSeenAtMillis = now;
        if (candidates.isEmpty() && pendingSubtrees.isEmpty() && inFlightSubtrees.isEmpty()) {
            endgame(usable, now);
        }
    }

    private static boolean allLack(List<Source> usable, String cid) {
        for (Source s : usable) {
            if (!s.lacks.contains(cid)) {
                return false;
            }
        }
        return true;
    }

    /** Every usable source has answered missing for what is left: the fetch waits as if it had no source (PD-X-08). */
    private void stalled(String what) {
        if (!waiting) {
            note("every source lacks " + what + "; waiting for another holder (PD-X-08)");
        }
        waiting = true;
    }

    /** Reads the root's children and queues a TREE_WANT per incomplete child, or one for the root (S2). */
    private void planSubtrees() {
        treePlanned = true;
        byte[] node = store.get(root).orElse(null);
        if (node == null) {
            treePlanned = false;
            return;
        }
        List<String> children;
        try {
            children = store.links(root, node);
        } catch (IllegalArgumentException e) {
            children = List.of();
        }
        for (String child : children) {
            expected.add(child);
        }
        if (children.size() <= 1) {
            pendingSubtrees.add(root);
        } else {
            for (String child : children) {
                if (!store.complete(child)) {
                    pendingSubtrees.add(child);
                }
            }
        }
        note("plan: " + pendingSubtrees.size() + " subtree walks of " + children.size() + " children");
    }

    /**
     * Duplicates a late outstanding batch to another source once; nothing is
     * cancelled. Runs only when no unasked work is left and no subtree walk is in
     * flight, and a batch is late when it has been out longer than
     * {@link #ENDGAME_LATE_MILLIS} or twice its source's smoothed round trip, so a
     * fetch that is merely busy does not pay for its frontier twice (PD-X-05).
     */
    private void endgame(List<Source> usable, long now) {
        for (Request r : List.copyOf(outstanding.values())) {
            if (r.kind != Messages.NODE_WANT || r.duplicated) {
                continue;
            }
            long late = Math.max(ENDGAME_LATE_MILLIS, 2 * Math.max(0, r.source.srttMillis));
            if (now - r.lastActivityMillis <= late) {
                continue;
            }
            for (Source s : usable) {
                if (s == r.source || s.inFlight >= s.window) {
                    continue;
                }
                List<String> batch = new ArrayList<>();
                for (String cid : r.cids) {
                    if (!store.has(cid) && !s.lacks.contains(cid)) {
                        batch.add(cid);
                    }
                }
                if (batch.isEmpty()) {
                    continue;
                }
                r.duplicated = true;
                note("endgame: batch " + r.req + " duplicated to " + s.id.display());
                send(s, Messages.NODE_WANT, batch, null, now).duplicated = true;
                break;
            }
        }
    }

    private Request send(Source source, int kind, List<String> cids, String subtree, long now) {
        int req = fabric.nextRequestId();
        int generation = 1;
        if (kind == Messages.TREE_WANT) {
            generation += generationOf.getOrDefault(subtree, 0);
        } else {
            for (String cid : cids) {
                generation = Math.max(generation, 1 + generationOf.getOrDefault(cid, 0));
            }
        }
        Request request = new Request(req, source, kind, cids, subtree, generation, now);
        outstanding.put(req, request);
        source.inFlight++;
        requestsSent++;
        roundTrips = Math.max(roundTrips, generation);
        if (kind == Messages.NODE_WANT) {
            note("NODE_WANT " + req + " to " + source.id.display() + ": " + cids.size() + " cids");
            fabric.send(source.id, new Messages.NodeWant(req, cids));
        } else {
            note("TREE_WANT " + req + " to " + source.id.display() + ": subtree " + subtree + ", " + haves.size() + " haves");
            fabric.send(source.id, new Messages.TreeWant(req, subtree, haves, TREE_BUDGET));
        }
        return request;
    }

    private void checkComplete() {
        if (done.isDone()) {
            return;
        }
        if (root == null) {
            for (String cid : wanted) {
                if (!store.has(cid)) {
                    return;
                }
            }
            finish();
            return;
        }
        pendingSubtrees.removeIf(store::complete); // a subtree the batches filled needs no walk
        if (!frontier.isEmpty() || !inFlightSubtrees.isEmpty()) {
            return;
        }
        if (store.complete(root)) {
            finish();
            return;
        }
        // nodes held before this fetch may hide missing children the walk never reached
        frontier.addAll(store.missing(root));
    }

    private void finish() {
        StringBuilder perSource = new StringBuilder();
        for (Source s : sources.values()) {
            perSource.append(perSource.isEmpty() ? "" : ", ").append(s.id.display()).append(' ').append(s.bytesVerified).append(" bytes");
        }
        note("complete: " + nodesVerified + " nodes, " + bytesReceived + " bytes, " + requestsSent + " requests, "
                + roundTrips + " round trips in " + (fabric.nowMillis() - startedAtMillis) + " ms"
                + (perSource.isEmpty() ? "" : "; by source: " + perSource));
        outstanding.clear();
        done.complete(null);
        fabric.onComplete(this);
    }

    private void note(String line) {
        if (trace.size() >= TRACE_CAPACITY) {
            trace.pollFirst();
        }
        trace.addLast(line);
    }
}
