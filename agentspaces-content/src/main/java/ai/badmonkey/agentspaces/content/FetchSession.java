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
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.tree.SourceRanking;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One fetch: the scheduler of srad/large-networks-bulk-data.md §3.4 and group S.
 * Sources are every holder the holdings show; pieces are chosen rarest-first with
 * a random first piece; each source has a window of ranges that grows on success
 * and halves on a timeout; endgame requests the last pieces from everyone and
 * cancels duplicates; a piece that fails its hash strikes its source and goes
 * elsewhere; verified pieces land in the store as they verify and the fetcher's
 * own holding follows them. The capability drives it from the group tick and from
 * the pipe handler; both paths take the session lock, which is reentrant so a
 * synchronous fabric (the test network delivers inline) nests cleanly. The source
 * rules (order, window, timeout, strikes, back-off) live in {@link SourceRanking},
 * which the tree fetch shares (srad/agentspaces-pdt.md PD-X-05).
 */
final class FetchSession implements Fetch {

    /** "Never happened" for a millisecond stamp: far enough in the past that {@code now - NEVER} cannot overflow. */
    static final long NEVER = Long.MIN_VALUE / 4;

    /** The unit all ranges align to; a range is a power-of-two multiple of it, except at a piece's end. */
    static final int RANGE_UNIT = 64 * 1024;

    /** First window per source, and the growth and timeout rules (LB-S-04), as {@link SourceRanking} keeps them. */
    static final int INITIAL_WINDOW = SourceRanking.INITIAL_WINDOW;
    static final long MIN_RANGE_TIMEOUT_MILLIS = SourceRanking.MIN_TIMEOUT_MILLIS;
    static final int RTT_MULTIPLE_FOR_TIMEOUT = SourceRanking.RTT_MULTIPLE_FOR_TIMEOUT;
    static final int SUCCESSES_PER_RANGE_DOUBLING = 8;
    static final int TIMEOUTS_BEFORE_DROP = SourceRanking.TIMEOUTS_BEFORE_DROP;
    static final long SOURCE_DROP_MILLIS = SourceRanking.SOURCE_DROP_MILLIS;
    static final long HOLDINGS_REREAD_MILLIS = 5_000;
    static final int TRACE_CAPACITY = 256;

    /** What the session needs from the capability. */
    interface Fabric {
        /** Opens a complete artifact for reading; a sealed-hashing root is opened to plaintext (LB-G-03). */
        default SeekableByteChannel openArtifact(PieceStore store, String root) throws IOException {
            return store.openArtifact(root);
        }

        PeerId self();
        long nowMillis();
        void send(PeerId to, Object message);
        int nextRequestId();
        List<Holding> holdings(String root);
        void strike(PeerId peer, String reason);
        void onPieceVerified(FetchSession session, int index);

        /** Whether a range of this many bytes may be requested now (LB-P-06); false holds the fetch back. */
        default boolean mayRequest(int bytes) {
            return true;
        }
        void onComplete(FetchSession session);
        Random random();
        /** Opens a sealed range, or returns it unchanged for a plain root; empty when it cannot be opened. */
        Optional<byte[]> open(Manifest manifest, int index, int offset, byte[] bytes);
        /** Whether this peer can open the root's pieces at all. */
        boolean canOpen(Manifest manifest);
        /** Relays between this peer and a holder: 0 direct, -1 unreachable (LB-R-10). */
        int hopsTo(PeerId peer);
    }

    /** A holder of pieces: the shared source state plus its bitfield, range length, retries, and assignments. */
    private static final class Source extends SourceRanking.Source {
        BitSet have;
        int rangeLength = RANGE_UNIT;
        final Deque<int[]> retry = new ArrayDeque<>(); // (index, offset, length)
        /**
         * The pieces this source is filling, by index (LB-Q-PRF-04): several at once, so a window
         * wider than one piece's ranges stays full; bounded so buffered bytes stay within the
         * window's ranges plus one piece (LB-Q-PRF-06).
         */
        final Map<Integer, Assignment> assignments = new LinkedHashMap<>();

        Source(PeerId id, BitSet have) {
            super(id);
            this.have = have;
        }
    }

    private static final class Assignment {
        final int index;
        final int length;
        final byte[] buffer;
        final BitSet unitsDone;
        int nextOffset;

        Assignment(int index, int length) {
            this.index = index;
            this.length = length;
            this.buffer = new byte[length];
            this.unitsDone = new BitSet((length + RANGE_UNIT - 1) / RANGE_UNIT);
        }

        boolean exhausted() {
            return nextOffset >= length;
        }

        boolean done() {
            int units = (length + RANGE_UNIT - 1) / RANGE_UNIT;
            return unitsDone.cardinality() >= units;
        }
    }

    private record Outstanding(Source source, int index, int offset, int length, long sentAtMillis) {
    }

    private final String root;
    private final Fabric fabric;
    private final PieceStore store;
    private final FetchOptions options;
    private final SourceRanking ranking;
    private final int maxSessions;
    private final int maxWindow;
    private final ReentrantLock lock = new ReentrantLock();
    private final CompletableFuture<Void> done = new CompletableFuture<>();
    private final Map<PeerId, Source> sources = new LinkedHashMap<>();
    private boolean downloadBudgetNoted;
    private final Map<Integer, Outstanding> outstanding = new HashMap<>();
    private final Map<Integer, List<Source>> assignedTo = new HashMap<>();
    private final Deque<String> trace = new ArrayDeque<>();
    private final long startedAtMillis;

    private Manifest manifest;
    private byte[] table;
    private BitSet verified = new BitSet();
    private long bytesReceived;
    /** Written without the lock: a space listener sets it inline from another peer's write (see holdingsChanged). */
    private volatile boolean holdingsDirty = true;
    private long holdingsReadAtMillis = NEVER;
    private boolean scheduling;
    private boolean rescheduleRequested;
    private volatile boolean cancelled;
    private boolean waiting;

    FetchSession(String root, Fabric fabric, PieceStore store, FetchOptions options,
                 int maxSessions, int maxWindow) {
        this.root = root;
        this.fabric = fabric;
        this.store = store;
        this.options = options;
        this.ranking = new SourceRanking(options.prefer());
        this.maxSessions = maxSessions;
        this.maxWindow = Math.max(1, maxWindow);
        this.startedAtMillis = fabric.nowMillis();
    }

    // ------------------------------------------------------------ Fetch

    @Override
    public String root() {
        return root;
    }

    @Override
    public Optional<Manifest> manifest() {
        lock.lock();
        try {
            return Optional.ofNullable(manifest);
        } finally {
            lock.unlock();
        }
    }

    /**
     * The time to completion at the verified rate since this session started (LB-S-01): the bytes
     * still missing over the bytes verified per millisecond; -1 before the manifest or the first
     * verified bytes, 0 once complete. Pieces resumed from the store count from the start, which
     * makes an early estimate after a resume optimistic.
     */
    private long etaMillis() {
        if (manifest == null) {
            return -1;
        }
        long remaining = manifest.size() - bytesReceived;
        if (remaining <= 0) {
            return 0;
        }
        long elapsed = fabric.nowMillis() - startedAtMillis;
        if (bytesReceived <= 0 || elapsed <= 0) {
            return -1;
        }
        return (long) Math.ceil(remaining / ((double) bytesReceived / elapsed));
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
            return new Progress(verified.cardinality(), manifest == null ? 0 : manifest.pieces(),
                    bytesReceived, bySource, active, waiting, etaMillis());
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
        } catch (TimeoutException e) {
            return false;
        } catch (java.util.concurrent.ExecutionException e) {
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
            for (Map.Entry<Integer, Outstanding> e : outstanding.entrySet()) {
                fabric.send(e.getValue().source().id, new Messages.Cancel(root, e.getValue().index(), e.getKey()));
            }
            outstanding.clear();
            note("cancelled");
            done.completeExceptionally(new java.util.concurrent.CancellationException("fetch cancelled"));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public SeekableByteChannel open() throws IOException {
        if (!complete()) {
            throw new IOException("fetch of " + root + " is not complete");
        }
        return fabric.openArtifact(store, root);
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

    /** The table, once known. */
    byte[] table() {
        lock.lock();
        try {
            return table;
        } finally {
            lock.unlock();
        }
    }

    boolean hasManifest() {
        lock.lock();
        try {
            return manifest != null && table != null;
        } finally {
            lock.unlock();
        }
    }

    /** Installs the manifest and its verified table, resuming from the store. */
    void start(Manifest manifest, byte[] table) {
        lock.lock();
        try {
            if (this.manifest != null) {
                return;
            }
            this.manifest = manifest;
            this.table = table;
            if (options.expectedSize() > 0 && manifest.size() != options.expectedSize()) {
                note("the manifest states " + manifest.size() + " bytes where " + options.expectedSize() + " were expected");
                done.completeExceptionally(new IllegalArgumentException("the artifact " + root + " is " + manifest.size()
                        + " bytes; the fetch expects " + options.expectedSize()));
                return;
            }
            if (!fabric.canOpen(manifest)) {
                note("sealed root and this peer holds no content key; the fetch cannot proceed");
                done.completeExceptionally(new IllegalStateException("root " + root + " is sealed under a content key this peer does not hold"));
                return;
            }
            // a hash-keyed store marks the pieces it holds under other roots (PDT 2 phase 1, P2-H-02)
            store.open(manifest, table);
            verified = store.have(root);
            verified.clear(manifest.pieces(), Integer.MAX_VALUE);
            for (int i = verified.nextSetBit(0); i >= 0; i = verified.nextSetBit(i + 1)) {
                bytesReceived += manifest.pieceLength(i);
            }
            note("start: " + manifest.pieces() + " pieces of " + manifest.pieceSize() + " bytes, "
                    + verified.cardinality() + " already verified");
            if (verified.cardinality() >= manifest.pieces()) {
                finish();
                return;
            }
            holdingsDirty = true;
            schedule();
        } finally {
            lock.unlock();
        }
    }

    /** Called from the group tick: time out ranges, re-read holdings, schedule, publish. */
    void tick() {
        lock.lock();
        try {
            if (done.isDone() || manifest == null) {
                return;
            }
            long now = fabric.nowMillis();
            timeOutRanges(now);
            if (holdingsDirty || now - holdingsReadAtMillis >= HOLDINGS_REREAD_MILLIS) {
                refreshSources(now);
            }
            schedule();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Marks the holdings as changed. Lock-free on purpose: the space fires this
     * listener inline from a replication delta, which another peer's handler may
     * publish while holding its own session lock and the fabric delivers at once;
     * taking this session's lock here while that peer waits on ours is the
     * deadlock the first asynchronous run found.
     */
    void holdingsChanged() {
        holdingsDirty = true;
    }

    boolean isDone() {
        return done.isDone();
    }

    @Override
    public Map<PeerId, Double> windowFill() {
        lock.lock();
        try {
            Map<PeerId, Double> fill = new LinkedHashMap<>();
            for (Source s : sources.values()) {
                s.fill().ifPresent(mean -> fill.put(s.id, mean));
            }
            return fill;
        } finally {
            lock.unlock();
        }
    }

    /** Ranges requested and not yet answered (LB-Q-PRF-06: what a fetch holds in flight). */
    int inFlightRanges() {
        lock.lock();
        try {
            return outstanding.size();
        } finally {
            lock.unlock();
        }
    }

    /** The holders this fetch draws from now (LB-H-08: a HAVE push goes to them). */
    java.util.List<PeerId> sourceIds() {
        lock.lock();
        try {
            return java.util.List.copyOf(sources.keySet());
        } finally {
            lock.unlock();
        }
    }

    /**
     * A HAVE push from a source (LB-H-08): the pieces it verified since its last holding, added to
     * what this fetch believes it holds, so the scheduler can ask it for them before the next holding
     * refresh. A push from a peer that is not a source changes nothing; the holding is the record.
     */
    void onHave(PeerId from, BitSet have) {
        lock.lock();
        try {
            Source source = sources.get(from);
            if (source == null) {
                return;
            }
            source.have.or(have);
            schedule(); // under the lock, as every other path schedules
        } finally {
            lock.unlock();
        }
    }

    BitSet verifiedPieces() {
        lock.lock();
        try {
            return (BitSet) verified.clone();
        } finally {
            lock.unlock();
        }
    }

    long bytesReceived() {
        return bytesReceived;
    }

    // ------------------------------------------------------------ messages

    void onPiece(PeerId from, Messages.Piece piece) {
        lock.lock();
        try {
            if (done.isDone() || manifest == null) {
                return;
            }
            Outstanding out = outstanding.remove(piece.req());
            if (out == null || !out.source().id.equals(from) || out.index() != piece.index()
                    || out.offset() != piece.offset()) {
                return; // unsolicited or mismatched (LB-X-02)
            }
            Source source = out.source();
            source.answered(fabric.nowMillis() - out.sentAtMillis(), piece.bytes().length);
            Assignment a = source.assignments.get(piece.index());
            if (a == null || verified.get(piece.index())) {
                // a cancelled or completed piece; nothing to keep
                schedule();
                return;
            }
            Optional<byte[]> opened = fabric.open(manifest, piece.index(), piece.offset(), piece.bytes());
            if (opened.isEmpty() || opened.get().length != out.length() || piece.offset() + out.length() > a.length) {
                note((opened.isEmpty() ? "unopenable" : "short or oversized") + " range from " + source.id.display()
                        + " for piece " + a.index);
                source.retry.addFirst(new int[]{a.index, out.offset(), out.length()});
                if (opened.isEmpty() && source.unopenable()) {
                    releaseAll(source);
                }
                schedule();
                return;
            }
            System.arraycopy(opened.get(), 0, a.buffer, out.offset(), out.length());
            int firstUnit = out.offset() / RANGE_UNIT;
            int lastUnit = (out.offset() + out.length() - 1) / RANGE_UNIT;
            a.unitsDone.set(firstUnit, lastUnit + 1);
            grow(source);
            if (a.done()) {
                completePiece(source, a);
            }
            schedule();
        } finally {
            lock.unlock();
        }
    }

    void onReject(PeerId from, Messages.Reject reject) {
        lock.lock();
        try {
            Source source = sources.get(from);
            if (source == null) {
                return;
            }
            Integer req = null;
            for (Map.Entry<Integer, Outstanding> e : outstanding.entrySet()) {
                if (e.getValue().source() == source && e.getValue().index() == reject.index()) {
                    req = e.getKey();
                    break;
                }
            }
            if (req != null) {
                Outstanding out = outstanding.remove(req);
                source.inFlight = Math.max(0, source.inFlight - 1);
                source.retry.addFirst(new int[]{out.index(), out.offset(), out.length()});
            }
            long now = fabric.nowMillis();
            if (reject.reason() == Messages.REASON_OVER_BUDGET || reject.reason() == Messages.REASON_POLICY) {
                source.backOff(now);
                note("source " + source.id.display() + " rejected (" + reject.reason() + "); dropped for a while");
                releaseAll(source);
            } else {
                // not held or unknown: trust the holding less, re-read it soon
                if (source.have != null) {
                    source.have.clear(reject.index());
                }
                releaseAssignment(source, reject.index());
                holdingsDirty = true;
            }
            schedule();
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------ scheduling

    private void refreshSources(long now) {
        holdingsDirty = false;
        holdingsReadAtMillis = now;
        List<Holding> holdings = fabric.holdings(root);
        java.util.Set<PeerId> seen = new java.util.HashSet<>();
        for (Holding h : holdings) {
            PeerId id = h.holder();
            if (id.equals(fabric.self()) || options.avoid().contains(id) || !seen.add(id)) {
                continue;
            }
            BitSet have = h.decodeHave(manifest.pieces());
            Source existing = sources.get(id);
            int hops = fabric.hopsTo(id);
            if (existing != null) {
                existing.have = have;
                existing.hops = hops;
            } else if (sources.size() < maxSessions) {
                Source added = new Source(id, have);
                added.hops = hops;
                sources.put(id, added);
                note("source " + id.display() + " (" + (h.complete() ? "complete" : h.count() + " pieces")
                        + (hops > 0 ? ", " + hops + " relay hop" + (hops > 1 ? "s" : "") : "") + ")");
            }
        }
        // a holder whose holding lapsed stops being a source once it has nothing in flight
        sources.values().removeIf(s -> !seen.contains(s.id) && s.inFlight == 0 && s.assignments.isEmpty());
    }

    private void timeOutRanges(long now) {
        List<Integer> expired = new ArrayList<>();
        for (Map.Entry<Integer, Outstanding> e : outstanding.entrySet()) {
            Outstanding out = e.getValue();
            if (out.source().timedOut(out.sentAtMillis(), now)) {
                expired.add(e.getKey());
            }
        }
        for (Integer req : expired) {
            Outstanding out = outstanding.remove(req);
            Source source = out.source();
            boolean dropped = source.timedOut(now);
            source.rangeLength = Math.max(RANGE_UNIT, source.rangeLength / 2);
            source.retry.addFirst(new int[]{out.index(), out.offset(), out.length()});
            if (dropped) {
                note("source " + source.id.display() + " timed out " + TIMEOUTS_BEFORE_DROP + " times; dropped for 60 s");
                releaseAll(source);
                holdingsDirty = true;
            }
        }
    }

    private void grow(Source source) {
        source.grow(maxWindow);
        if (source.successes % SUCCESSES_PER_RANGE_DOUBLING == 0 && source.rangeLength < manifest.pieceSize()) {
            source.rangeLength = Math.min(manifest.pieceSize(), source.rangeLength * 2);
        }
    }

    /** Runs the scheduler once, re-running while a nested arrival asked for it. */
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
        if (done.isDone() || manifest == null || table == null) {
            return;
        }
        long now = fabric.nowMillis();
        int[] availability = availability();
        boolean endgame = missingUnassigned(availability) == 0;
        // LB-S-05, LB-R-10: preferred, then direct sources first, then by relay hop count, then by throughput
        List<Source> order = ranking.order(sources.values());
        boolean anyUsable = false;
        for (Source source : order) {
            if (source.dropped(now)) {
                continue;
            }
            anyUsable = true;
            // One window's worth of requests per source per pass: a fabric that
            // delivers inline (the test network) would otherwise let the first
            // source satisfy every want before the next source is asked at all.
            int granted = 0;
            while (source.inFlight < source.window && granted < source.window) {
                if (!fabric.mayRequest(source.rangeLength)) {
                    if (!downloadBudgetNoted) {
                        downloadBudgetNoted = true;
                        note("the download budget for today is spent; waiting for the next day;"
                                + " ContentPolicy.downloadBytesPerDay sets the budget");
                    }
                    return;
                }
                int[] range = nextRange(source, availability, endgame);
                if (range == null) {
                    break;
                }
                int req = fabric.nextRequestId();
                outstanding.put(req, new Outstanding(source, range[0], range[1], range[2], now));
                source.inFlight++;
                granted++;
                fabric.send(source.id, new Messages.PieceWant(root, range[0], range[1], range[2], req));
                if (rescheduleRequested) {
                    break; // an inline arrival changed the state; let the next pass redistribute
                }
            }
            BitSet needed = (BitSet) source.have.clone();
            needed.andNot(verified);
            if (!needed.isEmpty()) {
                source.sampleFill();
            }
        }
        waiting = !anyUsable && verified.cardinality() < manifest.pieces();
    }

    private int[] availability() {
        int[] count = new int[manifest.pieces()];
        for (Source s : sources.values()) {
            if (s.have == null) {
                continue;
            }
            for (int i = s.have.nextSetBit(0); i >= 0 && i < count.length; i = s.have.nextSetBit(i + 1)) {
                count[i]++;
            }
        }
        return count;
    }

    private int missingUnassigned(int[] availability) {
        int n = 0;
        for (int i = 0; i < manifest.pieces(); i++) {
            if (!verified.get(i) && !assignedTo.containsKey(i)) {
                n++;
            }
        }
        return n;
    }

    /** The next (index, offset, length) to ask this source for, or null. */
    /** Whether a relayed source is held back: used only for pieces no direct source holds (LB-S-05). */
    private boolean directSourceHolds(int index, Source relayed) {
        for (Source s : sources.values()) {
            if (s != relayed && s.hops == 0 && !s.dropped(fabric.nowMillis()) && s.have != null && s.have.get(index)) {
                return true;
            }
        }
        return false;
    }

    private int[] nextRange(Source source, int[] availability, boolean endgame) {
        while (!source.retry.isEmpty()) {
            int[] r = source.retry.pollFirst();
            if (!verified.get(r[0]) && source.assignments.containsKey(r[0])) {
                return r;
            }
        }
        for (Assignment stale : List.copyOf(source.assignments.values())) {
            if (verified.get(stale.index) || stale.done()) {
                releaseAssignment(source, stale.index);
            }
        }
        Assignment a = null;
        for (Assignment open : source.assignments.values()) {
            if (!open.exhausted()) {
                a = open;
                break;
            }
        }
        if (a == null) {
            // every range of every piece is asked for: open another piece while the bytes buffered
            // for this source stay within its window's ranges plus one piece (LB-Q-PRF-04, -06)
            long buffered = (long) source.assignments.size() * manifest.pieceSize();
            if (!source.assignments.isEmpty() && buffered > (long) source.window * source.rangeLength) {
                return null;
            }
            int index = pickPiece(source, availability, endgame);
            if (index < 0) {
                return null;
            }
            a = new Assignment(index, manifest.pieceLength(index));
            source.assignments.put(index, a);
            assignedTo.computeIfAbsent(index, k -> new ArrayList<>()).add(source);
        }
        int offset = a.nextOffset;
        int length = Math.min(source.rangeLength, a.length - offset);
        a.nextOffset += length;
        return new int[]{a.index, offset, length};
    }

    /** Rarest first among the pieces this source has, random first piece, duplicates only in endgame. */
    private int pickPiece(Source source, int[] availability, boolean endgame) {
        int best = -1;
        int bestCount = Integer.MAX_VALUE;
        int ties = 0;
        boolean cold = verified.isEmpty() && assignedTo.isEmpty();
        for (int i = 0; i < manifest.pieces(); i++) {
            if (verified.get(i) || (source.have != null && !source.have.get(i))) {
                continue;
            }
            List<Source> holders = assignedTo.get(i);
            if (holders != null && (!endgame || holders.contains(source))) {
                continue;
            }
            if (source.hops > 0 && directSourceHolds(i, source)) {
                continue; // a direct source will take it; relayed bytes are the last resort
            }
            int count = cold ? 0 : availability[i];
            if (count < bestCount) {
                bestCount = count;
                best = i;
                ties = 1;
            } else if (count == bestCount && fabric.random().nextInt(++ties) == 0) {
                best = i;
            }
        }
        return best;
    }

    /** Releases one piece a source was filling; its queued retries for that piece go with it. */
    private void releaseAssignment(Source source, int index) {
        Assignment a = source.assignments.remove(index);
        if (a == null) {
            return;
        }
        source.retry.removeIf(r -> r[0] == index);
        List<Source> holders = assignedTo.get(index);
        if (holders != null) {
            holders.remove(source);
            if (holders.isEmpty()) {
                assignedTo.remove(index);
            }
        }
    }

    /** Releases every piece a source was filling (it was dropped or struck). */
    private void releaseAll(Source source) {
        for (Integer index : List.copyOf(source.assignments.keySet())) {
            releaseAssignment(source, index);
        }
        source.retry.clear();
    }

    private void completePiece(Source source, Assignment a) {
        if (ContentRoots.verifies(table, a.index, a.buffer)) {
            store.write(root, a.index, a.buffer);
            verified.set(a.index);
            bytesReceived += a.length;
            source.bytesVerified += a.length;
            List<Source> others = assignedTo.remove(a.index);
            if (others != null) {
                for (Source other : others) {
                    if (other != source) {
                        cancelOutstanding(other, a.index);
                        other.assignments.remove(a.index);
                        other.retry.removeIf(r -> r[0] == a.index);
                    }
                }
            }
            source.assignments.remove(a.index);
            fabric.onPieceVerified(this, a.index);
            if (verified.cardinality() >= manifest.pieces()) {
                finish();
            }
        } else {
            note("piece " + a.index + " from " + source.id.display() + " failed its hash; struck");
            fabric.strike(source.id, "content piece " + a.index + " of " + root + " failed its hash");
            releaseAll(source);
            source.dropForever(); // never again for this fetch (LB-X-03)
            holdingsDirty = true;
        }
    }

    private void cancelOutstanding(Source other, int index) {
        List<Integer> reqs = new ArrayList<>();
        for (Map.Entry<Integer, Outstanding> e : outstanding.entrySet()) {
            if (e.getValue().source() == other && e.getValue().index() == index) {
                reqs.add(e.getKey());
            }
        }
        for (Integer req : reqs) {
            outstanding.remove(req);
            other.inFlight = Math.max(0, other.inFlight - 1);
            fabric.send(other.id, new Messages.Cancel(root, index, req));
        }
    }

    private void finish() {
        note("complete: " + verified.cardinality() + " pieces, " + bytesReceived + " bytes in "
                + (fabric.nowMillis() - startedAtMillis) + " ms");
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
