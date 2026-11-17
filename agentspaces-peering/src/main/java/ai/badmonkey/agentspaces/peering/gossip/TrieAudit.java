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
package ai.badmonkey.agentspaces.peering.gossip;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.peering.wire.Bodies;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * The trie audit of SPEC annex I.6 (v0.1.35; srad/replication-by-entry.md §3.2, RE-T-04, RE-T-05): both halves of
 * the descent for one gossip bus. As the <em>partner</em> it answers a round's opening with the roots of the space
 * streams whose stream hashes differ, keeps the bounded sessions those roots open, and answers request nodes within
 * them. As the <em>asker</em> it compares each answer with its own trie, asks for the children that differ, and
 * fetches by id the entries an answer of lines names that its own set lacks.
 *
 * <p>Everything here reads the clock the bus was given, so a session's idle time and a copy's lateness are judged in
 * the same time as the rest of the node. Replies are built under this object's monitor and sent after it is
 * released, since a synchronous transport delivers the partner's answer on the sending thread.
 *
 * <p>The fetch (annex I.6, <em>The fetch</em>; RE-I-01 to RE-I-03) is the asker's queue of the entry ids its audits
 * found. It sends them in bodies of at most 4,096 ids under its budget, keeps {@link #FETCH_WINDOW} bodies in flight
 * per partner and sends the next as each answer lands, so a join that needs many budgeted answers continues at once.
 * An answer that carries a remainder sends the ids it left out to the same partner again at once; an id the partner
 * did not send and did not count as left out, or whose answer never came, is asked of the next member in view on a
 * later round after a jittered backoff; and an id every member in view left out waits for an audit to name it again.
 * No id is given up for good while an audit can still find it (ROBUSTNESS.md §10).
 */
final class TrieAudit {

    private static final System.Logger LOG = System.getLogger(TrieAudit.class.getName());

    /** Sessions a partner keeps open per asker by default and at most (annex I.6, <em>Sessions</em>). */
    static final int SESSIONS_DEFAULT = 4;
    /** The fewest sessions a member may keep per asker: with none, a large stream would never be audited. */
    static final int SESSIONS_FLOOR = 1;
    /** The most sessions a member may keep per asker; annex I.6 fixes it at four. */
    static final int SESSIONS_CEILING = 4;
    /** Exchanges a session holds at most before it closes. */
    static final int EXCHANGES_MAX = 64;
    /** How long a session lives with no exchange. */
    static final long IDLE_MILLIS = 30_000;
    /** The budget an asker puts on its requests by default: 1 MiB, annex I.6's default. */
    static final long BUDGET_DEFAULT = 1L << 20;
    /** The smallest budget an asker may set: a few nodes' worth, so every exchange still makes progress. */
    static final long BUDGET_FLOOR = 4L << 10;
    /** The largest budget an asker may set, and the cap this member puts on an answer: half the 8 MiB frame. */
    static final long BUDGET_CEILING = 4L << 20;
    /**
     * Fetch bodies an asker keeps in flight per partner. The next goes out as each answer lands, so a join that needs
     * many budgeted answers continues at once rather than one answer per round (RE-Q-PRF-03), and no more than this
     * many answers of one partner's are ever in flight.
     */
    static final int FETCH_WINDOW = 4;
    /** The fewest and the most fetch bodies a member may keep in flight per partner (RE-F-03). */
    static final int FETCH_WINDOW_FLOOR = 1;
    static final int FETCH_WINDOW_CEILING = 16;
    /**
     * The shortest wait for a fetch's answer before its ids are asked of another member. The wait is four times the
     * partner's last answer time when that is longer, up to {@link #LATE_COPY_MILLIS}, so a slow link is waited for
     * rather than asked again and again (decision 19: adjust, never thrash).
     */
    static final long FETCH_WAIT_FLOOR_MILLIS = 5_000;
    /** The bounds of the shortest wait (RE-F-03): a second, and the time a late copy is still taken. */
    static final long FETCH_WAIT_FLOOR_MIN_MILLIS = 1_000;
    /** The backoff before an id a partner left out is asked again: doubled per attempt up to the ceiling, jittered. */
    static final long RETRY_BASE_MILLIS = 1_000;
    /** The longest backoff between two asks of one id. */
    static final long RETRY_CEILING_MILLIS = 30_000;
    /**
     * How long an id that every member in view left out waits for an audit to name it again before it is forgotten.
     * The audit is the liveness path: an id still missing is found again by the next round that differs.
     */
    static final long PARKED_MILLIS = 600_000;
    /** Ids an asker keeps wanted per stream at most; past it the eldest waiting id goes, and a later audit finds it. */
    static final int WANTED_MAX = 1 << 17;
    /**
     * The fewest ids a fetch body names once a partner's answers have been cut by the budget. A body names twice as
     * many ids as the partner's last cut answer carried, from this floor up to 4,096, so a request is never much
     * larger than the answer it draws, and a body never names far more than one answer can hold.
     */
    static final int FETCH_IDS_FLOOR = 64;
    /**
     * How long an asker accepts the answer to a fetch: twice a session's idle time, so a copy that arrives after its
     * session idled is still applied when it is newer (RE-Q-REL-02); the merge judges newer, so an old copy changes
     * nothing.
     */
    static final long LATE_COPY_MILLIS = 2 * IDLE_MILLIS;
    /** Partners whose descents, sessions, openings and fetches this member tracks at most; the least recent go first. */
    static final int PARTNERS_MAX = 64;
    /**
     * Openings a partner's root answers are owed for, at most: two, so that the answers to a round and to the opening a
     * continuation or a probe sent just after it are both read, and no older opening admits a root.
     */
    static final int ROOTS_OWED_MAX = 2;
    /** Distinct (partner, reason) pairs logged once, at most. */
    private static final int LOGGED_MAX = 1024;

    /** The reasons a refusal or a fallback is counted under (RE-Q-MNT-03), one key each. */
    static final List<String> REFUSALS = List.of(
            "undecodable",           // a DIGEST or PULL_RESP that did not decode, a stale card's frame among them
            "lacks-hint",            // audit fields that reached a member not on wire.ae=2
            "request-without-session", // a request node with no session open for its asker and stream
            "session-busy",          // a root that waited, the asker's sessions at their bound
            "session-cooling",       // a root that waited a round after its session hit the exchange bound
            "session-capped",        // a session closed at the exchange bound
            "unsolicited-answer",    // an answer for a path this member did not ask, or from a partner it opened nothing with
            "fingerprint-mismatch",  // an answer whose preimage does not hash to the fingerprint its parent gave
            "shape-mismatch",        // an answer the trie's shape rules out: nodes at 64 nibbles, or counts that do
                                     // not sum to the count its parent gave
            "descent-capped",        // a descent the asker ended at the exchange bound, as the partner's session ends
            "repeated-request",      // a request body that named one path twice; each path is answered once
            "text-fallback");        // a root answered by the text form: one for a stream this member keeps no trie
                                     // for, or one for a round it opened in the text form before it read the card

    private final Map<String, ReconcilableState> states;
    private final GossipBus.FrameSender sender;
    private final CborCodec codec;
    private final LongSupplier clock;

    private volatile java.util.function.Predicate<PeerId> capable;
    private volatile int sessionsMax = SESSIONS_DEFAULT;
    private volatile long budget = BUDGET_DEFAULT;
    private volatile int fetchWindow = FETCH_WINDOW;
    private volatile long fetchWaitFloor = FETCH_WAIT_FLOOR_MILLIS;

    private final Map<String, AtomicLong> refusals = new ConcurrentHashMap<>();
    private final Set<String> logged = java.util.Collections.newSetFromMap(new LinkedHashMap<>(64, 0.75f, false) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return size() > LOGGED_MAX;
        }
    });

    private final AtomicLong roundsAudited = new AtomicLong();
    private final AtomicLong rootsAnswered = new AtomicLong();
    private final AtomicLong requestsAnswered = new AtomicLong();
    private final AtomicLong nodesAnswered = new AtomicLong();
    private final AtomicLong answerBytesSent = new AtomicLong();
    private final AtomicLong requestsSent = new AtomicLong();
    private final AtomicLong answersReceived = new AtomicLong();
    private final AtomicLong answerBytesReceived = new AtomicLong();
    private final AtomicLong idsFetched = new AtomicLong();
    private final AtomicLong fetchesAnswered = new AtomicLong();
    private final AtomicLong sessionsOpened = new AtomicLong();
    private final AtomicLong fetchContinued = new AtomicLong();
    private final AtomicLong fetchRequeued = new AtomicLong();
    private final AtomicLong fetchTimedOut = new AtomicLong();
    private final AtomicLong fetchParked = new AtomicLong();
    private final AtomicLong fetchQueueFull = new AtomicLong();
    private final AtomicLong fingerprintsChecked = new AtomicLong();
    /** The audit's cost and re-queues per stream (RE-Q-MNT-02); only streams this member holds get an entry. */
    private final Map<String, StreamCounters> perStream = new ConcurrentHashMap<>();

    private static final class StreamCounters {
        final AtomicLong rounds = new AtomicLong();
        final AtomicLong bytesSent = new AtomicLong();
        final AtomicLong bytesReceived = new AtomicLong();
        final AtomicLong requeued = new AtomicLong();
    }

    private StreamCounters counters(String stream) {
        return perStream.computeIfAbsent(stream, k -> new StreamCounters());
    }

    /**
     * One stream's audit figures (RE-Q-MNT-02), counters that only grow.
     *
     * @param rounds        audit rounds of the stream, as either side: roots this member answered and roots it took
     * @param bytesSent     preimage bytes of the answers it sent for the stream
     * @param bytesReceived preimage bytes of the answers it accepted for the stream
     * @param requeued      the stream's fetched ids it queued for another member
     */
    record StreamAudit(long rounds, long bytesSent, long bytesReceived, long requeued) {
    }

    StreamAudit streamAudit(String stream) {
        StreamCounters c = perStream.get(stream);
        return c == null ? new StreamAudit(0, 0, 0, 0)
                : new StreamAudit(c.rounds.get(), c.bytesSent.get(), c.bytesReceived.get(), c.requeued.get());
    }

    /** A partner's session for one asker and one stream. */
    private static final class Session {
        long lastMillis;
        int exchanges;
    }

    /** The node at a path as the partner's parent answer gave it: what the answer for the path must hash to. */
    private record Expected(byte[] fingerprint, long count) {
    }

    /** A path as a map key. */
    private record Path(byte[] nibbles) {
        @Override
        public boolean equals(Object o) {
            return o instanceof Path other && Arrays.equals(nibbles, other.nibbles);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(nibbles);
        }
    }

    /**
     * The rounds this member opened with a partner and not yet idled out, and the root answers each stream has taken
     * from them: a partner answers an opening with one root per stream that differs, so each opening admits one root
     * per stream and no more. Under the monitor.
     */
    private static final class Opened {
        long at;
        int openings;
        final Map<String, Integer> roots = new HashMap<>();
    }

    /** A request waiting for the next exchange: the path and the asker's own count there. */
    private record Pending(Path path, long count) {
    }

    /** The asker's descent of one stream with one partner, from a root answer to the end of the round. */
    private static final class Descent {
        final Map<Path, Expected> expected = new HashMap<>();
        final ArrayDeque<Pending> pending = new ArrayDeque<>();
        final Map<Path, Long> inFlight = new LinkedHashMap<>();
        long lastMillis;
        /**
         * Answer bodies this descent accepted, the root's included. The partner closes its session after
         * {@link #EXCHANGES_MAX} exchanges, so the asker ends the descent there too: without its own bound a partner
         * that keeps answering, consistently or not, would hold the asker descending for as long as it liked.
         */
        int exchanges;
    }

    /** An entry id this member wants, and where it stands. Under the monitor. */
    private static final class Wanted {
        /** The member it is ready for or in flight at; null while it waits to be asked again. */
        PeerId at;
        /** The body it is in flight in; null while it is ready or waits. */
        Batch in;
        /** The members that left it out since an audit last named it. */
        final Set<PeerId> asked = new HashSet<>();
        int attempts;
        long dueAt;
        /** When every member in view left it out; zero while it is not parked. */
        long parkedAt;
    }

    /** One fetch body sent: its partner, its stream and ids, and when it went. */
    static final class Batch {
        final PeerId partner;
        final String stream;
        final List<String> ids;
        final long sentAt;
        /** Whether its wait passed and its ids were asked again; its answer is then a late copy. */
        boolean late;
        private Set<String> idSet;

        Batch(PeerId partner, String stream, List<String> ids, long sentAt) {
            this.partner = partner;
            this.stream = stream;
            this.ids = ids;
            this.sentAt = sentAt;
        }

        String stream() {
            return stream;
        }

        boolean names(String id) {
            if (idSet == null) {
                idSet = new HashSet<>(ids);
            }
            return idSet.contains(id);
        }
    }

    // Under this object's monitor.
    private final Map<PeerId, Map<String, Session>> sessions = lru();
    private final Map<PeerId, Set<String>> cooling = lru();
    private final Map<PeerId, Integer> rotation = lru();
    private final Map<PeerId, Map<String, Descent>> descents = lru();
    private final Map<PeerId, Opened> opened = lru();
    // The fetch queue. These are not bounded by eviction, since an id whose partner were evicted would never be asked
    // again: they hold only members this member fetched from, and a member that leaves the view gives its ids back.
    private final Map<String, LinkedHashMap<String, Wanted>> wanted = new HashMap<>();
    private final Map<PeerId, Map<String, LinkedHashSet<String>>> ready = new HashMap<>();
    private final Map<PeerId, ArrayDeque<Batch>> inFlight = new HashMap<>();
    private final Map<PeerId, ArrayDeque<Batch>> late = new HashMap<>();
    private final Map<PeerId, Long> answerMillis = lru();
    /** States the partner's last answer carried when the budget cut it: what sizes the next body. */
    private final Map<PeerId, Integer> answerYield = lru();
    private final java.util.Random random;

    TrieAudit(Map<String, ReconcilableState> states, GossipBus.FrameSender sender, CborCodec codec,
              LongSupplier clock, java.util.Random random) {
        this.states = states;
        this.sender = sender;
        this.codec = codec;
        this.clock = clock;
        this.random = random;
    }

    private static <V> Map<PeerId, V> lru() {
        return new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<PeerId, V> eldest) {
                return size() > PARTNERS_MAX;
            }
        };
    }

    // ---- configuration

    void capable(java.util.function.Predicate<PeerId> which) {
        this.capable = which;
    }

    boolean enabled() {
        return capable != null;
    }

    boolean audits(PeerId peer) {
        java.util.function.Predicate<PeerId> which = capable;
        return which != null && peer != null && which.test(peer);
    }

    void sessionsMax(int max) {
        if (max < SESSIONS_FLOOR || max > SESSIONS_CEILING) {
            throw new IllegalArgumentException("audit-sessions must be from " + SESSIONS_FLOOR + " to "
                    + SESSIONS_CEILING + ", not " + max);
        }
        this.sessionsMax = max;
    }

    int sessionsMax() {
        return sessionsMax;
    }

    void budget(long bytes) {
        if (bytes < BUDGET_FLOOR || bytes > BUDGET_CEILING) {
            throw new IllegalArgumentException("audit-budget must be from " + BUDGET_FLOOR + " to " + BUDGET_CEILING
                    + " bytes, not " + bytes);
        }
        this.budget = bytes;
    }

    long budget() {
        return budget;
    }

    void fetchWindow(int bodies) {
        if (bodies < FETCH_WINDOW_FLOOR || bodies > FETCH_WINDOW_CEILING) {
            throw new IllegalArgumentException("the fetch window must be from " + FETCH_WINDOW_FLOOR + " to "
                    + FETCH_WINDOW_CEILING + " bodies, not " + bodies);
        }
        this.fetchWindow = bodies;
    }

    int fetchWindow() {
        return fetchWindow;
    }

    void fetchWaitFloor(long millis) {
        if (millis < FETCH_WAIT_FLOOR_MIN_MILLIS || millis > LATE_COPY_MILLIS) {
            throw new IllegalArgumentException("the fetch wait must be from " + FETCH_WAIT_FLOOR_MIN_MILLIS + " to "
                    + LATE_COPY_MILLIS + " ms, not " + millis);
        }
        this.fetchWaitFloor = millis;
    }

    long fetchWaitFloor() {
        return fetchWaitFloor;
    }

    // ---- counting

    void refuse(PeerId from, String stream, String reason) {
        refusals.computeIfAbsent(reason, k -> new AtomicLong()).incrementAndGet();
        String key = (from == null ? "?" : from.value()) + "|" + reason;
        boolean first;
        synchronized (logged) {
            first = logged.add(key);
        }
        if (first) {
            LOG.log(System.Logger.Level.INFO, "anti-entropy " + reason + " from "
                    + (from == null ? "?" : from.display()) + (stream == null ? "" : " on " + stream)
                    + "; counted, logged once per member");
        }
    }

    Map<String, Long> refusals() {
        Map<String, Long> out = new TreeMap<>();
        refusals.forEach((k, v) -> out.put(k, v.get()));
        return out;
    }

    /**
     * The audit's own signals, a snapshot of counters that only grow (RE-F-03, RE-Q-MNT-02).
     *
     * @param roundsAudited       openings this member answered with at least one root
     * @param sessionsOpened      sessions it opened as the partner
     * @param rootsAnswered       roots it answered
     * @param requestsAnswered    request bodies it answered within a session
     * @param nodesAnswered       nodes those answers carried
     * @param answerBytesSent     preimage bytes of every answer it sent
     * @param requestsSent        request bodies it sent as the asker
     * @param answersReceived     answer nodes it accepted as the asker
     * @param answerBytesReceived preimage bytes of those answers
     * @param idsFetched          entry ids it asked for in fetch bodies, each ask counted
     * @param fetchesAnswered     fetch bodies it answered
     * @param openSessions        sessions open now, as the partner
     * @param refusals            refusals and fallbacks by reason
     * @param fetchContinued      ids a budgeted answer left out and this member asked of the same partner at once
     * @param fetchRequeued       ids a partner neither sent nor counted as left out, queued for another member
     * @param fetchTimedOut       fetch bodies whose answer did not come within the wait
     * @param fetchParked         ids every member in view left out, left for an audit to name again
     * @param fetchQueueFull      ids the queue's bound turned away or pushed out
     * @param fetchQueued         ids wanted now, in flight or waiting
     * @param fingerprintsChecked fingerprints the asker computed: each answer checked against its parent, and each of
     *                            its own children compared with an answer's
     */
    record Signals(long roundsAudited, long sessionsOpened, long rootsAnswered, long requestsAnswered,
                   long nodesAnswered, long answerBytesSent, long requestsSent, long answersReceived,
                   long answerBytesReceived, long idsFetched, long fetchesAnswered, int openSessions,
                   Map<String, Long> refusals, long fetchContinued, long fetchRequeued, long fetchTimedOut,
                   long fetchParked, long fetchQueueFull, long fetchQueued, long fingerprintsChecked) {
    }

    Signals signals() {
        int open;
        long queued = 0;
        synchronized (this) {
            open = 0;
            for (Map<String, Session> byStream : sessions.values()) {
                open += byStream.size();
            }
            for (Map<String, Wanted> byId : wanted.values()) {
                queued += byId.size();
            }
        }
        return new Signals(roundsAudited.get(), sessionsOpened.get(), rootsAnswered.get(), requestsAnswered.get(),
                nodesAnswered.get(), answerBytesSent.get(), requestsSent.get(), answersReceived.get(),
                answerBytesReceived.get(), idsFetched.get(), fetchesAnswered.get(), open, refusals(),
                fetchContinued.get(), fetchRequeued.get(), fetchTimedOut.get(), fetchParked.get(),
                fetchQueueFull.get(), queued, fingerprintsChecked.get());
    }

    // ---- the asker opens a round

    /** Notes that this member opened a round with a partner, so a root answer from it is expected. */
    synchronized void opening(PeerId partner) {
        long now = clock.getAsLong();
        expire(now);
        Opened o = opened.computeIfAbsent(partner, k -> new Opened());
        o.at = now;
        o.openings++;
    }

    /**
     * Whether a root answer for a stream from a partner answers an opening this member sent, and takes it if so. A
     * root starts the stream's descent afresh with its exchange count at zero, so before this a partner that sent a
     * root in every body kept the asker descending past the 64-exchange bound for as long as the opening lived (the
     * TypeScript adversarial review of 2026-11-15, ADV-TS-2, which Java shared); now an opening admits one root per
     * stream, and a further one is unsolicited. Under the monitor.
     */
    private boolean takeRoot(PeerId from, String stream) {
        Opened o = opened.get(from);
        if (o == null) {
            return false;
        }
        // Only the last openings are owed a root: an opening whose hashes were equal draws none, and its credit must
        // not let a partner send a burst of roots later.
        int taken = Math.max(o.roots.getOrDefault(stream, 0), o.openings - ROOTS_OWED_MAX);
        if (taken >= o.openings) {
            return false;
        }
        o.roots.put(stream, taken + 1);
        return true;
    }

    // ---- the partner answers an opening

    /**
     * The partner's answer to the stream hashes of an opening, for the streams this member audits with the asker: a
     * root answer per stream whose hash differs, unless the stream waits for a later round. Streams whose hashes are
     * equal close their session. The streams are visited from a starting point that moves on each round, so when the
     * asker's sessions are at their bound no stream waits for ever.
     *
     * @param from    the asker
     * @param differs the space streams whose hashes differ, in stream-id order
     * @param equal   the space streams whose hashes are equal
     * @return the root answers by stream; empty when none
     */
    Map<String, List<Bodies.TreeNode>> roots(PeerId from, List<String> differs, List<String> equal) {
        long now = clock.getAsLong();
        Map<String, List<Bodies.TreeNode>> tree = new TreeMap<>();
        List<String> order = new ArrayList<>(differs);
        // The roots are computed before the monitor is taken: a large space's root is a few hashes, but it reads the
        // space's lock, and nothing here should wait on a space while it holds the audit's state.
        Map<String, ReconcilableState.AuditNode> rootNodes = new HashMap<>();
        for (String stream : order) {
            ReconcilableState state = states.get(stream);
            if (state != null) {
                rootNodes.put(stream, state.auditNode(new byte[0]));
            }
        }
        List<String> waited = new ArrayList<>();
        synchronized (this) {
            expire(now);
            Map<String, Session> open = sessions.computeIfAbsent(from, k -> new HashMap<>());
            for (String stream : equal) {
                open.remove(stream); // annex I.6: an opening that shows the hashes equal closes the session
            }
            int start = order.isEmpty() ? 0 : Math.floorMod(rotation.merge(from, 1, Integer::sum), order.size());
            for (int i = 0; i < order.size(); i++) {
                String stream = order.get((start + i) % order.size());
                ReconcilableState.AuditNode root = rootNodes.get(stream);
                if (root == null) {
                    continue;
                }
                if (!root.inner()) {
                    // A root answered by lines finishes the stream for the round and takes no session.
                    open.remove(stream);
                    tree.put(stream, List.of(Bodies.TreeNode.lines(new byte[0], text(root.preimage()))));
                    continue;
                }
                Session session = open.get(stream);
                if (session == null) {
                    Set<String> cool = cooling.get(from);
                    if (cool != null && cool.remove(stream)) {
                        waited.add(stream + "|session-cooling"); // RE-Q-REL-09: a capped session waits a full round
                        continue;
                    }
                    if (open.size() >= sessionsMax) {
                        waited.add(stream + "|session-busy");
                        continue;
                    }
                    session = new Session();
                    open.put(stream, session);
                    sessionsOpened.incrementAndGet();
                }
                // A reopening reuses the session and counts toward its bound, so reopening never resets it.
                session.lastMillis = now;
                session.exchanges++;
                if (session.exchanges >= EXCHANGES_MAX) {
                    capped(from, stream, open);
                }
                tree.put(stream, List.of(Bodies.TreeNode.inner(new byte[0], root.preimage())));
            }
            if (open.isEmpty()) {
                sessions.remove(from);
            }
        }
        for (String note : waited) {
            int bar = note.indexOf('|');
            refuse(from, note.substring(0, bar), note.substring(bar + 1));
        }
        if (!tree.isEmpty()) {
            roundsAudited.incrementAndGet();
            rootsAnswered.addAndGet(tree.size());
            for (Map.Entry<String, List<Bodies.TreeNode>> e : tree.entrySet()) {
                long bytes = preimageBytes(e.getValue());
                answerBytesSent.addAndGet(bytes);
                StreamCounters c = counters(e.getKey());
                c.rounds.incrementAndGet();
                c.bytesSent.addAndGet(bytes);
            }
        }
        return tree;
    }

    /** Closes a session at the exchange bound and makes its stream wait a full round. Under the monitor. */
    private void capped(PeerId from, String stream, Map<String, Session> open) {
        open.remove(stream);
        cooling.computeIfAbsent(from, k -> new HashSet<>()).add(stream);
        refusals.computeIfAbsent("session-capped", k -> new AtomicLong()).incrementAndGet();
    }

    /** Forgets every session idle for {@link #IDLE_MILLIS} and every descent as long untouched. Under the monitor. */
    private void expire(long now) {
        sessions.values().forEach(byStream -> byStream.values().removeIf(s -> now - s.lastMillis >= IDLE_MILLIS));
        sessions.values().removeIf(Map::isEmpty);
        descents.values().forEach(byStream -> byStream.values().removeIf(d -> now - d.lastMillis >= IDLE_MILLIS));
        descents.values().removeIf(Map::isEmpty);
        opened.values().removeIf(o -> now - o.at >= IDLE_MILLIS);
    }

    // ---- a body's tree

    /**
     * Handles the {@code tree} of a body: request nodes as the partner, answer nodes as the asker. Each stream is
     * handled on its own; a reply goes out once the whole body has been read.
     *
     * @param from      the sender
     * @param tree      the nodes by stream
     * @param budget    the body's budget, or null
     * @param textRound whether this member's round open with the sender went out in the hashed text form, since it
     *                  had not yet read the sender's card as one on {@code wire.ae=2}
     * @return the streams of a round this member opened whose root came back although this member keeps no trie for
     *         them, or although it opened the round in the text form; the caller falls back to the text form for
     *         those, as it answers a {@code want}, so such a stream still converges within the round
     */
    List<String> onTree(PeerId from, Map<String, List<Bodies.TreeNode>> tree, Long budget, boolean textRound) {
        Map<String, List<Bodies.TreeNode>> answers = new TreeMap<>();
        Map<String, List<Bodies.TreeNode>> requests = new TreeMap<>();
        Map<String, List<String>> fetch = new TreeMap<>();
        List<String> fallback = new ArrayList<>();
        for (Map.Entry<String, List<Bodies.TreeNode>> e : new TreeMap<>(tree).entrySet()) {
            String stream = e.getKey();
            ReconcilableState state = states.get(stream);
            if (state == null) {
                continue; // a stream this member does not hold says nothing to it
            }
            if (state.streamHash() == null) {
                // The partner audits the stream and this member's state for it keeps the text digest alone, so the
                // opening's hashes were of two kinds and could never match: answer a root with the text form.
                boolean root = e.getValue().stream().anyMatch(n -> n.count() == null && n.path().length == 0);
                boolean ours;
                synchronized (this) {
                    ours = opened.containsKey(from);
                }
                if (root && ours) {
                    fallback.add(stream);
                }
                continue;
            }
            boolean auditOpen = false;
            if (textRound) {
                synchronized (this) {
                    auditOpen = opened.containsKey(from);
                }
            }
            List<Bodies.TreeNode> asked = new ArrayList<>();
            List<Bodies.TreeNode> answered = new ArrayList<>();
            for (Bodies.TreeNode node : e.getValue()) {
                if (textRound && !auditOpen && node.count() == null && node.path().length == 0) {
                    // The sender read this member's card as one on wire.ae=2 before this member read the sender's,
                    // so it compared this member's text hash with its trie and answered a root. The root says only
                    // that the stream differs, which is what a want says: answered by the text form, as a want is.
                    if (!fallback.contains(stream)) {
                        fallback.add(stream);
                    }
                    continue;
                }
                (node.count() != null ? asked : answered).add(node);
            }
            if (!asked.isEmpty()) {
                List<Bodies.TreeNode> reply = answerRequests(from, stream, state, asked, budget);
                if (reply != null) {
                    answers.put(stream, reply);
                }
            }
            if (!answered.isEmpty()) {
                descend(from, stream, state, answered, requests, fetch);
            }
        }
        if (!answers.isEmpty()) {
            sender.sendDigest(from, new Bodies.Digest(null, null, null, answers, null, null, null));
        }
        if (!requests.isEmpty()) {
            requestsSent.incrementAndGet();
            sender.sendDigest(from, new Bodies.Digest(null, null, null, requests, null, budget(), null));
        }
        for (Map.Entry<String, List<String>> e : fetch.entrySet()) {
            want(from, e.getKey(), e.getValue());
        }
        for (String stream : fallback) {
            refuse(from, stream, "text-fallback");
        }
        return fallback;
    }

    /**
     * The partner's answer to request nodes, within the session open for the asker and the stream: each node in the
     * order asked, {@code lines} when its own count or the request's is at most 32 and {@code nodes} otherwise, up to
     * the budget. Null when no session is open, which drops and counts the requests.
     */
    private List<Bodies.TreeNode> answerRequests(PeerId from, String stream, ReconcilableState state,
                                                 List<Bodies.TreeNode> asked, Long askedBudget) {
        long now = clock.getAsLong();
        boolean inSession;
        synchronized (this) {
            expire(now);
            Map<String, Session> open = sessions.get(from);
            Session session = open == null ? null : open.get(stream);
            inSession = session != null;
            if (inSession) {
                session.lastMillis = now;
                session.exchanges++;
                if (session.exchanges >= EXCHANGES_MAX) {
                    capped(from, stream, open); // this exchange is answered; a later one finds the session closed
                    if (open.isEmpty()) {
                        sessions.remove(from);
                    }
                }
            }
        }
        if (!inSession) {
            refuse(from, stream, "request-without-session");
            return null;
        }
        long cap = askedBudget == null ? BUDGET_CEILING : Math.min(askedBudget, BUDGET_CEILING);
        // The body's own bytes beside its nodes: the map, the stream name, the list head and the other fields.
        long used = 64 + stream.length();
        List<Bodies.TreeNode> reply = new ArrayList<>(asked.size());
        // An honest asker names each path at most once per session (annex I.6, step 4), so a body that names one twice
        // is answered once for it: otherwise each copy would cost a walk under the path, and one small body could ask
        // for hundreds of walks of the whole space.
        Set<Path> seen = new HashSet<>();
        boolean repeated = false;
        for (Bodies.TreeNode request : asked) {
            byte[] path = request.path();
            if (!seen.add(new Path(path))) {
                repeated = true;
                continue;
            }
            ReconcilableState.AuditNode mine = state.auditNode(path);
            Bodies.TreeNode answer;
            if (mine.count() == 0) {
                answer = Bodies.TreeNode.lines(path, "");
            } else if (!mine.inner()) {
                answer = Bodies.TreeNode.lines(path, text(mine.preimage()));
            } else if (request.count() <= Bodies.TreeNode.LEAF_MAX && mine.count() * 4 <= cap) {
                // The asker's own count chose lines: every line under the path, unless they alone pass the budget,
                // in which case nodes let the asker descend further (annex I.6, step 4). Every slot holds a line of
                // at least four bytes, so a count past a quarter of the budget passes it without being read; below
                // that, the build stops as soon as the lines pass the budget, so lines that are not sent are never
                // all gathered and sorted.
                byte[] lines = state.auditLines(path, cap - 32L);
                answer = lines == null ? Bodies.TreeNode.inner(path, mine.preimage())
                        : Bodies.TreeNode.lines(path, text(lines));
            } else {
                answer = Bodies.TreeNode.inner(path, mine.preimage());
            }
            long size = encodedSize(answer);
            if (!reply.isEmpty() && used + size > cap) {
                break; // the rest are left out for the budget; the asker asks them again in the next exchange
            }
            used += size;
            reply.add(answer);
        }
        if (repeated) {
            refuse(from, stream, "repeated-request");
        }
        requestsAnswered.incrementAndGet();
        nodesAnswered.addAndGet(reply.size());
        long bytes = preimageBytes(reply);
        answerBytesSent.addAndGet(bytes);
        counters(stream).bytesSent.addAndGet(bytes);
        return reply;
    }

    /** A child whose fingerprint or count differs from this member's own: asked for in a later exchange. */
    private record Differs(Path path, Expected expected, long ownCount) {
    }

    /**
     * The asker's side of one stream's answers: check each against what its parent gave, compare it with this
     * member's own node, queue the children that differ, and collect the ids that an answer of lines names and this
     * member's set lacks. The next requests go into {@code requests}, at most 256 per stream; what does not fit waits
     * for the next exchange. The audit's monitor is never held while the space's trie is read, since a space may call
     * into the bus under its own lock.
     */
    private void descend(PeerId from, String stream, ReconcilableState state, List<Bodies.TreeNode> answered,
                         Map<String, List<Bodies.TreeNode>> requests, Map<String, List<String>> fetch) {
        long now = clock.getAsLong();
        List<String> refused = new ArrayList<>();
        List<Bodies.TreeNode> accepted = new ArrayList<>();
        Descent descent;
        boolean exchanged = false;
        // First, under the monitor: which answers this member asked for, and whether each hashes to what its parent gave.
        synchronized (this) {
            expire(now);
            Map<String, Descent> byStream = descents.computeIfAbsent(from, k -> new HashMap<>());
            descent = byStream.get(stream);
            for (Bodies.TreeNode answer : answered) {
                Path path = new Path(answer.path());
                byte[] bytes = answer.nodes() != null ? answer.nodes()
                        : answer.lines().getBytes(StandardCharsets.UTF_8);
                if (path.nibbles().length == 0) {
                    if (!takeRoot(from, stream)) {
                        refused.add("unsolicited-answer"); // no opening, or a root more than the openings asked for
                        continue;
                    }
                    // A root answer starts the stream's descent afresh: the partner's set may have moved since the
                    // last round, so nothing asked before is kept.
                    descent = new Descent();
                    byStream.put(stream, descent);
                    exchanged = true;
                } else {
                    Expected expected = descent == null ? null : descent.expected.get(path);
                    if (expected == null || !descent.inFlight.containsKey(path)) {
                        refused.add("unsolicited-answer"); // a late answer of a descent that moved on, or none asked
                        continue;
                    }
                    exchanged = true;
                    descent.expected.remove(path);
                    descent.inFlight.remove(path);
                    if ((answer.nodes() != null || expected.count() <= Bodies.TreeNode.LEAF_MAX)
                            && !Arrays.equals(fingerprint(bytes), expected.fingerprint())) {
                        // The partner's set moved between two exchanges; a later round asks again.
                        refused.add("fingerprint-mismatch");
                        continue;
                    }
                    if (answer.nodes() != null && !countsSumTo(answer.nodes(), expected.count())) {
                        // An inner node's count is the sum of its children's, so the preimage its parent listed
                        // could never hold these: no honest set gives them, and nothing under them is followed.
                        refused.add("shape-mismatch");
                        continue;
                    }
                }
                if (answer.nodes() != null && path.nibbles().length >= Bodies.TreeNode.PATH_MAX) {
                    // A node at 64 nibbles is a leaf whatever its count (annex I.6), so it has no children to ask.
                    refused.add("shape-mismatch");
                    continue;
                }
                descent.lastMillis = now;
                answersReceived.incrementAndGet();
                answerBytesReceived.addAndGet(bytes.length);
                StreamCounters c = counters(stream);
                c.bytesReceived.addAndGet(bytes.length);
                if (path.nibbles().length == 0) {
                    c.rounds.incrementAndGet();
                }
                accepted.add(answer);
            }
            if (exchanged) {
                descent.exchanges++;
            }
            if (byStream.isEmpty()) {
                descents.remove(from);
            }
        }
        // Then, with no monitor held: this member's own trie, which the space reads under its own lock.
        List<Differs> differs = new ArrayList<>();
        List<byte[]> matched = new ArrayList<>();
        List<String> seenLines = new ArrayList<>();
        Set<String> ids = new java.util.LinkedHashSet<>();
        for (Bodies.TreeNode answer : accepted) {
            byte[] path = answer.path();
            if (answer.nodes() != null) {
                byte[] nodes = answer.nodes();
                for (int c = 0; c < 16; c++) {
                    long count = countAt(nodes, c);
                    if (count == 0) {
                        continue; // entries the partner lacks: its own round brings them
                    }
                    byte[] childPath = Arrays.copyOf(path, path.length + 1);
                    childPath[childPath.length - 1] = (byte) c;
                    byte[] theirs = Arrays.copyOfRange(nodes, c * 24, c * 24 + 16);
                    ReconcilableState.AuditNode own = state.auditNode(childPath);
                    if (own.count() > 0) {
                        fingerprintsChecked.incrementAndGet();
                    }
                    if (own.count() == count && Arrays.equals(own.fingerprint(), theirs)) {
                        matched.add(childPath);
                    } else {
                        differs.add(new Differs(new Path(childPath), new Expected(theirs, count), own.count()));
                    }
                }
            } else if (!answer.lines().isEmpty()) {
                String lines = answer.lines();
                seenLines.add(lines);
                ids.addAll(missingIds(state, path, lines));
            }
        }
        // Last, under the monitor again: queue what differs and take the next requests, unless a newer round's root
        // replaced this descent meanwhile, in which case what was found here belongs to a descent that moved on. Only
        // a body that answered something moves the descent. A body whose every answer was refused (unsolicited, or a
        // root no opening is owed) asks nothing: before, it re-sent every request in flight without counting an
        // exchange, so a partner sending such bodies drew up to 256 request nodes per body for ever, past the
        // 64-exchange bound (the TypeScript adversarial review of 2026-11-15, ADV-TS-1, which Java shared).
        List<Bodies.TreeNode> next = new ArrayList<>();
        synchronized (this) {
            Map<String, Descent> byStream = descents.get(from);
            if (exchanged && descent != null && byStream != null && byStream.get(stream) == descent) {
                // No more can be asked in this descent than its remaining exchanges carry, so the queue holds no more:
                // a partner listing ever more children that differ cannot make it grow without bound.
                long room = (long) Math.max(0, EXCHANGES_MAX - descent.exchanges) * Bodies.Digest.TREE_NODES_MAX;
                boolean dropped = false;
                for (Differs child : differs) {
                    if (descent.pending.size() >= room) {
                        dropped = true; // the next round's descent finds what is left
                        break;
                    }
                    descent.expected.put(child.path(), child.expected());
                    descent.pending.add(new Pending(child.path(), child.ownCount()));
                }
                // A request this exchange left unanswered was left out for the budget: it is asked again next.
                for (Map.Entry<Path, Long> left : descent.inFlight.entrySet()) {
                    if (descent.expected.containsKey(left.getKey())) {
                        descent.pending.addFirst(new Pending(left.getKey(), left.getValue()));
                    }
                }
                descent.inFlight.clear();
                if (descent.exchanges >= EXCHANGES_MAX) {
                    // The partner's session closed with this exchange; asking more would only draw refusals there.
                    // What is left waits for the next round's root, as annex I.6 has it.
                    if (dropped || !descent.pending.isEmpty()) {
                        refused.add("descent-capped");
                    }
                    descent.pending.clear();
                }
                while (!descent.pending.isEmpty() && next.size() < Bodies.Digest.TREE_NODES_MAX) {
                    Pending ask = descent.pending.poll();
                    if (descent.inFlight.containsKey(ask.path()) || !descent.expected.containsKey(ask.path())) {
                        continue;
                    }
                    descent.inFlight.put(ask.path(), ask.count());
                    next.add(Bodies.TreeNode.request(ask.path().nibbles(), ask.count()));
                }
                if (descent.pending.isEmpty() && descent.inFlight.isEmpty()) {
                    byStream.remove(stream); // nothing left to ask: the stream is done for the round
                    if (byStream.isEmpty()) {
                        descents.remove(from);
                    }
                }
            }
        }
        for (String reason : refused) {
            refuse(from, stream, reason);
        }
        for (byte[] path : matched) {
            state.matchedUnder(from, path);
        }
        for (String lines : seenLines) {
            state.linesSeen(from, lines);
        }
        if (!next.isEmpty()) {
            requests.put(stream, next);
        }
        if (!ids.isEmpty()) {
            fetch.put(stream, new ArrayList<>(ids));
        }
    }

    // ---- the fetch, the asker's side (annex I.6, The fetch; RE-I-01 to RE-I-03)

    /**
     * Queues the ids an audit with a partner found and sends what the window allows. An id already ready for or in
     * flight at a member stays there; one that waits to be asked again goes to this partner, whose audit has just named
     * it, and forgets which members left it out, since the audit is new evidence.
     *
     * @param partner the member whose audit named the ids
     * @param stream  the stream
     * @param ids     the entry ids
     */
    void want(PeerId partner, String stream, java.util.Collection<String> ids) {
        List<Batch> out;
        synchronized (this) {
            long now = clock.getAsLong();
            LinkedHashMap<String, Wanted> byId = wanted.computeIfAbsent(stream, k -> new LinkedHashMap<>());
            LinkedHashSet<String> queue = null;
            for (String id : ids) {
                Wanted w = byId.get(id);
                if (w == null) {
                    if (byId.size() >= WANTED_MAX && !evictWaiting(byId)) {
                        fetchQueueFull.incrementAndGet(); // a later audit names it again
                        continue;
                    }
                    w = new Wanted();
                    byId.put(id, w);
                } else if (w.at != null) {
                    continue;
                }
                w.at = partner;
                w.parkedAt = 0;
                w.asked.clear();
                if (queue == null) {
                    queue = ready.computeIfAbsent(partner, k -> new LinkedHashMap<>())
                            .computeIfAbsent(stream, k -> new LinkedHashSet<>());
                }
                queue.add(id);
            }
            out = pump(partner, now);
        }
        send(out);
    }

    /** Pushes out the eldest id that waits, to make room; false when every id is ready or in flight. Under the monitor. */
    private boolean evictWaiting(LinkedHashMap<String, Wanted> byId) {
        java.util.Iterator<Wanted> it = byId.values().iterator();
        while (it.hasNext()) {
            if (it.next().at == null) {
                it.remove();
                fetchQueueFull.incrementAndGet();
                return true;
            }
        }
        return false;
    }

    /**
     * The fetch bodies the window lets go to a partner now, each of at most {@link Bodies.Digest#IDS_MAX} ids of one
     * stream, registered in flight. Under the monitor; the caller sends them after releasing it.
     */
    private List<Batch> pump(PeerId partner, long now) {
        Map<String, LinkedHashSet<String>> queues = ready.get(partner);
        if (queues == null) {
            return List.of();
        }
        ArrayDeque<Batch> flying = inFlight.computeIfAbsent(partner, k -> new ArrayDeque<>());
        List<Batch> out = new ArrayList<>();
        Integer yield = answerYield.get(partner);
        int perBody = yield == null ? Bodies.Digest.IDS_MAX
                : Math.max(FETCH_IDS_FLOOR, Math.min(Bodies.Digest.IDS_MAX, 2 * yield));
        java.util.Iterator<Map.Entry<String, LinkedHashSet<String>>> streams = queues.entrySet().iterator();
        int window = fetchWindow;
        while (flying.size() < window && streams.hasNext()) {
            Map.Entry<String, LinkedHashSet<String>> e = streams.next();
            LinkedHashSet<String> queue = e.getValue();
            LinkedHashMap<String, Wanted> byId = wanted.get(e.getKey());
            while (flying.size() < window && !queue.isEmpty()) {
                List<String> ids = new ArrayList<>(Math.min(queue.size(), perBody));
                java.util.Iterator<String> q = queue.iterator();
                while (q.hasNext() && ids.size() < perBody) {
                    ids.add(q.next());
                    q.remove();
                }
                Batch batch = new Batch(partner, e.getKey(), List.copyOf(ids), now);
                for (String id : ids) {
                    Wanted w = byId == null ? null : byId.get(id);
                    if (w != null) {
                        w.in = batch;
                    }
                }
                flying.add(batch);
                out.add(batch);
            }
            if (queue.isEmpty()) {
                streams.remove();
            }
        }
        if (queues.isEmpty()) {
            ready.remove(partner);
        }
        if (flying.isEmpty()) {
            inFlight.remove(partner);
        }
        return out;
    }

    /** Sends fetch bodies, each under this member's budget, with no monitor held. */
    private void send(List<Batch> batches) {
        long asked = budget;
        for (Batch batch : batches) {
            idsFetched.addAndGet(batch.ids.size());
            sender.sendDigest(batch.partner, new Bodies.Digest(null, null, null, null,
                    Map.of(batch.stream, batch.ids), asked, null));
        }
    }

    /**
     * Whether a {@code PULL_RESP} from a partner answers a fetch this member sent: the stream of a body in flight at the
     * partner that the answer names, or failing that one whose wait passed and whose late copy is still taken
     * (RE-Q-REL-02). Takes nothing: which body the answer settles is decided by what it carried ({@link #settle}),
     * since answers need not come back in the order the bodies went out. Null for an answer that names no such stream.
     *
     * @param from    the sender
     * @param streams the streams the answer's deltas and remainder name
     * @return the stream of the fetch the answer belongs to, or null
     */
    synchronized String fetchAnswer(PeerId from, Set<String> streams) {
        if (streams.isEmpty()) {
            return null;
        }
        expireFetches(clock.getAsLong());
        for (Map<PeerId, ArrayDeque<Batch>> byPartner : List.of(inFlight, late)) {
            ArrayDeque<Batch> batches = byPartner.get(from);
            if (batches != null) {
                for (Batch batch : batches) {
                    if (streams.contains(batch.stream)) {
                        return batch.stream;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Takes the body a fetch's answer settles: among the bodies in flight at the partner for the stream, the one whose
     * ids the answer carried most of, the eldest on a tie or when it carried none; failing those, a late one the same
     * way. Matching by content, not by arrival order, keeps an answer from being booked against another body when a
     * continuation overtakes the bodies before it, a frame is lost, or a partner answers out of order: booked wrongly,
     * the answer would count as no progress, and ids the partner holds would be sent away to wait for another member.
     *
     * @param from     the sender
     * @param stream   the fetch's stream
     * @param received the ids the answer carried for the stream
     * @return the body, or null when none is left (another answer settled it meanwhile)
     */
    synchronized Batch settle(PeerId from, String stream, Set<String> received) {
        long now = clock.getAsLong();
        Batch batch = takeBest(inFlight, from, stream, received);
        if (batch != null) {
            answerMillis.put(from, now - batch.sentAt);
            return batch;
        }
        return takeBest(late, from, stream, received);
    }

    private static Batch takeBest(Map<PeerId, ArrayDeque<Batch>> byPartner, PeerId from, String stream,
                                  Set<String> received) {
        ArrayDeque<Batch> batches = byPartner.get(from);
        if (batches == null) {
            return null;
        }
        Batch best = null;
        int bestShared = -1;
        for (Batch batch : batches) {
            if (!batch.stream.equals(stream)) {
                continue;
            }
            int shared = 0;
            for (String id : received) {
                if (batch.names(id)) {
                    shared++;
                }
            }
            if (shared > bestShared) { // strictly more, so the eldest wins a tie
                best = batch;
                bestShared = shared;
            }
        }
        if (best != null) {
            batches.remove(best);
            if (batches.isEmpty()) {
                byPartner.remove(from);
            }
        }
        return best;
    }

    /**
     * What a fetch's answer brought: the ids it carried are no longer wanted, and the ids of the body it left out are
     * asked again. When the answer carried a remainder and made progress, they go to the same partner at once, ahead
     * of its queue, as the continuation of the same fetch; otherwise the partner does not hold them, and they wait a
     * jittered backoff and go to the next member in view (RE-I-03, RE-Q-REL-03). A late copy's body was asked again
     * already, so only what it brought counts.
     *
     * @param from     the partner
     * @param batch    the body the answer answers
     * @param received the entry ids the answer carried for the body's stream
     * @param left     the answer's remainder for the stream, zero when it carried none
     */
    void fetched(PeerId from, Batch batch, Set<String> received, long left) {
        List<Batch> out;
        synchronized (this) {
            long now = clock.getAsLong();
            LinkedHashMap<String, Wanted> byId = wanted.get(batch.stream);
            boolean late = batch.late;
            int progress = 0;
            if (byId != null) {
                for (String id : received) {
                    Wanted w = byId.get(id);
                    if (w == null) {
                        continue;
                    }
                    boolean ours = batch.names(id);
                    if (ours) {
                        progress++;
                    }
                    // An id in flight in another body stays; that body's answer settles it.
                    if (w.at == null || (w.in == batch && ours)) {
                        byId.remove(id);
                    }
                }
                List<String> missing = new ArrayList<>();
                for (String id : batch.ids) {
                    Wanted w = byId.get(id);
                    if (w != null && w.in == batch) {
                        missing.add(id);
                    }
                }
                if (!late && left > 0 && progress > 0) {
                    answerYield.put(from, progress);
                }
                if (!missing.isEmpty()) {
                    if (left > 0 && progress > 0) {
                        // The continuation: the partner holds them and ran out of budget. Asked again at once, first.
                        Map<String, LinkedHashSet<String>> queues = ready.computeIfAbsent(from,
                                k -> new LinkedHashMap<>());
                        LinkedHashSet<String> front = new LinkedHashSet<>(missing);
                        LinkedHashSet<String> rest = queues.remove(batch.stream);
                        if (rest != null) {
                            front.addAll(rest);
                        }
                        // The stream's queue goes first among the partner's streams too.
                        Map<String, LinkedHashSet<String>> reordered = new LinkedHashMap<>();
                        reordered.put(batch.stream, front);
                        reordered.putAll(queues);
                        queues.clear();
                        queues.putAll(reordered);
                        for (String id : missing) {
                            Wanted w = byId.get(id);
                            w.in = null;
                            w.at = from;
                        }
                        fetchContinued.addAndGet(missing.size());
                    } else {
                        for (String id : missing) {
                            requeue(batch.stream, byId.get(id), from, now);
                        }
                    }
                }
                if (byId.isEmpty()) {
                    wanted.remove(batch.stream);
                }
            }
            out = late ? List.of() : pump(from, now);
        }
        send(out);
    }

    /** Sends an id back to wait for another member, after a jittered backoff. Under the monitor. */
    private void requeue(String stream, Wanted w, PeerId leftOutBy, long now) {
        w.in = null;
        w.at = null;
        w.asked.add(leftOutBy);
        w.attempts++;
        long base = Math.min(RETRY_CEILING_MILLIS, RETRY_BASE_MILLIS << Math.min(w.attempts - 1, 5));
        w.dueAt = now + base / 2 + (long) (random.nextDouble() * base); // half to one and a half times the base
        fetchRequeued.incrementAndGet();
        counters(stream).requeued.incrementAndGet();
    }

    /** How long this member waits for a partner's fetch answer: four times its last answer time, within bounds. */
    private long waitFor(PeerId partner) {
        Long last = answerMillis.get(partner);
        long floor = fetchWaitFloor;
        long wait = last == null ? floor : 4 * last;
        return Math.max(floor, Math.min(LATE_COPY_MILLIS, wait));
    }

    /**
     * Moves every body whose wait passed from in flight to late, asking its ids again of another member, and forgets
     * late bodies past {@link #LATE_COPY_MILLIS}. Under the monitor.
     */
    private void expireFetches(long now) {
        for (java.util.Iterator<Map.Entry<PeerId, ArrayDeque<Batch>>> it = inFlight.entrySet().iterator();
             it.hasNext(); ) {
            Map.Entry<PeerId, ArrayDeque<Batch>> e = it.next();
            long wait = waitFor(e.getKey());
            for (java.util.Iterator<Batch> b = e.getValue().iterator(); b.hasNext(); ) {
                Batch batch = b.next();
                if (now - batch.sentAt < wait) {
                    continue;
                }
                b.remove();
                batch.late = true;
                fetchTimedOut.incrementAndGet();
                late.computeIfAbsent(e.getKey(), k -> new ArrayDeque<>()).add(batch);
                LinkedHashMap<String, Wanted> byId = wanted.get(batch.stream);
                if (byId != null) {
                    for (String id : batch.ids) {
                        Wanted w = byId.get(id);
                        if (w != null && w.in == batch) {
                            requeue(batch.stream, w, e.getKey(), now);
                        }
                    }
                }
            }
            if (e.getValue().isEmpty()) {
                it.remove();
            }
        }
        late.values().forEach(batches -> batches.removeIf(batch -> now - batch.sentAt > LATE_COPY_MILLIS));
        late.values().removeIf(ArrayDeque::isEmpty);
    }

    /**
     * The fetch queue's turn on a round (RE-I-03): bodies whose answer did not come are asked of another member, ids
     * whose backoff passed go to the next member in view that has not left them out and that audits with this member,
     * an id every such member left out is parked until an audit names it again, and a member that left the view gives
     * back what was ready for it.
     *
     * @param view the members in view
     */
    void retry(List<PeerId> view) {
        List<Batch> out = new ArrayList<>();
        synchronized (this) {
            long now = clock.getAsLong();
            expireFetches(now);
            List<PeerId> members = new ArrayList<>();
            for (PeerId member : view) {
                if (audits(member)) {
                    members.add(member);
                }
            }
            if (!members.isEmpty()) {
                java.util.Collections.rotate(members, random.nextInt(members.size()));
            }
            Set<PeerId> inView = new HashSet<>(members);
            for (java.util.Iterator<Map.Entry<PeerId, Map<String, LinkedHashSet<String>>>> it =
                         ready.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<PeerId, Map<String, LinkedHashSet<String>>> e = it.next();
                if (inView.contains(e.getKey())) {
                    continue;
                }
                for (Map.Entry<String, LinkedHashSet<String>> q : e.getValue().entrySet()) {
                    LinkedHashMap<String, Wanted> byId = wanted.get(q.getKey());
                    for (String id : q.getValue()) {
                        Wanted w = byId == null ? null : byId.get(id);
                        if (w != null && w.in == null) {
                            w.at = null;
                            w.dueAt = now;
                        }
                    }
                }
                it.remove();
            }
            Set<PeerId> touched = new java.util.LinkedHashSet<>();
            for (Map.Entry<String, LinkedHashMap<String, Wanted>> s : wanted.entrySet()) {
                for (java.util.Iterator<Map.Entry<String, Wanted>> it = s.getValue().entrySet().iterator();
                     it.hasNext(); ) {
                    Map.Entry<String, Wanted> e = it.next();
                    Wanted w = e.getValue();
                    if (w.at != null) {
                        continue;
                    }
                    if (w.parkedAt != 0) {
                        if (now - w.parkedAt >= PARKED_MILLIS) {
                            it.remove(); // the audit is the liveness path; it names the id again if it is missing
                        }
                        continue;
                    }
                    if (w.dueAt > now) {
                        continue;
                    }
                    PeerId next = null;
                    for (PeerId member : members) {
                        if (!w.asked.contains(member)) {
                            next = member;
                            break;
                        }
                    }
                    if (next == null) {
                        w.parkedAt = now;
                        w.asked.clear();
                        fetchParked.incrementAndGet();
                        continue;
                    }
                    w.at = next;
                    ready.computeIfAbsent(next, k -> new LinkedHashMap<>())
                            .computeIfAbsent(s.getKey(), k -> new LinkedHashSet<>()).add(e.getKey());
                    touched.add(next);
                }
            }
            wanted.values().removeIf(Map::isEmpty);
            // Every member with ids ready, not only those given ids now: a body whose wait passed freed a place in
            // its partner's window, and the ids behind it go out on this round.
            touched.addAll(ready.keySet());
            for (PeerId member : touched) {
                out.addAll(pump(member, now));
            }
        }
        send(out);
    }

    // ---- the fetch, the partner's side

    /** Counts a fetch body this member answered. */
    void fetchAnswered() {
        fetchesAnswered.incrementAndGet();
    }

    // ---- helpers

    private long encodedSize(Bodies.TreeNode node) {
        return codec.toBytes(node).length;
    }

    private static long preimageBytes(List<Bodies.TreeNode> nodes) {
        long bytes = 0;
        for (Bodies.TreeNode node : nodes) {
            bytes += node.nodes() != null ? node.nodes().length
                    : node.lines() != null ? node.lines().length() : 0;
        }
        return bytes;
    }

    private byte[] fingerprint(byte[] preimage) {
        fingerprintsChecked.incrementAndGet();
        return Arrays.copyOf(Digests.sha256(preimage), 16);
    }

    /** Whether an inner preimage's sixteen counts, read as unsigned, sum to exactly the count given. */
    private static boolean countsSumTo(byte[] nodes, long expected) {
        long sum = 0;
        for (int c = 0; c < 16; c++) {
            long count = countAt(nodes, c);
            if (count < 0) {
                return false; // past what a long holds, so past any count a set has
            }
            sum += count;
            if (sum < 0) {
                return false;
            }
        }
        return sum == expected;
    }

    private static long countAt(byte[] nodes, int child) {
        long count = 0;
        for (int i = 0; i < 8; i++) {
            count = (count << 8) | (nodes[child * 24 + 16 + i] & 0xFF);
        }
        return count;
    }

    private static String text(byte[] utf8) {
        return new String(utf8, StandardCharsets.UTF_8);
    }

    /**
     * The entry ids of a partner's lines under a path whose line this member's own audited lines there lack, in the
     * order the answer names them. The state reads the one slot each line names ({@link ReconcilableState#auditHeld}),
     * so the cost follows the answer, not this member's own set under the path (ADV-TS-3). The ids are the ones the
     * comparison with every own line gave, since every audited line names its own slot's id.
     */
    private static Set<String> missingIds(ReconcilableState state, byte[] path, String lines) {
        Map<String, String> named = new LinkedHashMap<>(); // line to the id it names
        for (String line : lines.split("\n")) {
            String id = entryIdOf(line);
            if (id != null) {
                named.putIfAbsent(line, id);
            }
        }
        Set<String> ids = new LinkedHashSet<>();
        if (named.isEmpty()) {
            return ids;
        }
        Set<String> held = state.auditHeld(path, named.keySet());
        named.forEach((line, id) -> {
            if (!held.contains(line)) {
                ids.add(id);
            }
        });
        return ids;
    }

    /**
     * The entry id a line names: the text between the kind's {@code e:} or {@code c:} and the last {@code =}, since a
     * state hash is hex and a claim hash holds no {@code =}. Null for a line of another shape.
     */
    static String entryIdOf(String line) {
        if (line.length() < 4 || line.charAt(1) != ':' || (line.charAt(0) != 'e' && line.charAt(0) != 'c')) {
            return null;
        }
        int eq = line.lastIndexOf('=');
        return eq <= 2 ? null : line.substring(2, eq);
    }
}
