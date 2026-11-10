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

import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.Messages;
import ai.badmonkey.agentspaces.content.tree.sync.SketchServer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The holder side of the tree exchange (srad/agentspaces-pdt.md §3.5, PD-X-03,
 * PD-P-01 to PD-P-03): answers {@code NODE_WANT} and {@code TREE_WANT} from a
 * {@link TreeStore}. The capability owns one and hands it every tag 8 and 10
 * frame; the checks a piece want passes (seed mode, policy, the
 * {@code CONTENT_FETCH} grant, the upload budget, sealing) come through
 * {@link Host} so both paths run the same code.
 *
 * <p>Grants (PD-P-01). The scope of a tree request is the CID asked for: the root
 * of a {@code TREE_WANT}, and every CID of a {@code NODE_WANT}, each checked
 * through {@link Host#mayServe}. A batch with one refused CID is refused whole
 * with {@code REJECT} reason {@link Messages#REASON_POLICY} naming its first CID,
 * so a member granted one root cannot read another tree by listing its nodes
 * behind an allowed CID.
 *
 * <p>A {@code TREE_WANT} is walked breadth first from its root. Before the walk
 * the server computes the set of CIDs reachable from every {@code haves} root it
 * holds (each distinct root once, the walk charged to one shared cap of
 * {@link Messages#MAX_WALK_NODES} nodes across all the haves, and the result kept
 * for {@link #PRUNE_CACHE_MILLIS} so repeated requests do not walk again) and
 * skips a subtree whose CID is in that set. A have whose walk hit the cap prunes
 * what the walk reached, which is always sound: a pruned CID sits under a root
 * the asker holds complete. The send walk stops at the request's budget, at the
 * walk cap, or at the upload budget, and closes with {@code TREE_END} and the
 * frontier: the CIDs reached and not sent. When {@code haves} names roots and this
 * holder holds none of them, the walk sends the top two levels only and closes
 * with reason {@link Messages#END_HAVES_NOT_HELD}, so the asker continues with
 * {@code NODE_WANT} from its own store's view (S3 of §6.4).
 *
 * <p>Nodes travel in {@code NODES} messages whose encoded form stays within
 * {@link Messages#MAX_NODES_BYTES}: the packer counts the CBOR framing of each
 * node and of the list and the seal overhead, since the receiver bounds a sealed
 * answer by its sealed length. In a group with a content key the host seals each
 * message under the current write epoch with AAD {@link Messages#SEALING_AAD_PREFIX}
 * + the asker's PeerId text + {@code "|"} + req (PD-P-03); a host that cannot
 * seal refuses the request with {@code REJECT} reason {@link Messages#REASON_POLICY}
 * before any budget is spent. When the upload budget runs out inside a
 * {@code NODE_WANT} the nodes that fit go out and a {@code REJECT} reason
 * {@link Messages#REASON_OVER_BUDGET} follows, so the asker reassigns the rest at
 * once.
 *
 * <p>Level 2 (srad/agentspaces-pdt2.md §6.10, PDT 2 phase 3). A host that advertises the PDT feature
 * level 2 ({@link Host#levelTwo}) answers tag 12 {@code CHALLENGE} (P2-S-07): the grant is checked as
 * for a {@code NODE_WANT}, the held picks go back in {@code NODES} under the challenge's {@code req}
 * with the rest in {@code missing}, and a holder of no pick refuses the root with {@code REJECT}
 * reason {@link Messages#REASON_NOT_HELD}. An artifact root's picks name its pieces by the raw CID of
 * their table digests and come from {@link Host#piece}; a chunk-group pick waits for the BLAKE3
 * identity of phase 4 and is answered missing. A level-1 host drops both forms and counts them.
 *
 * <p>Reconciliation (PDT 2 phase 5, P2-K-02, rule 12). A level-2 host answers a sketch {@code TREE_WANT}
 * ({@code root ""}) through its {@link SketchServer} ({@link Host#sketches}) with a {@code TREE_END} of
 * reason {@link Messages#END_SKETCH}, an empty frontier, and the reply in {@code sketch}, after it spends
 * the reply's bytes from the upload budget. A host without one does not reconcile and refuses with
 * {@code REJECT} reason {@link Messages#REASON_POLICY}, as it refuses an asker whose advertisement it reads
 * at level 1 ({@link Host#readsLevelTwo}, P2-F-05); a sketch outside its grammar is dropped and
 * counted, and a session past its rounds is refused with reason {@link Messages#REASON_OVER_BUDGET}.
 *
 * <p>Intra-chunk deltas (PDT 2 phase 4, P2-Z-01, P2-Z-02, srad/agentspaces-pdt2.md §3.8). When this holder and the
 * asker are both at level 2 ({@link Host#levelTwo}, {@link Host#readsLevelTwo}) and the answer travels in
 * plaintext, a {@code TREE_WANT} walk sends a raw chunk as the {@code dcz} frame the store keeps for it
 * ({@link TreeStore#deltas}) in the appended {@code NODES.deltas}, when the frame's base lies under a haves root
 * the asker holds and the frame is under half the chunk. Budgets and packing count the frame's bytes, and a
 * {@code NODES} carries at most {@link Messages#MAX_WANT_CIDS} frames. Every other answer carries the chunk whole,
 * and a {@code NODES} without deltas keeps its level-1 bytes.
 */
public final class TreeServer {

    /** Levels a walk sends when it holds none of the asker's haves: the root level and the one below. */
    static final int LEVELS_WITHOUT_HAVES = 2;
    /** How long a haves root's reachable set is reused across requests. */
    static final long PRUNE_CACHE_MILLIS = 10_000;
    /** Haves roots whose reachable sets are kept at once. */
    static final int PRUNE_CACHE_ROOTS = 32;
    /** The CBOR framing one node may add inside a {@code NODES}: a byte string header of up to five bytes. */
    static final int NODE_FRAMING_BYTES = 5;
    /** The CBOR framing of the list itself plus the seal's nonce and tag (12 + 16 bytes). */
    static final int MESSAGE_FRAMING_BYTES = 5 + 12 + 16;
    /** The CBOR framing one delta may add: the map, its three keys, two CID texts, and the frame's header (P2-Z-01). */
    static final int DELTA_FRAMING_BYTES = 160;

    /** What the server needs from the capability. */
    public interface Host {
        /**
         * The checks of a piece want, for one CID of the tree exchange (PD-P-01):
         * seed mode, policy, grant. The scope is the CID asked for: the root of a
         * {@code TREE_WANT}, or each CID of a {@code NODE_WANT} in turn.
         *
         * @param from the asker
         * @param cid  the CID asked for
         * @return false when the request is refused with reason {@link Messages#REASON_POLICY}
         */
        boolean mayServe(PeerId from, String cid);

        /**
         * Spends upload budget (PD-P-02).
         *
         * @param bytes the bytes about to be sent
         * @return false when the day's budget is spent
         */
        boolean withinUploadBudget(int bytes);

        /**
         * Seals the canonical CBOR of a nodes list for one asker, or answers the
         * plaintext form (PD-P-03). The server calls it once with an empty list
         * before serving, to learn whether it can seal at all.
         *
         * @param to    the asker
         * @param req   the request
         * @param nodes the nodes
         * @return the message to send, or empty when this holder may not serve nodes to that asker
         */
        Optional<Messages.Nodes> seal(PeerId to, int req, List<byte[]> nodes, List<String> missing);

        /** Sends a message to a peer under the content-swarm capability. */
        void send(PeerId to, Object message);

        /** A node was served. */
        default void served(int nodes, long bytes) {
        }

        /** The time, for the prune cache. */
        default long nowMillis() {
            return System.currentTimeMillis();
        }

        /** Whether this holder advertises the PDT feature level 2, so it reads the level-2 forms (P2-F-05). */
        default boolean levelTwo() {
            return false;
        }

        /**
         * Whether a member's advertisement carries the PDT feature level 2 (P2-F-05), so this holder may send it
         * a level-2 form such as {@code NODES.deltas} (PDT 2 phase 4, P2-Z-01).
         *
         * @param peer the asker
         * @return true when the asker reads the level-2 forms
         */
        default boolean readsLevelTwo(PeerId peer) {
            return false;
        }

        /**
         * The piece of an artifact root this holder holds whose table digest the raw CID names, for a
         * challenge pick (P2-S-07).
         *
         * @param root the artifact root
         * @param cid  the raw CID of the piece's sha2-256 digest
         * @return the whole piece, or empty when the root does not list it or this holder lacks it
         */
        default Optional<byte[]> piece(String root, String cid) {
            return Optional.empty();
        }

        /** A challenge of a root was answered with every pick it named (P2-S-13 counts them). */
        default void challengeAnswered(String root) {
        }

        /** The holder's side of a reconciliation (P2-K-02), or empty when this holder does not reconcile. */
        default Optional<SketchServer> sketches() {
            return Optional.empty();
        }
    }

    private record Reachable(long atMillis, Set<String> cids) {
    }

    private final TreeStore store;
    private final Host host;
    private final int walkCap;
    private final AtomicLong rejected = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong nodesServed = new AtomicLong();
    private final AtomicLong nodesWalked = new AtomicLong();
    private final AtomicLong deltasServed = new AtomicLong();
    private final Map<String, Reachable> pruneCache = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Reachable> eldest) {
            return size() > PRUNE_CACHE_ROOTS;
        }
    };

    /**
     * A server with the walk cap of {@link Messages#MAX_WALK_NODES}.
     *
     * @param store the node store, or null when this peer serves no trees and refuses every request
     * @param host  the capability's checks and sender
     */
    public TreeServer(TreeStore store, Host host) {
        this(store, host, Messages.MAX_WALK_NODES);
    }

    /**
     * A server with an explicit walk cap, for tests and tuning.
     *
     * @param store   the node store, or null when this peer serves no trees and refuses every request
     * @param host    the capability's checks and sender
     * @param walkCap the most nodes one {@code TREE_WANT} walks to send, and the most the
     *                prune walk visits across all its haves (PD-P-02)
     */
    public TreeServer(TreeStore store, Host host, int walkCap) {
        this.store = store;
        this.host = Objects.requireNonNull(host, "host");
        if (walkCap <= 0) {
            throw new IllegalArgumentException("walkCap must be positive, got " + walkCap);
        }
        this.walkCap = walkCap;
    }

    /** Requests refused with a {@code REJECT}. */
    public long rejected() {
        return rejected.get();
    }

    /** Requests dropped for a violated bound (PD-X-02). */
    public long dropped() {
        return dropped.get();
    }

    /** Nodes sent. */
    public long nodesServed() {
        return nodesServed.get();
    }

    /** Nodes visited by send walks and prune walks together (PD-P-02). */
    public long nodesWalked() {
        return nodesWalked.get();
    }

    /** Chunks sent as a {@code dcz} frame in {@code NODES.deltas} (PDT 2 phase 4, P2-Z-02). */
    public long deltasServed() {
        return deltasServed.get();
    }

    /**
     * Answers a {@code NODE_WANT}: the held nodes in the order asked, the rest in
     * {@code missing}, in messages that encode within {@link Messages#MAX_NODES_BYTES}.
     *
     * @param from the asker
     * @param want the request
     */
    public void serve(PeerId from, Messages.NodeWant want) {
        if (want.violation().isPresent()) {
            dropped.incrementAndGet();
            return;
        }
        String first = want.cids().get(0);
        if (store == null) {
            reject(from, first, Messages.REASON_POLICY);
            return;
        }
        for (String cid : want.cids()) {
            if (!host.mayServe(from, cid)) {
                reject(from, first, Messages.REASON_POLICY);
                return;
            }
        }
        if (!canSeal(from, want.req())) {
            reject(from, first, Messages.REASON_POLICY);
            return;
        }
        List<byte[]> batch = new ArrayList<>();
        List<String> missing = new ArrayList<>();
        long packed = MESSAGE_FRAMING_BYTES;
        boolean overBudget = false;
        for (String cid : want.cids()) {
            Optional<byte[]> node = store.get(cid);
            if (node.isEmpty()) {
                missing.add(cid);
                continue;
            }
            byte[] bytes = node.get();
            if (!host.withinUploadBudget(bytes.length)) {
                overBudget = true;
                break; // what fits goes, then a REJECT 2 so the asker reassigns the rest at once
            }
            if (packed + bytes.length + NODE_FRAMING_BYTES > Messages.MAX_NODES_BYTES && !batch.isEmpty()) {
                if (!flush(from, want.req(), first, batch, List.of())) {
                    return;
                }
                batch = new ArrayList<>();
                packed = MESSAGE_FRAMING_BYTES;
            }
            batch.add(bytes);
            packed += bytes.length + NODE_FRAMING_BYTES;
        }
        if (overBudget) {
            if (!batch.isEmpty() || !missing.isEmpty()) {
                if (!flush(from, want.req(), first, batch, missing)) {
                    return;
                }
            }
            reject(from, first, Messages.REASON_OVER_BUDGET);
            return;
        }
        flush(from, want.req(), first, batch, missing);
    }

    /**
     * Answers a {@code TREE_WANT} with the walk the class comment describes.
     *
     * @param from the asker
     * @param want the request
     */
    public void serve(PeerId from, Messages.TreeWant want) {
        if (want.violation().isPresent() || (want.reconciles() && !host.levelTwo())) {
            dropped.incrementAndGet();
            return;
        }
        if (want.reconciles()) {
            reconcile(from, want);
            return;
        }
        String root = want.root();
        if (store == null || !host.mayServe(from, root)) {
            reject(from, root, Messages.REASON_POLICY);
            return;
        }
        if (!store.has(root)) {
            reject(from, root, Messages.REASON_NOT_HELD);
            return;
        }
        Optional<Messages.Nodes> probe = host.seal(from, want.req(), List.of(), null);
        if (probe.isEmpty()) {
            reject(from, root, Messages.REASON_POLICY);
            return;
        }
        // PDT 2 phase 4, P2-Z-01 and P2-F-05: frames go from a level-2 holder to a level-2 asker, in plaintext only
        boolean sendDeltas = probe.get().plaintext() && host.levelTwo() && host.readsLevelTwo(from);
        List<String> haves = want.haves() == null ? List.of() : want.haves();
        Set<String> prune = new HashSet<>();
        boolean anyHaveHeld = false;
        long pruneBudget = walkCap;
        for (String have : new LinkedHashSet<>(haves)) {
            if (!store.has(have)) {
                continue;
            }
            anyHaveHeld = true;
            if (pruneBudget <= 0) {
                continue; // the cap is spent: the rest of the haves prune nothing
            }
            Set<String> reachable = reachableFor(have, pruneBudget);
            pruneBudget -= reachable.size();
            prune.addAll(reachable);
        }
        boolean topLevelsOnly = !haves.isEmpty() && !anyHaveHeld;
        int maxDepth = topLevelsOnly ? LEVELS_WITHOUT_HAVES - 1 : Integer.MAX_VALUE;

        Deque<String> queue = new ArrayDeque<>();
        Deque<Integer> depths = new ArrayDeque<>();
        Set<String> visited = new HashSet<>();
        queue.add(root);
        depths.add(0);
        visited.add(root);
        List<byte[]> batch = new ArrayList<>();
        List<Messages.NodeDelta> deltas = new ArrayList<>();
        List<String> missing = new ArrayList<>();
        long packed = MESSAGE_FRAMING_BYTES;
        long sent = 0;
        int walked = 0;
        int reason = Messages.END_DONE;
        List<String> frontier = new ArrayList<>();
        boolean sentAnything = false;
        while (!queue.isEmpty()) {
            String cid = queue.peekFirst();
            int depth = depths.peekFirst();
            if (walked >= walkCap) {
                reason = Messages.END_WALK_CAP;
                break;
            }
            if (depth > maxDepth) {
                reason = Messages.END_HAVES_NOT_HELD;
                break;
            }
            Optional<byte[]> node = store.get(cid);
            if (node.isEmpty()) {
                queue.pollFirst();
                depths.pollFirst();
                if (missing.size() < Messages.MAX_WANT_CIDS) {
                    missing.add(cid);
                }
                continue;
            }
            byte[] bytes = node.get();
            Messages.NodeDelta delta = sendDeltas ? deltaFor(cid, bytes.length, prune) : null;
            int cost = delta == null ? bytes.length : delta.dcz().length;
            int framing = delta == null ? NODE_FRAMING_BYTES : DELTA_FRAMING_BYTES;
            if (sent + cost > want.budget()) {
                reason = Messages.END_BUDGET;
                break;
            }
            if (!host.withinUploadBudget(cost)) {
                if (!sentAnything && batch.isEmpty() && deltas.isEmpty()) {
                    reject(from, root, Messages.REASON_OVER_BUDGET);
                    return;
                }
                reason = Messages.END_BUDGET;
                break;
            }
            queue.pollFirst();
            depths.pollFirst();
            walked++;
            nodesWalked.incrementAndGet();
            // PDT 2 phase 4, P2-Z-01: a NODES also carries at most MAX_WANT_CIDS deltas, or its receiver drops it
            boolean full = packed + cost + framing > Messages.MAX_NODES_BYTES
                    || (delta != null && deltas.size() == Messages.MAX_WANT_CIDS);
            if (full && !(batch.isEmpty() && deltas.isEmpty())) {
                if (!flush(from, want.req(), root, batch, missing, deltas)) {
                    return;
                }
                sentAnything = true;
                batch = new ArrayList<>();
                deltas = new ArrayList<>();
                missing = new ArrayList<>();
                packed = MESSAGE_FRAMING_BYTES;
            }
            if (delta == null) {
                batch.add(bytes);
            } else {
                deltas.add(delta);
            }
            packed += cost + framing;
            sent += cost;
            List<String> links;
            try {
                links = store.links(cid, bytes);
            } catch (IllegalArgumentException e) {
                links = List.of(); // a node this store cannot read links from is a leaf to the walk
            }
            for (String child : links) {
                if (!prune.contains(child) && visited.add(child)) {
                    queue.addLast(child);
                    depths.addLast(depth + 1);
                }
            }
        }
        if (!batch.isEmpty() || !missing.isEmpty() || !deltas.isEmpty()) {
            if (!flush(from, want.req(), root, batch, missing, deltas)) {
                return;
            }
        }
        for (String left : queue) {
            if (frontier.size() >= Messages.MAX_FRONTIER) {
                break;
            }
            frontier.add(left);
        }
        if (reason == Messages.END_DONE && !queue.isEmpty()) {
            reason = Messages.END_BUDGET;
        }
        host.send(from, new Messages.TreeEnd(want.req(), frontier, reason));
    }

    /** Answers a sketch {@code TREE_WANT} (P2-K-02) as the class comment describes. */
    private void reconcile(PeerId from, Messages.TreeWant want) {
        Optional<SketchServer> sketches = host.sketches();
        if (sketches.isEmpty() || !host.readsLevelTwo(from)) {
            // rule 12: this holder does not reconcile; P2-F-05: a sketch TREE_END is a level-2 form, so a member this
            // holder reads at level 1 gets the level-1 refusal instead (PDT 2 phase 5 adversary)
            reject(from, want.root(), Messages.REASON_POLICY);
            return;
        }
        switch (sketches.get().answer(from, want.req(), want.sketch(), want.budget())) {
            case SketchServer.Reply reply -> {
                if (!host.withinUploadBudget(reply.sketch().length)) {
                    reject(from, want.root(), Messages.REASON_OVER_BUDGET);
                    return;
                }
                host.send(from, new Messages.TreeEnd(Messages.TREE_END, want.req(), List.of(), Messages.END_SKETCH, reply.sketch()));
            }
            case SketchServer.Refusal refusal -> reject(from, want.root(), refusal.reason());
            case SketchServer.Dropped ignored -> dropped.incrementAndGet();
        }
    }

    /**
     * Answers a {@code CHALLENGE} (P2-S-07) as the class comment describes. A tree root's picks are
     * granted one by one as a {@code NODE_WANT}'s CIDs are; an artifact root's picks are its own pieces,
     * so the root's grant covers them.
     *
     * @param from      the challenger
     * @param challenge the challenge
     */
    public void serve(PeerId from, Messages.Challenge challenge) {
        if (!host.levelTwo() || challenge.violation().isPresent()) {
            dropped.incrementAndGet();
            return;
        }
        String root = challenge.root();
        // an artifact root is the raw CID of its piece table; a tree root names a dag-cbor node
        boolean artifact = Cids.parse(root).codec() == Cids.RAW;
        if (!host.mayServe(from, root)) {
            reject(from, root, Messages.REASON_POLICY);
            return;
        }
        for (Messages.Pick pick : challenge.picks()) {
            if (!artifact && !host.mayServe(from, pick.cid())) {
                reject(from, root, Messages.REASON_POLICY);
                return;
            }
        }
        if (!canSeal(from, challenge.req())) {
            reject(from, root, Messages.REASON_POLICY);
            return;
        }
        List<byte[]> held = new ArrayList<>();
        List<String> missing = new ArrayList<>();
        long total = 0;
        for (Messages.Pick pick : challenge.picks()) {
            Optional<byte[]> bytes = Optional.empty();
            if (Messages.Pick.NODE.equals(pick.names())) {
                bytes = store == null ? Optional.empty() : store.get(pick.cid());
                if (bytes.isEmpty() && artifact) {
                    bytes = host.piece(root, pick.cid());
                }
            }
            if (bytes.isPresent()) {
                held.add(bytes.get());
                total += bytes.get().length;
            } else {
                missing.add(pick.cid());
            }
        }
        if (held.isEmpty()) {
            reject(from, root, Messages.REASON_NOT_HELD);
            return;
        }
        if (total > Integer.MAX_VALUE || !host.withinUploadBudget((int) total)) {
            reject(from, root, Messages.REASON_OVER_BUDGET);
            return;
        }
        List<byte[]> batch = new ArrayList<>();
        long packed = MESSAGE_FRAMING_BYTES;
        for (byte[] bytes : held) {
            if (packed + bytes.length + NODE_FRAMING_BYTES > Messages.MAX_NODES_BYTES && !batch.isEmpty()) {
                if (!flush(from, challenge.req(), root, batch, List.of())) {
                    return;
                }
                batch = new ArrayList<>();
                packed = MESSAGE_FRAMING_BYTES;
            }
            batch.add(bytes);
            packed += bytes.length + NODE_FRAMING_BYTES;
        }
        // only an answer that holds every pick counts as passed in the ledger rows (P2-S-13)
        if (flush(from, challenge.req(), root, batch, missing) && missing.isEmpty()) {
            host.challengeAnswered(root);
        }
    }

    /**
     * The CIDs reachable from a held haves root, from the cache when fresh, else
     * walked breadth first through held nodes up to {@code limit} nodes.
     */
    private Set<String> reachableFor(String have, long limit) {
        long now = host.nowMillis();
        synchronized (pruneCache) {
            Reachable cached = pruneCache.get(have);
            if (cached != null && now - cached.atMillis <= PRUNE_CACHE_MILLIS) {
                return cached.cids;
            }
        }
        Set<String> reachable = walkReachable(have, limit);
        synchronized (pruneCache) {
            pruneCache.put(have, new Reachable(now, reachable));
        }
        return reachable;
    }

    private Set<String> walkReachable(String root, long limit) {
        Set<String> seen = new LinkedHashSet<>();
        Deque<String> queue = new ArrayDeque<>();
        queue.add(root);
        seen.add(root);
        long walked = 0;
        while (!queue.isEmpty() && walked < limit) {
            String cid = queue.pollFirst();
            Optional<byte[]> node = store.get(cid);
            if (node.isEmpty()) {
                continue;
            }
            walked++;
            nodesWalked.incrementAndGet();
            List<String> links;
            try {
                links = store.links(cid, node.get());
            } catch (IllegalArgumentException e) {
                continue;
            }
            for (String child : links) {
                if (seen.add(child)) {
                    queue.addLast(child);
                }
            }
        }
        if (!queue.isEmpty()) {
            // the cap stopped the walk: only what was visited is known to sit under the root
            seen.removeAll(queue);
        }
        return seen;
    }

    private boolean canSeal(PeerId to, int req) {
        return host.seal(to, req, List.of(), null).isPresent();
    }

    /**
     * The kept frame of a raw chunk that this answer may send in its place (P2-Z-02): its base lies under a haves
     * root the asker holds, and it is under half the chunk. Null when there is none.
     */
    private Messages.NodeDelta deltaFor(String cid, int chunkBytes, Set<String> prune) {
        if (prune.isEmpty() || Cids.parse(cid).codec() != Cids.RAW) {
            return null;
        }
        for (Messages.NodeDelta delta : store.deltas(cid)) {
            if (prune.contains(delta.base()) && 2L * delta.dcz().length < chunkBytes) {
                return delta;
            }
        }
        return null;
    }

    private boolean flush(PeerId to, int req, String root, List<byte[]> batch, List<String> missing) {
        return flush(to, req, root, batch, missing, List.of());
    }

    private boolean flush(PeerId to, int req, String root, List<byte[]> batch, List<String> missing,
                          List<Messages.NodeDelta> deltas) {
        Optional<Messages.Nodes> message = host.seal(to, req, batch, missing.isEmpty() ? null : List.copyOf(missing));
        if (message.isEmpty() || (!deltas.isEmpty() && !message.get().plaintext())) {
            reject(to, root, Messages.REASON_POLICY);
            return false;
        }
        Messages.Nodes out = message.get();
        long bytes = 0;
        for (byte[] node : batch) {
            bytes += node.length;
        }
        if (!deltas.isEmpty()) {
            // rule 14 of §6.10.3: the frames ride in the appended field beside the plaintext nodes
            out = new Messages.Nodes(Messages.NODES, out.req(), out.nodes(), null, null, out.missing(), List.copyOf(deltas));
            for (Messages.NodeDelta delta : deltas) {
                bytes += delta.dcz().length;
            }
            deltasServed.addAndGet(deltas.size());
        }
        nodesServed.addAndGet(batch.size());
        host.served(batch.size() + deltas.size(), bytes);
        host.send(to, out);
        return true;
    }

    private void reject(PeerId to, String root, int reason) {
        rejected.incrementAndGet();
        host.send(to, new Messages.Reject(root, Messages.TREE_INDEX, reason));
    }
}
