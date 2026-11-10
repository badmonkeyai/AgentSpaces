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

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeStore;

import java.time.InstantSource;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The follower's side of eager push (PDT 2 phase 5, P2-P-02 to P2-P-04, P2-Q-SEC-03; srad/agentspaces-pdt2.md §3.4,
 * S5 of §6.4). A follower with push on follows the stream of each ref it follows fully. It reads each bundle strictly,
 * spends its bytes from the stream's budget ({@link PushPublisher.Config#bytesPerSecond} a second, up to
 * {@link PushPublisher.Config#maxBytes} at once) and from the content swarm's download budget, and holds its nodes
 * aside, at most {@link PushPublisher.Config#maxBytes} and {@link PushPublisher.Config#maxNodes} nodes per root and
 * {@link #PENDING_ROOTS} roots per stream; a root no head announced never displaces one a head announced. Once a live
 * head announces the push of its root ({@link PushPublisher#ATTR}), the follower stores each held node that hashes to
 * a CID the root reaches through nodes this push stored, and drops the rest, as an inline delta is verified; a
 * subtree the store held before is not entered. The walk runs once from the root and then resumes only at the links
 * the push still lacks, so a bundle that brings none of them costs no walk, and a complete root none at all. A root
 * whose bundles complete it needs no request. An announced root still incomplete after its grace is fetched by
 * {@code TREE_WANT} from what the push left missing, and its later bundles are still stored, for {@link #ORPHAN_GRACES}
 * graces from the announcement; nodes that no head named within {@link #ORPHAN_GRACES} floor graces are dropped. The
 * grace of an announced root is sized for lateness (swarm retries, ROBUSTNESS.md §10.3 P4): the bus's repair window,
 * by which a member that missed a bundle's eager copy has asked two members that announced it, plus the time the
 * announced bundles take at the stream's budget, between {@link PushPublisher.Config#grace} and
 * {@link PushPublisher.Config#maxGrace}. A bundle outside its grammar or a bound, or past a budget, is dropped, counted, and never forwarded: the
 * forward filter passes only the bundles this follower took. Nobody is struck, since a member that does not follow a
 * ref relays its bundles without reading them (rule 16).
 */
public final class PushReceiver {

    private static final System.Logger LOG = System.getLogger(PushReceiver.class.getName());

    /** Roots whose bundles one stream holds at once; a complete one goes first, then the oldest unannounced. */
    static final int PENDING_ROOTS = 4;
    /** Graces a root's bundles are held before they go, announced or not. */
    static final int ORPHAN_GRACES = 2;
    /**
     * What keeping one node aside costs beyond its bytes, about what the JVM spends on its digest key, its map entry,
     * and its array header. A bound on bytes alone let a bundle of tiny nodes hold a hundred times its bytes (PDT 2
     * phase 5 adversary), so a root also holds at most {@link PushPublisher.Config#maxNodes} nodes.
     */
    static final int HELD_NODE_OVERHEAD = 160;

    /** What a follower does with an announced root whose push is not complete. */
    enum Wait {
        /** No push announced the root: fetch at once. */
        NONE,
        /** The grace runs: wait for the bundles. */
        WAIT,
        /** The grace ran out: fetch what the push left missing. */
        LATE
    }

    private static final class Assembly {
        final String root;
        final long createdMillis;
        long anchoredMillis = -1;
        /** The wait an announced root gets before the follower fetches it, sized when the head announces the push. */
        long graceMillis;
        /** The grace ran out and the follower fetches the rest; later bundles are still stored. */
        boolean late;
        final Map<String, byte[]> held = new LinkedHashMap<>();
        long heldBytes;
        final Set<String> stored = new HashSet<>();
        /** The links the walk could not follow yet, by digest, where it resumes when a bundle brings one. */
        final Map<String, String> missing = new HashMap<>();
        boolean complete;

        Assembly(String root, long now) {
            this.root = root;
            this.createdMillis = now;
        }
    }

    private static final class Stream {
        double tokens;
        long refilledMillis;
        final LinkedHashMap<String, Assembly> assemblies = new LinkedHashMap<>();

        Stream(double tokens, long now) {
            this.tokens = tokens;
            this.refilledMillis = now;
        }
    }

    private final NodeStore store;
    private final Content content;
    private final InstantSource clock;
    private final PushPublisher.Config config;
    private final CborCodec codec = CborCodec.defaultCodec();
    private final Map<String, Stream> streams = new ConcurrentHashMap<>();
    private final AtomicLong bundlesReceived = new AtomicLong();
    private final AtomicLong bundlesDropped = new AtomicLong();
    private final AtomicLong nodesStored = new AtomicLong();
    private final AtomicLong bytesStored = new AtomicLong();
    private final AtomicLong nodesDropped = new AtomicLong();
    private final AtomicLong fallbacks = new AtomicLong();
    /**
     * The payloads this follower took, which its forward filter passes once: the bus hands a rumour's one array to the
     * stream's handler and then to its filter, so the filter forwards exactly what the handler kept.
     */
    private final Set<byte[]> taken = Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));

    PushReceiver(NodeStore store, Content content, InstantSource clock, PushPublisher.Config config) {
        this.store = Objects.requireNonNull(store, "store");
        this.content = Objects.requireNonNull(content, "content");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.config = Objects.requireNonNull(config, "config");
    }

    /**
     * The stream of a ref: {@code pdt:<space>:<name>} (P2-P-01).
     *
     * @param space the ref space's name
     * @param name  the ref's name
     * @return the stream id
     */
    public static String stream(String space, String name) {
        return "pdt:" + space + ":" + name;
    }

    /**
     * Follows a ref's stream: takes its bundles and forwards only the ones it took, within the upload budget (P2-P-04).
     *
     * @param stream the stream
     * @return the handle that stops taking the stream's bundles
     */
    AutoCloseable follow(String stream) {
        AutoCloseable handler;
        try {
            handler = content.gossip().onStream(stream, (from, itemId, payload) -> onBundle(stream, from, payload));
        } catch (IllegalStateException taken) {
            // another facade on this node follows the stream already; this one fetches, as without push
            LOG.log(System.Logger.Level.WARNING, "push stream " + stream + " has a handler on this node already; "
                    + "this facade fetches its deltas");
            return () -> { };
        }
        streams.putIfAbsent(stream, new Stream(config.maxBytes(), clock.millis()));
        content.gossip().forwardFilter(stream, payload -> taken.remove(payload) && content.chargeUpload(payload.length));
        return () -> {
            handler.close();
            streams.remove(stream);
        };
    }

    private Optional<PushBundle> read(byte[] payload) {
        try {
            PushBundle bundle = codec.fromWire(payload, PushBundle.class);
            return bundle.violation().isPresent() ? Optional.empty() : Optional.of(bundle);
        } catch (RuntimeException malformed) {
            return Optional.empty();
        }
    }

    /**
     * Takes one bundle of a followed stream, as the class comment says. Public so a host's instrumentation can wrap
     * the stream's handler ({@code GossipBus.replaceHandler}).
     *
     * @param stream  the stream
     * @param from    the member that forwarded it
     * @param payload the bundle's bytes
     */
    public void onBundle(String stream, PeerId from, byte[] payload) {
        Stream s = streams.get(stream);
        if (s == null) {
            return;
        }
        Optional<PushBundle> read = read(payload);
        if (read.isEmpty()) {
            bundlesDropped.incrementAndGet();
            return;
        }
        PushBundle bundle = read.get();
        long now = clock.millis();
        synchronized (s) {
            s.tokens = Math.min(config.maxBytes(), s.tokens + (now - s.refilledMillis) * (config.bytesPerSecond() / 1000.0));
            s.refilledMillis = now;
            if (s.tokens < payload.length || !content.chargeDownload(payload.length)) {
                bundlesDropped.incrementAndGet();
                return;
            }
            s.tokens -= payload.length;
            Assembly a = s.assemblies.get(bundle.root());
            if (a == null) {
                if (!room(s, false)) {
                    bundlesDropped.incrementAndGet(); // every root this stream holds was announced
                    return;
                }
                a = new Assembly(bundle.root(), now);
                s.assemblies.put(bundle.root(), a);
            }
            if (a.complete) {
                // nothing left to store here; another follower may still lack these nodes, so the bundle goes on
                bundlesReceived.incrementAndGet();
                nodesDropped.addAndGet(bundle.nodes().size());
                taken.add(payload);
                return;
            }
            long bytes = 0;
            for (byte[] node : bundle.nodes()) {
                bytes += node.length;
            }
            if (a.heldBytes + bytes > config.maxBytes() || a.held.size() + bundle.nodes().size() > config.maxNodes()) {
                bundlesDropped.incrementAndGet();
                return;
            }
            bundlesReceived.incrementAndGet();
            taken.add(payload);
            List<String> found = new ArrayList<>();
            for (byte[] node : bundle.nodes()) {
                String digest = HexFormat.of().formatHex(Digests.sha256(node));
                if (a.held.putIfAbsent(digest, node) == null) {
                    a.heldBytes += node.length;
                    String link = a.missing.remove(digest);
                    if (link != null) {
                        found.add(link);
                    }
                }
            }
            if (!found.isEmpty()) {
                walk(a, found);
            }
        }
    }

    /**
     * Makes room for one more root on a stream: a complete root goes first, then the oldest that no head announced,
     * and an announced root displaces the oldest announced one only for another announced root.
     *
     * @return false when every root the stream holds was announced and the new one was not
     */
    private boolean room(Stream s, boolean announced) {
        if (s.assemblies.size() < PENDING_ROOTS) {
            return true;
        }
        Assembly gone = null;
        for (Assembly a : s.assemblies.values()) {
            if (a.complete) {
                gone = a;
                break;
            }
        }
        if (gone == null) {
            for (Assembly a : s.assemblies.values()) {
                if (a.anchoredMillis < 0) {
                    gone = a;
                    break;
                }
            }
        }
        if (gone == null && announced) {
            gone = s.assemblies.values().iterator().next();
        }
        if (gone == null) {
            return false;
        }
        nodesDropped.addAndGet(gone.held.size());
        s.assemblies.remove(gone.root);
        return true;
    }

    /**
     * A live head announced the push of its root: the held nodes the root reaches are stored now, and the rest as
     * their bundles arrive.
     *
     * @param stream the ref's stream
     * @param root   the head's root
     * @param parts  the bundles the head announces
     */
    void expect(String stream, String root, int parts) {
        Stream s = streams.get(stream);
        if (s == null) {
            return;
        }
        long now = clock.millis();
        long grace = grace(parts);
        synchronized (s) {
            Assembly a = s.assemblies.get(root);
            if (a == null) {
                room(s, true);
                a = new Assembly(root, now);
                s.assemblies.put(root, a);
            }
            if (a.anchoredMillis < 0) {
                a.anchoredMillis = now;
                a.graceMillis = grace;
                walk(a, List.of(root));
            }
        }
    }

    /**
     * The wait for an announced push of {@code parts} bundles (swarm retries): the bus's repair window plus the
     * bundles' largest bytes at the stream's budget, between the configured grace and its ceiling.
     */
    long grace(int parts) {
        long transfer = (long) parts * PushBundle.MAX_BYTES * 1000 / config.bytesPerSecond();
        long sized = content.gossip().repairWindowMillis() + transfer;
        return Math.max(config.grace().toMillis(), Math.min(config.maxGrace().toMillis(), sized));
    }

    /**
     * Stores every held node that hashes to a CID the root reaches through nodes this push stored, walking from the
     * given CIDs: the root once, then the links a bundle just brought. A node stored by this push is not walked again,
     * a subtree the store held before is not entered, and a link no held node answers waits in {@code missing}. The
     * push is complete when nothing is missing.
     */
    private void walk(Assembly a, List<String> from) {
        Deque<String> frontier = new ArrayDeque<>(from);
        while (!frontier.isEmpty()) {
            String cid = frontier.poll();
            if (a.stored.contains(cid) || store.has(cid)) {
                continue;
            }
            Cids.Parsed parsed;
            try {
                parsed = Cids.parse(cid);
            } catch (IllegalArgumentException notACid) {
                continue;
            }
            String digest = HexFormat.of().formatHex(parsed.digest());
            byte[] bytes = a.held.remove(digest);
            if (bytes == null) {
                a.missing.put(digest, cid);
                continue;
            }
            a.heldBytes -= bytes.length;
            try {
                store.putVerified(cid, bytes);
            } catch (RuntimeException refused) {
                nodesDropped.incrementAndGet();
                a.missing.put(digest, cid);
                continue;
            }
            a.stored.add(cid);
            nodesStored.incrementAndGet();
            bytesStored.addAndGet(bytes.length);
            frontier.addAll(links(cid, bytes));
        }
        if (a.missing.isEmpty()) {
            a.complete = true;
            nodesDropped.addAndGet(a.held.size()); // what the root does not reach was never part of its delta
            a.held.clear();
            a.heldBytes = 0;
        }
    }

    private static List<String> links(String cid, byte[] bytes) {
        try {
            return NodeCodec.links(cid, bytes);
        } catch (RuntimeException leaf) {
            return List.of();
        }
    }

    /**
     * Whether a follower waits for the push of a root it does not hold. Past the grace the follower fetches what the
     * push left missing, and the root's held nodes and later bundles stay, so a bundle that is merely late still
     * stores its nodes and the fetch asks for less (swarm retries).
     *
     * @param stream the ref's stream
     * @param root   the root
     * @param now    the clock
     * @return {@link Wait#WAIT} within the grace of an announced push, {@link Wait#LATE} past it, else {@link Wait#NONE}
     */
    Wait check(String stream, String root, long now) {
        Stream s = streams.get(stream);
        if (s == null) {
            return Wait.NONE;
        }
        synchronized (s) {
            Assembly a = s.assemblies.get(root);
            if (a == null || a.anchoredMillis < 0 || a.complete) {
                return Wait.NONE;
            }
            if (now - a.anchoredMillis < a.graceMillis) {
                return Wait.WAIT;
            }
            if (!a.late) {
                a.late = true;
                fallbacks.incrementAndGet();
            }
            return Wait.LATE;
        }
    }

    /**
     * Drops the bundles of roots held too long: an announced root {@link #ORPHAN_GRACES} of its own graces after the
     * announcement, a root no head announced {@link #ORPHAN_GRACES} floor graces after its first bundle.
     */
    void expire(long now) {
        long floor = config.grace().toMillis() * ORPHAN_GRACES;
        for (Stream s : streams.values()) {
            synchronized (s) {
                Iterator<Assembly> it = s.assemblies.values().iterator();
                while (it.hasNext()) {
                    Assembly a = it.next();
                    boolean anchored = a.anchoredMillis >= 0;
                    long since = anchored ? a.anchoredMillis : a.createdMillis;
                    long limit = anchored ? a.graceMillis * ORPHAN_GRACES : floor;
                    if (now - since > limit) {
                        nodesDropped.addAndGet(a.held.size());
                        it.remove();
                    }
                }
            }
        }
    }

    long bundlesReceived() {
        return bundlesReceived.get();
    }

    long bundlesDropped() {
        return bundlesDropped.get();
    }

    long nodesStored() {
        return nodesStored.get();
    }

    long bytesStored() {
        return bytesStored.get();
    }

    long nodesDropped() {
        return nodesDropped.get();
    }

    long fallbacks() {
        return fallbacks.get();
    }
}
