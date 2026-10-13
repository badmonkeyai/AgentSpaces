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

import ai.badmonkey.agentspaces.api.spi.PeerSampler;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.peering.wire.Bodies;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * The two-channel gossip bus of one group (spec §5.3). The rumor channel pushes
 * new items on named streams with per-item hop decay and id-based deduplication;
 * the anti-entropy channel periodically reconciles registered
 * {@link ReconcilableState}s with one random partner, which guarantees
 * convergence for items the rumor channel missed.
 *
 * <p>Rumor forwarding happens inline on publish and receive; anti-entropy runs on
 * {@link #antiEntropyTick}, which the owning node calls once per
 * {@code GossipParameters.period} of its clock (spec §5.3). Both draw partners
 * from the group's {@link PeerSampler}. Rumors carry the fixed
 * {@link #DEFAULT_HOPS} budget unless a publisher chooses a smaller one.
 *
 * <p>Lazy push (PERF2 §3.4; Leitão, Pereira and Rodrigues' Plumtree): once {@link #lazyPush}
 * is enabled, members that understand it receive payloads eagerly along a spanning tree that
 * prunes itself on duplicates, plus a periodic HAVE announcement of what this node forwarded;
 * a member that hears an announcement for an item it never received asks for it with a GRAFT,
 * which repairs the tree. In steady state each member receives each item once instead of
 * {@code fanout} times. Members that do not understand it, and every member when lazy push is
 * off, get the random fan-out push of before, so the two kinds mix in one group.
 */
public final class GossipBus {

    /** Receives rumor items on a stream. */
    @FunctionalInterface
    public interface RumorHandler {

        /**
         * Handles one deduplicated rumor item.
         *
         * @param from    the forwarding peer (the previous hop, possibly the origin)
         * @param itemId  the item's unique id
         * @param payload the stream-specific payload
         */
        void onItem(PeerId from, String itemId, byte[] payload);
    }

    /** Sends kind-tagged frames to a specific peer; provided by the peer node. */
    public interface FrameSender {

        /**
         * Sends a RUMOR frame.
         *
         * @param to    the destination peer
         * @param rumor the rumor body
         */
        void sendRumor(PeerId to, Bodies.Rumor rumor);

        /**
         * Sends a DIGEST frame.
         *
         * @param to      the destination peer
         * @param digests the per-stream digests
         */
        void sendDigest(PeerId to, Bodies.Digest digests);

        /**
         * Sends a PULL_RESP frame.
         *
         * @param to     the destination peer
         * @param deltas the per-stream deltas
         */
        void sendPullResp(PeerId to, Bodies.PullResp deltas);

        /**
         * Sends a HAVE announcement (lazy push). Only called once {@link #lazyPush} is enabled.
         *
         * @param to   the destination
         * @param have the announced items
         */
        default void sendHave(PeerId to, Bodies.Have have) {
        }

        /**
         * Sends a GRAFT request (lazy push).
         *
         * @param to    the announcer being asked
         * @param graft the item asked for
         */
        default void sendGraft(PeerId to, Bodies.Graft graft) {
        }

        /**
         * Sends a PRUNE (lazy push).
         *
         * @param to    the peer being demoted to lazy
         * @param prune the duplicate that caused it
         */
        default void sendPrune(PeerId to, Bodies.Prune prune) {
        }
    }

    private static final int SEEN_CAPACITY = 4096;

    /**
     * The fixed default hop budget of a published rumor (spec §5.3): each
     * forward decrements it and forwarding stops at one. Six hops carry an
     * item across a fleet of several thousand members at fan-out 3
     * (3^6 &gt; 700 reach per origin before dedup), and, because inbound
     * budgets are clamped to this value, it also bounds the amplification any
     * single hostile item can buy (ASF-012). It is a constant, not a
     * {@code GossipParameters} field: the anti-entropy channel guarantees
     * convergence regardless, so the budget only tunes rumor reach.
     */
    public static final int DEFAULT_HOPS = 6;

    private final PeerSampler sampler;
    private final FrameSender sender;
    private final CborCodec codec;
    private final int fanout;
    private final int defaultHops;
    private final Map<String, RumorHandler> handlers = new ConcurrentHashMap<>();
    /** Optional per-stream forward filters (ASF-012): an item is re-forwarded
     * only when its payload passes the stream's filter, so an honest node never
     * amplifies content it can already tell is unverifiable. Delivery to the
     * local handler is unaffected — handlers validate for themselves. */
    private final Map<String, java.util.function.Predicate<byte[]>> forwardFilters =
            new ConcurrentHashMap<>();
    /** The partners this node has asked since its last periodic round: the only peers whose
     * PULL_RESP is applied (ASF-003), so an unsolicited delta cannot be injected into
     * replicated state by any other member. The periodic round clears it; a targeted round
     * with a newly admitted member adds to it; a reply consumes its entry. Bounded. */
    private final java.util.Set<PeerId> expectedPullPartners = Collections.synchronizedSet(
            new java.util.LinkedHashSet<>());
    private static final int PULL_PARTNERS_CAPACITY = 16;
    private final Map<String, ReconcilableState> states = new ConcurrentHashMap<>();
    /** The streams in registration order: the order a PULL_RESP's deltas are
     * applied in, whatever order the wire put them in (ISSUE-CanonicalMaps).
     * A node registers its revocation registries before any space attaches,
     * so a late joiner judges the records a pull brings with the revocations
     * the same pull brings, never before them. */
    private final List<String> registrationOrder = new java.util.concurrent.CopyOnWriteArrayList<>();
    /** Who delivered an item first and with what hop budget, so a later copy that travelled
     * fewer hops is recognised as the better edge (Plumtree's path optimisation). */
    private record Seen(PeerId from, int hops) {
    }

    private final Map<String, Seen> seen = Collections.synchronizedMap(
            new LinkedHashMap<>(256, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Seen> eldest) {
                    return size() > SEEN_CAPACITY;
                }
            });

    // ---- Lazy push (PERF2 §3.4) ----

    /** Announced items this node waits for before it grafts (bounded against a hostile announcer). */
    static final int MISSING_CAPACITY = 256;
    /** Items announced per round at most; older ones are left to anti-entropy. */
    static final int HAVE_CAPACITY = 256;
    /** Forwarded rumors kept to answer a GRAFT. */
    static final int RECENT_CAPACITY = 512;
    /** Items taken from one HAVE at most. */
    static final int HAVE_ITEMS_MAX = 256;
    /** GRAFTs sent per tick at most, so an announcer cannot make this node sign thousands of asks. */
    static final int GRAFTS_PER_TICK = 16;
    /** Ticks between a HAVE for an unseen item and the first GRAFT for it: more than the eager
     * push takes, batching included, or a graft would fetch a copy already on its way and the
     * duplicate would prune a sound edge (Plumtree's timer rule). */
    static final int GRAFT_AFTER_TICKS = 2;
    /** Ticks between one GRAFT and the next announcer being asked. */
    static final int GRAFT_RETRY_TICKS = 4;
    /**
     * A duplicate replaces the first copy's edge only when it travelled this many hops fewer
     * (Plumtree's path optimisation, with its threshold): a one-hop difference is what every
     * triangle produces for items from different sources, and swapping on it leaves the
     * shared partition source-dependent, so a node prunes every edge in turn and never settles.
     */
    static final int PATH_SWAP_THRESHOLD = 2;

    private static final class Missing {
        final String streamId;
        final byte[] hash;
        final java.util.ArrayDeque<PeerId> announcers = new java.util.ArrayDeque<>();
        long askAtTick;

        Missing(String streamId, byte[] hash, long askAtTick) {
            this.streamId = streamId;
            this.hash = hash;
            this.askAtTick = askAtTick;
        }
    }

    /** Whether a member understands HAVE, GRAFT and PRUNE; the owning node judges it from the wire hint. */
    private volatile java.util.function.Predicate<PeerId> lazyCapable;
    private volatile long ticks;
    /** Streams whose items are also delivered point to point, so a duplicate on them says nothing about the tree. */
    private final java.util.Set<String> directStreams = ConcurrentHashMap.newKeySet();
    /** Streams pushed to every capable member whatever the tree says: security state never waits on repair. */
    private final java.util.Set<String> eagerStreams = ConcurrentHashMap.newKeySet();

    /**
     * Marks a stream whose items go eagerly to every member, pruned or not (v0.1.17): the
     * revocation streams and the peer advertisement. A revocation that waited on an
     * announcement and a graft would leave a revoked peer connected for a few ticks; these
     * items are rare and small, so every edge carries them.
     *
     * @param streamId the stream
     */
    public void alwaysEager(String streamId) {
        eagerStreams.add(Objects.requireNonNull(streamId, "streamId"));
    }
    /** Capable members this node forwards announcements to instead of payloads: pruned tree edges. */
    private final java.util.Set<PeerId> lazy = ConcurrentHashMap.newKeySet();
    private record Forwarded(Bodies.HaveItem item, long tick) {
    }

    private final Map<String, Forwarded> pendingHaves = Collections.synchronizedMap(
            new LinkedHashMap<>(64, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Forwarded> eldest) {
                    return size() > HAVE_CAPACITY;
                }
            });
    private final Map<String, Missing> missing = Collections.synchronizedMap(
            new LinkedHashMap<>(64, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Missing> eldest) {
                    return size() > MISSING_CAPACITY;
                }
            });
    private final Map<String, Bodies.Rumor> recent = Collections.synchronizedMap(
            new LinkedHashMap<>(64, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Bodies.Rumor> eldest) {
                    return size() > RECENT_CAPACITY;
                }
            });
    /** Ticks after a topology change during which HAVE goes out on every tick. */
    static final int YOUNG_TICKS = 8;
    /** Ticks after a topology change during which HAVE goes out every {@link #SETTLING_EVERY} ticks; after that, probes carry it. */
    static final int SETTLING_TICKS = 32;
    static final int SETTLING_EVERY = 4;
    /** Items a probe carries at most. */
    static final int PIGGYBACK_ITEMS = 32;
    /** Streams a probe carries digests for; above this it carries the root hash alone. */
    static final int PROBE_DIGEST_STREAMS = 16;
    private volatile long lastTopologyChangeTick = Long.MIN_VALUE / 2;
    /** The tick each member was admitted at: a member is told only of items forwarded since. */
    private final Map<PeerId, Long> admittedAtTick = new ConcurrentHashMap<>();

    /**
     * Notes a member's admission (the owning node calls it): announcements and probe
     * piggybacks toward that member cover only items forwarded after this tick. What this
     * node forwarded before the member joined is history, which the member's anti-entropy
     * brings in registration order, revocations before the records they judge; a graft of an
     * older item would deliver it ahead of that.
     *
     * @param member the member just admitted
     */
    public void memberAdmitted(PeerId member) {
        admittedAtTick.put(Objects.requireNonNull(member, "member"), ticks);
    }

    /**
     * Notes a member's departure (the owning node calls it): a member gone may have been the
     * tree's only path to others, so every prune is forgotten and items go eagerly to every
     * capable member again until duplicates prune the tree afresh. A burst of duplicates after
     * a rare event, in exchange for delivery that never waits on repair.
     *
     * @param member the member that left the view
     */
    public void memberLeft(PeerId member) {
        admittedAtTick.remove(Objects.requireNonNull(member, "member"));
        forwarderLost(member);
    }

    /**
     * A member this node may have relied on to forward is gone for this node (left the view,
     * or quarantined here): every prune is forgotten, and every other capable member is asked, with
     * a GRAFT that names no item, to make this node eager again, so items flow on every edge
     * at once rather than after the next announcement and graft. One small frame per member,
     * on a rare event.
     *
     * @param lost the member gone, which is asked nothing
     */
    public void forwarderLost(PeerId lost) {
        lazy.clear();
        topologyChanged();
        java.util.function.Predicate<PeerId> capable = lazyCapable;
        if (capable == null) {
            return;
        }
        for (PeerId peer : sampler.members()) {
            if (!peer.equals(lost) && capable.test(peer)) {
                count("GRAFT");
                sender.sendGraft(peer, new Bodies.Graft(null, null));
            }
        }
    }

    private long admittedAt(PeerId member) {
        Long tick = admittedAtTick.get(member);
        return tick == null ? Long.MIN_VALUE : tick;
    }

    /** The items forwarded most recently, with the tick they were forwarded in, for the probe piggyback. */
    private final Map<String, Forwarded> recentForwarded = Collections.synchronizedMap(
            new LinkedHashMap<>(64, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Forwarded> eldest) {
                    return size() > PIGGYBACK_ITEMS;
                }
            });

    /**
     * Notes a change of the gossip tree or the view (a prune or graft either way, a member
     * admitted or dropped): announcements go out on every tick for {@link #YOUNG_TICKS} ticks
     * after it, then every few ticks until {@link #SETTLING_TICKS}, then only on probes, since
     * a member can only be cut off from the tree by such a change.
     */
    public void topologyChanged() {
        lastTopologyChangeTick = ticks;
    }

    /**
     * The items this node forwarded before the current tick and since {@code to} was admitted,
     * newest last, for a probe to carry. An item forwarded in this tick is left out: its eager
     * copy may still sit in a batch that the probe, which is never batched, would overtake, and
     * the receiver would graft a copy already on its way.
     *
     * @param to the member the probe goes to
     */
    public List<Bodies.HaveItem> piggyback(PeerId to) {
        long current = ticks;
        long since = admittedAt(to);
        List<Bodies.HaveItem> items = new ArrayList<>();
        synchronized (recentForwarded) {
            for (Forwarded forwarded : recentForwarded.values()) {
                if (forwarded.tick() < current && forwarded.tick() >= since) {
                    items.add(forwarded.item());
                }
            }
        }
        return items.isEmpty() ? null : items;
    }

    /**
     * A 32-byte hash per stream for a probe to carry, when the node has few streams, else null
     * (the root goes instead). A stream's anti-entropy digest is a full state summary that
     * grows with the state (a busy space's runs to a hundred kilobytes), so a probe carries its
     * hash, which is enough to tell two states apart.
     */
    public Map<String, byte[]> probeHashes() {
        if (states.isEmpty() || states.size() > PROBE_DIGEST_STREAMS) {
            return null;
        }
        Map<String, byte[]> hashes = new java.util.TreeMap<>();
        for (Map.Entry<String, ReconcilableState> e : states.entrySet()) {
            hashes.put(e.getKey(), Digests.sha256(e.getValue().digest()));
        }
        return hashes;
    }

    /** SHA-256 over the stream ids and digest hashes in stream-id order; null without streams. */
    public byte[] rootDigest() {
        if (states.isEmpty()) {
            return null;
        }
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        for (Map.Entry<String, ReconcilableState> e : new java.util.TreeMap<>(states).entrySet()) {
            out.writeBytes(e.getKey().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            out.write(0);
            out.writeBytes(Digests.sha256(e.getValue().digest()));
            out.write(0);
        }
        return Digests.sha256(out.toByteArray());
    }

    /** Whether a probe's stream hashes differ from this node's on any stream both hold. */
    public boolean differs(Map<String, byte[]> theirHashes) {
        for (Map.Entry<String, byte[]> e : theirHashes.entrySet()) {
            ReconcilableState state = states.get(e.getKey());
            if (state != null && e.getValue() != null
                    && !java.util.Arrays.equals(Digests.sha256(state.digest()), e.getValue())) {
                return true;
            }
        }
        return false;
    }

    /** Whether a probe's root hash differs from this node's. */
    public boolean differs(byte[] theirRoot) {
        byte[] own = rootDigest();
        return own != null && theirRoot != null && !java.util.Arrays.equals(own, theirRoot);
    }

    /** Rumor frames sent per stream, for diagnostics ({@code TrafficAudit}). */
    private final Map<String, java.util.concurrent.atomic.AtomicLong> rumorsByStream = new ConcurrentHashMap<>();

    /** How many RUMOR frames this bus has sent per stream, a snapshot. */
    public Map<String, Long> rumorsSentByStream() {
        Map<String, Long> out = new java.util.TreeMap<>();
        rumorsByStream.forEach((k, v) -> out.put(k, v.get()));
        return out;
    }

    private void countRumor(String streamId) {
        rumorsByStream.computeIfAbsent(streamId, k -> new java.util.concurrent.atomic.AtomicLong()).incrementAndGet();
    }

    /** Frames the lazy-push machinery sent, by kind name, for tests and diagnostics. */
    private final Map<String, java.util.concurrent.atomic.AtomicLong> lazyCounters = new ConcurrentHashMap<>();

    /**
     * Creates the bus.
     *
     * @param sampler     the group's peer sampler
     * @param sender      the frame sender
     * @param codec       the CBOR codec
     * @param fanout      peers contacted per rumor round
     * @param defaultHops default forward budget for published items
     */
    public GossipBus(PeerSampler sampler, FrameSender sender, CborCodec codec,
                     int fanout, int defaultHops) {
        this.sampler = Objects.requireNonNull(sampler, "sampler");
        this.sender = Objects.requireNonNull(sender, "sender");
        this.codec = Objects.requireNonNull(codec, "codec");
        if (fanout <= 0 || defaultHops <= 0) {
            throw new IllegalArgumentException("fanout and defaultHops must be positive");
        }
        this.fanout = fanout;
        this.defaultHops = defaultHops;
    }

    /**
     * Registers the handler for a rumor stream. One handler per stream.
     *
     * @param streamId the stream
     * @param handler  the handler
     */
    /**
     * Registers a forward filter for one stream (ASF-012): received items whose
     * payload fails the filter are still delivered locally but never forwarded,
     * so this node's identity and rate budget cannot be borrowed to amplify
     * content that every receiver would reject anyway.
     *
     * @param streamId the stream to filter
     * @param filter   returns whether a payload deserves re-forwarding
     */
    public void forwardFilter(String streamId, java.util.function.Predicate<byte[]> filter) {
        forwardFilters.put(Objects.requireNonNull(streamId, "streamId"),
                Objects.requireNonNull(filter, "filter"));
    }

    /**
     * Registers the handler for a rumor stream and returns the handle that
     * removes it (QA4 A4-9). One handler per stream id per node: a second
     * registration is refused rather than silently replacing the first, which
     * used to leave the earlier registrant deaf with nothing logged. Closing a
     * stale handle after a re-registration removes nothing.
     *
     * @param streamId the stream
     * @param handler  the handler
     * @return a handle that deregisters the handler when closed
     * @throws IllegalStateException when the stream already has a handler
     */
    public AutoCloseable onStream(String streamId, RumorHandler handler) {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(handler, "handler");
        if (handlers.putIfAbsent(streamId, handler) != null) {
            throw new IllegalStateException("stream '" + streamId + "' already has a handler on"
                    + " this node; close the earlier registration before registering another");
        }
        return () -> handlers.remove(streamId, handler);
    }

    /**
     * Replaces the handler for a stream, on purpose, returning the one displaced
     * (or null). This is the explicit form of what {@link #onStream} used to do
     * by accident (QA4 A4-9): instrumentation that wants to observe or silence a
     * stream says so here, and production code registers with {@code onStream}
     * and is refused a duplicate. The returned registration removes the
     * replacement only; restoring the original is the caller's business.
     *
     * @param streamId the stream
     * @param handler  the replacement handler
     * @return a handle that removes the replacement when closed
     */
    public AutoCloseable replaceHandler(String streamId, RumorHandler handler) {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(handler, "handler");
        handlers.put(streamId, handler);
        return () -> handlers.remove(streamId, handler);
    }

    /**
     * Registers a reconcilable state for anti-entropy under a stream id and
     * returns the handle that removes it (QA4 A4-9). One state per stream id
     * per node; a duplicate is refused, not silently substituted.
     *
     * @param streamId the stream
     * @param state    the state
     * @return a handle that deregisters the state when closed
     * @throws IllegalStateException when the stream already has a state
     */
    public AutoCloseable reconcile(String streamId, ReconcilableState state) {
        Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(state, "state");
        if (states.putIfAbsent(streamId, state) != null) {
            throw new IllegalStateException("stream '" + streamId + "' already has a reconcilable"
                    + " state on this node; close the earlier registration before registering"
                    + " another (a second space handle for one name is the usual cause)");
        }
        registrationOrder.add(streamId);
        return () -> {
            if (states.remove(streamId, state)) {
                registrationOrder.remove(streamId);
            }
        };
    }

    /**
     * The stream ids registered on this bus, for handlers or reconcilable
     * states; diagnostics, and the proof a closed registrant is really gone.
     *
     * @return an unmodifiable snapshot
     */
    public java.util.Set<String> streams() {
        java.util.Set<String> all = new java.util.TreeSet<>(handlers.keySet());
        all.addAll(states.keySet());
        return java.util.Collections.unmodifiableSet(all);
    }

    /**
     * Publishes an item: marks it seen locally and pushes it to {@code fanout}
     * sampled members with the default hop budget. The publisher applies the item
     * to its own state before publishing.
     *
     * @param streamId the stream
     * @param itemId   unique item id
     * @param payload  the payload
     */
    /** Set when anything moved since the last anti-entropy round: a rumor in or out, a delta either way. */
    private volatile boolean activity = true;

    /**
     * Whether anything happened on this bus since the last call (a rumor published or received, a
     * delta sent or applied). The runtime stretches the anti-entropy period while nothing does.
     */
    public boolean consumeActivity() {
        boolean was = activity;
        activity = false;
        return was;
    }

    /**
     * Runs one anti-entropy round: send digests to one random partner.
     *
     * @return the partner asked, if any
     */
    public java.util.Optional<PeerId> antiEntropyTick() {
        List<PeerId> partner = sampler.randomMembers(1);
        if (partner.isEmpty() || states.isEmpty()) {
            return java.util.Optional.empty();
        }
        expectedPullPartners.clear();
        digestTo(partner.get(0));
        return java.util.Optional.of(partner.get(0));
    }

    /**
     * Runs a targeted anti-entropy round with one member, outside the periodic schedule: the
     * owning node calls it for a member it has just admitted, so a newcomer pulls what the
     * fleet already holds (cards, space state) within a tick of joining rather than when the
     * random periodic partner happens to be a member that holds it. Anti-entropy pulls, so
     * the round benefits the asker; each side runs one on admitting the other.
     *
     * @param partner the member to reconcile with
     */
    public void antiEntropyWith(PeerId partner) {
        if (states.isEmpty()) {
            return;
        }
        digestTo(Objects.requireNonNull(partner, "partner"));
    }

    private final java.util.concurrent.atomic.AtomicLong rounds = new java.util.concurrent.atomic.AtomicLong();

    /** How many anti-entropy rounds (periodic and targeted) this bus has started. */
    public long antiEntropyRounds() {
        return rounds.get();
    }

    /** Which members understand the hashed DIGEST form; the owning node judges it from the wire hint. */
    private volatile java.util.function.Predicate<PeerId> hashedDigests = p -> false;

    /**
     * Enables the root-hash-first anti-entropy exchange toward the members {@code capable}
     * accepts (v0.1.17): a round opens with a hash per stream and the full digests follow only
     * for the streams that differ.
     *
     * @param capable which members understand a DIGEST of hashes
     */
    public void hashedDigests(java.util.function.Predicate<PeerId> capable) {
        this.hashedDigests = Objects.requireNonNull(capable, "capable");
    }

    private void digestTo(PeerId partner) {
        rounds.incrementAndGet();
        synchronized (expectedPullPartners) {
            while (expectedPullPartners.size() >= PULL_PARTNERS_CAPACITY) {
                java.util.Iterator<PeerId> eldest = expectedPullPartners.iterator();
                eldest.next();
                eldest.remove();
            }
            expectedPullPartners.add(partner);
        }
        if (hashedDigests.test(partner)) {
            Map<String, byte[]> hashes = new HashMap<>();
            for (Map.Entry<String, ReconcilableState> e : states.entrySet()) {
                hashes.put(e.getKey(), Digests.sha256(e.getValue().digest()));
            }
            sender.sendDigest(partner, new Bodies.Digest(null, hashes, null));
            return;
        }
        sender.sendDigest(partner, new Bodies.Digest(fullDigests(states.keySet())));
    }

    private Map<String, byte[]> fullDigests(java.util.Collection<String> streams) {
        Map<String, byte[]> digests = new HashMap<>();
        for (String streamId : streams) {
            ReconcilableState state = states.get(streamId);
            if (state != null) {
                digests.put(streamId, state.digest());
            }
        }
        return digests;
    }

    /**
     * Handles an inbound DIGEST: compute deltas the asker is missing and reply.
     *
     * @param from   the asking peer
     * @param digest the asker's digests
     */
    public void onDigest(PeerId from, Bodies.Digest digest) {
        if (digest.hashes() != null) {
            // Opening of a hashed round: name the streams both hold whose hashes differ.
            List<String> want = new ArrayList<>();
            for (Map.Entry<String, byte[]> e : digest.hashes().entrySet()) {
                ReconcilableState state = states.get(e.getKey());
                if (state != null && e.getValue() != null
                        && !java.util.Arrays.equals(Digests.sha256(state.digest()), e.getValue())) {
                    want.add(e.getKey());
                }
            }
            if (!want.isEmpty()) {
                java.util.Collections.sort(want);
                sender.sendDigest(from, new Bodies.Digest(null, null, want));
            }
            return;
        }
        if (digest.want() != null) {
            // The partner this node opened a round with asks for the full digests of the streams
            // that differ; nobody else is answered, so a stranger cannot make this node send them.
            if (!expectedPullPartners.contains(from) || digest.want().isEmpty()) {
                return;
            }
            Map<String, byte[]> digests = fullDigests(digest.want());
            if (!digests.isEmpty()) {
                sender.sendDigest(from, new Bodies.Digest(digests));
            }
            return;
        }
        if (digest.digests() == null) {
            return;
        }
        Map<String, byte[]> deltas = new HashMap<>();
        for (Map.Entry<String, byte[]> e : digest.digests().entrySet()) {
            ReconcilableState state = states.get(e.getKey());
            if (state == null) {
                continue;
            }
            byte[] delta = state.deltaFor(e.getValue());
            if (delta.length > 0) {
                deltas.put(e.getKey(), delta);
            }
        }
        if (!deltas.isEmpty()) {
            activity = true;
            sender.sendPullResp(from, new Bodies.PullResp(deltas));
        }
    }

    /**
     * Handles an inbound PULL_RESP: apply the deltas, in the order the streams
     * were registered, but only from a partner this bus sent a digest to and has not heard
     * back from. Anti-entropy is strictly request-response; an unsolicited PULL_RESP is a
     * delta-injection attempt and is dropped (ASF-003).
     *
     * @param from the sending peer
     * @param resp the deltas
     */
    public void onPullResp(PeerId from, Bodies.PullResp resp) {
        if (!expectedPullPartners.remove(from)) {
            return;
        }
        // In registration order, not wire order: the canonical map order of wire
        // v3 sorts shorter stream ids first, which would apply "space:x" before
        // "credential-revocations"; the revocation registries registered first.
        for (String streamId : registrationOrder) {
            byte[] delta = resp.deltas().get(streamId);
            ReconcilableState state = delta == null ? null : states.get(streamId);
            if (state != null) {
                activity = true;
                state.applyDelta(delta);
            }
        }
    }

    /**
     * Delivers a rumor from a peer outside the membership view: hand it to the
     * stream's handler, but never forward it, and never mark it seen — a
     * delivery the handler rejects must not dedup away a later legitimate copy
     * of the same item from an admitted member. This is the one gossip path
     * open to non-members (the bootstrap join, ASF-003); the caller rate-limits
     * it, and the peer-ad handler verifies each ad's own signature and
     * admission policy, so the sender's standing adds nothing.
     *
     * @param from  the sending peer
     * @param rumor the rumor body
     */
    public void onBootstrapRumor(PeerId from, Bodies.Rumor rumor) {
        RumorHandler handler = handlers.get(rumor.streamId());
        if (handler != null) {
            handler.onItem(from, rumor.itemId(), rumor.payload());
        }
    }

    public void publish(String streamId, String itemId, byte[] payload) {
        if (!directStreams.contains(streamId)) {
            activity = true; // a direct stream (the peer advertisement) is liveness, not data
        }
        byte[] hash = Digests.sha256(payload);
        String key = key(streamId, hash);
        seen.put(key, new Seen(null, defaultHops));
        Bodies.Rumor rumor = new Bodies.Rumor(streamId, itemId, defaultHops, payload);
        forward(key, hash, rumor, null);
    }

    /**
     * Handles an inbound RUMOR frame: deduplicate, deliver, and forward while the
     * hop budget lasts. Under lazy push a duplicate from a capable member demotes that member
     * to lazy with a PRUNE (the edge it came over is redundant), and a first copy from a lazy
     * member makes it eager again.
     *
     * @param from  the sending peer
     * @param rumor the rumor body
     */
    public void onRumor(PeerId from, Bodies.Rumor rumor) {
        byte[] hash = Digests.sha256(rumor.payload());
        String key = key(rumor.streamId(), hash);
        Seen first = seen.putIfAbsent(key, new Seen(from, rumor.hopsRemaining()));
        if (first != null) {
            if (!treeWorthwhile()) {
                return; // a small group: the fan-out reaches everyone in one hop, and the tree would only add fragility
            }
            // A duplicate over a tree edge means an edge is redundant (Plumtree PRUNE). A copy
            // delivered point to point (hop budget 1: an introduction, a GRAFT answer) and a
            // stream the node also delivers directly are not evidence about the tree. Which
            // edge goes: the duplicate's, unless it travelled markedly fewer hops than the first
            // copy (PATH_SWAP_THRESHOLD), in which case the first sender's edge is the longer
            // path and is pruned instead. The partition is shared by every source, so a node
            // keeps whichever edge delivered first and only swaps for a clearly shorter path,
            // with one exception: the origin's own push is the shortest path there is, so a
            // relayed copy that beat it (an asynchronous fabric does that at both receivers at
            // once, which would orphan the origin) never prunes the origin's edge.
            java.util.function.Predicate<PeerId> capable = lazyCapable;
            if (first.from() == null) {
                // An echo of this node's own item: the sender's path back here is a cycle, but
                // this node's direct edge to the sender is the shortest path there is, and the
                // sender is about to receive (or has received) this node's own push, which it
                // will judge the better copy. Pruning here would cut the one-hop edge in favour
                // of the cycle, which a synchronous fabric produces before the fan-out finishes.
                return;
            }
            if (capable != null && capable.test(from) && rumor.hopsRemaining() > 1
                    && !directStreams.contains(rumor.streamId())) {
                PeerId pruned = from;
                boolean directFromOrigin = rumor.hopsRemaining() >= defaultHops && first.hops() < defaultHops;
                if (first.from() != null && !first.from().equals(from) && capable.test(first.from())
                        && (directFromOrigin || rumor.hopsRemaining() - first.hops() >= PATH_SWAP_THRESHOLD)) {
                    pruned = first.from();
                    lazy.remove(from);
                    seen.put(key, new Seen(from, rumor.hopsRemaining()));
                }
                lazy.add(pruned);
                topologyChanged();
                count("PRUNE");
                sender.sendPrune(pruned, new Bodies.Prune(rumor.streamId(), hash));
            }
            return;
        }
        if (!directStreams.contains(rumor.streamId())) {
            activity = true;
        }
        missing.remove(key);
        // A first copy over a pruned edge restores it, when it was a tree push: a copy delivered
        // point to point (hop budget one: a refresh, an introduction, a graft answer) or on a
        // direct stream says nothing about the tree, just as such a duplicate never prunes.
        if (rumor.hopsRemaining() > 1 && !directStreams.contains(rumor.streamId())) {
            lazy.remove(from);
        }
        RumorHandler handler = handlers.get(rumor.streamId());
        if (handler != null) {
            handler.onItem(from, rumor.itemId(), rumor.payload());
        }
        java.util.function.Predicate<byte[]> filter = forwardFilters.get(rumor.streamId());
        if (filter != null && !filter.test(rumor.payload())) {
            return; // ASF-012: never amplify content this node can reject itself
        }
        // ASF-012: the inbound hop budget is clamped, so a hostile
        // hopsRemaining cannot buy amplification beyond this bus's own default.
        int hops = Math.min(defaultHops, rumor.hopsRemaining());
        if (hops > 1) {
            forward(key, hash, new Bodies.Rumor(rumor.streamId(), rumor.itemId(),
                    hops - 1, rumor.payload()), from);
        }
    }

    /**
     * Enables lazy push toward the members {@code capable} accepts (the owning node passes the
     * members whose advertisement carries the {@code wire.tree} hint). Announcements of what
     * this node forwarded go out on every {@link #tick} to a few lazy members.
     *
     * @param capable which members understand HAVE, GRAFT and PRUNE
     */
    public void lazyPush(java.util.function.Predicate<PeerId> capable) {
        this.lazyCapable = Objects.requireNonNull(capable, "capable");
    }

    /**
     * Marks a stream whose items this node also delivers point to point (the peer
     * advertisement: introduced directly on a dial and flooded on the interval), so a
     * duplicate on it never prunes a tree edge.
     *
     * @param streamId the stream
     */
    public void directDelivery(String streamId) {
        directStreams.add(Objects.requireNonNull(streamId, "streamId"));
    }

    /**
     * Whether the tree is worth pruning for: only once the group has more members than the
     * fan-out reaches in one hop. Below that every member pushes to every other, each item
     * arrives at most {@code fanout} times, and no bid or claim ever waits on a repair; a
     * settle window of a few ticks then means what it says (ROBUSTNESS.md).
     */
    boolean treeWorthwhile() {
        return sampler.members().size() > fanout;
    }

    /** The capable members this node currently announces to rather than pushes to. */
    public java.util.Set<PeerId> lazyPeers() {
        return java.util.Set.copyOf(lazy);
    }

    /** How many lazy-push frames of a kind ({@code HAVE}, {@code GRAFT}, {@code PRUNE}) this bus has sent. */
    public long lazyFramesSent(String kind) {
        java.util.concurrent.atomic.AtomicLong counter = lazyCounters.get(kind);
        return counter == null ? 0 : counter.get();
    }

    private void count(String kind) {
        lazyCounters.computeIfAbsent(kind, k -> new java.util.concurrent.atomic.AtomicLong()).incrementAndGet();
    }

    /**
     * Handles an inbound HAVE: items this node has seen are ignored; the rest are noted with
     * their announcer and asked for on a later tick if they are still missing then.
     *
     * @param from the announcing member
     * @param have the announced items
     */
    public void onHave(PeerId from, Bodies.Have have) {
        int taken = 0;
        for (Bodies.HaveItem item : have.items()) {
            if (++taken > HAVE_ITEMS_MAX || item.streamId() == null || item.hash() == null) {
                break;
            }
            String key = key(item.streamId(), item.hash());
            if (seen.containsKey(key)) {
                continue;
            }
            synchronized (missing) {
                Missing entry = missing.computeIfAbsent(key,
                        k -> new Missing(item.streamId(), item.hash(), ticks + GRAFT_AFTER_TICKS));
                if (!entry.announcers.contains(from)) {
                    entry.announcers.add(from);
                }
            }
        }
    }

    /**
     * Handles an inbound GRAFT: the asker is an eager member again, and gets the item if this
     * node still holds it. One answer per ask bounds what an asker can make this node send.
     *
     * @param from  the asking member
     * @param graft the item asked for
     */
    public void onGraft(PeerId from, Bodies.Graft graft) {
        if (lazy.remove(from)) {
            topologyChanged();
        }
        if (graft.streamId() == null || graft.hash() == null) {
            return; // an edge restored, no item asked for
        }
        Bodies.Rumor cached = recent.get(key(graft.streamId(), graft.hash()));
        if (cached != null) {
            sender.sendRumor(from, cached);
        }
    }

    /**
     * Handles an inbound PRUNE: the sender takes announcements from now on, not payloads.
     *
     * @param from  the member that received a duplicate from this node
     * @param prune the duplicate
     */
    public void onPrune(PeerId from, Bodies.Prune prune) {
        if (lazy.add(from)) {
            topologyChanged();
        }
    }

    /**
     * One lazy-push tick, driven by the owning node on every peer tick: forgets members that
     * left the view, grafts announced items that never arrived (a bounded number per tick,
     * each from the next announcer), and tells up to {@code fanout} random lazy members what
     * this node forwarded since the last tick, so a member cut off from the tree is grafted
     * back within two ticks. An idle group announces nothing.
     *
     * @param nowMillis the node's clock
     */
    public void tick(long nowMillis) {
        java.util.function.Predicate<PeerId> capable = lazyCapable;
        if (capable == null) {
            return;
        }
        long now = ++ticks;
        List<PeerId> members = sampler.members();
        if (!lazy.isEmpty()) {
            lazy.retainAll(members);
        }
        List<Missing> asks = new ArrayList<>();
        List<PeerId> askers = new ArrayList<>();
        synchronized (missing) {
            java.util.Iterator<Missing> it = missing.values().iterator();
            while (it.hasNext() && asks.size() < GRAFTS_PER_TICK) {
                Missing entry = it.next();
                if (entry.askAtTick > now) {
                    continue;
                }
                PeerId announcer = entry.announcers.poll();
                if (announcer == null) {
                    it.remove();
                    continue;
                }
                entry.askAtTick = now + GRAFT_RETRY_TICKS;
                asks.add(entry);
                askers.add(announcer);
            }
        }
        for (int i = 0; i < asks.size(); i++) {
            if (lazy.remove(askers.get(i))) {
                topologyChanged();
            }
            count("GRAFT");
            sender.sendGraft(askers.get(i), new Bodies.Graft(asks.get(i).streamId, asks.get(i).hash));
        }
        if (pendingHaves.isEmpty() || lazy.isEmpty()) {
            if (!pendingHaves.isEmpty() && lazy.isEmpty()) {
                pendingHaves.clear(); // every capable member got the payload
            }
            return;
        }
        // Standalone announcements only while the tree is young: a member is cut off only by a
        // topology change, so a settled tree needs none, and the probes carry the recent items.
        long since = now - lastTopologyChangeTick;
        if (since >= SETTLING_TICKS || (since >= YOUNG_TICKS && now % SETTLING_EVERY != 0)) {
            if (since >= SETTLING_TICKS) {
                pendingHaves.clear();
            }
            return;
        }
        List<Forwarded> pending;
        synchronized (pendingHaves) {
            pending = List.copyOf(pendingHaves.values());
            pendingHaves.clear();
        }
        for (PeerId peer : members) {
            if (!lazy.contains(peer) || !capable.test(peer)) {
                continue;
            }
            long admitted = admittedAt(peer);
            List<Bodies.HaveItem> items = new ArrayList<>();
            for (Forwarded forwarded : pending) {
                if (forwarded.tick() >= admitted) {
                    items.add(forwarded.item());
                }
            }
            if (!items.isEmpty()) {
                count("HAVE");
                sender.sendHave(peer, new Bodies.Have(items));
            }
        }
    }

    /**
     * Pushes a rumor on: to every capable member this node has not pruned (the tree), and to
     * {@code fanout} of the other members at random (the fan-out of spec §5.3, unchanged for a
     * group with no capable member). The item is remembered for a GRAFT and queued for the next
     * announcement when any capable member exists.
     */
    private void forward(String key, byte[] hash, Bodies.Rumor rumor, PeerId exclude) {
        java.util.function.Predicate<PeerId> capable = lazyCapable;
        int quota = fanout;
        boolean anyCapable = false;
        // In a group too small for the tree every capable member is pushed to, whatever a PRUNE
        // said: nothing prunes there, and a prune honoured there could only cost a repair.
        boolean eager = eagerStreams.contains(rumor.streamId()) || !treeWorthwhile();
        for (PeerId peer : sampler.members()) {
            if (peer.equals(exclude)) {
                continue;
            }
            if (capable != null && capable.test(peer)) {
                anyCapable = true;
                if (eager || !lazy.contains(peer)) {
                    countRumor(rumor.streamId());
                    sender.sendRumor(peer, rumor);
                }
            } else if (quota > 0) {
                quota--;
                countRumor(rumor.streamId());
                sender.sendRumor(peer, rumor);
            }
        }
        if (anyCapable && (!eager || !treeWorthwhile() && !eagerStreams.contains(rumor.streamId()))) {
            recent.put(key, rumor);
            Forwarded forwarded = new Forwarded(new Bodies.HaveItem(rumor.streamId(), hash), ticks);
            pendingHaves.put(key, forwarded);
            recentForwarded.put(key, forwarded);
        }
    }

    /**
     * The seen-cache key: the stream and a hash of the payload itself, never the
     * sender-chosen itemId (ASF-012): a predictable itemId let an attacker pre-seed the cache
     * with a victim's <em>future</em> ids, so honest nodes dropped the victim's
     * genuine items unread — targeted eviction from the fabric. A payload hash
     * cannot be precomputed for content the victim has not produced yet.
     */
    private static String key(String streamId, byte[] hash) {
        return streamId + "\u0000" + java.util.HexFormat.of().formatHex(hash);
    }
}
