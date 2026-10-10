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
    /** The partner of the most recent anti-entropy tick: the only peer whose
     * PULL_RESP is applied (ASF-003), so an unsolicited delta cannot be
     * injected into replicated state by any other member. */
    private volatile PeerId expectedPullPartner;
    private final Map<String, ReconcilableState> states = new ConcurrentHashMap<>();
    /** The streams in registration order: the order a PULL_RESP's deltas are
     * applied in, whatever order the wire put them in (ISSUE-CanonicalMaps).
     * A node registers its revocation registries before any space attaches,
     * so a late joiner judges the records a pull brings with the revocations
     * the same pull brings, never before them. */
    private final List<String> registrationOrder = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final Map<String, Boolean> seen = Collections.synchronizedMap(
            new LinkedHashMap<>(256, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > SEEN_CAPACITY;
                }
            });

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
    public void publish(String streamId, String itemId, byte[] payload) {
        markSeen(streamId, payload);
        forward(new Bodies.Rumor(streamId, itemId, defaultHops, payload), null);
    }

    /**
     * Handles an inbound RUMOR frame: deduplicate, deliver, and forward while the
     * hop budget lasts.
     *
     * @param from  the sending peer
     * @param rumor the rumor body
     */
    public void onRumor(PeerId from, Bodies.Rumor rumor) {
        if (!markSeen(rumor.streamId(), rumor.payload())) {
            return;
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
            forward(new Bodies.Rumor(rumor.streamId(), rumor.itemId(),
                    hops - 1, rumor.payload()), from);
        }
    }

    /** Runs one anti-entropy round: send digests to one random partner. */
    public void antiEntropyTick() {
        List<PeerId> partner = sampler.randomMembers(1);
        if (partner.isEmpty() || states.isEmpty()) {
            return;
        }
        Map<String, byte[]> digests = new HashMap<>();
        for (Map.Entry<String, ReconcilableState> e : states.entrySet()) {
            digests.put(e.getKey(), e.getValue().digest());
        }
        expectedPullPartner = partner.get(0);
        sender.sendDigest(partner.get(0), new Bodies.Digest(digests));
    }

    /**
     * Handles an inbound DIGEST: compute deltas the asker is missing and reply.
     *
     * @param from   the asking peer
     * @param digest the asker's digests
     */
    public void onDigest(PeerId from, Bodies.Digest digest) {
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
            sender.sendPullResp(from, new Bodies.PullResp(deltas));
        }
    }

    /**
     * Handles an inbound PULL_RESP: apply the deltas, in the order the streams
     * were registered, but only from the partner this bus sent its last digest to. Anti-entropy is strictly
     * request-response; an unsolicited PULL_RESP is a delta-injection attempt
     * and is dropped (ASF-003).
     *
     * @param from the sending peer
     * @param resp the deltas
     */
    public void onPullResp(PeerId from, Bodies.PullResp resp) {
        if (!from.equals(expectedPullPartner)) {
            return;
        }
        // In registration order, not wire order: the canonical map order of wire
        // v3 sorts shorter stream ids first, which would apply "space:x" before
        // "credential-revocations"; the revocation registries registered first.
        for (String streamId : registrationOrder) {
            byte[] delta = resp.deltas().get(streamId);
            ReconcilableState state = delta == null ? null : states.get(streamId);
            if (state != null) {
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

    private void forward(Bodies.Rumor rumor, PeerId exclude) {
        for (PeerId peer : sampler.randomMembers(fanout + (exclude != null ? 1 : 0))) {
            if (!peer.equals(exclude)) {
                sender.sendRumor(peer, rumor);
            }
        }
    }

    /**
     * Deduplicates by a hash of the payload itself, never by the sender-chosen
     * itemId (ASF-012): a predictable itemId let an attacker pre-seed the cache
     * with a victim's <em>future</em> ids, so honest nodes dropped the victim's
     * genuine items unread — targeted eviction from the fabric. A payload hash
     * cannot be precomputed for content the victim has not produced yet.
     */
    private boolean markSeen(String streamId, byte[] payload) {
        String key = streamId + "\u0000"
                + java.util.HexFormat.of().formatHex(Digests.sha256(payload));
        return seen.put(key, Boolean.TRUE) == null;
    }
}
