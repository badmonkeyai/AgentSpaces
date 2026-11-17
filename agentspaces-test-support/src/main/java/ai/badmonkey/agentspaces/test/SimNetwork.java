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
package ai.badmonkey.agentspaces.test;

import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.api.spi.TransportConnection;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

/**
 * A deterministic in-memory transport fabric for tests (plan §6): every node gets a
 * {@link Transport} whose scheme is {@code mem}, addresses are node names, and
 * delivery is synchronous on the sender's thread. Partitions are explicit:
 * {@link #partition(String, String)} blocks a pair of nodes in both directions
 * until {@link #heal()}.
 *
 * <p>The fabric is the decoupling artifact that lets peering, discovery, space,
 * and capability streams all test distributed behavior with no sockets, no
 * timing dependence, and no flakes.
 *
 * <p>Link filters ({@link #filter}) judge each frame on its kind, stream and decoded items, so a test
 * drops, holds under a named gate ({@link #releaseGate}), delays on the fleet clock ({@link #delay},
 * {@link #pump}) or duplicates only the frames it means (srad/spoiler-peers.md SP-N-01, SP-N-02). With
 * no filter a frame takes the path it always took.
 */
public final class SimNetwork {

    private final Map<String, Listener> listeners = new ConcurrentHashMap<>();
    private final Set<String> partitionedPairs = new CopyOnWriteArraySet<>();
    private final Map<String, ai.badmonkey.agentspaces.common.id.PeerId> attestations =
            new ConcurrentHashMap<>();

    // ---- controlled mode (ROBUSTNESS.md §2): script the sequence instead of hoping for it ----

    /** How a held queue is released. */
    public enum Order { FIFO, REVERSED, SHUFFLED, DUPLICATED }

    private record Held(SimConnection target, byte[] frame) {
    }

    private final Map<String, java.util.Deque<Held>> held = new ConcurrentHashMap<>();
    private final Set<String> holding = new CopyOnWriteArraySet<>();
    /** Orders a release against a concurrent send on the same link, so no frame is stranded in a queue nobody drains. */
    private final Object holdLock = new Object();
    private final Set<String> oneWayDrops = new CopyOnWriteArraySet<>();
    private final Map<String, java.util.concurrent.atomic.AtomicLong> oneWayDropped = new ConcurrentHashMap<>();
    private final Map<String, java.util.List<Consumer<byte[]>>> taps = new ConcurrentHashMap<>();
    private final Map<String, java.util.function.UnaryOperator<byte[]>> corruptors = new ConcurrentHashMap<>();
    private final Map<String, Set<SimConnection>> live = new ConcurrentHashMap<>();

    private static String directed(String from, String to) {
        return from + ">" + to;
    }

    // ---- link filters (srad/spoiler-peers.md §3.4, SP-N-01, SP-N-02): per-frame verdicts on kind, stream, item ----

    private record Filter(String from, String to, LinkFilter filter) {
        boolean covers(String a, String b) {
            return (from == null || from.equals(a)) && (to == null || to.equals(b));
        }
    }

    /** A frame waiting for its gate or its time, with the connection it was sent on. */
    private record Waiting(SimConnection target, LinkFrame frame, byte[] wire, int copies, long dueMillis) {
    }

    private final List<Filter> filters = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final java.util.concurrent.atomic.AtomicLong sequence = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong filteredDrops = new java.util.concurrent.atomic.AtomicLong();
    private final Map<String, List<Waiting>> gates = new java.util.LinkedHashMap<>();
    private final List<Waiting> delayed = new java.util.ArrayList<>();
    /** Guards {@link #gates} and {@link #delayed}; never held while a frame is delivered. */
    private final Object waitLock = new Object();
    private volatile FrameDecoder frameDecoder = new EnvelopeHeaders();
    private volatile ItemDecoder itemDecoder;
    private volatile java.time.InstantSource clock;

    /**
     * Plugs the decoders a {@link LinkFilter} matches with: the level-1 header ({@link EnvelopeHeaders}
     * unless replaced) and the level-2 items (none unless plugged; {@code agentspaces-spoilers} has one).
     *
     * @param headers the header decoder; null keeps the current one
     * @param items   the item decoder, or null for none
     * @return this fabric
     */
    public SimNetwork decoders(FrameDecoder headers, ItemDecoder items) {
        if (headers != null) {
            this.frameDecoder = headers;
        }
        this.itemDecoder = items;
        return this;
    }

    /**
     * The fleet clock {@link #delay} and {@link #pump} measure against; a fleet passes its {@link TestClock}.
     *
     * @param clock the clock
     * @return this fabric
     */
    public SimNetwork clock(java.time.InstantSource clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
        return this;
    }

    /**
     * Puts a filter on the frames from one node to another; a null address matches every node, so
     * {@code filter(null, null, f)} sees every frame of the fabric. Filters run in the order added, and
     * the most restrictive verdict wins ({@link LinkFilter.Verdict#combine}).
     *
     * @param from   the sending node, or null for any
     * @param to     the receiving node, or null for any
     * @param filter the filter
     * @return closing it removes the filter; frames it held stay held until released
     */
    public AutoCloseable filter(String from, String to, LinkFilter filter) {
        Filter f = new Filter(from, to, Objects.requireNonNull(filter, "filter"));
        filters.add(f);
        return () -> filters.remove(f);
    }

    /**
     * Delays every frame from one node to another by a span of fleet clock (ROBUSTNESS.md §2): each is
     * delivered on the first {@link #pump} at or past its due time. Needs {@link #clock}.
     *
     * @param from   the sending node, or null for any
     * @param to     the receiving node, or null for any
     * @param millis the span
     * @return closing it ends the delay for later frames
     */
    public AutoCloseable delay(String from, String to, long millis) {
        if (clock == null) {
            throw new IllegalStateException("a delay runs on the fleet clock: call clock(...) first");
        }
        return filter(from, to, frame -> LinkFilter.Verdict.delay(millis));
    }

    /**
     * Delivers every delayed frame whose time has come, in due order and then in send order; a fleet
     * calls it once per tick after advancing its clock. Delivery bypasses the filters, which already
     * judged each frame once.
     *
     * @return how many frames were delivered
     */
    public int pump() {
        java.time.InstantSource c = clock;
        if (c == null) {
            return 0;
        }
        long now = c.millis();
        List<Waiting> due = new java.util.ArrayList<>();
        synchronized (waitLock) {
            for (java.util.Iterator<Waiting> it = delayed.iterator(); it.hasNext(); ) {
                Waiting w = it.next();
                if (w.dueMillis() <= now) {
                    due.add(w);
                    it.remove();
                }
            }
        }
        due.sort(java.util.Comparator.comparingLong(Waiting::dueMillis).thenComparingLong(w -> w.frame().sequence()));
        int n = 0;
        for (Waiting w : due) {
            for (int i = 0; i < w.copies(); i++) {
                deliver(w.target(), w.wire().clone());
                n++;
            }
        }
        return n;
    }

    /**
     * Delivers every frame held under a gate, across all links, in the given order (FIFO is send
     * order); returns the frames, so a caller can record each one's stamp age at release. A frame
     * whose connection closed meanwhile is lost, as it would be on the wire.
     *
     * @param gate  the gate a {@link LinkFilter.Verdict#hold} named
     * @param order the order
     * @param seed  the seed of a {@link Order#SHUFFLED} release
     * @return the frames released, in delivery order
     */
    public List<LinkFrame> releaseGate(String gate, Order order, long seed) {
        List<Waiting> queue;
        synchronized (waitLock) {
            queue = gates.remove(gate);
        }
        if (queue == null) {
            return List.of();
        }
        List<Waiting> frames = new java.util.ArrayList<>(queue);
        switch (order) {
            case REVERSED -> java.util.Collections.reverse(frames);
            case SHUFFLED -> java.util.Collections.shuffle(frames, new java.util.Random(seed));
            case DUPLICATED -> {
                List<Waiting> twice = new java.util.ArrayList<>();
                for (Waiting w : frames) {
                    twice.add(w);
                    twice.add(w);
                }
                frames = twice;
            }
            default -> { }
        }
        List<LinkFrame> released = new java.util.ArrayList<>();
        for (Waiting w : frames) {
            deliver(w.target(), w.wire().clone());
            released.add(w.frame());
        }
        return released;
    }

    /** The frames held under a gate now, in send order. */
    public List<LinkFrame> gated(String gate) {
        synchronized (waitLock) {
            List<Waiting> queue = gates.get(gate);
            return queue == null ? List.of() : queue.stream().map(Waiting::frame).toList();
        }
    }

    /** How many frames wait for their time under {@link #delay} or a delaying filter. */
    public int delayedCount() {
        synchronized (waitLock) {
            return delayed.size();
        }
    }

    /** How many frames the filters dropped, across every link. */
    public long filteredDrops() {
        return filteredDrops.get();
    }

    private LinkFilter.Verdict verdict(LinkFrame frame) {
        LinkFilter.Verdict verdict = LinkFilter.Verdict.pass();
        for (Filter f : filters) {
            if (f.covers(frame.from(), frame.to())) {
                verdict = verdict.combine(f.filter().decide(frame));
            }
        }
        return verdict;
    }

    /** Runs the filters over a frame on its sender's thread; true when the frame went no further here. */
    private boolean filtered(String key, SimConnection target, String from, String to, byte[] wire) {
        if (filters.isEmpty()) {
            return false;
        }
        LinkFrame frame = new LinkFrame(from, to, wire, sequence.incrementAndGet(), frameDecoder, itemDecoder);
        LinkFilter.Verdict verdict = verdict(frame);
        switch (verdict.action()) {
            case DROP -> filteredDrops.incrementAndGet();
            case HOLD -> {
                synchronized (waitLock) {
                    gates.computeIfAbsent(verdict.gate(), k -> new java.util.ArrayList<>())
                            .add(new Waiting(target, frame, wire, 1, 0));
                }
            }
            case DELAY -> {
                java.time.InstantSource c = clock;
                long now = c == null ? 0 : c.millis();
                synchronized (waitLock) {
                    delayed.add(new Waiting(target, frame, wire, verdict.copies(), now + verdict.delayMillis()));
                }
            }
            case PASS -> {
                for (int i = 0; i < verdict.copies(); i++) {
                    forward(key, target, i == 0 ? wire : wire.clone());
                }
            }
        }
        if (verdict.then() != null) {
            verdict.then().run(); // after this frame's own delivery, outside every lock of the fabric
        }
        return true;
    }


    /** Frames from one node to another queue instead of delivering, until released. */
    public void hold(String from, String to) {
        holding.add(directed(from, to));
    }

    /** Delivers everything held from one node to another, in the given order; returns how many frames were delivered. */
    public int release(String from, String to, Order order, long seed) {
        String key = directed(from, to);
        java.util.Deque<Held> queue;
        synchronized (holdLock) { // a frame sent mid-release joins this queue or goes straight through, never neither
            holding.remove(key);
            queue = held.remove(key);
        }
        if (queue == null) {
            return 0;
        }
        java.util.List<Held> frames = new java.util.ArrayList<>(queue);
        switch (order) {
            case REVERSED -> java.util.Collections.reverse(frames);
            case SHUFFLED -> java.util.Collections.shuffle(frames, new java.util.Random(seed));
            case DUPLICATED -> {
                java.util.List<Held> twice = new java.util.ArrayList<>();
                for (Held h : frames) {
                    twice.add(h);
                    twice.add(h);
                }
                frames = twice;
            }
            default -> { }
        }
        for (Held h : frames) {
            deliver(h.target(), h.frame());
        }
        return frames.size();
    }

    /** Delivers the oldest held frame from one node to another, keeping the rest held; false when none. */
    public boolean releaseOne(String from, String to) {
        java.util.Deque<Held> queue = held.get(directed(from, to));
        Held h = queue == null ? null : queue.pollFirst();
        if (h == null) {
            return false;
        }
        deliver(h.target(), h.frame());
        return true;
    }

    /** How many frames are held from one node to another. */
    public int heldCount(String from, String to) {
        java.util.Deque<Held> queue = held.get(directed(from, to));
        return queue == null ? 0 : queue.size();
    }

    /** Drops frames from one node to another silently, as a lost direction of a link does; the reverse direction flows. */
    public void oneWay(String from, String to) {
        oneWayDrops.add(directed(from, to));
    }

    /** Restores the direction {@link #oneWay} dropped. */
    public void twoWay(String from, String to) {
        oneWayDrops.remove(directed(from, to));
    }

    /** How many frames {@link #oneWay} dropped on a direction. */
    public long oneWayDropped(String from, String to) {
        java.util.concurrent.atomic.AtomicLong n = oneWayDropped.get(directed(from, to));
        return n == null ? 0 : n.get();
    }

    /** Captures every frame sent from one node to another (the bytes on the wire), for a later {@link #inject}. */
    public void tap(String from, String to, Consumer<byte[]> consumer) {
        taps.computeIfAbsent(directed(from, to), k -> new java.util.concurrent.CopyOnWriteArrayList<>()).add(consumer);
    }

    /** Removes every tap on a direction. */
    public void untap(String from, String to) {
        taps.remove(directed(from, to));
    }

    /** Rewrites every frame sent on a direction (truncate, flip bytes); null restores the wire. */
    public void corrupt(String from, String to, java.util.function.UnaryOperator<byte[]> mutator) {
        if (mutator == null) {
            corruptors.remove(directed(from, to));
        } else {
            corruptors.put(directed(from, to), mutator);
        }
    }

    /**
     * Delivers a raw frame to {@code to} on a live connection from {@code from}, as if {@code from}
     * had sent it: a replay, a stale frame, a frame from a stranger. False when no connection is
     * live in that direction.
     */
    public boolean inject(String from, String to, byte[] frame) {
        Set<SimConnection> connections = live.get(directed(from, to));
        if (connections == null) {
            return false;
        }
        for (SimConnection c : connections) {
            SimConnection target = c.peer;
            if (!c.closed && target != null && !target.closed) {
                target.receiver.accept(frame.clone());
                return true;
            }
        }
        return false;
    }

    /** Delivers a frame that passed the filters, unless the older {@link #hold} queues its direction. */
    private void forward(String key, SimConnection target, byte[] wire) {
        if (holding.contains(key)) {
            synchronized (holdLock) {
                if (holding.contains(key)) { // still held: a release did not run since the check above
                    held.computeIfAbsent(key, k -> new java.util.concurrent.ConcurrentLinkedDeque<>()).add(new Held(target, wire));
                    return;
                }
            }
        }
        target.receiver.accept(wire);
    }

    private void deliver(SimConnection target, byte[] frame) {
        if (!target.closed) {
            target.receiver.accept(frame);
        }
    }

    private record Listener(Consumer<TransportConnection> onAccept) {
    }

    /**
     * Declares the PeerID this fabric attests for a node, standing in for a
     * TLS-authenticated transport (spec §5.6): every connection whose remote
     * end is {@code nodeId} then reports that PeerID from
     * {@code attestedPeer()}, so channel-attested frame flow is testable with
     * no real TLS. Nodes without a declaration stay unauthenticated.
     *
     * @param nodeId the node's address on this fabric
     * @param peerId the identity the simulated channel authentication proves
     */
    public void attest(String nodeId, ai.badmonkey.agentspaces.common.id.PeerId peerId) {
        attestations.put(Objects.requireNonNull(nodeId, "nodeId"),
                Objects.requireNonNull(peerId, "peerId"));
    }

    /**
     * Registers a node and returns its transport.
     *
     * @param nodeId the node's address on this fabric
     * @return the node's transport
     */
    public Transport register(String nodeId) {
        Objects.requireNonNull(nodeId, "nodeId");
        return new SimTransport(nodeId);
    }

    /**
     * Partitions two nodes from each other, in both directions. Existing
     * connections between them fail on the next send; new dials fail immediately.
     *
     * @param nodeA one node
     * @param nodeB the other node
     */
    public void partition(String nodeA, String nodeB) {
        partitionedPairs.add(pairKey(nodeA, nodeB));
    }

    /** Removes all partitions. */
    public void heal() {
        partitionedPairs.clear();
    }

    private boolean blocked(String a, String b) {
        return partitionedPairs.contains(pairKey(a, b));
    }

    private static String pairKey(String a, String b) {
        return a.compareTo(b) <= 0 ? a + "|" + b : b + "|" + a;
    }

    private final class SimTransport implements Transport {
        private final String nodeId;

        private SimTransport(String nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public String scheme() {
            return "mem";
        }

        /**
         * The fabric can attest ({@link SimNetwork#attest}), so it stands in
         * for TLS under {@code requireAttestation}; a node without a
         * declaration stays unattested, as a TLS peer without a certificate does.
         */
        @Override
        public java.util.Set<Capability> capabilities() {
            return java.util.Set.of(Capability.ATTESTING, Capability.RELIABLE, Capability.ORDERED);
        }

        @Override
        public TransportConnection dial(String address) throws IOException {
            Listener listener = listeners.get(address);
            if (listener == null) {
                throw new IOException("no listener at " + address);
            }
            if (blocked(nodeId, address)) {
                throw new IOException("partitioned: " + nodeId + " -> " + address);
            }
            SimConnection caller = new SimConnection(nodeId, address);
            SimConnection callee = new SimConnection(address, nodeId);
            caller.peer = callee;
            callee.peer = caller;
            live.computeIfAbsent(directed(nodeId, address), k -> new CopyOnWriteArraySet<>()).add(caller);
            live.computeIfAbsent(directed(address, nodeId), k -> new CopyOnWriteArraySet<>()).add(callee);
            listener.onAccept().accept(callee);
            return caller;
        }

        @Override
        public AutoCloseable listen(String bindAddress, Consumer<TransportConnection> onAccept) {
            listeners.put(bindAddress, new Listener(onAccept));
            return () -> listeners.remove(bindAddress);
        }
    }

    private final class SimConnection implements TransportConnection {
        private final String localNode;
        private final String remoteNode;
        private volatile SimConnection peer;
        private volatile Consumer<byte[]> receiver = frame -> {
        };
        private volatile Runnable closeListener = () -> {
        };
        private volatile boolean closed;

        private SimConnection(String localNode, String remoteNode) {
            this.localNode = localNode;
            this.remoteNode = remoteNode;
        }

        @Override
        public String remoteAddress() {
            return remoteNode;
        }

        @Override
        public java.util.Optional<ai.badmonkey.agentspaces.common.id.PeerId> attestedPeer() {
            return java.util.Optional.ofNullable(attestations.get(remoteNode));
        }

        @Override
        public void send(byte[] frame) throws IOException {
            SimConnection target = peer;
            if (closed || target == null || target.closed) {
                throw new IOException("connection closed: " + localNode + " -> " + remoteNode);
            }
            if (blocked(localNode, remoteNode)) {
                throw new IOException("partitioned: " + localNode + " -> " + remoteNode);
            }
            String key = directed(localNode, remoteNode);
            java.util.List<Consumer<byte[]>> watching = taps.get(key);
            if (watching != null) {
                for (Consumer<byte[]> t : watching) {
                    t.accept(frame.clone());
                }
            }
            if (oneWayDrops.contains(key)) {
                oneWayDropped.computeIfAbsent(key, k -> new java.util.concurrent.atomic.AtomicLong()).incrementAndGet();
                return; // lost on the wire: no error, as a dead direction looks to the sender
            }
            byte[] wire = frame.clone();
            java.util.function.UnaryOperator<byte[]> mutator = corruptors.get(key);
            if (mutator != null) {
                wire = mutator.apply(wire);
            }
            if (filtered(key, target, localNode, remoteNode, wire)) {
                return;
            }
            forward(key, target, wire);
        }

        @Override
        public void onReceive(Consumer<byte[]> receiver) {
            this.receiver = Objects.requireNonNull(receiver, "receiver");
        }

        @Override
        public void onClose(Runnable listener) {
            this.closeListener = Objects.requireNonNull(listener, "listener");
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            Set<SimConnection> mine = live.get(directed(localNode, remoteNode));
            if (mine != null) {
                mine.remove(this);
            }
            closeListener.run();
            SimConnection other = peer;
            if (other != null && !other.closed) {
                other.closed = true;      // the remote end sees the close at once, as a socket would
                other.closeListener.run();
            }
        }
    }
}
