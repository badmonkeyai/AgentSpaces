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
    private final Set<String> oneWayDrops = new CopyOnWriteArraySet<>();
    private final Map<String, java.util.concurrent.atomic.AtomicLong> oneWayDropped = new ConcurrentHashMap<>();
    private final Map<String, java.util.List<Consumer<byte[]>>> taps = new ConcurrentHashMap<>();
    private final Map<String, java.util.function.UnaryOperator<byte[]>> corruptors = new ConcurrentHashMap<>();
    private final Map<String, Set<SimConnection>> live = new ConcurrentHashMap<>();

    private static String directed(String from, String to) {
        return from + ">" + to;
    }

    /** Frames from one node to another queue instead of delivering, until released. */
    public void hold(String from, String to) {
        holding.add(directed(from, to));
    }

    /** Delivers everything held from one node to another, in the given order; returns how many frames were delivered. */
    public int release(String from, String to, Order order, long seed) {
        String key = directed(from, to);
        holding.remove(key);
        java.util.Deque<Held> queue = held.remove(key);
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
            if (holding.contains(key)) {
                held.computeIfAbsent(key, k -> new java.util.concurrent.ConcurrentLinkedDeque<>()).add(new Held(target, wire));
                return;
            }
            target.receiver.accept(wire);
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
