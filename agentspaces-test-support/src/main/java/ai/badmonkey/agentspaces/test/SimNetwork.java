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
            target.receiver.accept(frame.clone());
        }

        @Override
        public void onReceive(Consumer<byte[]> receiver) {
            this.receiver = Objects.requireNonNull(receiver, "receiver");
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
