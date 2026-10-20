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
package ai.badmonkey.agentspaces.api.spi;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * The transport SPI (spec §5.5, P7). AgentSpaces is transport-agnostic: TCP,
 * WebSocket, QUIC, Zenoh/DDS bridges, and filesystem sync all sit behind this
 * standards-based interface, and the choice of transport stays orthogonal to the
 * coordination semantics above it. Reference implementations arrive with
 * {@code agentspaces-peering}; a deterministic in-memory implementation lives in
 * {@code agentspaces-test-support}.
 */
public interface Transport {

    /**
     * What a binding provides (spec §5.5), so the node and the hosts can choose
     * and police bindings without knowing scheme names.
     */
    enum Capability {
        /** The link is encrypted end to end between the two peers. */
        ENCRYPTED,
        /**
         * The binding can report the remote end's PeerID from a channel
         * certificate ({@link TransportConnection#attestedPeer()}, spec §5.6).
         * A capability, not a promise: a remote end that presents no
         * certificate still connects, unattested.
         */
        ATTESTING,
        /** Every frame sent is delivered or the connection fails. */
        RELIABLE,
        /** Frames arrive in the order they were sent. */
        ORDERED,
        /**
         * Frames may be lost (datagram mode). Reserved: no binding reports it
         * yet, so a later datagram mode needs no SPI change.
         */
        DATAGRAM
    }

    /** Returns the transport scheme, e.g. {@code tcp}, {@code ws}, {@code mem}. */
    String scheme();

    /**
     * Reports what this binding provides. The default is the reliable, ordered,
     * unencrypted, non-attesting posture every binding has at least; bindings
     * that encrypt or attest override it.
     *
     * @return the capabilities, never empty
     */
    default java.util.Set<Capability> capabilities() {
        return java.util.Set.of(Capability.RELIABLE, Capability.ORDERED);
    }

    /**
     * Opens a connection to a peer address.
     *
     * @param address a transport-specific address
     * @return the open connection
     * @throws IOException if the peer cannot be reached
     */
    TransportConnection dial(String address) throws IOException;

    /**
     * Opens a connection to a member's advertised endpoint when the node
     * knows which member it is reaching (SRAD srad/agentspaces-js.md §3.4,
     * JS-W-08). A binding whose address names no destination of its own, such
     * as the {@code webrtc} binding's {@code sig} address meaning "reach me by
     * signalling over the fabric", needs the target to dial; every other
     * binding ignores it. The default dials the address alone.
     *
     * @param address a transport-specific address
     * @param target  the member the endpoint belongs to
     * @return the open connection
     * @throws IOException if the peer cannot be reached
     */
    default TransportConnection dial(String address, ai.badmonkey.agentspaces.common.id.PeerId target)
            throws IOException {
        return dial(address);
    }

    /**
     * Whether a dial on this binding is a round of signalling over the fabric
     * rather than the opening of a socket: the {@code webrtc} binding's
     * {@code sig} address (SRAD srad/agentspaces-js.md JS-W-08), whose dial
     * may take the binding's whole timeout and whose signals travel through
     * the node's own routing. A node never dials such an endpoint from inside
     * a send, where the wait would stall the sender and the routing could
     * re-enter the binding: it sends through a relay, and dials from its tick
     * once the frames it relays to or from the member pass a threshold
     * (JS-W-11). The default is false; every socket binding keeps it.
     *
     * @return true for a binding reached by signalling
     */
    default boolean signalled() {
        return false;
    }

    /**
     * Starts listening for inbound connections.
     *
     * @param bindAddress a transport-specific bind address
     * @param onAccept    invoked for each accepted connection
     * @return a handle that stops listening when closed
     * @throws IOException if the address cannot be bound
     */
    AutoCloseable listen(String bindAddress, Consumer<TransportConnection> onAccept) throws IOException;
}
