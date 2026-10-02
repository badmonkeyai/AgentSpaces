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
 * A bidirectional, frame-oriented connection between two peers. Frames are opaque
 * byte arrays; the wire protocol (spec §9) layers CBOR envelopes on top.
 */
public interface TransportConnection extends AutoCloseable {

    /** Returns the remote address in the transport's own format. */
    String remoteAddress();

    /**
     * Returns the PeerID this connection's remote end proved it controls, when
     * the transport authenticated the channel itself (spec §5.6): a TLS-backed
     * transport that verified the peer's channel certificate reports the bound
     * identity here, and the wire layer may then accept unsigned envelopes from
     * exactly that peer on exactly this connection. Transports without channel
     * authentication return empty, and every frame stays fully signed.
     *
     * @return the attested remote PeerID, or empty on unauthenticated channels
     */
    default java.util.Optional<ai.badmonkey.agentspaces.common.id.PeerId> attestedPeer() {
        return java.util.Optional.empty();
    }

    /**
     * Returns the certificate chain the remote end presented in this
     * connection's handshake, leaf first (SPEC §5.6, v0.1.13), whether or not
     * it attested: a node in the enterprise-CA mode judges it again when its
     * trust refreshes, and roots a peer revocation in the CA when the chain's
     * leaf is revoked. Empty on transports without a handshake.
     *
     * @return the presented chain, possibly empty
     */
    default java.security.cert.X509Certificate[] remoteChain() {
        return new java.security.cert.X509Certificate[0];
    }

    /**
     * Sends one frame.
     *
     * @param frame the frame bytes
     * @throws IOException if the connection has failed
     */
    void send(byte[] frame) throws IOException;

    /**
     * Registers the receiver for inbound frames. One receiver per connection;
     * registering replaces the previous receiver.
     *
     * @param receiver invoked for each inbound frame
     */
    void onReceive(Consumer<byte[]> receiver);

    /** Closes the connection. Idempotent. */
    @Override
    void close();
}
