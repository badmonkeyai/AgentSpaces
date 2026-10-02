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

    /** Returns the transport scheme, e.g. {@code tcp}, {@code ws}, {@code mem}. */
    String scheme();

    /**
     * Opens a connection to a peer address.
     *
     * @param address a transport-specific address
     * @return the open connection
     * @throws IOException if the peer cannot be reached
     */
    TransportConnection dial(String address) throws IOException;

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
