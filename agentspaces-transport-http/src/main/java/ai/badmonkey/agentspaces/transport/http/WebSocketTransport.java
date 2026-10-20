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
package ai.badmonkey.agentspaces.transport.http;

import java.util.Set;

/**
 * The {@code ws} binding (SRAD agentspaces-js §3.3; ISSUE-Websocket FR-1): one
 * binary WebSocket message per frame over plain TCP, subprotocol
 * {@code aspace.1}, path {@code /aspace} by default. The WebSocket analogue of
 * {@code tcp}: for development, tests, and links behind a TLS terminator
 * inside a trusted network; it attests nothing, so peers in the SESSION mode
 * attest the connection in band (SPEC §5.6), and a node that requires
 * attestation refuses it.
 */
public final class WebSocketTransport extends WebSocketBinding {

    /** The scheme. */
    public static final String SCHEME = "ws";

    public WebSocketTransport() {
        super(null);
    }

    /** Sets the options; valid before the first dial or listen. */
    public WebSocketTransport options(HttpOptions options) {
        setOptions(options);
        return this;
    }

    @Override
    public String scheme() {
        return SCHEME;
    }

    @Override
    public Set<Capability> capabilities() {
        return Set.of(Capability.RELIABLE, Capability.ORDERED);
    }
}
