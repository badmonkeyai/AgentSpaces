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

package ai.badmonkey.agentspaces.spring;

import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.identity.ChannelTrust;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.identity.ServingCredential;
import ai.badmonkey.agentspaces.transport.quic.QuicTransport;

import java.security.GeneralSecurityException;
import java.util.function.Supplier;

/**
 * Builds the QUIC binding from its block, attesting with the same material as
 * TLS; loaded only when the block is enabled and {@code agentspaces-transport-quic}
 * is present.
 */
final class QuicBlocks {

    private QuicBlocks() {
    }

    static Transport build(PeerIdentity identity, ServingCredential serving, Supplier<ChannelTrust> trust) {
        try {
            return trust == null ? new QuicTransport(identity)
                    : QuicTransport.withRefreshableTrust(identity, serving, trust);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("cannot create the QUIC channel credential", e);
        }
    }
}
