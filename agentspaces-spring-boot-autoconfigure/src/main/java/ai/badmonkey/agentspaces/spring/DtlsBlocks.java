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
import ai.badmonkey.agentspaces.transport.dtls.DtlsOptions;
import ai.badmonkey.agentspaces.transport.dtls.DtlsTransport;

import java.security.GeneralSecurityException;
import java.util.function.Supplier;

/**
 * Builds the DTLS binding from its block; loaded only when the block is
 * enabled and {@code agentspaces-transport-dtls} is present.
 */
final class DtlsBlocks {

    private DtlsBlocks() {
    }

    /** The bare UDP binding, tuned by the DTLS block's datagram settings. */
    static Transport buildUdp(AgentSpacesProperties.Transport.Dtls tuning) {
        return new ai.badmonkey.agentspaces.transport.dtls.UdpTransport().options(options(tuning));
    }

    private static DtlsOptions options(AgentSpacesProperties.Transport.Dtls block) {
        try {
            return new DtlsOptions(block.getMtu(), block.getWindow(), block.getIdleTimeout(),
                    block.getMaxHalfOpen(), block.getHelloRatePerSource(), block.isConnectionId(),
                    block.getProtocols());
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new IllegalStateException("agentspaces.transport.dtls: " + e.getMessage(), e);
        }
    }

    static Transport build(PeerIdentity identity, AgentSpacesProperties.Transport.Dtls block,
                           ServingCredential serving, Supplier<ChannelTrust> trust) {
        DtlsOptions options = options(block);
        try {
            DtlsTransport transport;
            if (TransportBlocks.unattested(block)) {
                transport = DtlsTransport.unattested();
            } else if (trust == null) {
                transport = new DtlsTransport(identity);
            } else {
                transport = DtlsTransport.withRefreshableTrust(identity, serving, trust);
            }
            return transport.options(options);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("cannot create the DTLS channel credential", e);
        }
    }
}
