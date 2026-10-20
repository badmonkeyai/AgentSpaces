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

import ai.badmonkey.agentspaces.identity.ChannelTrust;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.identity.ServingCredential;

import java.security.GeneralSecurityException;
import java.util.Set;

/**
 * The {@code https} binding (SRAD agentspaces-js §3.3, appendix C, JS-H-07):
 * the request and response exchange over TLS 1.3, for networks whose proxies
 * refuse the WebSocket upgrade. The listener presents the channel certificate
 * (or {@code serving}); no client certificate reaches the peer through a
 * proxy, so the exchange never attests and peers prove themselves by session
 * attestation (SPEC §5.6). A node that requires attestation refuses it.
 */
public final class TlsHttpExchangeTransport extends ExchangeBinding {

    /** The scheme. */
    public static final String SCHEME = "https";

    /** Presents this identity's channel certificate. */
    public TlsHttpExchangeTransport(PeerIdentity identity) throws GeneralSecurityException {
        this(identity, null, null);
    }

    /** Presents {@code serving} when given (an operator certificate browsers trust, say). */
    public TlsHttpExchangeTransport(PeerIdentity identity, ServingCredential serving, ChannelTrust trust)
            throws GeneralSecurityException {
        super(TlsSetup.of(identity, serving, trust == null ? null : () -> trust));
    }

    /** Sets the options; valid before the first dial or listen. */
    public TlsHttpExchangeTransport options(HttpOptions options) {
        setOptions(options);
        return this;
    }

    @Override
    public String scheme() {
        return SCHEME;
    }

    @Override
    public Set<Capability> capabilities() {
        return Set.of(Capability.ENCRYPTED, Capability.RELIABLE, Capability.ORDERED);
    }
}
