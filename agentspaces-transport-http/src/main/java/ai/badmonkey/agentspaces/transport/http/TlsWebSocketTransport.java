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
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The {@code wss} binding (SRAD agentspaces-js §3.3; ISSUE-Websocket FR-1,
 * FR-9, SR-1 to SR-4): WebSocket over TLS 1.3 with the channel certificates of
 * SPEC §5.6 in the three identity modes, exactly as {@code tls} offers them.
 * The listener requests, never requires, the dialer's certificate, so a
 * browser connects unattested and attests in band by session attestation; a
 * JVM dialer presenting its channel certificate is attested at the handshake.
 * Behind a TLS-terminating intermediary neither end attests (SR-10).
 */
public final class TlsWebSocketTransport extends WebSocketBinding {

    /** The scheme. */
    public static final String SCHEME = "wss";

    /** Identity-endorsed mode: presents this identity's channel certificate and attests peers by theirs. */
    public TlsWebSocketTransport(PeerIdentity identity) throws GeneralSecurityException {
        this(identity, null, null);
    }

    /**
     * Enterprise-CA mode: presents {@code serving} (or the identity-endorsed
     * certificate when null) and attests peers whose chain validates under
     * {@code trust} (or by their own endorsement when null).
     */
    public TlsWebSocketTransport(PeerIdentity identity, ServingCredential serving, ChannelTrust trust)
            throws GeneralSecurityException {
        super(TlsSetup.of(identity, serving, trust == null ? null : () -> trust));
    }

    private TlsWebSocketTransport(TlsSetup setup) {
        super(setup);
    }

    /** Enterprise-CA mode with a trust consulted on every handshake, so a fresh CRL applies at once. */
    public static TlsWebSocketTransport withRefreshableTrust(PeerIdentity identity, ServingCredential serving,
                                                             Supplier<ChannelTrust> trust)
            throws GeneralSecurityException {
        return new TlsWebSocketTransport(TlsSetup.of(identity, serving, Objects.requireNonNull(trust, "trust")));
    }

    /** Sets the options; valid before the first dial or listen. */
    public TlsWebSocketTransport options(HttpOptions options) {
        setOptions(options);
        return this;
    }

    @Override
    public String scheme() {
        return SCHEME;
    }

    @Override
    public Set<Capability> capabilities() {
        return Set.of(Capability.ENCRYPTED, Capability.ATTESTING, Capability.RELIABLE, Capability.ORDERED);
    }
}
