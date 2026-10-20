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

import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.ChannelCertificate;
import ai.badmonkey.agentspaces.identity.ChannelTrust;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.identity.ServingCredential;
import ai.badmonkey.agentspaces.identity.tls.AttestLaterTrustManager;
import ai.badmonkey.agentspaces.identity.tls.ChannelAttestation;
import ai.badmonkey.agentspaces.identity.tls.SingleKeyManager;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The TLS side of {@code wss} and {@code https} (ISSUE-Websocket §9.5, SR-1 to
 * SR-4): TLS 1.3 only on the JDK provider, the lifted
 * {@link SingleKeyManager} and {@link AttestLaterTrustManager} that
 * {@code tls}, {@code quic}, and {@code dtls} use, the three identity modes
 * of SPEC §5.6, and the ASF-021 posture (no resumption; every session
 * invalidated right after attestation).
 */
final class TlsSetup {

    private final SSLContext context;
    private final Supplier<ChannelTrust> trust;
    private final boolean attests;

    private TlsSetup(SSLContext context, Supplier<ChannelTrust> trust, boolean attests) {
        this.context = context;
        this.trust = trust;
        this.attests = attests;
    }

    /**
     * The three identity modes: identity-endorsed (null serving and trust), CA
     * (a trust supplier, optionally a CA-issued serving credential).
     */
    static TlsSetup of(PeerIdentity identity, ServingCredential serving, Supplier<ChannelTrust> trust)
            throws GeneralSecurityException {
        Objects.requireNonNull(identity, "identity");
        ServingCredential credential = serving;
        if (credential == null) {
            ChannelCertificate channel = ChannelCertificate.generate(identity);
            credential = new ServingCredential(channel.tlsKey(), new X509Certificate[]{channel.certificate()});
        }
        return new TlsSetup(context(new KeyManager[]{new SingleKeyManager(credential)}), trust, true);
    }

    /** A server credential with no attestation: an operator certificate a browser trusts, for instance. */
    static TlsSetup serving(ServingCredential credential) throws GeneralSecurityException {
        return new TlsSetup(context(new KeyManager[]{new SingleKeyManager(credential)}), null, false);
    }

    /** A client that presents nothing and attests nothing (the exchange's dialer). */
    static TlsSetup anonymousClient() throws GeneralSecurityException {
        return new TlsSetup(context(null), null, false);
    }

    private static SSLContext context(KeyManager[] keys) throws GeneralSecurityException {
        SSLContext context = SSLContext.getInstance("TLSv1.3");
        context.init(keys, new TrustManager[]{new AttestLaterTrustManager()}, null);
        // ASF-021: no resumption, so every attestation rests on a full handshake.
        context.getClientSessionContext().setSessionCacheSize(1);
        context.getClientSessionContext().setSessionTimeout(1);
        context.getServerSessionContext().setSessionCacheSize(1);
        context.getServerSessionContext().setSessionTimeout(1);
        return context;
    }

    SSLContext context() {
        return context;
    }

    boolean attests() {
        return attests;
    }

    /** A server engine that requests (never requires) the client's channel certificate (SR-4). */
    SSLEngine serverEngine(boolean wantClientAuth) {
        SSLEngine engine = context.createSSLEngine();
        engine.setUseClientMode(false);
        engine.setEnabledProtocols(new String[]{"TLSv1.3"});
        if (wantClientAuth && attests) {
            engine.setWantClientAuth(true);
        }
        return engine;
    }

    /** A client engine for the dialed host, with SNI for a hostname (JSSE adds it). */
    SSLEngine clientEngine(String host, int port) {
        SSLEngine engine = context.createSSLEngine(host, port);
        engine.setUseClientMode(true);
        engine.setEnabledProtocols(new String[]{"TLSv1.3"});
        return engine;
    }

    /** The chain the session's remote end presented, leaf first; empty when none. */
    static X509Certificate[] remoteChain(SSLSession session) {
        try {
            return ChannelAttestation.x509(session.getPeerCertificates());
        } catch (SSLPeerUnverifiedException | RuntimeException e) {
            return new X509Certificate[0];
        }
    }

    /** Attests the chain (SPEC §5.6) when this setup attests; empty otherwise. */
    Optional<PeerId> attest(X509Certificate[] chain) {
        return attests ? ChannelAttestation.attest(chain, trust) : Optional.empty();
    }
}
