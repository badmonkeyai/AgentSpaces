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
package ai.badmonkey.agentspaces.peering.transport;

import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.ChannelCertificate;
import ai.badmonkey.agentspaces.identity.ChannelTrust;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.identity.ServingCredential;
import ai.badmonkey.agentspaces.identity.tls.AttestLaterTrustManager;
import ai.badmonkey.agentspaces.identity.tls.ChannelAttestation;
import ai.badmonkey.agentspaces.identity.tls.SingleKeyManager;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * The TLS binding of the TCP transport (spec §5.6): the same length-prefixed
 * frames as {@link TcpTransport}, inside TLS (RFC 8446) with mutual channel
 * certificates. Each side presents its identity-endorsed
 * {@link ChannelCertificate}; the handshake proves possession of the
 * certificate's TLS key, the endorsement proves the peer identity stands
 * behind it, and the connection then reports the attested PeerID from
 * {@link TransportConnection#attestedPeer()}, which lets the wire layer accept
 * unsigned envelopes from exactly that peer. A remote end presenting no
 * certificate, or one that fails attestation, still connects; its channel just
 * stays unauthenticated and its frames stay fully signed.
 *
 * <p>No CA is involved: the TLS trust managers accept any certificate, and the
 * only trust decision is {@link ChannelCertificate#attest}. Addresses are
 * {@code host:port} and the advertised scheme is {@code tls}.
 */
public final class TlsTcpTransport implements Transport {

    private static final int MAX_FRAME = 8 * 1024 * 1024;

    private final SSLContext context;
    /** CA trust for attestation (enterprise mode), read on every handshake so
     * an operator's refreshed CRL takes effect on the next handshake without
     * a restart; null = self-signed mode. */
    private final java.util.function.Supplier<ChannelTrust> trust;

    /**
     * Creates the transport in the default mode, generating this peer's
     * identity-endorsed channel certificate.
     *
     * @param identity the local peer identity endorsing the channel certificate
     * @throws GeneralSecurityException when certificate generation fails
     */
    public TlsTcpTransport(PeerIdentity identity) throws GeneralSecurityException {
        this(identity, null, null);
    }

    /**
     * Creates the transport in the enterprise-CA mode (plan §3/WS4): this node
     * serves the given CA-issued credential, and peers attest only when their
     * chain validates to the trust's anchors — with the trust's revocation
     * policy — and their leaf CN carries their PeerID. A null {@code serving}
     * keeps the self-signed identity-endorsed certificate for this node while
     * still requiring CA validation of peers; a null {@code trust} keeps the
     * default self-signed attestation.
     *
     * @param identity this node's peer identity
     * @param serving  the CA-issued credential to present, or null
     * @param trust    the CA trust peers must validate against, or null
     * @throws GeneralSecurityException when key material cannot be prepared
     */
    public TlsTcpTransport(PeerIdentity identity, ServingCredential serving,
                           ChannelTrust trust) throws GeneralSecurityException {
        this(identity, serving, trust == null ? null : () -> trust, false);
    }

    /**
     * Creates the transport in the enterprise-CA mode with a <em>refreshable</em>
     * trust (v0.1.9 CA revocation as the authoritative eject): the supplier is
     * consulted on every handshake, so when an operator installs a fresh CRL
     * (a new {@link ChannelTrust}) the very next handshake from a revoked
     * peer attests nothing, and a node built with
     * {@code requireAttestation(true)} evicts it. {@link ChannelTrust} itself
     * stays immutable; the supplier is the one moving part.
     *
     * @param identity this node's peer identity
     * @param serving  the CA-issued credential to present, or null for the
     *                 self-signed identity-endorsed certificate
     * @param trust    supplies the current CA trust; must never return null
     * @return the transport
     * @throws GeneralSecurityException when key material cannot be prepared
     */
    public static TlsTcpTransport withRefreshableTrust(
            PeerIdentity identity, ServingCredential serving,
            java.util.function.Supplier<ChannelTrust> trust) throws GeneralSecurityException {
        return new TlsTcpTransport(identity, serving,
                Objects.requireNonNull(trust, "trust"), false);
    }

    private TlsTcpTransport(PeerIdentity identity, ServingCredential serving,
                            java.util.function.Supplier<ChannelTrust> trust, boolean unused)
            throws GeneralSecurityException {
        Objects.requireNonNull(identity, "identity");
        this.trust = trust;
        ServingCredential credential = serving;
        if (credential == null) {
            ChannelCertificate channel = ChannelCertificate.generate(identity);
            credential = new ServingCredential(channel.tlsKey(),
                    new X509Certificate[]{channel.certificate()});
        }
        this.context = SSLContext.getInstance("TLS");
        this.context.init(
                new javax.net.ssl.KeyManager[]{new SingleKeyManager(credential)},
                new javax.net.ssl.TrustManager[]{new AttestLaterTrustManager()},
                null);
        // ASF-021: an attestation must be pinned to a certificate the completed
        // handshake authenticated with a fresh CertificateVerify. Resumed
        // sessions skip that proof, so resumption is disabled: no cached
        // sessions worth reusing, and each session is invalidated right after
        // it is attested (below). JSSE implements no TLS 1.3 0-RTT early data,
        // so no early-data path exists to police.
        this.context.getClientSessionContext().setSessionCacheSize(1);
        this.context.getClientSessionContext().setSessionTimeout(1);
        this.context.getServerSessionContext().setSessionCacheSize(1);
        this.context.getServerSessionContext().setSessionTimeout(1);
    }

    @Override
    public String scheme() {
        return "tls";
    }

    @Override
    public java.util.Set<Capability> capabilities() {
        return java.util.Set.of(Capability.ENCRYPTED, Capability.ATTESTING,
                Capability.RELIABLE, Capability.ORDERED);
    }

    @Override
    public TransportConnection dial(String address) throws IOException {
        InetSocketAddress target = TcpTransport.parse(address);
        SSLSocket socket = (SSLSocket) context.getSocketFactory().createSocket();
        socket.connect(target, 5_000);
        socket.setUseClientMode(true);
        socket.startHandshake();
        X509Certificate[] presented = remoteChainOf(socket);
        Optional<PeerId> attested = attestOf(presented);
        // The attestation above came from this handshake; forbid resuming the
        // session so it can never vouch for a later connection (ASF-021).
        socket.getSession().invalidate();
        return new TlsConnection(socket, attested, presented);
    }

    @Override
    public AutoCloseable listen(String bindAddress, Consumer<TransportConnection> onAccept)
            throws IOException {
        Objects.requireNonNull(onAccept, "onAccept");
        InetSocketAddress bind = TcpTransport.parse(bindAddress);
        SSLServerSocket server =
                (SSLServerSocket) context.getServerSocketFactory().createServerSocket();
        server.bind(bind);
        // Request the client's channel certificate without requiring one:
        // a peer without a certificate still connects, merely unattested.
        server.setWantClientAuth(true);
        Thread acceptor = Thread.ofVirtual().name("tls-accept-" + bind.getPort()).start(() -> {
            while (!server.isClosed()) {
                try {
                    SSLSocket socket = (SSLSocket) server.accept();
                    // The handshake blocks, so it runs off the accept loop.
                    Thread.ofVirtual().name("tls-handshake").start(() -> {
                        try {
                            socket.startHandshake();
                            X509Certificate[] presented = remoteChainOf(socket);
                            Optional<PeerId> attested = attestOf(presented);
                            // Attested by this handshake alone (ASF-021).
                            socket.getSession().invalidate();
                            onAccept.accept(new TlsConnection(socket, attested, presented));
                        } catch (IOException e) {
                            try {
                                socket.close();
                            } catch (IOException ignored) {
                                // Best effort.
                            }
                        }
                    });
                } catch (IOException e) {
                    return; // closed
                }
            }
        });
        return () -> {
            server.close();
            acceptor.interrupt();
        };
    }

    private Optional<PeerId> attestOf(X509Certificate[] x509) {
        // Enterprise mode: the CA (and its revocation) vouches for the
        // PeerID binding. Default mode: the identity's own endorsement does.
        return ChannelAttestation.attest(x509, trust);
    }

    /** The chain the remote end presented, leaf first; empty when none or not X.509. */
    private static X509Certificate[] remoteChainOf(SSLSocket socket) {
        try {
            return ChannelAttestation.x509(socket.getSession().getPeerCertificates());
        } catch (SSLPeerUnverifiedException e) {
            return new X509Certificate[0];
        }
    }

    /** The TCP frame loop over an established TLS socket. */
    private static final class TlsConnection implements TransportConnection {
        private final SSLSocket socket;
        private final Optional<PeerId> attested;
        private final X509Certificate[] presented;
        private final DataOutputStream out;
        private volatile Consumer<byte[]> receiver = frame -> {
        };
        private volatile boolean readerStarted;

        private TlsConnection(SSLSocket socket, Optional<PeerId> attested,
                              X509Certificate[] presented) throws IOException {
            this.socket = socket;
            this.attested = attested;
            this.presented = presented.clone();
            this.out = new DataOutputStream(socket.getOutputStream());
        }

        @Override
        public String remoteAddress() {
            return socket.getRemoteSocketAddress().toString();
        }

        @Override
        public Optional<PeerId> attestedPeer() {
            return attested;
        }

        @Override
        public X509Certificate[] remoteChain() {
            return presented.clone();
        }

        @Override
        public synchronized void send(byte[] frame) throws IOException {
            if (frame.length > MAX_FRAME) {
                throw new IOException("frame exceeds " + MAX_FRAME + " bytes: " + frame.length);
            }
            out.writeInt(frame.length);
            out.write(frame);
            out.flush();
        }

        @Override
        public void onReceive(Consumer<byte[]> receiver) {
            this.receiver = Objects.requireNonNull(receiver, "receiver");
            if (!readerStarted) {
                readerStarted = true;
                Thread.ofVirtual().name("tls-read-" + remoteAddress()).start(this::readLoop);
            }
        }

        private void readLoop() {
            try (DataInputStream in = new DataInputStream(socket.getInputStream())) {
                while (!socket.isClosed()) {
                    int length = in.readInt();
                    if (length < 0 || length > MAX_FRAME) {
                        return;
                    }
                    byte[] frame = new byte[length];
                    in.readFully(frame);
                    receiver.accept(frame);
                }
            } catch (EOFException | java.net.SocketException e) {
                // Peer closed (spec P2: absence is the signal); the node is told below.
            } catch (IOException e) {
                // Connection failed; the node is told below and membership confirms by probe.
            } finally {
                notifyClosed();
            }
        }

        private final java.util.concurrent.atomic.AtomicBoolean closeNotified =
                new java.util.concurrent.atomic.AtomicBoolean();
        private volatile Runnable closeListener = () -> {
        };

        @Override
        public void onClose(Runnable listener) {
            this.closeListener = Objects.requireNonNull(listener, "listener");
        }

        private void notifyClosed() {
            if (closeNotified.compareAndSet(false, true)) {
                closeListener.run();
            }
        }

        @Override
        public void close() {
            try {
                socket.close();
            } catch (IOException ignored) {
                // Closing is best-effort.
            }
            notifyClosed();
        }
    }
}
