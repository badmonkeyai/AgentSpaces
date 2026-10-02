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
package ai.badmonkey.agentspaces.transport.quic;

import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.identity.ChannelCertificate;
import ai.badmonkey.agentspaces.identity.ChannelTrust;
import ai.badmonkey.agentspaces.identity.ServingCredential;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.incubator.codec.quic.InsecureQuicTokenHandler;
import io.netty.incubator.codec.quic.QuicChannel;
import io.netty.incubator.codec.quic.QuicClientCodecBuilder;
import io.netty.incubator.codec.quic.QuicServerCodecBuilder;
import io.netty.incubator.codec.quic.QuicSslContext;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import io.netty.incubator.codec.quic.QuicStreamType;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The QUIC (RFC 9000) binding of the transport SPI (spec §5.5), on Netty's
 * incubator QUIC codec over quiche: one bidirectional QUIC stream per peer
 * connection carries the same length-prefixed frames as the TCP transport, so
 * everything above the transport — membership, gossip, spaces — runs unchanged.
 * Addresses are {@code host:port}, and the scheme in peer advertisements is
 * {@code quic}.
 *
 * <p>What QUIC adds over TCP here: transport-level encryption always on, loss
 * recovery without head-of-line blocking between connections, faster
 * reconnection, and connection migration across address changes. This
 * binding deliberately uses neither 0-RTT early data nor session resumption
 * (ASF-021 parity with the TLS binding): every connection runs a full
 * handshake, so an attestation is always pinned to a certificate the
 * handshake proved possession of.
 *
 * <p>Identity comes in two modes. The no-argument constructor keeps the
 * original posture: an ephemeral unverified certificate, confidentiality only,
 * and every frame Ed25519-signed at the envelope layer. Constructed with a
 * {@link PeerIdentity}, the transport instead presents that identity's
 * {@link ChannelCertificate} on both sides of the handshake and attests the
 * remote end from the certificate it presented (spec §5.6), reporting the
 * bound PeerID from {@code attestedPeer()} so the wire layer can accept
 * unsigned envelopes from exactly that peer. A remote end without a channel
 * certificate still connects, merely unattested. In the enterprise-CA mode
 * ({@link #QuicTransport(PeerIdentity, ServingCredential, ChannelTrust)} and
 * {@link #withRefreshableTrust}), the node presents a CA-issued credential and
 * a remote end attests only when its full presented chain validates under the
 * {@link ChannelTrust}, with the trust's revocation policy, consulted afresh
 * for every connection — exactly as over TLS.
 *
 * <p>A future refinement maps capability pipes to QUIC streams one-to-one
 * (spec §9); this binding uses one stream per connection, which the SPI's
 * single-frame-channel model reflects today.
 *
 * <p>The transport owns a small Netty event loop; {@link #close()} releases it.
 * One instance serves any number of listens and dials, like the TCP transport.
 */
public final class QuicTransport implements Transport, AutoCloseable {

    /** The ALPN protocol name AgentSpaces peers negotiate. */
    public static final String ALPN = "aspace/1";

    private static final int MAX_FRAME = 8 * 1024 * 1024;
    private static final long DIAL_TIMEOUT_MILLIS = 10_000;
    private static final long SEND_TIMEOUT_MILLIS = 30_000;
    private static final long IDLE_TIMEOUT_MILLIS = 60_000;

    private final NioEventLoopGroup group = new NioEventLoopGroup(1);
    /** The key this node presents; null in unattested mode. */
    private final java.security.PrivateKey presentedKey;
    /** The chain this node presents (leaf first); null in unattested mode. */
    private final java.security.cert.X509Certificate[] presentedChain;
    /** The CA trust peers must validate against; null for identity-endorsed attestation. */
    private final Supplier<ChannelTrust> trust;
    private final boolean attesting;

    /** Creates the transport in unattested mode: encryption only. */
    public QuicTransport() {
        this.presentedKey = null;
        this.presentedChain = null;
        this.trust = null;
        this.attesting = false;
    }

    /**
     * Creates the transport in attested mode (spec §5.6): both handshake sides
     * present the identity's channel certificate, and connections report the
     * attested remote PeerID.
     *
     * @param identity the local peer identity endorsing the channel certificate
     * @throws GeneralSecurityException when certificate generation fails
     */
    public QuicTransport(PeerIdentity identity) throws GeneralSecurityException {
        this(identity, null, null, false);
    }

    /**
     * Creates the transport in the enterprise-CA mode (spec §5.6, TODO item 11):
     * this node presents the CA-issued credential, and peers attest only when
     * their chain validates to the trust's anchors under its revocation policy
     * and their leaf CN carries their PeerID. A null {@code serving} keeps the
     * identity-endorsed certificate for this node while still requiring CA
     * validation of peers; a null {@code trust} keeps identity-endorsed
     * attestation.
     *
     * @param identity this node's peer identity
     * @param serving  the CA-issued credential to present, or null
     * @param trust    the CA trust peers must validate against, or null
     * @throws GeneralSecurityException when key material cannot be prepared
     */
    public QuicTransport(PeerIdentity identity, ServingCredential serving, ChannelTrust trust)
            throws GeneralSecurityException {
        this(identity, serving, trust == null ? null : () -> trust, false);
    }

    /**
     * Creates the transport in the enterprise-CA mode with a refreshable trust:
     * the supplier is consulted for every connection, so a fresh CRL makes the
     * next connection from a revoked peer attest nothing.
     *
     * @param identity this node's peer identity
     * @param serving  the CA-issued credential to present, or null
     * @param trust    supplies the current CA trust; must never return null
     * @return the transport
     * @throws GeneralSecurityException when key material cannot be prepared
     */
    public static QuicTransport withRefreshableTrust(PeerIdentity identity,
                                                     ServingCredential serving,
                                                     Supplier<ChannelTrust> trust)
            throws GeneralSecurityException {
        return new QuicTransport(identity, serving, Objects.requireNonNull(trust, "trust"), false);
    }

    private QuicTransport(PeerIdentity identity, ServingCredential serving,
                          Supplier<ChannelTrust> trust, boolean unused)
            throws GeneralSecurityException {
        Objects.requireNonNull(identity, "identity");
        if (serving != null) {
            this.presentedKey = serving.key();
            this.presentedChain = serving.chain().clone();
        } else {
            ChannelCertificate channel = ChannelCertificate.generate(identity);
            this.presentedKey = channel.tlsKey();
            this.presentedChain = new java.security.cert.X509Certificate[]{channel.certificate()};
        }
        this.trust = trust;
        this.attesting = true;
    }

    /**
     * ASF-021 parity (TODO item 11): no 0-RTT early data and no reusable
     * sessions on either side, so every connection runs a full handshake whose
     * CertificateVerify proves the presented key. The client engine is also
     * created without a peer host and port, so the client session cache has no
     * key to store a session under; quiche 0.0.75 exposes no switch to stop a
     * server from issuing tickets to third-party clients, but no attestation
     * here ever rests on a resumed session, since every connection is attested
     * from the chain under the current trust.
     */
    private static QuicSslContextBuilder noResumption(QuicSslContextBuilder builder) {
        return builder.earlyData(false).sessionCacheSize(1).sessionTimeout(1);
    }

    @Override
    public String scheme() {
        return "quic";
    }

    @Override
    public TransportConnection dial(String address) throws IOException {
        InetSocketAddress target = parse(address);
        try {
            QuicSslContextBuilder clientSsl = noResumption(QuicSslContextBuilder.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .applicationProtocols(ALPN));
            if (attesting) {
                clientSsl.keyManager(presentedKey, null, presentedChain);
            }
            QuicSslContext ssl = clientSsl.build();
            ChannelHandler codec = new QuicClientCodecBuilder()
                    .sslContext(ssl)
                    .maxIdleTimeout(IDLE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                    .initialMaxData(MAX_FRAME * 2L)
                    .initialMaxStreamDataBidirectionalLocal(MAX_FRAME)
                    .initialMaxStreamDataBidirectionalRemote(MAX_FRAME)
                    .initialMaxStreamsBidirectional(16)
                    .build();
            Channel udp = new Bootstrap()
                    .group(group)
                    .channel(NioDatagramChannel.class)
                    .handler(codec)
                    .bind(0)
                    .sync()
                    .channel();
            QuicChannel quic = QuicChannel.newBootstrap(udp)
                    .handler(new ChannelInboundHandlerAdapter())
                    .streamHandler(new ChannelInboundHandlerAdapter())
                    .remoteAddress(target)
                    .connect()
                    .get(DIAL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            QuicConnection connection = new QuicConnection(address, udp);
            // The dialer's connect future completes with the handshake, so the
            // server's chain is final here.
            connection.remoteChain = remoteChainOf(quic);
            connection.attested = attesting ? attestOf(connection.remoteChain, trust)
                    : java.util.Optional.empty();
            connection.attestationDone = true;
            QuicStreamChannel stream = quic.createStream(QuicStreamType.BIDIRECTIONAL,
                            framePipeline(connection))
                    .sync()
                    .getNow();
            connection.stream = stream;
            return connection;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted dialing " + address, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IOException("cannot reach " + address + " over QUIC", e);
        }
    }

    @Override
    public AutoCloseable listen(String bindAddress, Consumer<TransportConnection> onAccept)
            throws IOException {
        Objects.requireNonNull(onAccept, "onAccept");
        InetSocketAddress bind = parse(bindAddress);
        try {
            QuicSslContext ssl;
            if (attesting) {
                ssl = noResumption(QuicSslContextBuilder
                        .forServer(presentedKey, null, presentedChain)
                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                        .clientAuth(io.netty.handler.ssl.ClientAuth.OPTIONAL)
                        .applicationProtocols(ALPN))
                        .build();
            } else {
                EphemeralCertificate certificate = EphemeralCertificate.generate("agentspaces");
                ssl = noResumption(QuicSslContextBuilder
                        .forServer(certificate.key(), null, certificate.cert())
                        .applicationProtocols(ALPN))
                        .build();
            }
            ChannelHandler codec = new QuicServerCodecBuilder()
                    .sslContext(ssl)
                    .maxIdleTimeout(IDLE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                    .initialMaxData(MAX_FRAME * 2L)
                    .initialMaxStreamDataBidirectionalLocal(MAX_FRAME)
                    .initialMaxStreamDataBidirectionalRemote(MAX_FRAME)
                    .initialMaxStreamsBidirectional(16)
                    .tokenHandler(InsecureQuicTokenHandler.INSTANCE)
                    // One handler instance serves every accepted QUIC connection,
                    // so it must be @Sharable: a plain adapter is refused by the
                    // second connection's pipeline, and the listener then
                    // accepted exactly one connection (found porting the CA tests).
                    .handler(SharedConnectionHandler.INSTANCE)
                    .streamHandler(new ChannelInitializer<QuicStreamChannel>() {
                        @Override
                        protected void initChannel(QuicStreamChannel stream) {
                            // The peer's first (and only) stream is the
                            // connection from the SPI's point of view.
                            QuicConnection connection = new QuicConnection(
                                    remoteOf(stream), null);
                            connection.stream = stream;
                            // Attested lazily, on first ask: a stream can be
                            // initialized before the handshake's last flight is
                            // processed, and attesting then would read an honest
                            // member as unattested (and evict it under
                            // requireAttestation). By the time any frame is
                            // delivered the handshake has completed.
                            if (attesting) {
                                connection.attestFrom((QuicChannel) stream.parent(), trust);
                            } else {
                                connection.attestationDone = true;
                            }
                            stream.pipeline().addLast(frameHandlers(connection));
                            onAccept.accept(connection);
                        }
                    })
                    .build();
            Channel server = new Bootstrap()
                    .group(group)
                    .channel(NioDatagramChannel.class)
                    .handler(codec)
                    .bind(bind)
                    .sync()
                    .channel();
            return server::close;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted binding " + bindAddress, e);
        } catch (GeneralSecurityException e) {
            throw new IOException("cannot create the transport certificate", e);
        }
    }

    /** Releases the transport's event loop. Open connections close with it. */
    @Override
    public void close() {
        group.shutdownGracefully(0, 500, TimeUnit.MILLISECONDS);
    }

    /** Parses {@code host:port}, the transport's address format. */
    public static InetSocketAddress parse(String address) {
        int colon = address.lastIndexOf(':');
        if (colon <= 0) {
            throw new IllegalArgumentException("expected host:port, got: " + address);
        }
        return new InetSocketAddress(address.substring(0, colon),
                Integer.parseInt(address.substring(colon + 1)));
    }

    private ChannelInitializer<QuicStreamChannel> framePipeline(QuicConnection connection) {
        return new ChannelInitializer<>() {
            @Override
            protected void initChannel(QuicStreamChannel stream) {
                stream.pipeline().addLast(frameHandlers(connection));
            }
        };
    }

    private static ChannelHandler[] frameHandlers(QuicConnection connection) {
        return new ChannelHandler[]{
                new LengthFieldBasedFrameDecoder(MAX_FRAME, 0, 4, 0, 4),
                new LengthFieldPrepender(4),
                new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) {
                        ByteBuf buf = (ByteBuf) msg;
                        try {
                            byte[] frame = new byte[buf.readableBytes()];
                            buf.readBytes(frame);
                            connection.deliver(frame);
                        } finally {
                            buf.release();
                        }
                    }
                }};
    }

    private static String remoteOf(QuicStreamChannel stream) {
        Object remote = stream.parent().remoteSocketAddress();
        return remote == null ? "quic:unknown" : remote.toString();
    }

    /**
     * Attests the remote end of a QUIC channel from the certificate its
     * handshake presented, or empty when none was presented or it proves
     * nothing.
     */
    static java.util.Optional<ai.badmonkey.agentspaces.common.id.PeerId> attestOf(
            QuicChannel quic, Supplier<ChannelTrust> trust) {
        return attestOf(remoteChainOf(quic), trust);
    }

    static java.util.Optional<ai.badmonkey.agentspaces.common.id.PeerId> attestOf(
            java.security.cert.X509Certificate[] x509, Supplier<ChannelTrust> trust) {
        if (x509.length == 0) {
            return java.util.Optional.empty();
        }
        try {
            // CA mode validates the whole presented chain under the current
            // trust; identity-endorsed mode attests the leaf (spec §5.6).
            return trust != null ? trust.get().attest(x509) : ChannelCertificate.attest(x509[0]);
        } catch (RuntimeException e) {
            return java.util.Optional.empty();
        }
    }

    /** The chain the remote end of a QUIC channel presented, leaf first; empty when none. */
    static java.security.cert.X509Certificate[] remoteChainOf(QuicChannel quic) {
        try {
            java.security.cert.Certificate[] chain =
                    quic.sslEngine().getSession().getPeerCertificates();
            if (chain == null) {
                return new java.security.cert.X509Certificate[0];
            }
            java.security.cert.X509Certificate[] x509 =
                    new java.security.cert.X509Certificate[chain.length];
            for (int i = 0; i < chain.length; i++) {
                if (!(chain[i] instanceof java.security.cert.X509Certificate certificate)) {
                    return new java.security.cert.X509Certificate[0];
                }
                x509[i] = certificate;
            }
            return x509;
        } catch (javax.net.ssl.SSLPeerUnverifiedException | RuntimeException e) {
            return new java.security.cert.X509Certificate[0];
        }
    }

    /** The accepted QUIC connection's own handler: nothing to do, shareable across connections. */
    @ChannelHandler.Sharable
    private static final class SharedConnectionHandler extends ChannelInboundHandlerAdapter {
        static final SharedConnectionHandler INSTANCE = new SharedConnectionHandler();
    }

    /** One QUIC stream as a frame connection. */
    private static final class QuicConnection implements TransportConnection {

        private final String remoteAddress;
        private final Channel udp; // dialer-owned; null on the accepting side
        private final Queue<byte[]> pending = new ConcurrentLinkedQueue<>();
        private volatile QuicStreamChannel stream;
        private volatile Consumer<byte[]> receiver;
        private volatile java.util.Optional<ai.badmonkey.agentspaces.common.id.PeerId>
                attested = java.util.Optional.empty();
        private volatile boolean attestationDone;
        private volatile java.security.cert.X509Certificate[] remoteChain =
                new java.security.cert.X509Certificate[0];
        private volatile QuicChannel attestChannel;
        private volatile Supplier<ChannelTrust> attestTrust;

        /**
         * Runs a read of the channel's TLS state on the channel's own event
         * loop: the quiche engine is not safe to touch from other threads.
         */
        private static <T> T onEventLoop(QuicChannel channel, java.util.concurrent.Callable<T> read) {
            try {
                if (channel.eventLoop().inEventLoop()) {
                    return read.call();
                }
                return channel.eventLoop().submit(read)
                        .get(DIAL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted attesting a QUIC channel", e);
            } catch (Exception e) {
                throw new IllegalStateException("cannot attest a QUIC channel", e);
            }
        }

        /** Defers attestation to the first {@link #attestedPeer()} call. */
        void attestFrom(QuicChannel channel, Supplier<ChannelTrust> trust) {
            this.attestChannel = channel;
            this.attestTrust = trust;
        }

        private QuicConnection(String remoteAddress, Channel udp) {
            this.remoteAddress = remoteAddress;
            this.udp = udp;
        }

        @Override
        public String remoteAddress() {
            return remoteAddress;
        }

        @Override
        public java.util.Optional<ai.badmonkey.agentspaces.common.id.PeerId> attestedPeer() {
            if (!attestationDone) {
                synchronized (this) {
                    if (!attestationDone) {
                        if (attestChannel != null) {
                            remoteChain = onEventLoop(attestChannel, () -> remoteChainOf(attestChannel));
                            attested = attestOf(remoteChain, attestTrust);
                        }
                        attestationDone = true;
                    }
                }
            }
            return attested;
        }

        @Override
        public java.security.cert.X509Certificate[] remoteChain() {
            attestedPeer(); // the chain is read where attestation reads it
            return remoteChain.clone();
        }

        @Override
        public void send(byte[] frame) throws IOException {
            if (frame.length > MAX_FRAME) {
                throw new IOException("frame exceeds " + MAX_FRAME + " bytes: " + frame.length);
            }
            try {
                if (!stream.writeAndFlush(Unpooled.wrappedBuffer(frame))
                        .await(SEND_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                    throw new IOException("send timed out to " + remoteAddress);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted sending to " + remoteAddress, e);
            }
            if (!stream.isActive()) {
                throw new IOException("connection to " + remoteAddress + " is closed");
            }
        }

        @Override
        public void onReceive(Consumer<byte[]> receiver) {
            this.receiver = Objects.requireNonNull(receiver, "receiver");
            byte[] queued;
            while ((queued = pending.poll()) != null) {
                receiver.accept(queued);
            }
        }

        /** Delivers inbound frames, queueing any that arrive before the receiver. */
        void deliver(byte[] frame) {
            Consumer<byte[]> current = receiver;
            if (current == null) {
                pending.add(frame);
                // A receiver registered while we queued drains the queue itself;
                // re-check so no frame is stranded between the races.
                current = receiver;
                if (current != null) {
                    byte[] queued;
                    while ((queued = pending.poll()) != null) {
                        current.accept(queued);
                    }
                }
            } else {
                current.accept(frame);
            }
        }

        @Override
        public void close() {
            QuicStreamChannel current = stream;
            if (current != null) {
                current.parent().close();
            }
            if (udp != null) {
                udp.close();
            }
        }
    }
}
