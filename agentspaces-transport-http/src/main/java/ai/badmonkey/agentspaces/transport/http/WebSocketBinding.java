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

import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.common.id.PeerId;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolConfig;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketDecoderConfig;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshakerFactory;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;

import java.io.IOException;
import java.net.URI;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * The WebSocket half of the HTTP module (ISSUE-Websocket §9.4): the upgrade
 * gate and handshake on the listening side, the Netty client on the dialing
 * side, both with subprotocol {@code aspace.1}, no extensions, 8 MiB
 * messages, and the 10 s dial bound.
 */
abstract class WebSocketBinding extends HttpBinding {

    /** The subprotocol both ends negotiate (FR-5): the binding version in token syntax. */
    public static final String SUBPROTOCOL = "aspace.1";
    static final long DIAL_TIMEOUT_MILLIS = 10_000;

    WebSocketBinding(TlsSetup tls) {
        super(tls);
    }

    private String urlScheme() {
        return tls == null ? "ws" : "wss";
    }

    @Override
    public AutoCloseable listen(String bindAddress, Consumer<TransportConnection> onAccept) throws IOException {
        Objects.requireNonNull(onAccept, "onAccept");
        HttpOptions options = use();
        HttpAddress bind = HttpAddress.parse(bindAddress, options.path());
        AutoCloseable registration = HttpServerCore.register(bind, tls, new UpgradeRoute(bind, options, onAccept));
        own(registration);
        return () -> {
            disown(registration);
            registration.close();
        };
    }

    /** The port a listener bound, for a bind address with port 0 (tests). */
    public int boundPort(String bindAddress) {
        return HttpServerCore.boundPort(HttpAddress.parse(bindAddress, options().path()));
    }

    /** The upgrade gate (ISSUE-Websocket §9.4): path, method, Origin, subprotocol, then the handshake. */
    private final class UpgradeRoute implements HttpServerCore.Route {
        private final HttpAddress bind;
        private final HttpOptions options;
        private final Consumer<TransportConnection> onAccept;

        UpgradeRoute(HttpAddress bind, HttpOptions options, Consumer<TransportConnection> onAccept) {
            this.bind = bind;
            this.options = options;
            this.onAccept = onAccept;
        }

        @Override
        public boolean serves(String path) {
            return path.equals(bind.path());
        }

        @Override
        public void handle(ChannelHandlerContext ctx, FullHttpRequest request, String path) {
            if (!request.headers().containsValue(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET, true)
                    || !HttpMethod.GET.equals(request.method())) {
                counters.refuse("not-upgrade");
                HttpServerCore.respond(ctx, request, HttpResponseStatus.UPGRADE_REQUIRED);
                return;
            }
            if (!options.origins().allows(request.headers().get(HttpHeaderNames.ORIGIN))) {
                counters.refuse("origin");
                HttpServerCore.respond(ctx, request, HttpResponseStatus.FORBIDDEN);
                return;
            }
            String offered = request.headers().get(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL, "");
            if (Arrays.stream(offered.split(",")).map(String::trim).noneMatch(SUBPROTOCOL::equals)) {
                counters.refuse("subprotocol");
                HttpServerCore.respond(ctx, request, HttpResponseStatus.BAD_REQUEST);
                return;
            }
            WebSocketDecoderConfig decoder = WebSocketDecoderConfig.newBuilder()
                    .maxFramePayloadLength(HttpOptions.MAX_FRAME)
                    .allowExtensions(false)
                    .expectMaskedFrames(true)
                    .allowMaskMismatch(false)
                    .build();
            WebSocketServerHandshaker handshaker = new WebSocketServerHandshakerFactory(
                    bind.url(urlScheme()), SUBPROTOCOL, decoder).newHandshaker(request);
            if (handshaker == null) {
                counters.refuse("version");
                WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(ctx.channel());
                return;
            }
            Channel channel = ctx.channel();
            X509Certificate[] chain = Optional.ofNullable(channel.attr(HttpServerCore.CHAIN).get())
                    .orElse(new X509Certificate[0]);
            Optional<PeerId> attested = tls == null ? Optional.empty() : tls.attest(chain);
            handshaker.handshake(channel, request).addListener(done -> {
                if (!done.isSuccess()) {
                    counters.refuse("handshake");
                    channel.close();
                    return;
                }
                counters.upgradesAccepted.incrementAndGet();
                ChannelPipeline pipeline = channel.pipeline();
                pipeline.remove("router");
                pipeline.remove("idle");
                WsConnection connection = track(new WsConnection(channel, String.valueOf(channel.remoteAddress()),
                        attested, chain, counters));
                pipeline.addLast("aggregate-ws", new WebSocketFrameAggregator(HttpOptions.MAX_FRAME));
                pipeline.addLast("idle-ws", new IdleStateHandler(options.idleTimeout().toMillis(),
                        options.pingInterval().toMillis(), 0, TimeUnit.MILLISECONDS));
                pipeline.addLast("frames", connection.handler(true));
                // onAccept may block (the node's attach); never on the event loop (FR-7).
                Thread.ofVirtual().name("ws-accept").start(() -> onAccept.accept(connection));
            });
        }
    }

    @Override
    public TransportConnection dial(String address) throws IOException {
        HttpOptions options = use();
        HttpAddress target = HttpAddress.parse(address, options.path());
        URI uri = URI.create(target.url(urlScheme()));
        WebSocketClientProtocolConfig config = WebSocketClientProtocolConfig.newBuilder()
                .webSocketUri(uri)
                .subprotocol(SUBPROTOCOL)
                .allowExtensions(false)
                .maxFramePayloadLength(HttpOptions.MAX_FRAME)
                .handshakeTimeoutMillis(DIAL_TIMEOUT_MILLIS)
                .dropPongFrames(true)
                .handleCloseFrames(true)
                .generateOriginHeader(false) // a peer is not a browser: no Origin (SR-5)
                .build();
        CompletableFuture<WsConnection> opened = new CompletableFuture<>();
        Bootstrap bootstrap = new Bootstrap()
                .group(NettyLoops.group())
                .channel(NioSocketChannel.class)
                .option(io.netty.channel.ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) DIAL_TIMEOUT_MILLIS)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) {
                        ChannelPipeline pipeline = channel.pipeline();
                        SslHandler ssl = tls == null ? null : new SslHandler(tls.clientEngine(target.host(), target.port()));
                        if (ssl != null) {
                            pipeline.addLast("ssl", ssl);
                        }
                        WebSocketClientProtocolHandler protocol = new WebSocketClientProtocolHandler(config);
                        pipeline.addLast("http", new HttpClientCodec());
                        pipeline.addLast("aggregate", new HttpObjectAggregator(65536));
                        pipeline.addLast("ws", protocol);
                        pipeline.addLast("aggregate-ws", new WebSocketFrameAggregator(HttpOptions.MAX_FRAME));
                        pipeline.addLast("idle-ws", new IdleStateHandler(options.idleTimeout().toMillis(),
                                options.pingInterval().toMillis(), 0, TimeUnit.MILLISECONDS));
                        pipeline.addLast("open", new ChannelInboundHandlerAdapter() {
                            @Override
                            public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
                                if (event == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
                                    WebSocketClientHandshaker handshaker = protocol.handshaker();
                                    if (!SUBPROTOCOL.equals(handshaker.actualSubprotocol())) {
                                        opened.completeExceptionally(new IOException(uri + " did not select "
                                                + SUBPROTOCOL));
                                        ctx.close();
                                        return;
                                    }
                                    X509Certificate[] chain = new X509Certificate[0];
                                    Optional<PeerId> attested = Optional.empty();
                                    if (ssl != null) {
                                        javax.net.ssl.SSLSession session = ssl.engine().getSession();
                                        chain = TlsSetup.remoteChain(session);
                                        attested = tls.attest(chain);
                                        session.invalidate(); // ASF-021
                                    }
                                    WsConnection connection = new WsConnection(ctx.channel(), target.toString(),
                                            attested, chain, counters);
                                    ctx.pipeline().replace(this, "frames", connection.handler(false));
                                    opened.complete(connection);
                                    return;
                                }
                                if (event == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_TIMEOUT) {
                                    opened.completeExceptionally(new IOException("WebSocket handshake with " + uri
                                            + " timed out"));
                                }
                                super.userEventTriggered(ctx, event);
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                opened.completeExceptionally(cause);
                                ctx.close();
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                                opened.completeExceptionally(new IOException("connection to " + uri + " closed"));
                                super.channelInactive(ctx);
                            }
                        });
                    }
                });
        Channel channel;
        try {
            channel = bootstrap.connect(target.socketAddress()).sync().channel();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted dialing " + uri, e);
        } catch (Exception e) {
            throw new IOException("cannot reach " + uri + ": " + e.getMessage(), e);
        }
        try {
            return track(opened.get(DIAL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.close();
            throw new IOException("interrupted dialing " + uri, e);
        } catch (ExecutionException e) {
            channel.close();
            Throwable cause = e.getCause();
            throw cause instanceof IOException io ? io
                    : new IOException("cannot open a WebSocket to " + uri + ": " + cause.getMessage(), cause);
        } catch (TimeoutException e) {
            channel.close();
            throw new IOException("WebSocket dial to " + uri + " timed out");
        }
    }
}
