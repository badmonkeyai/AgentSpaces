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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AttributeKey;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * One listening port serving the HTTP bindings registered on it (SRAD
 * agentspaces-js JS-H-02): {@code ws} and {@code http} share a plaintext
 * port, {@code wss} and {@code https} a TLS port, each routed by path, and any
 * other path answers 404 with an empty body. The pre-upgrade HTTP is bounded
 * (ISSUE-Websocket SR-6): 4 KiB request line, 8 KiB of headers, the exchange's
 * 9 MiB body cap, and a connection idle for longer than any long-poll closes.
 * On a TLS port the presented chain is read at the handshake, kept on the
 * channel for the route to attest, and the session invalidated (ASF-021).
 */
final class HttpServerCore {

    /** A route's view of one request. */
    interface Route {
        /** Whether this route serves the path. */
        boolean serves(String path);

        /** Handles the request; the core releases it afterwards. */
        void handle(ChannelHandlerContext ctx, FullHttpRequest request, String path);
    }

    static final AttributeKey<X509Certificate[]> CHAIN = AttributeKey.valueOf("aspace.chain");

    private static final Map<String, HttpServerCore> SERVERS = new HashMap<>();

    private final String key;
    private final boolean tls;
    private volatile Channel server;
    private final List<Route> routes = new CopyOnWriteArrayList<>();

    private HttpServerCore(String key, boolean tls) {
        this.key = key;
        this.tls = tls;
    }

    /**
     * Registers a route on the listener at {@code bind}, starting the listener
     * when it is the first; the returned handle unregisters it and stops the
     * listener with its last route.
     */
    static AutoCloseable register(HttpAddress bind, TlsSetup setup, Route route) throws IOException {
        String key = bind.authority();
        synchronized (SERVERS) {
            HttpServerCore core = SERVERS.get(key);
            if (core == null) {
                core = start(key, bind, setup);
                SERVERS.put(key, core);
            } else if (core.tls != (setup != null)) {
                throw new IOException(key + " is already listening " + (core.tls ? "with" : "without")
                        + " TLS; ws and http share a plaintext port, wss and https a TLS port");
            }
            core.routes.add(route);
            HttpServerCore registered = core;
            return () -> {
                synchronized (SERVERS) {
                    registered.routes.remove(route);
                    if (registered.routes.isEmpty()) {
                        SERVERS.remove(registered.key, registered);
                        registered.server.close();
                    }
                }
            };
        }
    }

    /** The port the listener at {@code bind} actually bound (for a bind of port 0). */
    static int boundPort(HttpAddress bind) {
        synchronized (SERVERS) {
            HttpServerCore core = SERVERS.get(bind.authority());
            return core == null ? -1 : ((InetSocketAddress) core.server.localAddress()).getPort();
        }
    }

    private static HttpServerCore start(String key, HttpAddress bind, TlsSetup setup) throws IOException {
        // The core exists before the port does, so no accepted connection can meet a missing router.
        HttpServerCore core = new HttpServerCore(key, setup != null);
        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(NettyLoops.group())
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) {
                        ChannelPipeline pipeline = channel.pipeline();
                        if (setup != null) {
                            SslHandler ssl = new SslHandler(setup.serverEngine(true));
                            pipeline.addLast("ssl", ssl);
                            pipeline.addLast("attest", new io.netty.channel.ChannelInboundHandlerAdapter() {
                                @Override
                                public void userEventTriggered(ChannelHandlerContext ctx, Object event)
                                        throws Exception {
                                    if (event instanceof SslHandshakeCompletionEvent done && done.isSuccess()) {
                                        javax.net.ssl.SSLSession session = ssl.engine().getSession();
                                        ctx.channel().attr(CHAIN).set(TlsSetup.remoteChain(session));
                                        session.invalidate(); // attested by this handshake alone (ASF-021)
                                        ctx.pipeline().remove(this);
                                    }
                                    super.userEventTriggered(ctx, event);
                                }
                            });
                        }
                        pipeline.addLast("http", new HttpServerCodec(4096, 8192, 8192));
                        pipeline.addLast("aggregate", new HttpObjectAggregator(HttpOptions.MAX_BODY + 1024));
                        pipeline.addLast("idle", new IdleStateHandler(0, 0, 120, TimeUnit.SECONDS));
                        pipeline.addLast("router", core.router());
                    }
                });
        try {
            core.server = bootstrap.bind(bind.socketAddress()).sync().channel();
            return core;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted binding " + key, e);
        } catch (Exception e) {
            throw new IOException("cannot bind " + key + ": " + e.getMessage(), e);
        }
    }

    private SimpleChannelInboundHandler<FullHttpRequest> router() {
        return new SimpleChannelInboundHandler<>() {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
                String path = new QueryStringDecoder(request.uri()).path();
                for (Route route : routes) {
                    if (route.serves(path)) {
                        route.handle(ctx, request, path);
                        return;
                    }
                }
                respond(ctx, request, HttpResponseStatus.NOT_FOUND);
            }

            @Override
            public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
                if (event instanceof IdleStateEvent) {
                    ctx.close();
                    return;
                }
                super.userEventTriggered(ctx, event);
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                ctx.close();
            }
        };
    }

    /** An empty response with the given status; closes the connection unless the request keeps it alive. */
    static void respond(ChannelHandlerContext ctx, FullHttpRequest request, HttpResponseStatus status) {
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, Unpooled.EMPTY_BUFFER);
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
        response.headers().set(HttpHeaderNames.CACHE_CONTROL, "no-store");
        boolean keepAlive = HttpUtil.isKeepAlive(request) && status.code() < 400;
        HttpUtil.setKeepAlive(response, keepAlive);
        var future = ctx.writeAndFlush(response);
        if (!keepAlive) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }
}
