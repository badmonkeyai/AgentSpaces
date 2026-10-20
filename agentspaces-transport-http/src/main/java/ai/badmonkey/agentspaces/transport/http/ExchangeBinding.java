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
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.concurrent.ScheduledFuture;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * The request and response exchange of {@code http} and {@code https} (SRAD
 * agentspaces-js §3.3, appendix C) for networks that refuse the WebSocket
 * upgrade: {@code POST P/open}, {@code POST P/s/<sid>/up} with
 * {@code Aspace-Seq}, {@code GET P/s/<sid>/down} with {@code Aspace-Ack},
 * {@code DELETE P/s/<sid>}. The listening side runs in Netty; the dialing
 * side is {@link ExchangeClient} on the JDK's {@code HttpClient}.
 */
abstract class ExchangeBinding extends HttpBinding {

    static final String SEQ = "Aspace-Seq";
    static final String ACK = "Aspace-Ack";
    private static final SecureRandom RANDOM = new SecureRandom();

    ExchangeBinding(TlsSetup tls) {
        super(tls);
    }

    final String urlScheme() {
        return tls == null ? "http" : "https";
    }

    @Override
    public AutoCloseable listen(String bindAddress, Consumer<TransportConnection> onAccept) throws IOException {
        Objects.requireNonNull(onAccept, "onAccept");
        HttpOptions options = use();
        HttpAddress bind = HttpAddress.parse(bindAddress, options.exchangePath());
        ExchangeRoute route = new ExchangeRoute(bind.path(), options, onAccept);
        AutoCloseable registration = HttpServerCore.register(bind, tls, route);
        AutoCloseable handle = () -> {
            route.stop();
            registration.close();
        };
        own(handle);
        return () -> {
            disown(handle);
            handle.close();
        };
    }

    /** The port a listener bound, for a bind address with port 0 (tests). */
    public int boundPort(String bindAddress) {
        return HttpServerCore.boundPort(HttpAddress.parse(bindAddress, options().exchangePath()));
    }

    @Override
    public TransportConnection dial(String address) throws IOException {
        HttpOptions options = use();
        HttpAddress target = HttpAddress.parse(address, options.exchangePath());
        return track(ExchangeClient.open(target.url(urlScheme()), tls != null, options, counters));
    }

    static String newSessionId() {
        byte[] bytes = new byte[16];
        RANDOM.nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    /** The listening side's route under the base path. */
    private final class ExchangeRoute implements HttpServerCore.Route {
        private final String base;
        private final HttpOptions options;
        private final Consumer<TransportConnection> onAccept;
        private final Map<String, ExchangeSession> sessions = new ConcurrentHashMap<>();
        private final Map<String, long[]> opens = new ConcurrentHashMap<>();
        private final ScheduledFuture<?> sweeper;

        ExchangeRoute(String base, HttpOptions options, Consumer<TransportConnection> onAccept) {
            this.base = base;
            this.options = options;
            this.onAccept = onAccept;
            long idle = options.idleTimeout().toNanos();
            this.sweeper = NettyLoops.group().next().scheduleAtFixedRate(() -> {
                long now = System.nanoTime();
                for (ExchangeSession session : sessions.values()) {
                    if (now - session.lastRequestNanos > idle) {
                        session.close();
                    }
                }
                opens.entrySet().removeIf(e -> now - e.getValue()[0] > TimeUnit.SECONDS.toNanos(5));
            }, 5, 5, TimeUnit.SECONDS);
        }

        void stop() {
            sweeper.cancel(false);
            for (ExchangeSession session : sessions.values()) {
                session.close();
            }
        }

        @Override
        public boolean serves(String path) {
            return path.equals(base + "/open") || path.startsWith(base + "/s/");
        }

        @Override
        public void handle(ChannelHandlerContext ctx, FullHttpRequest request, String path) {
            String origin = request.headers().get(HttpHeaderNames.ORIGIN);
            boolean allowed = options.origins().allows(origin);
            ExchangeSession.ResponseHeaders headers = new ExchangeSession.ResponseHeaders(
                    origin != null && allowed ? origin : null, HttpUtil.isKeepAlive(request));
            if (HttpMethod.OPTIONS.equals(request.method())) {
                preflight(ctx, request, origin, allowed);
                return;
            }
            if (!allowed) {
                counters.refuse("origin");
                ExchangeSession.reply(ctx, headers, HttpResponseStatus.FORBIDDEN, null, 0);
                return;
            }
            if (path.equals(base + "/open")) {
                open(ctx, request, headers);
                return;
            }
            String rest = path.substring((base + "/s/").length());
            int slash = rest.indexOf('/');
            String sid = slash < 0 ? rest : rest.substring(0, slash);
            String verb = slash < 0 ? "" : rest.substring(slash + 1);
            ExchangeSession session = sessions.get(sid);
            if (session == null) {
                ExchangeSession.reply(ctx, headers, HttpResponseStatus.NOT_FOUND, null, 0);
                return;
            }
            if (HttpMethod.DELETE.equals(request.method()) && verb.isEmpty()) {
                session.closeByClient();
                ExchangeSession.reply(ctx, headers, HttpResponseStatus.NO_CONTENT, null, 0);
            } else if (HttpMethod.POST.equals(request.method()) && verb.equals("up")) {
                upload(ctx, request, headers, session);
            } else if (HttpMethod.GET.equals(request.method()) && verb.equals("down")) {
                long ack = number(request.headers().get(ACK));
                if (ack < 0) {
                    ExchangeSession.reply(ctx, headers, HttpResponseStatus.BAD_REQUEST, null, 0);
                    return;
                }
                session.download(ctx, ack, headers);
            } else {
                ExchangeSession.reply(ctx, headers, HttpResponseStatus.METHOD_NOT_ALLOWED, null, 0);
            }
        }

        private void open(ChannelHandlerContext ctx, FullHttpRequest request,
                          ExchangeSession.ResponseHeaders headers) {
            if (!HttpMethod.POST.equals(request.method())) {
                ExchangeSession.reply(ctx, headers, HttpResponseStatus.METHOD_NOT_ALLOWED, null, 0);
                return;
            }
            String source = ctx.channel().remoteAddress() instanceof InetSocketAddress inet
                    ? inet.getAddress().getHostAddress() : String.valueOf(ctx.channel().remoteAddress());
            long now = System.nanoTime();
            long[] window = opens.computeIfAbsent(source, s -> new long[]{now, 0});
            synchronized (window) {
                if (now - window[0] > TimeUnit.SECONDS.toNanos(1)) {
                    window[0] = now;
                    window[1] = 0;
                }
                if (++window[1] > options.opensPerSourcePerSecond()) {
                    counters.refuse("rate");
                    ExchangeSession.reply(ctx, headers, HttpResponseStatus.TOO_MANY_REQUESTS, null, 0);
                    return;
                }
            }
            String sid = newSessionId();
            ExchangeSession session = new ExchangeSession(sid, source + "/" + sid, options, counters,
                    ended -> sessions.remove(ended.id, ended));
            sessions.put(sid, session);
            track(session);
            counters.exchangesOpened.incrementAndGet();
            FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CREATED);
            response.headers().set(HttpHeaderNames.LOCATION, base + "/s/" + sid);
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
            response.headers().set(HttpHeaderNames.CACHE_CONTROL, "no-store");
            if (headers.allowOrigin() != null) {
                response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, headers.allowOrigin());
                response.headers().set(HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS, "Aspace-Seq, Location");
                response.headers().set(HttpHeaderNames.VARY, "Origin");
            }
            HttpUtil.setKeepAlive(response, headers.keepAlive());
            ctx.writeAndFlush(response);
            Thread.ofVirtual().name("exchange-accept").start(() -> onAccept.accept(session));
        }

        private void upload(ChannelHandlerContext ctx, FullHttpRequest request,
                            ExchangeSession.ResponseHeaders headers, ExchangeSession session) {
            long seq = number(request.headers().get(SEQ));
            if (seq < 1) {
                ExchangeSession.reply(ctx, headers, HttpResponseStatus.BAD_REQUEST, null, 0);
                return;
            }
            ByteBuf content = request.content();
            byte[] body = new byte[content.readableBytes()];
            content.readBytes(body);
            List<byte[]> frames;
            try {
                frames = FrameBatch.decode(body);
            } catch (IOException e) {
                counters.refuse("malformed");
                ExchangeSession.reply(ctx, headers, HttpResponseStatus.BAD_REQUEST, null, 0);
                return;
            }
            ExchangeSession.reply(ctx, headers, session.upload(seq, frames), null, 0);
        }

        private void preflight(ChannelHandlerContext ctx, FullHttpRequest request, String origin, boolean allowed) {
            if (origin == null || !allowed) {
                counters.refuse("origin");
                HttpServerCore.respond(ctx, request, HttpResponseStatus.FORBIDDEN);
                return;
            }
            FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                    HttpResponseStatus.NO_CONTENT);
            response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, origin);
            response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS, "POST, GET, DELETE");
            response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS, "Aspace-Seq, Aspace-Ack, Content-Type");
            response.headers().set(HttpHeaderNames.ACCESS_CONTROL_MAX_AGE, 600);
            response.headers().set(HttpHeaderNames.VARY, "Origin");
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
            HttpUtil.setKeepAlive(response, HttpUtil.isKeepAlive(request));
            ctx.writeAndFlush(response);
        }
    }

    private static long number(String value) {
        if (value == null) {
            return -1;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}
