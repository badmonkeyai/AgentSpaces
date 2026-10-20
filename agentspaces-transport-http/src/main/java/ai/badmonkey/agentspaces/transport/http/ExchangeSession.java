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

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.concurrent.ScheduledFuture;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * The listening side of one HTTP exchange session (SRAD agentspaces-js
 * appendix C): frames sent to the client queue until a download request
 * takes them as a numbered batch; the batch is kept until the next request
 * acknowledges it, so a lost response is resent; a request with nothing to
 * take is held for the long-poll, then answered 204. A newer download request
 * supersedes a held one, which is answered 204, so a client whose connection
 * dropped mid-poll is never locked out. Unattested: the session proves its
 * peer by session attestation (SPEC §5.6).
 */
final class ExchangeSession extends FrameConnection {

    /** Outbound bytes held before {@link #send} waits (backpressure). */
    static final long OUTBOUND_BOUND = 16L * 1024 * 1024;

    final String id;
    private final HttpOptions options;
    private final Consumer<ExchangeSession> onEnd;
    private final ArrayDeque<byte[]> outbound = new ArrayDeque<>();
    private long outboundBytes;
    private long lastServedSeq;
    private List<byte[]> unacked;
    private long lastUploadSeq;
    private ChannelHandlerContext parked;
    private ResponseHeaders parkedHeaders;
    private ScheduledFuture<?> parkTimer;
    volatile long lastRequestNanos = System.nanoTime();

    /** Headers every response to this session carries (CORS echo of an allowed origin). */
    record ResponseHeaders(String allowOrigin, boolean keepAlive) {
    }

    ExchangeSession(String id, String remote, HttpOptions options, HttpCounters counters,
                    Consumer<ExchangeSession> onEnd) {
        super(remote, Optional.empty(), new X509Certificate[0], counters);
        this.id = id;
        this.options = options;
        this.onEnd = onEnd;
        onClose(() -> onEnd.accept(this));
        start();
    }

    void touch() {
        lastRequestNanos = System.nanoTime();
    }

    /** An upload: delivers once and in order; a repeat is acknowledged without redelivery; a gap is refused. */
    synchronized HttpResponseStatus upload(long seq, List<byte[]> frames) {
        touch();
        if (seq <= lastUploadSeq) {
            return HttpResponseStatus.NO_CONTENT;
        }
        if (seq != lastUploadSeq + 1) {
            return HttpResponseStatus.CONFLICT;
        }
        lastUploadSeq = seq;
        for (byte[] frame : frames) {
            deliver(frame);
        }
        return HttpResponseStatus.NO_CONTENT;
    }

    /** A download request acknowledging batch {@code ack}. */
    synchronized void download(ChannelHandlerContext ctx, long ack, ResponseHeaders headers) {
        touch();
        counters.longPolls.incrementAndGet();
        if (isClosed()) {
            reply(ctx, headers, HttpResponseStatus.GONE, null, 0);
            return;
        }
        if (parked != null) {
            releaseParked(HttpResponseStatus.NO_CONTENT); // superseded
        }
        if (unacked != null) {
            if (ack == lastServedSeq) {
                unacked = null; // acknowledged
            } else if (ack == lastServedSeq - 1) {
                reply(ctx, headers, HttpResponseStatus.OK, unacked, lastServedSeq); // the response was lost
                return;
            } else {
                reply(ctx, headers, HttpResponseStatus.CONFLICT, null, 0);
                return;
            }
        } else if (ack != lastServedSeq) {
            reply(ctx, headers, HttpResponseStatus.CONFLICT, null, 0);
            return;
        }
        if (!outbound.isEmpty()) {
            serve(ctx, headers);
            return;
        }
        parked = ctx;
        parkedHeaders = headers;
        parkTimer = ctx.executor().schedule(() -> {
            synchronized (ExchangeSession.this) {
                if (parked == ctx) {
                    releaseParked(HttpResponseStatus.NO_CONTENT);
                }
            }
        }, options.longPoll().toMillis(), TimeUnit.MILLISECONDS);
    }

    private void serve(ChannelHandlerContext ctx, ResponseHeaders headers) {
        List<byte[]> batch = new ArrayList<>();
        long bytes = 0;
        while (!outbound.isEmpty() && batch.size() < HttpOptions.MAX_BATCH_FRAMES
                && bytes + outbound.peek().length + 4 <= HttpOptions.MAX_BODY) {
            byte[] frame = outbound.poll();
            batch.add(frame);
            bytes += frame.length + 4;
            outboundBytes -= frame.length;
        }
        if (batch.isEmpty() && !outbound.isEmpty()) {
            batch.add(outbound.poll()); // a lone frame at the body cap still travels
        }
        notifyAll();
        lastServedSeq++;
        unacked = batch;
        reply(ctx, headers, HttpResponseStatus.OK, batch, lastServedSeq);
    }

    private void releaseParked(HttpResponseStatus status) {
        ChannelHandlerContext ctx = parked;
        ResponseHeaders headers = parkedHeaders;
        parked = null;
        parkedHeaders = null;
        if (parkTimer != null) {
            parkTimer.cancel(false);
            parkTimer = null;
        }
        if (ctx != null) {
            reply(ctx, headers, status, null, 0);
        }
    }

    @Override
    public void send(byte[] frame) throws IOException {
        if (frame.length > HttpOptions.MAX_FRAME) {
            throw new IOException("frame exceeds " + HttpOptions.MAX_FRAME + " bytes: " + frame.length);
        }
        synchronized (this) {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WsConnection.SEND_TIMEOUT_MILLIS);
            while (!isClosed() && outboundBytes + frame.length > OUTBOUND_BOUND && !outbound.isEmpty()) {
                long wait = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
                if (wait <= 0) {
                    throw new IOException("send timed out to " + remoteAddress());
                }
                try {
                    wait(wait);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted sending to " + remoteAddress(), e);
                }
            }
            if (isClosed()) {
                throw new IOException("exchange session " + id + " is closed");
            }
            outbound.add(frame);
            outboundBytes += frame.length;
            counters.out(frame.length);
            if (parked != null) {
                ChannelHandlerContext ctx = parked;
                ResponseHeaders headers = parkedHeaders;
                parked = null;
                parkedHeaders = null;
                if (parkTimer != null) {
                    parkTimer.cancel(false);
                    parkTimer = null;
                }
                serve(ctx, headers);
            }
        }
    }

    @Override
    public void close() {
        close(HttpResponseStatus.GONE);
    }

    /** The client asked to end the session: its parked poll is answered 204, since nothing is wrong. */
    void closeByClient() {
        close(HttpResponseStatus.NO_CONTENT);
    }

    private void close(HttpResponseStatus parkedStatus) {
        synchronized (this) {
            if (parked != null) {
                releaseParked(parkedStatus);
            }
            outbound.clear();
            notifyAll();
        }
        ended();
    }

    static void reply(ChannelHandlerContext ctx, ResponseHeaders headers, HttpResponseStatus status,
                      List<byte[]> frames, long seq) {
        byte[] body = frames == null ? new byte[0] : FrameBatch.encode(frames);
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status,
                Unpooled.wrappedBuffer(body));
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, body.length);
        response.headers().set(HttpHeaderNames.CACHE_CONTROL, "no-store");
        if (frames != null) {
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, FrameBatch.MEDIA_TYPE);
            response.headers().set(ExchangeBinding.SEQ, seq);
        }
        if (headers.allowOrigin() != null) {
            response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, headers.allowOrigin());
            response.headers().set(HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS, "Aspace-Seq, Location");
            response.headers().set(HttpHeaderNames.VARY, "Origin");
        }
        HttpUtil.setKeepAlive(response, headers.keepAlive());
        var future = ctx.writeAndFlush(response);
        if (!headers.keepAlive()) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }
}
