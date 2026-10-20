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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * One WebSocket connection carrying AgentSpaces frames (ISSUE-Websocket FR-4,
 * §9.8): one binary message is one frame, with no length prefix; a text
 * message closes with 1003, an oversize one with 1009, a protocol violation
 * with 1002; a ping is answered with a pong; a writer idle for the ping
 * interval pings and a reader idle for the idle timeout closes.
 */
final class WsConnection extends FrameConnection {

    static final long SEND_TIMEOUT_MILLIS = 30_000;

    private final Channel channel;

    WsConnection(Channel channel, String remote, Optional<PeerId> attested, X509Certificate[] chain,
                 HttpCounters counters) {
        super(remote, attested, chain, counters);
        this.channel = channel;
        channel.closeFuture().addListener(f -> ended());
        start();
    }

    /** The inbound handler that feeds this connection; it is added after the upgrade. */
    ChannelInboundHandlerAdapter handler(boolean answerControlFrames) {
        return new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                if (!(msg instanceof WebSocketFrame frame)) {
                    ctx.fireChannelRead(msg);
                    return;
                }
                try {
                    if (frame instanceof BinaryWebSocketFrame binary) {
                        ByteBuf content = binary.content();
                        byte[] bytes = new byte[content.readableBytes()];
                        content.readBytes(bytes);
                        deliver(bytes);
                        if (backlog() > QUEUE_BOUND) {
                            channel.config().setAutoRead(false); // a slow receiver stalls its own reads only
                        }
                    } else if (frame instanceof TextWebSocketFrame || frame instanceof ContinuationWebSocketFrame) {
                        closeWith(1003, "text messages are not frames");
                    } else if (answerControlFrames && frame instanceof PingWebSocketFrame ping) {
                        ctx.writeAndFlush(new PongWebSocketFrame(ping.content().retain()));
                    } else if (answerControlFrames && frame instanceof CloseWebSocketFrame close) {
                        counters.closed(close.statusCode());
                        ctx.writeAndFlush(new CloseWebSocketFrame(close.statusCode(), ""))
                                .addListener(ChannelFutureListener.CLOSE);
                    }
                } finally {
                    frame.release();
                }
            }

            @Override
            public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
                if (event instanceof IdleStateEvent idle) {
                    if (idle.state() == IdleState.READER_IDLE) {
                        closeWith(1001, "idle");
                    } else if (idle.state() == IdleState.WRITER_IDLE) {
                        ctx.writeAndFlush(new PingWebSocketFrame(Unpooled.EMPTY_BUFFER));
                    }
                    return;
                }
                ctx.fireUserEventTriggered(event);
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                if (cause instanceof TooLongFrameException) {
                    closeWith(1009, "frame too large");
                } else if (cause instanceof CorruptedFrameException) {
                    closeWith(1002, "protocol error");
                } else {
                    ctx.close();
                }
            }
        };
    }

    @Override
    void afterDelivery() {
        if (!channel.config().isAutoRead() && backlog() < QUEUE_BOUND / 2) {
            channel.config().setAutoRead(true);
        }
    }

    @Override
    public void send(byte[] frame) throws IOException {
        if (frame.length > HttpOptions.MAX_FRAME) {
            throw new IOException("frame exceeds " + HttpOptions.MAX_FRAME + " bytes: " + frame.length);
        }
        if (isClosed() || !channel.isActive()) {
            throw new IOException("connection to " + remoteAddress() + " is closed");
        }
        try {
            if (!channel.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(frame)))
                    .await(SEND_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                throw new IOException("send timed out to " + remoteAddress());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted sending to " + remoteAddress(), e);
        }
        counters.out(frame.length);
        if (!channel.isActive()) {
            throw new IOException("connection to " + remoteAddress() + " is closed");
        }
    }

    @Override
    public void close() {
        closeWith(1000, "");
    }

    void closeWith(int status, String reason) {
        counters.closed(status);
        if (channel.isActive()) {
            channel.writeAndFlush(new CloseWebSocketFrame(status, reason)).addListener(f -> channel.close());
            channel.eventLoop().schedule(() -> channel.close(), 1, TimeUnit.SECONDS);
        }
        ended();
    }
}
