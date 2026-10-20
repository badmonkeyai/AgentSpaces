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

package ai.badmonkey.agentspaces.transport.webrtc;

import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * One open {@code webrtc} connection: frames out as appendix B chunks on the engine session with
 * the backpressure of JS-W-06, frames in through the reassembler on a delivery thread of their
 * own, and the remote PeerID attested, because the channel opened only after DTLS proved the
 * fingerprint the remote signed (JS-W-04, decision 5).
 */
final class WebRtcConnection implements TransportConnection {

    private static final byte[] POISON = new byte[0];

    private final PeerId remote;
    private final DataChannelEngine.Session session;
    private final WebRtcOptions options;
    private final WebRtcCounters counters;
    private final int capacity;
    private final Chunks.Reassembler reassembler;
    private final LinkedBlockingQueue<byte[]> inbound = new LinkedBlockingQueue<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final List<Runnable> closeListeners = new ArrayList<>();
    private final Object writeLock = new Object();
    private final Object flow = new Object();
    private volatile Consumer<byte[]> receiver;
    private volatile Runnable onEnd = () -> {
    };
    private final Thread delivery;

    WebRtcConnection(PeerId remote, DataChannelEngine.Session session, WebRtcOptions options,
                     WebRtcCounters counters) {
        this.remote = Objects.requireNonNull(remote, "remote");
        this.session = Objects.requireNonNull(session, "session");
        this.options = options;
        this.counters = counters;
        this.capacity = Chunks.payloadCapacity(session.remoteMaxMessageSize());
        this.reassembler = new Chunks.Reassembler(options.maxFrame(), capacity);
        session.bufferedAmountLowThreshold(options.resumeBelow());
        this.delivery = Thread.ofVirtual().name("webrtc-deliver-" + remote.display()).unstarted(this::drain);
        delivery.start();
    }

    /** {@code C}: the payload bytes one chunk carries on this connection. */
    int chunkCapacity() {
        return capacity;
    }

    /** What the session tells the connection when it ends, so the owner can end it with a code. */
    void onEnd(Runnable action) {
        this.onEnd = Objects.requireNonNull(action);
    }

    @Override
    public String remoteAddress() {
        return "sig/" + remote.value();
    }

    @Override
    public Optional<PeerId> attestedPeer() {
        return Optional.of(remote);
    }

    @Override
    public void send(byte[] frame) throws IOException {
        Objects.requireNonNull(frame, "frame");
        if (closed.get()) {
            throw new IOException("webrtc connection to " + remote.display() + " is closed");
        }
        if (frame.length == 0) {
            return; // nothing to carry; a chunk may not have an empty payload (appendix B)
        }
        if (frame.length > options.maxFrame()) {
            throw new IOException("frame of " + frame.length + " bytes exceeds " + options.maxFrame());
        }
        synchronized (writeLock) {
            for (byte[] chunk : Chunks.split(frame, capacity, options.maxFrame())) {
                awaitWritable();
                session.send(ByteBuffer.wrap(chunk));
                counters.sent(chunk.length);
            }
        }
    }

    /**
     * JS-W-06: pause while {@code bufferedAmount} exceeds {@code pauseAbove}, resume at the
     * buffered-amount-low event below {@code resumeBelow}; fail after {@code sendBlocked} paused.
     */
    private void awaitWritable() throws IOException {
        if (session.bufferedAmount() <= options.pauseAbove()) {
            return;
        }
        long deadline = System.nanoTime() + options.sendBlocked().toNanos();
        synchronized (flow) {
            while (session.bufferedAmount() > options.pauseAbove()) {
                long left = deadline - System.nanoTime();
                if (closed.get()) {
                    throw new IOException("webrtc connection to " + remote.display() + " closed while paused");
                }
                if (left <= 0) {
                    counters.failed("blocked");
                    close();
                    throw new IOException("webrtc send to " + remote.display() + " paused longer than "
                            + options.sendBlocked());
                }
                try {
                    flow.wait(Math.max(1, Math.min(250, left / 1_000_000)));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted while paused", e);
                }
            }
        }
    }

    /** The engine's buffered-amount-low event. */
    void bufferedAmountLow() {
        synchronized (flow) {
            flow.notifyAll();
        }
    }

    /**
     * One binary message from the engine. Returns the reason to end the connection with
     * {@code END(3)} on a malformed chunk or an oversize frame, or null.
     */
    String message(ByteBuffer data) {
        if (closed.get()) {
            return null;
        }
        counters.received(data.remaining());
        byte[] frame;
        try {
            frame = reassembler.accept(data);
        } catch (Chunks.ProtocolViolation e) {
            counters.failed("protocol");
            return e.getMessage();
        }
        if (frame != null) {
            inbound.add(frame);
        }
        return null;
    }

    @Override
    public void onReceive(Consumer<byte[]> receiver) {
        this.receiver = Objects.requireNonNull(receiver, "receiver");
        synchronized (this) {
            notifyAll();
        }
    }

    @Override
    public void onClose(Runnable listener) {
        Objects.requireNonNull(listener, "listener");
        boolean already;
        synchronized (closeListeners) {
            already = closed.get();
            if (!already) {
                closeListeners.add(listener);
            }
        }
        if (already) {
            listener.run();
        }
    }

    boolean isClosed() {
        return closed.get();
    }

    private void drain() {
        try {
            while (true) {
                byte[] frame = inbound.take();
                if (frame == POISON) {
                    return;
                }
                Consumer<byte[]> target = receiver;
                while (target == null) {
                    synchronized (this) {
                        if (closed.get()) {
                            return;
                        }
                        wait(100);
                    }
                    target = receiver;
                }
                try {
                    target.accept(frame);
                } catch (RuntimeException e) {
                    // one bad frame never stops delivery
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** The engine reported the end: release the delivery thread and the close listeners. */
    void ended() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        inbound.add(POISON);
        synchronized (flow) {
            flow.notifyAll();
        }
        synchronized (this) {
            notifyAll();
        }
        List<Runnable> listeners;
        synchronized (closeListeners) {
            listeners = new ArrayList<>(closeListeners);
            closeListeners.clear();
        }
        for (Runnable listener : listeners) {
            try {
                listener.run();
            } catch (RuntimeException e) {
                // a listener's failure never stops the others
            }
        }
    }

    @Override
    public void close() {
        if (closed.get()) {
            return;
        }
        Runnable end = onEnd;
        ended();
        try {
            end.run();
        } catch (RuntimeException e) {
            // the session's own end never keeps the connection from closing
        }
        try {
            session.close();
        } catch (RuntimeException e) {
            // an engine that fails to close is closed as far as this connection is concerned
        }
    }

    @Override
    public String toString() {
        return "webrtc(" + remote.display() + ")";
    }
}
