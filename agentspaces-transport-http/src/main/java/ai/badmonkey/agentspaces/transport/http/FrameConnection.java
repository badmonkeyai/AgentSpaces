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

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * The bookkeeping every HTTP-carried connection shares: inbound frames queue
 * and reach the receiver on the connection's own virtual thread, never on a
 * Netty event loop or an HTTP client thread, so a receiver that replies on
 * the same connection cannot block the I/O that would carry its reply; frames
 * that arrive before a receiver is registered wait for it; close listeners
 * fire once.
 */
abstract class FrameConnection implements TransportConnection {

    /** The inbound queue bound; a full queue pauses reading (ISSUE-Websocket §9.8). */
    static final int QUEUE_BOUND = 1024;
    private static final byte[] POISON = new byte[0];

    private final String remoteAddress;
    private final Optional<PeerId> attested;
    private final X509Certificate[] chain;
    final HttpCounters counters;
    private final LinkedBlockingQueue<byte[]> inbound = new LinkedBlockingQueue<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final List<Runnable> closeListeners = new ArrayList<>();
    private volatile Consumer<byte[]> receiver;
    private final Thread delivery;

    FrameConnection(String remoteAddress, Optional<PeerId> attested, X509Certificate[] chain,
                    HttpCounters counters) {
        this.remoteAddress = remoteAddress;
        this.attested = attested;
        this.chain = chain.clone();
        this.counters = counters;
        this.delivery = Thread.ofVirtual().name("http-deliver-" + remoteAddress).unstarted(this::drain);
    }

    /** Starts delivery; call once the subclass is fully built. */
    final void start() {
        delivery.start();
    }

    @Override
    public final String remoteAddress() {
        return remoteAddress;
    }

    @Override
    public final Optional<PeerId> attestedPeer() {
        return attested;
    }

    @Override
    public final X509Certificate[] remoteChain() {
        return chain.clone();
    }

    @Override
    public final void onReceive(Consumer<byte[]> receiver) {
        this.receiver = Objects.requireNonNull(receiver, "receiver");
        synchronized (this) {
            notifyAll();
        }
    }

    @Override
    public final void onClose(Runnable listener) {
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

    /** Whether the connection has ended. */
    final boolean isClosed() {
        return closed.get();
    }

    /** Hands an inbound frame to the delivery thread. */
    final void deliver(byte[] frame) {
        if (closed.get()) {
            return;
        }
        counters.in(frame.length);
        inbound.add(frame);
    }

    /** Frames waiting for the receiver, for backpressure. */
    final int backlog() {
        return inbound.size();
    }

    /** Marks the connection ended, stops delivery, and fires each close listener once. */
    final void ended() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        inbound.add(POISON);
        List<Runnable> listeners;
        synchronized (closeListeners) {
            listeners = new ArrayList<>(closeListeners);
            closeListeners.clear();
        }
        for (Runnable listener : listeners) {
            try {
                listener.run();
            } catch (RuntimeException e) {
                // one listener's failure never stops the others
            }
        }
    }

    private void drain() {
        try {
            while (true) {
                byte[] frame = inbound.take();
                if (frame == POISON) {
                    return;
                }
                Consumer<byte[]> current = receiver;
                while (current == null) {
                    synchronized (this) {
                        if (receiver == null) {
                            wait(250);
                        }
                    }
                    if (closed.get()) {
                        return;
                    }
                    current = receiver;
                }
                try {
                    current.accept(frame);
                } catch (RuntimeException e) {
                    // one frame the receiver cannot handle never stops the connection
                }
                afterDelivery();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Called after each delivery, so a subclass can resume reading once the backlog drains. */
    void afterDelivery() {
    }
}
