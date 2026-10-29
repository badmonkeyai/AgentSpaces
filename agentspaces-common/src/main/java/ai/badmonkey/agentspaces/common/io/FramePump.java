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
package ai.badmonkey.agentspaces.common.io;

import java.util.ArrayDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Hands a stream connection's inbound frames from its reader to one consumer thread, in order.
 *
 * <p>A node answers some frames by sending (a gossip forward, a probe ack, a piece). Run inline
 * on the reader, a send that waits on a full socket stops the reader too, and two peers whose
 * readers each wait on a send to the other stop for good: neither drains the socket the other
 * is writing to. With the pump the reader keeps draining while the consumer waits, so a send
 * waits only for the far reader, which always reads. The queue is bounded by bytes; past the
 * bound the reader waits, which is TCP's own back pressure and needs a consumer that far behind
 * on both ends of a cycle to stall (srad/large-networks-bulk-data.md 10.4, found by LB-Q-PRF-01).
 *
 * <p>The TCP and TLS bindings use it from their reader threads, and the QUIC binding from its
 * event loop, where a send that waits on a backed-up stream is refused outright.
 */
public final class FramePump {

    /** Inbound bytes a connection may queue before its reader waits. */
    public static final long DEFAULT_CAP_BYTES = 64L * 1024 * 1024;

    private final ArrayDeque<byte[]> queue = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();
    private final long capBytes;
    private long queuedBytes;
    private boolean ended;
    private boolean stopped;
    private boolean overCap;
    private volatile Runnable onRelief = () -> {
    };

    /**
     * Starts the consumer.
     *
     * @param name     the consumer thread's name
     * @param receiver the current receiver, read for each frame
     * @param onFail   run when the receiver throws: the connection is closed
     * @param onEnd    run once every queued frame is delivered after {@link #end()}, or after a failure
     * @param capBytes the byte bound of the queue
     */
    public FramePump(String name, Supplier<Consumer<byte[]>> receiver, Runnable onFail, Runnable onEnd, long capBytes) {
        this.capBytes = capBytes;
        Thread.ofVirtual().name(name).start(() -> {
            try {
                while (true) {
                    byte[] frame;
                    boolean relieved;
                    lock.lock();
                    try {
                        while (queue.isEmpty() && !ended) {
                            notEmpty.await();
                        }
                        if (queue.isEmpty()) {
                            return;
                        }
                        frame = queue.pollFirst();
                        queuedBytes -= frame.length;
                        notFull.signalAll();
                        relieved = overCap && queuedBytes <= capBytes / 2;
                        if (relieved) {
                            overCap = false;
                        }
                    } finally {
                        lock.unlock();
                    }
                    if (relieved) {
                        onRelief.run();
                    }
                    try {
                        receiver.get().accept(frame);
                    } catch (RuntimeException e) {
                        // as when the reader delivered inline: a receiver that throws ends the connection
                        stop();
                        onFail.run();
                        return;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                stop();
                onEnd.run();
            }
        });
    }

    /**
     * Queues a frame, waiting while the queue holds its byte bound (a frame larger than the bound
     * is let in alone).
     *
     * @return false when the consumer has stopped and the reader should end
     */
    public boolean offer(byte[] frame) throws InterruptedException {
        lock.lock();
        try {
            while (!stopped && queuedBytes > 0 && queuedBytes + frame.length > capBytes) {
                notFull.await();
            }
            if (stopped) {
                return false;
            }
            queue.addLast(frame);
            queuedBytes += frame.length;
            notEmpty.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Queues a frame without waiting, for a reader that must never block (an event loop). A caller
     * told the queue is past its bound pauses reading itself and resumes when {@link #onRelief}
     * runs.
     *
     * @return false when the queue is past its byte bound or the consumer has stopped
     */
    public boolean offerNow(byte[] frame) {
        lock.lock();
        try {
            if (stopped) {
                return false;
            }
            queue.addLast(frame);
            queuedBytes += frame.length;
            notEmpty.signal();
            if (queuedBytes > capBytes) {
                overCap = true;
                return false;
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Sets what runs on the consumer's thread when a queue that went past its bound through
     * {@link #offerNow} drains to half of it.
     */
    public void onRelief(Runnable action) {
        this.onRelief = java.util.Objects.requireNonNull(action, "action");
    }

    /** Whether the consumer has stopped (the receiver failed or the stream ended). */
    public boolean stopped() {
        lock.lock();
        try {
            return stopped;
        } finally {
            lock.unlock();
        }
    }

    /** The reader is done: the consumer delivers what is queued and then runs its end action. */
    public void end() {
        lock.lock();
        try {
            ended = true;
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void stop() {
        lock.lock();
        try {
            stopped = true;
            ended = true;
            queue.clear();
            queuedBytes = 0;
            notFull.signalAll();
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Bytes waiting for the consumer. */
    public long queuedBytes() {
        lock.lock();
        try {
            return queuedBytes;
        } finally {
            lock.unlock();
        }
    }
}
