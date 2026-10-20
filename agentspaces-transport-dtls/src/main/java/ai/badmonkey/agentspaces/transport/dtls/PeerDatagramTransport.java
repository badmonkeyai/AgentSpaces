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

package ai.badmonkey.agentspaces.transport.dtls;

import org.bouncycastle.tls.DatagramTransport;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * BouncyCastle's view of one association's datagrams (SRAD §6.2). Inbound
 * datagrams arrive in a bounded queue fed by a socket read loop (the
 * listener's demultiplexer, or a dialer's own socket); BouncyCastle's single
 * reader polls it with a wait, which suits a virtual thread per association.
 * Outbound datagrams go to the current remote address, which moves when the
 * association migrates (Connection ID); until the new path is validated the
 * association sends at most three times what it received from it
 * (SRAD DT-K-04, the RFC 9000 §8.1 anti-amplification rule).
 */
final class PeerDatagramTransport implements DatagramTransport {

    /** Writes one datagram to an address. */
    interface Sender {
        void send(byte[] buf, int off, int len, InetSocketAddress to) throws IOException;
    }

    /** One received datagram and where it came from. */
    record Datagram(byte[] bytes, InetSocketAddress source) {
    }

    static final int QUEUE = 256;
    /** Larger than any datagram a peer may send (the mtu range tops out at 9,000). */
    static final int RECEIVE_LIMIT = 65_507;
    static final int AMPLIFICATION = 3;

    private final ArrayBlockingQueue<Datagram> inbound = new ArrayBlockingQueue<>(QUEUE);
    private final Sender sender;
    private final int mtu;
    private final Runnable onClose;
    private volatile InetSocketAddress remote;
    private volatile InetSocketAddress lastSource;
    private volatile boolean closed;
    private boolean budgeted;
    private long receivedOnPath;
    private long sentOnPath;

    PeerDatagramTransport(Sender sender, int mtu, InetSocketAddress remote, Runnable onClose) {
        this.sender = sender;
        this.mtu = mtu;
        this.remote = remote;
        this.onClose = onClose;
    }

    /** Queues an inbound datagram; false when the queue is full (the datagram is lost, recovery resends). */
    boolean offer(Datagram datagram) {
        return !closed && inbound.offer(datagram);
    }

    InetSocketAddress remote() {
        return remote;
    }

    /** The source of the datagram BouncyCastle read last. */
    InetSocketAddress lastSource() {
        return lastSource;
    }

    boolean isClosed() {
        return closed;
    }

    /** Moves the association to a new remote address, unvalidated. */
    synchronized void migrateTo(InetSocketAddress address, int bytesJustReceived) {
        remote = address;
        budgeted = true;
        receivedOnPath = bytesJustReceived;
        sentOnPath = 0;
    }

    /** The new path acknowledged data sent on it: lift the budget. */
    synchronized void pathValidated() {
        budgeted = false;
    }

    synchronized boolean budgeted() {
        return budgeted;
    }

    @Override
    public int getReceiveLimit() {
        return RECEIVE_LIMIT;
    }

    @Override
    public int getSendLimit() {
        return mtu;
    }

    @Override
    public int receive(byte[] buf, int off, int len, int waitMillis) throws IOException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(0, waitMillis));
        while (true) {
            if (closed) {
                // Throwing ends BouncyCastle's handshake or read at once instead of spinning to its timeout.
                throw new IOException("transport closed");
            }
            long remaining = deadline - System.nanoTime();
            Datagram datagram;
            try {
                datagram = remaining <= 0 ? inbound.poll() : inbound.poll(remaining, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException("interrupted");
            }
            if (datagram == null) {
                if (closed) {
                    throw new IOException("transport closed");
                }
                return -1; // timeout
            }
            if (closed) {
                throw new IOException("transport closed");
            }
            if (datagram.bytes().length > len) {
                continue; // cannot be a valid record for this association
            }
            lastSource = datagram.source();
            synchronized (this) {
                if (budgeted && datagram.source().equals(remote)) {
                    receivedOnPath += datagram.bytes().length;
                }
            }
            System.arraycopy(datagram.bytes(), 0, buf, off, datagram.bytes().length);
            return datagram.bytes().length;
        }
    }

    @Override
    public void send(byte[] buf, int off, int len) throws IOException {
        if (closed) {
            throw new IOException("transport closed");
        }
        InetSocketAddress to;
        synchronized (this) {
            if (budgeted) {
                if (sentOnPath + len > AMPLIFICATION * receivedOnPath) {
                    return; // over the unvalidated path's budget: dropped, recovery resends
                }
                sentOnPath += len;
            }
            to = remote;
        }
        sender.send(buf, off, len, to);
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            inbound.clear();
            inbound.offer(new Datagram(new byte[0], remote)); // wakes a reader blocked in poll
            onClose.run();
        }
    }
}
