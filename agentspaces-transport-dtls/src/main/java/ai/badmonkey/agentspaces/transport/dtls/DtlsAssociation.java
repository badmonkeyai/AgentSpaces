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

import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.common.id.PeerId;
import org.bouncycastle.tls.DTLSRecordFlags;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * One DTLS association as a {@link TransportConnection} (SRAD §3.2): a
 * {@link RecordLayer} (a BouncyCastle DTLS association, or bare UDP) carrying the recovery sublayer, so frames
 * arrive once, in order, or the association fails.
 *
 * <p>Threads: a drain thread is BouncyCastle's single reader (records in, PDUs
 * to {@link Recovery}); a delivery thread hands completed frames to the
 * receiver, so a slow receiver never stalls acknowledgements; the transport's
 * timer runs the sublayer's deadlines; {@link #send} runs on the caller's
 * thread. The {@link Recovery} instance's monitor serializes the sublayer.
 */
final class DtlsAssociation implements TransportConnection {

    static final long SEND_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final int DELIVERY_QUEUE = 64;

    private final RecordLayer dtls;
    private final PeerDatagramTransport path;
    private final Optional<PeerId> attested;
    private final X509Certificate[] chain;
    private final ScheduledExecutorService timers;
    private final DtlsCounters counters;
    private final Runnable onRelease;
    private final Recovery recovery;
    private final ArrayBlockingQueue<byte[]> deliveries = new ArrayBlockingQueue<>(DELIVERY_QUEUE);
    private final AtomicBoolean finished = new AtomicBoolean();
    private final AtomicBoolean closeNotified = new AtomicBoolean();
    private final Object timerLock = new Object();
    private ScheduledFuture<?> timer;
    private long armedAt = Long.MAX_VALUE;
    private volatile Consumer<byte[]> receiver;
    private volatile boolean deliveryStarted;
    private volatile Runnable closeListener = () -> {
    };

    DtlsAssociation(RecordLayer dtls, PeerDatagramTransport path, Optional<PeerId> attested,
                    X509Certificate[] chain, DtlsOptions options, ScheduledExecutorService timers,
                    DtlsCounters counters, Runnable onRelease) throws IOException {
        this.dtls = dtls;
        this.path = path;
        this.attested = attested;
        this.chain = chain.clone();
        this.timers = timers;
        this.counters = counters;
        this.onRelease = onRelease;
        int fragment = dtls.sendLimit() - Pdu.DATA_HEADER;
        this.recovery = new Recovery(this::transmit, fragment, options.window(),
                options.idleTimeout().toNanos(), System::nanoTime, counters);
    }

    /** Starts the drain thread and the timers; called once, after construction. */
    void start() {
        Thread.ofVirtual().name("dtls-drain-" + path.remote()).start(this::drain);
        rearm();
    }

    // ---------------------------------------------------------------- SPI

    @Override
    public String remoteAddress() {
        return path.remote().toString();
    }

    @Override
    public Optional<PeerId> attestedPeer() {
        return attested;
    }

    @Override
    public X509Certificate[] remoteChain() {
        return chain.clone();
    }

    @Override
    public void send(byte[] frame) throws IOException {
        Objects.requireNonNull(frame, "frame");
        long deadline = System.nanoTime() + SEND_TIMEOUT_NANOS;
        synchronized (recovery) {
            while (!recovery.offer(frame)) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    counters.failedSendTimeout.increment();
                    recovery.fail(Pdu.CLOSE_NORMAL, "send timed out", true);
                    break;
                }
                try {
                    recovery.wait(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new java.io.InterruptedIOException("interrupted while sending");
                }
            }
        }
        rearm();
        if (finishIfFailed()) {
            throw new IOException("send timed out to " + remoteAddress());
        }
    }

    @Override
    public void onReceive(Consumer<byte[]> receiver) {
        this.receiver = Objects.requireNonNull(receiver, "receiver");
        if (!deliveryStarted) {
            deliveryStarted = true;
            Thread.ofVirtual().name("dtls-deliver-" + path.remote()).start(this::deliver);
        }
    }

    @Override
    public void onClose(Runnable listener) {
        this.closeListener = Objects.requireNonNull(listener, "listener");
        if (finished.get()) {
            listener.run(); // closed before anyone listened
        }
    }

    @Override
    public void close() {
        synchronized (recovery) {
            recovery.close();
            recovery.notifyAll();
        }
        finish();
    }

    // ------------------------------------------------------------ threads

    private void transmit(byte[] pdu) throws IOException {
        dtls.send(pdu, 0, pdu.length);
    }

    private void drain() {
        try {
            byte[] buf = new byte[dtls.receiveLimit()];
            while (!finished.get() && !path.isClosed()) {
                int n = dtls.receive(buf, 0, buf.length, 1_000, this::recordAccepted);
                if (n < 0) {
                    continue;
                }
                List<byte[]> frames;
                synchronized (recovery) {
                    frames = recovery.onPdu(buf, 0, n);
                    recovery.notifyAll();
                    if (path.budgeted() && recovery.pathValidated()) {
                        path.pathValidated();
                    }
                }
                rearm();
                for (byte[] frame : frames) {
                    // Backpressure: a full delivery queue stops reading, and the peer's
                    // retransmissions wait until the receiver catches up.
                    while (!deliveries.offer(frame, 1, TimeUnit.SECONDS)) {
                        if (finished.get()) {
                            return;
                        }
                    }
                }
                if (finishIfFailed()) {
                    return;
                }
            }
        } catch (IOException e) {
            // The record layer failed (a fatal alert, close_notify, a closed socket).
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        synchronized (recovery) {
            recovery.fail(Pdu.CLOSE_NORMAL, "record layer closed", false);
            recovery.notifyAll();
        }
        finish();
    }

    /**
     * BouncyCastle accepted a record (RFC 9146 §6): when it carried the
     * listener's Connection ID, is the newest seen, and came from a new
     * address, the peer moved; follow it, on a budget until validated.
     */
    private void recordAccepted(int flags) {
        boolean moved = (flags & DTLSRecordFlags.USES_CONNECTION_ID) != 0
                && (flags & DTLSRecordFlags.IS_NEWEST) != 0;
        InetSocketAddress source = path.lastSource();
        if (moved && source != null && !source.equals(path.remote())) {
            path.migrateTo(source, 0);
            synchronized (recovery) {
                recovery.pathChanged();
            }
            counters.migrations.increment();
            migrated(source);
        }
    }

    /** Hook for the listener to re-key its address map; the dialer has none. */
    void migrated(InetSocketAddress source) {
        Runnable hook = migrationHook;
        if (hook != null) {
            hook.run();
        }
    }

    private volatile Runnable migrationHook;

    void onMigrate(Runnable hook) {
        this.migrationHook = hook;
    }

    private void deliver() {
        try {
            while (true) {
                byte[] frame = deliveries.poll(250, TimeUnit.MILLISECONDS);
                if (frame == null) {
                    if (finished.get()) {
                        return; // frames complete before the end were delivered first (DT-R-12)
                    }
                    continue;
                }
                try {
                    receiver.accept(frame);
                } catch (RuntimeException e) {
                    // A receiver's failure is its own; the association carries on.
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void onTimer() {
        synchronized (timerLock) {
            armedAt = Long.MAX_VALUE;
            timer = null;
        }
        synchronized (recovery) {
            recovery.onTimer();
            recovery.notifyAll();
        }
        rearm();
        finishIfFailed();
    }

    private void rearm() {
        long deadline;
        synchronized (recovery) {
            deadline = recovery.nextDeadline();
        }
        synchronized (timerLock) {
            if (deadline == armedAt || finished.get()) {
                return;
            }
            if (timer != null) {
                timer.cancel(false);
                timer = null;
            }
            armedAt = deadline;
            if (deadline == Long.MAX_VALUE) {
                return;
            }
            try {
                timer = timers.schedule(this::onTimer, Math.max(0, deadline - System.nanoTime()),
                        TimeUnit.NANOSECONDS);
            } catch (java.util.concurrent.RejectedExecutionException e) {
                // The transport is closing; it closes this association too.
            }
        }
    }

    private boolean finishIfFailed() {
        Recovery.Failure failure;
        synchronized (recovery) {
            failure = recovery.failure();
        }
        if (failure != null) {
            finish();
            return true;
        }
        return false;
    }

    private void finish() {
        if (!finished.compareAndSet(false, true)) {
            return;
        }
        synchronized (timerLock) {
            if (timer != null) {
                timer.cancel(false);
            }
        }
        try {
            dtls.close(); // close_notify, best effort
        } catch (IOException | RuntimeException ignored) {
            // Closing is best-effort.
        }
        path.close();
        onRelease.run();
        notifyClosed();
    }

    private void notifyClosed() {
        if (closeNotified.compareAndSet(false, true)) {
            closeListener.run();
        }
    }

    /** Why the association ended, or null while it is open. */
    Recovery.Failure failure() {
        synchronized (recovery) {
            return recovery.failure();
        }
    }

    boolean isOpen() {
        return !finished.get();
    }
}
