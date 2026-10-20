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

import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.api.spi.TransportConnection;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * The bare {@code udp} binding (SRAD dtls-support §1.2.2, group U): the
 * recovery sublayer over plaintext UDP, for closed test ranges, links already
 * encrypted underneath, and devices with no budget for a handshake. Frames
 * stay signed, so integrity and attribution hold end to end; confidentiality
 * and channel attestation do not, and an on-path attacker can forge
 * acknowledgements and break delivery. It reports {@code RELIABLE, ORDERED}
 * only, so a node requiring attestation refuses it, and the hosts register it
 * only under {@code dev-local} or an explicit {@code allow-plaintext}.
 *
 * <p>An association starts with a cleartext exchange: the dialer sends
 * {@code HELLO(nonce)}, padded to {@value #HELLO_LENGTH} bytes; the listener
 * answers {@code HELLO_ACK(nonce, token)}, smaller than the hello, where the
 * token is a keyed hash of the dialer's address under a secret rotated every
 * 60 s. The listener allocates nothing until a PDU arrives carrying a valid
 * token, and every PDU carries it.
 */
public final class UdpTransport implements Transport, AutoCloseable {

    /** The advertised scheme. */
    public static final String SCHEME = "udp";

    static final int HELLO = 0x07;
    static final int HELLO_ACK = 0x08;
    static final int HELLO_LENGTH = 28;
    static final int HELLO_ACK_LENGTH = 20;
    private static final long SECRET_ROTATION_NANOS = TimeUnit.SECONDS.toNanos(60);

    private final DtlsCounters counters = new DtlsCounters();
    private final Set<DtlsAssociation> associations = ConcurrentHashMap.newKeySet();
    private final Set<Listener> listeners = ConcurrentHashMap.newKeySet();
    private final SecureRandom random = new SecureRandom();
    private volatile DtlsOptions options = DtlsOptions.defaults();
    private volatile boolean used;
    private volatile boolean closed;
    private ScheduledThreadPoolExecutor timers;

    /** Creates the binding with the default options. */
    public UdpTransport() {
    }

    /**
     * Sets the tunables (the datagram budget, window, idle timeout, and hello
     * rate apply; the DTLS-only options are ignored); valid before first use.
     *
     * @param options the options
     * @return this transport
     */
    public UdpTransport options(DtlsOptions options) {
        if (used) {
            throw new IllegalStateException("options are fixed once the transport has dialed or listened");
        }
        this.options = Objects.requireNonNull(options, "options");
        return this;
    }

    /** @return this transport's counters */
    public DtlsCounters counters() {
        return counters;
    }

    @Override
    public String scheme() {
        return SCHEME;
    }

    @Override
    public Set<Capability> capabilities() {
        return Set.of(Capability.RELIABLE, Capability.ORDERED);
    }

    @Override
    public TransportConnection dial(String address) throws IOException {
        prepare();
        InetSocketAddress remote = BcSupport.parse(address);
        if (remote.isUnresolved()) {
            throw new IOException("cannot resolve " + address);
        }
        DtlsOptions opts = options;
        DatagramSocket socket = BcSupport.dialSocket(remote);
        PeerDatagramTransport path = new PeerDatagramTransport(
                (buf, off, len, to) -> socket.send(new DatagramPacket(buf, off, len, to)),
                opts.mtu(), remote, socket::close);
        Thread.ofVirtual().name("udp-dial-" + remote).start(() -> {
            byte[] buf = new byte[PeerDatagramTransport.RECEIVE_LIMIT];
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            while (!socket.isClosed()) {
                try {
                    packet.setLength(buf.length);
                    socket.receive(packet);
                } catch (IOException e) {
                    return;
                }
                if (packet.getSocketAddress().equals(remote)) {
                    path.offer(new PeerDatagramTransport.Datagram(
                            java.util.Arrays.copyOf(buf, packet.getLength()), remote));
                }
            }
        });
        long nonce = random.nextLong();
        byte[] hello = ByteBuffer.allocate(HELLO_LENGTH).put((byte) Pdu.VERSION).put((byte) HELLO)
                .putShort((short) 0).putLong(nonce).array();
        long token = 0;
        boolean answered = false;
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DtlsTransport.DIAL_TIMEOUT_MILLIS);
        int wait = 1_000;
        byte[] reply = new byte[PeerDatagramTransport.RECEIVE_LIMIT];
        try {
            while (!answered && System.nanoTime() < deadline) {
                path.send(hello, 0, hello.length);
                long until = Math.min(deadline, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(wait));
                while (System.nanoTime() < until) {
                    int n = path.receive(reply, 0, reply.length,
                            (int) Math.max(1, TimeUnit.NANOSECONDS.toMillis(until - System.nanoTime())));
                    if (n == HELLO_ACK_LENGTH && reply[0] == Pdu.VERSION && reply[1] == HELLO_ACK
                            && ByteBuffer.wrap(reply, 4, 8).getLong() == nonce) {
                        token = ByteBuffer.wrap(reply, 12, 8).getLong();
                        answered = true;
                        break;
                    }
                }
                wait = Math.min(wait * 2, 8_000);
            }
        } catch (IOException e) {
            path.close();
            throw e;
        }
        if (!answered) {
            counters.handshakesFailed.increment();
            path.close();
            throw new IOException("cannot reach " + address + " over UDP: no HELLO_ACK");
        }
        return open(path, token, opts, () -> { });
    }

    private DtlsAssociation open(PeerDatagramTransport path, long token, DtlsOptions opts, Runnable released)
            throws IOException {
        DtlsAssociation[] self = new DtlsAssociation[1];
        DtlsAssociation association = new DtlsAssociation(new PlainRecordLayer(path, token, opts.mtu()), path,
                Optional.empty(), new X509Certificate[0], opts, timers, counters,
                () -> {
                    associations.remove(self[0]);
                    released.run();
                });
        self[0] = association;
        counters.associationsOpened.increment();
        associations.add(association);
        association.start();
        return association;
    }

    @Override
    public AutoCloseable listen(String bindAddress, Consumer<TransportConnection> onAccept) throws IOException {
        Objects.requireNonNull(onAccept, "onAccept");
        prepare();
        Listener listener = new Listener(BcSupport.parse(bindAddress), options, onAccept);
        listeners.add(listener);
        return () -> {
            listeners.remove(listener);
            listener.close();
        };
    }

    /** Listens and returns the bound port, for tests. */
    int listenPort(String bindAddress, Consumer<TransportConnection> onAccept) throws IOException {
        prepare();
        Listener listener = new Listener(BcSupport.parse(bindAddress), options, onAccept);
        listeners.add(listener);
        return listener.socket.getLocalPort();
    }

    @Override
    public void close() {
        closed = true;
        for (DtlsAssociation association : associations) {
            association.close();
        }
        associations.clear();
        for (Listener listener : listeners) {
            listener.close();
        }
        listeners.clear();
        synchronized (this) {
            if (timers != null) {
                timers.shutdownNow();
            }
        }
    }

    private synchronized void prepare() throws IOException {
        if (closed) {
            throw new IOException("transport closed");
        }
        used = true;
        if (timers == null) {
            timers = new ScheduledThreadPoolExecutor(1, runnable -> {
                Thread thread = new Thread(runnable, "udp-timer");
                thread.setDaemon(true);
                return thread;
            });
            timers.setRemoveOnCancelPolicy(true);
        }
    }

    /** One bound UDP port. */
    private final class Listener implements AutoCloseable {
        private final DatagramSocket socket;
        private final DtlsOptions opts;
        private final Consumer<TransportConnection> onAccept;
        private final Map<InetSocketAddress, PeerDatagramTransport> byAddress = new ConcurrentHashMap<>();
        private final Set<DtlsAssociation> accepted = ConcurrentHashMap.newKeySet();
        private final Map<InetAddress, double[]> buckets = new LinkedHashMap<>(64, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<InetAddress, double[]> eldest) {
                return size() > 4_096;
            }
        };
        private byte[] secret = new byte[32];
        private byte[] previousSecret;
        private long secretSince = System.nanoTime();

        Listener(InetSocketAddress bind, DtlsOptions opts, Consumer<TransportConnection> onAccept)
                throws IOException {
            this.opts = opts;
            this.onAccept = onAccept;
            random.nextBytes(secret);
            this.socket = new DatagramSocket(null);
            try {
                socket.bind(bind);
            } catch (IOException e) {
                socket.close();
                throw new IOException("cannot bind UDP listener to " + bind + ": " + e.getMessage(), e);
            }
            Thread.ofVirtual().name("udp-listen-" + socket.getLocalPort()).start(this::readLoop);
        }

        private void readLoop() {
            byte[] buf = new byte[PeerDatagramTransport.RECEIVE_LIMIT];
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            while (!socket.isClosed()) {
                try {
                    packet.setLength(buf.length);
                    socket.receive(packet);
                } catch (IOException e) {
                    return;
                }
                byte[] bytes = java.util.Arrays.copyOf(buf, packet.getLength());
                InetSocketAddress source = (InetSocketAddress) packet.getSocketAddress();
                try {
                    route(bytes, source);
                } catch (RuntimeException | IOException e) {
                    counters.datagramsDropped.increment();
                }
            }
        }

        private void route(byte[] bytes, InetSocketAddress source) throws IOException {
            PeerDatagramTransport known = byAddress.get(source);
            if (known != null) {
                if (!known.offer(new PeerDatagramTransport.Datagram(bytes, source))) {
                    counters.datagramsDropped.increment();
                }
                return;
            }
            if (bytes.length < 2 || bytes[0] != Pdu.VERSION) {
                counters.datagramsDropped.increment();
                return;
            }
            rotate();
            if (bytes[1] == HELLO) {
                if (bytes.length != HELLO_LENGTH || !tryAcquire(source.getAddress())) {
                    counters.clientHellosDropped.increment();
                    return;
                }
                byte[] ack = ByteBuffer.allocate(HELLO_ACK_LENGTH).put((byte) Pdu.VERSION).put((byte) HELLO_ACK)
                        .putShort((short) 0).put(bytes, 4, 8).putLong(token(secret, source)).array();
                socket.send(new DatagramPacket(ack, ack.length, source));
                counters.helloVerifyRequestsSent.increment();
                return;
            }
            if (bytes.length < 2 + PlainRecordLayer.TOKEN_LENGTH) {
                counters.datagramsDropped.increment();
                return;
            }
            long presented = ByteBuffer.wrap(bytes, 2, 8).getLong();
            boolean valid = presented == token(secret, source)
                    || (previousSecret != null && presented == token(previousSecret, source));
            if (!valid) {
                counters.datagramsDropped.increment();
                return;
            }
            PeerDatagramTransport path = new PeerDatagramTransport(
                    (buf, off, len, to) -> socket.send(new DatagramPacket(buf, off, len, to)),
                    opts.mtu(), source, () -> byAddress.remove(source));
            byAddress.put(source, path);
            path.offer(new PeerDatagramTransport.Datagram(bytes, source));
            DtlsAssociation[] self = new DtlsAssociation[1];
            DtlsAssociation association = open(path, presented, opts, () -> accepted.remove(self[0]));
            self[0] = association;
            accepted.add(association);
            Thread.ofVirtual().start(() -> onAccept.accept(association));
        }

        private void rotate() {
            long now = System.nanoTime();
            if (now - secretSince >= SECRET_ROTATION_NANOS) {
                previousSecret = secret;
                secret = new byte[32];
                random.nextBytes(secret);
                secretSince = now;
            }
        }

        private boolean tryAcquire(InetAddress source) {
            double rate = opts.helloRatePerSource();
            long now = System.nanoTime();
            double[] bucket = buckets.computeIfAbsent(source, k -> new double[]{rate, now});
            double tokens = Math.min(rate, bucket[0] + (now - bucket[1]) / 1e9 * rate);
            bucket[1] = now;
            bucket[0] = tokens < 1 ? tokens : tokens - 1;
            return tokens >= 1;
        }

        @Override
        public void close() {
            for (DtlsAssociation association : accepted) {
                association.close();
            }
            socket.close();
        }
    }

    /** The association token: a keyed hash of the source address, so it is checked with no state. */
    static long token(byte[] secret, InetSocketAddress source) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret, "HmacSHA256"));
            mac.update(source.getAddress().getAddress());
            mac.update(ByteBuffer.allocate(2).putShort((short) source.getPort()).array());
            return ByteBuffer.wrap(mac.doFinal()).getLong();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HMAC-SHA-256 unavailable", e);
        }
    }
}
