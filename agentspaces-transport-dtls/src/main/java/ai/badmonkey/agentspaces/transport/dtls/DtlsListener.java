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
import org.bouncycastle.tls.ContentType;
import org.bouncycastle.tls.DTLSRequest;
import org.bouncycastle.tls.DTLSServerProtocol;
import org.bouncycastle.tls.DTLSTransport;
import org.bouncycastle.tls.DTLSVerifier;
import org.bouncycastle.tls.DatagramSender;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * One bound UDP port accepting DTLS associations (SRAD §3.3, §3.4, S5). A
 * single read loop demultiplexes every datagram: a record carrying a
 * Connection ID goes to the association that issued it, any other record to
 * the association of its source address, and a ClientHello from an unknown
 * source to BouncyCastle's stateless {@link DTLSVerifier}, which answers with
 * a HelloVerifyRequest and allocates nothing until the ClientHello carries a
 * valid cookie. Only then, within the per-source rate and the half-open bound,
 * does a handshake start, on its own virtual thread.
 */
final class DtlsListener implements AutoCloseable {

    /** Connection IDs this listener issues: fixed length, so a record's id sits at a fixed offset. */
    static final int CID_LENGTH = 8;
    /** Type, version, epoch, and sequence precede the id in a {@code tls12_cid} record (RFC 9146 §4). */
    static final int CID_OFFSET = 11;
    private static final long VERIFIER_ROTATION_NANOS = TimeUnit.SECONDS.toNanos(60);
    private static final int MAX_RATE_SOURCES = 4_096;

    private final DtlsTransport owner;
    private final DtlsOptions options;
    private final Consumer<TransportConnection> onAccept;
    private final DatagramSocket socket;
    private final Map<InetSocketAddress, PeerDatagramTransport> byAddress = new ConcurrentHashMap<>();
    private final Map<Long, PeerDatagramTransport> byConnectionId = new ConcurrentHashMap<>();
    private final AtomicInteger halfOpen = new AtomicInteger();
    private final java.util.Set<DtlsAssociation> accepted = ConcurrentHashMap.newKeySet();
    private final SecureRandom random = new SecureRandom();
    private final Map<InetAddress, double[]> buckets = new LinkedHashMap<>(256, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<InetAddress, double[]> eldest) {
            return size() > MAX_RATE_SOURCES;
        }
    };
    private DTLSVerifier verifier;
    private DTLSVerifier previousVerifier;
    private long verifierSince;
    private final Thread reader;

    DtlsListener(DtlsTransport owner, InetSocketAddress bind, DtlsOptions options,
                 Consumer<TransportConnection> onAccept) throws IOException {
        this.owner = owner;
        this.options = options;
        this.onAccept = onAccept;
        this.socket = new DatagramSocket(null);
        try {
            socket.setReuseAddress(false);
            socket.bind(bind);
        } catch (IOException e) {
            socket.close();
            throw new IOException("cannot bind DTLS listener to " + bind + ": " + e.getMessage(), e);
        }
        this.verifier = new DTLSVerifier(owner.crypto());
        this.verifierSince = System.nanoTime();
        this.reader = Thread.ofVirtual().name("dtls-listen-" + socket.getLocalPort()).start(this::readLoop);
    }

    /** The bound port (useful when bound to port 0). */
    int port() {
        return socket.getLocalPort();
    }

    int halfOpen() {
        return halfOpen.get();
    }

    /**
     * Stops listening. The associations this listener accepted share its
     * socket, so each closes first, sending {@code CLOSE(0)} while it still can.
     */
    @Override
    public void close() {
        for (DtlsAssociation association : accepted) {
            association.close();
        }
        socket.close();
        reader.interrupt();
        for (PeerDatagramTransport path : byAddress.values()) {
            path.close();
        }
    }

    private void readLoop() {
        byte[] buf = new byte[PeerDatagramTransport.RECEIVE_LIMIT];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        while (!socket.isClosed()) {
            try {
                packet.setLength(buf.length);
                socket.receive(packet);
            } catch (IOException e) {
                return; // closed
            }
            byte[] bytes = java.util.Arrays.copyOf(buf, packet.getLength());
            InetSocketAddress source = (InetSocketAddress) packet.getSocketAddress();
            try {
                route(bytes, source);
            } catch (RuntimeException e) {
                owner.counters().datagramsDropped.increment(); // never let one datagram end the loop
            }
        }
    }

    private void route(byte[] bytes, InetSocketAddress source) {
        DtlsCounters counters = owner.counters();
        if (bytes.length >= CID_OFFSET + CID_LENGTH + 2 && (bytes[0] & 0xFF) == ContentType.tls12_cid) {
            PeerDatagramTransport path = byConnectionId.get(ByteBuffer.wrap(bytes, CID_OFFSET, CID_LENGTH).getLong());
            if (path == null) {
                counters.unknownConnectionIds.increment();
            } else if (!path.offer(new PeerDatagramTransport.Datagram(bytes, source))) {
                counters.datagramsDropped.increment();
            }
            return;
        }
        PeerDatagramTransport known = byAddress.get(source);
        if (known != null) {
            if (!known.offer(new PeerDatagramTransport.Datagram(bytes, source))) {
                counters.datagramsDropped.increment();
            }
            return;
        }
        if (bytes.length < 13 || (bytes[0] & 0xFF) != ContentType.handshake) {
            counters.datagramsDropped.increment();
            return;
        }
        if (!tryAcquire(source.getAddress())) {
            counters.clientHellosDropped.increment();
            return;
        }
        DTLSRequest request = verify(bytes, source);
        if (request == null) {
            return; // answered with a HelloVerifyRequest, or not a ClientHello at all
        }
        if (halfOpen.incrementAndGet() > options.maxHalfOpen()) {
            halfOpen.decrementAndGet();
            counters.clientHellosDropped.increment();
            return;
        }
        accept(source, request);
    }

    /** BouncyCastle's stateless cookie check, under a secret replaced every 60 s; the last secret's cookies still count. */
    private DTLSRequest verify(byte[] bytes, InetSocketAddress source) {
        long now = System.nanoTime();
        if (now - verifierSince >= VERIFIER_ROTATION_NANOS) {
            previousVerifier = verifier;
            verifier = new DTLSVerifier(owner.crypto());
            verifierSince = now;
        }
        byte[] clientId = clientId(source);
        if (previousVerifier != null) {
            DTLSRequest request = previousVerifier.verifyRequest(clientId, bytes, 0, bytes.length, DISCARD);
            if (request != null) {
                return request;
            }
        }
        DatagramSender reply = new DatagramSender() {
            @Override
            public int getSendLimit() {
                return options.mtu();
            }

            @Override
            public void send(byte[] buf, int off, int len) throws IOException {
                // A HelloVerifyRequest is smaller than the ClientHello it answers (RFC 6347 §4.2.1).
                if (len <= bytes.length) {
                    owner.counters().helloVerifyRequestsSent.increment();
                    socket.send(new DatagramPacket(buf, off, len, source));
                }
            }
        };
        return verifier.verifyRequest(clientId, bytes, 0, bytes.length, reply);
    }

    private static final DatagramSender DISCARD = new DatagramSender() {
        @Override
        public int getSendLimit() {
            return 1_200;
        }

        @Override
        public void send(byte[] buf, int off, int len) {
        }
    };

    private static byte[] clientId(InetSocketAddress source) {
        byte[] address = source.getAddress().getAddress();
        return ByteBuffer.allocate(address.length + 2).put(address).putShort((short) source.getPort()).array();
    }

    /** A token bucket per source address, refilled at the configured hellos per second. */
    private boolean tryAcquire(InetAddress source) {
        double rate = options.helloRatePerSource();
        long now = System.nanoTime();
        double[] bucket = buckets.computeIfAbsent(source, k -> new double[]{rate, now});
        double tokens = Math.min(rate, bucket[0] + (now - bucket[1]) / 1e9 * rate);
        bucket[1] = now;
        if (tokens < 1) {
            bucket[0] = tokens;
            return false;
        }
        bucket[0] = tokens - 1;
        return true;
    }

    private void accept(InetSocketAddress source, DTLSRequest request) {
        DtlsCounters counters = owner.counters();
        long[] issued = {0};
        boolean[] hasId = {false};
        PeerDatagramTransport path = new PeerDatagramTransport(
                (buf, off, len, to) -> socket.send(new DatagramPacket(buf, off, len, to)),
                options.mtu(), source, () -> {
                });
        byAddress.put(source, path);
        Thread.ofVirtual().name("dtls-accept-" + source).start(() -> {
            AspaceDtlsServer server = new AspaceDtlsServer(owner.crypto(), owner.credential(),
                    owner.bcCertificate(), BcSupport.versions(options.protocols()),
                    options.connectionId() ? () -> {
                        byte[] id = new byte[CID_LENGTH];
                        long key;
                        do {
                            random.nextBytes(id);
                            key = ByteBuffer.wrap(id).getLong();
                        } while (byConnectionId.putIfAbsent(key, path) != null);
                        issued[0] = key;
                        hasId[0] = true;
                        return id;
                    } : null);
            Runnable release = () -> {
                byAddress.values().removeIf(p -> p == path);
                if (hasId[0]) {
                    byConnectionId.remove(issued[0], path);
                }
            };
            DTLSTransport dtls;
            try {
                dtls = new DTLSServerProtocol().accept(server, path, request);
            } catch (IOException | RuntimeException e) {
                halfOpen.decrementAndGet();
                counters.handshakesFailed.increment();
                release.run();
                path.close();
                return;
            }
            halfOpen.decrementAndGet();
            X509Certificate[] chain = BcSupport.x509(server.presented());
            Optional<PeerId> attested = owner.attest(chain);
            DtlsAssociation[] self = new DtlsAssociation[1];
            DtlsAssociation association;
            try {
                association = new DtlsAssociation(RecordLayer.dtls(dtls), path, attested, chain, options, owner.timers(),
                        counters, () -> {
                            release.run();
                            accepted.remove(self[0]);
                            owner.untrack(self[0]);
                        });
                self[0] = association;
            } catch (IOException e) {
                counters.handshakesFailed.increment();
                release.run();
                path.close();
                return;
            }
            association.onMigrate(() -> {
                byAddress.values().removeIf(p -> p == path);
                byAddress.put(path.remote(), path);
            });
            counters.associationsOpened.increment();
            accepted.add(association);
            owner.track(association);
            association.start();
            onAccept.accept(association);
        });
    }
}
