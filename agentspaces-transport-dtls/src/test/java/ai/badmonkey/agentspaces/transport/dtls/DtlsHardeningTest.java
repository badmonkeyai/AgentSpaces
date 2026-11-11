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

import ai.badmonkey.agentspaces.identity.PeerIdentity;
import org.bouncycastle.tls.CipherSuite;
import org.bouncycastle.tls.DTLSClientProtocol;
import org.bouncycastle.tls.DatagramTransport;
import org.bouncycastle.tls.DefaultTlsClient;
import org.bouncycastle.tls.ProtocolVersion;
import org.bouncycastle.tls.ServerOnlyTlsAuthentication;
import org.bouncycastle.tls.TlsAuthentication;
import org.bouncycastle.tls.TlsServerCertificate;
import org.bouncycastle.tls.UDPTransport;
import org.bouncycastle.tls.crypto.impl.jcajce.JcaTlsCryptoProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The listener under hostile input and the handshake policy (SRAD DT-Q-SEC-01
 * to DT-Q-SEC-05, DT-Q-SEC-08, scenario 7).
 */
class DtlsHardeningTest {

    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable closeable : closeables.reversed()) {
            closeable.close();
        }
    }

    private DtlsTransport.Bound listen(DtlsOptions options) throws Exception {
        DtlsTransport listener = new DtlsTransport(PeerIdentity.generate()).options(options);
        closeables.add(listener);
        DtlsTransport.Bound bound = listener.listenBound("127.0.0.1:0", c -> c.onReceive(f -> { }));
        closeables.add(bound.listener());
        return bound;
    }

    /** A real ClientHello, captured from BouncyCastle's client without any network. */
    private static byte[] clientHello() {
        byte[][] captured = new byte[1][];
        DatagramTransport capture = new DatagramTransport() {
            @Override
            public int getReceiveLimit() {
                return 1_500;
            }

            @Override
            public int getSendLimit() {
                return 1_200;
            }

            @Override
            public int receive(byte[] buf, int off, int len, int waitMillis) throws IOException {
                throw new IOException("captured");
            }

            @Override
            public void send(byte[] buf, int off, int len) {
                if (captured[0] == null) {
                    captured[0] = java.util.Arrays.copyOfRange(buf, off, off + len);
                }
            }

            @Override
            public void close() {
            }
        };
        try {
            new DTLSClientProtocol().connect(new PolicyClient(ProtocolVersion.DTLSv12, BcSupport.ALL_SUITES, true),
                    capture);
        } catch (IOException expected) {
            // the capture ends the handshake
        }
        return captured[0];
    }

    @Test
    @Timeout(30)
    void aSpoofedClientHelloAllocatesNothingAndIsAnsweredSmaller() throws Exception {
        DtlsTransport.Bound bound = listen(DtlsOptions.defaults());
        byte[] hello = clientHello();
        int sent;
        try (DatagramSocket spoofer = new DatagramSocket()) {
            byte[] buf = new byte[2_048];
            DatagramPacket reply = new DatagramPacket(buf, buf.length);
            sent = Datagrams.ask(spoofer, hello, new InetSocketAddress("127.0.0.1", bound.port()), reply, 10);
            assertThat(reply.getLength()).isLessThanOrEqualTo(hello.length);
            assertThat(buf[0]).as("a handshake record: HelloVerifyRequest").isEqualTo((byte) 22);
        }
        assertThat(bound.listener().halfOpen()).isZero();
        DtlsTransport owner = (DtlsTransport) closeables.get(0);
        assertThat(owner.associations()).isEmpty();
        // One answer per hello the listener read; a resent hello (after a lost one) is answered again.
        assertThat(owner.counters().helloVerifyRequestsSent()).isBetween(1L, (long) sent);
    }

    @Test
    @Timeout(30)
    void aFloodFromOneSourceIsRateLimited() throws Exception {
        DtlsOptions options = DtlsOptions.defaults(); // 8 per second per source
        DtlsTransport.Bound bound = listen(options);
        DtlsTransport owner = (DtlsTransport) closeables.get(0);
        DtlsCounters counters = owner.counters();
        byte[] hello = clientHello();
        int flood = 100;
        int rate = options.helloRatePerSource();
        int replies = 0;
        try (DatagramSocket flooder = new DatagramSocket()) {
            InetSocketAddress target = new InetSocketAddress("127.0.0.1", bound.port());
            long start = System.nanoTime();
            long deadline = start + TimeUnit.SECONDS.toNanos(12);
            // UDP promises nothing: the kernel may drop part of a loopback burst and a loaded
            // listener may read it late. So judge by what the listener actually read, and top up
            // only the shortfall once the listener has had a moment, never a fixed client window.
            while (seen(counters) < flood && System.nanoTime() < deadline) {
                for (long i = seen(counters); i < flood; i++) {
                    flooder.send(new DatagramPacket(hello, hello.length, target));
                }
                long settle = Math.min(deadline, System.nanoTime() + TimeUnit.SECONDS.toNanos(1));
                while (seen(counters) < flood && System.nanoTime() < settle) {
                    Thread.sleep(10);
                }
            }
            double elapsed = (System.nanoTime() - start) / 1e9;
            long answered = counters.helloVerifyRequestsSent();
            long dropped = counters.clientHellosDropped();
            long seen = answered + dropped;
            // A token bucket admits at most its burst plus what it refilled while the flood lasted,
            // however slowly the listener read it.
            long budget = (long) Math.floor(rate * (1 + elapsed));
            String state = "seen=" + seen + " answered=" + answered + " dropped=" + dropped
                    + " elapsed=" + elapsed + "s budget=" + budget;
            assertThat(seen).as("the listener read the flood: " + state).isGreaterThanOrEqualTo(flood);
            assertThat(answered).as("hellos answered within the per-source rate: " + state)
                    .isBetween(1L, budget);
            assertThat(dropped).as("the rest dropped: " + state).isGreaterThanOrEqualTo(seen - budget);

            // The answers are datagrams too: take what arrives, and if every one was lost, wait out
            // a token and ask again rather than fail on one unlucky burst.
            byte[] buf = new byte[2_048];
            flooder.setSoTimeout(1_000);
            long replyDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
            while (replies < counters.helloVerifyRequestsSent() && System.nanoTime() < replyDeadline) {
                try {
                    flooder.receive(new DatagramPacket(buf, buf.length));
                    replies++;
                } catch (SocketTimeoutException quiet) {
                    if (replies > 0) {
                        break; // the rest were lost; one answer is enough
                    }
                    Thread.sleep(1_000L / rate + 1);
                    flooder.send(new DatagramPacket(hello, hello.length, target));
                }
            }
        }
        assertThat(replies).as("HelloVerifyRequests reached the flooder")
                .isBetween(1, (int) counters.helloVerifyRequestsSent());
    }

    /** ClientHellos the listener has read: each is either answered or dropped by the rate. */
    private static long seen(DtlsCounters counters) {
        return counters.helloVerifyRequestsSent() + counters.clientHellosDropped();
    }

    @Test
    @Timeout(60)
    void halfOpenHandshakesAreBounded() throws Exception {
        DtlsTransport.Bound bound = listen(DtlsOptions.defaults().withMaxHalfOpen(16).withHelloRatePerSource(1_000));
        List<Thread> stallers = new ArrayList<>();
        List<DatagramSocket> sockets = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            DatagramSocket socket = new DatagramSocket();
            socket.connect(new InetSocketAddress("127.0.0.1", bound.port()));
            sockets.add(socket);
            // Answers the cookie exchange, then never reads the listener's flight: a stalled handshake.
            DatagramTransport stalling = new UDPTransport(socket, 1_200) {
                private int received;

                @Override
                public int receive(byte[] buf, int off, int len, int waitMillis) throws IOException {
                    if (received++ == 0) {
                        return super.receive(buf, off, len, waitMillis);
                    }
                    try {
                        Thread.sleep(waitMillis);
                    } catch (InterruptedException e) {
                        throw new java.io.InterruptedIOException();
                    }
                    return -1;
                }
            };
            stallers.add(Thread.ofVirtual().start(() -> {
                try {
                    new DTLSClientProtocol().connect(
                            new PolicyClient(ProtocolVersion.DTLSv12, BcSupport.ALL_SUITES, true), stalling);
                } catch (IOException expected) {
                    // never completes
                }
            }));
        }
        DtlsTransport owner = (DtlsTransport) closeables.get(0);
        long deadline = System.nanoTime() + 15_000_000_000L;
        while (System.nanoTime() < deadline
                && (bound.listener().halfOpen() < 16 || owner.counters().clientHellosDropped() < 4)) {
            Thread.sleep(100);
        }
        assertThat(bound.listener().halfOpen()).isEqualTo(16);
        assertThat(owner.counters().clientHellosDropped()).isGreaterThanOrEqualTo(4);
        sockets.forEach(DatagramSocket::close);
        stallers.forEach(Thread::interrupt);
    }

    @Test
    @Timeout(30)
    void dtlsTenIsRefused() throws Exception {
        assertHandshakeFails(new PolicyClient(ProtocolVersion.DTLSv10, BcSupport.ALL_SUITES, true));
    }

    @Test
    @Timeout(30)
    void nonAeadSuitesAreRefused() throws Exception {
        assertHandshakeFails(new PolicyClient(ProtocolVersion.DTLSv12,
                new int[]{CipherSuite.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256}, true));
    }

    @Test
    @Timeout(30)
    void theExtendedMasterSecretIsRequired() throws Exception {
        assertHandshakeFails(new PolicyClient(ProtocolVersion.DTLSv12, BcSupport.ALL_SUITES, false));
    }

    @Test
    @Timeout(30)
    void aConformingClientCompletes() throws Exception {
        DtlsTransport.Bound bound = listen(DtlsOptions.defaults());
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", bound.port()));
            new DTLSClientProtocol().connect(new PolicyClient(ProtocolVersion.DTLSv12, BcSupport.ALL_SUITES, true),
                    new UDPTransport(socket, 1_200)).close();
        }
    }

    @Test
    void thePeersRefuseResumptionRenegotiationAndHeartbeat() throws Exception {
        var crypto = new JcaTlsCryptoProvider().create(new SecureRandom());
        AspaceDtlsClient client = new AspaceDtlsClient(crypto, null, null,
                new ProtocolVersion[]{ProtocolVersion.DTLSv12}, true, 10_000);
        AspaceDtlsServer server = new AspaceDtlsServer(crypto, null, null,
                new ProtocolVersion[]{ProtocolVersion.DTLSv12}, null);
        assertThat(client.getSessionToResume()).isNull();
        assertThat(server.getNewSessionID()).isNull();
        assertThat(client.requiresExtendedMasterSecret()).isTrue();
        assertThat(server.requiresExtendedMasterSecret()).isTrue();
        assertThat(client.getRenegotiationPolicy()).isEqualTo(org.bouncycastle.tls.RenegotiationPolicy.DENY);
        assertThat(server.getRenegotiationPolicy()).isEqualTo(org.bouncycastle.tls.RenegotiationPolicy.DENY);
        assertThat(client.getHeartbeat()).isNull();
        assertThat(server.getHeartbeatPolicy()).isEqualTo(org.bouncycastle.tls.HeartbeatMode.peer_not_allowed_to_send);
        assertThat(client.allowLegacyResumption()).isFalse();
    }

    private void assertHandshakeFails(PolicyClient client) throws Exception {
        DtlsTransport.Bound bound = listen(DtlsOptions.defaults());
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", bound.port()));
            assertThatThrownBy(() -> new DTLSClientProtocol().connect(client, new UDPTransport(socket, 1_200)))
                    .isInstanceOf(IOException.class);
        }
        assertThat(((DtlsTransport) closeables.get(0)).associations()).isEmpty();
    }

    /** A client offering exactly what a test names, presenting no certificate. */
    static final class PolicyClient extends DefaultTlsClient {
        private final ProtocolVersion version;
        private final int[] suites;
        private final boolean extendedMasterSecret;

        PolicyClient(ProtocolVersion version, int[] suites, boolean extendedMasterSecret) {
            super(new JcaTlsCryptoProvider().create(new SecureRandom()));
            this.version = version;
            this.suites = suites;
            this.extendedMasterSecret = extendedMasterSecret;
        }

        @Override
        protected ProtocolVersion[] getSupportedVersions() {
            return new ProtocolVersion[]{version};
        }

        @Override
        protected int[] getSupportedCipherSuites() {
            return suites;
        }

        @Override
        public boolean shouldUseExtendedMasterSecret() {
            return extendedMasterSecret;
        }

        @Override
        public boolean shouldCheckSigAlgOfPeerCerts() {
            return false;
        }

        @Override
        public int getHandshakeTimeoutMillis() {
            return 8_000;
        }

        @Override
        public TlsAuthentication getAuthentication() {
            return new ServerOnlyTlsAuthentication() {
                @Override
                public void notifyServerCertificate(TlsServerCertificate serverCertificate) {
                }
            };
        }
    }
}
