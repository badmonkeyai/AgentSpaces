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
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.DatagramSocket;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The binding end to end on loopback UDP (SRAD DT-B-01 to DT-B-09, DT-Q-PRF-01). */
class DtlsTransportTest {

    private final java.util.List<AutoCloseable> closeables = new java.util.ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable closeable : closeables.reversed()) {
            closeable.close();
        }
    }

    record Pair(TransportConnection dialed, TransportConnection accepted,
                BlockingQueue<byte[]> atListener, BlockingQueue<byte[]> atDialer,
                DtlsTransport dialer, DtlsTransport listener, PeerIdentity dialerId, PeerIdentity listenerId) {
    }

    Pair connect(DtlsTransport dialer, DtlsTransport listener, PeerIdentity dialerId, PeerIdentity listenerId)
            throws Exception {
        closeables.add(dialer);
        closeables.add(listener);
        BlockingQueue<TransportConnection> accepted = new LinkedBlockingQueue<>();
        DtlsTransport.Bound bound = listener.listenBound("127.0.0.1:0", accepted::add);
        closeables.add(bound.listener());
        TransportConnection dialed = dialer.dial("127.0.0.1:" + bound.port());
        TransportConnection inbound = accepted.poll(10, TimeUnit.SECONDS);
        assertThat(inbound).isNotNull();
        BlockingQueue<byte[]> atListener = new LinkedBlockingQueue<>();
        BlockingQueue<byte[]> atDialer = new LinkedBlockingQueue<>();
        inbound.onReceive(atListener::add);
        dialed.onReceive(atDialer::add);
        return new Pair(dialed, inbound, atListener, atDialer, dialer, listener, dialerId, listenerId);
    }

    Pair connect() throws Exception {
        PeerIdentity a = PeerIdentity.generate();
        PeerIdentity b = PeerIdentity.generate();
        return connect(new DtlsTransport(a), new DtlsTransport(b), a, b);
    }

    /**
     * EL-W-06 (srad/agentspaces-event-loop.md, §10.47): an idle association's delivery thread waits on a take, not a
     * 250 ms poll, and ends at the close through a sentinel behind the frames already complete.
     */
    @Test
    @Timeout(60)
    void anIdleDeliveryWaitsOnATakeAndEndsAtTheClose() throws Exception {
        Pair pair = connect();
        pair.dialed().send("ping".getBytes());
        assertThat(pair.atListener().poll(10, TimeUnit.SECONDS)).isEqualTo("ping".getBytes());
        Thread deliver = ((DtlsAssociation) pair.accepted()).deliverThread();
        assertThat(deliver).isNotNull();
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (deliver.getState() != Thread.State.WAITING && System.nanoTime() - until < 0) {
            Thread.sleep(5); // a poll for a state the thread reaches; no timing is asserted
        }
        assertThat(deliver.getState()).as("idle: parked on a take, with no timeout to wake for").isEqualTo(
                Thread.State.WAITING);
        pair.dialed().send("last".getBytes());
        pair.accepted().close();
        assertThat(deliver.join(Duration.ofSeconds(5))).as("the close ends the delivery thread").isTrue();
    }

    @Test
    @Timeout(60)
    void framesFlowBothWaysAndBothEndsAttest() throws Exception {
        Pair pair = connect();
        assertThat(pair.dialed().attestedPeer()).contains(pair.listenerId().peerId());
        assertThat(pair.accepted().attestedPeer()).contains(pair.dialerId().peerId());
        assertThat(pair.dialed().remoteChain()).hasSize(1);

        pair.dialed().send("ping".getBytes());
        pair.accepted().send("pong".getBytes());
        assertThat(pair.atListener().poll(10, TimeUnit.SECONDS)).isEqualTo("ping".getBytes());
        assertThat(pair.atDialer().poll(10, TimeUnit.SECONDS)).isEqualTo("pong".getBytes());

        for (int i = 0; i < 200; i++) {
            pair.dialed().send(("frame-" + i).getBytes());
        }
        for (int i = 0; i < 200; i++) {
            assertThat(new String(pair.atListener().poll(10, TimeUnit.SECONDS))).isEqualTo("frame-" + i);
        }
        assertThat(pair.dialer().counters().associationsOpened()).isEqualTo(1);
        assertThat(pair.listener().counters().helloVerifyRequestsSent()).isEqualTo(1);
    }

    @Test
    @Timeout(60)
    void largeFramesArriveWithinTheSmokeBounds() throws Exception {
        Pair pair = connect();
        byte[] medium = new byte[64 * 1024];
        new Random(1).nextBytes(medium);
        long start = System.nanoTime();
        pair.dialed().send(medium);
        assertThat(pair.atListener().poll(10, TimeUnit.SECONDS)).isEqualTo(medium);
        long mediumMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        byte[] large = new byte[Recovery.MAX_FRAME];
        new Random(2).nextBytes(large);
        start = System.nanoTime();
        pair.accepted().send(large);
        assertThat(pair.atDialer().poll(30, TimeUnit.SECONDS)).isEqualTo(large);
        long largeMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        // SRAD DT-Q-PRF-01 smoke bounds; the first frame also warms the JIT, so it gets slack.
        assertThat(mediumMillis).isLessThan(2_000);
        assertThat(largeMillis).isLessThan(10_000);
    }

    @Test
    @Timeout(30)
    void oversizeFramesAreRefusedAndCloseIsIdempotent() throws Exception {
        Pair pair = connect();
        assertThatThrownBy(() -> pair.dialed().send(new byte[Recovery.MAX_FRAME + 1]))
                .isInstanceOf(IOException.class).hasMessageContaining("exceeds");
        CountDownLatch closedAtListener = new CountDownLatch(1);
        pair.accepted().onClose(closedAtListener::countDown);
        pair.dialed().close();
        pair.dialed().close();
        assertThat(closedAtListener.await(10, TimeUnit.SECONDS)).isTrue();
        assertThatThrownBy(() -> pair.dialed().send(new byte[1])).isInstanceOf(IOException.class);
    }

    @Test
    @Timeout(30)
    void anUnreachablePeerFailsTheDialWithinTheDialTimeout() throws Exception {
        // A short dial timeout: the bound is the transport's setting, judged relative to it here; the 10 s default
        // is pinned in aTransportDialsWithTheDefaultTimeoutUntilOneIsSet without waiting it out.
        Duration timeout = Duration.ofSeconds(1);
        DtlsTransport dialer = new DtlsTransport(PeerIdentity.generate()).dialTimeout(timeout);
        closeables.add(dialer);
        // A socket that holds the port and never answers: the host sends no port unreachable, so only the
        // handshake's timeout ends the dial, as for a peer behind a firewall that drops the datagrams.
        DatagramSocket silent = new DatagramSocket(new java.net.InetSocketAddress("127.0.0.1", 0));
        closeables.add(silent);
        long start = System.nanoTime();
        assertThatThrownBy(() -> dialer.dial("127.0.0.1:" + silent.getLocalPort()))
                .isInstanceOf(IOException.class).hasMessageContaining("over DTLS")
                .hasMessageNotContaining("port unreachable");
        Duration took = Duration.ofNanos(System.nanoTime() - start);
        System.out.println("a dtls dial to a silent port failed in " + took.toMillis() + " ms (dial timeout "
                + timeout.toMillis() + " ms)");
        // the dial runs to its timeout and not far past it (5 s of room for a loaded host)
        assertThat(took).isGreaterThanOrEqualTo(timeout.minusMillis(50)).isLessThan(timeout.plusSeconds(5));
        assertThat(dialer.counters().handshakesFailed()).isEqualTo(1);
    }

    /** The dial timeout's default, and that it is fixed once the transport is used; nothing is waited out. */
    @Test
    void aTransportDialsWithTheDefaultTimeoutUntilOneIsSet() throws Exception {
        DtlsTransport dtls = new DtlsTransport(PeerIdentity.generate());
        closeables.add(dtls);
        assertThat(DtlsTransport.DEFAULT_DIAL_TIMEOUT).isEqualTo(Duration.ofSeconds(10));
        assertThat(dtls.dialTimeout()).isEqualTo(DtlsTransport.DEFAULT_DIAL_TIMEOUT);
        assertThat(dtls.dialTimeout(Duration.ofMillis(1_500)).dialTimeout()).isEqualTo(Duration.ofMillis(1_500));
        assertThatThrownBy(() -> dtls.dialTimeout(Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> dtls.dialTimeout(Duration.ofMinutes(11))).isInstanceOf(IllegalArgumentException.class);
        UdpTransport udp = new UdpTransport();
        closeables.add(udp);
        assertThat(udp.dialTimeout()).isEqualTo(DtlsTransport.DEFAULT_DIAL_TIMEOUT);
        assertThat(DtlsOptions.defaults().idleTimeout()).isEqualTo(Duration.ofSeconds(60));
        AutoCloseable listening = dtls.listen("127.0.0.1:0", c -> { });
        closeables.add(listening);
        assertThatThrownBy(() -> dtls.dialTimeout(Duration.ofSeconds(1))).isInstanceOf(IllegalStateException.class);
        AutoCloseable udpListening = udp.listen("127.0.0.1:0", c -> { });
        closeables.add(udpListening);
        assertThatThrownBy(() -> udp.dialTimeout(Duration.ofSeconds(1))).isInstanceOf(IllegalStateException.class);
    }

    /**
     * A dial to a port nobody holds (a member that left or crashed) fails as soon as the host answers with a port
     * unreachable, as a TCP dial to a closed port is refused, instead of after the 10 s handshake timeout. A sender that
     * dials such a member (a node's tick, a gossip forward) used to stall for the whole timeout on every send.
     */
    @Test
    @Timeout(30)
    void aDialToAPortNobodyHoldsFailsWhenTheHostSaysSo() throws Exception {
        DtlsTransport dialer = new DtlsTransport(PeerIdentity.generate());
        closeables.add(dialer);
        int port;
        try (DatagramSocket nobody = new DatagramSocket(new java.net.InetSocketAddress("127.0.0.1", 0))) {
            port = nobody.getLocalPort();
        }
        long start = System.nanoTime();
        assertThatThrownBy(() -> dialer.dial("127.0.0.1:" + port))
                .isInstanceOf(IOException.class).hasMessageContaining("over DTLS: port unreachable");
        long millis = Duration.ofNanos(System.nanoTime() - start).toMillis();
        System.out.println("a dtls dial to a closed loopback port failed in " + millis + " ms (the dial timeout is "
                + DtlsTransport.DIAL_TIMEOUT_MILLIS + " ms)");
        assertThat(dialer.counters().handshakesFailed()).isEqualTo(1);

        // the binding still dials a live listener afterwards, and the association works past the handshake,
        // when the socket is no longer connected to the peer (DT-K-05)
        Pair pair = connect();
        pair.dialed().send("after".getBytes());
        assertThat(pair.atListener().poll(10, TimeUnit.SECONDS)).isEqualTo("after".getBytes());
        pair.accepted().send("back".getBytes());
        assertThat(pair.atDialer().poll(10, TimeUnit.SECONDS)).isEqualTo("back".getBytes());
    }

    @Test
    void capabilitiesAndOptionsFollowTheMode() throws Exception {
        DtlsTransport attested = new DtlsTransport(PeerIdentity.generate());
        DtlsTransport plain = DtlsTransport.unattested();
        closeables.add(attested);
        closeables.add(plain);
        assertThat(attested.scheme()).isEqualTo("dtls");
        assertThat(attested.capabilities()).contains(Transport.Capability.ATTESTING, Transport.Capability.ENCRYPTED);
        assertThat(plain.capabilities()).contains(Transport.Capability.ENCRYPTED)
                .doesNotContain(Transport.Capability.ATTESTING);
        assertThat(attested.options().mtu()).isEqualTo(1_200);
        attested.options(DtlsOptions.defaults().withWindow(128));
        attested.listen("127.0.0.1:0", c -> { }).close();
        assertThatThrownBy(() -> attested.options(DtlsOptions.defaults()))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> DtlsOptions.defaults().withMtu(100)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DtlsOptions.defaults().withProtocols(java.util.List.of("DTLSv1.3")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("DTLSv1.3");
    }

    @Test
    @Timeout(30)
    void anUnattestedEndConnectsEncryptedButAttestsNobody() throws Exception {
        PeerIdentity listenerId = PeerIdentity.generate();
        Pair pair = connect(DtlsTransport.unattested(), new DtlsTransport(listenerId), null, listenerId);
        assertThat(pair.accepted().attestedPeer()).isEmpty();
        assertThat(pair.dialed().attestedPeer()).isEmpty(); // an unattested transport attests nobody
        pair.dialed().send("hello".getBytes());
        assertThat(pair.atListener().poll(10, TimeUnit.SECONDS)).isEqualTo("hello".getBytes());
    }

    @Test
    @Timeout(30)
    void aLoopbackDialNeverSharesAPortWithALoopbackListener() throws Exception {
        // macOS hands a wildcard socket ports that 127.0.0.1 sockets hold, about one in 80 here;
        // a dialer on its listener's port handshakes with the listener's own socket and times out.
        java.util.Set<Integer> held = new java.util.HashSet<>();
        for (int i = 0; i < 200; i++) {
            DatagramSocket socket = new DatagramSocket(new java.net.InetSocketAddress("127.0.0.1", 0));
            closeables.add(socket);
            held.add(socket.getLocalPort());
        }
        java.net.InetSocketAddress remote = new java.net.InetSocketAddress("127.0.0.1", 9);
        for (int i = 0; i < 2_000; i++) {
            try (DatagramSocket dial = BcSupport.dialSocket(remote)) {
                assertThat(held).doesNotContain(dial.getLocalPort());
            }
        }
        try (DatagramSocket dial = BcSupport.dialSocket(new java.net.InetSocketAddress("192.0.2.1", 9))) {
            assertThat(dial.getLocalAddress().isAnyLocalAddress()).isTrue(); // DT-K-05: off-host dials stay unbound
        }
    }
}
