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
        DtlsTransport dialer = new DtlsTransport(PeerIdentity.generate());
        closeables.add(dialer);
        int port;
        try (DatagramSocket nobody = new DatagramSocket(0)) {
            port = nobody.getLocalPort();
        }
        long start = System.nanoTime();
        assertThatThrownBy(() -> dialer.dial("127.0.0.1:" + port))
                .isInstanceOf(IOException.class).hasMessageContaining("over DTLS");
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(15));
        assertThat(dialer.counters().handshakesFailed()).isEqualTo(1);
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
}
