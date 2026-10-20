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
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Associations across a relay that rebinds, drops, and goes silent (SRAD
 * scenarios 2 to 4; DT-K-01 to DT-K-06, DT-Q-REL-02, DT-Q-REL-03, DT-Q-REL-07,
 * DT-Q-SEC-11).
 */
class DtlsRelayTest {

    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable closeable : closeables.reversed()) {
            closeable.close();
        }
    }

    private record Link(DtlsTransport dialer, DtlsTransport listener, UdpRelay relay,
                        TransportConnection dialed, TransportConnection accepted,
                        BlockingQueue<byte[]> atListener, BlockingQueue<byte[]> atDialer) {
    }

    private Link link(double loss, DtlsOptions options) throws Exception {
        DtlsTransport listener = new DtlsTransport(PeerIdentity.generate()).options(options);
        DtlsTransport dialer = new DtlsTransport(PeerIdentity.generate()).options(options);
        closeables.add(dialer);
        closeables.add(listener);
        BlockingQueue<TransportConnection> accepted = new LinkedBlockingQueue<>();
        DtlsTransport.Bound bound = listener.listenBound("127.0.0.1:0", accepted::add);
        closeables.add(bound.listener());
        UdpRelay relay = new UdpRelay(bound.port(), loss, 99);
        closeables.add(relay);
        TransportConnection dialed = dialer.dial(relay.address());
        TransportConnection inbound = accepted.poll(15, TimeUnit.SECONDS);
        assertThat(inbound).isNotNull();
        BlockingQueue<byte[]> atListener = new LinkedBlockingQueue<>();
        BlockingQueue<byte[]> atDialer = new LinkedBlockingQueue<>();
        inbound.onReceive(atListener::add);
        dialed.onReceive(atDialer::add);
        return new Link(dialer, listener, relay, dialed, inbound, atListener, atDialer);
    }

    @Test
    @Timeout(60)
    void anAssociationFollowsAPeerWhoseAddressChanges() throws Exception {
        Link link = link(0, DtlsOptions.defaults());
        String before = link.accepted().remoteAddress();
        link.dialed().send("before".getBytes());
        assertThat(link.atListener().poll(10, TimeUnit.SECONDS)).isEqualTo("before".getBytes());

        link.relay().rebind(); // the NAT moves the dialer to a new source port
        for (int i = 0; i < 50; i++) {
            link.dialed().send(("after-" + i).getBytes());
            link.accepted().send(("reply-" + i).getBytes());
        }
        for (int i = 0; i < 50; i++) {
            assertThat(new String(link.atListener().poll(15, TimeUnit.SECONDS))).isEqualTo("after-" + i);
            assertThat(new String(link.atDialer().poll(15, TimeUnit.SECONDS))).isEqualTo("reply-" + i);
        }
        assertThat(link.accepted().remoteAddress()).isNotEqualTo(before)
                .endsWith(":" + link.relay().backPort());
        assertThat(link.listener().counters().migrations()).isEqualTo(1);
        assertThat(link.listener().counters().associationsOpened()).isEqualTo(1);
        assertThat(link.dialer().counters().associationsOpened()).isEqualTo(1);
    }

    @Test
    @Timeout(60)
    void withoutConnectionIdARebindStrandsTheAssociation() throws Exception {
        Link link = link(0, DtlsOptions.defaults().withConnectionId(false).withIdleTimeout(Duration.ofSeconds(10)));
        link.dialed().send("before".getBytes());
        assertThat(link.atListener().poll(10, TimeUnit.SECONDS)).isEqualTo("before".getBytes());
        link.relay().rebind();
        link.dialed().send("after".getBytes());
        // The listener cannot tell the new port is the same peer: the record is dropped.
        assertThat(link.atListener().poll(3, TimeUnit.SECONDS)).isNull();
        assertThat(link.listener().counters().migrations()).isZero();
    }

    @Test
    @Timeout(120)
    void framesSurviveTenPercentLossInBothDirections() throws Exception {
        Link link = link(0, DtlsOptions.defaults());
        link.relay().loss = 0.10;
        byte[] block = new byte[256 * 1024];
        new java.util.Random(5).nextBytes(block);
        for (int i = 0; i < 20; i++) {
            link.dialed().send(("small-" + i).getBytes());
        }
        link.dialed().send(block);
        for (int i = 0; i < 20; i++) {
            assertThat(new String(link.atListener().poll(30, TimeUnit.SECONDS))).isEqualTo("small-" + i);
        }
        assertThat(link.atListener().poll(60, TimeUnit.SECONDS)).isEqualTo(block);
        assertThat(link.dialer().counters().retransmitted()).isPositive();
    }

    @Test
    @Timeout(180)
    void handshakesCompleteUnderTenPercentLoss() throws Exception {
        DtlsTransport listener = new DtlsTransport(PeerIdentity.generate())
                .options(DtlsOptions.defaults().withHelloRatePerSource(1_000));
        closeables.add(listener);
        DtlsTransport.Bound bound = listener.listenBound("127.0.0.1:0", c -> c.onReceive(f -> { }));
        closeables.add(bound.listener());
        int succeeded = 0;
        for (int i = 0; i < 10; i++) {
            try (UdpRelay relay = new UdpRelay(bound.port(), 0.10, 1_000 + i);
                 DtlsTransport dialer = new DtlsTransport(PeerIdentity.generate())) {
                try {
                    dialer.dial(relay.address()).close();
                    succeeded++;
                } catch (java.io.IOException e) {
                    // counted below
                }
            }
        }
        // SRAD DT-Q-REL-02 as built: at least 9 of 10 within the 10 s dial timeout at 10 percent loss.
        assertThat(succeeded).isGreaterThanOrEqualTo(9);
    }

    @Test
    @Timeout(60)
    void aSilentPeerFailsTheAssociationAfterTheIdleTimeout() throws Exception {
        Link link = link(0, DtlsOptions.defaults().withIdleTimeout(Duration.ofSeconds(10)));
        CountDownLatch closed = new CountDownLatch(1);
        link.dialed().onClose(closed::countDown);
        link.relay().loss = 1.0; // the link goes dark
        long start = System.nanoTime();
        assertThat(closed.await(30, TimeUnit.SECONDS)).isTrue();
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertThat(elapsed).isBetween(9_000L, 22_000L);
        assertThat(link.dialer().counters().failedIdle()).isEqualTo(1);
    }

    @Test
    @Timeout(120)
    void oneListenerServesThirtyTwoConcurrentAssociations() throws Exception {
        DtlsTransport listener = new DtlsTransport(PeerIdentity.generate())
                .options(DtlsOptions.defaults().withHelloRatePerSource(1_000));
        closeables.add(listener);
        BlockingQueue<byte[]> received = new LinkedBlockingQueue<>();
        DtlsTransport.Bound bound = listener.listenBound("127.0.0.1:0", c -> c.onReceive(received::add));
        closeables.add(bound.listener());
        DtlsTransport dialer = new DtlsTransport(PeerIdentity.generate());
        closeables.add(dialer);
        List<Thread> threads = new ArrayList<>();
        List<Throwable> failures = java.util.Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < 32; i++) {
            int n = i;
            threads.add(Thread.ofVirtual().start(() -> {
                try {
                    dialer.dial("127.0.0.1:" + bound.port()).send(("from-" + n).getBytes());
                } catch (Exception e) {
                    failures.add(e);
                }
            }));
        }
        for (Thread thread : threads) {
            thread.join();
        }
        assertThat(failures).isEmpty();
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (int i = 0; i < 32; i++) {
            seen.add(new String(received.poll(15, TimeUnit.SECONDS)));
        }
        assertThat(seen).hasSize(32);
        assertThat(listener.counters().associationsOpened()).isEqualTo(32);
        assertThat(listener.associations()).hasSize(32);
    }
}
