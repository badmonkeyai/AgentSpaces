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
        return link(loss, options, null);
    }

    /** As above; with a time source, the dialer's association timers (retransmit, keepalive, idle) run on it. */
    private Link link(double loss, DtlsOptions options, ai.badmonkey.agentspaces.common.time.TimeSource dialerTime)
            throws Exception {
        DtlsTransport listener = new DtlsTransport(PeerIdentity.generate()).options(options);
        DtlsTransport dialer = new DtlsTransport(PeerIdentity.generate()).options(options);
        if (dialerTime != null) {
            dialer.time(dialerTime);
        }
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
        // The ten handshakes run at once, each through its own seeded relay: one after another they waited out each
        // lost flight's retransmit in turn (23 s). Each still has the default 10 s dial timeout of DT-Q-REL-02. Once
        // nine have completed the verdict is in, so the rest are stopped rather than waited out to their timeout.
        java.util.concurrent.atomic.AtomicInteger succeeded = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger ended = new java.util.concurrent.atomic.AtomicInteger();
        List<Thread> dials = new ArrayList<>();
        Object progress = new Object();
        for (int i = 0; i < 10; i++) {
            int seed = 1_000 + i;
            DtlsTransport dialer = new DtlsTransport(PeerIdentity.generate());
            closeables.add(dialer);
            dials.add(Thread.ofVirtual().name("lossy-dial-" + i).start(() -> {
                try (UdpRelay relay = new UdpRelay(bound.port(), 0.10, seed); dialer) {
                    dialer.dial(relay.address()).close();
                    succeeded.incrementAndGet();
                } catch (Exception e) {
                    // counted below
                } finally {
                    ended.incrementAndGet();
                    synchronized (progress) {
                        progress.notifyAll();
                    }
                }
            }));
        }
        long start = System.nanoTime();
        synchronized (progress) {
            while (ended.get() < dials.size() && succeeded.get() < 9) {
                progress.wait(1_000);
            }
        }
        int verdict = succeeded.get();
        System.out.printf("10 lossy dials: %d succeeded, %d ended, after %d ms%n", verdict, ended.get(),
                Duration.ofNanos(System.nanoTime() - start).toMillis());
        for (Thread dial : dials) {
            dial.interrupt(); // a dial still in its handshake ends at its own timeout; nobody waits for it here
        }
        // SRAD DT-Q-REL-02 as built: at least 9 of 10 within the 10 s dial timeout at 10 percent loss.
        assertThat(verdict).isGreaterThanOrEqualTo(9);
    }

    @Test
    @Timeout(60)
    void aSilentPeerFailsTheAssociationAfterTheIdleTimeout() throws Exception {
        // The dialer's timers run on virtual time, so the 10 s idle timeout (the shortest DtlsOptions allows) is
        // judged exactly and costs no wall time: the handshake is real, the silence is advanced through.
        Duration idle = Duration.ofSeconds(10);
        ai.badmonkey.agentspaces.test.VirtualTime time = ai.badmonkey.agentspaces.test.VirtualTime.create();
        Link link = link(0, DtlsOptions.defaults().withIdleTimeout(idle), time);
        java.util.concurrent.atomic.AtomicLong closedAt = new java.util.concurrent.atomic.AtomicLong(-1);
        CountDownLatch closed = new CountDownLatch(1);
        link.dialed().onClose(() -> {
            closedAt.compareAndSet(-1, time.monoNanos());
            closed.countDown();
        });
        link.relay().loss = 1.0; // the link goes dark
        long start = time.monoNanos();
        Duration step = Duration.ofMillis(250);
        for (long advanced = 0; closed.getCount() > 0 && advanced < 3 * idle.toMillis(); advanced += step.toMillis()) {
            time.advance(step);
        }
        assertThat(closed.await(5, TimeUnit.SECONDS)).as("the association failed").isTrue();
        long elapsed = TimeUnit.NANOSECONDS.toMillis(closedAt.get() - start);
        // Not before the idle timeout after the last datagram heard (at or after the start), and within one
        // keepalive period (a quarter of it) of it should a datagram in flight have landed after the start.
        assertThat(elapsed).isBetween(idle.toMillis(), idle.toMillis() + idle.toMillis() / 4 + step.toMillis());
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
