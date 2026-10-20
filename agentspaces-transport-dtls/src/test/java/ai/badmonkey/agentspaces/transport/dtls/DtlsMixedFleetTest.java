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

import ai.badmonkey.agentspaces.api.ad.GroupAdvertisement;
import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.membership.GroupMembership;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import ai.badmonkey.agentspaces.peering.transport.TlsTcpTransport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A peer advertising {@code dtls} at priority 0 and {@code tls} at 1 is
 * reached over DTLS, and once its DTLS listener is gone a dialer tries the
 * DTLS endpoint first and falls through to TLS
 * (SRAD scenario 8; DT-N-05, DT-Q-REL-06, DT-Q-AVL-02).
 */
class DtlsMixedFleetTest {

    /** Counts the connections a binding accepts. */
    static final class Counting implements Transport {
        final Transport delegate;
        final AtomicInteger accepted = new AtomicInteger();

        Counting(Transport delegate) {
            this.delegate = delegate;
        }

        @Override
        public String scheme() {
            return delegate.scheme();
        }

        @Override
        public Set<Capability> capabilities() {
            return delegate.capabilities();
        }

        @Override
        public TransportConnection dial(String address) throws IOException {
            return delegate.dial(address);
        }

        @Override
        public AutoCloseable listen(String bindAddress, Consumer<TransportConnection> onAccept) throws IOException {
            return delegate.listen(bindAddress, connection -> {
                accepted.incrementAndGet();
                onAccept.accept(connection);
            });
        }
    }

    @Test
    @Timeout(120)
    void dtlsIsDialedFirstAndTlsTakesOverWhenDtlsGoesAway() throws Exception {
        GroupId groupId = GroupId.fromFounding("dtls-mixed-fleet".getBytes(StandardCharsets.UTF_8));
        GroupAdvertisement group = new GroupAdvertisement("aspace://" + groupId.value(),
                PeerIdentity.generate().peerId(), groupId, Instant.EPOCH, Duration.ofDays(1), "dtls-mixed",
                GroupAdvertisement.MembershipPolicy.OPEN, ConflictStrategyType.LEASE_RACE,
                GroupAdvertisement.GossipParameters.defaults());
        GroupMembership.Config config = new GroupMembership.Config(Duration.ofMinutes(5), Duration.ofSeconds(30), 2);

        PeerIdentity idA = PeerIdentity.generate();
        PeerIdentity idB = PeerIdentity.generate();
        int dtlsPort = freeUdpPort();
        int tlsPort = freeTcpPort();
        DtlsTransport dtlsA = new DtlsTransport(idA);
        Counting tlsA = new Counting(new TlsTcpTransport(idA));
        DtlsTransport dtlsB = new DtlsTransport(idB);
        PeerNode a = PeerNode.builder(idA).build();
        PeerNode b = PeerNode.builder(idB).build();
        try {
            a.listen(dtlsA, "127.0.0.1:" + dtlsPort, 0);
            a.listen(tlsA, "127.0.0.1:" + tlsPort, 1);
            b.transport(dtlsB);
            b.transport(new TlsTcpTransport(idB));
            GroupRuntime atA = a.joinGroup(group, config, List.of());
            // Every seed is introduced to (seeds may be different peers), so B seeds over DTLS alone;
            // A's advertisement then carries both endpoints for B's later dials.
            GroupRuntime atB = b.joinGroup(group, config, List.of(
                    new PeerAdvertisement.Endpoint("dtls", "127.0.0.1:" + dtlsPort, 0)));
            a.startTicking(Duration.ofMillis(200));
            b.startTicking(Duration.ofMillis(200));

            List<String> news = new CopyOnWriteArrayList<>();
            atA.gossip().onStream("news", (from, id, payload) -> news.add(new String(payload, StandardCharsets.UTF_8)));
            await("membership over DTLS", () -> atA.membership().member(idB.peerId()).isPresent()
                    && atB.membership().member(idA.peerId()).isPresent());
            atB.gossip().publish("news", "n1", "over-dtls".getBytes(StandardCharsets.UTF_8));
            await("news over DTLS", () -> news.contains("over-dtls"));
            assertThat(dtlsA.counters().associationsOpened()).isPositive();
            assertThat(tlsA.accepted.get()).isZero();

            dtlsA.close(); // the DTLS listener and its associations go away
            atB.gossip().publish("news", "n2", "over-tls".getBytes(StandardCharsets.UTF_8));
            await("news over TLS after DTLS went away", () -> news.contains("over-tls"));
            assertThat(tlsA.accepted.get()).isPositive();
            // B tried A's priority-0 DTLS endpoint first, and only then fell through to TLS.
            assertThat(dtlsB.counters().handshakesFailed()).isPositive();
        } finally {
            a.close();
            b.close();
            dtlsA.close();
            dtlsB.close();
        }
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.sleep(100);
        }
    }

    private static int freeUdpPort() throws IOException {
        try (DatagramSocket socket = new DatagramSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static int freeTcpPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
