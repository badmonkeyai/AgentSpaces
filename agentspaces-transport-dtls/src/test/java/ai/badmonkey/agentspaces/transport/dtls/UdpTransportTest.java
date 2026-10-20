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
import ai.badmonkey.agentspaces.peering.node.PeerNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The bare {@code udp} binding (SRAD dtls-support group U). */
class UdpTransportTest {

    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable closeable : closeables.reversed()) {
            closeable.close();
        }
    }

    @Test
    @Timeout(60)
    void framesFlowReliablyInBothDirectionsAndNobodyIsAttested() throws Exception {
        UdpTransport listener = new UdpTransport();
        UdpTransport dialer = new UdpTransport();
        closeables.add(dialer);
        closeables.add(listener);
        BlockingQueue<TransportConnection> accepted = new LinkedBlockingQueue<>();
        BlockingQueue<byte[]> atListener = new LinkedBlockingQueue<>();
        int port = listener.listenPort("127.0.0.1:0", c -> {
            c.onReceive(atListener::add);
            accepted.add(c);
        });
        UdpRelay relay = new UdpRelay(port, 0.10, 3); // a lossy segment
        closeables.add(relay);
        TransportConnection dialed = dialer.dial(relay.address());
        BlockingQueue<byte[]> atDialer = new LinkedBlockingQueue<>();
        dialed.onReceive(atDialer::add);
        byte[] block = new byte[100_000];
        new java.util.Random(4).nextBytes(block);
        for (int i = 0; i < 20; i++) {
            dialed.send(("udp-" + i).getBytes());
        }
        dialed.send(block);
        for (int i = 0; i < 20; i++) {
            assertThat(new String(atListener.poll(30, TimeUnit.SECONDS))).isEqualTo("udp-" + i);
        }
        assertThat(atListener.poll(30, TimeUnit.SECONDS)).isEqualTo(block);
        TransportConnection inbound = accepted.poll(5, TimeUnit.SECONDS);
        inbound.send("back".getBytes());
        assertThat(atDialer.poll(30, TimeUnit.SECONDS)).isEqualTo("back".getBytes());
        assertThat(dialed.attestedPeer()).isEmpty();
        assertThat(inbound.attestedPeer()).isEmpty();
        assertThat(listener.counters().associationsOpened()).isEqualTo(1);
    }

    @Test
    @Timeout(30)
    void theHelloIsAnsweredSmallerAndForgedTokensAllocateNothing() throws Exception {
        UdpTransport listener = new UdpTransport();
        closeables.add(listener);
        int port = listener.listenPort("127.0.0.1:0", c -> { });
        InetSocketAddress target = new InetSocketAddress("127.0.0.1", port);
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.setSoTimeout(2_000);
            byte[] hello = ByteBuffer.allocate(UdpTransport.HELLO_LENGTH).put((byte) 1).put((byte) UdpTransport.HELLO)
                    .putShort((short) 0).putLong(42).array();
            socket.send(new DatagramPacket(hello, hello.length, target));
            byte[] buf = new byte[256];
            DatagramPacket reply = new DatagramPacket(buf, buf.length);
            socket.receive(reply);
            assertThat(reply.getLength()).isLessThan(hello.length);
            assertThat(ByteBuffer.wrap(buf, 4, 8).getLong()).isEqualTo(42);

            // A PDU with a guessed token is dropped and allocates nothing.
            byte[] forged = new byte[Pdu.DATA_HEADER + PlainRecordLayer.TOKEN_LENGTH + 1];
            forged[0] = 1;
            forged[1] = Pdu.PING;
            ByteBuffer.wrap(forged, 2, 8).putLong(12345);
            socket.send(new DatagramPacket(forged, forged.length, target));
            Thread.sleep(300);
            assertThat(listener.counters().associationsOpened()).isZero();
            assertThatThrownBy(() -> {
                socket.setSoTimeout(500);
                socket.receive(new DatagramPacket(buf, buf.length));
            }).isInstanceOf(SocketTimeoutException.class);
        }
    }

    @Test
    void itNeitherEncryptsNorAttestsSoANodeRequiringAttestationRefusesIt() throws Exception {
        UdpTransport udp = new UdpTransport();
        closeables.add(udp);
        assertThat(udp.scheme()).isEqualTo("udp");
        assertThat(udp.capabilities()).containsExactlyInAnyOrder(Transport.Capability.RELIABLE,
                Transport.Capability.ORDERED);
        try (PeerNode node = PeerNode.builder(PeerIdentity.generate()).requireAttestation(true).build()) {
            assertThatThrownBy(() -> node.transport(udp)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    @Timeout(30)
    void anUnansweredHelloFailsTheDial() throws Exception {
        UdpTransport dialer = new UdpTransport();
        closeables.add(dialer);
        int port;
        try (DatagramSocket nobody = new DatagramSocket(0)) {
            port = nobody.getLocalPort();
        }
        assertThatThrownBy(() -> dialer.dial("127.0.0.1:" + port))
                .isInstanceOf(java.io.IOException.class).hasMessageContaining("over UDP");
    }
}
