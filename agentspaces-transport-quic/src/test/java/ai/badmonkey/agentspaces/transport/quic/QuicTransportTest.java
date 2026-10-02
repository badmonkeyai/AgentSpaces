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
package ai.badmonkey.agentspaces.transport.quic;

import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.DatagramSocket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The QUIC transport as a frame channel: dial, exchange, framing bounds. */
class QuicTransportTest {

    @org.junit.jupiter.api.BeforeAll
    static void requireNative() {
        QuicNative.assumeAvailable();
    }

    private final QuicTransport transport = new QuicTransport();

    @AfterEach
    void tearDown() {
        transport.close();
    }

    @Test
    @Timeout(60)
    void framesFlowBothWaysOverOneQuicStream() throws Exception {
        int port = freeUdpPort();
        BlockingQueue<byte[]> serverReceived = new ArrayBlockingQueue<>(16);
        BlockingQueue<TransportConnection> accepted = new ArrayBlockingQueue<>(1);
        AutoCloseable listener = transport.listen("127.0.0.1:" + port, connection -> {
            connection.onReceive(serverReceived::add);
            accepted.add(connection);
        });
        try {
            TransportConnection dialed = transport.dial("127.0.0.1:" + port);
            BlockingQueue<byte[]> clientReceived = new ArrayBlockingQueue<>(16);
            dialed.onReceive(clientReceived::add);

            dialed.send("hello over quic".getBytes(StandardCharsets.UTF_8));
            byte[] atServer = serverReceived.poll(15, TimeUnit.SECONDS);
            assertThat(atServer).isNotNull();
            assertThat(new String(atServer, StandardCharsets.UTF_8))
                    .isEqualTo("hello over quic");

            TransportConnection server = accepted.poll(5, TimeUnit.SECONDS);
            assertThat(server).isNotNull();
            server.send("hello back".getBytes(StandardCharsets.UTF_8));
            byte[] atClient = clientReceived.poll(15, TimeUnit.SECONDS);
            assertThat(atClient).isNotNull();
            assertThat(new String(atClient, StandardCharsets.UTF_8)).isEqualTo("hello back");

            // A large frame survives QUIC's packetization intact.
            byte[] large = new byte[512 * 1024];
            for (int i = 0; i < large.length; i++) {
                large[i] = (byte) (i % 251);
            }
            dialed.send(large);
            byte[] largeAtServer = serverReceived.poll(20, TimeUnit.SECONDS);
            assertThat(largeAtServer).isEqualTo(large);

            dialed.close();
        } finally {
            listener.close();
        }
    }

    @Test
    @Timeout(30)
    void oversizeFramesAreRefusedAndUnreachablePeersFailFast() throws Exception {
        int port = freeUdpPort();
        AutoCloseable listener = transport.listen("127.0.0.1:" + port, connection -> {
        });
        try {
            TransportConnection dialed = transport.dial("127.0.0.1:" + port);
            assertThatThrownBy(() -> dialed.send(new byte[9 * 1024 * 1024]))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("frame exceeds");
            dialed.close();
        } finally {
            listener.close();
        }
        // Dialing a port nobody serves fails with an IOException, not a hang.
        int deadPort = freeUdpPort();
        assertThatThrownBy(() -> transport.dial("127.0.0.1:" + deadPort))
                .isInstanceOf(IOException.class);
    }

    private static int freeUdpPort() throws IOException {
        try (DatagramSocket socket = new DatagramSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
