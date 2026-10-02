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
package ai.badmonkey.agentspaces.peering.transport;

import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TcpTransportTest {

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    void framesRoundTripOverLocalhost() throws Exception {
        TcpTransport transport = new TcpTransport();
        int port = freePort();
        BlockingQueue<String> serverInbox = new LinkedBlockingQueue<>();
        AtomicReference<TransportConnection> serverSide = new AtomicReference<>();

        try (AutoCloseable listener = transport.listen("127.0.0.1:" + port, connection -> {
            serverSide.set(connection);
            connection.onReceive(frame ->
                    serverInbox.add(new String(frame, StandardCharsets.UTF_8)));
        })) {
            TransportConnection client = transport.dial("127.0.0.1:" + port);
            BlockingQueue<String> clientInbox = new LinkedBlockingQueue<>();
            client.onReceive(frame -> clientInbox.add(new String(frame, StandardCharsets.UTF_8)));

            client.send("ping over tcp".getBytes(StandardCharsets.UTF_8));
            assertThat(serverInbox.poll(5, TimeUnit.SECONDS)).isEqualTo("ping over tcp");

            serverSide.get().send("pong".getBytes(StandardCharsets.UTF_8));
            assertThat(clientInbox.poll(5, TimeUnit.SECONDS)).isEqualTo("pong");

            client.close();
        }
    }

    @Test
    void multipleFramesArriveInOrder() throws Exception {
        TcpTransport transport = new TcpTransport();
        int port = freePort();
        BlockingQueue<String> inbox = new LinkedBlockingQueue<>();

        try (AutoCloseable listener = transport.listen("127.0.0.1:" + port, connection ->
                connection.onReceive(frame -> inbox.add(new String(frame, StandardCharsets.UTF_8))))) {
            TransportConnection client = transport.dial("127.0.0.1:" + port);
            for (String message : List.of("one", "two", "three")) {
                client.send(message.getBytes(StandardCharsets.UTF_8));
            }
            assertThat(inbox.poll(5, TimeUnit.SECONDS)).isEqualTo("one");
            assertThat(inbox.poll(5, TimeUnit.SECONDS)).isEqualTo("two");
            assertThat(inbox.poll(5, TimeUnit.SECONDS)).isEqualTo("three");
            client.close();
        }
    }

    /** Spec §5.5 / TECH-SPEC §5.5: the TCP binding refuses frames over its 8 MiB maximum before writing. */
    @Test
    void oversizeFramesAreRefused() throws Exception {
        TcpTransport transport = new TcpTransport();
        int port = freePort();
        BlockingQueue<Integer> inbox = new LinkedBlockingQueue<>();

        try (AutoCloseable listener = transport.listen("127.0.0.1:" + port, connection ->
                connection.onReceive(frame -> inbox.add(frame.length)))) {
            TransportConnection client = transport.dial("127.0.0.1:" + port);
            assertThatThrownBy(() -> client.send(new byte[8 * 1024 * 1024 + 1]))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("frame exceeds");

            // The connection is still usable: the oversize frame never hit the wire.
            client.send(new byte[]{1, 2, 3});
            assertThat(inbox.poll(5, TimeUnit.SECONDS)).isEqualTo(3);
            client.close();
        }
    }

    @Test
    void dialToClosedPortFails() throws Exception {
        TcpTransport transport = new TcpTransport();
        int port = freePort();

        assertThatThrownBy(() -> transport.dial("127.0.0.1:" + port))
                .isInstanceOf(IOException.class);
    }

    @Test
    void addressParsingValidates() {
        assertThat(TcpTransport.parse("10.0.0.5:7401").getPort()).isEqualTo(7401);
        assertThatThrownBy(() -> TcpTransport.parse("no-port"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
