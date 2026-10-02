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
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.ServerSocket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A QUIC listener accepts any number of connections, in every identity mode.
 * Found while porting the CA tests (TODO item 11): the server codec shared one
 * non-{@code @Sharable} connection handler, so the second connection's
 * pipeline refused it and every listener accepted exactly one peer.
 */
class QuicListenerTest {

    @BeforeAll
    static void requireNative() {
        QuicNative.assumeAvailable();
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void acceptsThree(QuicTransport server, QuicTransport... clients) throws Exception {
        int port = freePort();
        BlockingQueue<TransportConnection> accepted = new ArrayBlockingQueue<>(8);
        try (AutoCloseable listener = server.listen("127.0.0.1:" + port, connection -> {
            connection.onReceive(frame -> { });
            accepted.add(connection);
        })) {
            for (int i = 0; i < 3; i++) {
                TransportConnection dialed = QuicNative.dial(clients[i % clients.length], "127.0.0.1:" + port);
                dialed.send(new byte[]{(byte) i});
                assertThat(accepted.poll(15, TimeUnit.SECONDS)).as("connection %d accepted", i)
                        .isNotNull();
            }
        }
    }

    @Test
    @Timeout(60)
    void anUnattestedListenerAcceptsManyConnections() throws Exception {
        try (QuicTransport server = new QuicTransport();
             QuicTransport a = new QuicTransport();
             QuicTransport b = new QuicTransport()) {
            acceptsThree(server, a, b);
        }
    }

    @Test
    @Timeout(60)
    void anAttestedListenerAcceptsManyConnectionsFromOneOrManyClients() throws Exception {
        try (QuicTransport server = new QuicTransport(PeerIdentity.generate());
             QuicTransport a = new QuicTransport(PeerIdentity.generate());
             QuicTransport b = new QuicTransport(PeerIdentity.generate())) {
            acceptsThree(server, a, b);
            acceptsThree(server, a);
        }
    }
}
