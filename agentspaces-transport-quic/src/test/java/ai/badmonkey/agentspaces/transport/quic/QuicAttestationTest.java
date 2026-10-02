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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Attested mode over real QUIC (spec §5.6): both handshake sides present
 * identity-endorsed channel certificates, both connections report the bound
 * PeerIDs, and the legacy unattested mode keeps reporting nothing.
 */
class QuicAttestationTest {

    @org.junit.jupiter.api.BeforeAll
    static void requireNative() {
        QuicNative.assumeAvailable();
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    @Timeout(60)
    void mutualChannelCertificatesAttestBothEndsOverQuic() throws Exception {
        PeerIdentity serverIdentity = PeerIdentity.generate();
        PeerIdentity clientIdentity = PeerIdentity.generate();
        int port = freePort();

        try (QuicTransport serverTransport = new QuicTransport(serverIdentity);
             QuicTransport clientTransport = new QuicTransport(clientIdentity)) {
            BlockingQueue<TransportConnection> accepted = new ArrayBlockingQueue<>(1);
            BlockingQueue<byte[]> serverInbox = new ArrayBlockingQueue<>(1);
            AutoCloseable listener = serverTransport.listen("127.0.0.1:" + port,
                    connection -> {
                        connection.onReceive(serverInbox::add);
                        accepted.add(connection);
                    });
            try {
                TransportConnection dialed = clientTransport.dial("127.0.0.1:" + port);

                assertThat(dialed.attestedPeer()).contains(serverIdentity.peerId());

                dialed.send("attested hello".getBytes(StandardCharsets.UTF_8));
                byte[] frame = serverInbox.poll(15, TimeUnit.SECONDS);
                assertThat(frame).isNotNull();
                assertThat(new String(frame, StandardCharsets.UTF_8))
                        .isEqualTo("attested hello");

                TransportConnection inbound = accepted.poll(15, TimeUnit.SECONDS);
                assertThat(inbound).isNotNull();
                assertThat(inbound.attestedPeer()).contains(clientIdentity.peerId());

                dialed.close();
            } finally {
                listener.close();
            }
        }
    }

    @Test
    @Timeout(60)
    void legacyModeStaysUnattestedInBothDirections() throws Exception {
        int port = freePort();
        try (QuicTransport transport = new QuicTransport()) {
            BlockingQueue<TransportConnection> accepted = new ArrayBlockingQueue<>(1);
            AutoCloseable listener = transport.listen("127.0.0.1:" + port, accepted::add);
            try {
                TransportConnection dialed = transport.dial("127.0.0.1:" + port);
                assertThat(dialed.attestedPeer()).isEmpty();

                dialed.send("ping".getBytes(StandardCharsets.UTF_8));
                TransportConnection inbound = accepted.poll(15, TimeUnit.SECONDS);
                assertThat(inbound).isNotNull();
                assertThat(inbound.attestedPeer()).isEmpty();

                dialed.close();
            } finally {
                listener.close();
            }
        }
    }
}
