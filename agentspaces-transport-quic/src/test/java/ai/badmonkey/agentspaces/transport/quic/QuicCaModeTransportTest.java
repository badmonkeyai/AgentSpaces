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
import ai.badmonkey.agentspaces.identity.ChannelTrust;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.identity.ServingCredential;
import ai.badmonkey.agentspaces.test.TestCa;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TODO item 11 (QA2 M): the enterprise-CA channel mode over QUIC, ported from
 * {@code CaModeTlsTransportTest}. A CA-issued credential attests its PeerID on
 * both ends, an unenrolled self-signed peer connects but attests nothing, and
 * a CRL-revoked credential attests nothing on its next connection.
 */
class QuicCaModeTransportTest {

    private final TestCa ca = TestCa.create();

    @BeforeAll
    static void requireNative() {
        QuicNative.assumeAvailable();
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private QuicTransport caTransport(PeerIdentity identity, ChannelTrust trust) throws Exception {
        TestCa.Issued issued = ca.issue(identity.rawPublicKey());
        return new QuicTransport(identity,
                new ServingCredential(issued.key().getPrivate(), issued.chain(ca)), trust);
    }

    /** Dials, sends one frame so the listener sees the stream, and returns the accepted end. */
    private static TransportConnection[] connect(QuicTransport client, int port,
                                                 BlockingQueue<TransportConnection> accepted)
            throws Exception {
        TransportConnection dialed = QuicNative.dial(client, "127.0.0.1:" + port);
        dialed.send("hello".getBytes(StandardCharsets.UTF_8));
        TransportConnection inbound = accepted.poll(15, TimeUnit.SECONDS);
        assertThat(inbound).isNotNull();
        return new TransportConnection[]{dialed, inbound};
    }

    @Test
    @Timeout(60)
    void caIssuedPeersAttestAndSelfSignedPeersDoNot() throws Exception {
        PeerIdentity serverIdentity = PeerIdentity.generate();
        PeerIdentity clientIdentity = PeerIdentity.generate();
        ChannelTrust trust = ChannelTrust.of(List.of(ca.certificate()));
        int port = freePort();
        try (QuicTransport server = caTransport(serverIdentity, trust);
             QuicTransport client = caTransport(clientIdentity, trust);
             QuicTransport outsider = new QuicTransport(PeerIdentity.generate())) {
            BlockingQueue<TransportConnection> accepted = new ArrayBlockingQueue<>(4);
            try (AutoCloseable listener = server.listen("127.0.0.1:" + port, connection -> {
                connection.onReceive(frame -> { });
                accepted.add(connection);
            })) {
                TransportConnection[] enrolled = connect(client, port, accepted);
                assertThat(enrolled[0].attestedPeer())
                        .as("the CA vouches the server's PeerID binding")
                        .contains(serverIdentity.peerId());
                assertThat(enrolled[1].attestedPeer())
                        .as("mutually: the CA vouches the client too")
                        .contains(clientIdentity.peerId());

                TransportConnection[] unenrolled = connect(outsider, port, accepted);
                assertThat(unenrolled[1].attestedPeer())
                        .as("a self-signed certificate attests nothing under CA trust").isEmpty();
                enrolled[0].close();
                unenrolled[0].close();
            }
        }
    }

    @Test
    @Timeout(60)
    void aCaRevokedCredentialAttestsNothingOnItsNextConnection() throws Exception {
        PeerIdentity serverIdentity = PeerIdentity.generate();
        PeerIdentity revokedIdentity = PeerIdentity.generate();
        TestCa.Issued revokedLeaf = ca.issue(revokedIdentity.rawPublicKey());
        ChannelTrust trust = ChannelTrust.withCrls(List.of(ca.certificate()),
                List.of(ca.crl(revokedLeaf.certificate())));
        int port = freePort();
        try (QuicTransport server = caTransport(serverIdentity, trust);
             QuicTransport revoked = new QuicTransport(revokedIdentity,
                     new ServingCredential(revokedLeaf.key().getPrivate(), revokedLeaf.chain(ca)),
                     ChannelTrust.withCrls(List.of(ca.certificate()), List.of(ca.crl())))) {
            BlockingQueue<TransportConnection> accepted = new ArrayBlockingQueue<>(2);
            try (AutoCloseable listener = server.listen("127.0.0.1:" + port, connection -> {
                connection.onReceive(frame -> { });
                accepted.add(connection);
            })) {
                TransportConnection[] ends = connect(revoked, port, accepted);
                assertThat(ends[1].attestedPeer())
                        .as("the CRL withdrew this credential; it attests nothing").isEmpty();
                ends[0].close();
            }
        }
    }

    /** ASF-021 parity: two sequential connections each attest from a fresh full handshake; a second connection after the CRL changes reflects the change (no cached session carries the old verdict). */
    @Test
    @Timeout(60)
    void everyConnectionIsAttestedAfreshUnderTheCurrentTrust() throws Exception {
        PeerIdentity serverIdentity = PeerIdentity.generate();
        PeerIdentity clientIdentity = PeerIdentity.generate();
        TestCa.Issued clientLeaf = ca.issue(clientIdentity.rawPublicKey());
        java.util.concurrent.atomic.AtomicReference<ChannelTrust> current =
                new java.util.concurrent.atomic.AtomicReference<>(
                        ChannelTrust.withCrls(List.of(ca.certificate()), List.of(ca.crl())));
        TestCa.Issued serverLeaf = ca.issue(serverIdentity.rawPublicKey());
        int port = freePort();
        try (QuicTransport server = QuicTransport.withRefreshableTrust(serverIdentity,
                     new ServingCredential(serverLeaf.key().getPrivate(), serverLeaf.chain(ca)),
                     current::get);
             QuicTransport client = new QuicTransport(clientIdentity,
                     new ServingCredential(clientLeaf.key().getPrivate(), clientLeaf.chain(ca)),
                     ChannelTrust.of(List.of(ca.certificate())))) {
            BlockingQueue<TransportConnection> accepted = new ArrayBlockingQueue<>(4);
            try (AutoCloseable listener = server.listen("127.0.0.1:" + port, connection -> {
                connection.onReceive(frame -> { });
                accepted.add(connection);
            })) {
                TransportConnection[] first = connect(client, port, accepted);
                assertThat(first[1].attestedPeer()).contains(clientIdentity.peerId());
                first[0].close();

                current.set(ChannelTrust.withCrls(List.of(ca.certificate()),
                        List.of(ca.crl(clientLeaf.certificate()))));
                TransportConnection[] second = connect(client, port, accepted);
                assertThat(second[1].attestedPeer())
                        .as("the second connection is judged under the refreshed CRL").isEmpty();
                second[0].close();
            }
        }
    }
}
