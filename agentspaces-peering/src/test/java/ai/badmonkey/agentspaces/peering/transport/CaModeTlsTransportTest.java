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
import ai.badmonkey.agentspaces.identity.ChannelTrust;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.identity.ServingCredential;
import ai.badmonkey.agentspaces.test.TestCa;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.ServerSocket;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The enterprise-CA channel mode over real TLS 1.3 (plan §3/WS4): peers with
 * CA-issued credentials attest each other; a peer with only a self-signed
 * identity-endorsed certificate still connects but attests nothing; and a
 * CA-revoked credential attests nothing on its very next handshake.
 */
class CaModeTlsTransportTest {

    private final TestCa ca = TestCa.create();

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private TlsTcpTransport caTransport(PeerIdentity identity, ChannelTrust trust)
            throws Exception {
        TestCa.Issued issued = ca.issue(identity.rawPublicKey());
        return new TlsTcpTransport(identity,
                new ServingCredential(issued.key().getPrivate(), issued.chain(ca)),
                trust);
    }

    @Test
    @Timeout(30)
    void caIssuedPeersAttestAndSelfSignedPeersDoNot() throws Exception {
        PeerIdentity serverIdentity = PeerIdentity.generate();
        PeerIdentity clientIdentity = PeerIdentity.generate();
        PeerIdentity outsiderIdentity = PeerIdentity.generate();
        ChannelTrust trust = ChannelTrust.of(List.of(ca.certificate()));

        TlsTcpTransport server = caTransport(serverIdentity, trust);
        TlsTcpTransport client = caTransport(clientIdentity, trust);
        // Enrolled nowhere: a plain self-signed identity-endorsed certificate.
        TlsTcpTransport outsider = new TlsTcpTransport(outsiderIdentity);

        int port = freePort();
        BlockingQueue<TransportConnection> accepted = new ArrayBlockingQueue<>(2);
        try (AutoCloseable listener = server.listen("127.0.0.1:" + port, accepted::add)) {
            TransportConnection enrolled = client.dial("127.0.0.1:" + port);
            assertThat(enrolled.attestedPeer())
                    .as("the CA vouches the server's PeerID binding")
                    .contains(serverIdentity.peerId());
            TransportConnection atServer = accepted.poll(10, TimeUnit.SECONDS);
            assertThat(atServer).isNotNull();
            assertThat(atServer.attestedPeer())
                    .as("mutually: the CA vouches the client too")
                    .contains(clientIdentity.peerId());

            // The unenrolled peer connects — signed frames still work — but no
            // CA vouches its binding, so it attests nothing at the server.
            TransportConnection unenrolled = outsider.dial("127.0.0.1:" + port);
            assertThat(unenrolled).isNotNull();
            TransportConnection outsiderAtServer = accepted.poll(10, TimeUnit.SECONDS);
            assertThat(outsiderAtServer).isNotNull();
            assertThat(outsiderAtServer.attestedPeer()).isEmpty();
        }
    }

    @Test
    @Timeout(30)
    void aCaRevokedCredentialAttestsNothingOnItsNextHandshake() throws Exception {
        PeerIdentity serverIdentity = PeerIdentity.generate();
        PeerIdentity revokedIdentity = PeerIdentity.generate();
        TestCa.Issued revokedLeaf = ca.issue(revokedIdentity.rawPublicKey());

        // The server's trust carries the CRL naming the revoked leaf: the
        // authoritative eject, independent of anything the fabric gossips.
        ChannelTrust trust = ChannelTrust.withCrls(List.of(ca.certificate()),
                List.of(ca.crl(revokedLeaf.certificate())));
        TlsTcpTransport server = caTransport(serverIdentity, trust);
        TlsTcpTransport revoked = new TlsTcpTransport(revokedIdentity,
                new ServingCredential(revokedLeaf.key().getPrivate(),
                        revokedLeaf.chain(ca)),
                ChannelTrust.withCrls(List.of(ca.certificate()), List.of(ca.crl())));

        int port = freePort();
        BlockingQueue<TransportConnection> accepted = new ArrayBlockingQueue<>(1);
        try (AutoCloseable listener = server.listen("127.0.0.1:" + port, accepted::add)) {
            TransportConnection outbound = revoked.dial("127.0.0.1:" + port);
            assertThat(outbound).isNotNull();
            TransportConnection atServer = accepted.poll(10, TimeUnit.SECONDS);
            assertThat(atServer).isNotNull();
            assertThat(atServer.attestedPeer())
                    .as("the CRL withdrew this credential; it attests nothing")
                    .isEmpty();
        }
    }
}
