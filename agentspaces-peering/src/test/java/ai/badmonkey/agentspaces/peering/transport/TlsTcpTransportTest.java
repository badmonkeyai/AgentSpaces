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
import ai.badmonkey.agentspaces.identity.ChannelCertificate;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.identity.ServingCredential;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real TLS 1.3 over loopback: mutual channel certificates attest both ends of
 * the connection, frames round-trip through the same length-prefixed loop as
 * TCP, and a client without a channel certificate still connects but stays
 * unattested.
 */
class TlsTcpTransportTest {

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    @Timeout(30)
    void mutualChannelCertificatesAttestBothEndsAndFramesFlow() throws Exception {
        PeerIdentity serverIdentity = PeerIdentity.generate();
        PeerIdentity clientIdentity = PeerIdentity.generate();
        TlsTcpTransport serverTransport = new TlsTcpTransport(serverIdentity);
        TlsTcpTransport clientTransport = new TlsTcpTransport(clientIdentity);
        int port = freePort();

        BlockingQueue<TransportConnection> accepted = new ArrayBlockingQueue<>(1);
        BlockingQueue<byte[]> serverInbox = new ArrayBlockingQueue<>(1);
        try (AutoCloseable listener = serverTransport.listen("127.0.0.1:" + port, connection -> {
            connection.onReceive(frame -> serverInbox.add(frame));
            accepted.add(connection);
        })) {
            TransportConnection outbound = clientTransport.dial("127.0.0.1:" + port);
            BlockingQueue<byte[]> clientInbox = new ArrayBlockingQueue<>(1);
            outbound.onReceive(clientInbox::add);

            // The dialer attested the listener's identity.
            assertThat(outbound.attestedPeer()).contains(serverIdentity.peerId());

            outbound.send("hello over tls".getBytes(StandardCharsets.UTF_8));
            byte[] atServer = serverInbox.poll(10, TimeUnit.SECONDS);
            assertThat(atServer).isNotNull();
            assertThat(new String(atServer, StandardCharsets.UTF_8))
                    .isEqualTo("hello over tls");

            // The listener attested the dialer's identity (mutual TLS).
            TransportConnection inbound = accepted.poll(10, TimeUnit.SECONDS);
            assertThat(inbound).isNotNull();
            assertThat(inbound.attestedPeer()).contains(clientIdentity.peerId());

            inbound.send("and back".getBytes(StandardCharsets.UTF_8));
            byte[] atClient = clientInbox.poll(10, TimeUnit.SECONDS);
            assertThat(atClient).isNotNull();
            assertThat(new String(atClient, StandardCharsets.UTF_8)).isEqualTo("and back");

            outbound.close();
            inbound.close();
        }
    }

    @Test
    @Timeout(30)
    void aClientWithoutAChannelCertificateConnectsUnattested() throws Exception {
        PeerIdentity serverIdentity = PeerIdentity.generate();
        TlsTcpTransport serverTransport = new TlsTcpTransport(serverIdentity);
        int port = freePort();

        BlockingQueue<TransportConnection> accepted = new ArrayBlockingQueue<>(1);
        BlockingQueue<byte[]> serverInbox = new ArrayBlockingQueue<>(1);
        try (AutoCloseable listener = serverTransport.listen("127.0.0.1:" + port, connection -> {
            connection.onReceive(serverInbox::add);
            accepted.add(connection);
        })) {
            // A bare TLS client: trusts anything, presents nothing.
            SSLContext anonymous = SSLContext.getInstance("TLS");
            anonymous.init(null, new javax.net.ssl.TrustManager[]{
                    new X509ExtendedTrustManager() {
                        @Override
                        public void checkClientTrusted(X509Certificate[] c, String a) {
                        }

                        @Override
                        public void checkClientTrusted(X509Certificate[] c, String a,
                                                       Socket s) {
                        }

                        @Override
                        public void checkClientTrusted(X509Certificate[] c, String a,
                                                       javax.net.ssl.SSLEngine e) {
                        }

                        @Override
                        public void checkServerTrusted(X509Certificate[] c, String a) {
                        }

                        @Override
                        public void checkServerTrusted(X509Certificate[] c, String a,
                                                       Socket s) {
                        }

                        @Override
                        public void checkServerTrusted(X509Certificate[] c, String a,
                                                       javax.net.ssl.SSLEngine e) {
                        }

                        @Override
                        public X509Certificate[] getAcceptedIssuers() {
                            return new X509Certificate[0];
                        }
                    }}, null);
            try (SSLSocket socket = (SSLSocket) anonymous.getSocketFactory()
                    .createSocket("127.0.0.1", port)) {
                socket.startHandshake();
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                byte[] payload = "unattested frame".getBytes(StandardCharsets.UTF_8);
                out.writeInt(payload.length);
                out.write(payload);
                out.flush();

                TransportConnection inbound = accepted.poll(10, TimeUnit.SECONDS);
                assertThat(inbound).isNotNull();
                assertThat(inbound.attestedPeer()).isEmpty();
                byte[] frame = serverInbox.poll(10, TimeUnit.SECONDS);
                assertThat(frame).isNotNull();
                assertThat(new String(frame, StandardCharsets.UTF_8))
                        .isEqualTo("unattested frame");
            }
        }
    }

    /** SPEC §5.6 attestation over real TLS 1.3: a certificate whose CN names the victim but whose Ed25519 endorsement is the attacker's still connects, and attests nothing. */
    @Test
    @Timeout(30)
    void aForgedChannelCertificateConnectsButAttestsNothingOverRealTls() throws Exception {
        PeerIdentity serverIdentity = PeerIdentity.generate();
        PeerIdentity victim = PeerIdentity.generate();
        PeerIdentity attacker = PeerIdentity.generate();
        TlsTcpTransport serverTransport = new TlsTcpTransport(serverIdentity);

        // The attacker presents a certificate with the victim's identity key in
        // the CN but re-signs the TBSCertificate with its own identity key. The
        // P-256 TLS key is one the attacker holds (here: the victim's channel
        // key, handed over by the test), so the TLS handshake itself succeeds;
        // only the identity endorsement is wrong, and that alone must be enough.
        ChannelCertificate victimChannel = ChannelCertificate.generate(victim);
        byte[] tbs = victimChannel.certificate().getTBSCertificate();
        X509Certificate forged = (X509Certificate) CertificateFactory.getInstance("X.509")
                .generateCertificate(new ByteArrayInputStream(
                        reassemble(tbs, attacker.sign(tbs))));
        assertThat(ChannelCertificate.attest(forged)).as("precondition: unit attest refuses").isEmpty();
        TlsTcpTransport forger = new TlsTcpTransport(attacker,
                new ServingCredential(victimChannel.tlsKey(), new X509Certificate[]{forged}),
                null);

        int port = freePort();
        BlockingQueue<TransportConnection> accepted = new ArrayBlockingQueue<>(1);
        BlockingQueue<byte[]> serverInbox = new ArrayBlockingQueue<>(1);
        try (AutoCloseable listener = serverTransport.listen("127.0.0.1:" + port, connection -> {
            connection.onReceive(serverInbox::add);
            accepted.add(connection);
        })) {
            TransportConnection outbound = forger.dial("127.0.0.1:" + port);
            // The honest server is attested toward the dialer as usual.
            assertThat(outbound.attestedPeer()).contains(serverIdentity.peerId());
            outbound.send("frame from a forged channel".getBytes(StandardCharsets.UTF_8));

            TransportConnection inbound = accepted.poll(10, TimeUnit.SECONDS);
            assertThat(inbound).isNotNull();
            assertThat(inbound.attestedPeer())
                    .as("neither the victim (whose key did not endorse) nor the attacker "
                            + "(whose key is not in the CN) is attested")
                    .isEmpty();
            // The channel still carries frames; they simply stay fully signed.
            byte[] frame = serverInbox.poll(10, TimeUnit.SECONDS);
            assertThat(frame).isNotNull();
            assertThat(new String(frame, StandardCharsets.UTF_8))
                    .isEqualTo("frame from a forged channel");
            outbound.close();
            inbound.close();
        }
    }

    /** Re-encloses a TBSCertificate with an id-Ed25519 AlgorithmIdentifier and the given signature (DER). */
    private static byte[] reassemble(byte[] tbs, byte[] signature) {
        byte[] algorithm = new byte[]{0x30, 0x05, 0x06, 0x03, 0x2B, 0x65, 0x70};
        byte[] bitString = new byte[signature.length + 1];
        System.arraycopy(signature, 0, bitString, 1, signature.length);
        byte[] sig = tlv((byte) 0x03, bitString);
        return tlv((byte) 0x30, concat(tbs, algorithm, sig));
    }

    private static byte[] tlv(byte tag, byte[] content) {
        byte[] length;
        if (content.length < 0x80) {
            length = new byte[]{(byte) content.length};
        } else if (content.length < 0x100) {
            length = new byte[]{(byte) 0x81, (byte) content.length};
        } else {
            length = new byte[]{(byte) 0x82, (byte) (content.length >> 8), (byte) content.length};
        }
        byte[] out = new byte[1 + length.length + content.length];
        out[0] = tag;
        System.arraycopy(length, 0, out, 1, length.length);
        System.arraycopy(content, 0, out, 1 + length.length, content.length);
        return out;
    }

    private static byte[] concat(byte[]... parts) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.writeBytes(part);
        }
        return out.toByteArray();
    }
}
