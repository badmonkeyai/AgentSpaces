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
import ai.badmonkey.agentspaces.identity.ChannelCertificate;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.identity.ServingCredential;
import ai.badmonkey.agentspaces.identity.tls.AttestLaterTrustManager;
import ai.badmonkey.agentspaces.identity.tls.SingleKeyManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static javax.net.ssl.SSLEngineResult.HandshakeStatus.FINISHED;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_TASK;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_UNWRAP;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_UNWRAP_AGAIN;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_WRAP;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * An independent DTLS 1.2 implementation, the JDK's JSSE engine driven by
 * hand, completes a handshake with the BouncyCastle listener, attests through
 * the channel certificate, and exchanges recovery-sublayer PDUs with it
 * (SRAD DT-Q-INT-01). JSSE offers no Connection ID, so this also covers the
 * association without one.
 */
class DtlsInteropTest {

    @Test
    @Timeout(60)
    void jsseClientHandshakesAttestsAndExchangesFrames() throws Exception {
        PeerIdentity clientId = PeerIdentity.generate();
        ChannelCertificate channel = ChannelCertificate.generate(clientId);
        ServingCredential credential = new ServingCredential(channel.tlsKey(),
                new X509Certificate[]{channel.certificate()});

        try (DtlsTransport listener = new DtlsTransport(PeerIdentity.generate());
             DatagramSocket socket = new DatagramSocket()) {
            BlockingQueue<TransportConnection> accepted = new LinkedBlockingQueue<>();
            BlockingQueue<byte[]> frames = new LinkedBlockingQueue<>();
            DtlsTransport.Bound bound = listener.listenBound("127.0.0.1:0", c -> {
                c.onReceive(frames::add);
                accepted.add(c);
            });
            InetSocketAddress peer = new InetSocketAddress("127.0.0.1", bound.port());
            socket.setSoTimeout(1_000);

            SSLContext context = SSLContext.getInstance("DTLS");
            context.init(new KeyManager[]{new SingleKeyManager(credential)},
                    new TrustManager[]{new AttestLaterTrustManager()}, null);
            SSLEngine engine = context.createSSLEngine();
            engine.setUseClientMode(true);
            SSLParameters parameters = engine.getSSLParameters();
            parameters.setMaximumPacketSize(1_200);
            parameters.setProtocols(new String[]{"DTLSv1.2"});
            engine.setSSLParameters(parameters);

            handshake(engine, socket, peer);
            assertThat(engine.getSession().getProtocol()).isEqualTo("DTLSv1.2");
            assertThat(engine.getSession().getCipherSuite()).contains("GCM").contains("ECDHE_ECDSA");

            TransportConnection inbound = accepted.poll(10, TimeUnit.SECONDS);
            assertThat(inbound).isNotNull();
            assertThat(inbound.attestedPeer()).contains(clientId.peerId());

            // A frame from JSSE, framed by hand as one DATA PDU.
            byte[] frame = "hello from JSSE".getBytes();
            sendRecord(engine, socket, peer, Pdu.data(0, 0, 0, 1, null, frame, 0, frame.length));
            assertThat(frames.poll(10, TimeUnit.SECONDS)).isEqualTo(frame);

            // And one back: the listener's DATA PDU, decrypted by JSSE.
            inbound.send("hello from BouncyCastle".getBytes());
            Pdu.Data reply = null;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (reply == null && System.nanoTime() < deadline) {
                byte[] plaintext = receiveRecord(engine, socket);
                if (plaintext != null && Pdu.decode(plaintext, 0, plaintext.length) instanceof Pdu.Data data) {
                    reply = data;
                }
            }
            assertThat(reply).isNotNull();
            assertThat(new String(reply.payload())).isEqualTo("hello from BouncyCastle");
        }
    }

    private static void sendRecord(SSLEngine engine, DatagramSocket socket, InetSocketAddress peer, byte[] pdu)
            throws Exception {
        ByteBuffer out = ByteBuffer.allocate(engine.getSession().getPacketBufferSize());
        engine.wrap(ByteBuffer.wrap(pdu), out);
        out.flip();
        socket.send(new DatagramPacket(out.array(), out.limit(), peer));
    }

    private static byte[] receiveRecord(SSLEngine engine, DatagramSocket socket) throws Exception {
        byte[] buf = new byte[65_536];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        try {
            socket.receive(packet);
        } catch (SocketTimeoutException e) {
            return null;
        }
        ByteBuffer app = ByteBuffer.allocate(engine.getSession().getApplicationBufferSize());
        engine.unwrap(ByteBuffer.wrap(buf, 0, packet.getLength()), app);
        app.flip();
        if (!app.hasRemaining()) {
            return null;
        }
        byte[] plaintext = new byte[app.remaining()];
        app.get(plaintext);
        return plaintext;
    }

    /** The handshake loop of the JDK's DTLSOverDatagram sample, condensed. */
    private static void handshake(SSLEngine engine, DatagramSocket socket, InetSocketAddress peer) throws Exception {
        engine.beginHandshake();
        for (int loops = 0; loops < 500; loops++) {
            SSLEngineResult.HandshakeStatus status = engine.getHandshakeStatus();
            if (status == NOT_HANDSHAKING || status == FINISHED) {
                return;
            }
            if (status == NEED_UNWRAP || status == NEED_UNWRAP_AGAIN) {
                ByteBuffer in;
                if (status == NEED_UNWRAP) {
                    byte[] buf = new byte[65_536];
                    DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    try {
                        socket.receive(packet);
                    } catch (SocketTimeoutException e) {
                        for (DatagramPacket retransmit : wrapFlight(engine, peer)) {
                            socket.send(retransmit);
                        }
                        continue;
                    }
                    in = ByteBuffer.wrap(buf, 0, packet.getLength());
                } else {
                    in = ByteBuffer.allocate(0);
                }
                ByteBuffer app = ByteBuffer.allocate(engine.getSession().getApplicationBufferSize());
                SSLEngineResult result = engine.unwrap(in, app);
                if (result.getHandshakeStatus() == NEED_TASK) {
                    runTasks(engine);
                }
                if (result.getHandshakeStatus() == FINISHED) {
                    return;
                }
            } else if (status == NEED_WRAP) {
                for (DatagramPacket packet : wrapFlight(engine, peer)) {
                    socket.send(packet);
                }
            } else if (status == NEED_TASK) {
                runTasks(engine);
            }
        }
        throw new IllegalStateException("handshake did not finish");
    }

    private static List<DatagramPacket> wrapFlight(SSLEngine engine, InetSocketAddress peer) throws Exception {
        List<DatagramPacket> packets = new ArrayList<>();
        for (int loops = 0; loops < 50; loops++) {
            ByteBuffer out = ByteBuffer.allocate(32_768);
            SSLEngineResult result = engine.wrap(ByteBuffer.allocate(0), out);
            out.flip();
            if (out.hasRemaining()) {
                byte[] bytes = new byte[out.remaining()];
                out.get(bytes);
                packets.add(new DatagramPacket(bytes, bytes.length, peer));
            }
            SSLEngineResult.HandshakeStatus next = result.getHandshakeStatus();
            while (next == NEED_TASK) {
                runTasks(engine);
                next = engine.getHandshakeStatus();
            }
            if (next != NEED_WRAP) {
                return packets;
            }
        }
        return packets;
    }

    private static void runTasks(SSLEngine engine) {
        Runnable task;
        while ((task = engine.getDelegatedTask()) != null) {
            task.run();
        }
    }
}
