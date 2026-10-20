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
package ai.badmonkey.agentspaces.transport.http;

import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code ws} and {@code wss} bindings (SRAD agentspaces-js JS-H-01 to
 * JS-H-06, JS-Q-REL-01, JS-Q-SEC-01, JS-Q-INT-01): frames of 1 byte to 8 MiB
 * both ways and in order, the upgrade gate, the close codes, mutual
 * attestation over wss, and an independent client (the JDK's WebSocket).
 */
class WebSocketTransportTest {

    private static final Duration WAIT = Duration.ofSeconds(20);

    @Test
    @Timeout(120)
    void framesOfEverySizeArriveOnceAndInOrderBothWays() throws Exception {
        try (WebSocketTransport ws = new WebSocketTransport(); Wire wire = Wire.of(ws, ws, "/aspace")) {
            Random random = new Random(7);
            for (int size : new int[]{1, 64 * 1024, 2 * 1024 * 1024, HttpOptions.MAX_FRAME}) {
                byte[] frame = new byte[size];
                random.nextBytes(frame);
                wire.dialed.send(frame);
                assertThat(Wire.await(wire.atListener, WAIT)).isEqualTo(frame);
                wire.accepted.send(frame);
                assertThat(Wire.await(wire.atDialer, WAIT)).isEqualTo(frame);
            }
            for (int i = 0; i < 1000; i++) {
                wire.dialed.send(new byte[]{(byte) (i >> 8), (byte) i});
            }
            for (int i = 0; i < 1000; i++) {
                assertThat(Wire.await(wire.atListener, WAIT)).containsExactly((byte) (i >> 8), (byte) i);
            }
            assertThat(wire.dialed.attestedPeer()).isEmpty();
            assertThat(ws.counters().upgradesAccepted()).isEqualTo(1);
        }
    }

    @Test
    @Timeout(60)
    void anOversizeFrameIsRefusedAndACloseFromEitherEndIsSeen() throws Exception {
        try (WebSocketTransport ws = new WebSocketTransport(); Wire wire = Wire.of(ws, ws, "/aspace")) {
            assertThatThrownBy(() -> wire.dialed.send(new byte[HttpOptions.MAX_FRAME + 1]))
                    .isInstanceOf(IOException.class).hasMessageContaining("exceeds");
            CompletableFuture<Void> closed = new CompletableFuture<>();
            wire.accepted.onClose(() -> closed.complete(null));
            wire.dialed.close();
            closed.get(10, TimeUnit.SECONDS);
            assertThatThrownBy(() -> wire.dialed.send(new byte[]{1})).isInstanceOf(IOException.class);
        }
    }

    @Test
    @Timeout(60)
    void wssAttestsBothJvmEndsByTheirChannelCertificates() throws Exception {
        PeerIdentity listener = PeerIdentity.generate();
        PeerIdentity dialer = PeerIdentity.generate();
        try (TlsWebSocketTransport serving = new TlsWebSocketTransport(listener);
             TlsWebSocketTransport dialing = new TlsWebSocketTransport(dialer);
             Wire wire = Wire.of(serving, dialing, "/aspace")) {
            assertThat(wire.dialed.attestedPeer()).contains(listener.peerId());
            assertThat(wire.accepted.attestedPeer()).contains(dialer.peerId());
            assertThat(wire.accepted.remoteChain()).hasSize(1);
            byte[] frame = "over tls 1.3".getBytes();
            wire.dialed.send(frame);
            assertThat(Wire.await(wire.atListener, WAIT)).isEqualTo(frame);
            assertThat(serving.capabilities()).contains(Transport.Capability.ATTESTING,
                    Transport.Capability.ENCRYPTED);
        }
    }

    @Test
    @Timeout(60)
    void theJdkWebSocketClientInteroperatesWithTheListener() throws Exception {
        int port = Wire.freePort();
        CompletableFuture<TransportConnection> accepted = new CompletableFuture<>();
        try (WebSocketTransport ws = new WebSocketTransport()) {
            ws.listen("127.0.0.1:" + port, accepted::complete);
            CompletableFuture<byte[]> echoed = new CompletableFuture<>();
            WebSocket client = HttpClient.newHttpClient().newWebSocketBuilder()
                    .subprotocols(WebSocketBinding.SUBPROTOCOL)
                    .buildAsync(URI.create("ws://127.0.0.1:" + port + "/aspace"), new WebSocket.Listener() {
                        @Override
                        public CompletionStage<?> onBinary(WebSocket socket, ByteBuffer data, boolean last) {
                            byte[] bytes = new byte[data.remaining()];
                            data.get(bytes);
                            echoed.complete(bytes);
                            return null;
                        }
                    }).get(10, TimeUnit.SECONDS);
            assertThat(client.getSubprotocol()).isEqualTo("aspace.1");
            TransportConnection connection = accepted.get(10, TimeUnit.SECONDS);
            CompletableFuture<byte[]> received = new CompletableFuture<>();
            connection.onReceive(received::complete);
            client.sendBinary(ByteBuffer.wrap(new byte[]{1, 2, 3}), true).get(10, TimeUnit.SECONDS);
            assertThat(received.get(10, TimeUnit.SECONDS)).containsExactly(1, 2, 3);
            connection.send(new byte[]{4, 5});
            assertThat(echoed.get(10, TimeUnit.SECONDS)).containsExactly(4, 5);
            // A text message is not a frame: the listener closes with 1003 (SR-7).
            CompletableFuture<Void> closed = new CompletableFuture<>();
            connection.onClose(() -> closed.complete(null));
            client.sendText("not a frame", true);
            closed.get(10, TimeUnit.SECONDS);
            assertThat(ws.counters().closes()).containsKey(1003);
        }
    }

    @Test
    @Timeout(60)
    void theUpgradeGateRefusesWrongPathsOriginsAndSubprotocols() throws Exception {
        int port = Wire.freePort();
        AtomicInteger accepted = new AtomicInteger();
        try (WebSocketTransport ws = new WebSocketTransport()
                .options(HttpOptions.defaults().withOrigins(OriginPolicy.of(List.of("https://app.example.com"))))) {
            ws.listen("127.0.0.1:" + port, c -> accepted.incrementAndGet());
            HttpClient http = HttpClient.newHttpClient();
            String base = "http://127.0.0.1:" + port;
            assertThat(http.send(HttpRequest.newBuilder(URI.create(base + "/elsewhere")).build(),
                    HttpResponse.BodyHandlers.discarding()).statusCode()).isEqualTo(404);
            assertThat(http.send(HttpRequest.newBuilder(URI.create(base + "/aspace")).build(),
                    HttpResponse.BodyHandlers.discarding()).statusCode()).isEqualTo(426);
            assertThatThrownBy(() -> http.newWebSocketBuilder().header("Origin", "https://evil.example.com")
                    .subprotocols("aspace.1").buildAsync(URI.create("ws://127.0.0.1:" + port + "/aspace"),
                            new WebSocket.Listener() { }).get(10, TimeUnit.SECONDS))
                    .hasStackTraceContaining("403");
            assertThatThrownBy(() -> http.newWebSocketBuilder().buildAsync(
                    URI.create("ws://127.0.0.1:" + port + "/aspace"), new WebSocket.Listener() { })
                    .get(10, TimeUnit.SECONDS)).hasStackTraceContaining("400");
            WebSocket allowed = http.newWebSocketBuilder().header("Origin", "https://app.example.com")
                    .subprotocols("aspace.1").buildAsync(URI.create("ws://127.0.0.1:" + port + "/aspace"),
                            new WebSocket.Listener() { }).get(10, TimeUnit.SECONDS);
            assertThat(allowed.getSubprotocol()).isEqualTo("aspace.1");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (accepted.get() < 1 && System.nanoTime() < deadline) {
                Thread.sleep(50);
            }
            assertThat(accepted.get()).isEqualTo(1);
            assertThat(ws.counters().refused()).containsKeys("origin", "subprotocol", "not-upgrade");
        }
    }

    @Test
    @Timeout(60)
    void wsAndHttpShareOnePortAndWssRefusesAPlaintextNeighbour() throws Exception {
        int port = Wire.freePort();
        try (WebSocketTransport ws = new WebSocketTransport(); HttpExchangeTransport http = new HttpExchangeTransport();
             TlsWebSocketTransport wss = new TlsWebSocketTransport(PeerIdentity.generate())) {
            ws.listen("127.0.0.1:" + port, c -> { });
            CompletableFuture<TransportConnection> exchange = new CompletableFuture<>();
            http.listen("127.0.0.1:" + port, exchange::complete);
            TransportConnection dialed = http.dial("127.0.0.1:" + port);
            assertThat(exchange.get(10, TimeUnit.SECONDS)).isNotNull();
            dialed.close();
            assertThatThrownBy(() -> wss.listen("127.0.0.1:" + port, c -> { }))
                    .isInstanceOf(IOException.class).hasMessageContaining("without TLS");
        }
    }
}
