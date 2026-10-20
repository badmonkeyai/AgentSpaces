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

import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code http} and {@code https} exchange (SRAD agentspaces-js JS-H-07,
 * appendix C, JS-Q-SEC-09): frames both ways in order, a repeated upload
 * delivered once, a gap refused, a held download answered 204, a lost batch
 * resent, the CORS preflight, and the per-source open rate.
 */
class HttpExchangeTransportTest {

    private static final Duration WAIT = Duration.ofSeconds(20);

    @Test
    @Timeout(120)
    void framesOfEverySizeArriveOnceAndInOrderOverHttpAndHttps() throws Exception {
        try (HttpExchangeTransport http = new HttpExchangeTransport(); Wire wire = Wire.of(http, http, "/aspace/h")) {
            exercise(wire);
        }
        try (TlsHttpExchangeTransport https = new TlsHttpExchangeTransport(PeerIdentity.generate());
             Wire wire = Wire.of(https, https, "/aspace/h")) {
            exercise(wire);
            assertThat(wire.dialed.attestedPeer()).isEmpty(); // session attestation proves the peers
        }
    }

    private static void exercise(Wire wire) throws Exception {
        Random random = new Random(11);
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
            wire.accepted.send(new byte[]{(byte) (i >> 8), (byte) i});
        }
        for (int i = 0; i < 1000; i++) {
            assertThat(Wire.await(wire.atListener, WAIT)).containsExactly((byte) (i >> 8), (byte) i);
            assertThat(Wire.await(wire.atDialer, WAIT)).containsExactly((byte) (i >> 8), (byte) i);
        }
    }

    @Test
    @Timeout(90)
    void theExchangeDeliversRepeatsOnceRefusesGapsAndResendsALostBatch() throws Exception {
        int port = Wire.freePort();
        CompletableFuture<TransportConnection> accepted = new CompletableFuture<>();
        try (HttpExchangeTransport http = new HttpExchangeTransport()
                .options(HttpOptions.defaults().withLongPoll(Duration.ofSeconds(1)))) {
            http.listen("127.0.0.1:" + port, accepted::complete);
            HttpClient client = HttpClient.newHttpClient();
            String base = "http://127.0.0.1:" + port + "/aspace/h";
            HttpResponse<Void> open = client.send(HttpRequest.newBuilder(URI.create(base + "/open"))
                    .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.discarding());
            assertThat(open.statusCode()).isEqualTo(201);
            URI session = URI.create(base).resolve(open.headers().firstValue("location").orElseThrow());
            TransportConnection connection = accepted.get(10, TimeUnit.SECONDS);
            java.util.concurrent.BlockingQueue<byte[]> received = new java.util.concurrent.LinkedBlockingQueue<>();
            connection.onReceive(received::add);

            byte[] body = FrameBatch.encode(List.of(new byte[]{9}));
            for (int i = 0; i < 2; i++) { // the same batch twice: delivered once
                assertThat(post(client, session, 1, body)).isEqualTo(204);
            }
            assertThat(post(client, session, 3, body)).isEqualTo(409); // a gap
            assertThat(Wire.await(received, WAIT)).containsExactly(9);
            assertThat(received.poll(300, TimeUnit.MILLISECONDS)).isNull();

            assertThat(get(client, session, 0).statusCode()).isEqualTo(204); // nothing to take: held, then 204
            connection.send(new byte[]{7, 7});
            HttpResponse<byte[]> first = get(client, session, 0);
            assertThat(first.statusCode()).isEqualTo(200);
            assertThat(first.headers().firstValue("aspace-seq")).contains("1");
            HttpResponse<byte[]> again = get(client, session, 0); // the answer was "lost": resent
            assertThat(again.headers().firstValue("aspace-seq")).contains("1");
            assertThat(FrameBatch.decode(again.body()).getFirst()).containsExactly(7, 7);
            assertThat(get(client, session, 5).statusCode()).isEqualTo(409);
            assertThat(client.send(HttpRequest.newBuilder(session).DELETE().build(),
                    HttpResponse.BodyHandlers.discarding()).statusCode()).isEqualTo(204);
            assertThat(get(client, session, 1).statusCode()).isEqualTo(404);
        }
    }

    @Test
    @Timeout(60)
    void thePreflightAnswersOnlyAllowedOriginsAndOpensAreRateLimited() throws Exception {
        int port = Wire.freePort();
        try (HttpExchangeTransport http = new HttpExchangeTransport().options(HttpOptions.defaults()
                .withOrigins(OriginPolicy.of(List.of("http://localhost:8080"))).withOpensPerSourcePerSecond(2))) {
            http.listen("127.0.0.1:" + port, c -> { });
            HttpClient client = HttpClient.newHttpClient();
            URI open = URI.create("http://127.0.0.1:" + port + "/aspace/h/open");
            HttpResponse<Void> preflight = client.send(HttpRequest.newBuilder(open)
                    .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
                    .header("Origin", "http://localhost:8080")
                    .header("Access-Control-Request-Method", "POST").build(), HttpResponse.BodyHandlers.discarding());
            assertThat(preflight.statusCode()).isEqualTo(204);
            assertThat(preflight.headers().firstValue("access-control-allow-origin")).contains("http://localhost:8080");
            assertThat(client.send(HttpRequest.newBuilder(open).POST(HttpRequest.BodyPublishers.noBody())
                    .header("Origin", "https://evil.example.com").build(),
                    HttpResponse.BodyHandlers.discarding()).statusCode()).isEqualTo(403);
            int created = 0;
            int limited = 0;
            for (int i = 0; i < 6; i++) {
                int status = client.send(HttpRequest.newBuilder(open).POST(HttpRequest.BodyPublishers.noBody())
                        .build(), HttpResponse.BodyHandlers.discarding()).statusCode();
                created += status == 201 ? 1 : 0;
                limited += status == 429 ? 1 : 0;
            }
            assertThat(created).isBetween(2, 4); // at most two a second, and the loop may cross a second
            assertThat(limited).isGreaterThan(0);
        }
    }

    private static int post(HttpClient client, URI session, long seq, byte[] body) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(session + "/up")).header("Aspace-Seq", Long.toString(seq))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build(),
                HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    private static HttpResponse<byte[]> get(HttpClient client, URI session, long ack) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(session + "/down")).header("Aspace-Ack", Long.toString(ack))
                .GET().build(), HttpResponse.BodyHandlers.ofByteArray());
    }
}
