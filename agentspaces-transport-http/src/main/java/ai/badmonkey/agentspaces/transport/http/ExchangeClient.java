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

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * The dialing side of the HTTP exchange (appendix C) on the JDK's
 * {@code HttpClient}: one upload thread posts batches of up to 64 frames or
 * 1 MiB with rising {@code Aspace-Seq}, repeating a batch whose answer was
 * lost; one download thread long-polls with {@code Aspace-Ack}; a session
 * silent for the idle timeout ends. Over {@code https} the client presents no
 * certificate and attests nothing; session attestation (SPEC §5.6) proves the
 * peers to each other.
 */
final class ExchangeClient extends FrameConnection {

    private static final int UPLOAD_BYTES = 1024 * 1024;

    private final HttpClient client;
    private final URI session;
    private final HttpOptions options;
    private final LinkedBlockingQueue<byte[]> outbound = new LinkedBlockingQueue<>();
    private volatile boolean stopping;

    private ExchangeClient(HttpClient client, URI session, HttpOptions options, HttpCounters counters) {
        super(session.toString(), Optional.empty(), new X509Certificate[0], counters);
        this.client = client;
        this.session = session;
        this.options = options;
        start();
        Thread.ofVirtual().name("exchange-up").start(this::uploadLoop);
        Thread.ofVirtual().name("exchange-down").start(this::downloadLoop);
    }

    static ExchangeClient open(String base, boolean tls, HttpOptions options, HttpCounters counters)
            throws IOException {
        HttpClient.Builder builder = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(10))
                .followRedirects(HttpClient.Redirect.NEVER);
        if (tls) {
            try {
                builder.sslContext(TlsSetup.anonymousClient().context());
            } catch (GeneralSecurityException e) {
                throw new IOException("cannot prepare TLS for " + base, e);
            }
        }
        HttpClient client = builder.build();
        URI open = URI.create(base + "/open");
        HttpResponse<Void> response;
        try {
            response = client.send(HttpRequest.newBuilder(open).timeout(Duration.ofSeconds(10))
                    .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.discarding());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted opening " + open, e);
        }
        String location = response.headers().firstValue("location").orElse(null);
        if (response.statusCode() != 201 || location == null) {
            throw new IOException(open + " answered " + response.statusCode());
        }
        return new ExchangeClient(client, open.resolve(location), options, counters);
    }

    @Override
    public void send(byte[] frame) throws IOException {
        if (frame.length > HttpOptions.MAX_FRAME) {
            throw new IOException("frame exceeds " + HttpOptions.MAX_FRAME + " bytes: " + frame.length);
        }
        if (isClosed()) {
            throw new IOException("exchange session " + session + " is closed");
        }
        outbound.add(frame);
        counters.out(frame.length);
    }

    @Override
    public void close() {
        if (isClosed()) {
            return;
        }
        stopping = true;
        ended();
        try {
            client.sendAsync(HttpRequest.newBuilder(session).timeout(Duration.ofSeconds(5)).DELETE().build(),
                    HttpResponse.BodyHandlers.discarding());
        } catch (RuntimeException e) {
            // best effort
        }
    }

    private boolean alive(long lastSuccessNanos) {
        return !stopping && System.nanoTime() - lastSuccessNanos < options.idleTimeout().toNanos();
    }

    private void uploadLoop() {
        long seq = 0;
        long lastSuccess = System.nanoTime();
        try {
            while (!stopping) {
                byte[] first = outbound.poll(1, TimeUnit.SECONDS);
                if (first == null) {
                    continue;
                }
                List<byte[]> batch = new ArrayList<>();
                batch.add(first);
                long bytes = first.length;
                byte[] next;
                while (batch.size() < HttpOptions.MAX_BATCH_FRAMES && (next = outbound.peek()) != null
                        && bytes + next.length <= UPLOAD_BYTES) {
                    batch.add(outbound.poll());
                    bytes += next.length;
                }
                seq++;
                byte[] body = FrameBatch.encode(batch);
                while (true) {
                    try {
                        HttpResponse<Void> response = client.send(HttpRequest.newBuilder(URI.create(session + "/up"))
                                .timeout(Duration.ofSeconds(30))
                                .header("Content-Type", FrameBatch.MEDIA_TYPE)
                                .header(ExchangeBinding.SEQ, Long.toString(seq))
                                .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build(),
                                HttpResponse.BodyHandlers.discarding());
                        if (response.statusCode() == 204) {
                            lastSuccess = System.nanoTime();
                            break;
                        }
                        close(); // 404, 409, 410: the session is gone or out of step
                        return;
                    } catch (IOException e) {
                        if (!alive(lastSuccess)) {
                            close();
                            return;
                        }
                        Thread.sleep(500); // the same batch again, with the same sequence number
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void downloadLoop() {
        long ack = 0;
        long lastSuccess = System.nanoTime();
        try {
            while (!stopping) {
                try {
                    HttpResponse<byte[]> response = client.send(HttpRequest.newBuilder(URI.create(session + "/down"))
                            .timeout(options.longPoll().plusSeconds(15))
                            .header(ExchangeBinding.ACK, Long.toString(ack))
                            .GET().build(), HttpResponse.BodyHandlers.ofByteArray());
                    lastSuccess = System.nanoTime();
                    if (response.statusCode() == 204) {
                        continue;
                    }
                    if (response.statusCode() != 200) {
                        close();
                        return;
                    }
                    long seq = Long.parseLong(response.headers().firstValue(ExchangeBinding.SEQ).orElse("-1"));
                    if (seq == ack + 1) {
                        for (byte[] frame : FrameBatch.decode(response.body())) {
                            deliver(frame);
                        }
                        ack = seq;
                    }
                } catch (IOException | NumberFormatException e) {
                    if (!alive(lastSuccess)) {
                        close();
                        return;
                    }
                    Thread.sleep(500);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
