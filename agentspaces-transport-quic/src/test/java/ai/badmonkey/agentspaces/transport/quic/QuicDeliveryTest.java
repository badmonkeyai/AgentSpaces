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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.DatagramSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A receiver that answers every frame by sending, under load (srad/large-networks-bulk-data.md 10.4).
 * The node does this all the time: it forwards gossip, acks probes, and serves pieces from inside
 * frame handling. The binding hands frames to the receiver off its event loop, so an answer that
 * waits on a stream backed up by flow control waits on a thread that may wait, and every answer
 * arrives in order.
 */
class QuicDeliveryTest {

    private static final int FRAMES = 64;
    private static final int FRAME_BYTES = 1024 * 1024;

    @BeforeAll
    static void requireNative() {
        QuicNative.assumeAvailable();
    }

    private final QuicTransport server = new QuicTransport();
    private final QuicTransport client = new QuicTransport();

    @AfterEach
    void tearDown() {
        client.close();
        server.close();
    }

    @Test
    @Timeout(120)
    void aReceiverThatAnswersEveryFrameKeepsUpWhenBothDirectionsAreLoaded() throws Exception {
        int port;
        try (DatagramSocket socket = new DatagramSocket(0)) {
            port = socket.getLocalPort();
        }
        List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger echoed = new AtomicInteger();
        AutoCloseable listener = server.listen("127.0.0.1:" + port, connection -> connection.onReceive(frame -> {
            try {
                connection.send(frame); // answer from inside the receiver, as the node does
                echoed.incrementAndGet();
            } catch (IOException | RuntimeException e) {
                failures.add(e);
            }
        }));
        try {
            TransportConnection dialed = client.dial("127.0.0.1:" + port);
            CountDownLatch back = new CountDownLatch(FRAMES);
            List<Integer> order = Collections.synchronizedList(new ArrayList<>());
            dialed.onReceive(frame -> {
                order.add(frame[0] & 0xff);
                back.countDown();
            });
            for (int i = 0; i < FRAMES; i++) {
                byte[] frame = new byte[FRAME_BYTES];
                frame[0] = (byte) i;
                dialed.send(frame);
            }
            boolean all = back.await(90, TimeUnit.SECONDS);
            assertThat(failures).as("answers sent from inside the receiver").isEmpty();
            assertThat(all).as("every answer came back: echoed " + echoed.get() + ", received "
                    + (FRAMES - back.getCount())).isTrue();
            List<Integer> expected = new ArrayList<>();
            for (int i = 0; i < FRAMES; i++) {
                expected.add(i);
            }
            assertThat(order).as("in order").isEqualTo(expected);
            dialed.close();
        } finally {
            listener.close();
        }
    }
}
