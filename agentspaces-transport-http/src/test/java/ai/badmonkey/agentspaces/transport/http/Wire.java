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

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/** A listener and a dialer of one binding wired to queues, for the transport tests. */
final class Wire implements AutoCloseable {

    final TransportConnection dialed;
    final TransportConnection accepted;
    final BlockingQueue<byte[]> atListener = new LinkedBlockingQueue<>();
    final BlockingQueue<byte[]> atDialer = new LinkedBlockingQueue<>();
    private final AutoCloseable listener;

    private Wire(TransportConnection dialed, TransportConnection accepted, AutoCloseable listener) {
        this.dialed = dialed;
        this.accepted = accepted;
        this.listener = listener;
        accepted.onReceive(atListener::add);
        dialed.onReceive(atDialer::add);
    }

    static Wire of(Transport listening, Transport dialing, String path) throws Exception {
        int port = freePort();
        CompletableFuture<TransportConnection> accepted = new CompletableFuture<>();
        AutoCloseable listener = listening.listen("127.0.0.1:" + port + path, accepted::complete);
        TransportConnection dialed = dialing.dial("127.0.0.1:" + port + path);
        return new Wire(dialed, accepted.get(10, TimeUnit.SECONDS), listener);
    }

    static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    static byte[] await(BlockingQueue<byte[]> queue, Duration timeout) throws InterruptedException {
        byte[] frame = queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (frame == null) {
            throw new AssertionError("no frame within " + timeout);
        }
        return frame;
    }

    @Override
    public void close() throws Exception {
        dialed.close();
        accepted.close();
        listener.close();
    }
}
