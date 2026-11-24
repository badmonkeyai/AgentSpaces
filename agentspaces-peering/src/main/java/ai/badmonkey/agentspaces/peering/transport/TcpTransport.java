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

import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.common.io.FramePump;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * The TCP reference transport (spec §5.5): length-prefixed frames over a socket,
 * with one virtual thread per connection reading inbound frames. Addresses are
 * {@code host:port}.
 */
public final class TcpTransport implements Transport {

    private static final int MAX_FRAME = 8 * 1024 * 1024;

    @Override
    public String scheme() {
        return "tcp";
    }

    @Override
    public TransportConnection dial(String address) throws IOException {
        InetSocketAddress target = parse(address);
        Socket socket = new Socket();
        socket.connect(target, 5_000);
        return new TcpConnection(socket);
    }

    @Override
    public AutoCloseable listen(String bindAddress, Consumer<TransportConnection> onAccept)
            throws IOException {
        Objects.requireNonNull(onAccept, "onAccept");
        InetSocketAddress bind = parse(bindAddress);
        ServerSocket server = new ServerSocket();
        server.bind(bind);
        Thread acceptor = Thread.ofVirtual().name("tcp-accept-" + bind.getPort()).start(() -> {
            while (!server.isClosed()) {
                try {
                    Socket socket = server.accept();
                    onAccept.accept(new TcpConnection(socket));
                } catch (IOException e) {
                    return; // closed
                }
            }
        });
        return () -> {
            server.close();
            acceptor.interrupt();
        };
    }

    /**
     * Reads one frame's body off a stream, telling {@code progress} after the length prefix and after each read that
     * leaves the frame short (SPEC v0.1.37 §9.4: bytes of a frame still arriving count as the far end's answer). A
     * read returns what the socket holds, so a frame trickling over a slow link tells it as each piece lands.
     *
     * @param in       the stream, its length prefix already read
     * @param frame    the body to fill, sized by the prefix
     * @param progress run on the reader's thread; must only note the time
     * @throws IOException when the stream ends or fails before the frame is whole
     */
    static void readFrame(DataInputStream in, byte[] frame, Runnable progress) throws IOException {
        if (frame.length > 0 && in.available() < frame.length) {
            progress.run(); // the prefix is here and the body is not, whole
        }
        int off = 0;
        while (off < frame.length) {
            int n = in.read(frame, off, frame.length - off);
            if (n < 0) {
                throw new EOFException("the stream ended " + (frame.length - off) + " bytes short of a frame");
            }
            off += n;
            if (off < frame.length) {
                progress.run();
            }
        }
    }

    /** Returns the local port a listener handle is bound to; helper for tests and demos. */
    public static InetSocketAddress parse(String address) {
        int colon = address.lastIndexOf(':');
        if (colon <= 0) {
            throw new IllegalArgumentException("expected host:port, got: " + address);
        }
        return new InetSocketAddress(address.substring(0, colon),
                Integer.parseInt(address.substring(colon + 1)));
    }

    private static final class TcpConnection implements TransportConnection {
        private final Socket socket;
        private final DataOutputStream out;
        private volatile Consumer<byte[]> receiver = frame -> {
        };
        private volatile boolean readerStarted;

        private TcpConnection(Socket socket) throws IOException {
            this.socket = socket;
            this.out = new DataOutputStream(socket.getOutputStream());
        }

        @Override
        public String remoteAddress() {
            return socket.getRemoteSocketAddress().toString();
        }

        @Override
        public synchronized void send(byte[] frame) throws IOException {
            if (frame.length > MAX_FRAME) {
                throw new IOException("frame exceeds " + MAX_FRAME + " bytes: " + frame.length);
            }
            out.writeInt(frame.length);
            out.write(frame);
            out.flush();
        }

        @Override
        public void onReceive(Consumer<byte[]> receiver) {
            this.receiver = Objects.requireNonNull(receiver, "receiver");
            if (!readerStarted) {
                readerStarted = true;
                FramePump pump = new FramePump("tcp-deliver-" + remoteAddress(), () -> this.receiver,
                        this::close, this::notifyClosed, FramePump.DEFAULT_CAP_BYTES);
                Thread.ofVirtual().name("tcp-read-" + remoteAddress()).start(() -> readLoop(pump));
            }
        }

        /** Reads frames into the pump, which delivers them in order on its own thread. */
        private void readLoop(FramePump pump) {
            try (DataInputStream in = new DataInputStream(socket.getInputStream())) {
                while (!socket.isClosed()) {
                    int length = in.readInt();
                    if (length < 0 || length > MAX_FRAME) {
                        return;
                    }
                    byte[] frame = new byte[length];
                    readFrame(in, frame, progressListener);
                    if (!pump.offer(frame)) {
                        return; // the receiver failed and the connection is closing
                    }
                }
            } catch (EOFException | java.net.SocketException e) {
                // Peer closed (spec P2: absence is the signal); the node is told once the pump drains.
            } catch (IOException e) {
                // Connection failed; the node is told once the pump drains and membership confirms by probe.
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                pump.end();
            }
        }

        private final java.util.concurrent.atomic.AtomicBoolean closeNotified =
                new java.util.concurrent.atomic.AtomicBoolean();
        private volatile Runnable closeListener = () -> {
        };

        @Override
        public void onClose(Runnable listener) {
            this.closeListener = Objects.requireNonNull(listener, "listener");
        }

        /** Bytes of a frame not yet whole (SPEC v0.1.37 §9.4): told as each piece of one lands. */
        private volatile Runnable progressListener = () -> {
        };

        @Override
        public void onProgress(Runnable listener) {
            this.progressListener = Objects.requireNonNull(listener, "listener");
        }

        private void notifyClosed() {
            if (closeNotified.compareAndSet(false, true)) {
                closeListener.run();
            }
        }

        @Override
        public void close() {
            try {
                socket.close();
            } catch (IOException ignored) {
                // Closing is best-effort.
            }
            notifyClosed();
        }
    }
}
