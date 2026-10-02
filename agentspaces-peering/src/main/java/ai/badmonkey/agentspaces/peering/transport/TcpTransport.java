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
                Thread.ofVirtual().name("tcp-read-" + remoteAddress()).start(this::readLoop);
            }
        }

        private void readLoop() {
            try (DataInputStream in = new DataInputStream(socket.getInputStream())) {
                while (!socket.isClosed()) {
                    int length = in.readInt();
                    if (length < 0 || length > MAX_FRAME) {
                        return;
                    }
                    byte[] frame = new byte[length];
                    in.readFully(frame);
                    receiver.accept(frame);
                }
            } catch (EOFException | java.net.SocketException e) {
                // Peer closed; nothing to do (spec P2: absence is the signal).
            } catch (IOException e) {
                // Connection failed; membership will notice via probes.
            }
        }

        @Override
        public void close() {
            try {
                socket.close();
            } catch (IOException ignored) {
                // Closing is best-effort.
            }
        }
    }
}
