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

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.PortUnreachableException;
import java.net.SocketTimeoutException;

/**
 * A dialer's socket and its read loop, shared by the {@code dtls} and {@code udp} bindings.
 *
 * <p>The socket is connected to the peer while the handshake runs, and only then. A host
 * answers a datagram for a port nobody holds with an ICMP port unreachable, and only a
 * connected socket hears it: the read loop then closes the path, so the dial fails at once,
 * as a TCP dial to a closed port is refused at once. Before this, a dial to a member that had
 * left or crashed waited out the whole dial timeout (10 s), and the thread that sent to it
 * (a node's tick, a gossip forward, a content answer) stalled for that long on every send.
 *
 * <p>Once the association is up the read loop disconnects the socket, so the dialer keeps its
 * association when its own address changes (SRAD DT-K-05): an unconnected socket sends from
 * the host's current address. While connected the loop wakes every
 * {@value #CONNECTED_POLL_MILLIS} ms, so the disconnect never waits on a datagram; the JDK
 * keeps the bound address and port across the disconnect.
 */
final class DialSocket {

    /** How often the read loop wakes while connected, to disconnect soon after the handshake. */
    static final int CONNECTED_POLL_MILLIS = 50;

    private final DatagramSocket socket;
    private volatile boolean established;
    private volatile boolean refused;

    private DialSocket(DatagramSocket socket) {
        this.socket = socket;
    }

    /** Opens a dialer's socket toward a peer, connected for the handshake. */
    static DialSocket open(InetSocketAddress remote) throws IOException {
        DatagramSocket socket = BcSupport.dialSocket(remote);
        try {
            socket.connect(remote);
            socket.setSoTimeout(CONNECTED_POLL_MILLIS);
        } catch (IOException | RuntimeException e) {
            socket.close();
            throw e instanceof IOException io ? io : new IOException("cannot open a socket toward " + remote, e);
        }
        return new DialSocket(socket);
    }

    /** Sends one datagram; while the socket is connected the path's address is the peer's. */
    void send(byte[] buf, int off, int len, InetSocketAddress to) throws IOException {
        socket.send(new DatagramPacket(buf, off, len, to));
    }

    void close() {
        socket.close();
    }

    /** The handshake finished: the read loop disconnects the socket at its next wake (DT-K-05). */
    void established() {
        established = true;
    }

    /** Whether the peer's host answered that nobody holds the port, which ended the dial. */
    boolean refused() {
        return refused;
    }

    /** Starts the read loop, which hands the peer's datagrams to the path. */
    void start(PeerDatagramTransport path, DtlsCounters counters, String name) {
        Thread.ofVirtual().name(name).start(() -> readLoop(path, counters));
    }

    private void readLoop(PeerDatagramTransport path, DtlsCounters counters) {
        byte[] buf = new byte[PeerDatagramTransport.RECEIVE_LIMIT];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        boolean disconnected = false;
        while (!socket.isClosed()) {
            if (established && !disconnected) {
                disconnected = true;
                try {
                    socket.disconnect();
                    socket.setSoTimeout(0);
                } catch (IOException | java.io.UncheckedIOException e) {
                    // Still connected to the peer, which keeps working; only a change of this host's address would
                    // strand the association now, as it would without Connection ID.
                }
            }
            try {
                packet.setLength(buf.length);
                socket.receive(packet);
            } catch (SocketTimeoutException e) {
                continue;
            } catch (PortUnreachableException e) {
                if (!established) {
                    // Nobody holds the peer's port: end the dial now instead of at its timeout.
                    refused = true;
                    path.close();
                    return;
                }
                continue; // the association is up; its own recovery and idle timer judge the peer
            } catch (IOException e) {
                return; // closed
            }
            InetSocketAddress source = (InetSocketAddress) packet.getSocketAddress();
            if (!source.equals(path.remote())) {
                counters.datagramsDropped.increment();
                continue;
            }
            byte[] bytes = java.util.Arrays.copyOf(buf, packet.getLength());
            if (!path.offer(new PeerDatagramTransport.Datagram(bytes, source))) {
                counters.datagramsDropped.increment();
            }
        }
    }
}
