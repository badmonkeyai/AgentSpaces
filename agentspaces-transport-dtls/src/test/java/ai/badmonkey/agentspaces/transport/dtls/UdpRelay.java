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
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A UDP relay standing between a dialer and a listener, as a NAT does: the
 * dialer sends to the relay's front port, the relay forwards from its back
 * port. {@link #rebind()} moves the back port (a NAT rebinding, SRAD scenario
 * 4); {@link #loss} drops datagrams in both directions under a seed.
 */
final class UdpRelay implements AutoCloseable {

    private final DatagramSocket front;
    private final InetSocketAddress target;
    private final Random random;
    private volatile DatagramSocket back;
    private volatile InetSocketAddress client;
    volatile double loss;
    final AtomicLong forwarded = new AtomicLong();
    final AtomicLong dropped = new AtomicLong();

    UdpRelay(int targetPort, double loss, long seed) throws IOException {
        this.front = new DatagramSocket(new InetSocketAddress("127.0.0.1", 0));
        this.target = new InetSocketAddress("127.0.0.1", targetPort);
        this.loss = loss;
        this.random = new Random(seed);
        this.back = new DatagramSocket(new InetSocketAddress("127.0.0.1", 0));
        Thread.ofVirtual().start(this::frontLoop);
        startBackLoop(back);
    }

    String address() {
        return "127.0.0.1:" + front.getLocalPort();
    }

    int backPort() {
        return back.getLocalPort();
    }

    /** Moves the relay's outbound port, as a NAT rebinding does; the old port closes. */
    synchronized void rebind() throws IOException {
        DatagramSocket old = back;
        DatagramSocket fresh = new DatagramSocket(new InetSocketAddress("127.0.0.1", 0));
        back = fresh;
        startBackLoop(fresh);
        old.close();
    }

    private boolean drop() {
        synchronized (random) {
            return random.nextDouble() < loss;
        }
    }

    private void frontLoop() {
        byte[] buf = new byte[65_536];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        while (!front.isClosed()) {
            try {
                packet.setLength(buf.length);
                front.receive(packet);
                client = (InetSocketAddress) packet.getSocketAddress();
                if (drop()) {
                    dropped.incrementAndGet();
                    continue;
                }
                back.send(new DatagramPacket(buf, packet.getLength(), target));
                forwarded.incrementAndGet();
            } catch (IOException e) {
                if (front.isClosed()) {
                    return;
                }
            }
        }
    }

    private void startBackLoop(DatagramSocket socket) {
        Thread.ofVirtual().start(() -> {
            byte[] buf = new byte[65_536];
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            while (!socket.isClosed()) {
                try {
                    packet.setLength(buf.length);
                    socket.receive(packet);
                    if (client == null || drop()) {
                        dropped.incrementAndGet();
                        continue;
                    }
                    front.send(new DatagramPacket(buf, packet.getLength(), client));
                    forwarded.incrementAndGet();
                } catch (IOException e) {
                    return;
                }
            }
        });
    }

    @Override
    public void close() {
        front.close();
        back.close();
    }
}
