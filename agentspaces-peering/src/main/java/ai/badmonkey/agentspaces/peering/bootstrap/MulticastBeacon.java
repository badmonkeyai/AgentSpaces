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
package ai.badmonkey.agentspaces.peering.bootstrap;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The opt-in LAN bootstrap beacon (spec §10.1 {@code bootstrap: multicast},
 * roadmap M1). A node with the beacon enabled periodically sends, for each
 * group it has joined, one datagram whose payload is <em>exactly</em> the
 * signed self-advertisement it publishes on that group's {@code peers} gossip
 * stream (the CBOR of {@code PeerNode.SignedPeerAd}: the advertisement's
 * canonical bytes, the issuer's raw Ed25519 key, and the signature over the
 * bytes). Nothing else travels: no envelope, no new frame kind, no seed list
 * beyond the endpoints the advertisement itself carries.
 *
 * <p>A receiver treats a datagram precisely as it treats a bootstrap
 * {@code RUMOR} on the {@code peers} stream from a stranger: it spends one
 * token of the shared bootstrap budget, verifies the advertisement end to end
 * (key hashes to issuer, signature valid, not expired, not far-future), applies
 * the group's admission policy, and only then learns the sender's endpoints and
 * introduces itself to them as it would to a configured seed. A datagram that
 * fails any step is dropped without side effects; one larger than
 * {@link #MAX_DATAGRAM} is dropped unread.
 *
 * <p>The beacon is written against the small {@link Datagrams} carrier so the
 * discovery-to-admission path is testable in memory; {@link #multicast} is the
 * real UDP multicast carrier.
 */
public final class MulticastBeacon implements AutoCloseable {

    /**
     * Largest datagram the beacon sends or reads: the UDP maximum payload over
     * IPv4 (65,535 minus the 8-byte UDP and 20-byte IP headers). Far below the
     * 8 MiB frame cap of the stream transports, since a self-advertisement is
     * a few hundred bytes.
     */
    public static final int MAX_DATAGRAM = 65_507;

    /**
     * A datagram carrier: a connectionless, unreliable, best-effort way to
     * broadcast bytes to whoever listens and to hear what others broadcast.
     * {@link #multicast} is the UDP implementation; tests substitute an
     * in-memory one.
     */
    public interface Datagrams extends AutoCloseable {

        /**
         * Broadcasts one datagram.
         *
         * @param datagram the bytes; at most {@link #MAX_DATAGRAM}
         * @throws IOException when the carrier cannot send
         */
        void send(byte[] datagram) throws IOException;

        /**
         * Registers the receiver of inbound datagrams; one receiver per
         * carrier, registering replaces the previous one.
         *
         * @param receiver invoked with each datagram's bytes
         */
        void onReceive(Consumer<byte[]> receiver);

        /** Stops the carrier. Idempotent. */
        @Override
        void close();
    }

    private final Datagrams carrier;
    private final Supplier<List<byte[]>> announcements;

    /**
     * Creates a beacon.
     *
     * @param carrier       the datagram carrier
     * @param announcements supplies the datagrams to send each round (one
     *                      signed self-advertisement per joined group)
     * @param inbound       receives verified-by-caller inbound datagrams; the
     *                      beacon delivers raw bytes and the node verifies
     */
    public MulticastBeacon(Datagrams carrier, Supplier<List<byte[]>> announcements,
                           Consumer<byte[]> inbound) {
        this.carrier = Objects.requireNonNull(carrier, "carrier");
        this.announcements = Objects.requireNonNull(announcements, "announcements");
        Objects.requireNonNull(inbound, "inbound");
        carrier.onReceive(datagram -> {
            if (datagram != null && datagram.length <= MAX_DATAGRAM) {
                inbound.accept(datagram);
            }
        });
    }

    /**
     * Sends one announcement round: every supplied datagram, best effort.
     * Oversized datagrams are skipped, and send failures are ignored (the
     * next round retries; the beacon is only ever a discovery hint).
     */
    public void announce() {
        for (byte[] datagram : announcements.get()) {
            if (datagram.length > MAX_DATAGRAM) {
                continue;
            }
            try {
                carrier.send(datagram);
            } catch (IOException e) {
                // Best effort: multicast is a hint, not a channel.
            }
        }
    }

    @Override
    public void close() {
        carrier.close();
    }

    /**
     * Opens the UDP multicast carrier on a group address, e.g.
     * {@code 239.255.42.99:7787}, joined on every multicast-capable interface
     * (falling back to the default interface). Datagrams are sent with TTL 1
     * so they never leave the local network.
     *
     * @param group the multicast group address and port
     * @return the carrier
     * @throws IOException when the socket cannot be opened or the group joined
     */
    public static Datagrams multicast(InetSocketAddress group) throws IOException {
        return new UdpMulticast(Objects.requireNonNull(group, "group"));
    }

    /** UDP multicast over {@link MulticastSocket}, receiving on a virtual thread. */
    private static final class UdpMulticast implements Datagrams {
        private final InetSocketAddress group;
        private final MulticastSocket socket;
        private volatile Consumer<byte[]> receiver = datagram -> {
        };
        private volatile boolean readerStarted;

        private UdpMulticast(InetSocketAddress group) throws IOException {
            this.group = group;
            this.socket = new MulticastSocket(group.getPort());
            socket.setTimeToLive(1);
            socket.setReuseAddress(true);
            boolean joined = false;
            try {
                for (NetworkInterface nic : List.copyOf(
                        java.util.Collections.list(NetworkInterface.getNetworkInterfaces()))) {
                    if (!nic.isUp() || !nic.supportsMulticast()) {
                        continue;
                    }
                    try {
                        socket.joinGroup(group, nic);
                        joined = true;
                    } catch (IOException e) {
                        // Interface cannot join this group; try the rest.
                    }
                }
            } catch (SocketException e) {
                // Interface enumeration failed; fall through to the default.
            }
            if (!joined) {
                socket.joinGroup(group, null);
            }
        }

        @Override
        public void send(byte[] datagram) throws IOException {
            if (datagram.length > MAX_DATAGRAM) {
                throw new IOException("datagram exceeds " + MAX_DATAGRAM + " bytes");
            }
            socket.send(new DatagramPacket(datagram, datagram.length, group));
        }

        @Override
        public synchronized void onReceive(Consumer<byte[]> receiver) {
            this.receiver = Objects.requireNonNull(receiver, "receiver");
            if (!readerStarted) {
                readerStarted = true;
                Thread.ofVirtual().name("multicast-beacon-" + group.getPort())
                        .start(this::readLoop);
            }
        }

        private void readLoop() {
            byte[] buffer = new byte[MAX_DATAGRAM];
            while (!socket.isClosed()) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                try {
                    socket.receive(packet);
                } catch (IOException e) {
                    return; // closed
                }
                byte[] datagram = java.util.Arrays.copyOfRange(packet.getData(),
                        packet.getOffset(), packet.getOffset() + packet.getLength());
                receiver.accept(datagram);
            }
        }

        @Override
        public void close() {
            socket.close();
        }

        /** The address datagrams go to, for diagnostics. */
        InetAddress groupAddress() {
            return group.getAddress();
        }
    }
}
