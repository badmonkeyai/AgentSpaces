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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.api.spi.Transport;
import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.wire.Bodies;
import ai.badmonkey.agentspaces.peering.wire.Envelope;
import ai.badmonkey.agentspaces.peering.wire.WireCodec;
import ai.badmonkey.agentspaces.test.SimNetwork;

import java.io.IOException;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A dial-only peer as the thin clients are (srad/agentspaces-event-loop.md §3.12; the TypeScript client's
 * {@code peer.ts}): it dials one member, sends its signed advertisement once with no endpoint and its seed in
 * {@code relay.via}, answers every PING with an ACK, and sends nothing else. It never reissues its card, so a member
 * that drops it can only re-admit it from an advertisement issued after the drop, which never comes.
 */
public final class ThinSimPeer implements AutoCloseable {

    private static final CborCodec CODEC = CborCodec.defaultCodec();
    private static final WireCodec WIRE = new WireCodec(CODEC);

    private final PeerIdentity identity = PeerIdentity.generate();
    private final SimNetwork network;
    private final String address;
    private final GroupId group;
    private final InstantSource clock;
    private final AtomicInteger logical = new AtomicInteger();
    private final Map<PeerId, AtomicLong> pingsFrom = new ConcurrentHashMap<>();
    private final AtomicLong framesReceived = new AtomicLong();
    private volatile TransportConnection link;

    /**
     * Creates the peer; nothing is sent until {@link #dial}.
     *
     * @param network the fleet's network
     * @param address this peer's address on it, which it never listens on
     * @param group   the group
     * @param clock   the fleet's clock, which stamps its frames
     */
    public ThinSimPeer(SimNetwork network, String address, GroupId group, InstantSource clock) {
        this.network = network;
        this.address = address;
        this.group = group;
        this.clock = clock;
    }

    /** This peer's id. */
    public PeerId peerId() {
        return identity.peerId();
    }

    /**
     * Dials a member and introduces itself: its card names {@code holders} in {@code relay.via} and no endpoint.
     *
     * @param seedAddress the member's address
     * @param seed        the member's id, the frame's addressee
     * @param holders     the members it names as its holders (its seed, or a victim)
     * @throws IOException when the dial fails
     */
    public void dial(String seedAddress, PeerId seed, List<PeerId> holders) throws IOException {
        Transport transport = network.register(address);
        TransportConnection connection = transport.dial(seedAddress);
        connection.onReceive(this::onFrame);
        link = connection;
        PeerAdvertisement ad = new PeerAdvertisement("aspace://" + group.value() + "/peer/" + peerId().value(),
                peerId(), group, clock.instant(), Duration.ofMinutes(10), List.of(), Set.of(),
                Map.of(RelayPaths.VIA_HINT, RelayPaths.encode(holders)));
        byte[] adBytes = CODEC.toBytes(ad);
        byte[] payload = CODEC.toBytes(new PeerNode.SignedPeerAd(adBytes, identity.rawPublicKey(),
                identity.sign(adBytes)));
        send(seed, Envelope.Kind.RUMOR, new Bodies.Rumor("peers", "peer:" + peerId().value() + ":"
                + ad.issued().toEpochMilli(), 6, payload));
    }

    private void onFrame(byte[] frame) {
        framesReceived.incrementAndGet();
        Optional<Envelope> decoded;
        try {
            decoded = WIRE.decode(frame);
        } catch (RuntimeException e) {
            return;
        }
        if (decoded.isEmpty() || decoded.get().kind() != Envelope.Kind.PING) {
            return; // a thin client ignores what it does not speak
        }
        Envelope ping = decoded.get();
        pingsFrom.computeIfAbsent(ping.from(), p -> new AtomicLong()).incrementAndGet();
        Bodies.Ping body = CODEC.fromWire(ping.body(), Bodies.Ping.class);
        send(ping.from(), Envelope.Kind.ACK, new Bodies.Ack(body.nonce(), null, null, null, null));
    }

    private void send(PeerId to, Envelope.Kind kind, Object body) {
        TransportConnection connection = link;
        if (connection == null) {
            return;
        }
        try {
            connection.send(TestFrames.signed(identity, group, kind, to, clock, logical.incrementAndGet() % 60_000,
                    body));
        } catch (IOException e) {
            // the link is gone: a thin client sends nothing more
        }
    }

    /**
     * The PINGs this peer answered, by the member that sent them: its seed's own and the ones the seed relayed for
     * others; any other sender means a member PINGed it without a link, which the targeted relay never does.
     *
     * @return a copy
     */
    public Map<PeerId, Long> pingsFrom() {
        Map<PeerId, Long> out = new java.util.HashMap<>();
        pingsFrom.forEach((k, v) -> out.put(k, v.get()));
        return out;
    }

    /** Frames this peer received. */
    public long framesReceived() {
        return framesReceived.get();
    }

    @Override
    public void close() {
        TransportConnection connection = link;
        link = null;
        if (connection != null) {
            connection.close();
        }
    }
}
