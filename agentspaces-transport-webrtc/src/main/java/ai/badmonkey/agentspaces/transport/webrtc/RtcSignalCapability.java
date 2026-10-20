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

package ai.badmonkey.agentspaces.transport.webrtc;

import ai.badmonkey.agentspaces.api.spi.TransportConnection;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.peering.wire.Envelope;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * The {@code aspace:cap/rtc-signal} capability (SRAD srad/agentspaces-js.md JS-N-02, JS-W-03,
 * JS-W-10): registers the pipe on its node, verifies and bounds every signal, and hands sessions
 * to the binding. Signals travel as {@code PIPE_DATA} frames the node signs and routes, directly
 * when the target is connected and through a RELAY-role member otherwise (SPEC §5.4), so this
 * class never sees an address. The bounds are per remote peer, per session, and per body, each a
 * property of {@link WebRtcOptions}; nothing caps members, sessions, or open connections. A
 * node without this module ignores the capability's frames as unknown pipe data.
 */
public final class RtcSignalCapability {

    /** The capability type URI. */
    public static final String CAPABILITY = RtcSignal.CAPABILITY;

    private static final System.Logger LOG = System.getLogger(RtcSignalCapability.class.getName());

    /**
     * Where the signals travel: the capability pipe multiplexer of the node's group
     * ({@code CapabilityPipes} in agentspaces-capabilities has exactly this shape), or
     * {@link #attach(GroupRuntime)} for a node without that module.
     */
    public interface SignalPipe {
        /** Registers the handler for one capability's frames. */
        void onCapability(String capabilityType, BiConsumer<PeerId, byte[]> handler);

        /** Sends one capability frame to a member. */
        void send(PeerId to, String capabilityType, byte[] payload);
    }

    /** The head of a {@code PIPE_DATA} body, as {@code CapabilityPipes} and the TypeScript core encode it. */
    private record PipeFrame(String capability, byte[] payload) {
    }

    private final WebRtcTransport transport;
    private final CborCodec codec = CborCodec.defaultCodec();
    private final SecureRandom random = new SecureRandom();
    private final Map<String, WebRtcSession> sessions = new ConcurrentHashMap<>();
    private final Map<PeerId, Deque<Long>> offerTimes = new ConcurrentHashMap<>();
    private final ScheduledExecutorService sweeper;
    private volatile SignalPipe pipe;
    private volatile Predicate<PeerId> members = peer -> true;

    RtcSignalCapability(WebRtcTransport transport) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.sweeper = Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().name("webrtc-sweeper").factory());
        sweeper.scheduleAtFixedRate(this::sweep, 1, 1, TimeUnit.SECONDS);
    }

    WebRtcOptions options() {
        return transport.options();
    }

    WebRtcCounters counters() {
        return transport.counters();
    }

    /**
     * Registers the capability on a pipe and sends through it. The members predicate stays as set
     * by {@link #members(Predicate)}; by default every verified sender counts, because the node
     * admits a frame's sender before any handler sees it (ASF-003).
     *
     * @param pipe the group's capability pipe
     * @return this capability
     */
    public RtcSignalCapability attach(SignalPipe pipe) {
        this.pipe = Objects.requireNonNull(pipe, "pipe");
        pipe.onCapability(CAPABILITY, this::onSignal);
        return this;
    }

    /**
     * Registers the capability straight on a group runtime, taking its {@code PIPE_DATA} handler:
     * for a node that runs no other capability pipe. A node with agentspaces-capabilities attaches
     * its {@code CapabilityPipes} through {@link #attach(SignalPipe)} instead, so the two never
     * compete for the one handler.
     *
     * @param runtime the group runtime
     * @return this capability
     */
    public RtcSignalCapability attach(GroupRuntime runtime) {
        Objects.requireNonNull(runtime, "runtime");
        members(peer -> runtime.membership().member(peer).isPresent());
        return attach(new SignalPipe() {
            @Override
            public void onCapability(String capabilityType, BiConsumer<PeerId, byte[]> handler) {
                runtime.onKind(Envelope.Kind.PIPE_DATA, (from, body) -> {
                    PipeFrame frame;
                    try {
                        frame = codec.fromBytes(body, PipeFrame.class);
                    } catch (RuntimeException e) {
                        return;
                    }
                    if (frame != null && capabilityType.equals(frame.capability())) {
                        handler.accept(from, frame.payload() == null ? new byte[0] : frame.payload());
                    }
                });
            }

            @Override
            public void send(PeerId to, String capabilityType, byte[] payload) {
                runtime.send(to, Envelope.Kind.PIPE_DATA, new PipeFrame(capabilityType, payload));
            }
        });
    }

    /** Who counts as a member: an {@code OFFER} from anyone else is ignored (JS-W-10). */
    public RtcSignalCapability members(Predicate<PeerId> members) {
        this.members = Objects.requireNonNull(members, "members");
        return this;
    }

    /** Whether a pipe is attached, so the binding can dial and answer. */
    public boolean attached() {
        return pipe != null;
    }

    /** Sessions pending or open, for the console and the tests. */
    public int sessions() {
        return sessions.size();
    }

    /**
     * One signal from a member, as the pipe delivers it (JS-W-10 bounds first, then by kind).
     *
     * @param from    the verified origin
     * @param payload the body
     */
    public void onSignal(PeerId from, byte[] payload) {
        WebRtcCounters counters = counters();
        if (payload == null || payload.length > options().maxSignalBytes()) {
            counters.dropped("size");
            return;
        }
        RtcSignal signal;
        try {
            signal = codec.fromBytes(payload, RtcSignal.class);
        } catch (RuntimeException e) {
            counters.dropped("malformed");
            return;
        }
        Optional<String> malformed = signal == null ? Optional.of("empty") : signal.malformed();
        if (malformed.isPresent()) {
            counters.dropped("malformed");
            LOG.log(System.Logger.Level.DEBUG, "malformed signal from " + from.display() + ": " + malformed.get());
            return;
        }
        if (RtcSignal.OFFER.equals(signal.kind())) {
            onOffer(from, signal);
            return;
        }
        WebRtcSession session = sessions.get(signal.sessionKey());
        if (session == null) {
            counters.dropped("unknown-session");
            return;
        }
        if (!session.remote().equals(from)) {
            counters.dropped("wrong-origin");
            return;
        }
        switch (signal.kind()) {
            case RtcSignal.ANSWER -> session.onAnswer(signal);
            case RtcSignal.CAND -> session.onCandidate(signal);
            case RtcSignal.END -> session.onEnd(signal.code());
            default -> counters.dropped("malformed");
        }
    }

    private void onOffer(PeerId from, RtcSignal offer) {
        WebRtcCounters counters = counters();
        if (!members.test(from)) {
            counters.dropped("non-member");
            return;
        }
        if (!admitOffer(from)) {
            counters.dropped("rate");
            return;
        }
        Consumer<TransportConnection> acceptor = transport.acceptor();
        if (acceptor == null) {
            counters.dropped("no-acceptor");
            send(from, RtcSignal.end(offer.sid(), RtcSignal.END_REFUSED));
            return;
        }
        if (sessions.containsKey(offer.sessionKey())) {
            counters.dropped("duplicate");
            return;
        }
        // Appendix B tie rule: when both ends offer at once, the lower PeerID keeps its offer and
        // the higher answers it.
        for (WebRtcSession pending : sessions.values()) {
            if (pending.offerer() && pending.remote().equals(from) && !pending.opened().isDone()) {
                if (transport.self().value().compareTo(from.value()) < 0) {
                    counters.dropped("tie");
                    send(from, RtcSignal.end(offer.sid(), RtcSignal.END_REFUSED));
                    return;
                }
                pending.end(RtcSignal.END_NORMAL, "tie: answering the lower PeerID's offer instead");
            }
        }
        WebRtcSession session = new WebRtcSession(this, offer.sid(), from, false);
        sessions.put(session.key(), session);
        try {
            session.startAnswer(transport.engine(), offer);
        } catch (IOException e) {
            LOG.log(System.Logger.Level.DEBUG, "offer from " + from.display() + " refused: " + e.getMessage());
        }
    }

    /** JS-W-10: at most {@code offersPerPeerPerMinute} offers from one peer in any sliding minute. */
    private boolean admitOffer(PeerId from) {
        long now = System.nanoTime();
        long window = TimeUnit.MINUTES.toNanos(1);
        Deque<Long> times = offerTimes.computeIfAbsent(from, p -> new ArrayDeque<>());
        synchronized (times) {
            while (!times.isEmpty() && now - times.peekFirst() > window) {
                times.pollFirst();
            }
            if (times.size() >= options().offersPerPeerPerMinute()) {
                return false;
            }
            times.addLast(now);
            return true;
        }
    }

    /** Opens a dial to {@code target}: the offerer session, pending until its channel opens. */
    WebRtcSession dial(PeerId target) throws IOException {
        if (pipe == null) {
            throw new IOException("the webrtc signalling capability is not attached to a group's pipes");
        }
        // A dial's own signals travel through the node, whose routing may reach for this very
        // binding again (the target's webrtc endpoint comes first): a second dial to a peer whose
        // session is still in the table fails at once, so the node falls through to the next
        // endpoint or the relay path for the signal instead of nesting dials (JS-W-09). An ended
        // session counts until it is forgotten: its END signal is sent after it is marked ended,
        // and that send is exactly the re-entry this guard exists for (a timed-out dial whose END
        // dialed again, fifteen seconds per level, without end).
        for (WebRtcSession pending : sessions.values()) {
            if (pending.remote().equals(target)) {
                throw new IOException("webrtc signalling with " + target.display() + " already in progress");
            }
        }
        byte[] sid = new byte[RtcSignal.SID_LENGTH];
        random.nextBytes(sid);
        WebRtcSession session = new WebRtcSession(this, sid, target, true);
        sessions.put(session.key(), session);
        session.startOffer(transport.engine());
        return session;
    }

    void send(PeerId to, RtcSignal signal) {
        SignalPipe current = pipe;
        if (current == null) {
            return;
        }
        try {
            current.send(to, CAPABILITY, codec.toBytes(signal));
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.DEBUG, "cannot send " + signal + " to " + to.display() + ": " + e);
        }
    }

    void opened(WebRtcSession session, WebRtcConnection connection) {
        if (!session.offerer()) {
            Consumer<TransportConnection> acceptor = transport.acceptor();
            if (acceptor == null) {
                connection.close();
                return;
            }
            acceptor.accept(connection);
        }
    }

    void forget(WebRtcSession session) {
        sessions.remove(session.key(), session);
    }

    /** Expires sessions that have not opened within the dial timeout (JS-W-10). */
    void sweep() {
        long now = System.nanoTime();
        for (WebRtcSession session : sessions.values()) {
            if (session.expired(now)) {
                session.expire();
            }
        }
        offerTimes.entrySet().removeIf(e -> {
            synchronized (e.getValue()) {
                return !e.getValue().isEmpty() && now - e.getValue().peekLast() > TimeUnit.MINUTES.toNanos(2);
            }
        });
    }

    void close() {
        sweeper.shutdownNow();
        for (WebRtcSession session : sessions.values()) {
            session.end(RtcSignal.END_NORMAL, "binding closed");
        }
        sessions.clear();
    }
}
