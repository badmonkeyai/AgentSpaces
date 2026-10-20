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

import tel.schich.libdatachannel.DataChannel;
import tel.schich.libdatachannel.DataChannelCallback;
import tel.schich.libdatachannel.DataChannelInitSettings;
import tel.schich.libdatachannel.GatheringState;
import tel.schich.libdatachannel.IceState;
import tel.schich.libdatachannel.LibDataChannel;
import tel.schich.libdatachannel.PeerConnection;
import tel.schich.libdatachannel.PeerConnectionConfiguration;
import tel.schich.libdatachannel.PeerState;
import tel.schich.libdatachannel.SessionDescriptionType;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The first engine (SRAD srad/agentspaces-js.md decision 3): libdatachannel through
 * libdatachannel-java, whose native library this module's platform classifier carries. The
 * engine generates an ECDSA certificate per peer connection (libdatachannel's default) and
 * runs DTLS 1.2 with its AEAD ECDHE suites as the floor (JS-D-02); libdatachannel 0.24 offers no
 * DTLS 1.3, so 1.3 negotiates only once the engine does. Auto-negotiation is off: the binding
 * drives the offer and answer so both ends create the negotiated channel before either
 * description completes (appendix B).
 */
final class LibDataChannelEngine implements DataChannelEngine {

    static final String ARTIFACT = "tel.schich:libdatachannel-java";
    private static final System.Logger LOG = System.getLogger(LibDataChannelEngine.class.getName());

    private static volatile Optional<String> unavailable;

    static LibDataChannelEngine instance() {
        return Holder.INSTANCE;
    }

    private static final class Holder {
        static final LibDataChannelEngine INSTANCE = new LibDataChannelEngine();
    }

    private LibDataChannelEngine() {
    }

    @Override
    public String name() {
        return "libdatachannel-java";
    }

    @Override
    public Optional<String> unavailable() {
        Optional<String> known = unavailable;
        if (known != null) {
            return known;
        }
        synchronized (LibDataChannelEngine.class) {
            if (unavailable == null) {
                unavailable = probe();
            }
            return unavailable;
        }
    }

    private static Optional<String> probe() {
        try {
            LibDataChannel.initialize();
            return Optional.empty();
        } catch (LinkageError | RuntimeException e) {
            String platform = System.getProperty("os.name") + " " + System.getProperty("os.arch");
            return Optional.of("the webrtc engine " + ARTIFACT + " cannot load its native library on " + platform
                    + " (add the platform classifier of libdatachannel-java, or set"
                    + " -Dlibdatachannel.native.datachannel-java.path): " + e);
        }
    }

    @Override
    public Session open(WebRtcOptions options, Events events) throws IOException {
        Optional<String> missing = unavailable();
        if (missing.isPresent()) {
            throw new IOException(missing.get());
        }
        try {
            return new LibSession(options, events);
        } catch (RuntimeException | LinkageError e) {
            throw new IOException("cannot create a peer connection: " + e.getMessage(), e);
        }
    }

    /** libdatachannel takes ICE servers as URIs with the TURN credential in the authority. */
    static List<URI> iceUris(List<WebRtcOptions.IceServer> servers) {
        List<URI> out = new ArrayList<>();
        for (WebRtcOptions.IceServer server : servers) {
            for (String url : server.urls()) {
                int colon = url.indexOf(':');
                String scheme = url.substring(0, colon).toLowerCase(Locale.ROOT);
                String rest = url.substring(colon + 1);
                if (server.username() != null && (scheme.equals("turn") || scheme.equals("turns"))) {
                    rest = encode(server.username()) + ":" + encode(server.credential()) + "@" + rest;
                }
                out.add(URI.create(scheme + ":" + rest));
            }
        }
        return out;
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static final class LibSession implements Session {
        private final Events events;
        private final PeerConnection peer;
        private final DataChannel channel;
        private final ExecutorService callbacks;
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicBoolean opened = new AtomicBoolean();
        /** Local candidates gathered, {@code ip:port} to type, to name the selected one after open. */
        private final Map<String, String> localCandidates = new ConcurrentHashMap<>();

        LibSession(WebRtcOptions options, Events events) {
            this.events = events;
            // libdatachannel-java dispatches each listener through the executor given to createPeer,
            // and the binary-message ByteBuffer is a view over native memory valid only during that
            // synchronous callback. So the engine's executor is inline (Runnable::run, the library's
            // own thread): the message bytes are copied there and then, before the native frees them.
            // Everything past the copy runs on `worker`, one ordered thread per session, so the
            // binding's signalling, reassembly, and native getters never run on the callback thread
            // (which could deadlock the library) and the channel's frames stay in order.
            this.callbacks = Executors.newSingleThreadExecutor(Thread.ofVirtual().name("webrtc-session-", 0).factory());
            PeerConnectionConfiguration config = PeerConnectionConfiguration.DEFAULT
                    .withDisableAutoNegotiation(true)
                    .withCertificateType(PeerConnectionConfiguration.CertificateType.RTC_CERTIFICATE_ECDSA)
                    .withIceServers(iceUris(options.iceServers()));
            this.peer = PeerConnection.createPeer(config, Runnable::run);
            peer.onLocalDescription.register((p, sdp, type) ->
                    defer(() -> events.localDescription(sdp, type == SessionDescriptionType.ANSWER ? "answer" : "offer")));
            peer.onLocalCandidate.register((p, candidate, mid) -> {
                rememberCandidate(candidate);
                defer(() -> events.localCandidate(candidate, mid == null ? "" : mid));
            });
            peer.onGatheringStateChange.register((p, state) -> {
                if (state == GatheringState.RTC_GATHERING_COMPLETE) {
                    defer(events::gatheringComplete);
                }
            });
            peer.onIceStateChange.register((p, state) -> {
                if (state == IceState.RTC_ICE_FAILED) {
                    defer(() -> events.failed("ice failed"));
                }
            });
            peer.onStateChange.register((p, state) -> {
                if (state == PeerState.RTC_FAILED) {
                    defer(() -> events.failed("connection failed"));
                } else if (state == PeerState.RTC_CLOSED || state == PeerState.RTC_DISCONNECTED) {
                    defer(() -> events.closed(state == PeerState.RTC_CLOSED ? "closed" : "disconnected"));
                }
            });
            DataChannelInitSettings init = DataChannelInitSettings.DEFAULT.withNegotiated(true)
                    .withStream(WebRtcOptions.CHANNEL_ID);
            this.channel = peer.createDataChannel(WebRtcOptions.CHANNEL_LABEL, init);
            channel.onOpen.register(c -> {
                if (opened.compareAndSet(false, true)) {
                    defer(events::open);
                }
            });
            channel.onClosed.register(c -> defer(() -> events.closed("channel closed")));
            channel.onError.register((c, error) -> defer(() -> events.failed("channel error: " + error)));
            channel.onBufferedAmountLow.register(c -> defer(events::bufferedAmountLow));
            channel.onMessage.register(new DataChannelCallback.Message() {
                @Override
                public void onText(DataChannel c, String text) {
                    defer(events::textMessage);
                }

                @Override
                public void onBinary(DataChannel c, ByteBuffer buffer) {
                    // On the library's thread: copy the transient native buffer here and now.
                    byte[] copy = new byte[buffer.remaining()];
                    buffer.get(copy);
                    defer(() -> events.message(ByteBuffer.wrap(copy)));
                }
            });
        }

        /** Runs {@code action} on the session's worker thread, after the library's callback returns. */
        private void defer(Runnable action) {
            if (closed.get()) {
                return;
            }
            try {
                callbacks.execute(action);
            } catch (java.util.concurrent.RejectedExecutionException e) {
                // the session closed between the check and the submit; the event is moot
            }
        }

        private void rememberCandidate(String candidate) {
            // candidate:<foundation> <component> <proto> <priority> <ip> <port> typ <type> ...
            String[] parts = candidate.trim().split("\\s+");
            if (parts.length >= 8 && "typ".equals(parts[6])) {
                localCandidates.put(parts[4] + ":" + parts[5], parts[7].toLowerCase(Locale.ROOT));
            }
        }

        @Override
        public void offer() throws IOException {
            run(() -> peer.setLocalDescription("offer"));
        }

        @Override
        public void answer(String offerSdp) throws IOException {
            run(() -> {
                peer.setRemoteDescription(offerSdp, SessionDescriptionType.OFFER);
                peer.setLocalDescription("answer");
            });
        }

        @Override
        public void acceptAnswer(String answerSdp) throws IOException {
            run(() -> peer.setRemoteDescription(answerSdp, SessionDescriptionType.ANSWER));
        }

        @Override
        public void addCandidate(String candidate, String mid) throws IOException {
            run(() -> peer.addRemoteCandidate(candidate, mid == null || mid.isEmpty() ? null : mid));
        }

        @Override
        public void send(ByteBuffer message) throws IOException {
            ByteBuffer direct = message.isDirect() ? message : ByteBuffer.allocateDirect(message.remaining())
                    .put(message.duplicate()).flip();
            run(() -> channel.sendMessage(direct));
        }

        @Override
        public long bufferedAmount() {
            try {
                return channel.bufferedAmount();
            } catch (RuntimeException e) {
                return 0;
            }
        }

        @Override
        public void bufferedAmountLowThreshold(long bytes) {
            try {
                channel.bufferedAmountLowThreshold((int) Math.min(Integer.MAX_VALUE, bytes));
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.DEBUG, "cannot set the buffered-amount-low threshold: " + e);
            }
        }

        @Override
        public int remoteMaxMessageSize() {
            try {
                return peer.remoteMaxMessageSize();
            } catch (RuntimeException e) {
                return 0;
            }
        }

        @Override
        public Optional<String> selectedCandidateType() {
            try {
                InetSocketAddress local = peer.localAddress();
                if (local != null) {
                    String key = local.getHostString() + ":" + local.getPort();
                    String type = localCandidates.get(key);
                    if (type != null) {
                        return Optional.of(type);
                    }
                }
            } catch (RuntimeException e) {
                // the binding's address accessors are best effort (its candidate-pair call overflows for IPv6)
            }
            return Optional.empty();
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            try {
                peer.close();
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.DEBUG, "closing a peer connection: " + e);
            }
            callbacks.shutdown();
        }

        private interface Native {
            void run();
        }

        private static void run(Native action) throws IOException {
            try {
                action.run();
            } catch (RuntimeException e) {
                throw new IOException(e.getMessage() == null ? e.toString() : e.getMessage(), e);
            }
        }
    }
}
