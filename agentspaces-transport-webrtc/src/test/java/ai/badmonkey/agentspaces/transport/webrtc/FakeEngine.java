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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An in-memory engine behind the {@link DataChannelEngine} seam: sessions pair through a token in
 * their session descriptions, each presents a random "certificate" fingerprint in its SDP, the
 * offerer's {@code acceptAnswer} opens both ends, and messages cross directly. Knobs stall sends
 * (for backpressure), keep a session from opening (for the dial timeout), and fail the engine
 * (for the unavailable path). Events arrive on a thread per session, in order, as with a real
 * engine.
 */
final class FakeEngine implements DataChannelEngine {

    private static final SecureRandom RANDOM = new SecureRandom();

    final Map<String, FakeSession> byToken = new ConcurrentHashMap<>();
    final List<FakeSession> sessions = new CopyOnWriteArrayList<>();
    final List<String> channelsCreated = new CopyOnWriteArrayList<>();
    volatile boolean neverOpen;
    volatile boolean stallSends;
    volatile int remoteMaxMessageSize = 65536;
    volatile Optional<String> unavailable = Optional.empty();

    @Override
    public String name() {
        return "fake";
    }

    @Override
    public Optional<String> unavailable() {
        return unavailable;
    }

    @Override
    public Session open(WebRtcOptions options, Events events) throws IOException {
        if (unavailable.isPresent()) {
            throw new IOException(unavailable.get());
        }
        FakeSession session = new FakeSession(this, events);
        sessions.add(session);
        byToken.put(session.token, session);
        channelsCreated.add(WebRtcOptions.CHANNEL_LABEL + "#" + WebRtcOptions.CHANNEL_ID);
        return session;
    }

    static String randomFingerprint() {
        byte[] digest = new byte[32];
        RANDOM.nextBytes(digest);
        return "sha-256 " + HexFormat.ofDelimiter(":").withUpperCase().formatHex(digest);
    }

    static String tokenOf(String sdp) {
        for (String line : sdp.split("\r?\n")) {
            if (line.startsWith("a=fake-token:")) {
                return line.substring("a=fake-token:".length()).trim();
            }
        }
        return null;
    }

    /** Releases every stalled send on every session, as the real channel drains. */
    void drainAll() {
        stallSends = false;
        for (FakeSession session : sessions) {
            session.drain();
        }
    }

    /** Posts to a session thread; a session closed meanwhile simply drops the event, as an engine would. */
    static void post(ExecutorService thread, Runnable action) {
        try {
            thread.execute(action);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // the session closed between the check and the post
        }
    }

    final class FakeSession implements Session {
        final String token = HexFormat.of().formatHex(RANDOM.generateSeed(8));
        final String fingerprint = randomFingerprint();
        final Events events;
        final ExecutorService thread = Executors.newSingleThreadExecutor(Thread.ofVirtual().factory());
        final AtomicInteger candidatesAdded = new AtomicInteger();
        final AtomicLong buffered = new AtomicLong();
        final List<ByteBuffer> stalled = new CopyOnWriteArrayList<>();
        volatile FakeSession remote;
        volatile long lowThreshold;
        volatile boolean closed;
        volatile boolean remoteDescribed;

        FakeSession(FakeEngine engine, Events events) {
            this.events = events;
        }

        String sdp(String type) {
            return "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\na=fingerprint:" + fingerprint
                    + "\r\na=max-message-size:" + remoteMaxMessageSize + "\r\na=fake-token:" + token
                    + "\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\na=mid:0\r\na=setup:"
                    + ("offer".equals(type) ? "actpass" : "active") + "\r\n";
        }

        private void gather() {
            post(thread, () -> {
                events.localCandidate("candidate:1 1 UDP 2122260223 192.0.2.10 40000 typ host", "0");
                events.gatheringComplete();
            });
        }

        @Override
        public void offer() {
            String sdp = sdp("offer");
            post(thread, () -> events.localDescription(sdp, "offer"));
            gather();
        }

        @Override
        public void answer(String offerSdp) throws IOException {
            FakeSession partner = byToken.get(String.valueOf(tokenOf(offerSdp)));
            if (partner == null) {
                throw new IOException("unknown offer");
            }
            remote = partner;
            remoteDescribed = true;
            String sdp = sdp("answer");
            post(thread, () -> events.localDescription(sdp, "answer"));
            gather();
        }

        @Override
        public void acceptAnswer(String answerSdp) throws IOException {
            FakeSession partner = byToken.get(String.valueOf(tokenOf(answerSdp)));
            if (partner == null) {
                throw new IOException("unknown answer");
            }
            remote = partner;
            remoteDescribed = true;
            if (!neverOpen) {
                post(thread, events::open);
                post(partner.thread, partner.events::open);
            }
        }

        @Override
        public void addCandidate(String candidate, String mid) throws IOException {
            if (!remoteDescribed) {
                throw new IOException("no remote description yet");
            }
            candidatesAdded.incrementAndGet();
        }

        @Override
        public void send(ByteBuffer message) throws IOException {
            FakeSession target = remote;
            if (closed || target == null) {
                throw new IOException("not open");
            }
            byte[] copy = new byte[message.remaining()];
            message.duplicate().get(copy);
            if (stallSends) {
                buffered.addAndGet(copy.length);
                stalled.add(ByteBuffer.wrap(copy));
                return;
            }
            post(target.thread, () -> target.events.message(ByteBuffer.wrap(copy)));
        }

        void drain() {
            FakeSession target = remote;
            for (ByteBuffer message : stalled) {
                if (target != null) {
                    post(target.thread, () -> target.events.message(message));
                }
            }
            stalled.clear();
            buffered.set(0);
            post(thread, events::bufferedAmountLow);
        }

        @Override
        public long bufferedAmount() {
            return buffered.get();
        }

        @Override
        public void bufferedAmountLowThreshold(long bytes) {
            lowThreshold = bytes;
        }

        @Override
        public int remoteMaxMessageSize() {
            return remoteMaxMessageSize;
        }

        @Override
        public Optional<String> selectedCandidateType() {
            return Optional.of("host");
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            byToken.remove(token);
            post(thread, () -> events.closed("closed"));
            FakeSession target = remote;
            if (target != null && !target.closed) {
                post(target.thread, () -> target.events.closed("remote closed"));
                target.closed = true;
                byToken.remove(target.token);
            }
            thread.shutdown();
        }
    }
}
