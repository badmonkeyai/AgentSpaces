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

import ai.badmonkey.agentspaces.common.id.PeerId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * One signalling session (SRAD srad/agentspaces-js.md §3.5, S3): a dial in progress on the
 * offerer, an {@code OFFER} being answered on the acceptor, and the open connection either
 * becomes. It drives the engine session, turns its events into signals to the remote, applies
 * the remote's signals, checks fingerprints, and ends with an {@code END} code. Sessions are
 * keyed by {@code sid} in {@link RtcSignalCapability}, which also expires a session that has not
 * opened within the dial timeout (JS-W-09, JS-W-10).
 */
final class WebRtcSession implements DataChannelEngine.Events {

    private static final System.Logger LOG = System.getLogger(WebRtcSession.class.getName());

    private final RtcSignalCapability signalling;
    private final WebRtcOptions options;
    private final WebRtcCounters counters;
    private final byte[] sid;
    private final String key;
    private final PeerId remote;
    private final boolean offerer;
    private final long createdNanos = System.nanoTime();
    private final CompletableFuture<WebRtcConnection> opened = new CompletableFuture<>();
    private final Object lock = new Object();
    private final Deque<String[]> queuedCandidates = new ArrayDeque<>();
    private DataChannelEngine.Session engine;
    private volatile WebRtcConnection connection;
    private boolean remoteDescribed;
    private int candidatesReceived;
    private boolean ended;

    WebRtcSession(RtcSignalCapability signalling, byte[] sid, PeerId remote, boolean offerer) {
        this.signalling = signalling;
        this.options = signalling.options();
        this.counters = signalling.counters();
        this.sid = sid.clone();
        this.key = java.util.HexFormat.of().formatHex(sid);
        this.remote = remote;
        this.offerer = offerer;
    }

    String key() {
        return key;
    }

    PeerId remote() {
        return remote;
    }

    boolean offerer() {
        return offerer;
    }

    CompletableFuture<WebRtcConnection> opened() {
        return opened;
    }

    boolean isOpen() {
        return connection != null && !connection.isClosed();
    }

    boolean isEnded() {
        synchronized (lock) {
            return ended;
        }
    }

    /** Whether a session that has not opened is older than the dial timeout. */
    boolean expired(long nowNanos) {
        return !opened.isDone() && nowNanos - createdNanos > options.dialTimeout().toNanos();
    }

    /** Starts the offerer: the engine's offer arrives through {@link #localDescription}. */
    void startOffer(DataChannelEngine engineFactory) throws IOException {
        try {
            synchronized (lock) {
                engine = engineFactory.open(options, this);
                engine.offer();
            }
            counters.sessionsOffered.incrementAndGet();
        } catch (IOException e) {
            counters.failed("engine");
            finish(RtcSignal.END_FAILED, "engine: " + e.getMessage(), true);
            throw e;
        }
    }

    /** Starts the acceptor of {@code offer}: fingerprints first, then the engine's answer. */
    void startAnswer(DataChannelEngine engineFactory, RtcSignal offer) throws IOException {
        if (!Fingerprints.matches(offer.sdp(), offer.fp())) {
            counters.failed("fingerprint");
            finish(RtcSignal.END_REFUSED, "offer fingerprints differ from the signed fp", true);
            throw new IOException("offer fingerprints differ from the signed fp");
        }
        try {
            synchronized (lock) {
                engine = engineFactory.open(options, this);
                engine.answer(offer.sdp());
                remoteDescribed = true;
            }
            counters.sessionsAnswered.incrementAndGet();
        } catch (IOException e) {
            counters.failed("engine");
            finish(RtcSignal.END_FAILED, "engine: " + e.getMessage(), true);
            throw e;
        }
    }

    // ---- signals from the remote

    void onAnswer(RtcSignal answer) {
        synchronized (lock) {
            if (!offerer || remoteDescribed || ended) {
                counters.dropped("unexpected");
                return;
            }
            if (!Fingerprints.matches(answer.sdp(), answer.fp())) {
                counters.failed("fingerprint");
                finish(RtcSignal.END_REFUSED, "answer fingerprints differ from the signed fp", true);
                return;
            }
            try {
                engine.acceptAnswer(answer.sdp());
                remoteDescribed = true;
                flushCandidates();
            } catch (IOException e) {
                counters.failed("engine");
                finish(RtcSignal.END_FAILED, "engine: " + e.getMessage(), true);
            }
        }
    }

    void onCandidate(RtcSignal signal) {
        synchronized (lock) {
            if (ended) {
                return;
            }
            if (++candidatesReceived > options.candidatesPerSession()) {
                counters.dropped("candidates");
                return;
            }
            if (signal.cand().isEmpty()) {
                return; // the end of the trickle: nothing more to add
            }
            if (!remoteDescribed) {
                queuedCandidates.add(new String[]{signal.cand(), signal.mid()});
                return;
            }
            addCandidate(signal.cand(), signal.mid());
        }
    }

    private void flushCandidates() {
        String[] next;
        while ((next = queuedCandidates.poll()) != null) {
            addCandidate(next[0], next[1]);
        }
    }

    private void addCandidate(String candidate, String mid) {
        try {
            engine.addCandidate(candidate, mid);
        } catch (IOException e) {
            LOG.log(System.Logger.Level.DEBUG, "candidate refused by the engine for " + key.substring(0, 8) + ": " + e);
        }
    }

    void onEnd(int code) {
        counters.failed(opened.isDone() ? "closed" : code == RtcSignal.END_REFUSED ? "refused"
                : code == RtcSignal.END_TIMEOUT ? "timeout" : "failed");
        finish(code, "remote END(" + code + ")", false);
    }

    /** The dial timeout passed without a channel: {@code END(2)} to the remote (JS-W-09). */
    void expire() {
        counters.failed("timeout");
        finish(RtcSignal.END_TIMEOUT, "no channel within " + options.dialTimeout(), true);
    }

    /** Ends from this side with a code, telling the remote. */
    void end(int code, String reason) {
        finish(code, reason, true);
    }

    // ---- engine events (on the session's engine thread, in order)

    @Override
    public void localDescription(String sdp, String type) {
        List<String> fp = Fingerprints.of(sdp);
        if (fp.isEmpty()) {
            counters.failed("engine");
            finish(RtcSignal.END_FAILED, "the engine's " + type + " carries no fingerprint", true);
            return;
        }
        LOG.log(System.Logger.Level.TRACE, () -> "local " + type + " for " + key.substring(0, 8) + ":\n" + sdp);
        signalling.send(remote, "answer".equals(type) ? RtcSignal.answer(sid, sdp, fp) : RtcSignal.offer(sid, sdp, fp));
    }

    @Override
    public void localCandidate(String candidate, String mid) {
        LOG.log(System.Logger.Level.TRACE, () -> "local candidate for " + key.substring(0, 8) + ": " + candidate);
        signalling.send(remote, RtcSignal.candidate(sid, candidate, mid));
    }

    @Override
    public void gatheringComplete() {
        signalling.send(remote, RtcSignal.candidate(sid, "", ""));
    }

    @Override
    public void open() {
        WebRtcConnection fresh;
        synchronized (lock) {
            if (ended || connection != null) {
                return;
            }
            fresh = new WebRtcConnection(remote, engine, options, counters);
            connection = fresh;
        }
        fresh.onEnd(() -> finish(RtcSignal.END_NORMAL, "closed here", true));
        counters.opened(engine.selectedCandidateType().orElse("unknown"));
        LOG.log(System.Logger.Level.DEBUG, "webrtc channel open with " + remote.display() + " (" + key.substring(0, 8)
                + ", " + (offerer ? "offerer" : "answerer") + ")");
        opened.complete(fresh);
        signalling.opened(this, fresh);
    }

    @Override
    public void message(ByteBuffer data) {
        WebRtcConnection current = connection;
        if (current == null) {
            return;
        }
        String violation = current.message(data);
        if (violation != null) {
            finish(RtcSignal.END_PROTOCOL, violation, true);
        }
    }

    @Override
    public void textMessage() {
        counters.failed("protocol");
        finish(RtcSignal.END_PROTOCOL, "string message on the channel", true);
    }

    @Override
    public void bufferedAmountLow() {
        WebRtcConnection current = connection;
        if (current != null) {
            current.bufferedAmountLow();
        }
    }

    @Override
    public void closed(String reason) {
        if (!isEnded()) {
            counters.failed(opened.isDone() ? "closed" : "failed");
        }
        finish(RtcSignal.END_FAILED, reason, false);
    }

    @Override
    public void failed(String reason) {
        if (!isEnded()) {
            counters.failed("failed");
        }
        finish(RtcSignal.END_FAILED, reason, !opened.isDone());
    }

    /**
     * Ends the session once: tells the remote when {@code signal} is set and the remote has not
     * ended first, closes the engine, fails a dial still waiting, ends an open connection, and
     * forgets the session.
     */
    private void finish(int code, String reason, boolean signal) {
        DataChannelEngine.Session toClose;
        synchronized (lock) {
            if (ended) {
                return;
            }
            ended = true;
            toClose = engine;
        }
        LOG.log(System.Logger.Level.DEBUG, "webrtc session " + key.substring(0, 8) + " with " + remote.display()
                + " ended: " + reason + " (END " + code + ")");
        if (signal) {
            signalling.send(remote, RtcSignal.end(sid, code));
        }
        opened.completeExceptionally(new IOException("webrtc to " + remote.display() + ": " + reason));
        WebRtcConnection current = connection;
        if (current != null) {
            current.ended();
        }
        if (toClose != null) {
            toClose.close();
        }
        signalling.forget(this);
    }
}
