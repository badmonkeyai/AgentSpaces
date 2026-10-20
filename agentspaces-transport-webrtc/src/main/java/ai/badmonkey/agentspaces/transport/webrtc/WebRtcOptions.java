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

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * The tunables of the {@code webrtc} binding (SRAD srad/agentspaces-js.md §4.3), each a
 * per-source rate, a per-message size, or a bound on one session; nothing here caps members,
 * sessions, or open connections (JS-W-10).
 *
 * @param iceServers            the ICE servers; empty by default, so only host and mDNS candidates
 *                              gather until an operator names servers (JS-W-07, decision 9)
 * @param dialTimeout           the bound on signalling, ICE, DTLS, and channel open together, and the
 *                              life of a pending session (JS-W-09, JS-W-10); 15 s
 * @param pauseAbove            the {@code bufferedAmount} above which a sender pauses (JS-W-06); 4 MiB
 * @param resumeBelow           the {@code bufferedamountlow} threshold at which it resumes; 1 MiB
 * @param sendBlocked           how long a paused {@code send} waits before it fails; 30 s
 * @param maxFrame              the largest frame a receiver reassembles (JS-W-05); 8 MiB
 * @param offersPerPeerPerMinute the {@code OFFER} rate accepted from one remote peer; 6
 * @param candidatesPerSession  the {@code CAND} signals accepted for one session; 64
 * @param maxSignalBytes        the largest signal body accepted; 64 KiB
 */
public record WebRtcOptions(List<IceServer> iceServers, Duration dialTimeout, long pauseAbove, long resumeBelow,
                            Duration sendBlocked, int maxFrame, int offersPerPeerPerMinute,
                            int candidatesPerSession, int maxSignalBytes) {

    /** The label of the one reliable, ordered data channel (appendix B, JS-F-01). */
    public static final String CHANNEL_LABEL = "aspace/1";
    /** The negotiated stream id of that channel. */
    public static final int CHANNEL_ID = 0;
    /** The reserved label of the unreliable channel of the {@code DATAGRAM} capability (JS-D-05). */
    public static final String DATAGRAM_LABEL = "aspace-dgram/1";
    /** The reserved stream id of that channel. */
    public static final int DATAGRAM_ID = 1;

    /**
     * One ICE server: STUN, TURN, or TURN over TLS, with the credential TURN needs (JS-W-07).
     *
     * @param urls       the server URLs ({@code stun:}, {@code turn:}, {@code turns:})
     * @param username   the TURN username, or null
     * @param credential the TURN credential, or null
     */
    public record IceServer(List<String> urls, String username, String credential) {
        public IceServer {
            urls = List.copyOf(Objects.requireNonNull(urls, "urls"));
            if (urls.isEmpty()) {
                throw new IllegalArgumentException("an ice server needs at least one url");
            }
            for (String url : urls) {
                String lower = url.toLowerCase(java.util.Locale.ROOT);
                if (!lower.startsWith("stun:") && !lower.startsWith("stuns:") && !lower.startsWith("turn:")
                        && !lower.startsWith("turns:")) {
                    throw new IllegalArgumentException("not an ice server url (stun:, turn:, turns:): " + url);
                }
            }
            if ((username == null) != (credential == null)) {
                throw new IllegalArgumentException("username and credential come together");
            }
        }

        /** A server without credentials. */
        public static IceServer of(String... urls) {
            return new IceServer(List.of(urls), null, null);
        }
    }

    public WebRtcOptions {
        iceServers = List.copyOf(Objects.requireNonNull(iceServers, "iceServers"));
        Objects.requireNonNull(dialTimeout, "dialTimeout");
        Objects.requireNonNull(sendBlocked, "sendBlocked");
        if (dialTimeout.isNegative() || dialTimeout.isZero() || sendBlocked.isNegative() || sendBlocked.isZero()) {
            throw new IllegalArgumentException("dial-timeout and send-blocked must be positive");
        }
        if (resumeBelow <= 0 || pauseAbove <= resumeBelow) {
            throw new IllegalArgumentException("pause-above must exceed resume-below, both positive");
        }
        if (maxFrame < Chunks.CHUNK_CEILING) {
            throw new IllegalArgumentException("max-frame must hold at least one chunk (" + Chunks.CHUNK_CEILING + ")");
        }
        if (offersPerPeerPerMinute < 1 || candidatesPerSession < 1 || maxSignalBytes < 1024) {
            throw new IllegalArgumentException("offers-per-peer-per-minute and candidates-per-session must be"
                    + " positive; max-signal-bytes at least 1024");
        }
    }

    /** The defaults of SRAD §4.3. */
    public static WebRtcOptions defaults() {
        return new WebRtcOptions(List.of(), Duration.ofSeconds(15), 4L * 1024 * 1024, 1024 * 1024,
                Duration.ofSeconds(30), 8 * 1024 * 1024, 6, 64, 64 * 1024);
    }

    public WebRtcOptions withIceServers(List<IceServer> value) {
        return new WebRtcOptions(value, dialTimeout, pauseAbove, resumeBelow, sendBlocked, maxFrame,
                offersPerPeerPerMinute, candidatesPerSession, maxSignalBytes);
    }

    public WebRtcOptions withDialTimeout(Duration value) {
        return new WebRtcOptions(iceServers, value, pauseAbove, resumeBelow, sendBlocked, maxFrame,
                offersPerPeerPerMinute, candidatesPerSession, maxSignalBytes);
    }

    public WebRtcOptions withPauseAbove(long value) {
        return new WebRtcOptions(iceServers, dialTimeout, value, resumeBelow, sendBlocked, maxFrame,
                offersPerPeerPerMinute, candidatesPerSession, maxSignalBytes);
    }

    public WebRtcOptions withResumeBelow(long value) {
        return new WebRtcOptions(iceServers, dialTimeout, pauseAbove, value, sendBlocked, maxFrame,
                offersPerPeerPerMinute, candidatesPerSession, maxSignalBytes);
    }

    public WebRtcOptions withSendBlocked(Duration value) {
        return new WebRtcOptions(iceServers, dialTimeout, pauseAbove, resumeBelow, value, maxFrame,
                offersPerPeerPerMinute, candidatesPerSession, maxSignalBytes);
    }

    public WebRtcOptions withMaxFrame(int value) {
        return new WebRtcOptions(iceServers, dialTimeout, pauseAbove, resumeBelow, sendBlocked, value,
                offersPerPeerPerMinute, candidatesPerSession, maxSignalBytes);
    }

    public WebRtcOptions withOffersPerPeerPerMinute(int value) {
        return new WebRtcOptions(iceServers, dialTimeout, pauseAbove, resumeBelow, sendBlocked, maxFrame,
                value, candidatesPerSession, maxSignalBytes);
    }

    public WebRtcOptions withCandidatesPerSession(int value) {
        return new WebRtcOptions(iceServers, dialTimeout, pauseAbove, resumeBelow, sendBlocked, maxFrame,
                offersPerPeerPerMinute, value, maxSignalBytes);
    }

    public WebRtcOptions withMaxSignalBytes(int value) {
        return new WebRtcOptions(iceServers, dialTimeout, pauseAbove, resumeBelow, sendBlocked, maxFrame,
                offersPerPeerPerMinute, candidatesPerSession, value);
    }
}
