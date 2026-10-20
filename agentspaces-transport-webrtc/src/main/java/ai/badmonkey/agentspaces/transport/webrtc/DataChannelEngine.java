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
import java.util.Optional;

/**
 * The engine seam (SRAD srad/agentspaces-js.md JS-W-01, JS-F-05, decision 3): what the binding
 * needs of a WebRTC implementation, and nothing of any one engine's API. One {@link Session} is
 * one RTCPeerConnection with the {@code aspace/1} data channel created before the offer or answer
 * (negotiated, id 0, ordered, reliable), its DTLS certificate generated per connection by the
 * engine; the engine reports the local description, the trickled candidates, the open, every
 * binary message, the buffered-amount-low event, and the end. A pure-Java engine (JS-D-04) or a
 * fake for tests implements this and nothing else changes.
 */
interface DataChannelEngine {

    /** The engine's name, for logs and the console. */
    String name();

    /**
     * Why the engine cannot run here (a native library missing for this platform, say), naming
     * the artifact; empty when it can.
     */
    Optional<String> unavailable();

    /**
     * Creates a peer connection with the {@code aspace/1} channel and the options' ICE servers.
     * Nothing is sent until {@link Session#offer()} or {@link Session#answer(String)}.
     *
     * @param options the binding's options
     * @param events  the sink of the connection's events, called on an engine thread of its own
     *                in order
     * @return the session
     * @throws IOException when the engine cannot create a connection
     */
    Session open(WebRtcOptions options, Events events) throws IOException;

    /** One peer connection and its channel. */
    interface Session extends AutoCloseable {

        /** Makes this side the offerer: the engine reports the offer through {@link Events#localDescription}. */
        void offer() throws IOException;

        /** Makes this side the answerer of {@code offerSdp}: the engine reports the answer. */
        void answer(String offerSdp) throws IOException;

        /** Completes the offerer's negotiation with the remote answer. */
        void acceptAnswer(String answerSdp) throws IOException;

        /** Adds one remote candidate; the engine may be given it only after a remote description. */
        void addCandidate(String candidate, String mid) throws IOException;

        /** Sends one binary message (one chunk) on the channel. */
        void send(ByteBuffer message) throws IOException;

        /** The channel's buffered, unsent bytes. */
        long bufferedAmount();

        /** Sets the threshold below which {@link Events#bufferedAmountLow()} fires. */
        void bufferedAmountLowThreshold(long bytes);

        /** The remote's advertised {@code a=max-message-size}, 0 when unknown or unlimited. */
        int remoteMaxMessageSize();

        /** The type of the selected local candidate ({@code host}, {@code srflx}, {@code relay}, ...), when known. */
        Optional<String> selectedCandidateType();

        /** Closes the channel and the connection; idempotent. */
        @Override
        void close();
    }

    /** What a session reports; the engine calls each on its own thread, in order. */
    interface Events {

        /** The local description is ready: {@code type} is {@code offer} or {@code answer}. */
        void localDescription(String sdp, String type);

        /** One local candidate gathered, to trickle. */
        void localCandidate(String candidate, String mid);

        /** Gathering finished: the trickle ends. */
        void gatheringComplete();

        /** The {@code aspace/1} channel opened: DTLS proved the remote's fingerprint. */
        void open();

        /** One binary message arrived. */
        void message(ByteBuffer data);

        /** A string message arrived: a protocol error (appendix B). */
        void textMessage();

        /** The buffered amount fell below the threshold. */
        void bufferedAmountLow();

        /** The connection ended, after open or before it. */
        void closed(String reason);

        /** The connection failed (ICE or DTLS), before or after open. */
        void failed(String reason);
    }
}
