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

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.HexFormat;
import java.util.List;
import java.util.Optional;

/**
 * One signalling body (SRAD srad/agentspaces-js.md §3.5, appendix B): the payload of a
 * {@code PIPE_DATA} frame of capability {@link #CAPABILITY}, a CBOR map in the canonical order of
 * wire version 3. The components are declared in that order ({@code v, fp, mid, sdp, sid, cand,
 * code, kind}: RFC 8949 §4.2.1, length first, then bytewise), absent keys are omitted, and a
 * receiver ignores keys it does not know (JS-F-02), so later fields need no migration.
 *
 * @param v    the body version, 1
 * @param fp   OFFER and ANSWER: {@code "sha-256 " + uppercase colon-separated hex} for each
 *             certificate the sender will present, 1 to 4 entries
 * @param mid  CAND: the media id the candidate belongs to
 * @param sdp  OFFER and ANSWER: the full session description, at most 16 KiB
 * @param sid  the session id, 16 bytes chosen by the offerer at random
 * @param cand CAND: one candidate line, at most 1 KiB; the empty string ends the trickle
 * @param code END: 0 normal, 1 refused, 2 timeout, 3 protocol error, 4 failed
 * @param kind {@code OFFER}, {@code ANSWER}, {@code CAND}, or {@code END}
 */
public record RtcSignal(int v,
                        @JsonInclude(JsonInclude.Include.NON_NULL) List<String> fp,
                        @JsonInclude(JsonInclude.Include.NON_NULL) String mid,
                        @JsonInclude(JsonInclude.Include.NON_NULL) String sdp,
                        byte[] sid,
                        @JsonInclude(JsonInclude.Include.NON_NULL) String cand,
                        @JsonInclude(JsonInclude.Include.NON_NULL) Integer code,
                        String kind) {

    /** The capability type URI the signals travel under. */
    public static final String CAPABILITY = "aspace:cap/rtc-signal";
    /** The body version this binding writes and accepts. */
    public static final int VERSION = 1;
    public static final String OFFER = "OFFER";
    public static final String ANSWER = "ANSWER";
    public static final String CAND = "CAND";
    public static final String END = "END";
    /** END codes (appendix B). */
    public static final int END_NORMAL = 0;
    public static final int END_REFUSED = 1;
    public static final int END_TIMEOUT = 2;
    public static final int END_PROTOCOL = 3;
    public static final int END_FAILED = 4;
    /** The bounds of appendix B. */
    public static final int SID_LENGTH = 16;
    public static final int MAX_SDP = 16 * 1024;
    public static final int MAX_CAND = 1024;
    public static final int MAX_FINGERPRINTS = 4;

    public static RtcSignal offer(byte[] sid, String sdp, List<String> fp) {
        return new RtcSignal(VERSION, List.copyOf(fp), null, sdp, sid, null, null, OFFER);
    }

    public static RtcSignal answer(byte[] sid, String sdp, List<String> fp) {
        return new RtcSignal(VERSION, List.copyOf(fp), null, sdp, sid, null, null, ANSWER);
    }

    /** A trickled candidate; an empty {@code cand} ends the trickle. */
    public static RtcSignal candidate(byte[] sid, String cand, String mid) {
        return new RtcSignal(VERSION, null, mid == null ? "" : mid, null, sid, cand, null, CAND);
    }

    public static RtcSignal end(byte[] sid, int code) {
        return new RtcSignal(VERSION, null, null, null, sid, null, code, END);
    }

    /**
     * Why this body is not a well-formed signal of appendix B, or empty when it is. Unknown keys
     * never arrive here (the codec drops them), so only the known keys are judged.
     */
    public Optional<String> malformed() {
        if (v != VERSION) {
            return Optional.of("v " + v);
        }
        if (sid == null || sid.length != SID_LENGTH) {
            return Optional.of("sid");
        }
        if (kind == null) {
            return Optional.of("kind");
        }
        switch (kind) {
            case OFFER, ANSWER -> {
                if (sdp == null || sdp.isEmpty() || sdp.length() > MAX_SDP) {
                    return Optional.of("sdp");
                }
                if (fp == null || fp.isEmpty() || fp.size() > MAX_FINGERPRINTS
                        || fp.stream().anyMatch(f -> Fingerprints.normalize(f).isEmpty())) {
                    return Optional.of("fp");
                }
            }
            case CAND -> {
                if (cand == null || cand.length() > MAX_CAND || mid == null) {
                    return Optional.of("cand");
                }
            }
            case END -> {
                if (code == null || code < 0) {
                    return Optional.of("code");
                }
            }
            default -> {
                return Optional.of("kind " + kind);
            }
        }
        return Optional.empty();
    }

    /** The session id as lowercase hex, the key of a pending session. */
    public String sessionKey() {
        return HexFormat.of().formatHex(sid);
    }

    @Override
    public String toString() {
        // Never the SDP or a candidate (JS-W-13): what a log line may carry.
        return kind + "(" + (sid == null ? "?" : sessionKey().substring(0, 8)) + (code == null ? "" : ", " + code) + ")";
    }
}
