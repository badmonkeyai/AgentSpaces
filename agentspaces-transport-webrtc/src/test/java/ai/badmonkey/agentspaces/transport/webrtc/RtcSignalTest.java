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

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import org.junit.jupiter.api.Test;

import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SRAD srad/agentspaces-js.md §3.5, appendix B, JS-F-02, JS-W-03: the signalling bodies, their
 * canonical bytes, their bounds, and the rule that a receiver ignores keys it does not know.
 *
 * <p>The encodings below are pinned byte for byte, and the same hex strings live in
 * {@code agentspaces-typescript/packages/webrtc/test/signal.test.ts} (the Java-to-TypeScript byte
 * test, in place of golden vectors the generator does not yet emit — JS-W-03). Maps are
 * indefinite-length ({@code 0xbf ... 0xff}) in the record's declared component order ({@code v, fp,
 * mid, sdp, sid, cand, code, kind}), omitting absent keys, exactly as the shared CBOR codec and the
 * TypeScript {@code dumps} both encode a record or an ordered object.
 */
class RtcSignalTest {

    static final CborCodec CODEC = CborCodec.defaultCodec();
    static final byte[] SID = HexFormat.of().parseHex("000102030405060708090a0b0c0d0e0f");
    static final String FP = "sha-256 AB:CD";
    static final String SDP = "v=0\r\na=fingerprint:sha-256 AB:CD\r\n";

    static String hex(RtcSignal signal) {
        return HexFormat.of().formatHex(CODEC.toBytes(signal));
    }

    @Test
    void bodiesEncodeInCanonicalOrderWithAbsentKeysOmitted() {
        // END(2): v, sid, code, kind.
        assertThat(hex(RtcSignal.end(SID, RtcSignal.END_TIMEOUT)))
                .isEqualTo("bf6176016373696450000102030405060708090a0b0c0d0e0f64636f646502646b696e6463454e44ff");
        // The end of the trickle: v, mid, sid, cand(""), kind.
        assertThat(hex(RtcSignal.candidate(SID, "", "0")))
                .isEqualTo("bf617601636d696461306373696450000102030405060708090a0b0c0d0e0f6463616e6460646b696e646443414e44ff");
        // ANSWER: v, fp, sdp, sid, kind.
        assertThat(hex(RtcSignal.answer(SID, SDP, List.of(FP)))).isEqualTo("bf617601626670816d7368612d3235362041"
                + "423a4344637364707822763d300d0a613d66696e6765727072696e743a7368612d3235362041423a43440d0a637369645000"
                + "0102030405060708090a0b0c0d0e0f646b696e6466414e53574552ff");
    }

    @Test
    void theJavaToTypeScriptPins() {
        // The same hex is asserted in packages/webrtc/test/signal.test.ts (JS-W-03), each body
        // with no free text so the two languages pin the structure, not a random SDP.
        assertThat(hex(RtcSignal.offer(SID, SDP, List.of(FP)))).isEqualTo("bf617601626670816d7368612d32353620414"
                + "23a4344637364707822763d300d0a613d66696e6765727072696e743a7368612d3235362041423a43440d0a637369645000"
                + "0102030405060708090a0b0c0d0e0f646b696e64654f46464552ff");
        assertThat(hex(RtcSignal.candidate(SID, "candidate:1 1 UDP 1 192.0.2.1 5000 typ host", "0")))
                .isEqualTo("bf617601636d696461306373696450000102030405060708090a0b0c0d0e0f6463616e64782b63616e646964"
                        + "6174653a312031205544502031203139322e302e322e3120353030302074797020686f7374646b696e646443414e44ff");
    }

    @Test
    void aReceiverIgnoresUnknownKeysAndJudgesTheKnownOnes() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("v", 1);
        body.put("sid", SID);
        body.put("kind", "END");
        body.put("code", 0);
        body.put("certhash", "later");       // a WebRTC Direct hash of a later revision
        body.put("renegotiate", "ice-restart");
        RtcSignal decoded = CODEC.fromBytes(CODEC.toBytes(body), RtcSignal.class);
        assertThat(decoded.malformed()).isEmpty();
        assertThat(decoded.kind()).isEqualTo("END");
        assertThat(decoded.code()).isZero();

        body.put("v", 2);
        assertThat(CODEC.fromBytes(CODEC.toBytes(body), RtcSignal.class).malformed()).contains("v 2");
        body.put("v", 1);
        body.put("sid", new byte[15]);
        assertThat(CODEC.fromBytes(CODEC.toBytes(body), RtcSignal.class).malformed()).contains("sid");
    }

    @Test
    void theBoundsOfAppendixB() {
        assertThat(RtcSignal.offer(SID, "x".repeat(RtcSignal.MAX_SDP + 1), List.of(FP)).malformed()).contains("sdp");
        assertThat(RtcSignal.offer(SID, SDP, List.of()).malformed()).contains("fp");
        assertThat(RtcSignal.offer(SID, SDP, List.of(FP, FP, FP, FP, FP)).malformed()).contains("fp");
        assertThat(RtcSignal.offer(SID, SDP, List.of("md5 AB:CD")).malformed()).as("any algorithm name").isEmpty();
        assertThat(RtcSignal.offer(SID, SDP, List.of("garbage")).malformed()).contains("fp");
        assertThat(RtcSignal.candidate(SID, "c".repeat(RtcSignal.MAX_CAND + 1), "0").malformed()).contains("cand");
        assertThat(new RtcSignal(1, null, null, null, SID, null, null, "RENEG").malformed()).contains("kind RENEG");
        assertThat(new RtcSignal(1, null, null, null, SID, null, -1, "END").malformed()).contains("code");
        assertThat(RtcSignal.end(SID, 4).toString()).as("a log line names kind, sid prefix, and code only")
                .isEqualTo("END(00010203, 4)");
        assertThat(RtcSignal.offer(SID, SDP, List.of(FP)).toString()).doesNotContain("fingerprint").doesNotContain("v=0");
    }
}
