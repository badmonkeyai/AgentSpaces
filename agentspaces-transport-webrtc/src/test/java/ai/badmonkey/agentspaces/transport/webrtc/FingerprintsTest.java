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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** SRAD srad/agentspaces-js.md JS-W-04: the fingerprints of an SDP against the signed {@code fp} list. */
class FingerprintsTest {

    static final String FP = "sha-256 0C:07:63:37:F9:7E:61:23:41:CC:47:09:09:BE:F6:34:92:D9:D7:F9:DF:2A:83:0E:35:1E:D4:79:40:F9:12:24";
    static final String SDP = "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\na=fingerprint:sha-256 "
            + "0c:07:63:37:f9:7e:61:23:41:cc:47:09:09:be:f6:34:92:d9:d7:f9:df:2a:83:0e:35:1e:d4:79:40:f9:12:24\r\n"
            + "a=max-message-size:262144\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n"
            + "a=fingerprint:sha-256 0C:07:63:37:F9:7E:61:23:41:CC:47:09:09:BE:F6:34:92:D9:D7:F9:DF:2A:83:0E:35:1E:D4:79:40:F9:12:24\r\n";

    @Test
    void fingerprintLinesNormalizeToTheSignedForm() {
        assertThat(Fingerprints.of(SDP)).as("session and media level lines, case-normalized, deduplicated")
                .containsExactly(FP);
        assertThat(Fingerprints.of("v=0\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n")).isEmpty();
        assertThat(Fingerprints.normalize("SHA-256 ab:cd")).contains("sha-256 AB:CD");
        assertThat(Fingerprints.normalize("sha-256")).isEmpty();
        assertThat(Fingerprints.normalize("sha-256 ZZ:00")).isEmpty();
    }

    @Test
    void anSdpMatchesExactlyTheSignedListAndNothingElse() {
        assertThat(Fingerprints.matches(SDP, List.of(FP))).isTrue();
        assertThat(Fingerprints.matches(SDP, List.of(FP.toLowerCase()))).as("case is not identity").isTrue();
        assertThat(Fingerprints.matches(SDP, List.of(FP.replace("0C:07", "0C:08")))).as("one byte off").isFalse();
        assertThat(Fingerprints.matches(SDP, List.of(FP, FakeEngine.randomFingerprint()))).as("an extra entry").isFalse();
        assertThat(Fingerprints.matches(SDP, List.of())).isFalse();
        assertThat(Fingerprints.matches("v=0\r\n", List.of(FP))).as("an SDP without a fingerprint").isFalse();
        assertThat(Fingerprints.matches(SDP, List.of("not a fingerprint"))).isFalse();
    }
}
