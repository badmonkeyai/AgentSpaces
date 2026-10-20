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

package ai.badmonkey.agentspaces.transport.dtls;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The PDU codec of SRAD appendix B: round trips, every malformed case, and a fuzz (DT-Q-SEC-07). */
class PduTest {

    @Test
    void dataRoundTripsWithAndWithoutAPiggybackedAck() throws Exception {
        byte[] frame = "hello, fleet".getBytes();
        Pdu.Data plain = (Pdu.Data) decode(Pdu.data(7, 3, 1, 4, null, frame, 0, frame.length));
        assertThat(plain.pn()).isEqualTo(7);
        assertThat(plain.fs()).isEqualTo(3);
        assertThat(plain.index()).isEqualTo(1);
        assertThat(plain.count()).isEqualTo(4);
        assertThat(plain.ack()).isNull();
        assertThat(plain.payload()).isEqualTo(frame);

        Pdu.AckBlock block = Pdu.block(List.of(new long[]{90, 100}, new long[]{70, 80}, new long[]{5, 5}), 250);
        Pdu.Data withAck = (Pdu.Data) decode(Pdu.data(0xFFFF_FFFEL, 0xFFFF_FFFFL, 0, 1, block, frame, 0, 5));
        assertThat(withAck.pn()).isEqualTo(0xFFFF_FFFEL);
        assertThat(withAck.fs()).isEqualTo(0xFFFF_FFFFL);
        assertThat(withAck.payload()).isEqualTo("hello".getBytes());
        assertThat(withAck.ack().largestAcked()).isEqualTo(100);
        assertThat(withAck.ack().ackDelayMicros()).isEqualTo(250);
        assertThat(withAck.ack().ranges()).extracting(r -> r[0] + ".." + r[1])
                .containsExactly("90..100", "70..80", "5..5");
    }

    @Test
    void ackPingPongCloseRoundTrip() throws Exception {
        Pdu.AckBlock block = Pdu.block(List.of(new long[]{0, 0}), 0);
        Pdu.Ack ack = (Pdu.Ack) decode(Pdu.ack(block));
        assertThat(ack.block().acknowledges(0)).isTrue();
        assertThat(ack.block().acknowledges(1)).isFalse();
        assertThat(decode(Pdu.ping())).isInstanceOf(Pdu.Ping.class);
        assertThat(decode(Pdu.pong())).isInstanceOf(Pdu.Pong.class);
        assertThat(((Pdu.Close) decode(Pdu.close(Pdu.CLOSE_IDLE))).code()).isEqualTo(Pdu.CLOSE_IDLE);
    }

    @Test
    void ackBlocksClipToThirtyThreeRangesAndSixteenBitFields() throws Exception {
        List<long[]> many = new java.util.ArrayList<>();
        for (long high = 10_000; high > 0; high -= 10) {
            many.add(new long[]{high - 1, high});
        }
        Pdu.AckBlock block = Pdu.block(many, 0);
        assertThat(block.ranges()).hasSize(Pdu.MAX_RANGES + 1);
        Pdu.Ack decoded = (Pdu.Ack) decode(Pdu.ack(block));
        assertThat(decoded.block().ranges()).hasSize(Pdu.MAX_RANGES + 1);
        assertThat(Pdu.ack(block).length).isEqualTo(4 + 12 + 4 * Pdu.MAX_RANGES);

        Pdu.AckBlock wide = Pdu.block(List.of(new long[]{0, 200_000}), 0);
        assertThat(wide.ranges().get(0)[0]).isEqualTo(200_000 - 0xFFFF);
    }

    @Test
    void anUnknownVersionIsDroppedNotFailed() throws Exception {
        byte[] future = Pdu.ping();
        future[0] = 0x02;
        assertThat(Pdu.decode(future, 0, future.length)).isNull();
    }

    @Test
    void malformedPdusAreProtocolErrors() {
        byte[] frame = new byte[4];
        byte[] data = Pdu.data(1, 1, 0, 1, null, frame, 0, 4);

        assertMalformed(new byte[]{1, 1});                          // shorter than a header
        assertMalformed(new byte[]{1, 9, 0, 0});                    // unknown type
        assertMalformed(java.util.Arrays.copyOf(data, Pdu.DATA_HEADER)); // empty payload
        byte[] lane = data.clone();
        lane[3] = 1;
        assertMalformed(lane);                                      // lane not negotiated
        byte[] zeroCount = data.clone();
        zeroCount[14] = 0;
        zeroCount[15] = 0;
        assertMalformed(zeroCount);                                 // count 0
        byte[] indexPastCount = Pdu.data(1, 1, 0, 1, null, frame, 0, 4);
        indexPastCount[13] = 1;
        assertMalformed(indexPastCount);                            // index >= count
        byte[] tooManyRanges = Pdu.ack(Pdu.block(List.of(new long[]{0, 0}), 0));
        tooManyRanges[12] = 0;
        tooManyRanges[13] = 33;
        assertMalformed(tooManyRanges);                             // n > 32
        byte[] belowZero = Pdu.ack(Pdu.block(List.of(new long[]{5, 5}), 0));
        belowZero[14] = 0;
        belowZero[15] = 9;
        assertMalformed(belowZero);                                 // first range below 0
        byte[] truncated = Pdu.ack(Pdu.block(List.of(new long[]{5, 9}, new long[]{0, 1}), 0));
        assertMalformed(java.util.Arrays.copyOf(truncated, truncated.length - 2));
        byte[] trailing = java.util.Arrays.copyOf(Pdu.ack(Pdu.block(List.of(new long[]{0, 0}), 0)), 17);
        assertMalformed(trailing);
    }

    @Test
    void decodingRandomBytesNeverThrowsPastTheDecoder() {
        Random random = new Random(42);
        for (int i = 0; i < 10_000; i++) {
            byte[] noise = new byte[random.nextInt(64)];
            random.nextBytes(noise);
            if (noise.length > 0 && random.nextBoolean()) {
                noise[0] = Pdu.VERSION;
            }
            if (noise.length > 1 && random.nextBoolean()) {
                noise[1] = (byte) (1 + random.nextInt(5));
            }
            try {
                Pdu.decode(noise, 0, noise.length);
            } catch (Pdu.ProtocolError expected) {
                // the only failure the decoder may report
            }
        }
    }

    /** DT-Q-TST-06: the codec matches the golden PDUs laid out from the annex by hand. */
    @Test
    void theCodecMatchesTheGoldenVectors() throws Exception {
        java.nio.file.Path golden = java.nio.file.Path.of("..", "tools", "golden", "golden.json");
        String json = java.nio.file.Files.readString(golden);
        java.util.HexFormat hex = java.util.HexFormat.of();
        Pdu.AckBlock block = Pdu.block(List.of(new long[]{90, 100}, new long[]{70, 80}, new long[]{5, 5}), 250);
        assertThat(hex.formatHex(Pdu.data(7, 3, 1, 4, null, "aspace".getBytes(), 0, 6)))
                .isEqualTo(field(json, "dtls_pdu_data"));
        assertThat(hex.formatHex(Pdu.data(8, 4, 0, 1, block, "x".getBytes(), 0, 1)))
                .isEqualTo(field(json, "dtls_pdu_data_with_ack"));
        assertThat(hex.formatHex(Pdu.ack(block))).isEqualTo(field(json, "dtls_pdu_ack_three_ranges"));
        assertThat(hex.formatHex(Pdu.ping())).isEqualTo(field(json, "dtls_pdu_ping"));
        assertThat(hex.formatHex(Pdu.pong())).isEqualTo(field(json, "dtls_pdu_pong"));
        assertThat(hex.formatHex(Pdu.close(Pdu.CLOSE_IDLE))).isEqualTo(field(json, "dtls_pdu_close_idle"));
        Pdu.Data decoded = (Pdu.Data) decode(hex.parseHex(field(json, "dtls_pdu_data_with_ack")));
        assertThat(decoded.ack().ranges()).extracting(r -> r[0] + ".." + r[1])
                .containsExactly("90..100", "70..80", "5..5");
    }

    private static String field(String json, String name) {
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("\"" + name + "\": \"([0-9a-f]+)\"").matcher(json);
        assertThat(m.find()).as(name).isTrue();
        return m.group(1);
    }

    private static Pdu.Decoded decode(byte[] pdu) throws Pdu.ProtocolError {
        return Pdu.decode(pdu, 0, pdu.length);
    }

    private static void assertMalformed(byte[] pdu) {
        assertThatThrownBy(() -> Pdu.decode(pdu, 0, pdu.length)).isInstanceOf(Pdu.ProtocolError.class);
    }
}
