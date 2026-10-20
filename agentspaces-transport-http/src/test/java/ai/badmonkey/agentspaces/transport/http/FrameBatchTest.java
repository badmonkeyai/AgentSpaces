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

package ai.badmonkey.agentspaces.transport.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * The exchange body codec at its boundaries (SRAD agentspaces-js appendix C;
 * the low-level copy sites of the binding): every bound is judged before a
 * copy, a hostile length prefix never reads past the body, and a round trip
 * of random frames is exact.
 */
class FrameBatchTest {

    private static byte[] bytes(int n, int seed) {
        byte[] out = new byte[n];
        new Random(seed).nextBytes(out);
        return out;
    }

    @Test
    void roundTripsFramesOfEverySizeIncludingTheLargestAllowed() throws IOException {
        List<byte[]> frames = List.of(bytes(1, 1), bytes(2, 2), bytes(65_535, 3), bytes(65_536, 4),
                bytes(HttpOptions.MAX_FRAME, 5));
        List<byte[]> decoded = FrameBatch.decode(FrameBatch.encode(frames));
        assertThat(decoded).hasSize(frames.size());
        for (int i = 0; i < frames.size(); i++) {
            assertThat(decoded.get(i)).isEqualTo(frames.get(i));
        }
        assertThat(FrameBatch.decode(FrameBatch.encode(List.of()))).isEmpty();
        Random random = new Random(7);
        for (int round = 0; round < 200; round++) {
            List<byte[]> some = new ArrayList<>();
            int count = 1 + random.nextInt(8);
            for (int i = 0; i < count; i++) {
                some.add(bytes(1 + random.nextInt(4096), random.nextInt()));
            }
            List<byte[]> back = FrameBatch.decode(FrameBatch.encode(some));
            assertThat(back).hasSize(count);
            for (int i = 0; i < count; i++) {
                assertThat(back.get(i)).isEqualTo(some.get(i));
            }
        }
    }

    @Test
    void theEncoderRefusesWhatTheDecoderWouldBeforeCopying() {
        assertThatThrownBy(() -> FrameBatch.encode(List.of(new byte[0])))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("1 to");
        assertThatThrownBy(() -> FrameBatch.encode(List.of(new byte[HttpOptions.MAX_FRAME + 1])))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(String.valueOf(HttpOptions.MAX_FRAME));
        List<byte[]> tooMany = new ArrayList<>();
        for (int i = 0; i <= HttpOptions.MAX_BATCH_FRAMES; i++) {
            tooMany.add(new byte[1]);
        }
        assertThatThrownBy(() -> FrameBatch.encode(tooMany))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("frames in one body");
        // Two frames each under the frame cap but together over the body cap.
        assertThatThrownBy(() -> FrameBatch.encode(List.of(new byte[HttpOptions.MAX_FRAME], new byte[HttpOptions.MAX_FRAME])))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("exceeds");
    }

    @Test
    void theDecoderRefusesHostileLengthsWithoutReadingPastTheBody() {
        byte[] overBody = new byte[HttpOptions.MAX_BODY + 1];
        assertThatThrownBy(() -> FrameBatch.decode(overBody)).isInstanceOf(IOException.class).hasMessageContaining("body exceeds");
        assertThatThrownBy(() -> FrameBatch.decode(new byte[] {0, 0, 1})).isInstanceOf(IOException.class)
                .hasMessageContaining("truncated");
        for (int length : new int[] {0, -1, Integer.MIN_VALUE, Integer.MAX_VALUE, HttpOptions.MAX_FRAME + 1, 5}) {
            byte[] body = ByteBuffer.allocate(8).putInt(length).putInt(0x01020304).array();
            assertThatThrownBy(() -> FrameBatch.decode(body)).as("length " + length)
                    .isInstanceOf(IOException.class).hasMessageContaining("bad frame length");
        }
        // A length that overruns the body by one byte is refused even when the body is otherwise sound.
        byte[] oneShort = ByteBuffer.allocate(4 + 3).putInt(4).put(new byte[3]).array();
        assertThatThrownBy(() -> FrameBatch.decode(oneShort)).isInstanceOf(IOException.class);
        // More frames than the batch bound, all well-formed, are refused.
        ByteBuffer many = ByteBuffer.allocate((4 + 1) * (HttpOptions.MAX_BATCH_FRAMES + 1));
        for (int i = 0; i <= HttpOptions.MAX_BATCH_FRAMES; i++) {
            many.putInt(1).put((byte) 1);
        }
        assertThatThrownBy(() -> FrameBatch.decode(many.array())).isInstanceOf(IOException.class)
                .hasMessageContaining("frames in one body");
    }
}
