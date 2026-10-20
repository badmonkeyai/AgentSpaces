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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** SRAD srad/agentspaces-js.md JS-W-05, JS-F-01, JS-D-05, appendix B: the chunk, its bounds, and the round trip. */
class ChunksTest {

    private static final int MAX = 8 * 1024 * 1024;

    @Test
    void capacityIsMinOf16KiBAndTheRemoteMaxMessageSizeLessOne() {
        assertThat(Chunks.payloadCapacity(0)).as("unlimited").isEqualTo(16 * 1024 - 1);
        assertThat(Chunks.payloadCapacity(65536)).as("RFC 8841 default").isEqualTo(16 * 1024 - 1);
        assertThat(Chunks.payloadCapacity(262144)).isEqualTo(16 * 1024 - 1);
        assertThat(Chunks.payloadCapacity(1200)).isEqualTo(1199);
        assertThat(Chunks.payloadCapacity(2)).isEqualTo(1);
        assertThatThrownBy(() -> Chunks.payloadCapacity(1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void splitRejectsBadInputsBeforeCopying() {
        assertThatThrownBy(() -> Chunks.split(null, 10, MAX)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> Chunks.split(new byte[4], 0, MAX)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("capacity");
        assertThatThrownBy(() -> Chunks.split(new byte[MAX + 1], 10, MAX)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(String.valueOf(MAX));
        assertThat(Chunks.split(new byte[MAX], 16383, MAX)).as("exactly maxFrame is allowed").isNotEmpty();
        assertThat(Chunks.split(new byte[0], 10, MAX)).as("nothing to carry").isEmpty();
        assertThat(Chunks.split(new byte[1], 1, MAX)).hasSize(1);
    }

    @Test
    void aFrameSendsCeilNOverCChunksAtEveryBoundary() {
        int capacity = Chunks.payloadCapacity(0);
        Random random = new Random(7);
        for (int n : new int[]{0, 1, capacity - 1, capacity, capacity + 1, 2 * capacity, 2 * capacity + 1}) {
            byte[] frame = new byte[n];
            random.nextBytes(frame);
            List<byte[]> chunks = Chunks.split(frame, capacity, MAX);
            assertThat(chunks).hasSize(n == 0 ? 0 : (n + capacity - 1) / capacity);
            for (int i = 0; i < chunks.size(); i++) {
                assertThat(chunks.get(i)[0]).isEqualTo(i == chunks.size() - 1 ? Chunks.FINAL : Chunks.MORE);
                assertThat(chunks.get(i).length).isBetween(2, capacity + 1);
            }
            assertThat(reassemble(chunks, capacity)).isEqualTo(frame);
        }
    }

    @Test
    void aFinalThenMoreStreamStartsAFreshFrame() throws Exception {
        int capacity = 16;
        Chunks.Reassembler r = new Chunks.Reassembler(MAX, capacity);
        assertThat(r.accept(chunk(Chunks.FINAL, "one"))).isEqualTo("one".getBytes());
        assertThat(r.accept(chunk(Chunks.MORE, "two-"))).isNull();
        assertThat(r.accept(chunk(Chunks.FINAL, "three"))).isEqualTo("two-three".getBytes());
    }

    @Test
    void aMalformedChunkOrAnOversizeFrameIsAProtocolViolationAndResetsState() throws Exception {
        int capacity = Chunks.payloadCapacity(0);
        Chunks.Reassembler r = new Chunks.Reassembler(4 * capacity, capacity);
        assertThatThrownBy(() -> r.accept(ByteBuffer.wrap(new byte[]{Chunks.FINAL})))
                .isInstanceOf(Chunks.ProtocolViolation.class).hasMessageContaining("empty payload");
        assertThatThrownBy(() -> r.accept(ByteBuffer.wrap(new byte[]{0x02, 1})))
                .isInstanceOf(Chunks.ProtocolViolation.class).hasMessageContaining("flags");
        assertThatThrownBy(() -> r.accept(ByteBuffer.wrap(new byte[capacity + 2])))
                .isInstanceOf(Chunks.ProtocolViolation.class).hasMessageContaining("C + 1");
        // A partial frame, then a violation, then a fresh frame still reassembles.
        assertThat(r.accept(chunk(Chunks.MORE, "partial"))).isNull();
        assertThat(r.pendingBytes()).isPositive();
        assertThatThrownBy(() -> r.accept(ByteBuffer.wrap(new byte[]{0x7f, 1})))
                .isInstanceOf(Chunks.ProtocolViolation.class);
        assertThat(r.pendingBytes()).as("a violation discards the partial frame").isZero();
        assertThat(r.accept(chunk(Chunks.FINAL, "clean"))).isEqualTo("clean".getBytes());
    }

    @Test
    void anAccumulatedSizeOverflowIsRefusedAtTheBound() throws Exception {
        int capacity = Chunks.payloadCapacity(0);
        Chunks.Reassembler r = new Chunks.Reassembler(4 * capacity, capacity);
        byte[] full = new byte[capacity + 1];
        full[0] = Chunks.MORE;
        for (int i = 0; i < 4; i++) {
            assertThat(r.accept(ByteBuffer.wrap(full))).isNull(); // 4 * capacity accumulated, at the bound
        }
        assertThatThrownBy(() -> r.accept(ByteBuffer.wrap(new byte[]{Chunks.FINAL, 1})))
                .isInstanceOf(Chunks.ProtocolViolation.class).hasMessageContaining("exceeds");
        assertThat(r.pendingBytes()).isZero();
    }

    @Test
    void aRandomizedRoundTripAtManySizesAndCapacities() throws Exception {
        Random random = new Random(99);
        for (int i = 0; i < 400; i++) {
            int capacity = 1 + random.nextInt(4096);
            int n = random.nextInt(50_000);
            byte[] frame = new byte[n];
            random.nextBytes(frame);
            assertThat(reassemble(Chunks.split(frame, capacity, MAX), capacity)).isEqualTo(frame);
        }
    }

    @Test
    void theChannelLabelsCarryTheirVersionAndTheDatagramLabelIsReserved() {
        assertThat(WebRtcOptions.CHANNEL_LABEL).isEqualTo("aspace/1");
        assertThat(WebRtcOptions.CHANNEL_ID).isZero();
        assertThat(WebRtcOptions.DATAGRAM_LABEL).isEqualTo("aspace-dgram/1");
        assertThat(WebRtcOptions.DATAGRAM_ID).isEqualTo(1);
        assertThat(WebRtcOptions.defaults().maxFrame()).isEqualTo(MAX);
    }

    private static ByteBuffer chunk(byte flags, String payload) {
        byte[] bytes = payload.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(1 + bytes.length);
        buffer.put(flags).put(bytes).flip();
        return buffer;
    }

    private static byte[] reassemble(List<byte[]> chunks, int capacity) {
        Chunks.Reassembler reassembler = new Chunks.Reassembler(MAX, capacity);
        byte[] out = chunks.isEmpty() ? new byte[0] : null;
        try {
            for (byte[] chunk : chunks) {
                assertThat(out).isNull();
                out = reassembler.accept(ByteBuffer.wrap(chunk));
            }
        } catch (Chunks.ProtocolViolation e) {
            throw new AssertionError(e);
        }
        return out;
    }
}
