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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * The data-channel chunk of SRAD srad/agentspaces-js.md appendix B: one flags byte ({@code 0x00}
 * final, {@code 0x01} more) then 1 to {@code C} payload bytes, {@code C = min(16 KiB,
 * maxMessageSize) - 1}. A frame is the concatenation of consecutive payloads up to the first final
 * chunk; nothing carries a length, an index, or a checksum, because the channel is ordered and
 * reliable and DTLS authenticates every record (JS-W-05).
 */
final class Chunks {

    static final int CHUNK_CEILING = 16 * 1024;
    static final byte FINAL = 0x00;
    static final byte MORE = 0x01;

    private Chunks() {
    }

    /** {@code C}: the payload bytes one chunk may carry for a remote advertising {@code maxMessageSize}. */
    static int payloadCapacity(int remoteMaxMessageSize) {
        int message = remoteMaxMessageSize <= 0 ? CHUNK_CEILING : Math.min(CHUNK_CEILING, remoteMaxMessageSize);
        if (message < 2) {
            throw new IllegalArgumentException("maxMessageSize too small for one chunk: " + remoteMaxMessageSize);
        }
        return message - 1;
    }

    /**
     * The {@code ceil(n / C)} chunks of a frame of {@code n} bytes, {@code C = capacity}; an empty
     * frame has no chunk. Inputs are validated before any copy: a null frame, a capacity below 1, or
     * a frame longer than {@code maxFrame} is rejected naming the bound, so the arithmetic below
     * stays obviously in range ({@code offset + take <= frame.length}). One allocation per chunk,
     * the header byte then a {@link Arrays#copyOfRange} slice; no intermediate concatenation.
     *
     * @param frame    the whole frame
     * @param capacity {@code C}, the payload bytes one chunk may carry (at least 1)
     * @param maxFrame the largest frame allowed (the 8 MiB reassembly cap, JS-W-05)
     * @return the chunks, each {@code [flags] + 1..C} bytes
     */
    static List<byte[]> split(byte[] frame, int capacity, int maxFrame) {
        Objects.requireNonNull(frame, "frame");
        if (capacity < 1) {
            throw new IllegalArgumentException("chunk capacity must be at least 1, not " + capacity);
        }
        if (frame.length > maxFrame) {
            throw new IllegalArgumentException("frame of " + frame.length + " bytes exceeds the "
                    + maxFrame + "-byte bound");
        }
        List<byte[]> out = new ArrayList<>((frame.length + capacity - 1) / capacity);
        int offset = 0;
        while (offset < frame.length) {
            int take = Math.min(capacity, frame.length - offset); // offset + take <= frame.length
            byte[] chunk = new byte[1 + take];
            chunk[0] = offset + take < frame.length ? MORE : FINAL;
            System.arraycopy(frame, offset, chunk, 1, take);
            out.add(chunk);
            offset += take;
        }
        return out;
    }

    /** A chunk that the receiver must close the connection on, with {@code END(3)}. */
    static final class ProtocolViolation extends Exception {
        ProtocolViolation(String message) {
            super(message);
        }
    }

    /** Reassembles frames from consecutive chunks, bounded by {@code maxFrame} and {@code C + 1} per chunk. */
    static final class Reassembler {
        private final int maxFrame;
        private final int capacity;
        private ByteArrayOutputStream pending;

        Reassembler(int maxFrame, int capacity) {
            this.maxFrame = maxFrame;
            this.capacity = capacity;
        }

        /**
         * The frame the chunk completes, or null while more chunks are due. Every violation
         * ({@code END(3)} to the sender) discards any partial frame first, so the reassembler is
         * reusable: the connection closes on a violation, but the unit stays in a clean state.
         *
         * @param message one binary message, positioned at its flags byte
         * @return the completed frame, or null
         * @throws ProtocolViolation on an empty payload, a bad flag, an oversize chunk, or a frame
         *                           past {@code maxFrame}
         */
        byte[] accept(ByteBuffer message) throws ProtocolViolation {
            int length = message.remaining();
            if (length < 2) {
                throw violation("chunk with an empty payload");
            }
            if (length > capacity + 1) {
                throw violation("chunk of " + length + " bytes exceeds C + 1 = " + (capacity + 1));
            }
            byte flags = message.get();
            if (flags != FINAL && flags != MORE) {
                throw violation("chunk flags 0x" + Integer.toHexString(flags & 0xff));
            }
            int payload = message.remaining(); // = length - 1, in 1..capacity
            long accumulated = (long) (pending == null ? 0 : pending.size()) + payload;
            if (accumulated > maxFrame) {
                throw violation("frame exceeds " + maxFrame + " bytes");
            }
            if (flags == FINAL && pending == null) {
                byte[] frame = new byte[payload];
                message.get(frame);
                return frame;
            }
            if (pending == null) {
                pending = new ByteArrayOutputStream(Math.min(maxFrame, Math.max(payload, 1024)));
            }
            byte[] part = new byte[payload]; // one copy, safe for a heap or a direct buffer
            message.get(part);
            pending.write(part, 0, payload);
            if (flags == MORE) {
                return null;
            }
            byte[] frame = pending.toByteArray();
            pending = null;
            return frame;
        }

        private ProtocolViolation violation(String message) {
            pending = null; // discard any partial frame: a violation resets the reassembler
            return new ProtocolViolation(message);
        }

        /** Bytes of an unfinished frame held, for tests. */
        int pendingBytes() {
            return pending == null ? 0 : pending.size();
        }
    }
}
