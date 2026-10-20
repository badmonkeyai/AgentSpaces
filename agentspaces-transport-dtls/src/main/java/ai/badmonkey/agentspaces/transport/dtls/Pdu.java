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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * The recovery sublayer's protocol data units (SRAD appendix B, SPEC annex).
 * Every PDU starts with the sublayer version and its type; all integers are
 * big-endian and unsigned. Decoding is bounded on every field and never
 * throws past {@link #decode}: a malformed PDU becomes a
 * {@link ProtocolError} the association answers with {@code CLOSE(1)}.
 */
final class Pdu {

    static final int VERSION = 0x01;

    static final int DATA = 0x01;
    static final int ACK = 0x02;
    static final int PING = 0x03;
    static final int PONG = 0x04;
    static final int CLOSE = 0x05;

    /** Bytes before a DATA payload (or its piggybacked ACK block). */
    static final int DATA_HEADER = 16;
    /** Additional ACK ranges an ACK block may carry. */
    static final int MAX_RANGES = 32;
    static final int FLAG_ACK = 0x01;
    static final int FLAG_FIN = 0x02;

    static final int CLOSE_NORMAL = 0;
    static final int CLOSE_PROTOCOL = 1;
    static final int CLOSE_BOUND = 2;
    static final int CLOSE_IDLE = 3;

    static final long MAX_U32 = 0xFFFF_FFFFL;

    private Pdu() {
    }

    /** A malformed PDU: the association closes with {@code CLOSE(1)}. */
    static final class ProtocolError extends Exception {
        ProtocolError(String message) {
            super(message);
        }
    }

    /** A decoded PDU. */
    sealed interface Decoded permits Data, Ack, Ping, Pong, Close {
    }

    /** Fragment {@code index} of {@code count} of frame {@code fs}, sent as packet {@code pn}. */
    record Data(long pn, long fs, int index, int count, AckBlock ack, byte[] payload) implements Decoded {
    }

    /** A standalone acknowledgement. */
    record Ack(AckBlock block) implements Decoded {
    }

    /** A keepalive probe. */
    record Ping() implements Decoded {
    }

    /** The answer to a {@link Ping}. */
    record Pong() implements Decoded {
    }

    /** The peer closed the association with a reason code. */
    record Close(int code) implements Decoded {
    }

    /**
     * An RFC 9000 §19.3-style acknowledgement: the largest packet number
     * acknowledged, the receiver's delay before acknowledging it, and the
     * acknowledged ranges, newest first, as inclusive {@code [low, high]} pairs.
     */
    record AckBlock(long largestAcked, long ackDelayMicros, List<long[]> ranges) {

        /** Whether the block acknowledges {@code pn}. */
        boolean acknowledges(long pn) {
            for (long[] range : ranges) {
                if (pn >= range[0] && pn <= range[1]) {
                    return true;
                }
            }
            return false;
        }

        int encodedLength() {
            return 4 + 4 + 2 + 2 + 4 * (ranges.size() - 1);
        }
    }

    // ------------------------------------------------------------- encoding

    static byte[] data(long pn, long fs, int index, int count, AckBlock ack,
                       byte[] frame, int offset, int length) {
        int ackLength = ack == null ? 0 : ack.encodedLength();
        ByteBuffer out = ByteBuffer.allocate(DATA_HEADER + ackLength + length);
        out.put((byte) VERSION).put((byte) DATA).put((byte) (ack == null ? 0 : FLAG_ACK)).put((byte) 0);
        out.putInt((int) pn).putInt((int) fs).putShort((short) index).putShort((short) count);
        if (ack != null) {
            putAck(out, ack);
        }
        out.put(frame, offset, length);
        return out.array();
    }

    static byte[] ack(AckBlock block) {
        ByteBuffer out = ByteBuffer.allocate(4 + block.encodedLength());
        out.put((byte) VERSION).put((byte) ACK).put((byte) 0).put((byte) 0);
        putAck(out, block);
        return out.array();
    }

    static byte[] ping() {
        return new byte[]{VERSION, PING, 0, 0};
    }

    static byte[] pong() {
        return new byte[]{VERSION, PONG, 0, 0};
    }

    static byte[] close(int code) {
        return new byte[]{VERSION, CLOSE, (byte) code, 0};
    }

    private static void putAck(ByteBuffer out, AckBlock block) {
        List<long[]> ranges = block.ranges();
        out.putInt((int) block.largestAcked());
        out.putInt((int) Math.min(block.ackDelayMicros(), MAX_U32));
        out.putShort((short) (ranges.size() - 1));
        long[] first = ranges.get(0);
        out.putShort((short) (first[1] - first[0]));
        long previousLow = first[0];
        for (int i = 1; i < ranges.size(); i++) {
            long[] range = ranges.get(i);
            // RFC 9000 §19.3.1: gap = previous smallest - this largest - 2.
            out.putShort((short) (previousLow - range[1] - 2));
            out.putShort((short) (range[1] - range[0]));
            previousLow = range[0];
        }
    }

    /**
     * Builds an ACK block from received ranges, newest first, keeping at most
     * {@link #MAX_RANGES} additional ranges and clipping any range or gap a
     * 16-bit field cannot carry (an unacknowledged packet is detected as lost
     * and retransmitted, so clipping costs a retransmission, never data).
     */
    static AckBlock block(List<long[]> newestFirst, long ackDelayMicros) {
        List<long[]> ranges = new ArrayList<>();
        long previousLow = -1;
        for (long[] range : newestFirst) {
            long low = range[0];
            long high = range[1];
            if (!ranges.isEmpty()) {
                long gap = previousLow - high - 2;
                if (gap < 0 || gap > 0xFFFF) {
                    break;
                }
            }
            if (high - low > 0xFFFF) {
                low = high - 0xFFFF;
            }
            ranges.add(new long[]{low, high});
            previousLow = low;
            if (ranges.size() == MAX_RANGES + 1) {
                break;
            }
        }
        return new AckBlock(ranges.get(0)[1], ackDelayMicros, List.copyOf(ranges));
    }

    // ------------------------------------------------------------- decoding

    /**
     * Decodes one PDU.
     *
     * @return the PDU, or {@code null} when its version is unknown (dropped and counted)
     * @throws ProtocolError when the PDU is malformed
     */
    static Decoded decode(byte[] buf, int off, int len) throws ProtocolError {
        if (len < 4) {
            throw new ProtocolError("PDU shorter than its header");
        }
        if ((buf[off] & 0xFF) != VERSION) {
            return null;
        }
        ByteBuffer in = ByteBuffer.wrap(buf, off, len);
        in.position(off + 1);
        int type = in.get() & 0xFF;
        int flagsOrCode = in.get() & 0xFF;
        int lane = in.get() & 0xFF;
        switch (type) {
            case DATA -> {
                if (lane != 0) {
                    throw new ProtocolError("lane " + lane + " is not negotiated");
                }
                if (len < DATA_HEADER + 1) {
                    throw new ProtocolError("DATA shorter than its header");
                }
                long pn = in.getInt() & MAX_U32;
                long fs = in.getInt() & MAX_U32;
                int index = in.getShort() & 0xFFFF;
                int count = in.getShort() & 0xFFFF;
                if (count == 0 || index >= count) {
                    throw new ProtocolError("fragment " + index + " of " + count);
                }
                AckBlock ack = (flagsOrCode & FLAG_ACK) != 0 ? getAck(in) : null;
                int payload = in.remaining();
                if (payload == 0) {
                    throw new ProtocolError("empty DATA payload");
                }
                byte[] bytes = new byte[payload];
                in.get(bytes);
                return new Data(pn, fs, index, count, ack, bytes);
            }
            case ACK -> {
                AckBlock block = getAck(in);
                if (in.hasRemaining()) {
                    throw new ProtocolError("trailing bytes after ACK");
                }
                return new Ack(block);
            }
            case PING -> {
                return new Ping();
            }
            case PONG -> {
                return new Pong();
            }
            case CLOSE -> {
                return new Close(flagsOrCode);
            }
            default -> throw new ProtocolError("unknown PDU type " + type);
        }
    }

    private static AckBlock getAck(ByteBuffer in) throws ProtocolError {
        if (in.remaining() < 12) {
            throw new ProtocolError("ACK block truncated");
        }
        long largest = in.getInt() & MAX_U32;
        long delay = in.getInt() & MAX_U32;
        int n = in.getShort() & 0xFFFF;
        int first = in.getShort() & 0xFFFF;
        if (n > MAX_RANGES) {
            throw new ProtocolError("ACK block with " + n + " ranges");
        }
        if (in.remaining() < 4 * n) {
            throw new ProtocolError("ACK ranges truncated");
        }
        if (first > largest) {
            throw new ProtocolError("ACK range below packet number 0");
        }
        List<long[]> ranges = new ArrayList<>(n + 1);
        long low = largest - first;
        ranges.add(new long[]{low, largest});
        for (int i = 0; i < n; i++) {
            int gap = in.getShort() & 0xFFFF;
            int length = in.getShort() & 0xFFFF;
            long high = low - gap - 2;
            if (high < 0 || high - length < 0) {
                throw new ProtocolError("ACK range below packet number 0");
            }
            low = high - length;
            ranges.add(new long[]{low, high});
        }
        return new AckBlock(largest, delay, List.copyOf(ranges));
    }
}
