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
package ai.badmonkey.agentspaces.content.tree.sync;

import ai.badmonkey.agentspaces.common.crypto.Digests;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The negentropy v1 protocol (PDT 2 phase 5, P2-K-01; srad/agentspaces-pdt2.md §3.6): range-based set
 * reconciliation as the reference implementation writes it, byte for byte (hoytech/negentropy, commit
 * {@code fe0bb8ad}, {@code docs/negentropy-protocol-v1.md}). A message opens with the version byte
 * {@link #PROTOCOL_VERSION}, then lists ranges, each an upper bound, a mode, and the mode's payload. A bound is a
 * timestamp (coded 0 for infinity, else 1 plus the offset from the previous bound of the message) and an id prefix.
 * The modes are {@link #MODE_SKIP}, {@link #MODE_FINGERPRINT} (the first 16 bytes of sha-256 over the 256-bit
 * little-endian sum of the range's ids and the varint count), and {@link #MODE_ID_LIST}. A mismatched range splits
 * into {@link #BUCKETS} buckets, or lists its ids below 32 items. Varints are base 128, most significant group
 * first, every byte but the last with its top bit set.
 *
 * <p>The initiator calls {@link #initiate} once and then {@link #reconcile(byte[], List, List)} on each reply until it
 * returns empty, collecting the ids it holds that the other side lacks ({@code have}) and the ids the other side holds
 * that it lacks ({@code need}). The other side answers each message with {@link #reconcile(byte[])}. With a frame size
 * limit (0 for none, else at least {@value #MIN_FRAME_SIZE_LIMIT}) a message stops 200 bytes short of it and closes
 * with a fingerprint of the rest. A responder that receives another version byte in {@code 0x60..0x6f} answers with
 * the bare version byte, so the asker can fall back.
 *
 * <p>Java codes timestamps as 64-bit varints, so a timestamp may be any value below {@link Long#MAX_VALUE}, which
 * stands for infinity. A malformed message (a premature end, a varint past 63 bits, a bound key over 32 bytes, an
 * unknown mode, a version byte outside {@code 0x60..0x6f}) throws {@link IllegalArgumentException} naming the fault,
 * so a caller drops and counts it (P2-K-04). An instance belongs to one session and one thread.
 */
public final class Negentropy {

    /** The version byte of protocol v1. */
    public static final int PROTOCOL_VERSION = 0x61;
    /** The bytes of an id. */
    public static final int ID_SIZE = 32;
    /** The bytes of a fingerprint. */
    public static final int FINGERPRINT_SIZE = 16;
    /** The smallest frame size limit other than 0. */
    public static final int MIN_FRAME_SIZE_LIMIT = 4096;
    /** The buckets a mismatched range splits into. */
    static final int BUCKETS = 16;
    /** A message stops this many bytes short of the frame size limit. */
    static final int FRAME_MARGIN = 200;

    /** A range both sides hold alike. */
    public static final int MODE_SKIP = 0;
    /** A range named by its fingerprint. */
    public static final int MODE_FINGERPRINT = 1;
    /** A range named by its ids. */
    public static final int MODE_ID_LIST = 2;

    /** The timestamp of the infinite bound. */
    static final long INFINITY = Long.MAX_VALUE;

    /**
     * One element of a set: a timestamp and a 32-byte id.
     *
     * @param timestamp at least 0 and below {@link Long#MAX_VALUE}
     * @param id        {@link #ID_SIZE} bytes
     */
    public record Item(long timestamp, byte[] id) {
        public Item {
            Objects.requireNonNull(id, "id");
            if (id.length != ID_SIZE) {
                throw new IllegalArgumentException("an id is " + ID_SIZE + " bytes, got " + id.length);
            }
            if (timestamp < 0 || timestamp == INFINITY) {
                throw new IllegalArgumentException("a timestamp is in [0, 2^63 - 1), got " + timestamp);
            }
        }
    }

    /** A sealed, sorted set of items: by timestamp, then by id as unsigned bytes. */
    public static final class Storage {
        private final long[] timestamps;
        private final byte[] ids;
        private final int size;

        private Storage(long[] timestamps, byte[] ids, int size) {
            this.timestamps = timestamps;
            this.ids = ids;
            this.size = size;
        }

        /**
         * Seals a set of items.
         *
         * @param items the items, in any order
         * @return the storage
         * @throws IllegalArgumentException on a duplicate item
         */
        public static Storage of(Collection<Item> items) {
            List<Item> sorted = new ArrayList<>(items);
            sorted.sort((a, b) -> a.timestamp() != b.timestamp() ? Long.compare(a.timestamp(), b.timestamp())
                    : Arrays.compareUnsigned(a.id(), b.id()));
            long[] timestamps = new long[sorted.size()];
            byte[] ids = new byte[sorted.size() * ID_SIZE];
            for (int i = 0; i < sorted.size(); i++) {
                Item item = sorted.get(i);
                if (i > 0 && timestamps[i - 1] == item.timestamp()
                        && Arrays.equals(ids, (i - 1) * ID_SIZE, i * ID_SIZE, item.id(), 0, ID_SIZE)) {
                    throw new IllegalArgumentException("duplicate item");
                }
                timestamps[i] = item.timestamp();
                System.arraycopy(item.id(), 0, ids, i * ID_SIZE, ID_SIZE);
            }
            return new Storage(timestamps, ids, sorted.size());
        }

        /** The items held. */
        public int size() {
            return size;
        }

        byte[] id(int index) {
            return Arrays.copyOfRange(ids, index * ID_SIZE, (index + 1) * ID_SIZE);
        }

        /** Whether item {@code index} sorts before a bound: an id that starts with the bound's prefix does not. */
        private boolean before(int index, Bound bound) {
            if (timestamps[index] != bound.timestamp) {
                return timestamps[index] < bound.timestamp;
            }
            return Arrays.compareUnsigned(ids, index * ID_SIZE, index * ID_SIZE + bound.prefix.length,
                    bound.prefix, 0, bound.prefix.length) < 0;
        }

        /** The first index in {@code [begin, end)} whose item does not sort before the bound, else {@code end}. */
        int lowerBound(int begin, int end, Bound bound) {
            int lo = begin;
            int hi = end;
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (before(mid, bound)) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            return lo;
        }

        /** The fingerprint of the items in {@code [begin, end)}. */
        byte[] fingerprint(int begin, int end) {
            long[] sum = new long[4];
            for (int i = begin; i < end; i++) {
                int at = i * ID_SIZE;
                long carry = 0;
                for (int word = 0; word < 4; word++) {
                    long add = littleEndian(ids, at + word * 8);
                    long before = sum[word];
                    long next = before + add + carry;
                    carry = (Long.compareUnsigned(next, before) < 0 || (carry == 1 && next == before)) ? 1 : 0;
                    sum[word] = next;
                }
            }
            ByteArrayOutputStream input = new ByteArrayOutputStream(ID_SIZE + 10);
            for (long word : sum) {
                for (int b = 0; b < 8; b++) {
                    input.write((int) (word >>> (8 * b)));
                }
            }
            writeVarInt(input, end - begin);
            return Arrays.copyOf(Digests.sha256(input.toByteArray()), FINGERPRINT_SIZE);
        }

        private static long littleEndian(byte[] bytes, int at) {
            long value = 0;
            for (int b = 7; b >= 0; b--) {
                value = (value << 8) | (bytes[at + b] & 0xffL);
            }
            return value;
        }
    }

    /** A range's upper bound: a timestamp and an id prefix of at most 32 bytes. */
    record Bound(long timestamp, byte[] prefix) {
        static Bound of(long timestamp) {
            return new Bound(timestamp, new byte[0]);
        }
    }

    private final Storage storage;
    private final int frameSizeLimit;
    private boolean initiator;
    private long lastTimestampIn;
    private long lastTimestampOut;

    /**
     * One side of a session.
     *
     * @param storage        the set this side holds
     * @param frameSizeLimit 0 for none, else at least {@value #MIN_FRAME_SIZE_LIMIT}
     */
    public Negentropy(Storage storage, int frameSizeLimit) {
        this.storage = Objects.requireNonNull(storage, "storage");
        if (frameSizeLimit != 0 && frameSizeLimit < MIN_FRAME_SIZE_LIMIT) {
            throw new IllegalArgumentException("frameSizeLimit is 0 or at least " + MIN_FRAME_SIZE_LIMIT + ", got " + frameSizeLimit);
        }
        this.frameSizeLimit = frameSizeLimit;
    }

    /**
     * The initiator's first message: the whole set split into ranges.
     *
     * @return the message
     * @throws IllegalStateException when this side initiated already
     */
    public byte[] initiate() {
        if (initiator) {
            throw new IllegalStateException("already initiated");
        }
        initiator = true;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(PROTOCOL_VERSION);
        splitRange(0, storage.size(), Bound.of(INFINITY), out);
        return out.toByteArray();
    }

    /**
     * The responder's answer to one message.
     *
     * @param query the message
     * @return the reply
     * @throws IllegalArgumentException when the message is malformed
     * @throws IllegalStateException    when this side is the initiator
     */
    public byte[] reconcile(byte[] query) {
        if (initiator) {
            throw new IllegalStateException("the initiator reconciles with have and need lists");
        }
        return answer(query, null, null);
    }

    /**
     * The initiator's handling of one reply.
     *
     * @param reply the reply
     * @param have  receives the ids this side holds and the other lacks
     * @param need  receives the ids the other side holds and this side lacks
     * @return the next message, or empty when the session is done
     * @throws IllegalArgumentException when the reply is malformed or names another protocol version
     * @throws IllegalStateException    when this side did not initiate
     */
    public Optional<byte[]> reconcile(byte[] reply, List<byte[]> have, List<byte[]> need) {
        if (!initiator) {
            throw new IllegalStateException("the responder reconciles without have and need lists");
        }
        Objects.requireNonNull(have, "have");
        Objects.requireNonNull(need, "need");
        byte[] out = answer(reply, have, need);
        return out.length == 1 ? Optional.empty() : Optional.of(out);
    }

    private byte[] answer(byte[] query, List<byte[]> have, List<byte[]> need) {
        Objects.requireNonNull(query, "query");
        Reader in = new Reader(query);
        lastTimestampIn = 0;
        lastTimestampOut = 0;
        ByteArrayOutputStream full = new ByteArrayOutputStream();
        full.write(PROTOCOL_VERSION);
        int version = in.next();
        if (version < 0x60 || version > 0x6f) {
            throw new IllegalArgumentException("invalid negentropy protocol version byte 0x" + Integer.toHexString(version));
        }
        if (version != PROTOCOL_VERSION) {
            if (initiator) {
                throw new IllegalArgumentException("unsupported negentropy protocol version " + (version - 0x60));
            }
            return full.toByteArray();
        }
        int size = storage.size();
        Bound prevBound = Bound.of(0);
        int prevIndex = 0;
        boolean skip = false;
        while (in.remaining() > 0) {
            ByteArrayOutputStream o = new ByteArrayOutputStream();
            Bound currBound = decodeBound(in);
            long mode = in.varInt();
            int lower = prevIndex;
            int upper = storage.lowerBound(prevIndex, size, currBound);
            if (mode == MODE_SKIP) {
                skip = true;
            } else if (mode == MODE_FINGERPRINT) {
                byte[] theirs = in.bytes(FINGERPRINT_SIZE);
                if (!Arrays.equals(theirs, storage.fingerprint(lower, upper))) {
                    if (skip) {
                        skip = false;
                        encodeBound(prevBound, o);
                        writeVarInt(o, MODE_SKIP);
                    }
                    splitRange(lower, upper, currBound, o);
                } else {
                    skip = true;
                }
            } else if (mode == MODE_ID_LIST) {
                long count = in.varInt();
                if (count > in.remaining() / ID_SIZE) {
                    throw new IllegalArgumentException("an id list of " + count + " ids runs past the message");
                }
                Map<String, byte[]> theirs = new LinkedHashMap<>();
                for (long i = 0; i < count; i++) {
                    byte[] id = in.bytes(ID_SIZE);
                    if (initiator) {
                        theirs.put(java.util.HexFormat.of().formatHex(id), id);
                    }
                }
                if (initiator) {
                    skip = true;
                    for (int i = lower; i < upper; i++) {
                        byte[] id = storage.id(i);
                        if (theirs.remove(java.util.HexFormat.of().formatHex(id)) == null) {
                            have.add(id);
                        }
                    }
                    need.addAll(theirs.values());
                } else {
                    if (skip) {
                        skip = false;
                        encodeBound(prevBound, o);
                        writeVarInt(o, MODE_SKIP);
                    }
                    ByteArrayOutputStream ids = new ByteArrayOutputStream();
                    int listed = 0;
                    Bound endBound = currBound;
                    for (int i = lower; i < upper; i++) {
                        if (exceeded(full.size() + ids.size())) {
                            endBound = new Bound(storage.timestamps[i], storage.id(i));
                            upper = i; // the rest of the range goes into the closing fingerprint
                            break;
                        }
                        ids.writeBytes(storage.id(i));
                        listed++;
                    }
                    encodeBound(endBound, o);
                    writeVarInt(o, MODE_ID_LIST);
                    writeVarInt(o, listed);
                    o.writeBytes(ids.toByteArray());
                    full.writeBytes(o.toByteArray());
                    o.reset();
                }
            } else {
                throw new IllegalArgumentException("unexpected mode " + mode);
            }
            if (exceeded(full.size() + o.size())) {
                byte[] rest = storage.fingerprint(upper, size);
                encodeBound(Bound.of(INFINITY), full);
                writeVarInt(full, MODE_FINGERPRINT);
                full.writeBytes(rest);
                break;
            }
            full.writeBytes(o.toByteArray());
            prevIndex = upper;
            prevBound = currBound;
        }
        return full.toByteArray();
    }

    private void splitRange(int lower, int upper, Bound upperBound, ByteArrayOutputStream o) {
        int count = upper - lower;
        if (count < BUCKETS * 2) {
            encodeBound(upperBound, o);
            writeVarInt(o, MODE_ID_LIST);
            writeVarInt(o, count);
            for (int i = lower; i < upper; i++) {
                o.writeBytes(storage.id(i));
            }
            return;
        }
        int perBucket = count / BUCKETS;
        int withExtra = count % BUCKETS;
        int curr = lower;
        for (int bucket = 0; bucket < BUCKETS; bucket++) {
            int bucketSize = perBucket + (bucket < withExtra ? 1 : 0);
            byte[] fingerprint = storage.fingerprint(curr, curr + bucketSize);
            curr += bucketSize;
            Bound next = curr == upper ? upperBound : minimalBound(curr - 1, curr);
            encodeBound(next, o);
            writeVarInt(o, MODE_FINGERPRINT);
            o.writeBytes(fingerprint);
        }
    }

    /** The shortest bound that separates item {@code prev} from item {@code curr}. */
    private Bound minimalBound(int prev, int curr) {
        if (storage.timestamps[curr] != storage.timestamps[prev]) {
            return Bound.of(storage.timestamps[curr]);
        }
        int shared = 0;
        while (shared < ID_SIZE && storage.ids[prev * ID_SIZE + shared] == storage.ids[curr * ID_SIZE + shared]) {
            shared++;
        }
        return new Bound(storage.timestamps[curr], Arrays.copyOfRange(storage.ids, curr * ID_SIZE, curr * ID_SIZE + shared + 1));
    }

    private boolean exceeded(int bytes) {
        return frameSizeLimit != 0 && bytes > frameSizeLimit - FRAME_MARGIN;
    }

    private Bound decodeBound(Reader in) {
        long timestamp = decodeTimestamp(in);
        long length = in.varInt();
        if (length > ID_SIZE) {
            throw new IllegalArgumentException("bound key too long: " + length + " bytes");
        }
        return new Bound(timestamp, in.bytes((int) length));
    }

    private long decodeTimestamp(Reader in) {
        long coded = in.varInt();
        long timestamp = coded == 0 ? INFINITY : coded - 1;
        if (lastTimestampIn == INFINITY || timestamp == INFINITY) {
            lastTimestampIn = INFINITY;
            return INFINITY;
        }
        if (timestamp >= INFINITY - lastTimestampIn) {
            throw new IllegalArgumentException("a bound's timestamp passes 2^63 - 1");
        }
        timestamp += lastTimestampIn;
        lastTimestampIn = timestamp;
        return timestamp;
    }

    private void encodeBound(Bound bound, ByteArrayOutputStream out) {
        if (bound.timestamp == INFINITY) {
            lastTimestampOut = INFINITY;
            writeVarInt(out, 0);
        } else {
            long offset = bound.timestamp - lastTimestampOut;
            lastTimestampOut = bound.timestamp;
            writeVarInt(out, offset + 1);
        }
        writeVarInt(out, bound.prefix.length);
        out.writeBytes(bound.prefix);
    }

    /** Writes a varint: base 128, most significant group first, the top bit set on every byte but the last. */
    static void writeVarInt(ByteArrayOutputStream out, long value) {
        if (value < 0) {
            throw new IllegalArgumentException("a varint is not negative, got " + value);
        }
        if (value == 0) {
            out.write(0);
            return;
        }
        byte[] groups = new byte[10];
        int n = 0;
        while (value != 0) {
            groups[n++] = (byte) (value & 0x7f);
            value >>>= 7;
        }
        for (int i = n - 1; i >= 0; i--) {
            out.write(i == 0 ? groups[i] : groups[i] | 0x80);
        }
    }

    /** A cursor over a message. */
    static final class Reader {
        private final byte[] bytes;
        private int at;

        Reader(byte[] bytes) {
            this.bytes = bytes;
        }

        int remaining() {
            return bytes.length - at;
        }

        int next() {
            if (at >= bytes.length) {
                throw new IllegalArgumentException("the message ends prematurely");
            }
            return bytes[at++] & 0xff;
        }

        byte[] bytes(int n) {
            if (n > remaining()) {
                throw new IllegalArgumentException("the message ends prematurely");
            }
            byte[] out = Arrays.copyOfRange(bytes, at, at + n);
            at += n;
            return out;
        }

        long varInt() {
            long value = 0;
            while (true) {
                int b = next();
                if (value > (Long.MAX_VALUE >>> 7)) {
                    throw new IllegalArgumentException("a varint passes 63 bits");
                }
                value = (value << 7) | (b & 0x7f);
                if ((b & 0x80) == 0) {
                    return value;
                }
            }
        }
    }
}
