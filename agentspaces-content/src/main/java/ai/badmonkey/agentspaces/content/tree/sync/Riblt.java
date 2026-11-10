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

import java.io.ByteArrayOutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;

/**
 * Rateless invertible Bloom lookup tables (PDT 2 phase 5, P2-K-05; srad/agentspaces-pdt2.md §3.6; Yang, Gilad and
 * Alizadeh, "Practical Rateless Set Reconciliation", SIGCOMM 2024): the one-round sketch for small differences. A
 * side streams coded symbols of its set; the other subtracts its own symbols at the same indexes and peels the
 * difference out as the symbols arrive, in one pass, finishing once symbol 0, which every id maps to, is empty.
 *
 * <p>A coded symbol holds the XOR of the 32-byte ids mapped to it ({@code sum}), the XOR of their checksums, and
 * their count. An id's checksum is SipHash-2-4 of the id under the session's 16-byte key, so a member cannot aim
 * collisions at another session. An id maps to symbol 0 and then to the indexes a generator seeded by its checksum
 * draws, with the density of α = 0.5 (the probability of index i is about 1 / (1 + i / 2)): with {@code r} the state
 * times {@code 0xda942042e4dd58b5} modulo 2^64, the next index adds {@code max(1, ceil((i + 1.5) * (2^32 / sqrt(x + 1)
 * - 1)))} to the last, where {@code x} is {@code r} with its low 11 bits cleared, as a double; an index past 2^62
 * ends the mapping. Every port computes the same doubles, since IEEE 754 rounds the square root and the division
 * exactly.
 *
 * <p>On the wire a symbol is the sum, the checksum as 8 little-endian bytes, and the count as a negentropy varint. A
 * sketch query is {@link #ENCODING}, the key, the index of its first symbol and the symbol count as varints, and the
 * symbols; a reply is {@link #ENCODING} and a status: {@link #MORE}, {@link #DECODED} followed by the ids the asker
 * holds that the other lacks and then the ids the other holds that the asker lacks, each list a varint count and the
 * ids, or {@link #TOO_MANY} when the difference does not fit one reply or the session reached its symbol cap.
 */
public final class Riblt {

    /** The leading byte of a RIBLT sketch (P2-F-01). */
    public static final int ENCODING = 0x62;
    /** The bytes of a session key. */
    public static final int KEY_BYTES = 16;
    /** A reply status: not decoded yet, send more symbols. */
    public static final int MORE = 0;
    /** A reply status: decoded, the two lists follow. */
    public static final int DECODED = 1;
    /** A reply status: the difference does not fit, fall back to negentropy. */
    public static final int TOO_MANY = 2;

    static final int ID_SIZE = Negentropy.ID_SIZE;
    private static final long MULTIPLIER = 0xda942042e4dd58b5L;
    /** An index past this ends a mapping. */
    static final long LAST_INDEX = 1L << 62;

    private Riblt() {
    }

    /**
     * One coded symbol as it travels.
     *
     * @param sum      the XOR of the ids mapped to it
     * @param checksum the XOR of their checksums
     * @param count    how many ids map to it
     */
    public record Symbol(byte[] sum, long checksum, long count) {
        public Symbol {
            Objects.requireNonNull(sum, "sum");
            if (sum.length != ID_SIZE || count < 0) {
                throw new IllegalArgumentException("a symbol holds a 32-byte sum and a count of at least 0");
            }
        }

        /** Writes the wire form. */
        public void write(ByteArrayOutputStream out) {
            out.writeBytes(sum);
            for (int b = 0; b < 8; b++) {
                out.write((int) (checksum >>> (8 * b)));
            }
            Negentropy.writeVarInt(out, count);
        }

        /** Reads one wire form. */
        static Symbol read(Negentropy.Reader in) {
            byte[] sum = in.bytes(ID_SIZE);
            long checksum = 0;
            for (int b = 0; b < 8; b++) {
                checksum |= (long) in.next() << (8 * b);
            }
            return new Symbol(sum, checksum, in.varInt());
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Symbol s && checksum == s.checksum && count == s.count && Arrays.equals(sum, s.sum);
        }

        @Override
        public int hashCode() {
            return 31 * Arrays.hashCode(sum) + Long.hashCode(checksum) * 7 + Long.hashCode(count);
        }
    }

    /** SipHash-2-4 of a message under a 16-byte key (Aumasson and Bernstein, 2012). */
    static long sipHash24(byte[] key, byte[] message) {
        long k0 = littleEndian(key, 0, 8);
        long k1 = littleEndian(key, 8, 8);
        long[] v = {k0 ^ 0x736f6d6570736575L, k1 ^ 0x646f72616e646f6dL, k0 ^ 0x6c7967656e657261L, k1 ^ 0x7465646279746573L};
        int whole = message.length - message.length % 8;
        for (int at = 0; at < whole; at += 8) {
            long m = littleEndian(message, at, 8);
            v[3] ^= m;
            sipRound(v);
            sipRound(v);
            v[0] ^= m;
        }
        long last = ((long) message.length << 56) | littleEndian(message, whole, message.length - whole);
        v[3] ^= last;
        sipRound(v);
        sipRound(v);
        v[0] ^= last;
        v[2] ^= 0xff;
        for (int i = 0; i < 4; i++) {
            sipRound(v);
        }
        return v[0] ^ v[1] ^ v[2] ^ v[3];
    }

    private static void sipRound(long[] v) {
        v[0] += v[1];
        v[1] = Long.rotateLeft(v[1], 13) ^ v[0];
        v[0] = Long.rotateLeft(v[0], 32);
        v[2] += v[3];
        v[3] = Long.rotateLeft(v[3], 16) ^ v[2];
        v[0] += v[3];
        v[3] = Long.rotateLeft(v[3], 21) ^ v[0];
        v[2] += v[1];
        v[1] = Long.rotateLeft(v[1], 17) ^ v[2];
        v[2] = Long.rotateLeft(v[2], 32);
    }

    private static long littleEndian(byte[] bytes, int at, int n) {
        long value = 0;
        for (int b = n - 1; b >= 0; b--) {
            value = (value << 8) | (bytes[at + b] & 0xffL);
        }
        return value;
    }

    private static byte[] checkedKey(byte[] key) {
        Objects.requireNonNull(key, "key");
        if (key.length != KEY_BYTES) {
            throw new IllegalArgumentException("a session key is " + KEY_BYTES + " bytes, got " + key.length);
        }
        return key.clone();
    }

    /** The indexes one id maps to, in order, with the sign it carries in a decoder. */
    private static final class Mapping {
        final byte[] id;
        final long checksum;
        final int sign;
        long state;
        long index;

        Mapping(byte[] id, long checksum, int sign) {
            this.id = id;
            this.checksum = checksum;
            this.sign = sign;
            this.state = checksum;
        }

        void advance() {
            state *= MULTIPLIER;
            double x = (double) (state >>> 11) * 0x1p11;
            double step = Math.max(1, Math.ceil((index + 1.5) * (0x1p32 / Math.sqrt(x + 1) - 1)));
            double next = index + step;
            index = next >= LAST_INDEX ? Long.MAX_VALUE : (long) next;
        }
    }

    /** A symbol a decoder works on: the remote symbol less the local one, so its count may be negative. */
    private static final class Cell {
        final byte[] sum = new byte[ID_SIZE];
        long checksum;
        long count;

        void xor(byte[] id, long sum2, long delta) {
            for (int i = 0; i < ID_SIZE; i++) {
                sum[i] ^= id[i];
            }
            checksum ^= sum2;
            count += delta;
        }

        boolean empty() {
            if (count != 0 || checksum != 0) {
                return false;
            }
            for (byte b : sum) {
                if (b != 0) {
                    return false;
                }
            }
            return true;
        }
    }

    /** Produces the coded symbols of a set, index after index. */
    public static final class Encoder {
        private final PriorityQueue<Mapping> queue = new PriorityQueue<>((a, b) -> Long.compare(a.index, b.index));
        private long produced;

        /**
         * An encoder.
         *
         * @param key the session key
         * @param ids the set's 32-byte ids
         */
        public Encoder(byte[] key, Collection<byte[]> ids) {
            byte[] k = checkedKey(key);
            for (byte[] id : ids) {
                if (id.length != ID_SIZE) {
                    throw new IllegalArgumentException("an id is " + ID_SIZE + " bytes, got " + id.length);
                }
                queue.add(new Mapping(id, sipHash24(k, id), 1));
            }
        }

        /** The index of the next symbol. */
        public long produced() {
            return produced;
        }

        /**
         * The next symbols.
         *
         * @param count how many
         * @return the symbols, in index order
         */
        public List<Symbol> next(int count) {
            List<Symbol> out = new ArrayList<>(count);
            for (int n = 0; n < count; n++) {
                Cell cell = new Cell();
                long at = produced++;
                while (!queue.isEmpty() && queue.peek().index == at) {
                    Mapping m = queue.poll();
                    cell.xor(m.id, m.checksum, 1);
                    m.advance();
                    if (m.index != Long.MAX_VALUE) {
                        queue.add(m);
                    }
                }
                out.add(new Symbol(cell.sum, cell.checksum, cell.count));
            }
            return out;
        }
    }

    /** Subtracts the local set's symbols from a remote stream and peels the difference out. */
    public static final class Decoder {
        private final byte[] key;
        private final Encoder local;
        private final List<Cell> cells = new ArrayList<>();
        private final PriorityQueue<Mapping> recovered = new PriorityQueue<>((a, b) -> Long.compare(a.index, b.index));
        private final List<byte[]> remoteOnly = new ArrayList<>();
        private final List<byte[]> localOnly = new ArrayList<>();

        /**
         * A decoder.
         *
         * @param key the session key the remote side encodes with
         * @param ids this side's 32-byte ids
         */
        public Decoder(byte[] key, Collection<byte[]> ids) {
            this.key = checkedKey(key);
            this.local = new Encoder(this.key, ids);
        }

        /**
         * Adds the next remote symbol and peels what it frees.
         *
         * @throws IllegalArgumentException when the stream peels more ids than it has symbols, which no coding of a
         *                                  set does: a remote side that knows the key can forge a pure symbol whose id
         *                                  its other indexes lack, and the id would peel back and forth for ever (PDT 2
         *                                  phase 5 adversary, P2-Q-SEC-04); the decoder is then unusable
         */
        public void add(Symbol remote) {
            Symbol mine = local.next(1).get(0);
            Cell cell = new Cell();
            cell.xor(remote.sum(), remote.checksum(), remote.count());
            cell.xor(mine.sum(), mine.checksum(), -mine.count());
            int at = cells.size();
            while (!recovered.isEmpty() && recovered.peek().index == at) {
                Mapping m = recovered.poll();
                cell.xor(m.id, m.checksum, -m.sign);
                m.advance();
                if (m.index != Long.MAX_VALUE) {
                    recovered.add(m);
                }
            }
            cells.add(cell);
            peel(at);
        }

        private void peel(int start) {
            Deque<Integer> pending = new ArrayDeque<>();
            pending.push(start);
            while (!pending.isEmpty()) {
                Cell cell = cells.get(pending.pop());
                if ((cell.count != 1 && cell.count != -1) || sipHash24(key, cell.sum) != cell.checksum) {
                    continue;
                }
                if (remoteOnly.size() + localOnly.size() >= cells.size()) {
                    // each symbol of a true coding frees one id at most, and then stays empty
                    throw new IllegalArgumentException("the stream peels more ids than its " + cells.size() + " symbols");
                }
                Mapping m = new Mapping(cell.sum.clone(), cell.checksum, (int) cell.count);
                (m.sign == 1 ? remoteOnly : localOnly).add(m.id);
                while (m.index < cells.size()) {
                    cells.get((int) m.index).xor(m.id, m.checksum, -m.sign);
                    pending.push((int) m.index);
                    m.advance();
                }
                if (m.index != Long.MAX_VALUE) {
                    recovered.add(m);
                }
            }
        }

        /** Whether the difference is decoded: symbol 0, which every id maps to, is empty. */
        public boolean decoded() {
            return !cells.isEmpty() && cells.get(0).empty();
        }

        /** The remote symbols added. */
        public int symbols() {
            return cells.size();
        }

        /** The ids the remote side holds and this side lacks, decoded so far. */
        public List<byte[]> remoteOnly() {
            return List.copyOf(remoteOnly);
        }

        /** The ids this side holds and the remote side lacks, decoded so far. */
        public List<byte[]> localOnly() {
            return List.copyOf(localOnly);
        }
    }
}
