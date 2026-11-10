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
package ai.badmonkey.agentspaces.pdt;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/**
 * One datom of a {@code dn} node (PDT 2 phase 0, P2-D-01; srad/agentspaces-pdt2.md §3.1.1): on the
 * wire the definite array {@code [e, a, v, tx]}, with the entity and the transaction as CBOR
 * integers (a negative transaction marks a retraction), the attribute as the keyword's print form
 * ({@code ":ns/name"}), and the value under the grammar of §3.1.1. {@link NodeCodec} reads and
 * writes the array; this record holds one decoded datom and refuses a value outside the grammar.
 *
 * <p>The value is one of: a {@link Long} (an integer, or a ref under a {@code :db.type/ref}
 * attribute), a {@link String} (text, or a keyword as its print form), a {@link Boolean}, a
 * {@code byte[]}, a finite {@link Double} (always float64 on the wire), a {@link Link} (a value
 * spilled out of line, PD-N-05), or a {@link Tagged} pair for a UUID, an instant, or a big integer.
 *
 * @param e  the entity id
 * @param a  the attribute, the keyword's print form
 * @param v  the value
 * @param tx the transaction id, negative for a retraction and never 0
 */
public record Datom(long e, String a, Object v, long tx) {

    /** Reads an {@link Integer} value as the {@link Long} it encodes and refuses tx 0 and a value outside the grammar. */
    public Datom {
        Objects.requireNonNull(a, "a");
        if (tx == 0) {
            // PDT 2 phase 0 (P2-D-01, §3.1.1): the sign of tx marks a retraction, so 0 has no retraction form
            throw new IllegalArgumentException("a datom's tx is non-zero: its sign marks a retraction");
        }
        if (v instanceof Integer i) {
            v = (long) i;
        }
        checkValue(v);
    }

    private static void checkValue(Object v) {
        if (v instanceof Double d) {
            if (!Double.isFinite(d)) {
                throw new IllegalArgumentException("a datom float is finite, found " + d);
            }
            return;
        }
        if (v instanceof Long || v instanceof String || v instanceof Boolean || v instanceof byte[]
                || v instanceof Link || v instanceof Tagged) {
            return;
        }
        throw new IllegalArgumentException("a datom value is an integer, text, a boolean, bytes, a float64, a link, "
                + "or a tagged pair, found " + (v == null ? "null" : v.getClass().getSimpleName()));
    }

    /**
     * A tagged datom value, {@code [tag, inner]} on the wire (§3.1.1): {@link #UUID} over the 16
     * bytes of a UUID, {@link #INSTANT} over epoch milliseconds, {@link #BIGINT} over the minimal
     * big-endian two's complement bytes of a big integer ({@link BigInteger#toByteArray()}).
     *
     * @param tag   {@link #UUID}, {@link #INSTANT}, or {@link #BIGINT}
     * @param inner a {@code byte[]} for {@code u} and {@code n}, a {@link Long} for {@code i}
     */
    public record Tagged(String tag, Object inner) {

        /** The tag of a UUID: 16 bytes, most significant first. */
        public static final String UUID = "u";

        /** The tag of an instant: epoch milliseconds. */
        public static final String INSTANT = "i";

        /** The tag of a big integer: minimal two's complement bytes. */
        public static final String BIGINT = "n";

        /** Checks the inner item against the tag. */
        public Tagged {
            Objects.requireNonNull(tag, "tag");
            if (inner instanceof Integer i) {
                inner = (long) i;
            }
            switch (tag) {
                case UUID -> {
                    if (!(inner instanceof byte[] b) || b.length != 16) {
                        throw new IllegalArgumentException("a uuid value holds 16 bytes");
                    }
                }
                case INSTANT -> {
                    if (!(inner instanceof Long)) {
                        throw new IllegalArgumentException("an instant value holds an integer of epoch milliseconds");
                    }
                }
                case BIGINT -> {
                    if (!(inner instanceof byte[] b) || !minimal(b)) {
                        throw new IllegalArgumentException("a bigint value holds its minimal two's complement bytes");
                    }
                }
                default -> throw new IllegalArgumentException("unknown value tag '" + tag + "'");
            }
        }

        /** The tagged form of a UUID. */
        public static Tagged uuid(java.util.UUID uuid) {
            return new Tagged(UUID, ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits())
                    .putLong(uuid.getLeastSignificantBits()).array());
        }

        /** The tagged form of an instant in epoch milliseconds. */
        public static Tagged instant(long epochMillis) {
            return new Tagged(INSTANT, epochMillis);
        }

        /** The tagged form of a big integer. */
        public static Tagged bigint(BigInteger value) {
            return new Tagged(BIGINT, value.toByteArray());
        }

        /** Whether bytes are what {@link BigInteger#toByteArray()} writes: non-empty, no redundant sign byte. */
        private static boolean minimal(byte[] b) {
            if (b.length == 0) {
                return false;
            }
            if (b.length == 1) {
                return true;
            }
            return !(b[0] == 0 && b[1] >= 0) && !(b[0] == -1 && b[1] < 0);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Tagged t && tag.equals(t.tag) && sameValue(inner, t.inner);
        }

        @Override
        public int hashCode() {
            return 31 * tag.hashCode() + valueHash(inner);
        }

        @Override
        public String toString() {
            return "[" + tag + " " + (inner instanceof byte[] b ? java.util.HexFormat.of().formatHex(b) : inner) + "]";
        }
    }

    private static boolean sameValue(Object a, Object b) {
        return a instanceof byte[] x && b instanceof byte[] y ? Arrays.equals(x, y) : Objects.equals(a, b);
    }

    private static int valueHash(Object v) {
        return v instanceof byte[] b ? Arrays.hashCode(b) : Objects.hashCode(v);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Datom d && e == d.e && tx == d.tx && a.equals(d.a) && sameValue(v, d.v);
    }

    @Override
    public int hashCode() {
        return Objects.hash(e, a, tx) * 31 + valueHash(v);
    }

    @Override
    public String toString() {
        return "[" + e + " " + a + " " + (v instanceof byte[] b ? java.util.HexFormat.of().formatHex(b) : v) + " " + tx + "]";
    }
}
