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
package ai.badmonkey.agentspaces.agent.content;

import ai.badmonkey.agentspaces.common.id.PeerId;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * One member's statement of which pieces of one root it holds
 * (srad/large-networks-bulk-data.md §3.2): the provider record. Written short-leased
 * to the content space and refreshed by the holder; tagged {@link #ROOT_TAG} and
 * {@link #HOLDER_TAG}. Named {@code ai.badmonkey.agentspaces.agent.content.Holding#v1}
 * on the wire.
 *
 * @param v        schema version, 1
 * @param root     the manifest root
 * @param holder   the holder, redundant with the issuer and kept for template matching
 * @param complete whether every piece is held
 * @param have     run-length encoded bitfield over pieces ({@link #encodeHave}); null when complete or when it would exceed {@link #MAX_HAVE_BYTES}
 * @param count    pieces held
 * @param via      relays this holder is attached to, from its advertisement reach hint; null when it listens
 * @param cost     optional cost hints in the CapabilityAdvertisement vocabulary
 */
public record Holding(int v, String root, PeerId holder, boolean complete, byte[] have, int count,
                      List<PeerId> via, Map<String, String> cost) {

    /** Schema version this record carries. */
    public static final int VERSION = 1;

    /** Bound on the encoded bitfield; a larger one is published as null with {@code count} (LB-H-06). */
    public static final int MAX_HAVE_BYTES = 8 * 1024;

    public static final String ROOT_TAG = "root";
    public static final String HOLDER_TAG = "holder";

    public Holding {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(holder, "holder");
        via = via == null ? null : List.copyOf(via);
        cost = cost == null ? null : Map.copyOf(cost);
    }

    /** Builds a holding from a bitfield, encoding it or dropping it per the bound. */
    public static Holding of(String root, PeerId holder, BitSet have, int pieces, List<PeerId> via) {
        int count = have.cardinality();
        boolean complete = pieces > 0 && count >= pieces;
        byte[] encoded = complete ? null : encodeHave(have, pieces);
        if (encoded != null && encoded.length > MAX_HAVE_BYTES) {
            encoded = null;
        }
        return new Holding(VERSION, root, holder, complete, encoded, count, via, null);
    }

    /** The tags a holding entry is written with. */
    public Map<String, String> tags() {
        return Map.of(ROOT_TAG, root, HOLDER_TAG, holder.value());
    }

    /**
     * Encodes a bitfield as alternating runs, zeros first, each run a varint
     * (7 bits per byte, high bit continues), starting at bit 0.
     */
    public static byte[] encodeHave(BitSet have, int pieces) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        int i = 0;
        boolean value = false;
        while (i < pieces) {
            int run = 0;
            while (i < pieces && have.get(i) == value) {
                run++;
                i++;
            }
            writeVarint(out, run);
            value = !value;
        }
        return out.toByteArray();
    }

    /**
     * Decodes {@link #encodeHave}; a null field decodes as "every piece" when the
     * holding is complete and as "unknown, treat as a candidate for every piece"
     * otherwise, which this method also answers with every bit set.
     */
    public BitSet decodeHave(int pieces) {
        BitSet bits = new BitSet(pieces);
        if (have == null) {
            bits.set(0, pieces);
            return bits;
        }
        int pos = 0;
        int i = 0;
        boolean value = false;
        while (pos < have.length && i < pieces) {
            int run = 0;
            int shift = 0;
            while (pos < have.length) {
                int b = have[pos++] & 0xff;
                run |= (b & 0x7f) << shift;
                shift += 7;
                if ((b & 0x80) == 0) {
                    break;
                }
            }
            int end = Math.min(pieces, i + run);
            if (value) {
                bits.set(i, end);
            }
            i = end;
            value = !value;
        }
        return bits;
    }

    private static void writeVarint(java.io.ByteArrayOutputStream out, int value) {
        while ((value & ~0x7f) != 0) {
            out.write((value & 0x7f) | 0x80);
            value >>>= 7;
        }
        out.write(value);
    }
}
