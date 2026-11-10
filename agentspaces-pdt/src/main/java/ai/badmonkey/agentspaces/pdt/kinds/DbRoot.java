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
package ai.badmonkey.agentspaces.pdt.kinds;

import ai.badmonkey.agentspaces.pdt.Embedded;
import ai.badmonkey.agentspaces.pdt.Kind;
import ai.badmonkey.agentspaces.pdt.Link;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeFormatException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The root of a DataScript database, kind {@code db} (PDT 2 phase 0, P2-D-01;
 * srad/agentspaces-pdt2.md §3.1.1):
 * {@code { k:"db", v: 1, s: schema, me: max-eid, mt: max-tx, bf: branching-factor,
 * i: { aevt: link, avet: link, eavt: link }, t: tail link or null }}.
 *
 * <p>The build pins the key §3.1.1 lacked for the tail that §3.1.3 links (§10.4, phase 0): {@code t}
 * is always present and links the {@code dn} tail node, whether the tail travels inline on the head
 * or by request, and is null when every datom sits in the indexes. A head's inline tail is therefore
 * reachable from the root, which every inline delta must be ({@code InlineDelta.accept}).
 *
 * @param schema    the database schema, an application map with keyword keys as their print form, or a
 *                  spilled link to one
 * @param maxEid    the largest entity id the database has issued
 * @param maxTx     the largest transaction id
 * @param branching the branching factor of the indexes, {@link #BRANCHING} by decision 2
 * @param eavt      the root of the EAVT index
 * @param aevt      the root of the AEVT index
 * @param avet      the root of the AVET index
 * @param tail      the tail node, or null
 */
public record DbRoot(Embedded schema, long maxEid, long maxTx, long branching, Link eavt, Link aevt, Link avet,
                     Link tail) {

    /** The version this release writes and reads. */
    public static final long VERSION = 1;

    /** The branching factor of decision 2. */
    public static final long BRANCHING = 128;

    private static final List<String> REQUIRED = List.of("v", "s", "me", "mt", "bf", "i", "t");

    private static final int MAJOR_TEXT = 3;
    private static final int MAJOR_MAP = 5;
    private static final int INDEFINITE = 31;
    private static final int BREAK = 0xff;

    /** Checks the fields. */
    public DbRoot {
        Objects.requireNonNull(schema, "schema");
        Objects.requireNonNull(eavt, "eavt");
        Objects.requireNonNull(aevt, "aevt");
        Objects.requireNonNull(avet, "avet");
        if (maxEid < 0 || maxTx < 0) {
            throw new IllegalArgumentException("max-eid and max-tx are non-negative");
        }
        if (branching < 2) {
            throw new IllegalArgumentException("a db root's branching factor is at least 2, found " + branching);
        }
        checkSchema(schema.bytes());
    }

    /**
     * The schema is a map whose keys are text, the keywords' print forms, or a spilled link to one
     * (PDT 2 phase 0, P2-D-01; §3.1.1), so every reader finds the schema it expects.
     */
    private static void checkSchema(byte[] item) {
        if (NodeCodec.isLinkShaped(item)) {
            return;
        }
        if (NodeCodec.skip(item, 0) != item.length) {
            throw new IllegalArgumentException("a db schema is one CBOR item, found trailing bytes");
        }
        int initial = item[0] & 0xff;
        if (initial >> 5 != MAJOR_MAP) {
            throw new IllegalArgumentException("a db schema is a map, found CBOR major type " + (initial >> 5));
        }
        // the item is whole, so a definite map holds its count of entries and an indefinite one ends at BREAK
        int p = 1;
        long entries = Long.MAX_VALUE;
        if ((initial & 0x1f) != INDEFINITE) {
            long[] header = argument(item, initial);
            entries = header[0];
            p = (int) header[1];
        }
        for (long i = 0; i < entries && (item[p] & 0xff) != BREAK; i++) {
            if ((item[p] & 0xff) >> 5 != MAJOR_TEXT) {
                throw new IllegalArgumentException("a db schema's keys are text, found CBOR major type " + ((item[p] & 0xff) >> 5));
            }
            p = NodeCodec.skip(item, NodeCodec.skip(item, p));
        }
    }

    /** The count of a definite map header and the offset after it; the item already passed the wire check. */
    private static long[] argument(byte[] item, int initial) {
        int info = initial & 0x1f;
        if (info < 24) {
            return new long[]{info, 1};
        }
        int width = 1 << (info - 24);
        long value = 0;
        for (int i = 1; i <= width; i++) {
            value = value << 8 | (item[i] & 0xff);
        }
        return new long[]{value, 1 + width};
    }

    /** The canonical node bytes. */
    public byte[] encode() {
        Map<String, Object> index = new LinkedHashMap<>();
        index.put("eavt", eavt);
        index.put("aevt", aevt);
        index.put("avet", avet);
        Map<String, Object> node = new LinkedHashMap<>();
        node.put(NodeCodec.KIND_KEY, Kind.DB);
        node.put("v", VERSION);
        node.put("s", schema);
        node.put("me", maxEid);
        node.put("mt", maxTx);
        node.put("bf", branching);
        node.put("i", index);
        node.put("t", tail);
        return NodeCodec.encode(node);
    }

    /**
     * Reads a {@code db} root.
     *
     * @param cid   the CID, named in a refusal; may be null
     * @param bytes the node bytes
     * @return the root
     * @throws NodeFormatException on another kind, another version, a missing key, or a broken field
     */
    public static DbRoot decode(String cid, byte[] bytes) {
        Map<String, Object> node = Nodes.decode(cid, bytes, Kind.DB, REQUIRED);
        if (!Long.valueOf(VERSION).equals(node.get("v"))) {
            throw new NodeFormatException(cid, "db root version " + node.get("v") + "; this release reads version " + VERSION);
        }
        Map<?, ?> index = (Map<?, ?>) node.get("i");
        return Nodes.build(cid, () -> new DbRoot((Embedded) node.get("s"), (Long) node.get("me"), (Long) node.get("mt"),
                (Long) node.get("bf"), (Link) index.get("eavt"), (Link) index.get("aevt"), (Link) index.get("avet"),
                (Link) node.get("t")));
    }
}
