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

import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.content.tree.Cids;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The self-contained canonical codec of a structured node (srad/agentspaces-pdt.md
 * §3.1; PD-N-01, PD-N-03, PD-N-04). A node is a CBOR map with text keys, encoded in
 * the RFC 8949 deterministic form: definite lengths, shortest integer forms, no
 * floats, and keys sorted by length then bytes (the DAG-CBOR order; every key is
 * short ASCII, so this equals the RFC 8949 §4.2.1 order). A link is tag 42 over a
 * byte string of {@code 0x00} and the binary CID. Application keys and values are
 * {@link Embedded} items: the bytes {@code CborCodec.toBytes} produced, spliced in
 * unchanged, and recovered by skipping one CBOR item. The node's own layer is strict
 * DAG-CBOR, and any RFC 8949 decoder reads every node. An embedded item keeps the
 * form {@code CborCodec} gives it, which is an indefinite-length map ({@code bf..ff})
 * for a map value and a 32-bit float for a float value; a strict DAG-CBOR decoder such
 * as {@code @ipld/dag-cbor} therefore accepts a node only when every embedded item is
 * a scalar, a text or byte string, or a definite array of those (§10.2, PD-N-03).
 * Both directions refuse an embedded item that is not a document in the wire encoding
 * (rules S1 to S12, the big-number tags 2, 3 and 4 included, no other tag), so every
 * implementation reads each application key and value to the same value (HLC round
 * six, 2026-11-08, owner ruling F2 and change JD-4).
 *
 * <p>A bare link item (tag 42 over {@code 0x00} and a binary CID) in a value slot
 * names a spilled node (PD-N-05). The rule is total: an application value is never
 * a bare link item, the values layer refuses one on the way in, and the decoder
 * counts every such item as a link of the node against {@link #MAX_LINKS}.
 *
 * <p>A node is a {@code Map<String, Object>} whose values are {@link Long} (or an
 * {@link Integer} on the way in), {@link String}, {@code byte[]}, {@link Boolean},
 * {@code null}, {@link List}, {@link Link}, or {@link Embedded}. Each kind of
 * {@link Kind} has a schema that names its keys and their types; both directions
 * refuse a key outside the schema, a value of the wrong type, an unknown or
 * reserved kind, a node over {@link #MAX_NODE_BYTES}, more than {@link #MAX_LINKS}
 * links, or a link whose codec or multihash is unknown, and every refusal is a
 * {@link NodeFormatException} naming the rule and the CID when known.
 *
 * <p>Schemas (§3.2, §3.3, PD-N-05):
 * <ul>
 *   <li>{@code m}: {@code d} uint, {@code n} uint, {@code e} [embedded...], {@code c} [link...], {@code cnt} uint (root only)</li>
 *   <li>{@code mc}: {@code e} [embedded...]</li>
 *   <li>{@code v}: {@code c} [link...] or {@code e} [embedded...]</li>
 *   <li>{@code vr}: {@code n} uint, {@code s} uint, {@code r} link or null, {@code t} [embedded...]</li>
 *   <li>{@code b}: {@code h} uint, {@code c} [link...], {@code z} [uint...]</li>
 *   <li>{@code x}: {@code v} embedded</li>
 *   <li>{@code sn}: {@code ct} bytes, {@code ep} uint, {@code ln} [link...], {@code nc} bytes (the sealed
 *       node envelope of §3.8, PD-P-05: the ciphertext and tag, the key epoch, the links of the
 *       plaintext node in the clear, and the 12-byte nonce; bounded by {@link #MAX_SEALED_NODE_BYTES})</li>
 * </ul>
 *
 * <p>PDT 2 phase 0 (srad/agentspaces-pdt2.md §3) adds the seven level-2 kinds, whose fields are
 * strict DAG-CBOR down to every item: no embedded application item except a {@code db} root's
 * schema and the elements of an {@code rr} tail, which keep the rules above.
 * <ul>
 *   <li>{@code db} (P2-D-01): {@code v} uint, {@code s} embedded (the schema), {@code me} uint,
 *       {@code mt} uint, {@code bf} uint, {@code i} the index map {@code {aevt, avet, eavt}} of
 *       links in that order, {@code t} the tail link or null</li>
 *   <li>{@code dn} (P2-D-01): {@code l} uint, {@code x} text, {@code e} [datom...] ({@link Datom}),
 *       {@code c} [link...]</li>
 *   <li>{@code rr} (P2-R-01): as {@code vr}; {@code rv}: {@code c} [link...], {@code z} [uint...]</li>
 *   <li>{@code t} (P2-T-01): {@code r} [run...] ({@link TextRun}), {@code v} uint</li>
 *   <li>{@code vg} (P2-V-01): {@code r} CID text, {@code b} [CID text...], {@code i} text,
 *       {@code t} text, {@code p} [link...]</li>
 *   <li>{@code bo} (P2-B-02): {@code g} uint, {@code n} uint, {@code p} bytes, {@code c} [link...]</li>
 * </ul>
 * Their text is well-formed UTF-8 with no lone surrogate, their integers are shortest, and a
 * float is a finite float64; each typed reader in {@code ai.badmonkey.agentspaces.pdt.kinds}
 * checks the rules that span fields.
 */
public final class NodeCodec {

    /** The largest structured node, in bytes (PD-N-04). */
    public static final int MAX_NODE_BYTES = 1024 * 1024;

    /**
     * The largest sealed node envelope ({@code sn}): a plaintext node at {@link #MAX_NODE_BYTES}
     * plus the envelope's own fields, which at {@link #MAX_LINKS} links in the clear reach
     * about 42 KiB (PD-P-05).
     */
    public static final int MAX_SEALED_NODE_BYTES = MAX_NODE_BYTES + 48 * 1024;

    /** The most links one node may carry (PD-N-04). */
    public static final int MAX_LINKS = 1024;

    /** The deepest CBOR nesting inside one item, and the deepest tree (PD-N-04). */
    public static final int MAX_DEPTH = 64;

    /** The CBOR tag of a link (PD-N-03). */
    public static final int LINK_TAG = 42;

    /** The key that names a node's kind. */
    public static final String KIND_KEY = "k";

    private static final int MAJOR_UINT = 0;
    private static final int MAJOR_NEGATIVE = 1;
    private static final int MAJOR_BYTES = 2;
    private static final int MAJOR_TEXT = 3;
    private static final int MAJOR_ARRAY = 4;
    private static final int MAJOR_MAP = 5;
    private static final int MAJOR_TAG = 6;
    private static final int MAJOR_SIMPLE = 7;

    private static final int SIMPLE_FALSE = 0xf4;
    private static final int SIMPLE_TRUE = 0xf5;
    private static final int SIMPLE_NULL = 0xf6;
    private static final int BREAK = 0xff;

    /** The length of a link's byte string: a zero byte and the binary CID. */
    private static final int LINK_BYTES = 1 + Cids.BINARY_LENGTH;

    /** The field types a schema names. */
    private enum Field {
        UINT, LINK, LINK_OR_NULL, LINKS, EMBEDDED, EMBEDDEDS, UINTS, BYTES,
        // PDT 2 phase 0 (P2-D-01, P2-T-01, P2-V-01): the strict field types of the level-2 kinds
        TEXT, CID, CIDS, INDEX, DATOMS, RUNS
    }

    /** The keys of a {@code db} root's index map, in DAG-CBOR order (PDT 2 phase 0, P2-D-01). */
    private static final List<String> INDEX_KEYS = List.of("aevt", "avet", "eavt");

    private static final Map<String, Map<String, Field>> SCHEMAS = Map.ofEntries(
            Map.entry(Kind.M, Map.of("d", Field.UINT, "n", Field.UINT, "e", Field.EMBEDDEDS, "c", Field.LINKS, "cnt", Field.UINT)),
            Map.entry(Kind.MC, Map.of("e", Field.EMBEDDEDS)),
            Map.entry(Kind.V, Map.of("c", Field.LINKS, "e", Field.EMBEDDEDS)),
            Map.entry(Kind.VR, Map.of("n", Field.UINT, "s", Field.UINT, "r", Field.LINK_OR_NULL, "t", Field.EMBEDDEDS)),
            Map.entry(Kind.B, Map.of("h", Field.UINT, "c", Field.LINKS, "z", Field.UINTS)),
            Map.entry(Kind.X, Map.of("v", Field.EMBEDDED)),
            Map.entry(Kind.SN, Map.of("ct", Field.BYTES, "ep", Field.UINT, "ln", Field.LINKS, "nc", Field.BYTES)),
            // PDT 2 phase 0 (srad/agentspaces-pdt2.md §3.1.1, §3.5, §3.7, §3.9, §3.10)
            Map.entry(Kind.DB, Map.of("v", Field.UINT, "s", Field.EMBEDDED, "me", Field.UINT, "mt", Field.UINT,
                    "bf", Field.UINT, "i", Field.INDEX, "t", Field.LINK_OR_NULL)),
            Map.entry(Kind.DN, Map.of("l", Field.UINT, "x", Field.TEXT, "e", Field.DATOMS, "c", Field.LINKS)),
            Map.entry(Kind.RR, Map.of("n", Field.UINT, "s", Field.UINT, "r", Field.LINK_OR_NULL, "t", Field.EMBEDDEDS)),
            Map.entry(Kind.RV, Map.of("c", Field.LINKS, "z", Field.UINTS)),
            Map.entry(Kind.T, Map.of("r", Field.RUNS, "v", Field.UINT)),
            Map.entry(Kind.VG, Map.of("r", Field.CID, "b", Field.CIDS, "i", Field.TEXT, "t", Field.TEXT, "p", Field.LINKS)),
            Map.entry(Kind.BO, Map.of("g", Field.UINT, "n", Field.UINT, "p", Field.BYTES, "c", Field.LINKS)));

    private NodeCodec() {
    }

    // ------------------------------------------------------------------ encode

    /**
     * Encodes a node to its canonical bytes.
     *
     * @param node the node: {@code k} and the keys of its kind's schema
     * @return the canonical DAG-CBOR bytes
     * @throws NodeFormatException on an unknown or reserved kind, a key outside the schema, a value
     *                             of the wrong type, more than 1,024 links, or more than 1 MiB
     */
    public static byte[] encode(Map<String, Object> node) {
        Objects.requireNonNull(node, "node");
        Map<String, Field> schema = schemaOf(kindIn(node));
        List<String> keys = new ArrayList<>(node.keySet());
        for (String key : keys) {
            if (key == null) {
                throw new NodeFormatException("a node key is null");
            }
            if (!KIND_KEY.equals(key) && !schema.containsKey(key)) {
                throw new NodeFormatException("key '" + key + "' outside the schema of kind '" + node.get(KIND_KEY) + "'");
            }
        }
        keys.sort(NodeCodec::compareKeys);
        Out out = new Out();
        int[] links = {0};
        out.header(MAJOR_MAP, keys.size());
        for (String key : keys) {
            out.text(key);
            Object value = node.get(key);
            if (KIND_KEY.equals(key)) {
                out.text((String) value);
            } else {
                writeField(out, key, schema.get(key), value, links, isKeyed(kindIn(node), key));
            }
        }
        int limit = limitFor(kindIn(node));
        if (out.size() > limit) {
            throw new NodeFormatException("node of " + out.size() + " bytes exceeds " + boundName(limit));
        }
        return out.toBytes();
    }

    /** The byte bound of a kind: {@link #MAX_SEALED_NODE_BYTES} for a sealed envelope, {@link #MAX_NODE_BYTES} otherwise. */
    public static int limitFor(String kind) {
        return Kind.SN.equals(kind) ? MAX_SEALED_NODE_BYTES : MAX_NODE_BYTES;
    }

    private static String boundName(int limit) {
        return limit == MAX_NODE_BYTES ? MAX_NODE_BYTES + " bytes (1 MiB)" : limit + " bytes (1 MiB plus the sealed envelope)";
    }

    private static String kindIn(Map<String, Object> node) {
        Object kind = node.get(KIND_KEY);
        if (kind == null) {
            throw new NodeFormatException("node has no '" + KIND_KEY + "' field");
        }
        if (!(kind instanceof String s)) {
            throw new NodeFormatException("'" + KIND_KEY + "' must be a text string");
        }
        return s;
    }

    private static Map<String, Field> schemaOf(String kind) {
        if (Kind.reserved(kind)) {
            throw new NodeFormatException("kind '" + kind + "' is reserved and has no schema in this release");
        }
        Map<String, Field> schema = SCHEMAS.get(kind);
        if (schema == null) {
            throw new NodeFormatException("unknown kind '" + kind + "'");
        }
        return schema;
    }

    private static void writeField(Out out, String key, Field field, Object value, int[] links, boolean keyed) {
        switch (field) {
            case UINT -> out.uint(uintOf(key, value));
            case LINK -> writeLink(out, key, value, links);
            case LINK_OR_NULL -> {
                if (value == null) {
                    out.write(SIMPLE_NULL);
                } else {
                    writeLink(out, key, value, links);
                }
            }
            case LINKS -> {
                List<?> list = listOf(key, value);
                out.header(MAJOR_ARRAY, list.size());
                for (Object item : list) {
                    writeLink(out, key, item, links);
                }
            }
            case EMBEDDED -> writeEmbedded(out, key, value, links, false);
            case EMBEDDEDS -> {
                List<?> list = listOf(key, value);
                out.header(MAJOR_ARRAY, list.size());
                for (int i = 0; i < list.size(); i++) {
                    writeEmbedded(out, key, list.get(i), links, keyed && i % 2 == 0);
                }
            }
            case UINTS -> {
                List<?> list = listOf(key, value);
                out.header(MAJOR_ARRAY, list.size());
                for (Object item : list) {
                    out.uint(uintOf(key, item));
                }
            }
            case BYTES -> {
                if (!(value instanceof byte[] bytes)) {
                    throw new NodeFormatException("'" + key + "' must hold a byte string, found " + typeName(value));
                }
                out.bytes(bytes);
            }
            case TEXT -> out.strictText(key, textOf(key, value));
            case CID -> out.strictText(key, cidOf(key, value));
            case CIDS -> {
                List<?> list = listOf(key, value);
                out.header(MAJOR_ARRAY, list.size());
                for (Object item : list) {
                    out.strictText(key, cidOf(key, item));
                }
            }
            case INDEX -> writeIndex(out, key, value, links);
            case DATOMS -> {
                List<?> list = listOf(key, value);
                out.header(MAJOR_ARRAY, list.size());
                for (Object item : list) {
                    if (!(item instanceof Datom datom)) {
                        throw new NodeFormatException("'" + key + "' must hold datoms, found " + typeName(item));
                    }
                    writeDatom(out, key, datom, links);
                }
            }
            case RUNS -> {
                List<?> list = listOf(key, value);
                out.header(MAJOR_ARRAY, list.size());
                for (Object item : list) {
                    if (!(item instanceof TextRun run)) {
                        throw new NodeFormatException("'" + key + "' must hold text runs, found " + typeName(item));
                    }
                    writeRun(out, key, run);
                }
            }
            default -> throw new IllegalStateException("unhandled field " + field);
        }
    }

    private static String textOf(String key, Object value) {
        if (value instanceof String text) {
            return text;
        }
        throw new NodeFormatException("'" + key + "' must hold text, found " + typeName(value));
    }

    /**
     * A CID held as text (PDT 2 phase 0, P2-V-01): a {@code vg} node names roots without linking them.
     * The text is canonical base32, its padding bits zero, as every port reads it (gate finding G2).
     */
    private static String cidOf(String key, Object value) {
        if (value instanceof String text && PRef.isCid(text)) {
            return text;
        }
        throw new NodeFormatException("'" + key + "' must hold a CID as text, found "
                + (value instanceof String text ? "'" + text + "'" : typeName(value)));
    }

    /** The {@code db} root's index map (PDT 2 phase 0, P2-D-01): exactly aevt, avet, eavt, each a link. */
    private static void writeIndex(Out out, String key, Object value, int[] links) {
        if (!(value instanceof Map<?, ?> map) || map.size() != INDEX_KEYS.size() || !map.keySet().containsAll(INDEX_KEYS)) {
            throw new NodeFormatException("'" + key + "' must map exactly aevt, avet and eavt to links, found "
                    + (value instanceof Map<?, ?> map ? map.keySet() : typeName(value)));
        }
        out.header(MAJOR_MAP, INDEX_KEYS.size());
        for (String index : INDEX_KEYS) {
            out.text(index);
            writeLink(out, key, map.get(index), links);
        }
    }

    /** A datom as the definite array {@code [e, a, v, tx]} (PDT 2 phase 0, P2-D-01). */
    private static void writeDatom(Out out, String key, Datom datom, int[] links) {
        out.header(MAJOR_ARRAY, 4);
        out.sint(datom.e());
        out.strictText(key, datom.a());
        Object v = datom.v();
        switch (v) {
            case Long n -> out.sint(n);
            case String text -> out.strictText(key, text);
            case Boolean b -> out.write(b ? SIMPLE_TRUE : SIMPLE_FALSE);
            case byte[] bytes -> out.bytes(bytes);
            case Double d -> out.float64(d);
            case Link link -> writeLink(out, key, link, links);
            case Datom.Tagged tagged -> {
                out.header(MAJOR_ARRAY, 2);
                out.text(tagged.tag());
                if (tagged.inner() instanceof Long n) {
                    out.sint(n);
                } else {
                    out.bytes((byte[]) tagged.inner());
                }
            }
            default -> throw new IllegalStateException("a datom holds no " + typeName(v));
        }
        out.sint(datom.tx());
    }

    /** A run as the definite array {@code [replica, counter, parent, side, length, deleted, text]} (P2-T-01). */
    private static void writeRun(Out out, String key, TextRun run) {
        out.header(MAJOR_ARRAY, 7);
        out.strictText(key, run.replica());
        out.uint(run.counter());
        if (run.parent() == null) {
            out.write(SIMPLE_NULL);
        } else {
            out.header(MAJOR_ARRAY, 2);
            out.strictText(key, run.parent().replica());
            out.uint(run.parent().counter());
        }
        out.text(run.side());
        out.uint(run.length());
        out.write(run.deleted() ? SIMPLE_TRUE : SIMPLE_FALSE);
        out.strictText(key, run.text());
    }

    private static long uintOf(String key, Object value) {
        long n;
        if (value instanceof Integer i) {
            n = i;
        } else if (value instanceof Long l) {
            n = l;
        } else {
            throw new NodeFormatException("'" + key + "' must be an unsigned integer, found " + typeName(value));
        }
        if (n < 0) {
            throw new NodeFormatException("'" + key + "' must be an unsigned integer, found " + n);
        }
        return n;
    }

    private static List<?> listOf(String key, Object value) {
        if (value instanceof List<?> list) {
            return list;
        }
        throw new NodeFormatException("'" + key + "' must be a list, found " + typeName(value));
    }

    private static void writeLink(Out out, String key, Object value, int[] links) {
        if (!(value instanceof Link link)) {
            throw new NodeFormatException("'" + key + "' must hold a link, found " + typeName(value));
        }
        if (++links[0] > MAX_LINKS) {
            throw new NodeFormatException("node has more than " + MAX_LINKS + " links");
        }
        out.link(link);
    }

    private static void writeEmbedded(Out out, String key, Object value, int[] links, boolean keySlot) {
        if (!(value instanceof Embedded embedded)) {
            throw new NodeFormatException("'" + key + "' must hold an embedded CBOR item, found " + typeName(value));
        }
        byte[] bytes = embedded.bytes();
        int end = skip(bytes, 0);
        if (end != bytes.length) {
            throw new NodeFormatException("'" + key + "' embedded item has trailing bytes");
        }
        checkEmbedded(key, bytes, keySlot);
        if (isLinkShaped(bytes) && ++links[0] > MAX_LINKS) {
            throw new NodeFormatException("node has more than " + MAX_LINKS + " links");
        }
        out.raw(bytes);
    }

    /**
     * Checks an embedded item against the wire encoding (HLC round six, 2026-11-08, owner ruling
     * F2 and change JD-4): every application key and value is a document under rules S1 to S12,
     * so the big-number tags 2, 3 and 4 stand in it and no other tag does. The one exception is a
     * bare link item in a value slot, a spilled value (PD-N-05); a key never spills, so the codec
     * refuses a link item in a key slot, as every reader refuses tag 42 in any other document.
     */
    private static void checkEmbedded(String key, byte[] bytes, boolean keySlot) {
        if (!keySlot && isLinkShaped(bytes)) {
            return;
        }
        try {
            CborCodec.checkDocument(bytes);
        } catch (UncheckedIOException e) {
            throw new NodeFormatException(null, "'" + key + "' " + (keySlot ? "key item" : "embedded item")
                    + " breaks the wire encoding: " + e.getCause().getMessage(), e);
        }
    }

    /** Whether a field of a kind is an entry list whose even slots hold keys. */
    private static boolean isKeyed(String kind, String key) {
        return isMapKind(kind) && "e".equals(key);
    }

    private static String typeName(Object value) {
        return value == null ? "null" : value.getClass().getSimpleName();
    }

    // ------------------------------------------------------------------ decode

    /**
     * Decodes a node whose CID the caller does not know.
     *
     * @param bytes the canonical bytes
     * @return the node in canonical key order, with {@link Long} integers, {@link Link} links, and
     *         {@link Embedded} application items
     * @throws NodeFormatException on any refusal of PD-N-04
     */
    public static Map<String, Object> decode(byte[] bytes) {
        return decode(null, bytes);
    }

    /**
     * Decodes a named node.
     *
     * @param cid   the CID, named in any refusal; may be null
     * @param bytes the canonical bytes
     * @return the node in canonical key order
     * @throws NodeFormatException on any refusal of PD-N-04
     */
    public static Map<String, Object> decode(String cid, byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        try {
            Scan scan = scan(bytes);
            Map<String, Field> schema = schemaOf(scan.kind);
            In in = new In(bytes);
            Map<String, Object> node = new LinkedHashMap<>();
            for (Map.Entry<String, Integer> entry : scan.offsets.entrySet()) {
                String key = entry.getKey();
                if (KIND_KEY.equals(key)) {
                    node.put(key, scan.kind);
                    continue;
                }
                Field field = schema.get(key);
                if (field == null) {
                    throw new NodeFormatException("key '" + key + "' outside the schema of kind '" + scan.kind + "'");
                }
                in.p = entry.getValue();
                node.put(key, readField(in, key, field, isKeyed(scan.kind, key)));
            }
            return node;
        } catch (NodeFormatException e) {
            throw e.withCid(cid);
        }
    }

    /**
     * The kind of a structured node, from its {@code k} field, without decoding the rest.
     *
     * @param bytes the canonical bytes
     * @return the kind, which may be unknown or reserved; the caller decides
     * @throws NodeFormatException when the bytes are not a well-formed node map with a text {@code k}
     */
    public static String kindOf(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        return scan(bytes).kind;
    }

    /**
     * The links of a structured node in field order, without building the map.
     *
     * @param bytes the canonical bytes of a structured node
     * @return the CIDs, in order: for {@code vr} the root link, for others the {@code c} list
     * @throws NodeFormatException on any refusal of PD-N-04
     */
    public static List<String> links(byte[] bytes) {
        return links(null, bytes);
    }

    /**
     * The links of a named node. A raw chunk (codec {@code raw}) has none, and this
     * fast path returns an empty list without reading its bytes.
     *
     * @param cid   the CID, which decides whether the bytes are a chunk; may be null for a structured node
     * @param bytes the bytes
     * @return the linked CIDs in order, empty for a chunk
     * @throws NodeFormatException on any refusal of PD-N-04
     */
    public static List<String> links(String cid, byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (cid != null && Cids.parse(cid).codec() == Cids.RAW) {
            return List.of();
        }
        try {
            Scan scan = scan(bytes);
            Map<String, Field> schema = schemaOf(scan.kind);
            In in = new In(bytes);
            List<String> out = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : scan.offsets.entrySet()) {
                String key = entry.getKey();
                if (KIND_KEY.equals(key)) {
                    continue;
                }
                Field field = schema.get(key);
                if (field == null) {
                    throw new NodeFormatException("key '" + key + "' outside the schema of kind '" + scan.kind + "'");
                }
                if (field == Field.EMBEDDED || field == Field.EMBEDDEDS) {
                    // A spilled value (PD-N-05) is a tag-42 link item in a value slot; it is a link of the node.
                    // On a map node the entry list alternates key, value: keys never spill and are skipped.
                    in.p = entry.getValue();
                    boolean keyed = isKeyed(scan.kind, key);
                    Object value = readField(in, key, field, keyed);
                    List<?> items = value instanceof List<?> list ? list : List.of(value);
                    for (int i = 0; i < items.size(); i++) {
                        if (keyed && i % 2 == 0) {
                            continue;
                        }
                        String spilled = spilledLink((Embedded) items.get(i));
                        if (spilled != null) {
                            out.add(spilled);
                        }
                    }
                    continue;
                }
                if (field == Field.INDEX) {
                    in.p = entry.getValue();
                    for (Object link : ((Map<?, ?>) readField(in, key, field, false)).values()) {
                        out.add(((Link) link).cid());
                    }
                    continue;
                }
                if (field == Field.DATOMS) {
                    // PDT 2 phase 0 (P2-D-01): a spilled datom value is a link of the node, as in an embedded slot
                    in.p = entry.getValue();
                    for (Object datom : (List<?>) readField(in, key, field, false)) {
                        if (((Datom) datom).v() instanceof Link link) {
                            out.add(link.cid());
                        }
                    }
                    continue;
                }
                if (field != Field.LINK && field != Field.LINK_OR_NULL && field != Field.LINKS) {
                    continue;
                }
                in.p = entry.getValue();
                Object value = readField(in, key, field, false);
                if (value instanceof Link link) {
                    out.add(link.cid());
                } else if (value instanceof List<?> list) {
                    for (Object item : list) {
                        out.add(((Link) item).cid());
                    }
                }
            }
            return Collections.unmodifiableList(out);
        } catch (NodeFormatException e) {
            throw e.withCid(cid);
        }
    }

    /** Whether a kind's {@code e} list alternates keys and values. */
    private static boolean isMapKind(String kind) {
        return Kind.M.equals(kind) || Kind.MC.equals(kind);
    }

    /**
     * Whether an embedded item has the exact shape of a link item: 41 bytes, tag 42,
     * a 37-byte string, and the leading identity byte.
     *
     * @param bytes the item
     * @return true for the link shape, whatever CID it carries
     */
    public static boolean isLinkShaped(byte[] bytes) {
        return bytes.length == 2 + 2 + LINK_BYTES && (bytes[0] & 0xff) == 0xd8 && bytes[1] == LINK_TAG
                && (bytes[2] & 0xff) == 0x58 && bytes[3] == LINK_BYTES && bytes[4] == 0x00;
    }

    /**
     * The CID of an embedded item that is exactly a link item (tag 42 over 0x00 and a
     * binary CID), the form a spilled value takes in a value slot (PD-N-05); null for
     * any other item.
     *
     * @param item an embedded application item
     * @return the linked CID, or null when the item is not a bare link
     * @throws NodeFormatException when the item is a link whose codec or multihash is unknown
     */
    public static String spilledLink(Embedded item) {
        byte[] bytes = item.bytes();
        if (!isLinkShaped(bytes)) {
            return null;
        }
        try {
            return Cids.fromBinary(Arrays.copyOfRange(bytes, 5, bytes.length));
        } catch (IllegalArgumentException e) {
            throw new NodeFormatException(null, "spilled value link refused: " + e.getMessage(), e);
        }
    }

    /**
     * Skips one CBOR data item of any form, including the indefinite forms and floats
     * that {@code CborCodec} may produce inside an embedded item.
     *
     * @param bytes  the buffer
     * @param offset where the item starts
     * @return the offset just past the item
     * @throws NodeFormatException when the item is truncated, malformed, or nested deeper than 64
     */
    public static int skip(byte[] bytes, int offset) {
        Objects.requireNonNull(bytes, "bytes");
        In in = new In(bytes);
        in.p = offset;
        skipItem(in, 0);
        return in.p;
    }

    /** The keys a kind's schema allows, besides {@code k}. */
    public static Set<String> schemaKeys(String kind) {
        return schemaOf(kind).keySet();
    }

    /**
     * The DAG-CBOR key order: shorter UTF-8 encodings first, then unsigned byte order.
     *
     * @return negative, zero, or positive as {@code a} sorts before, with, or after {@code b}
     */
    public static int compareKeys(String a, String b) {
        byte[] ab = a.getBytes(StandardCharsets.UTF_8);
        byte[] bb = b.getBytes(StandardCharsets.UTF_8);
        if (ab.length != bb.length) {
            return Integer.compare(ab.length, bb.length);
        }
        return Arrays.compareUnsigned(ab, bb);
    }

    // ------------------------------------------------------------------ scanning

    /** Pass one over a node: the kind and where each key's value starts, in key order. */
    private static final class Scan {
        String kind;
        final Map<String, Integer> offsets = new LinkedHashMap<>();
    }

    private static Scan scan(byte[] bytes) {
        if (bytes.length > MAX_SEALED_NODE_BYTES) {
            throw new NodeFormatException("node of " + bytes.length + " bytes exceeds " + boundName(MAX_SEALED_NODE_BYTES));
        }
        In in = new In(bytes);
        long entries = in.header(MAJOR_MAP, "node");
        Scan scan = new Scan();
        String previous = null;
        for (long i = 0; i < entries; i++) {
            String key = in.text("key");
            if (previous != null) {
                int order = compareKeys(previous, key);
                if (order == 0) {
                    throw new NodeFormatException("duplicate key '" + key + "'");
                }
                if (order > 0) {
                    throw new NodeFormatException("keys out of canonical order: '" + key + "' after '" + previous + "'");
                }
            }
            previous = key;
            if (KIND_KEY.equals(key)) {
                scan.offsets.put(key, in.p);
                scan.kind = in.text(KIND_KEY);
            } else {
                scan.offsets.put(key, in.p);
                skipField(in);
            }
        }
        if (in.p != bytes.length) {
            throw new NodeFormatException("trailing bytes after the node map");
        }
        if (scan.kind == null) {
            throw new NodeFormatException("node has no '" + KIND_KEY + "' field");
        }
        if (bytes.length > limitFor(scan.kind)) {
            throw new NodeFormatException("node of " + bytes.length + " bytes exceeds " + boundName(limitFor(scan.kind)));
        }
        return scan;
    }

    private static Object readField(In in, String key, Field field, boolean keyed) {
        return switch (field) {
            case UINT -> in.uint(key);
            case LINK -> in.link(key);
            case LINK_OR_NULL -> in.peek() == SIMPLE_NULL ? in.nullValue() : in.link(key);
            case LINKS -> {
                long n = in.header(MAJOR_ARRAY, key);
                List<Object> list = new ArrayList<>((int) Math.min(n, MAX_LINKS));
                for (long i = 0; i < n; i++) {
                    list.add(in.link(key));
                }
                yield Collections.unmodifiableList(list);
            }
            case EMBEDDED -> in.embedded(key, false);
            case EMBEDDEDS -> {
                long n = in.header(MAJOR_ARRAY, key);
                List<Object> list = new ArrayList<>((int) Math.min(n, 1 << 16));
                for (long i = 0; i < n; i++) {
                    list.add(in.embedded(key, keyed && i % 2 == 0));
                }
                yield Collections.unmodifiableList(list);
            }
            case UINTS -> {
                long n = in.header(MAJOR_ARRAY, key);
                List<Object> list = new ArrayList<>((int) Math.min(n, 1 << 16));
                for (long i = 0; i < n; i++) {
                    list.add(in.uint(key));
                }
                yield Collections.unmodifiableList(list);
            }
            case BYTES -> in.bytes(key);
            case TEXT -> in.strictText(key);
            case CID -> in.cidText(key);
            case CIDS -> {
                long n = in.header(MAJOR_ARRAY, key);
                List<Object> list = new ArrayList<>((int) Math.min(n, 1 << 16));
                for (long i = 0; i < n; i++) {
                    list.add(in.cidText(key));
                }
                yield Collections.unmodifiableList(list);
            }
            case INDEX -> {
                long n = in.header(MAJOR_MAP, key);
                if (n != INDEX_KEYS.size()) {
                    throw new NodeFormatException("'" + key + "' must map exactly aevt, avet and eavt to links, in that order");
                }
                Map<String, Object> index = new LinkedHashMap<>();
                for (String expected : INDEX_KEYS) {
                    if (!expected.equals(in.strictText(key))) {
                        throw new NodeFormatException("'" + key + "' must map exactly aevt, avet and eavt to links, in that order");
                    }
                    index.put(expected, in.link(key));
                }
                yield Collections.unmodifiableMap(index);
            }
            case DATOMS -> {
                long n = in.header(MAJOR_ARRAY, key);
                List<Object> list = new ArrayList<>((int) Math.min(n, 1 << 16));
                for (long i = 0; i < n; i++) {
                    list.add(in.datom(key));
                }
                yield Collections.unmodifiableList(list);
            }
            case RUNS -> {
                long n = in.header(MAJOR_ARRAY, key);
                List<Object> list = new ArrayList<>((int) Math.min(n, 1 << 16));
                for (long i = 0; i < n; i++) {
                    list.add(in.run(key));
                }
                yield Collections.unmodifiableList(list);
            }
        };
    }

    /**
     * Skips one field value of a node: a definite list's elements each count their nesting from
     * zero, as every other item does. A list field holds embedded items, links, or integers, and
     * the item rules judge an embedded item by its own bytes (HLC round six, 2026-11-08, change
     * JD-4), so the node's list adds no level; reading the field then checks each item again.
     */
    private static void skipField(In in) {
        int initial = in.peek();
        if (initial >> 5 != MAJOR_ARRAY || (initial & 0x1f) == 31) {
            skipItem(in, 0);
            return;
        }
        in.advance(1);
        long n = in.argument(initial & 0x1f);
        for (long i = 0; i < n; i++) {
            skipItem(in, 0);
        }
    }

    /** Skips any item; {@code depth} counts containers entered. */
    private static void skipItem(In in, int depth) {
        if (depth > MAX_DEPTH) {
            throw new NodeFormatException("CBOR nesting deeper than " + MAX_DEPTH + " levels");
        }
        int initial = in.u8();
        int major = initial >> 5;
        int info = initial & 0x1f;
        if (major == MAJOR_SIMPLE) {
            switch (info) {
                case 24 -> in.advance(1);
                case 25 -> in.advance(2);
                case 26 -> in.advance(4);
                case 27 -> in.advance(8);
                case 28, 29, 30 -> throw new NodeFormatException("reserved additional information " + info);
                case 31 -> throw new NodeFormatException("unexpected break code");
                default -> {
                    // a simple value in the initial byte
                }
            }
            return;
        }
        if (info == 31) {
            switch (major) {
                case MAJOR_BYTES, MAJOR_TEXT -> {
                    while (in.peek() != BREAK) {
                        int chunk = in.u8();
                        if (chunk >> 5 != major || (chunk & 0x1f) == 31) {
                            throw new NodeFormatException("malformed indefinite string chunk");
                        }
                        in.advance(in.argument(chunk & 0x1f));
                    }
                    in.advance(1);
                }
                case MAJOR_ARRAY -> {
                    while (in.peek() != BREAK) {
                        skipItem(in, depth + 1);
                    }
                    in.advance(1);
                }
                case MAJOR_MAP -> {
                    while (in.peek() != BREAK) {
                        skipItem(in, depth + 1);
                        skipItem(in, depth + 1);
                    }
                    in.advance(1);
                }
                default -> throw new NodeFormatException("indefinite length on major type " + major);
            }
            return;
        }
        long argument = in.argument(info);
        switch (major) {
            case MAJOR_UINT, MAJOR_NEGATIVE -> {
                // the argument is the value
            }
            case MAJOR_BYTES, MAJOR_TEXT -> in.advance(argument);
            case MAJOR_ARRAY -> {
                for (long i = 0; i < argument; i++) {
                    skipItem(in, depth + 1);
                }
            }
            case MAJOR_MAP -> {
                for (long i = 0; i < argument; i++) {
                    skipItem(in, depth + 1);
                    skipItem(in, depth + 1);
                }
            }
            case MAJOR_TAG -> {
                // HLC round six, 2026-11-08 (rule S2): a tag is no container, so its content sits at the tag's own
                // depth, as Jackson counts a decimal's tag 4 array as the level; further tags are read in a loop,
                // so a chain of tags costs no stack
                while (in.peek() >> 5 == MAJOR_TAG) {
                    int tag = in.u8();
                    if ((tag & 0x1f) == 31) {
                        throw new NodeFormatException("indefinite length on major type " + MAJOR_TAG);
                    }
                    in.argument(tag & 0x1f);
                }
                skipItem(in, depth);
            }
            default -> throw new IllegalStateException("unreachable major type " + major);
        }
    }

    // ------------------------------------------------------------------ the writer

    private static final class Out {
        private byte[] buf = new byte[256];
        private int n;

        int size() {
            return n;
        }

        byte[] toBytes() {
            return Arrays.copyOf(buf, n);
        }

        private void ensure(int more) {
            if (n + more > buf.length) {
                buf = Arrays.copyOf(buf, Math.max(buf.length * 2, n + more));
            }
        }

        void write(int b) {
            ensure(1);
            buf[n++] = (byte) b;
        }

        void raw(byte[] bytes) {
            ensure(bytes.length);
            System.arraycopy(bytes, 0, buf, n, bytes.length);
            n += bytes.length;
        }

        /** Writes a header with the shortest form of an unsigned argument. */
        void header(int major, long value) {
            int m = major << 5;
            if (value >= 0 && value < 24) {
                write(m | (int) value);
            } else if (value >= 0 && value < 0x100) {
                write(m | 24);
                write((int) value);
            } else if (value >= 0 && value < 0x10000) {
                write(m | 25);
                write((int) (value >>> 8));
                write((int) value);
            } else if (value >= 0 && value < 0x100000000L) {
                write(m | 26);
                for (int shift = 24; shift >= 0; shift -= 8) {
                    write((int) (value >>> shift));
                }
            } else {
                write(m | 27);
                for (int shift = 56; shift >= 0; shift -= 8) {
                    write((int) (value >>> shift));
                }
            }
        }

        void uint(long value) {
            header(MAJOR_UINT, value);
        }

        void text(String s) {
            byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
            header(MAJOR_TEXT, utf8.length);
            raw(utf8);
        }

        void bytes(byte[] bytes) {
            header(MAJOR_BYTES, bytes.length);
            raw(bytes);
        }

        /** A signed integer: major type 0, or major type 1 over {@code -1 - value} (PDT 2 phase 0, P2-D-01). */
        void sint(long value) {
            if (value >= 0) {
                header(MAJOR_UINT, value);
            } else {
                header(MAJOR_NEGATIVE, -1 - value);
            }
        }

        /** A float64, the one float form of a level-2 node (PDT 2 phase 0, P2-D-01). */
        void float64(double value) {
            write(0xfb);
            long bits = Double.doubleToRawLongBits(value);
            for (int shift = 56; shift >= 0; shift -= 8) {
                write((int) (bits >>> shift));
            }
        }

        /** Text with no lone surrogate, which UTF-8 cannot hold (PDT 2 phase 0). */
        void strictText(String key, String s) {
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                if (Character.isHighSurrogate(c) && i + 1 < s.length() && Character.isLowSurrogate(s.charAt(i + 1))) {
                    i++;
                } else if (Character.isSurrogate(c)) {
                    throw new NodeFormatException("'" + key + "' text holds a lone surrogate");
                }
            }
            text(s);
        }

        void link(Link link) {
            header(MAJOR_TAG, LINK_TAG);
            header(MAJOR_BYTES, LINK_BYTES);
            write(0x00);
            raw(link.binary());
        }
    }

    // ------------------------------------------------------------------ the reader

    private static final class In {
        private final byte[] b;
        private int p;
        private int links;

        In(byte[] b) {
            this.b = b;
        }

        int peek() {
            if (p >= b.length) {
                throw new NodeFormatException("truncated node");
            }
            return b[p] & 0xff;
        }

        int u8() {
            int v = peek();
            p++;
            return v;
        }

        void advance(long count) {
            if (count < 0 || count > b.length - p) {
                throw new NodeFormatException("truncated node");
            }
            p += (int) count;
        }

        /** Reads the argument that follows an initial byte, in any form (the skipper's rule). */
        long argument(int info) {
            if (info < 24) {
                return info;
            }
            return switch (info) {
                case 24 -> u8();
                case 25 -> (long) u8() << 8 | u8();
                case 26 -> (long) u8() << 24 | (long) u8() << 16 | (long) u8() << 8 | u8();
                case 27 -> {
                    long v = 0;
                    for (int i = 0; i < 8; i++) {
                        v = v << 8 | u8();
                    }
                    yield v;
                }
                case 28, 29, 30 -> throw new NodeFormatException("reserved additional information " + info);
                default -> throw new NodeFormatException("indefinite length in a structured field");
            };
        }

        /** Reads a definite, shortest-form header of the expected major type (the structural rule). */
        long header(int major, String what) {
            int initial = u8();
            if (initial >> 5 != major) {
                throw new NodeFormatException("'" + what + "' must be CBOR major type " + major + ", found major type " + (initial >> 5));
            }
            int info = initial & 0x1f;
            if (info == 31) {
                throw new NodeFormatException("'" + what + "' uses an indefinite length");
            }
            long value = argument(info);
            if (value < 0) {
                throw new NodeFormatException("'" + what + "' integer above 2^63 - 1");
            }
            boolean shortest = switch (info) {
                case 24 -> value >= 24;
                case 25 -> value >= 0x100;
                case 26 -> value >= 0x10000;
                case 27 -> value >= 0x100000000L;
                default -> true;
            };
            if (!shortest) {
                throw new NodeFormatException("'" + what + "' integer not in shortest form");
            }
            return value;
        }

        long uint(String what) {
            return header(MAJOR_UINT, what);
        }

        String text(String what) {
            long length = header(MAJOR_TEXT, what);
            if (length > b.length - p) {
                throw new NodeFormatException("truncated node");
            }
            String s = new String(b, p, (int) length, StandardCharsets.UTF_8);
            p += (int) length;
            return s;
        }

        Object nullValue() {
            advance(1);
            return null;
        }

        byte[] bytes(String what) {
            long length = header(MAJOR_BYTES, what);
            if (length > b.length - p) {
                throw new NodeFormatException("truncated node");
            }
            byte[] out = Arrays.copyOfRange(b, p, p + (int) length);
            p += (int) length;
            return out;
        }

        /** Text that is well-formed UTF-8 (PDT 2 phase 0): a malformed sequence is refused, never replaced. */
        String strictText(String what) {
            long length = header(MAJOR_TEXT, what);
            if (length > b.length - p) {
                throw new NodeFormatException("truncated node");
            }
            String s;
            try {
                s = StandardCharsets.UTF_8.newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .decode(ByteBuffer.wrap(b, p, (int) length)).toString();
            } catch (CharacterCodingException e) {
                throw new NodeFormatException(null, "'" + what + "' text is not well-formed UTF-8", e);
            }
            p += (int) length;
            return s;
        }

        /** A CID as canonical base32 text, as {@link #cidOf} writes it (PDT 2 phase 0, P2-V-01, gate finding G2). */
        String cidText(String what) {
            String text = strictText(what);
            if (!PRef.isCid(text)) {
                throw new NodeFormatException("'" + what + "' must hold a CID as text, found '" + text + "'");
            }
            return text;
        }

        /** A signed integer under its shortest head: major type 0 or 1, within the long range. */
        long sint(String what) {
            int major = peek() >> 5;
            if (major == MAJOR_NEGATIVE) {
                return -1 - header(MAJOR_NEGATIVE, what);
            }
            return header(MAJOR_UINT, what);
        }

        boolean bool(String what) {
            int initial = u8();
            if (initial == SIMPLE_TRUE || initial == SIMPLE_FALSE) {
                return initial == SIMPLE_TRUE;
            }
            throw new NodeFormatException("'" + what + "' must be a boolean, found initial byte 0x" + Integer.toHexString(initial));
        }

        /** One datom {@code [e, a, v, tx]} under the value grammar of §3.1.1 (PDT 2 phase 0, P2-D-01). */
        Datom datom(String what) {
            if (header(MAJOR_ARRAY, what) != 4) {
                throw new NodeFormatException("'" + what + "' datom must hold four items");
            }
            long e = sint(what);
            String a = strictText(what);
            Object v = datomValue(what);
            long tx = sint(what);
            try {
                return new Datom(e, a, v, tx);
            } catch (IllegalArgumentException refused) {
                throw new NodeFormatException(null, "'" + what + "' datom refused: " + refused.getMessage(), refused);
            }
        }

        private Object datomValue(String what) {
            int initial = peek();
            int major = initial >> 5;
            return switch (major) {
                case MAJOR_UINT, MAJOR_NEGATIVE -> sint(what);
                case MAJOR_BYTES -> bytes(what);
                case MAJOR_TEXT -> strictText(what);
                case MAJOR_TAG -> link(what);
                case MAJOR_ARRAY -> {
                    if (header(MAJOR_ARRAY, what) != 2) {
                        throw new NodeFormatException("'" + what + "' tagged datom value must hold two items");
                    }
                    String tag = strictText(what);
                    Object inner = switch (tag) {
                        case Datom.Tagged.INSTANT -> {
                            int innerMajor = peek() >> 5;
                            if (innerMajor != MAJOR_UINT && innerMajor != MAJOR_NEGATIVE) {
                                throw new NodeFormatException("'" + what + "' instant value must hold an integer");
                            }
                            yield sint(what);
                        }
                        case Datom.Tagged.UUID, Datom.Tagged.BIGINT -> bytes(what);
                        default -> throw new NodeFormatException("'" + what + "' unknown value tag '" + tag + "'");
                    };
                    try {
                        yield new Datom.Tagged(tag, inner);
                    } catch (IllegalArgumentException refused) {
                        throw new NodeFormatException(null, "'" + what + "' " + refused.getMessage(), refused);
                    }
                }
                case MAJOR_SIMPLE -> switch (initial) {
                    case SIMPLE_TRUE, SIMPLE_FALSE -> bool(what);
                    case 0xfb -> {
                        advance(1);
                        if (8 > b.length - p) {
                            throw new NodeFormatException("truncated node");
                        }
                        long bits = 0;
                        for (int i = 0; i < 8; i++) {
                            bits = bits << 8 | u8();
                        }
                        double d = Double.longBitsToDouble(bits);
                        if (!Double.isFinite(d)) {
                            throw new NodeFormatException("'" + what + "' float value is not finite");
                        }
                        yield d;
                    }
                    case 0xfa -> throw new NodeFormatException("'" + what + "' float value is float32; a datom float is float64");
                    case 0xf9 -> throw new NodeFormatException("'" + what + "' float value is a half float; a datom float is float64");
                    case SIMPLE_NULL -> throw new NodeFormatException("'" + what + "' datom value is null");
                    default -> throw new NodeFormatException("'" + what + "' datom value is simple value 0x" + Integer.toHexString(initial));
                };
                default -> throw new NodeFormatException("'" + what + "' datom value of major type " + major
                        + "; the grammar holds integers, bytes, text, links, tagged pairs, booleans and float64");
            };
        }

        /** One run {@code [replica, counter, parent, side, length, deleted, text]} (PDT 2 phase 0, P2-T-01). */
        TextRun run(String what) {
            if (header(MAJOR_ARRAY, what) != 7) {
                throw new NodeFormatException("'" + what + "' run must hold seven items");
            }
            String replica = strictText(what);
            long counter = uint(what);
            TextRun.CharId parent = null;
            if (peek() == SIMPLE_NULL) {
                advance(1);
            } else {
                if (header(MAJOR_ARRAY, what) != 2) {
                    throw new NodeFormatException("'" + what + "' run parent must be null or [replica, counter]");
                }
                String parentReplica = strictText(what);
                long parentCounter = uint(what);
                try {
                    parent = new TextRun.CharId(parentReplica, parentCounter);
                } catch (IllegalArgumentException refused) {
                    throw new NodeFormatException(null, "'" + what + "' run parent refused: " + refused.getMessage(), refused);
                }
            }
            String side = strictText(what);
            long length = uint(what);
            boolean deleted = bool(what);
            String text = strictText(what);
            try {
                return new TextRun(replica, counter, parent, side, length, deleted, text);
            } catch (IllegalArgumentException refused) {
                throw new NodeFormatException(null, "'" + what + "' run refused: " + refused.getMessage(), refused);
            }
        }

        Link link(String what) {
            if (++links > MAX_LINKS) {
                throw new NodeFormatException("node has more than " + MAX_LINKS + " links");
            }
            long tag = header(MAJOR_TAG, what);
            if (tag != LINK_TAG) {
                throw new NodeFormatException("'" + what + "' carries tag " + tag + "; a link is tag " + LINK_TAG);
            }
            long length = header(MAJOR_BYTES, what);
            if (length != LINK_BYTES) {
                throw new NodeFormatException("'" + what + "' link of " + length + " bytes; a link holds 0x00 and a 36-byte CID");
            }
            if (u8() != 0x00) {
                throw new NodeFormatException("'" + what + "' link does not start with the 0x00 identity byte");
            }
            if (Cids.BINARY_LENGTH > b.length - p) {
                throw new NodeFormatException("truncated node");
            }
            byte[] binary = Arrays.copyOfRange(b, p, p + Cids.BINARY_LENGTH);
            p += Cids.BINARY_LENGTH;
            try {
                return new Link(Cids.fromBinary(binary));
            } catch (IllegalArgumentException e) {
                throw new NodeFormatException(null, "'" + what + "' link refused: " + e.getMessage(), e);
            }
        }

        /**
         * Reads one embedded item, which must be in the wire encoding; a link-shaped item in a value
         * slot is a spilled-value link and counts against the link bound.
         */
        Embedded embedded(String what, boolean keySlot) {
            int start = p;
            skipItem(this, 0);
            byte[] bytes = Arrays.copyOfRange(b, start, p);
            checkEmbedded(what, bytes, keySlot);
            if (isLinkShaped(bytes) && ++links > MAX_LINKS) {
                throw new NodeFormatException("node has more than " + MAX_LINKS + " links");
            }
            return new Embedded(bytes);
        }
    }
}
