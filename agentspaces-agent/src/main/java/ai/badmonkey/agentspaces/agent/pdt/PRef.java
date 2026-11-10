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
package ai.badmonkey.agentspaces.agent.pdt;

import ai.badmonkey.agentspaces.common.codec.CanonicalMaps;
import ai.badmonkey.agentspaces.common.codec.WireRecord;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * One head of a ref (srad/agentspaces-pdt.md §3.6, PD-R-01): a named, leased,
 * signed entry that points at the current root of a persistent value. A ref is
 * the set of live {@code PRef} entries with one {@link #name} in one space, and
 * its heads are the entries no other live entry names in {@link #base}. Written
 * to the ref space as an ordinary signed, leased entry; tagged {@link #NAME_TAG}
 * and {@link #ROOT_TAG} so a template finds every head of a name without
 * decoding. Named {@code ai.badmonkey.agentspaces.agent.pdt.PRef#v1} on the wire
 * and pinned by the golden vectors {@code pdt_ref_cbor} and {@code pdt_ref_min_cbor}.
 *
 * <p>The three optional fields ({@link #nodes}, {@link #keyEpoch}, {@link #attrs})
 * leave the wire when null, so a head without them is the minimal form.
 *
 * @param v        schema version: 1, or {@link #VERSION_2} for a head whose tree holds a level-2 kind
 * @param name     the ref name, at most {@link #MAX_NAME_BYTES} bytes; a tag
 * @param kind     {@link #KIND_MAP}, {@link #KIND_SET}, {@link #KIND_VECTOR}, {@link #KIND_BLOB}, {@link #KIND_TREE},
 *                 or {@link #KIND_DB}
 * @param root     the root CID of this version
 * @param base     the roots this version was computed from, 0 to {@link #MAX_BASE}
 * @param seq      one more than the largest {@code seq} of the bases; 1 for a first version
 * @param size     entries (map, set, vector, tree) or bytes (blob) of the version
 * @param nodes    the inline delta (§3.5): the nodes this version added, at most {@link #INLINE_DELTA_BYTES} in all; null when the delta travels by request
 * @param mode     {@link #MODE_MULTI} or {@link #MODE_LINEAR}; an open set, a reader keeps an unknown mode
 * @param sealing  {@link #SEALING_NONE}, {@link #SEALING_GROUP_KEY}, or {@link #SEALING_SEALED_NODES}; an open set
 * @param keyEpoch the content-key epoch the nodes are sealed under, or null
 * @param attrs    user attributes, at most {@link #MAX_ATTRS} entries and {@link #MAX_ATTRS_BYTES} bytes, or null:
 *                 text under every key, and under {@link #ISSUED_ATTR} and {@link #ISSUER_ATTR} any
 *                 application value, null included, which reads as absent when it is no text (HLC
 *                 round six, 2026-11-08, ruling R1)
 */
@WireRecord
public record PRef(int v, String name, String kind, String root, List<String> base, long seq, long size,
                   @JsonInclude(JsonInclude.Include.NON_NULL) List<byte[]> nodes,
                   String mode, String sealing,
                   @JsonInclude(JsonInclude.Include.NON_NULL) Long keyEpoch,
                   @JsonInclude(JsonInclude.Include.NON_NULL)
                   @JsonDeserialize(using = PRef.Attrs.class) Map<String, ?> attrs) {

    /** Schema version this record carries. */
    public static final int VERSION = 1;

    /**
     * PDT 2 phase 2, P2-D-03 and P2-F-06 (srad/agentspaces-pdt2.md §6.10.3, rule 8): the version of a head
     * whose tree holds a node of a level-2 kind. The schema keeps its twelve fields; a level-1 reader refuses
     * the version and ignores the head, and a level-2 reader accepts versions 1 and 2.
     */
    public static final int VERSION_2 = 2;

    /** Tag every head carries: the ref name. */
    public static final String NAME_TAG = "name";

    /** Tag every head carries: the root CID. */
    public static final String ROOT_TAG = "root";

    public static final String KIND_MAP = "map";
    public static final String KIND_SET = "set";
    public static final String KIND_VECTOR = "vector";
    public static final String KIND_BLOB = "blob";
    public static final String KIND_TREE = "tree";

    /** PDT 2 phase 2, P2-D-03: a DataScript database, a {@code db} root over {@code dn} nodes; always {@link #VERSION_2}. */
    public static final String KIND_DB = "db";

    /** The kinds a head may name. */
    public static final Set<String> KINDS = Set.of(KIND_MAP, KIND_SET, KIND_VECTOR, KIND_BLOB, KIND_TREE, KIND_DB);

    /** Several heads may coexist and readers merge them (§3.6.1). */
    public static final String MODE_MULTI = "multi";

    /** One head at a time, advanced under {@code Space.take} (PD-R-04). */
    public static final String MODE_LINEAR = "linear";

    public static final String SEALING_NONE = "none";
    public static final String SEALING_GROUP_KEY = "group-key";
    public static final String SEALING_SEALED_NODES = "sealed-nodes";

    /** Bound on {@link #name}. */
    public static final int MAX_NAME_BYTES = 256;

    /** Bound on {@link #base}. */
    public static final int MAX_BASE = 8;

    /** Bound on the bytes of {@link #nodes} in all (PD-X-07): the delta that travels on the head. */
    public static final int INLINE_DELTA_BYTES = 32 * 1024;

    /** Bounds on {@link #attrs}. */
    public static final int MAX_ATTRS = 32;
    public static final int MAX_ATTRS_BYTES = 4 * 1024;

    /** Bound on {@link #mode} and {@link #sealing}, in bytes: open sets, but short names. */
    public static final int MAX_MODE_BYTES = 32;

    /** Bound on {@link #seq}: 2^53, so every language's integers hold it and a successor always exists below it. */
    public static final long MAX_SEQ = 1L << 53;

    /** The length of a root or base string: a base32 CIDv1 of a sha2-256 digest (10.2). */
    public static final int CID_LENGTH = 59;

    /**
     * Reserved attr: the issue stamp (the canonical {@code physical:logical:node}
     * HLC form) of the head a rewrite reproduces, so readers keep the merge order
     * of the original head after a lapsed head is rewritten (PD-R-08).
     */
    public static final String ISSUED_ATTR = "pdt.issued";

    /** Reserved attr: the issuer ({@code <peerId>/<localName>}) of the head a rewrite reproduces (PD-R-08). */
    public static final String ISSUER_ATTR = "pdt.issuer";

    private static final String BASE32 = "abcdefghijklmnopqrstuvwxyz234567";

    public PRef {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(sealing, "sealing");
        base = base == null ? List.of() : List.copyOf(base);
        nodes = nodes == null ? null : List.copyOf(nodes);
        attrs = attrs == null ? null : copyOf(attrs);
    }

    /** The two reserved attrs, whose value is an application value (HLC round six, 2026-11-08, ruling R1). */
    private static final Set<String> RESERVED_ATTRS = Set.of(ISSUED_ATTR, ISSUER_ATTR);

    /**
     * A read-only copy of the attrs that keeps a null value (HLC round five, 2026-11-08, decision
     * E10): a reader keeps a head whose attr is present and null, and its reserved pair then falls
     * back to the entry's own stamp and issuer. A null key is refused.
     */
    private static Map<String, ?> copyOf(Map<String, ?> attrs) {
        Map<String, Object> copy = new LinkedHashMap<>();
        for (Map.Entry<String, ?> e : attrs.entrySet()) {
            copy.put(Objects.requireNonNull(e.getKey(), "attr key"), e.getValue());
        }
        return Collections.unmodifiableMap(copy);
    }

    /**
     * Reads the attrs (HLC round six, 2026-11-08, ruling R1): text under every key but the two
     * reserved ones, where any item reads as the application value it is, null included, so the
     * head re-encodes to the bytes it arrived as. A null or any other item under another key
     * makes the head undecodable, which every implementation counts in {@code headsIgnored}; a
     * byte string never reads as base64 text, so it can never pass for an agent id.
     */
    static final class Attrs extends StdDeserializer<Map<String, ?>> {
        private static final long serialVersionUID = 1L;

        Attrs() {
            super(Map.class);
        }

        @Override
        public Map<String, ?> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            if (p.currentToken() != JsonToken.START_OBJECT) {
                return mismatch(ctxt, "attrs are a map, got " + p.currentToken());
            }
            Map<String, Object> attrs = new LinkedHashMap<>();
            for (String key = p.nextFieldName(); key != null; key = p.nextFieldName()) {
                JsonToken token = p.nextToken();
                if (RESERVED_ATTRS.contains(key)) {
                    attrs.put(key, token == JsonToken.VALUE_NULL ? null : inOrder(ctxt.readValue(p, Object.class)));
                } else if (token == JsonToken.VALUE_STRING) {
                    attrs.put(key, p.getText());
                } else {
                    return mismatch(ctxt, "the attr " + key + " holds " + token + "; only "
                            + ISSUED_ATTR + " and " + ISSUER_ATTR + " hold anything but text");
                }
            }
            return attrs;
        }

        private Map<String, ?> mismatch(DeserializationContext ctxt, String message) throws IOException {
            @SuppressWarnings("unchecked")
            Map<String, ?> refused = (Map<String, ?>) ctxt.reportInputMismatch(this, "%s", message);
            return refused;
        }

        /** An application value with every map in its arrival order, which is part of its value. */
        private static Object inOrder(Object value) {
            if (value instanceof Map<?, ?> map) {
                CanonicalMaps.Verbatim ordered = new CanonicalMaps.Verbatim();
                map.forEach((k, v) -> ordered.put((String) k, inOrder(v)));
                return ordered;
            }
            if (value instanceof List<?> list) {
                List<Object> ordered = new ArrayList<>(list.size());
                list.forEach(element -> ordered.add(inOrder(element)));
                return ordered;
            }
            return value;
        }
    }

    /** The tags a head entry is written with: the name and the root. */
    public Map<String, String> tags() {
        return Map.of(NAME_TAG, name, ROOT_TAG, root);
    }

    /** The bytes of the inline delta, 0 when there is none. */
    public long inlineBytes() {
        if (nodes == null) {
            return 0;
        }
        long total = 0;
        for (byte[] node : nodes) {
            total += node == null ? 0 : node.length;
        }
        return total;
    }

    /** This head with another inline delta (null removes it). */
    public PRef withNodes(List<byte[]> nodes) {
        return new PRef(v, name, kind, root, base, seq, size, nodes, mode, sealing, keyEpoch, attrs);
    }

    /** This head without its inline delta: what a renew or a rewrite carries. */
    public PRef withoutNodes() {
        return nodes == null ? this : withNodes(null);
    }

    /** This head with other attrs (null removes them). */
    public PRef withAttrs(Map<String, ?> attrs) {
        return new PRef(v, name, kind, root, base, seq, size, nodes, mode, sealing, keyEpoch, attrs);
    }

    /** The reserved {@link #ISSUED_ATTR}, or null when the head carries none or carries it as no text. */
    public String issuedAttr() {
        return textAttr(ISSUED_ATTR);
    }

    /** The reserved {@link #ISSUER_ATTR}, or null when the head carries none or carries it as no text. */
    public String issuerAttr() {
        return textAttr(ISSUER_ATTR);
    }

    private String textAttr(String key) {
        return attrs != null && attrs.get(key) instanceof String text ? text : null;
    }

    /**
     * Whether a string is a CID of the form 10.2 pins: {@code "b"} and 58 base32
     * characters decoding to {@code 0x01}, codec {@code 0x71} (dag-cbor) or
     * {@code 0x55} (raw), {@code 0x12 0x20}, and a 32-byte digest. The check is
     * local to this module so a head is validated without the content module.
     *
     * @param s the string
     * @return whether it is a CID
     */
    public static boolean isCid(String s) {
        if (s == null || s.length() != CID_LENGTH || s.charAt(0) != 'b') {
            return false;
        }
        long buffer = 0;
        int bits = 0;
        int produced = 0;
        int[] head = new int[4];
        for (int i = 1; i < s.length(); i++) {
            int value = BASE32.indexOf(s.charAt(i));
            if (value < 0) {
                return false;
            }
            buffer = (buffer << 5) | value;
            bits += 5;
            if (bits >= 8) {
                bits -= 8;
                int b = (int) ((buffer >> bits) & 0xff);
                if (produced < 4) {
                    head[produced] = b;
                }
                produced++;
            }
        }
        if (produced != 36 || (buffer & ((1 << bits) - 1)) != 0) {
            return false;
        }
        return head[0] == 0x01 && (head[1] == 0x71 || head[1] == 0x55) && head[2] == 0x12 && head[3] == 0x20;
    }

    /**
     * The version a writer puts on a head of a kind (PDT 2 phase 2, P2-D-03; rule 8 of
     * srad/agentspaces-pdt2.md §6.10.3): {@link #VERSION_2} for {@link #KIND_DB}, whose tree holds the
     * level-2 kinds {@code db} and {@code dn}, and {@link #VERSION} for every other kind.
     *
     * @param kind the head's kind
     * @return the version to write
     */
    public static int versionFor(String kind) {
        return KIND_DB.equals(kind) ? VERSION_2 : VERSION;
    }

    /**
     * Checks the bounds of §3.6: the version, the name length, the kind, the CID
     * grammar of the root and every base, the base count, the range of {@code seq}
     * (0 to {@link #MAX_SEQ}) and the sign of {@code size}, the inline delta
     * bound, the mode and sealing lengths, and the attrs bounds. The mode and the
     * sealing are open sets, so a reader keeps a head whose mode it does not know.
     *
     * @return the first violated rule, or empty when the head is well formed
     */
    public Optional<String> violation() {
        if (v != VERSION && v != VERSION_2) {
            return Optional.of("unsupported PRef version " + v);
        }
        if (KIND_DB.equals(kind) && v != VERSION_2) {
            return Optional.of("a db head is version " + VERSION_2 + ", found " + v);
        }
        if (name.isEmpty()) {
            return Optional.of("empty name");
        }
        if (name.getBytes(StandardCharsets.UTF_8).length > MAX_NAME_BYTES) {
            return Optional.of("name longer than " + MAX_NAME_BYTES + " bytes");
        }
        if (!KINDS.contains(kind)) {
            return Optional.of("unknown kind " + kind);
        }
        if (root.isEmpty()) {
            return Optional.of("empty root");
        }
        if (!isCid(root)) {
            return Optional.of("root is not a CID");
        }
        if (base.size() > MAX_BASE) {
            return Optional.of("more than " + MAX_BASE + " bases");
        }
        for (String b : base) {
            if (b == null || b.isEmpty()) {
                return Optional.of("empty base root");
            }
            if (!isCid(b)) {
                return Optional.of("base root is not a CID");
            }
        }
        if (seq < 0) {
            return Optional.of("negative seq");
        }
        if (seq > MAX_SEQ) {
            return Optional.of("seq above " + MAX_SEQ);
        }
        if (size < 0) {
            return Optional.of("negative size");
        }
        if (nodes != null) {
            for (byte[] node : nodes) {
                if (node == null || node.length == 0) {
                    return Optional.of("empty inline node");
                }
            }
            if (inlineBytes() > INLINE_DELTA_BYTES) {
                return Optional.of("inline nodes larger than " + INLINE_DELTA_BYTES + " bytes");
            }
        }
        if (mode.isEmpty()) {
            return Optional.of("empty mode");
        }
        if (mode.getBytes(StandardCharsets.UTF_8).length > MAX_MODE_BYTES) {
            return Optional.of("mode longer than " + MAX_MODE_BYTES + " bytes");
        }
        if (sealing.isEmpty()) {
            return Optional.of("empty sealing");
        }
        if (sealing.getBytes(StandardCharsets.UTF_8).length > MAX_MODE_BYTES) {
            return Optional.of("sealing longer than " + MAX_MODE_BYTES + " bytes");
        }
        if (keyEpoch != null && keyEpoch < 0) {
            return Optional.of("negative keyEpoch");
        }
        if (attrs != null) {
            if (attrs.size() > MAX_ATTRS) {
                return Optional.of("more than " + MAX_ATTRS + " attrs");
            }
            // HLC round six, 2026-11-08 (ruling R1): the UTF-8 bytes of the keys and the text values
            int bytes = 0;
            for (Map.Entry<String, ?> e : attrs.entrySet()) {
                bytes += e.getKey().getBytes(StandardCharsets.UTF_8).length
                        + (e.getValue() instanceof String text ? text.getBytes(StandardCharsets.UTF_8).length : 0);
            }
            if (bytes > MAX_ATTRS_BYTES) {
                return Optional.of("attrs larger than " + MAX_ATTRS_BYTES + " bytes");
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PRef p)) {
            return false;
        }
        return v == p.v && name.equals(p.name) && kind.equals(p.kind) && root.equals(p.root)
                && base.equals(p.base) && seq == p.seq && size == p.size && sameNodes(nodes, p.nodes)
                && mode.equals(p.mode) && sealing.equals(p.sealing) && Objects.equals(keyEpoch, p.keyEpoch)
                && Objects.equals(attrs, p.attrs);
    }

    @Override
    public int hashCode() {
        int nodesHash = 0;
        if (nodes != null) {
            List<Integer> hashes = new ArrayList<>();
            for (byte[] node : nodes) {
                hashes.add(Arrays.hashCode(node));
            }
            nodesHash = hashes.hashCode();
        }
        return Objects.hash(v, name, kind, root, base, seq, size, nodesHash, mode, sealing, keyEpoch, attrs);
    }

    private static boolean sameNodes(List<byte[]> a, List<byte[]> b) {
        if (a == null || b == null) {
            return a == b;
        }
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (!Arrays.equals(a.get(i), b.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "PRef(" + name + " " + kind + " seq " + seq + " root " + root + " base " + base
                + (nodes == null ? "" : " +" + nodes.size() + " inline nodes") + " " + mode + ")";
    }
}
