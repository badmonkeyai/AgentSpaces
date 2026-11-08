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
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The manifest of a large artifact (srad/large-networks-bulk-data.md §3.1): the
 * artifact's root, size, piece size, and piece hashes (inline up to
 * {@link #INLINE_TABLE_PIECES} pieces, otherwise a block referenced by CID), with
 * its sealing state and media metadata. Written to the group's content space as
 * an ordinary signed, leased entry; tagged {@link #ROOT_TAG} so a template finds
 * it without decoding. Named {@code ai.badmonkey.agentspaces.agent.content.Manifest#v1}
 * on the wire.
 *
 * @param v         schema version, 1
 * @param root      CIDv1 (multibase base32, codec raw, multihash sha2-256) over the concatenated piece hashes
 * @param size      artifact bytes
 * @param pieceSize piece bytes, a power of two from 64 KiB to 4 MiB; the last piece may be shorter
 * @param pieces    {@code ceil(size / pieceSize)}
 * @param table     the piece hashes, 32 bytes each in order, when {@code pieces <= INLINE_TABLE_PIECES}; else null
 * @param tableRef  legacy block CID of the table when it is not inline; else null
 * @param tableRefs reserved for artifacts above one table block (LB-F-12); null
 * @param name      display name, at most 256 bytes, or null
 * @param mediaType RFC 6838 media type, or null
 * @param sealing   {@code none} or {@code group-key}
 * @param keyEpoch  the content-key epoch of sealing, when sealed; else null
 * @param hashing   {@code plain} (hashes over plaintext pieces) or {@code sealed}
 * @param origin    the first publisher; informative, the record signature is the proof
 * @param created   epoch millis of publication
 * @param attrs     user attributes, at most 32 entries and 4 KiB, or null
 * @param layout    how the artifact's bytes are laid out (srad/agentspaces-pdt.md PD-X-09): null or
 *                  {@link #LAYOUT_PIECES} for the flat piece table, {@link #LAYOUT_TREE} when {@code root}
 *                  names a blob tree the tree exchange fetches; omitted from the wire when null, so a
 *                  manifest without it keeps its 0.5.0 bytes
 */
public record Manifest(int v, String root, long size, int pieceSize, int pieces, byte[] table,
                       String tableRef, List<String> tableRefs, String name, String mediaType,
                       String sealing, Long keyEpoch, String hashing, PeerId origin, long created,
                       Map<String, String> attrs,
                       // Appended for the tree exchange (PD-X-09) and omitted from the wire when absent, so
                       // every manifest without a layout is byte-identical to the 0.5.0 manifest.
                       @JsonInclude(JsonInclude.Include.NON_NULL) String layout) {

    /** Schema version this record carries. */
    public static final int VERSION = 1;

    /** Up to this many piece hashes travel inline; beyond it the table is a block (decision 4). */
    public static final int INLINE_TABLE_PIECES = 64;

    /** Bytes of one piece hash (sha2-256). */
    public static final int HASH_BYTES = 32;

    /** Smallest piece size. */
    public static final int MIN_PIECE_SIZE = 64 * 1024;

    /** Largest piece size, bounded by the frame cap with margin. */
    public static final int MAX_PIECE_SIZE = 4 * 1024 * 1024;

    /** Bound on {@link #name}. */
    public static final int MAX_NAME_BYTES = 256;

    /** Bounds on {@link #attrs}. */
    public static final int MAX_ATTRS = 32;
    public static final int MAX_ATTRS_BYTES = 4 * 1024;

    /** Tag every manifest entry carries: the root. */
    public static final String ROOT_TAG = "root";

    /** Tag a manifest carries when it has a name. */
    public static final String NAME_TAG = "name";

    public static final String SEALING_NONE = "none";
    public static final String SEALING_GROUP_KEY = "group-key";
    public static final String HASHING_PLAIN = "plain";
    public static final String HASHING_SEALED = "sealed";

    /** The flat piece table of srad/large-networks-bulk-data.md §3.1, the layout a null {@code layout} means. */
    public static final String LAYOUT_PIECES = "pieces";
    /** A blob tree (srad/agentspaces-pdt.md §3.3) whose root the tree exchange fetches (PD-X-09). */
    public static final String LAYOUT_TREE = "tree";

    public Manifest {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(sealing, "sealing");
        Objects.requireNonNull(hashing, "hashing");
        Objects.requireNonNull(origin, "origin");
        tableRefs = tableRefs == null ? null : List.copyOf(tableRefs);
        attrs = attrs == null ? null : Map.copyOf(attrs);
    }

    /** The 0.5.0 shape: a manifest without a layout, which means {@link #LAYOUT_PIECES}. */
    public Manifest(int v, String root, long size, int pieceSize, int pieces, byte[] table,
                    String tableRef, List<String> tableRefs, String name, String mediaType,
                    String sealing, Long keyEpoch, String hashing, PeerId origin, long created,
                    Map<String, String> attrs) {
        this(v, root, size, pieceSize, pieces, table, tableRef, tableRefs, name, mediaType, sealing, keyEpoch,
                hashing, origin, created, attrs, null);
    }

    /**
     * Whether the root names a blob tree (PD-X-09); false means the flat piece
     * table. Named without a {@code get}/{@code is} prefix so Jackson never
     * treats it as a property of the wire record.
     */
    public boolean treeLayout() {
        return LAYOUT_TREE.equals(layout);
    }

    /** This manifest with a layout (null removes it). */
    public Manifest withLayout(String layout) {
        return new Manifest(v, root, size, pieceSize, pieces, table, tableRef, tableRefs, name, mediaType, sealing,
                keyEpoch, hashing, origin, created, attrs, layout);
    }

    /**
     * Checks the invariants a validator enforces on write and on read: the
     * version, the piece-size bounds, the piece count, exactly one of the inline
     * table and the table reference, the table length, the size bounds on
     * name and attributes, and the layout name (PD-X-09). The root itself is
     * checked against the table by the content module, which holds the hashing code.
     *
     * @return the first violated rule, or empty when the manifest is well formed
     */
    public java.util.Optional<String> violation() {
        if (v != VERSION) {
            return java.util.Optional.of("unsupported manifest version " + v);
        }
        if (pieceSize < MIN_PIECE_SIZE || pieceSize > MAX_PIECE_SIZE || Integer.bitCount(pieceSize) != 1) {
            return java.util.Optional.of("pieceSize " + pieceSize + " is not a power of two in [64 KiB, 4 MiB]");
        }
        if (size < 0) {
            return java.util.Optional.of("negative size");
        }
        long expectedPieces = size == 0 ? 0 : (size + pieceSize - 1) / pieceSize;
        if (pieces != expectedPieces) {
            return java.util.Optional.of("pieces " + pieces + " does not match ceil(size / pieceSize) = " + expectedPieces);
        }
        if ((table == null) == (tableRef == null)) {
            return java.util.Optional.of("exactly one of table and tableRef must be present");
        }
        if (table != null) {
            if (pieces > INLINE_TABLE_PIECES) {
                return java.util.Optional.of("inline table with " + pieces + " pieces exceeds " + INLINE_TABLE_PIECES);
            }
            if (table.length != (long) pieces * HASH_BYTES) {
                return java.util.Optional.of("table of " + table.length + " bytes for " + pieces + " pieces");
            }
        }
        if (name != null && name.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_NAME_BYTES) {
            return java.util.Optional.of("name longer than " + MAX_NAME_BYTES + " bytes");
        }
        if (attrs != null) {
            if (attrs.size() > MAX_ATTRS) {
                return java.util.Optional.of("more than " + MAX_ATTRS + " attrs");
            }
            int bytes = 0;
            for (Map.Entry<String, String> e : attrs.entrySet()) {
                bytes += e.getKey().getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                        + e.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            }
            if (bytes > MAX_ATTRS_BYTES) {
                return java.util.Optional.of("attrs larger than " + MAX_ATTRS_BYTES + " bytes");
            }
        }
        if (!SEALING_NONE.equals(sealing) && !SEALING_GROUP_KEY.equals(sealing)) {
            return java.util.Optional.of("unknown sealing " + sealing);
        }
        if (!HASHING_PLAIN.equals(hashing) && !HASHING_SEALED.equals(hashing)) {
            return java.util.Optional.of("unknown hashing " + hashing);
        }
        if (layout != null && !LAYOUT_PIECES.equals(layout) && !LAYOUT_TREE.equals(layout)) {
            return java.util.Optional.of("unknown layout " + layout);
        }
        return java.util.Optional.empty();
    }

    /** The length of piece {@code index}, which is shorter only for the last piece. */
    public int pieceLength(int index) {
        if (index < 0 || index >= pieces) {
            throw new IndexOutOfBoundsException("piece " + index + " of " + pieces);
        }
        long start = (long) index * pieceSize;
        return (int) Math.min(pieceSize, size - start);
    }

    /** A copy sharing every field, with the table array cloned; equality on records is by reference for arrays. */
    public Manifest withTableCopy() {
        return new Manifest(v, root, size, pieceSize, pieces, table == null ? null : table.clone(), tableRef,
                tableRefs, name, mediaType, sealing, keyEpoch, hashing, origin, created, attrs, layout);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Manifest m && v == m.v && root.equals(m.root) && size == m.size
                && pieceSize == m.pieceSize && pieces == m.pieces && java.util.Arrays.equals(table, m.table)
                && Objects.equals(tableRef, m.tableRef) && Objects.equals(tableRefs, m.tableRefs)
                && Objects.equals(name, m.name) && Objects.equals(mediaType, m.mediaType)
                && sealing.equals(m.sealing) && Objects.equals(keyEpoch, m.keyEpoch) && hashing.equals(m.hashing)
                && origin.equals(m.origin) && created == m.created && Objects.equals(attrs, m.attrs)
                && Objects.equals(layout, m.layout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(v, root, size, pieceSize, pieces, java.util.Arrays.hashCode(table), tableRef, tableRefs,
                name, mediaType, sealing, keyEpoch, hashing, origin, created, attrs, layout);
    }

    /** The tags a manifest entry is written with. */
    public Map<String, String> tags() {
        // LB-M-09: the user's attrs ride as tags too, so Template.whereTag finds "the latest model for
        // run X"; root and name keep their meaning when an attr reuses the key.
        Map<String, String> tags = new java.util.LinkedHashMap<>();
        if (attrs != null) {
            tags.putAll(attrs);
        }
        tags.put(ROOT_TAG, root);
        if (name != null) {
            tags.put(NAME_TAG, name);
        } else {
            tags.remove(NAME_TAG);
        }
        return Map.copyOf(tags);
    }
}
