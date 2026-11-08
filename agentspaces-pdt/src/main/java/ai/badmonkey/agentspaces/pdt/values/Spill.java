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
package ai.badmonkey.agentspaces.pdt.values;

import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.Embedded;
import ai.badmonkey.agentspaces.pdt.Kind;
import ai.badmonkey.agentspaces.pdt.Link;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeFormatException;

import java.io.ByteArrayOutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;

/**
 * The spill rules of srad/agentspaces-pdt.md §3.1 and PD-N-05. An embedded value
 * whose CBOR exceeds {@link #VALUE_LIMIT} (1 KiB) is held in its own {@code x} node
 * and the value slot carries a link to it. Above {@link #TREE_LIMIT} (4 KiB) a map
 * becomes a CHAMP sub-tree of kind {@code m} and an array a vector sub-tree of kind
 * {@code vr}, each entry or element subject to the same rules again, so a change
 * inside a nested value copies only the nested path and the outer path.
 *
 * <p>The link in a value slot is the same tag-42 item a link field uses, so a node
 * stays valid DAG-CBOR. The rule is total: a bare link item in a value slot always
 * names a spilled node, so an application value or key is never a bare link item,
 * and {@link #spill} and {@link #checkKey} refuse one with an
 * {@link IllegalArgumentException}; hold a CID as text instead. Reassembly
 * reproduces the bytes {@code CborCodec.toBytes} produced: a map comes back in the
 * indefinite-length form with keys in DAG-CBOR order, an array in the definite form,
 * and a byte string in the definite form. A map in the definite form or an array in
 * the indefinite form therefore stays whole in an {@code x} node. A byte string
 * above {@link #TREE_LIMIT} becomes the blob tree of §3.3 through {@link BlobNode}
 * ({@code raw} chunks under {@code b} nodes, or a single chunk), and this class
 * reads a spilled blob back into a byte string.
 *
 * <p>An indefinite-length map whose keys are not in canonical order is a record
 * (the codec writes a record's fields in their declared order, §10.2) and a CHAMP
 * sub-tree would hand it back sorted. Such a map spills to an {@code x} node whose
 * {@code v} is the link to a {@code vr} vector of its keys and values in order, each
 * under the same rules again, so a record's large fields still share between versions
 * and its bytes come back as written (PD-E-03). An {@code x} node whose {@code v} is a
 * bare link therefore names a spilled value whose outer form the linked kind decides:
 * a {@code vr} is an ordered map, a {@code raw} or {@code b} node is a byte string
 * (the form {@code PayloadTree} gives a single-chunk byte string, so a payload root
 * stays dag-cbor).
 *
 * <p>A value that would need an {@code x} node over the 1 MiB node bound of PD-N-04
 * is refused on the way in ({@link #MAX_X_VALUE}), so a map never holds a value it
 * cannot write. Resolution of nested spills is bounded by
 * {@link NodeCodec#MAX_DEPTH} levels and a blob read by the same tree depth, so a
 * crafted chain of nodes ends in a {@link NodeFormatException} naming the CID.
 *
 * <p>In memory a value reference is {@code byte[]} (inline), a {@link TreeNode}
 * (spilled, perhaps unhashed), or a {@link String} CID (spilled and still cold).
 */
public final class Spill {

    /** A value above this many CBOR bytes leaves its node (PD-N-05). */
    public static final int VALUE_LIMIT = 1024;

    /** A map, array, or byte string above this many CBOR bytes becomes a sub-tree (PD-N-05). */
    public static final int TREE_LIMIT = 4096;

    /** The largest value an {@code x} node can hold: the node bound of PD-N-04 less the node's seven bytes of framing. */
    public static final int MAX_X_VALUE = NodeCodec.MAX_NODE_BYTES - 7;

    private static final int LINK_ITEM_LENGTH = 5 + Cids.BINARY_LENGTH;
    private static final int MAJOR_BYTES = 2;
    private static final int MAJOR_ARRAY = 4;
    private static final int MAJOR_MAP = 5;
    private static final int INDEFINITE = 31;
    private static final int BREAK = 0xff;

    private Spill() {
    }

    // ------------------------------------------------------------------ link items

    /** The CBOR item a value slot carries for a spilled node: tag 42 over 0x00 and the binary CID. */
    public static byte[] linkItem(String cid) {
        byte[] binary = Cids.binary(cid);
        byte[] out = new byte[LINK_ITEM_LENGTH];
        out[0] = (byte) 0xd8;
        out[1] = 0x2a;
        out[2] = 0x58;
        out[3] = 0x25;
        out[4] = 0x00;
        System.arraycopy(binary, 0, out, 5, binary.length);
        return out;
    }

    /** Whether an embedded item is a link to a spilled node. */
    public static boolean isLinkItem(byte[] item) {
        return item.length == LINK_ITEM_LENGTH && (item[0] & 0xff) == 0xd8 && item[1] == 0x2a
                && item[2] == 0x58 && item[3] == 0x25 && item[4] == 0x00;
    }

    /**
     * Refuses a key that is a bare link item: keys never spill, and a link-shaped key
     * would read as a link of the node.
     *
     * @param key the key's CBOR
     * @throws IllegalArgumentException when the key is a bare link item
     */
    public static void checkKey(byte[] key) {
        if (isLinkItem(key)) {
            throw new IllegalArgumentException("a key must not be a bare link item (CBOR tag 42 over a CID): "
                    + "a link item in a map node names a spilled node (PD-N-05); hold a CID as text");
        }
    }

    /** The CID a link item names. */
    public static String linkCid(byte[] item) {
        return Cids.fromBinary(Arrays.copyOfRange(item, 5, LINK_ITEM_LENGTH));
    }

    /** The in-memory reference of a decoded value slot: the bytes, or the CID of a spilled node. */
    static Object refOf(byte[] item) {
        return isLinkItem(item) ? linkCid(item) : item;
    }

    /** The embedded item of a value reference, hashing a spilled node through the flusher. */
    static Embedded embed(Object ref, Flusher flusher) {
        if (ref instanceof byte[] inline) {
            return new Embedded(inline);
        }
        return new Embedded(linkItem(flusher.cidOf(ref)));
    }

    /** Whether two references hold the same value without loading anything. */
    static boolean sameValue(Object a, Object b) {
        if (a == b) {
            return true;
        }
        if (a instanceof byte[] x && b instanceof byte[] y) {
            return Arrays.equals(x, y);
        }
        if (a instanceof XNode x && b instanceof XNode y) {
            return Arrays.equals(x.value, y.value);
        }
        if (a instanceof BlobNode x && b instanceof BlobNode y) {
            return Arrays.equals(x.content, y.content);
        }
        return false;
    }

    /** The CID of a spilled reference, or null for inline bytes; a node must be hashed first. */
    static String cidOf(Object ref) {
        if (ref instanceof String s) {
            return s;
        }
        if (ref instanceof TreeNode node) {
            return node.cid;
        }
        return null;
    }

    /** Whether two hashed references hold equal values: inline bytes compare, spilled nodes compare by CID. */
    static boolean equalHashed(Object a, Object b) {
        if (a instanceof byte[] x) {
            return b instanceof byte[] y && Arrays.equals(x, y);
        }
        String ca = cidOf(a);
        String cb = cidOf(b);
        return ca != null && ca.equals(cb);
    }

    // ------------------------------------------------------------------ spilling

    /**
     * Applies the rules to one encoded value.
     *
     * @param cbor the canonical CBOR of the value
     * @param ctx  the context whose loader and hasher a sub-tree uses
     * @return the bytes when they fit inline, else the new spill node
     */
    static Object spill(byte[] cbor, Ctx ctx) {
        if (isLinkItem(cbor)) {
            throw new IllegalArgumentException("a value must not be a bare link item (CBOR tag 42 over a CID): "
                    + "a link item in a value slot names a spilled node (PD-N-05); hold a CID as text");
        }
        if (cbor.length <= VALUE_LIMIT) {
            return cbor;
        }
        int major = (cbor[0] & 0xff) >> 5;
        int info = cbor[0] & 0x1f;
        if (cbor.length > TREE_LIMIT) {
            if (major == MAJOR_MAP && info == INDEFINITE) {
                return canonicalKeys(cbor) ? subMap(cbor, ctx) : subOrdered(cbor, ctx);
            }
            if (major == MAJOR_ARRAY && info != INDEFINITE) {
                return subVector(cbor, ctx);
            }
            if (major == MAJOR_BYTES && info != INDEFINITE) {
                return new BlobNode(Arrays.copyOfRange(cbor, headerLength(info), cbor.length));
            }
        }
        if (cbor.length > MAX_X_VALUE) {
            throw new IllegalArgumentException("a value of " + cbor.length + " CBOR bytes exceeds the " + MAX_X_VALUE
                    + " bytes an x node can hold under the 1 MiB node bound (PD-N-04)");
        }
        return new XNode(cbor);
    }

    /** The length of a definite CBOR header with the given additional information. */
    private static int headerLength(int info) {
        return info < 24 ? 1 : 1 + (1 << (info - 24));
    }

    private static ChampNode subMap(byte[] cbor, Ctx ctx) {
        Ctx sub = new Ctx(ctx.loader, KeyHasher.SHA256, ctx.edit);
        ChampNode root = ChampNode.empty();
        long count = 0;
        int p = 1;
        ChampNode.Change change = new ChampNode.Change();
        while (p < cbor.length && (cbor[p] & 0xff) != BREAK) {
            int keyEnd = NodeCodec.skip(cbor, p);
            int valueEnd = NodeCodec.skip(cbor, keyEnd);
            byte[] key = Arrays.copyOfRange(cbor, p, keyEnd);
            byte[] value = Arrays.copyOfRange(cbor, keyEnd, valueEnd);
            checkKey(key);
            change.added = false;
            change.replaced = false;
            root = root.assoc(sub.hasher.hash(key), 0, key, spill(value, sub), sub, change);
            if (change.added) {
                count++;
            }
            p = valueEnd;
        }
        root.cnt = count;
        return root;
    }

    /**
     * Whether an indefinite map's keys come in canonical order (shorter first, then
     * bytewise), the order {@code CborCodec} gives a map and a CHAMP reassembly restores.
     */
    static boolean canonicalKeys(byte[] cbor) {
        int p = 1;
        byte[] previous = null;
        while (p < cbor.length && (cbor[p] & 0xff) != BREAK) {
            int keyEnd = NodeCodec.skip(cbor, p);
            byte[] key = Arrays.copyOfRange(cbor, p, keyEnd);
            if (previous != null && compareKeys(previous, key) >= 0) {
                return false;
            }
            previous = key;
            p = NodeCodec.skip(cbor, keyEnd);
        }
        return true;
    }

    private static int compareKeys(byte[] a, byte[] b) {
        if (a.length != b.length) {
            return Integer.compare(a.length, b.length);
        }
        return Arrays.compareUnsigned(a, b);
    }

    /** The ordered-map spill: a vector of the keys and values in the order written, under an {@code x} envelope. */
    private static OrderedNode subOrdered(byte[] cbor, Ctx ctx) {
        VecRoot items = VecRoot.empty();
        int p = 1;
        while (p < cbor.length && (cbor[p] & 0xff) != BREAK) {
            int keyEnd = NodeCodec.skip(cbor, p);
            int valueEnd = NodeCodec.skip(cbor, keyEnd);
            byte[] key = Arrays.copyOfRange(cbor, p, keyEnd);
            checkKey(key);
            items = items.conj(spill(key, ctx), ctx);
            items = items.conj(spill(Arrays.copyOfRange(cbor, keyEnd, valueEnd), ctx), ctx);
            p = valueEnd;
        }
        return new OrderedNode(items);
    }

    /**
     * A spilled map in its written order: an {@code x} node whose value is the link to the
     * {@code vr} of its keys and values. Hashing the vector first gives the link.
     */
    static final class OrderedNode extends TreeNode {
        final VecRoot items;

        OrderedNode(VecRoot items) {
            this.items = items;
        }

        @Override
        byte[] encode(Flusher flusher) {
            return new XNode(linkItem(flusher.cidOf(items))).encode(flusher);
        }
    }

    private static VecRoot subVector(byte[] cbor, Ctx ctx) {
        int info = cbor[0] & 0x1f;
        int p = 1;
        long n;
        if (info < 24) {
            n = info;
        } else {
            int width = 1 << (info - 24);
            n = 0;
            for (int i = 0; i < width; i++) {
                n = (n << 8) | (cbor[p++] & 0xff);
            }
        }
        VecRoot root = VecRoot.empty();
        for (long i = 0; i < n; i++) {
            int end = NodeCodec.skip(cbor, p);
            root = root.conj(spill(Arrays.copyOfRange(cbor, p, end), ctx), ctx);
            p = end;
        }
        return root;
    }

    // ------------------------------------------------------------------ resolving

    /**
     * The CBOR bytes of a value reference, loading and reassembling a spilled node.
     *
     * @param ref the reference
     * @param ctx the context whose loader reads cold nodes
     * @return the value's canonical CBOR
     */
    static byte[] resolve(Object ref, Ctx ctx) {
        return resolve(ref, ctx, 0);
    }

    /** Resolves at a nesting depth of spilled sub-trees; past {@link NodeCodec#MAX_DEPTH} the chain is refused. */
    private static byte[] resolve(Object ref, Ctx ctx, int depth) {
        if (ref instanceof byte[] inline) {
            return inline;
        }
        if (ref instanceof BlobNode blob) {
            return byteString(blob.content);
        }
        if (depth > NodeCodec.MAX_DEPTH) {
            throw new NodeFormatException(cidOf(ref), "spilled values nested deeper than " + NodeCodec.MAX_DEPTH + " levels");
        }
        if (ref instanceof XNode x) {
            return isLinkItem(x.value) ? resolveLinked(linkCid(x.value), ctx, depth + 1) : x.value;
        }
        if (ref instanceof OrderedNode ordered) {
            return reassembleOrdered(ordered.items, ctx, depth);
        }
        if (ref instanceof ChampNode map) {
            return reassembleMap(map, ctx, depth);
        }
        if (ref instanceof VecRoot vector) {
            return reassembleArray(vector, ctx, depth);
        }
        String cid = (String) ref;
        byte[] bytes = ctx.loader.load(cid);
        if (Cids.parse(cid).codec() == Cids.RAW) {
            return byteString(bytes);
        }
        String kind = NodeCodec.kindOf(bytes);
        return switch (kind) {
            case Kind.X -> {
                XNode x = XNode.decode(cid, NodeCodec.decode(cid, bytes));
                yield isLinkItem(x.value) ? resolveLinked(linkCid(x.value), ctx, depth + 1) : x.value;
            }
            case Kind.M -> reassembleMap(ChampNode.loadRoot(cid, bytes, ctx.hasher), ctx, depth);
            case Kind.VR -> reassembleArray(VecRoot.decode(cid, bytes), ctx, depth);
            case Kind.B -> byteString(readBlob(cid, bytes, ctx));
            default -> throw new NodeFormatException(cid, "a spilled value cannot be a node of kind '" + kind + "'");
        };
    }

    /**
     * The value an {@code x} envelope links: an ordered map when the linked node is a
     * {@code vr}, a byte string when it is a chunk or a {@code b} node.
     */
    private static byte[] resolveLinked(String cid, Ctx ctx, int depth) {
        if (depth > NodeCodec.MAX_DEPTH) {
            throw new NodeFormatException(cid, "spilled values nested deeper than " + NodeCodec.MAX_DEPTH + " levels");
        }
        byte[] bytes = ctx.loader.load(cid);
        if (Cids.parse(cid).codec() == Cids.RAW) {
            return byteString(bytes);
        }
        String kind = NodeCodec.kindOf(bytes);
        return switch (kind) {
            case Kind.VR -> reassembleOrdered(VecRoot.decode(cid, bytes), ctx, depth);
            case Kind.B -> byteString(readBlob(cid, bytes, ctx));
            default -> throw new NodeFormatException(cid, "an x envelope links a vr, b, or raw node, not kind '" + kind + "'");
        };
    }

    private static byte[] reassembleOrdered(VecRoot items, Ctx ctx, int depth) {
        if (items.n % 2 != 0) {
            throw new NodeFormatException(items.cid, "an ordered map holds keys and values in pairs, found " + items.n + " items");
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0xbf);
        for (int i = 0; i < items.n; i++) {
            out.writeBytes(resolve(items.nth(i, ctx), ctx, depth + 1));
        }
        out.write(BREAK);
        return out.toByteArray();
    }

    private static byte[] reassembleMap(ChampNode root, Ctx ctx, int depth) {
        List<RawMap.RawEntry> entries = new ArrayList<>();
        RawMap.RawIterator it = new RawMap.RawIterator(root, ctx);
        while (it.hasNext()) {
            entries.add(it.next());
        }
        entries.sort((a, b) -> {
            if (a.key().length != b.key().length) {
                return Integer.compare(a.key().length, b.key().length);
            }
            return Arrays.compareUnsigned(a.key(), b.key());
        });
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0xbf);
        for (RawMap.RawEntry entry : entries) {
            out.writeBytes(entry.key());
            out.writeBytes(resolve(entry.value(), ctx, depth + 1));
        }
        out.write(BREAK);
        return out.toByteArray();
    }

    private static byte[] reassembleArray(VecRoot root, Ctx ctx, int depth) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        header(out, MAJOR_ARRAY, root.n);
        for (int i = 0; i < root.n; i++) {
            out.writeBytes(resolve(root.nth(i, ctx), ctx, depth + 1));
        }
        return out.toByteArray();
    }

    private static byte[] byteString(byte[] raw) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(raw.length + 9);
        header(out, MAJOR_BYTES, raw.length);
        out.writeBytes(raw);
        return out.toByteArray();
    }

    /** Reads a stored blob tree depth first with an explicit stack, bounded by the tree depth of PD-N-04. */
    private static byte[] readBlob(String root, byte[] rootBytes, Ctx ctx) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Deque<String> cids = new ArrayDeque<>();
        Deque<Integer> depths = new ArrayDeque<>();
        cids.push(root);
        depths.push(1);
        while (!cids.isEmpty()) {
            String cid = cids.pop();
            int depth = depths.pop();
            byte[] bytes = cid.equals(root) ? rootBytes : ctx.loader.load(cid);
            if (Cids.parse(cid).codec() == Cids.RAW) {
                out.writeBytes(bytes);
                continue;
            }
            if (depth >= NodeCodec.MAX_DEPTH) {
                throw new NodeFormatException(cid, "tree deeper than " + NodeCodec.MAX_DEPTH + " levels");
            }
            Map<String, Object> node = NodeCodec.decode(cid, bytes);
            if (!Kind.B.equals(node.get(NodeCodec.KIND_KEY))) {
                throw new NodeFormatException(cid, "expected a blob node, found kind '" + node.get(NodeCodec.KIND_KEY) + "'");
            }
            List<?> children = node.get("c") instanceof List<?> list ? list : List.of();
            for (int i = children.size() - 1; i >= 0; i--) {
                cids.push(((Link) children.get(i)).cid());
                depths.push(depth + 1);
            }
        }
        return out.toByteArray();
    }

    private static void header(ByteArrayOutputStream out, int major, long value) {
        int m = major << 5;
        if (value < 24) {
            out.write(m | (int) value);
        } else if (value < 0x100) {
            out.write(m | 24);
            out.write((int) value);
        } else if (value < 0x10000) {
            out.write(m | 25);
            out.write((int) (value >>> 8));
            out.write((int) value);
        } else {
            out.write(m | 26);
            for (int shift = 24; shift >= 0; shift -= 8) {
                out.write((int) (value >>> shift));
            }
        }
    }
}
