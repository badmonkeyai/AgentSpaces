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

import ai.badmonkey.agentspaces.pdt.Embedded;
import ai.badmonkey.agentspaces.pdt.Kind;
import ai.badmonkey.agentspaces.pdt.Link;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeFormatException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A node of the CHAMP trie (srad/agentspaces-pdt.md §3.2, PD-V-01): kind {@code m}
 * with a datamap of inline entries, a nodemap of children, entries in slot order
 * and children in slot order, or kind {@code mc}, the collision node past level 51,
 * whose keys sort by their bytes. Level {@code d} indexes by bits {@code 5d} to
 * {@code 5d+4} of the key digest. After a removal a child left with one entry and
 * no children inlines into its parent, so the layout is a function of the key set
 * alone. Keys are canonical CBOR bytes; a value is inline bytes, a spilled
 * {@link TreeNode}, or the CID of a spilled node (PD-N-05); a child is a
 * {@code ChampNode} or the CID of one not yet loaded (PD-V-06).
 *
 * <p>A node is immutable once shared. A transient marks the nodes it creates with
 * its edit token and changes them in place; every other node is copied (PD-V-05).
 *
 * <p>Every lazy load carries the level the node sits at (PD-N-04, PD-V-01): a child
 * is loaded through {@link #child(int, int, Ctx)}, which refuses an {@code m} node at
 * or past {@link #COLLISION_LEVEL}, an {@code mc} node before it, a {@code cnt} on a
 * node that is not a root, and an inline key whose digest does not name the slot it
 * sits in at that level, so a drifted producer cannot hand this reader a second root
 * for the same value or a key that {@code keys()} lists and {@code get} cannot find.
 */
final class ChampNode extends TreeNode {

    /** The first level whose keys sit in a collision node: level 51 reads bit 255 and four zero bits. */
    static final int COLLISION_LEVEL = 52;

    /** Returned by {@link #find} for an absent key. */
    static final Object NOT_FOUND = new Object();

    private static final byte[][] NO_KEYS = new byte[0][];
    private static final Object[] NONE = new Object[0];

    final boolean collision;
    int datamap;
    int nodemap;
    byte[][] keys;
    Object[] values;
    Object[] children;
    AtomicReference<Thread> edit;

    /** The entry count, written as {@code cnt} on a root; -1 on a non-root. */
    long cnt = -1;

    ChampNode(boolean collision, int datamap, int nodemap, byte[][] keys, Object[] values, Object[] children,
              AtomicReference<Thread> edit) {
        this.collision = collision;
        this.datamap = datamap;
        this.nodemap = nodemap;
        this.keys = keys;
        this.values = values;
        this.children = children;
        this.edit = edit;
    }

    static ChampNode empty() {
        return new ChampNode(false, 0, 0, NO_KEYS, NONE, NONE, null);
    }

    /** The outcome of one assoc or dissoc. */
    static final class Change {
        boolean added;
        boolean removed;
        boolean replaced;
        Object before;

        boolean happened() {
            return added || removed || replaced;
        }
    }

    // ------------------------------------------------------------------ hashing

    /** The five-bit index of a digest at a level: bits {@code 5d..5d+4}, big-endian, zero past the digest. */
    static int index(byte[] digest, int level) {
        int first = level * 5;
        int idx = 0;
        for (int i = 0; i < 5; i++) {
            int bit = first + i;
            int v = bit < digest.length * 8 ? (digest[bit >>> 3] >>> (7 - (bit & 7))) & 1 : 0;
            idx = (idx << 1) | v;
        }
        return idx;
    }

    private int dataIndex(int bit) {
        return Integer.bitCount(datamap & (bit - 1));
    }

    private int nodeIndex(int bit) {
        return Integer.bitCount(nodemap & (bit - 1));
    }

    boolean singleEntry() {
        return keys.length == 1 && children.length == 0;
    }

    // ------------------------------------------------------------------ reads

    /**
     * The child at a position, loaded and verified on first use.
     *
     * @param i     the child's position in {@link #children}
     * @param level the child's level, one past this node's
     * @param ctx   the context whose loader and hasher the load uses
     */
    ChampNode child(int i, int level, Ctx ctx) {
        Object ref = children[i];
        if (ref instanceof ChampNode node) {
            return node;
        }
        String cid = (String) ref;
        ChampNode loaded = load(cid, level, ctx);
        children[i] = loaded;
        return loaded;
    }

    /** Loads a non-root node at a level through the context's loader and verifies its shape at that level. */
    static ChampNode load(String cid, int level, Ctx ctx) {
        ChampNode node = decode(cid, ctx.loader.load(cid), false);
        node.verifyAt(level, ctx.hasher);
        return node;
    }

    /** Decodes a root node (level 0, {@code cnt} present) and verifies its inline keys. */
    static ChampNode loadRoot(String cid, byte[] bytes, KeyHasher hasher) {
        ChampNode node = decode(cid, bytes, true);
        node.verifyAt(0, hasher);
        return node;
    }

    /**
     * Checks that this node is the kind its level allows and that each inline key's
     * digest names the slot it sits in (PD-N-04, PD-V-01).
     */
    void verifyAt(int level, KeyHasher hasher) {
        if (collision && level < COLLISION_LEVEL) {
            throw new NodeFormatException(cid, "collision node at level " + level + "; keys collide only past level " + (COLLISION_LEVEL - 1));
        }
        if (!collision && level >= COLLISION_LEVEL) {
            throw new NodeFormatException(cid, "map node at level " + level + "; keys past level " + (COLLISION_LEVEL - 1) + " sit in a collision node");
        }
        if (collision) {
            return;
        }
        int remaining = datamap;
        for (int i = 0; i < keys.length; i++) {
            int slot = Integer.numberOfTrailingZeros(remaining);
            remaining &= remaining - 1;
            int named = index(hasher.hash(keys[i]), level);
            if (named != slot) {
                throw new NodeFormatException(cid, "entry " + i + " sits in slot " + slot + " at level " + level
                        + " where its key digest names slot " + named);
            }
        }
    }

    Object find(byte[] digest, int level, byte[] key, Ctx ctx) {
        ChampNode node = this;
        while (true) {
            if (node.collision) {
                for (int i = 0; i < node.keys.length; i++) {
                    if (Arrays.equals(node.keys[i], key)) {
                        return node.values[i];
                    }
                }
                return NOT_FOUND;
            }
            int bit = 1 << index(digest, level);
            if ((node.datamap & bit) != 0) {
                int i = node.dataIndex(bit);
                return Arrays.equals(node.keys[i], key) ? node.values[i] : NOT_FOUND;
            }
            if ((node.nodemap & bit) != 0) {
                level++;
                node = node.child(node.nodeIndex(bit), level, ctx);
                continue;
            }
            return NOT_FOUND;
        }
    }

    /** The deepest level under this node, counting this node as one; {@code level} is this node's level. */
    int depth(int level, Ctx ctx) {
        int deepest = 1;
        for (int i = 0; i < children.length; i++) {
            deepest = Math.max(deepest, 1 + child(i, level + 1, ctx).depth(level + 1, ctx));
        }
        return deepest;
    }

    // ------------------------------------------------------------------ writes

    private ChampNode editable(Ctx ctx) {
        if (ctx.owns(edit)) {
            return this;
        }
        return new ChampNode(collision, datamap, nodemap, keys.clone(), values.clone(), children.clone(), ctx.edit);
    }

    ChampNode assoc(byte[] digest, int level, byte[] key, Object value, Ctx ctx, Change change) {
        if (collision) {
            return assocCollision(key, value, ctx, change);
        }
        int bit = 1 << index(digest, level);
        if ((datamap & bit) != 0) {
            int i = dataIndex(bit);
            if (Arrays.equals(keys[i], key)) {
                if (Spill.sameValue(values[i], value)) {
                    return this;
                }
                change.replaced = true;
                change.before = values[i];
                ChampNode n = editable(ctx);
                n.values[i] = value;
                return n;
            }
            byte[] otherDigest = ctx.hasher.hash(keys[i]);
            ChampNode sub = mergeTwo(keys[i], values[i], otherDigest, key, value, digest, level + 1, ctx);
            change.added = true;
            ChampNode n = editable(ctx);
            n.removeEntryAt(i);
            n.datamap &= ~bit;
            n.nodemap |= bit;
            n.insertChildAt(n.nodeIndex(bit), sub);
            return n;
        }
        if ((nodemap & bit) != 0) {
            int i = nodeIndex(bit);
            ChampNode child = child(i, level + 1, ctx);
            ChampNode next = child.assoc(digest, level + 1, key, value, ctx, change);
            if (!change.happened()) {
                return this;
            }
            if (next == child) {
                return this;
            }
            ChampNode n = editable(ctx);
            n.children[i] = next;
            return n;
        }
        change.added = true;
        ChampNode n = editable(ctx);
        n.datamap |= bit;
        n.insertEntryAt(n.dataIndex(bit), key, value);
        return n;
    }

    private ChampNode assocCollision(byte[] key, Object value, Ctx ctx, Change change) {
        int pos = 0;
        while (pos < keys.length) {
            int order = Arrays.compareUnsigned(keys[pos], key);
            if (order == 0) {
                if (Spill.sameValue(values[pos], value)) {
                    return this;
                }
                change.replaced = true;
                change.before = values[pos];
                ChampNode n = editable(ctx);
                n.values[pos] = value;
                return n;
            }
            if (order > 0) {
                break;
            }
            pos++;
        }
        change.added = true;
        ChampNode n = editable(ctx);
        n.insertEntryAt(pos, key, value);
        return n;
    }

    private static ChampNode mergeTwo(byte[] k1, Object v1, byte[] d1, byte[] k2, Object v2, byte[] d2, int level, Ctx ctx) {
        if (level >= COLLISION_LEVEL) {
            boolean firstLower = Arrays.compareUnsigned(k1, k2) < 0;
            byte[][] ks = firstLower ? new byte[][] {k1, k2} : new byte[][] {k2, k1};
            Object[] vs = firstLower ? new Object[] {v1, v2} : new Object[] {v2, v1};
            return new ChampNode(true, 0, 0, ks, vs, NONE, ctx.edit);
        }
        int i1 = index(d1, level);
        int i2 = index(d2, level);
        if (i1 != i2) {
            boolean firstLower = i1 < i2;
            byte[][] ks = firstLower ? new byte[][] {k1, k2} : new byte[][] {k2, k1};
            Object[] vs = firstLower ? new Object[] {v1, v2} : new Object[] {v2, v1};
            return new ChampNode(false, (1 << i1) | (1 << i2), 0, ks, vs, NONE, ctx.edit);
        }
        ChampNode sub = mergeTwo(k1, v1, d1, k2, v2, d2, level + 1, ctx);
        return new ChampNode(false, 0, 1 << i1, NO_KEYS, NONE, new Object[] {sub}, ctx.edit);
    }

    ChampNode dissoc(byte[] digest, int level, byte[] key, Ctx ctx, Change change) {
        if (collision) {
            for (int i = 0; i < keys.length; i++) {
                if (Arrays.equals(keys[i], key)) {
                    change.removed = true;
                    change.before = values[i];
                    ChampNode n = editable(ctx);
                    n.removeEntryAt(i);
                    return n;
                }
            }
            return this;
        }
        int bit = 1 << index(digest, level);
        if ((datamap & bit) != 0) {
            int i = dataIndex(bit);
            if (!Arrays.equals(keys[i], key)) {
                return this;
            }
            change.removed = true;
            change.before = values[i];
            ChampNode n = editable(ctx);
            n.removeEntryAt(i);
            n.datamap &= ~bit;
            return n;
        }
        if ((nodemap & bit) != 0) {
            int i = nodeIndex(bit);
            ChampNode child = child(i, level + 1, ctx);
            ChampNode next = child.dissoc(digest, level + 1, key, ctx, change);
            if (!change.removed) {
                return this;
            }
            ChampNode n = editable(ctx);
            if (next.singleEntry()) {
                n.removeChildAt(i);
                n.nodemap &= ~bit;
                n.datamap |= bit;
                n.insertEntryAt(n.dataIndex(bit), next.keys[0], next.values[0]);
                return n;
            }
            n.children[i] = next;
            return n;
        }
        return this;
    }

    private void insertEntryAt(int i, byte[] key, Object value) {
        byte[][] ks = new byte[keys.length + 1][];
        Object[] vs = new Object[values.length + 1];
        System.arraycopy(keys, 0, ks, 0, i);
        System.arraycopy(values, 0, vs, 0, i);
        ks[i] = key;
        vs[i] = value;
        System.arraycopy(keys, i, ks, i + 1, keys.length - i);
        System.arraycopy(values, i, vs, i + 1, values.length - i);
        keys = ks;
        values = vs;
    }

    private void removeEntryAt(int i) {
        byte[][] ks = new byte[keys.length - 1][];
        Object[] vs = new Object[values.length - 1];
        System.arraycopy(keys, 0, ks, 0, i);
        System.arraycopy(values, 0, vs, 0, i);
        System.arraycopy(keys, i + 1, ks, i, keys.length - i - 1);
        System.arraycopy(values, i + 1, vs, i, values.length - i - 1);
        keys = ks;
        values = vs;
    }

    private void insertChildAt(int i, Object child) {
        Object[] cs = new Object[children.length + 1];
        System.arraycopy(children, 0, cs, 0, i);
        cs[i] = child;
        System.arraycopy(children, i, cs, i + 1, children.length - i);
        children = cs;
    }

    private void removeChildAt(int i) {
        Object[] cs = new Object[children.length - 1];
        System.arraycopy(children, 0, cs, 0, i);
        System.arraycopy(children, i + 1, cs, i, children.length - i - 1);
        children = cs;
    }

    // ------------------------------------------------------------------ codec

    @Override
    byte[] encode(Flusher flusher) {
        Map<String, Object> node = new LinkedHashMap<>();
        List<Object> entries = new ArrayList<>(keys.length * 2);
        for (int i = 0; i < keys.length; i++) {
            entries.add(new Embedded(keys[i]));
            entries.add(Spill.embed(values[i], flusher));
        }
        if (collision) {
            node.put(NodeCodec.KIND_KEY, Kind.MC);
            node.put("e", entries);
            return NodeCodec.encode(node);
        }
        List<Object> links = new ArrayList<>(children.length);
        for (Object child : children) {
            links.add(new Link(flusher.cidOf(child)));
        }
        node.put(NodeCodec.KIND_KEY, Kind.M);
        node.put("d", Integer.toUnsignedLong(datamap));
        node.put("n", Integer.toUnsignedLong(nodemap));
        node.put("e", entries);
        node.put("c", links);
        if (cnt >= 0) {
            node.put("cnt", cnt);
        }
        return NodeCodec.encode(node);
    }

    /**
     * Decodes a stored node of kind {@code m} or {@code mc}. A root carries {@code cnt},
     * which lands in {@link #cnt}; any other node must not carry it.
     *
     * @param cid   the CID
     * @param bytes the node bytes
     * @param root  whether the node is a map root
     */
    static ChampNode decode(String cid, byte[] bytes, boolean root) {
        Map<String, Object> node = NodeCodec.decode(cid, bytes);
        String kind = (String) node.get(NodeCodec.KIND_KEY);
        if (!Kind.M.equals(kind) && !Kind.MC.equals(kind)) {
            throw new NodeFormatException(cid, "expected a map node, found kind '" + kind + "'");
        }
        List<?> e = node.get("e") instanceof List<?> list ? list : List.of();
        if (e.size() % 2 != 0) {
            throw new NodeFormatException(cid, "entry list has an odd length " + e.size());
        }
        byte[][] keys = new byte[e.size() / 2][];
        Object[] values = new Object[keys.length];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = ((Embedded) e.get(2 * i)).bytes();
            if (Spill.isLinkItem(keys[i])) {
                throw new NodeFormatException(cid, "entry " + i + " has a bare link item as its key; keys never spill");
            }
            values[i] = Spill.refOf(((Embedded) e.get(2 * i + 1)).bytes());
        }
        if (node.containsKey("cnt") && !root) {
            throw new NodeFormatException(cid, "'cnt' on a node that is not a map root");
        }
        if (Kind.MC.equals(kind)) {
            for (int i = 1; i < keys.length; i++) {
                if (Arrays.compareUnsigned(keys[i - 1], keys[i]) >= 0) {
                    throw new NodeFormatException(cid, "collision node keys out of order");
                }
            }
            ChampNode n = new ChampNode(true, 0, 0, keys, values, NONE, null);
            n.cid = cid;
            return n;
        }
        long d = node.get("d") instanceof Long l ? l : 0L;
        long nm = node.get("n") instanceof Long l ? l : 0L;
        if (d > 0xFFFFFFFFL || nm > 0xFFFFFFFFL) {
            throw new NodeFormatException(cid, "datamap or nodemap above 32 bits");
        }
        int datamap = (int) d;
        int nodemap = (int) nm;
        if ((datamap & nodemap) != 0) {
            throw new NodeFormatException(cid, "datamap and nodemap share a slot");
        }
        if (Integer.bitCount(datamap) != keys.length) {
            throw new NodeFormatException(cid, "datamap names " + Integer.bitCount(datamap) + " slots for " + keys.length + " entries");
        }
        List<?> c = node.get("c") instanceof List<?> list ? list : List.of();
        if (Integer.bitCount(nodemap) != c.size()) {
            throw new NodeFormatException(cid, "nodemap names " + Integer.bitCount(nodemap) + " slots for " + c.size() + " children");
        }
        Object[] children = new Object[c.size()];
        for (int i = 0; i < children.length; i++) {
            children[i] = ((Link) c.get(i)).cid();
        }
        ChampNode n = new ChampNode(false, datamap, nodemap, keys, values, children, null);
        if (node.get("cnt") instanceof Long count) {
            n.cnt = count;
        }
        n.cid = cid;
        return n;
    }
}
