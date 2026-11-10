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
import ai.badmonkey.agentspaces.pdt.kinds.RvNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A node of the vector trie, kind {@code v} (srad/agentspaces-pdt.md §3.2, PD-V-03):
 * an interior node holds up to 32 child references ({@code c}), a leaf holds up to
 * 32 element references ({@code e}). Interior slots are exactly sized, so a
 * right-edge interior node is shorter than 32; a leaf in the trie is always full,
 * because only a full tail is pushed into the trie and the tail holds the rest. A
 * transient marks the nodes it creates with its edit token and changes them in
 * place (PD-V-05).
 *
 * <p>A stored node is checked on decode (a leaf of exactly 32 items, 1 to 32
 * children) and on every slot access (PD-N-04), so a trie narrower than its root's
 * count claims is refused by name before any index fails.
 *
 * <p>PDT 2 phase 6 (P2-R-01, P2-R-04; srad/agentspaces-pdt2.md §3.7). Under an {@code rr} root a
 * leaf holds 1 to 32 items, and an interior node whose children are not all full, the last one
 * aside, is relaxed: it carries the cumulative element counts of its children ({@link #sizes}) and
 * encodes as kind {@code rv} with {@code z}. Every other interior node stays a {@code v} node, so
 * a node has one encoding and a dense sub-tree a relaxed vector shares with a {@code vr} vector
 * keeps its CID. {@link #decodeRelaxed} reads both kinds at a known level.
 */
final class VecNode extends TreeNode {

    static final int WIDTH = 32;
    static final int BITS = 5;
    static final int MASK = WIDTH - 1;

    final boolean leaf;
    Object[] slots;
    /**
     * PDT 2 phase 6 (P2-R-01): the cumulative element counts of a relaxed interior node, one per
     * child; null on a leaf and on a dense node. Never changed in place.
     */
    final int[] sizes;
    AtomicReference<Thread> edit;

    VecNode(boolean leaf, Object[] slots, AtomicReference<Thread> edit) {
        this(leaf, slots, null, edit);
    }

    VecNode(boolean leaf, Object[] slots, int[] sizes, AtomicReference<Thread> edit) {
        this.leaf = leaf;
        this.slots = slots;
        this.sizes = sizes;
        this.edit = edit;
    }

    VecNode editable(Ctx ctx) {
        if (ctx.owns(edit)) {
            return this;
        }
        return new VecNode(leaf, slots.clone(), sizes, ctx.edit);
    }

    /** The slot at an index, refused by name when the node is narrower than the index expects. */
    Object item(int i) {
        if (i < 0 || i >= slots.length) {
            throw new NodeFormatException(cid, "vector node has " + slots.length + " slots where index " + i + " was expected");
        }
        return slots[i];
    }

    VecNode child(int i, Ctx ctx) {
        Object ref = item(i);
        if (ref instanceof VecNode node) {
            return node;
        }
        String cid = (String) ref;
        VecNode loaded = decode(cid, ctx.loader.load(cid));
        slots[i] = loaded;
        return loaded;
    }

    /** A copy with one more slot at the end. */
    VecNode appended(Object slot, Ctx ctx) {
        VecNode n = editable(ctx);
        Object[] grown = new Object[slots.length + 1];
        System.arraycopy(slots, 0, grown, 0, slots.length);
        grown[slots.length] = slot;
        n.slots = grown;
        return n;
    }

    /** A copy without its last slot. */
    VecNode shrunk(Ctx ctx) {
        VecNode n = editable(ctx);
        Object[] cut = new Object[slots.length - 1];
        System.arraycopy(slots, 0, cut, 0, cut.length);
        n.slots = cut;
        return n;
    }

    VecNode withSlot(int i, Object slot, Ctx ctx) {
        VecNode n = editable(ctx);
        n.slots[i] = slot;
        return n;
    }

    @Override
    byte[] encode(Flusher flusher) {
        Map<String, Object> node = new LinkedHashMap<>();
        // PDT 2 phase 6 (P2-R-01): a relaxed node is kind rv and carries its size table
        node.put(NodeCodec.KIND_KEY, sizes == null ? Kind.V : Kind.RV);
        List<Object> list = new ArrayList<>(slots.length);
        if (leaf) {
            for (Object item : slots) {
                list.add(Spill.embed(item, flusher));
            }
            node.put("e", list);
        } else {
            for (Object child : slots) {
                list.add(new Link(flusher.cidOf(child)));
            }
            node.put("c", list);
            if (sizes != null) {
                List<Long> z = new ArrayList<>(sizes.length);
                for (int size : sizes) {
                    z.add((long) size);
                }
                node.put("z", z);
            }
        }
        return NodeCodec.encode(node);
    }

    static VecNode decode(String cid, byte[] bytes) {
        VecNode node = decodeV(cid, bytes);
        if (node.leaf && node.slots.length != WIDTH) {
            throw new NodeFormatException(cid, "vector leaf with " + node.slots.length + " items; a leaf in the trie holds exactly 32");
        }
        return node;
    }

    /** A {@code v} node of 1 to 32 slots. */
    private static VecNode decodeV(String cid, byte[] bytes) {
        Map<String, Object> node = NodeCodec.decode(cid, bytes);
        if (!Kind.V.equals(node.get(NodeCodec.KIND_KEY))) {
            throw new NodeFormatException(cid, "expected a vector node, found kind '" + node.get(NodeCodec.KIND_KEY) + "'");
        }
        boolean hasC = node.get("c") instanceof List<?>;
        boolean hasE = node.get("e") instanceof List<?>;
        if (hasC == hasE) {
            throw new NodeFormatException(cid, "a vector node holds either 'c' or 'e'");
        }
        List<?> list = (List<?>) node.get(hasC ? "c" : "e");
        if (list.isEmpty() || list.size() > WIDTH) {
            throw new NodeFormatException(cid, "vector node with " + list.size() + " slots; 1 to 32 expected");
        }
        Object[] slots = new Object[list.size()];
        for (int i = 0; i < slots.length; i++) {
            slots[i] = hasC ? ((Link) list.get(i)).cid() : Spill.refOf(((Embedded) list.get(i)).bytes());
        }
        VecNode n = new VecNode(hasE, slots, null);
        n.cid = cid;
        return n;
    }

    /**
     * Reads a node of a relaxed tree at a known level (PDT 2 phase 6, P2-R-04): a {@code v} leaf of
     * 1 to 32 items at level 0, a dense {@code v} interior node above it, or an {@code rv} node whose
     * size table describes a relaxed node. The reader refuses by name an {@code rv} node without
     * {@code z} (a dense interior node is a {@code v} node), a size table a radix index would read
     * the same way, and a child that holds more elements than its level holds, so a size table
     * never routes an index outside the tree. The PDT 2 phase 6 review added the count its parent
     * gives it: a leaf holds exactly that many elements, a relaxed node's size table ends at it,
     * and a dense node has the number of children it implies, so every count a reader derives
     * from the root down matches the nodes below and an index and an iteration read the same
     * elements.
     *
     * @param cid   the CID, named in a refusal
     * @param bytes the node bytes
     * @param level the node's level: 0 for a leaf, else the shift of its children's index bits
     * @param count the elements its parent counts under it
     * @return the node
     */
    static VecNode decodeRelaxed(String cid, byte[] bytes, int level, int count) {
        VecNode node = Kind.RV.equals(NodeCodec.kindOf(bytes)) ? decodeRv(cid, bytes, level) : decodeDense(cid, bytes, level);
        int width = node.slots.length;
        long capacity = 1L << level;
        boolean fits = node.sizes != null ? node.sizes[width - 1] == count
                : (width - 1) * capacity < count && count <= width * capacity;
        if (!fits) {
            throw new NodeFormatException(cid, "a vector node of " + width + " slots at level " + level
                    + " does not hold the " + count + " elements its parent counts under it");
        }
        return node;
    }

    private static VecNode decodeDense(String cid, byte[] bytes, int level) {
        VecNode node = decodeV(cid, bytes);
        if (node.leaf != (level == 0)) {
            throw new NodeFormatException(cid, "a vector " + (node.leaf ? "leaf" : "interior node") + " at level " + level);
        }
        return node;
    }

    private static VecNode decodeRv(String cid, byte[] bytes, int level) {
        if (level == 0) {
            throw new NodeFormatException(cid, "an rv node where a vector leaf belongs");
        }
        RvNode rv = RvNode.decode(cid, bytes);
        if (rv.sizes() == null) {
            throw new NodeFormatException(cid, "an rv node without z; a dense interior node is a v node");
        }
        int children = rv.children().size();
        int[] sizes = new int[children];
        long capacity = 1L << level;
        long previous = 0;
        boolean dense = true;
        for (int i = 0; i < children; i++) {
            long size = rv.sizes().get(i);
            if (size > Integer.MAX_VALUE) {
                throw new NodeFormatException(cid, "an rv size of " + size + " passes the 2^31 - 1 elements a vector holds");
            }
            if (size - previous > capacity) {
                throw new NodeFormatException(cid, "an rv child holds " + (size - previous) + " elements, more than the "
                        + capacity + " its level holds");
            }
            dense &= i == children - 1 || size - previous == capacity;
            sizes[i] = (int) size;
            previous = size;
        }
        if (dense) {
            throw new NodeFormatException(cid, "a size table on a dense node; a dense interior node is a v node");
        }
        Object[] slots = new Object[children];
        for (int i = 0; i < children; i++) {
            slots[i] = rv.children().get(i).cid();
        }
        VecNode node = new VecNode(false, slots, sizes, null);
        node.cid = cid;
        return node;
    }
}
