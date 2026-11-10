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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * The operations of an RRB vector over {@link VecRoot} and {@link VecNode} (PDT 2 phase 6, P2-R-02
 * to P2-R-04; srad/agentspaces-pdt2.md §3.7, after Bagwell and Rompf's relaxed radix balanced tree).
 *
 * <p>A relaxed interior node carries the cumulative counts of its children, so an index finds its
 * child by a binary search of that table, and a dense node finds it by radix. {@code slice} copies
 * the nodes on its two cut paths. {@code join} descends the right edge of its left tree and the
 * left edge of its right tree to the leaves, merges the two leaves at the seam when they fit one
 * leaf, splits them evenly when either holds less than half of one, and packs the children around
 * the seam into one or two new nodes per level, split evenly when they pass 32, so a join copies
 * O(depth) nodes and never more than two per level. An append (the tail pushed as a leaf) joins
 * without touching the leaves and fills the left node of a split, as the {@code vr} layout does,
 * so a dense vector stays dense. {@code splice} is two slices and two joins; {@code insertAt} is a
 * splice of one element.
 *
 * <p>The PDT 2 phase 6 review replaced the full left node of a seam split with the even split.
 * A split that fills its left node leaves a node of one child beside it, and the next insertion
 * at the same place splits the full node again, so every level split as often as the leaves and
 * the tree gained a level every 500 insertions at one index: 100,000 elements reached shift 35
 * after 2,000 insertions and failed after 2,300. An even split leaves every node it makes at
 * least half full, so a level splits once per 16 splits below it and the depth stays
 * logarithmic. A tree that would still pass {@link #MAX_SHIFT}, which only a tree of thin nodes
 * another port wrote can reach, packs.
 *
 * <p>Every result is normalized the same way: a root above level 5 with one child collapses, the
 * rightmost leaf becomes the tail, and the root is relaxed only when its tree lacks the
 * {@code vr} shape (a dense right edge, full leaves, the tail rule, and the minimal shift), so an
 * edit that leaves the shape of a dense vector writes the {@code vr} root of the same elements.
 * {@link #pack} rebuilds the leftwise-dense {@code vr} tree of the same elements.
 *
 * <p>Nothing here changes a node in place: every new node and root holds fresh arrays and carries
 * the context's edit token, so a transient that continues on a dense result may change exactly the
 * nodes these operations made for it.
 */
final class Rrb {

    private static final int WIDTH = VecNode.WIDTH;
    private static final int BITS = VecNode.BITS;
    private static final int HALF = WIDTH / 2;
    /** The deepest shift a reader reads (P2-R-04), which the {@code vr} layout of 2^31 - 1 elements needs. */
    static final int MAX_SHIFT = 30;

    private Rrb() {
    }

    /**
     * A sub-tree: a reference to a node at a level, and the elements under it.
     *
     * @param ref   a {@link VecNode} or the CID of one
     * @param level 0 for a leaf, else the shift of its children's index bits
     * @param count the elements under it
     */
    record Sub(Object ref, int level, int count) {
    }

    // ------------------------------------------------------------------ loading and counting

    /** A node at a level, read and checked against the elements its parent counts under it (P2-R-04). */
    private static VecNode load(Sub sub, Ctx ctx) {
        if (sub.ref instanceof VecNode node) {
            return node;
        }
        String cid = (String) sub.ref;
        return VecNode.decodeRelaxed(cid, ctx.loader.load(cid), sub.level, sub.count);
    }

    /** Child {@code j} of a sub-tree, loaded and kept in its parent's slot. */
    private static Sub kid(Sub sub, int j, Ctx ctx) {
        VecNode node = load(sub, ctx);
        Sub kid = new Sub(node.item(j), sub.level - BITS, childCount(node, j, sub.level, sub.count));
        if (kid.ref instanceof VecNode) {
            return kid;
        }
        VecNode loaded = load(kid, ctx);
        node.slots[j] = loaded;
        return new Sub(loaded, kid.level, kid.count);
    }

    /** The trie of a root as a sub-tree, loaded and kept on the root; null when the trie is empty. */
    private static Sub trieSub(VecRoot root, Ctx ctx) {
        if (root.trie == null) {
            return null;
        }
        Sub trie = new Sub(root.trie, root.shift, root.trieCount());
        VecNode loaded = load(trie, ctx);
        root.trie = loaded;
        return new Sub(loaded, trie.level, trie.count);
    }

    /**
     * The elements under child {@code j} of a node at a level that holds {@code count} elements:
     * from the size table when relaxed, else a full child but the last, which holds the rest.
     */
    private static int childCount(VecNode node, int j, int level, int count) {
        if (node.sizes != null) {
            return node.sizes[j] - (j == 0 ? 0 : node.sizes[j - 1]);
        }
        int last = node.slots.length - 1;
        return j < last ? 1 << level : (int) (count - ((long) last << level));
    }

    /** The child of a node at a level that holds a local index: by radix when dense, by binary search when relaxed. */
    private static int locate(VecNode node, int level, int index) {
        if (node.sizes == null) {
            return index >>> level;
        }
        int[] z = node.sizes;
        int lo = 0;
        int hi = z.length - 1;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (z[mid] > index) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        return lo;
    }

    /** The elements before child {@code j} of a node at a level. */
    private static int before(VecNode node, int level, int j) {
        if (node.sizes == null) {
            return j << level;
        }
        return j == 0 ? 0 : node.sizes[j - 1];
    }

    private static List<Sub> kids(Sub sub, Ctx ctx) {
        VecNode node = load(sub, ctx);
        List<Sub> out = new ArrayList<>(node.slots.length);
        for (int j = 0; j < node.slots.length; j++) {
            out.add(new Sub(node.slots[j], sub.level - BITS, childCount(node, j, sub.level, sub.count)));
        }
        return out;
    }

    // ------------------------------------------------------------------ reads

    /** The element reference at an index of a relaxed root. */
    static Object nth(VecRoot root, int index, Ctx ctx) {
        if (index < 0 || index >= root.n) {
            throw new IndexOutOfBoundsException("index " + index + " of a vector of " + root.n);
        }
        int off = root.trieCount();
        if (index >= off) {
            return root.tail[index - off];
        }
        int[] start = new int[1];
        VecNode leaf = leafAt(root, index, ctx, start);
        return leaf.item(index - start[0]);
    }

    /** The leaf of a relaxed root that holds a trie index; {@code start[0]} receives the index of its first element. */
    static VecNode leafAt(VecRoot root, int index, Ctx ctx, int[] start) {
        Sub at = trieSub(root, ctx);
        int base = 0;
        while (at.level > 0) {
            VecNode node = load(at, ctx);
            int j = locate(node, at.level, index - base);
            base += before(node, at.level, j);
            at = kid(at, j, ctx);
        }
        start[0] = base;
        return load(at, ctx);
    }

    /** Every element reference of a root, dense or relaxed, in index order. */
    static void forEach(VecRoot root, Ctx ctx, Consumer<Object> out) {
        if (root.trie != null) {
            leaves(trieSub(root, ctx), ctx, out);
        }
        for (Object item : root.tail) {
            out.accept(item);
        }
    }

    private static void leaves(Sub sub, Ctx ctx, Consumer<Object> out) {
        VecNode node = load(sub, ctx);
        if (sub.level == 0) {
            for (Object item : node.slots) {
                out.accept(item);
            }
            return;
        }
        for (int j = 0; j < node.slots.length; j++) {
            leaves(kid(sub, j, ctx), ctx, out);
        }
    }

    // ------------------------------------------------------------------ writes

    /** A relaxed root with an element appended. */
    static VecRoot conj(VecRoot root, Object item, Ctx ctx) {
        VecRoot r = normalized(root, ctx);
        if (r.tail.length < WIDTH) {
            Object[] tail = Arrays.copyOf(r.tail, r.tail.length + 1);
            tail[r.tail.length] = item;
            return make(trieSub(r, ctx), tail, ctx);
        }
        return make(join(trieSub(r, ctx), leaf(r.tail.clone(), ctx), false, ctx), new Object[] {item}, ctx);
    }

    /** A relaxed root without its last element. */
    static VecRoot pop(VecRoot root, Ctx ctx) {
        if (root.n == 0) {
            throw new IllegalStateException("pop on an empty vector");
        }
        VecRoot r = normalized(root, ctx);
        if (r.tail.length > 1) {
            return make(trieSub(r, ctx), Arrays.copyOf(r.tail, r.tail.length - 1), ctx);
        }
        return rootOf(trieSub(r, ctx), ctx);
    }

    /** A relaxed root with the element at an index replaced, copying the path to it. */
    static VecRoot assoc(VecRoot root, int index, Object item, Ctx ctx) {
        if (index < 0 || index >= root.n) {
            throw new IndexOutOfBoundsException("index " + index + " of a vector of " + root.n);
        }
        int off = root.trieCount();
        if (index >= off) {
            if (Spill.sameValue(root.tail[index - off], item)) {
                return root;
            }
            Object[] tail = root.tail.clone();
            tail[index - off] = item;
            return new VecRoot(root.n, root.shift, root.trie, tail, ctx.edit, true);
        }
        Sub trie = trieSub(root, ctx);
        VecNode next = assoc(trie, index, item, ctx);
        return next == trie.ref ? root : new VecRoot(root.n, root.shift, next, root.tail.clone(), ctx.edit, true);
    }

    private static VecNode assoc(Sub sub, int index, Object item, Ctx ctx) {
        VecNode node = load(sub, ctx);
        if (sub.level == 0) {
            if (Spill.sameValue(node.item(index), item)) {
                return node;
            }
            Object[] slots = node.slots.clone();
            slots[index] = item;
            return new VecNode(true, slots, null, ctx.edit);
        }
        int j = locate(node, sub.level, index);
        Sub kid = kid(sub, j, ctx);
        VecNode next = assoc(kid, index - before(node, sub.level, j), item, ctx);
        if (next == kid.ref) {
            return node;
        }
        Object[] slots = node.slots.clone();
        slots[j] = next;
        return new VecNode(false, slots, node.sizes, ctx.edit);
    }

    /** The elements {@code [from, to)} of a root, dense or relaxed. */
    static VecRoot slice(VecRoot root, int from, int to, Ctx ctx) {
        if (from == 0 && to == root.n) {
            return root;
        }
        if (from == to) {
            return VecRoot.empty();
        }
        Sub full = full(root, ctx);
        return rootOf(sliceRight(sliceLeft(full, to, ctx), from, ctx), ctx);
    }

    /**
     * A root with {@code deleteCount} elements from {@code at} replaced by {@code items}. When
     * nothing follows the replaced range the items are appended one by one, so an edit at the end
     * of a dense vector keeps it dense.
     */
    static VecRoot splice(VecRoot root, int at, int deleteCount, Object[] items, Ctx ctx) {
        Sub full = full(root, ctx);
        Sub left = at == 0 ? null : sliceLeft(full, at, ctx);
        if (at + deleteCount == root.n) {
            VecRoot out = rootOf(left, ctx);
            for (Object item : items) {
                out = out.relaxed ? conj(out, item, ctx) : out.conj(item, ctx);
            }
            return out;
        }
        Sub right = sliceRight(full, at + deleteCount, ctx);
        return rootOf(join(join(left, build(items, ctx), true, ctx), right, true, ctx), ctx);
    }

    /** Two roots over one store joined, {@code a}'s elements first. */
    static VecRoot concat(VecRoot a, VecRoot b, Ctx ctx) {
        if (b.n == 0) {
            return a;
        }
        if (a.n == 0) {
            return b;
        }
        VecRoot right = normalized(b, ctx);
        if (right.trie == null) {
            VecRoot out = a;
            for (Object item : right.tail) {
                out = out.relaxed ? conj(out, item, ctx) : out.conj(item, ctx);
            }
            return out;
        }
        return make(join(full(a, ctx), trieSub(right, ctx), true, ctx), right.tail.clone(), ctx);
    }

    /**
     * The leftwise-dense {@code vr} root of a relaxed root's elements (P2-R-03): every node full
     * but the last of its level, built by the {@code vr} append under a private edit token.
     */
    static VecRoot pack(VecRoot root, Ctx ctx) {
        if (!root.relaxed) {
            return root;
        }
        Ctx edit = new Ctx(ctx.loader, ctx.hasher, new AtomicReference<>(Thread.currentThread()));
        VecRoot[] out = {VecRoot.empty()};
        forEach(root, ctx, item -> out[0] = out[0].conj(item, edit));
        return out[0];
    }

    // ------------------------------------------------------------------ sub-trees

    /** The whole tree of a root, its tail appended as the last leaf; null when empty. */
    private static Sub full(VecRoot root, Ctx ctx) {
        return join(trieSub(root, ctx), leaf(root.tail.clone(), ctx), false, ctx);
    }

    /** A root whose tail another port left empty, with its last leaf moved into the tail. */
    private static VecRoot normalized(VecRoot root, Ctx ctx) {
        return root.trie != null && root.tail.length == 0 ? rootOf(trieSub(root, ctx), ctx) : root;
    }

    private static Sub leaf(Object[] items, Ctx ctx) {
        return items.length == 0 ? null : new Sub(new VecNode(true, items, null, ctx.edit), 0, items.length);
    }

    /** A dense sub-tree of some elements: full leaves, the last one short, and full nodes above them. */
    private static Sub build(Object[] items, Ctx ctx) {
        if (items.length == 0) {
            return null;
        }
        List<Sub> level = new ArrayList<>();
        for (int i = 0; i < items.length; i += WIDTH) {
            level.add(leaf(Arrays.copyOfRange(items, i, Math.min(items.length, i + WIDTH)), ctx));
        }
        int shift = 0;
        while (level.size() > 1) {
            shift += BITS;
            List<Sub> up = new ArrayList<>();
            for (int i = 0; i < level.size(); i += WIDTH) {
                up.add(node(level.subList(i, Math.min(level.size(), i + WIDTH)), shift, ctx));
            }
            level = up;
        }
        return level.get(0);
    }

    /** A new interior node over children at the level below, relaxed only when a child but the last is not full. */
    private static Sub node(List<Sub> kids, int level, Ctx ctx) {
        Object[] slots = new Object[kids.size()];
        int[] sizes = new int[kids.size()];
        long total = 0;
        boolean dense = true;
        for (int j = 0; j < slots.length; j++) {
            Sub kid = kids.get(j);
            slots[j] = kid.ref;
            total += kid.count;
            if (total > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("a vector holds at most 2^31 - 1 elements");
            }
            sizes[j] = (int) total;
            dense &= j == slots.length - 1 || kid.count == 1L << level;
        }
        return new Sub(new VecNode(false, slots, dense ? null : sizes, ctx.edit), level, (int) total);
    }

    /** The elements {@code [0, to)} of a sub-tree at its own level, {@code 0 < to <= count}. */
    private static Sub sliceLeft(Sub sub, int to, Ctx ctx) {
        if (to == sub.count) {
            return sub;
        }
        VecNode node = load(sub, ctx);
        if (sub.level == 0) {
            return leaf(Arrays.copyOf(node.slots, to), ctx);
        }
        int j = locate(node, sub.level, to - 1);
        List<Sub> kids = new ArrayList<>(j + 1);
        for (int k = 0; k < j; k++) {
            kids.add(new Sub(node.slots[k], sub.level - BITS, childCount(node, k, sub.level, sub.count)));
        }
        Sub cut = new Sub(node.slots[j], sub.level - BITS, childCount(node, j, sub.level, sub.count));
        kids.add(sliceLeft(cut, to - before(node, sub.level, j), ctx));
        return node(kids, sub.level, ctx);
    }

    /** The elements {@code [from, count)} of a sub-tree at its own level, {@code 0 <= from < count}. */
    private static Sub sliceRight(Sub sub, int from, Ctx ctx) {
        if (from == 0) {
            return sub;
        }
        VecNode node = load(sub, ctx);
        if (sub.level == 0) {
            return leaf(Arrays.copyOfRange(node.slots, from, node.slots.length), ctx);
        }
        int j = locate(node, sub.level, from);
        List<Sub> kids = new ArrayList<>(node.slots.length - j);
        Sub cut = new Sub(node.slots[j], sub.level - BITS, childCount(node, j, sub.level, sub.count));
        kids.add(sliceRight(cut, from - before(node, sub.level, j), ctx));
        for (int k = j + 1; k < node.slots.length; k++) {
            kids.add(new Sub(node.slots[k], sub.level - BITS, childCount(node, k, sub.level, sub.count)));
        }
        return node(kids, sub.level, ctx);
    }

    /**
     * Two sub-trees joined into one, {@code x}'s elements first; either may be null. With
     * {@code rebalance} false, the join of an append, the two leaves at the seam stay as they are
     * and a level that passes 32 children fills its left node; with it true the seam rebalances
     * and such a level splits evenly.
     */
    private static Sub join(Sub x, Sub y, boolean rebalance, Ctx ctx) {
        if (x == null) {
            return y;
        }
        if (y == null) {
            return x;
        }
        List<Sub> top = merge(x, y, rebalance, ctx);
        return top.size() == 1 ? top.get(0) : node(top, top.get(0).level + BITS, ctx);
    }

    /** One or two sub-trees at the higher of the two levels holding {@code x}'s elements then {@code y}'s. */
    private static List<Sub> merge(Sub x, Sub y, boolean rebalance, Ctx ctx) {
        if (x.level == 0 && y.level == 0) {
            return rebalance ? seam(x, y, ctx) : List.of(x, y);
        }
        List<Sub> kids = new ArrayList<>();
        int level;
        if (x.level > y.level) {
            level = x.level;
            List<Sub> xs = kids(x, ctx);
            kids.addAll(xs.subList(0, xs.size() - 1));
            kids.addAll(merge(xs.get(xs.size() - 1), y, rebalance, ctx));
        } else if (x.level < y.level) {
            level = y.level;
            List<Sub> ys = kids(y, ctx);
            kids.addAll(merge(x, ys.get(0), rebalance, ctx));
            kids.addAll(ys.subList(1, ys.size()));
        } else {
            level = x.level;
            List<Sub> xs = kids(x, ctx);
            List<Sub> ys = kids(y, ctx);
            kids.addAll(xs.subList(0, xs.size() - 1));
            kids.addAll(merge(xs.get(xs.size() - 1), ys.get(0), rebalance, ctx));
            kids.addAll(ys.subList(1, ys.size()));
        }
        if (kids.size() <= WIDTH) {
            return List.of(node(kids, level, ctx));
        }
        int cut = rebalance ? (kids.size() + 1) / 2 : WIDTH;
        return List.of(node(kids.subList(0, cut), level, ctx), node(kids.subList(cut, kids.size()), level, ctx));
    }

    /** The two leaves at a seam: one leaf when they fit, both as they are when each is at least half full, else two even leaves. */
    private static List<Sub> seam(Sub x, Sub y, Ctx ctx) {
        if (x.count + y.count > WIDTH && x.count >= HALF && y.count >= HALF) {
            return List.of(x, y);
        }
        Object[] a = load(x, ctx).slots;
        Object[] b = load(y, ctx).slots;
        Object[] all = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, all, a.length, b.length);
        if (all.length <= WIDTH) {
            return List.of(leaf(all, ctx));
        }
        int half = (all.length + 1) / 2;
        return List.of(leaf(Arrays.copyOf(all, half), ctx), leaf(Arrays.copyOfRange(all, half, all.length), ctx));
    }

    private static Sub collapse(Sub sub, int floor, Ctx ctx) {
        Sub s = sub;
        while (s.level > floor) {
            VecNode node = load(s, ctx);
            if (node.slots.length != 1) {
                break;
            }
            s = new Sub(node.slots[0], s.level - BITS, s.count);
        }
        return s;
    }

    /** The root of a whole tree: its rightmost leaf becomes the tail. */
    private static VecRoot rootOf(Sub full, Ctx ctx) {
        if (full == null) {
            return VecRoot.empty();
        }
        Sub s = collapse(full, 0, ctx);
        if (s.level == 0) {
            return new VecRoot(s.count, BITS, null, load(s, ctx).slots.clone(), ctx.edit, false);
        }
        Sub last = s;
        while (last.level > 0) {
            last = kid(last, load(last, ctx).slots.length - 1, ctx);
        }
        Object[] tail = load(last, ctx).slots.clone();
        return make(sliceLeft(s, s.count - tail.length, ctx), tail, ctx);
    }

    /**
     * The root of a trie and a non-empty tail, relaxed unless the tree has the {@code vr} shape,
     * and packed when the tree is deeper than {@link #MAX_SHIFT}.
     */
    private static VecRoot make(Sub trie, Object[] tail, Ctx ctx) {
        if (trie == null) {
            return new VecRoot(tail.length, BITS, null, tail, ctx.edit, false);
        }
        Sub t = trie.level == 0 ? node(List.of(trie), BITS, ctx) : collapse(trie, BITS, ctx);
        int n = t.count + tail.length;
        VecRoot root = new VecRoot(n, t.level, t.ref, tail, ctx.edit, !regular(t, n, ctx));
        return t.level > MAX_SHIFT ? pack(root, ctx) : root;
    }

    /**
     * Whether a trie under a tail has the {@code vr} shape: a count that is a multiple of 32 (the
     * tail rule), the shift the count implies, a dense node at every level of the right edge, and
     * a full rightmost leaf. A dense node's other children are full, so they have the shape too.
     */
    private static boolean regular(Sub trie, int n, Ctx ctx) {
        if (trie.count % WIDTH != 0 || trie.level != VecRoot.shiftFor(n)) {
            return false;
        }
        Sub at = trie;
        while (at.level > 0) {
            VecNode node = load(at, ctx);
            if (node.sizes != null) {
                return false;
            }
            at = kid(at, node.slots.length - 1, ctx);
        }
        return at.count == WIDTH;
    }
}
