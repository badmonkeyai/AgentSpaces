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
import ai.badmonkey.agentspaces.pdt.kinds.RrRoot;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The root of a vector, kind {@code vr} (srad/agentspaces-pdt.md §3.2, PD-V-03):
 * the count {@code n}, the shift {@code s} of the trie (5 when the trie is empty),
 * the trie root {@code r} (a link or null), and the tail {@code t} of up to 32
 * elements. The algorithms are Clojure's {@code PersistentVector}: the tail sits
 * outside the trie, {@code tailoff = n < 32 ? 0 : ((n - 1) >>> 5) << 5}, and
 * {@code conj} and {@code pop} rewrite only this node until the tail fills or
 * empties. The shape is a function of {@code n}, so the layout is canonical.
 *
 * <p>PDT 2 phase 6 (P2-R-01; srad/agentspaces-pdt2.md §3.7). A {@link #relaxed} root is the root
 * of an RRB tree: its tail holds 1 to 32 elements whatever the count, its trie may hold relaxed
 * nodes and short leaves, it encodes as kind {@code rr}, and {@link Rrb} runs its operations. A
 * relaxed root whose tree has the {@code vr} shape is no longer relaxed, so it encodes as the
 * {@code vr} root of the same elements.
 */
final class VecRoot extends TreeNode {

    private static final Object[] NONE = new Object[0];

    int n;
    int shift;
    /** A {@link VecNode}, the CID of one, or null when the trie is empty. */
    Object trie;
    Object[] tail;
    AtomicReference<Thread> edit;
    /** PDT 2 phase 6 (P2-R-01): whether the tree lacks the {@code vr} shape and so is an RRB tree of kind {@code rr}. */
    final boolean relaxed;
    /** PDT 2 phase 6 (P2-R-03): the root of the packed form, once computed. */
    volatile String packedCid;

    VecRoot(int n, int shift, Object trie, Object[] tail, AtomicReference<Thread> edit) {
        this(n, shift, trie, tail, edit, false);
    }

    VecRoot(int n, int shift, Object trie, Object[] tail, AtomicReference<Thread> edit, boolean relaxed) {
        this.n = n;
        this.shift = shift;
        this.trie = trie;
        this.tail = tail;
        this.edit = edit;
        this.relaxed = relaxed;
    }

    /** The elements in the trie: every element but the tail's. */
    int trieCount() {
        return n - tail.length;
    }

    static VecRoot empty() {
        return new VecRoot(0, VecNode.BITS, null, NONE, null);
    }

    static int tailoff(int n) {
        return n < VecNode.WIDTH ? 0 : ((n - 1) >>> VecNode.BITS) << VecNode.BITS;
    }

    int tailoff() {
        return tailoff(n);
    }

    private VecRoot editable(Ctx ctx) {
        if (ctx.owns(edit)) {
            return this;
        }
        return new VecRoot(n, shift, trie, tail.clone(), ctx.edit, relaxed);
    }

    VecNode trieNode(Ctx ctx) {
        if (trie instanceof VecNode node) {
            return node;
        }
        String cid = (String) trie;
        VecNode loaded = VecNode.decode(cid, ctx.loader.load(cid));
        trie = loaded;
        return loaded;
    }

    // ------------------------------------------------------------------ reads

    /** The leaf holding index {@code i}, which must be below the tailoff. */
    VecNode leafFor(int i, Ctx ctx) {
        VecNode node = trieNode(ctx);
        for (int level = shift; level > 0; level -= VecNode.BITS) {
            node = node.child((i >>> level) & VecNode.MASK, ctx);
        }
        return node;
    }

    Object nth(int i, Ctx ctx) {
        if (i < 0 || i >= n) {
            throw new IndexOutOfBoundsException("index " + i + " of a vector of " + n);
        }
        if (i >= tailoff()) {
            return tail[i - tailoff()];
        }
        return leafFor(i, ctx).item(i & VecNode.MASK);
    }

    /**
     * The shift a count implies: 5 while the trie is empty, else the smallest multiple
     * of 5 under which the trie's elements fit 32 leaves of 32 per level, exactly as
     * {@code conj} grows and {@code pop} shrinks the root (PD-V-03).
     */
    static int shiftFor(int n) {
        int off = tailoff(n);
        int s = VecNode.BITS;
        while (off > 0 && ((off - 1) >>> s) >= VecNode.WIDTH) {
            s += VecNode.BITS;
        }
        return s;
    }

    // ------------------------------------------------------------------ writes

    VecRoot conj(Object item, Ctx ctx) {
        int off = tailoff();
        if (n - off < VecNode.WIDTH) {
            VecRoot r = editable(ctx);
            Object[] grown = new Object[tail.length + 1];
            System.arraycopy(tail, 0, grown, 0, tail.length);
            grown[tail.length] = item;
            r.tail = grown;
            r.n = n + 1;
            return r;
        }
        VecNode tailNode = new VecNode(true, tail, ctx.edit);
        VecRoot r = editable(ctx);
        if (trie == null) {
            r.trie = new VecNode(false, new Object[] {tailNode}, ctx.edit);
            r.shift = VecNode.BITS;
        } else if ((n >>> VecNode.BITS) > (1 << shift)) {
            Object[] pair = {trie, newPath(shift, tailNode, ctx)};
            r.trie = new VecNode(false, pair, ctx.edit);
            r.shift = shift + VecNode.BITS;
        } else {
            r.trie = pushTail(shift, trieNode(ctx), tailNode, ctx);
        }
        r.tail = new Object[] {item};
        r.n = n + 1;
        return r;
    }

    private VecNode pushTail(int level, VecNode parent, VecNode tailNode, Ctx ctx) {
        int subidx = ((n - 1) >>> level) & VecNode.MASK;
        if (level == VecNode.BITS) {
            return parent.appended(tailNode, ctx);
        }
        if (subidx < parent.slots.length) {
            VecNode child = parent.child(subidx, ctx);
            VecNode next = pushTail(level - VecNode.BITS, child, tailNode, ctx);
            return next == child ? parent : parent.withSlot(subidx, next, ctx);
        }
        return parent.appended(newPath(level - VecNode.BITS, tailNode, ctx), ctx);
    }

    private static VecNode newPath(int level, VecNode node, Ctx ctx) {
        if (level == 0) {
            return node;
        }
        return new VecNode(false, new Object[] {newPath(level - VecNode.BITS, node, ctx)}, ctx.edit);
    }

    VecRoot pop(Ctx ctx) {
        if (n == 0) {
            throw new IllegalStateException("pop on an empty vector");
        }
        int off = tailoff();
        if (n - off > 1) {
            VecRoot r = editable(ctx);
            Object[] cut = new Object[tail.length - 1];
            System.arraycopy(tail, 0, cut, 0, cut.length);
            r.tail = cut;
            r.n = n - 1;
            return r;
        }
        if (n == 1) {
            VecRoot r = editable(ctx);
            r.tail = NONE;
            r.n = 0;
            r.trie = null;
            r.shift = VecNode.BITS;
            return r;
        }
        Object[] newTail = leafFor(n - 2, ctx).slots.clone();
        VecNode newTrie = popTail(shift, trieNode(ctx), ctx);
        VecRoot r = editable(ctx);
        int newShift = shift;
        if (newTrie == null) {
            newShift = VecNode.BITS;
        } else if (shift > VecNode.BITS && newTrie.slots.length == 1) {
            newTrie = newTrie.child(0, ctx);
            newShift = shift - VecNode.BITS;
        }
        r.trie = newTrie;
        r.shift = newShift;
        r.tail = newTail;
        r.n = n - 1;
        return r;
    }

    private VecNode popTail(int level, VecNode node, Ctx ctx) {
        int subidx = ((n - 2) >>> level) & VecNode.MASK;
        if (level > VecNode.BITS) {
            VecNode next = popTail(level - VecNode.BITS, node.child(subidx, ctx), ctx);
            if (next == null) {
                return subidx == 0 ? null : node.shrunk(ctx);
            }
            return node.withSlot(subidx, next, ctx);
        }
        return subidx == 0 ? null : node.shrunk(ctx);
    }

    VecRoot assocN(int i, Object item, Ctx ctx) {
        if (i < 0 || i >= n) {
            throw new IndexOutOfBoundsException("index " + i + " of a vector of " + n);
        }
        int off = tailoff();
        if (i >= off) {
            if (Spill.sameValue(tail[i - off], item)) {
                return this;
            }
            VecRoot r = editable(ctx);
            r.tail[i - off] = item;
            return r;
        }
        VecNode next = doAssoc(shift, trieNode(ctx), i, item, ctx);
        if (next == trie) {
            return this;
        }
        VecRoot r = editable(ctx);
        r.trie = next;
        return r;
    }

    private static VecNode doAssoc(int level, VecNode node, int i, Object item, Ctx ctx) {
        if (level == 0) {
            if (Spill.sameValue(node.slots[i & VecNode.MASK], item)) {
                return node;
            }
            return node.withSlot(i & VecNode.MASK, item, ctx);
        }
        int subidx = (i >>> level) & VecNode.MASK;
        VecNode child = node.child(subidx, ctx);
        VecNode next = doAssoc(level - VecNode.BITS, child, i, item, ctx);
        return next == child ? node : node.withSlot(subidx, next, ctx);
    }

    /** The deepest level of nodes under this root, counting this root as one. */
    int depth() {
        return trie == null ? 1 : 2 + shift / VecNode.BITS;
    }

    // ------------------------------------------------------------------ codec

    @Override
    byte[] encode(Flusher flusher) {
        Map<String, Object> node = new LinkedHashMap<>();
        node.put(NodeCodec.KIND_KEY, relaxed ? Kind.RR : Kind.VR);
        node.put("n", (long) n);
        node.put("s", (long) shift);
        node.put("r", trie == null ? null : new Link(flusher.cidOf(trie)));
        List<Object> items = new ArrayList<>(tail.length);
        for (Object item : tail) {
            items.add(Spill.embed(item, flusher));
        }
        node.put("t", items);
        return NodeCodec.encode(node);
    }

    static VecRoot decode(String cid, byte[] bytes) {
        if (Kind.RR.equals(NodeCodec.kindOf(bytes))) {
            return decodeRelaxed(cid, bytes);
        }
        Map<String, Object> node = NodeCodec.decode(cid, bytes);
        if (!Kind.VR.equals(node.get(NodeCodec.KIND_KEY))) {
            throw new NodeFormatException(cid, "expected a vector root, found kind '" + node.get(NodeCodec.KIND_KEY) + "'");
        }
        long n = node.get("n") instanceof Long l ? l : 0L;
        long shift = node.get("s") instanceof Long l ? l : VecNode.BITS;
        if (n > Integer.MAX_VALUE || shift < VecNode.BITS || shift > 30 || shift % VecNode.BITS != 0) {
            throw new NodeFormatException(cid, "vector root with count " + n + " and shift " + shift);
        }
        Object trie = node.get("r") instanceof Link link ? link.cid() : null;
        List<?> t = node.get("t") instanceof List<?> list ? list : List.of();
        if (t.size() > VecNode.WIDTH) {
            throw new NodeFormatException(cid, "vector tail of " + t.size() + " elements");
        }
        int count = (int) n;
        if (t.size() != count - tailoff(count) || (trie == null) != (tailoff(count) == 0)) {
            throw new NodeFormatException(cid, "vector tail and trie do not match the count " + count);
        }
        if (shift != shiftFor(count)) {
            throw new NodeFormatException(cid, "vector root with shift " + shift + " where the count " + count + " implies " + shiftFor(count));
        }
        Object[] tail = new Object[t.size()];
        for (int i = 0; i < tail.length; i++) {
            tail[i] = Spill.refOf(((Embedded) t.get(i)).bytes());
        }
        VecRoot r = new VecRoot(count, (int) shift, trie, tail, null);
        r.cid = cid;
        return r;
    }

    /**
     * Reads an {@code rr} root (PDT 2 phase 6, P2-R-04): the shape {@link RrRoot} checks, a count
     * a vector can hold, and a shift of at most 30. The nodes below it load on demand through
     * {@link VecNode#decodeRelaxed}.
     */
    private static VecRoot decodeRelaxed(String cid, byte[] bytes) {
        RrRoot rr = RrRoot.decode(cid, bytes);
        if (rr.count() > Integer.MAX_VALUE || rr.shift() > 30) {
            throw new NodeFormatException(cid, "rr root with count " + rr.count() + " and shift " + rr.shift());
        }
        Object[] tail = new Object[rr.tail().size()];
        for (int i = 0; i < tail.length; i++) {
            tail[i] = Spill.refOf(rr.tail().get(i).bytes());
        }
        VecRoot r = new VecRoot((int) rr.count(), (int) rr.shift(), rr.trie() == null ? null : rr.trie().cid(), tail,
                null, true);
        r.cid = cid;
        return r;
    }
}
