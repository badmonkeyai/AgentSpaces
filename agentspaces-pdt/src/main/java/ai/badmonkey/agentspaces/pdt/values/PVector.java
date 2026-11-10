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

import ai.badmonkey.agentspaces.pdt.NodeStore;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 * A persistent vector held as Clojure's tailed trie of content-addressed nodes
 * (srad/agentspaces-pdt.md §3.2; PD-V-03, PD-V-04, PD-V-07, PD-V-08). {@code conj}
 * and {@code pop} rewrite only the root node until the tail fills or empties;
 * {@code assoc} at an index copies at most {@code shift / 5 + 1} nodes. The shape
 * is a function of the count, so equal vectors have equal roots. Every operation
 * returns a new vector and leaves the receiver unchanged; iteration is in index
 * order. A null element is refused.
 *
 * <p>PDT 2 phase 6 (P2-R-02 to P2-R-04; srad/agentspaces-pdt2.md §3.7). {@code splice},
 * {@code insertAt}, {@code slice}, and {@code concat} run in O(log n) node copies through the RRB
 * operations of {@link Rrb} and return a {@linkplain #relaxed() relaxed} vector when the result
 * lacks the shape of a dense one. A relaxed vector commits as it stands, an {@code rr} root over
 * {@code rv} nodes with size tables, so an edit ships its path; its root depends on the edits
 * that made it, and {@link #packed()} gives the {@code vr} vector of the same elements, the one
 * root those elements have whatever the history. An edit that leaves the shape of a dense vector,
 * an append, a prefix, or a splice at the end, returns the dense vector itself. Equality and the
 * hash follow the elements: two vectors are equal when their packed roots are, which costs one
 * packing of a relaxed vector, once.
 *
 * @param <T> the element type
 */
public final class PVector<T> implements Iterable<T> {

    private final Backing backing;
    private final Codec<T> codec;
    private final VecRoot node;

    PVector(Backing backing, Codec<T> codec, VecRoot node) {
        this.backing = backing;
        this.codec = codec;
        this.node = node;
    }

    /** The empty vector over a store. */
    public static <T> PVector<T> empty(NodeStore store, Codec<T> codec) {
        return empty(store, NodeLoader.of(store), codec);
    }

    /** The empty vector over a store with an explicit loader. */
    public static <T> PVector<T> empty(NodeStore store, NodeLoader loader, Codec<T> codec) {
        return new PVector<>(new Backing(store, loader), Objects.requireNonNull(codec, "codec"), VecRoot.empty());
    }

    /** A vector whose root is in the store. */
    public static <T> PVector<T> load(NodeStore store, String root, Codec<T> codec) {
        return load(store, NodeLoader.of(store), root, codec);
    }

    /** A vector whose root the loader can provide. */
    public static <T> PVector<T> load(NodeStore store, NodeLoader loader, String root, Codec<T> codec) {
        Backing backing = new Backing(store, loader);
        Objects.requireNonNull(root, "root");
        return new PVector<>(backing, Objects.requireNonNull(codec, "codec"), VecRoot.decode(root, loader.load(root)));
    }

    Ctx ctx() {
        return new Ctx(backing.loader(), KeyHasher.SHA256, null);
    }

    // ------------------------------------------------------------------ reads

    /** The element at an index, loading nodes on demand. */
    public T nth(long index) {
        Ctx ctx = ctx();
        int i = checkIndex(index);
        return codec.decode(Spill.resolve(node.relaxed ? Rrb.nth(node, i, ctx) : node.nth(i, ctx), ctx));
    }

    /** The last element, if any. */
    public Optional<T> peek() {
        return node.n == 0 ? Optional.empty() : Optional.of(nth(node.n - 1));
    }

    /** The number of elements, held on the root node. */
    public long count() {
        return node.n;
    }

    /** Whether the vector has no elements. */
    public boolean isEmpty() {
        return node.n == 0;
    }

    /** The root CID, computed lazily and cached; writes the new nodes to the store once. */
    public String root() {
        String cid = node.cid;
        if (cid != null) {
            return cid;
        }
        flush();
        return node.cid;
    }

    Map<String, byte[]> flush() {
        synchronized (node) {
            return Flusher.flush(node, backing.store());
        }
    }

    /** A plain list copy in index order. */
    public List<T> toList() {
        List<T> out = new ArrayList<>(node.n);
        for (T element : this) {
            out.add(element);
        }
        return out;
    }

    @Override
    public Iterator<T> iterator() {
        Ctx ctx = ctx();
        return new Iterator<>() {
            private int i;
            private VecNode leaf;
            private int leafBase = -1;

            @Override
            public boolean hasNext() {
                return i < node.n;
            }

            @Override
            public T next() {
                if (i >= node.n) {
                    throw new NoSuchElementException();
                }
                Object ref;
                int off = node.trieCount();
                if (i >= off) {
                    ref = node.tail[i - off];
                } else if (node.relaxed) {
                    // PDT 2 phase 6 (P2-R-04): a relaxed leaf starts where its parents' size tables say
                    if (leaf == null || i - leafBase >= leaf.slots.length) {
                        int[] start = new int[1];
                        leaf = Rrb.leafAt(node, i, ctx, start);
                        leafBase = start[0];
                    }
                    ref = leaf.item(i - leafBase);
                } else {
                    int base = i & ~VecNode.MASK;
                    if (base != leafBase) {
                        leaf = node.leafFor(i, ctx);
                        leafBase = base;
                    }
                    ref = leaf.item(i & VecNode.MASK);
                }
                i++;
                return codec.decode(Spill.resolve(ref, ctx));
            }
        };
    }

    // ------------------------------------------------------------------ writes

    /** A vector with the element appended. */
    public PVector<T> conj(T element) {
        Objects.requireNonNull(element, "element");
        Ctx ctx = ctx();
        Object item = Spill.spill(codec.encode(element), ctx);
        return new PVector<>(backing, codec, node.relaxed ? Rrb.conj(node, item, ctx) : node.conj(item, ctx));
    }

    /** A vector without its last element. */
    public PVector<T> pop() {
        Ctx ctx = ctx();
        return new PVector<>(backing, codec, node.relaxed ? Rrb.pop(node, ctx) : node.pop(ctx));
    }

    /** A vector with the element at an index replaced. */
    public PVector<T> assoc(long index, T element) {
        Objects.requireNonNull(element, "element");
        Ctx ctx = ctx();
        int i = checkIndex(index);
        Object item = Spill.spill(codec.encode(element), ctx);
        VecRoot next = node.relaxed ? Rrb.assoc(node, i, item, ctx) : node.assocN(i, item, ctx);
        return next == node ? this : new PVector<>(backing, codec, next);
    }

    /**
     * A vector with {@code deleteCount} elements from {@code index} replaced by {@code elements}
     * (PDT 2 phase 6, P2-R-02), in O(log n) node copies plus the new elements.
     *
     * @param index       the first replaced index, at most the count
     * @param deleteCount how many elements to remove there
     * @param elements    the elements to put in their place, none null
     * @return the vector
     * @throws IndexOutOfBoundsException when the range passes the count
     */
    public PVector<T> splice(long index, long deleteCount, List<? extends T> elements) {
        checkRange(index, index + deleteCount);
        Ctx ctx = ctx();
        return new PVector<>(backing, codec, Rrb.splice(node, (int) index, (int) deleteCount, items(elements, ctx), ctx));
    }

    /** A vector with an element inserted before an index, at most the count (PDT 2 phase 6, P2-R-02). */
    public PVector<T> insertAt(long index, T element) {
        return splice(index, 0, List.of(Objects.requireNonNull(element, "element")));
    }

    /** The elements {@code [from, to)} (PDT 2 phase 6, P2-R-02), sharing every node off the two cut paths. */
    public PVector<T> slice(long from, long to) {
        checkRange(from, to);
        return new PVector<>(backing, codec, Rrb.slice(node, (int) from, (int) to, ctx()));
    }

    /**
     * This vector followed by another (PDT 2 phase 6, P2-R-02): in O(log n) node copies when both
     * share this vector's store, else by appending the other's elements one by one, since its
     * nodes live in a store this vector cannot read.
     */
    public PVector<T> concat(PVector<T> other) {
        if (other.isEmpty()) {
            return this;
        }
        if (other.store() != store()) {
            VectorEditor<T> editor = editor();
            other.forEach(editor::conj);
            return editor.done().value();
        }
        if (count() + other.count() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("a vector holds at most 2^31 - 1 elements");
        }
        return new PVector<>(backing, codec, Rrb.concat(node, other.node, ctx()));
    }

    /**
     * The leftwise-dense {@code vr} vector of the same elements (PDT 2 phase 6, P2-R-03): this
     * vector when it is dense, else a rebuild whose cost is linear in the count.
     */
    public PVector<T> packed() {
        return node.relaxed ? new PVector<>(backing, codec, Rrb.pack(node, ctx())) : this;
    }

    /**
     * Whether the tree lacks the shape of a dense vector (PDT 2 phase 6, P2-R-01): its root is an
     * {@code rr} node, and a head that names it is {@code PRef.v} 2 (srad/agentspaces-pdt2.md §6.10.3, rule 2).
     */
    public boolean relaxed() {
        return node.relaxed;
    }

    private Object[] items(List<? extends T> elements, Ctx ctx) {
        Object[] out = new Object[elements.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = Spill.spill(codec.encode(Objects.requireNonNull(elements.get(i), "element")), ctx);
        }
        return out;
    }

    private void checkRange(long from, long to) {
        if (from < 0 || to < from || to > node.n) {
            throw new IndexOutOfBoundsException("range [" + from + ", " + to + ") of a vector of " + node.n);
        }
    }

    /** A vector with the element at an index replaced by {@code f} of it. */
    public PVector<T> update(long index, UnaryOperator<T> f) {
        return assoc(index, f.apply(nth(index)));
    }

    /** A transient owned by the calling thread (PD-V-05). */
    public VectorEditor<T> editor() {
        return new VectorEditor<>(backing, codec, node);
    }

    /** The changed index ranges against a base, descending only where CIDs differ (PD-V-07). */
    public VectorDiff diff(PVector<T> base) {
        return VectorDiff.of(this, Objects.requireNonNull(base, "base"));
    }

    private static int checkIndex(long index) {
        if (index < 0 || index > Integer.MAX_VALUE) {
            throw new IndexOutOfBoundsException("index " + index);
        }
        return (int) index;
    }

    /** Equality by root (PD-V-04), through the packed root when either vector is relaxed (PDT 2 phase 6, P2-R-03). */
    @Override
    public boolean equals(Object o) {
        return o instanceof PVector<?> other && packedRoot().equals(other.packedRoot());
    }

    @Override
    public int hashCode() {
        return packedRoot().hashCode();
    }

    /**
     * The root of the packed form: the root itself when dense, else the CID of the packed tree,
     * hashed without writing it once this vector's own nodes are written, and kept on the root.
     */
    private String packedRoot() {
        String root = root();
        if (!node.relaxed) {
            return root;
        }
        String known = node.packedCid;
        if (known == null) {
            known = new Flusher(backing.store()).cidOf(Rrb.pack(node, ctx()));
            node.packedCid = known;
        }
        return known;
    }

    @Override
    public String toString() {
        return "PVector[" + node.n + " elements]";
    }

    /** The store new nodes are written to. */
    public NodeStore store() {
        return backing.store();
    }

    /** The loader cold nodes are read through. */
    public NodeLoader loader() {
        return backing.loader();
    }

    VecRoot node() {
        return node;
    }

    Backing backing() {
        return backing;
    }

    Codec<T> codec() {
        return codec;
    }

    /** The number of node levels, root included. */
    int depth() {
        return node.depth();
    }
}
