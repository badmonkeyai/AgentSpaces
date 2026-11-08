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
        return codec.decode(Spill.resolve(node.nth(checkIndex(index), ctx()), ctx()));
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
                int off = node.tailoff();
                if (i >= off) {
                    ref = node.tail[i - off];
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
        return new PVector<>(backing, codec, node.conj(Spill.spill(codec.encode(element), ctx), ctx));
    }

    /** A vector without its last element. */
    public PVector<T> pop() {
        return new PVector<>(backing, codec, node.pop(ctx()));
    }

    /** A vector with the element at an index replaced. */
    public PVector<T> assoc(long index, T element) {
        Objects.requireNonNull(element, "element");
        Ctx ctx = ctx();
        VecRoot next = node.assocN(checkIndex(index), Spill.spill(codec.encode(element), ctx), ctx);
        return next == node ? this : new PVector<>(backing, codec, next);
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

    /** Equality by root (PD-V-04). */
    @Override
    public boolean equals(Object o) {
        return o instanceof PVector<?> other && root().equals(other.root());
    }

    @Override
    public int hashCode() {
        return root().hashCode();
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
