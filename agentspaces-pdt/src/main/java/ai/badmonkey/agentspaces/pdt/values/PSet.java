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

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * A persistent set (srad/agentspaces-pdt.md §3.2, PD-V-02): a {@link PMap} whose
 * every value is CBOR {@code null}, with {@code conj}, {@code disj}, and
 * {@code contains}, and equality by root. Iteration is in slot order.
 *
 * @param <T> the element type
 */
public final class PSet<T> implements Iterable<T> {

    /** The one value of every entry: CBOR {@code null}. */
    static final byte[] NIL = {(byte) 0xf6};

    private final RawMap raw;
    private final Codec<T> codec;

    PSet(RawMap raw, Codec<T> codec) {
        this.raw = raw;
        this.codec = codec;
    }

    /** The empty set over a store. */
    public static <T> PSet<T> empty(NodeStore store, Codec<T> codec) {
        return empty(store, NodeLoader.of(store), codec);
    }

    /** The empty set over a store with an explicit loader. */
    public static <T> PSet<T> empty(NodeStore store, NodeLoader loader, Codec<T> codec) {
        return new PSet<>(RawMap.empty(new Backing(store, loader), KeyHasher.SHA256), Objects.requireNonNull(codec, "codec"));
    }

    /** A set whose root is in the store. */
    public static <T> PSet<T> load(NodeStore store, String root, Codec<T> codec) {
        return load(store, NodeLoader.of(store), root, codec);
    }

    /** A set whose root the loader can provide. */
    public static <T> PSet<T> load(NodeStore store, NodeLoader loader, String root, Codec<T> codec) {
        return new PSet<>(RawMap.load(new Backing(store, loader), KeyHasher.SHA256, root), Objects.requireNonNull(codec, "codec"));
    }

    /** Whether the element is present, loading nodes on demand. */
    public boolean contains(T element) {
        return raw.find(codec.encode(Objects.requireNonNull(element, "element"))) != ChampNode.NOT_FOUND;
    }

    /** A set with the element. */
    public PSet<T> conj(T element) {
        RawMap next = raw.assoc(codec.encode(Objects.requireNonNull(element, "element")), NIL);
        return next == raw ? this : new PSet<>(next, codec);
    }

    /** A set without the element. */
    public PSet<T> disj(T element) {
        RawMap next = raw.dissoc(codec.encode(Objects.requireNonNull(element, "element")));
        return next == raw ? this : new PSet<>(next, codec);
    }

    /** The number of elements. */
    public long count() {
        return raw.count;
    }

    /** Whether the set has no elements. */
    public boolean isEmpty() {
        return raw.count == 0;
    }

    /** The root CID, computed lazily and cached; writes the new nodes once. */
    public String root() {
        return raw.root();
    }

    /** A transient owned by the calling thread (PD-V-05). */
    public SetEditor<T> editor() {
        return new SetEditor<>(raw.editor(), codec);
    }

    /** The elements added and removed against a base (PD-V-07). */
    public SetDiff<T> diff(PSet<T> base) {
        Diff.Raw rawDiff = Diff.raw(raw, Objects.requireNonNull(base, "base").raw);
        Set<T> added = new LinkedHashSet<>();
        Set<T> removed = new LinkedHashSet<>();
        for (Diff.RawChange change : rawDiff.changes()) {
            if (change.added()) {
                added.add(codec.decode(change.key()));
            } else if (change.removed()) {
                removed.add(codec.decode(change.key()));
            }
        }
        return new SetDiff<>(Collections.unmodifiableSet(added), Collections.unmodifiableSet(removed), rawDiff.nodesVisited());
    }

    /**
     * The three-way merge of §3.6.1 (PD-V-09): an element added or removed on either
     * side takes that side; both sides cannot disagree on a set element.
     */
    public PSet<T> merge3(PSet<T> base, PSet<T> other) {
        RawMap merged = Merge.merge3(raw, base.raw, other.raw, (key, b, m, t) -> {
            throw new IllegalStateException("a set merge cannot conflict");
        });
        return new PSet<>(merged, codec);
    }

    /** A plain set copy in slot order. */
    public Set<T> toSet() {
        Set<T> out = new LinkedHashSet<>();
        for (T element : this) {
            out.add(element);
        }
        return out;
    }

    @Override
    public Iterator<T> iterator() {
        RawMap.RawIterator it = raw.entries();
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public T next() {
                return codec.decode(it.next().key());
            }
        };
    }

    /** Equality by root (PD-V-02). */
    @Override
    public boolean equals(Object o) {
        return o instanceof PSet<?> other && root().equals(other.root());
    }

    @Override
    public int hashCode() {
        return root().hashCode();
    }

    @Override
    public String toString() {
        return "PSet[" + raw.count + " elements]";
    }

    /**
     * The result of {@link PSet#diff}.
     *
     * @param added        the elements in mine and absent from base
     * @param removed      the elements in base and absent from mine
     * @param nodesVisited how many nodes the walk read
     */
    public record SetDiff<T>(Set<T> added, Set<T> removed, int nodesVisited) {
        /** Whether the sets are equal. */
        public boolean isEmpty() {
            return added.isEmpty() && removed.isEmpty();
        }
    }

    RawMap raw() {
        return raw;
    }

    int depth() {
        return raw.depth();
    }
}
