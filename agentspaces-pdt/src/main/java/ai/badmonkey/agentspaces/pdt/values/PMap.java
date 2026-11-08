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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 * A persistent hash map held as a CHAMP trie of content-addressed nodes
 * (srad/agentspaces-pdt.md §3.2; PD-V-01, PD-V-04, PD-V-07 to PD-V-09). Keys hash
 * by the sha2-256 of their canonical CBOR, the layout is canonical, and equal maps
 * have equal roots. Every operation returns a new map and leaves the receiver
 * unchanged; nodes load on demand through the backing {@link NodeLoader}, and new
 * nodes stay unhashed until {@link #root()} needs the CID (PD-V-08). Iteration is
 * in slot order. A null value is refused: use {@link PSet} for presence alone.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class PMap<K, V> implements Iterable<Map.Entry<K, V>> {

    private final RawMap raw;
    private final Codec<K> keys;
    private final Codec<V> values;

    PMap(RawMap raw, Codec<K> keys, Codec<V> values) {
        this.raw = raw;
        this.keys = keys;
        this.values = values;
    }

    // ------------------------------------------------------------------ factories

    /** The empty map over a store, loading through {@link NodeLoader#of}. */
    public static <K, V> PMap<K, V> empty(NodeStore store, Codec<K> keys, Codec<V> values) {
        return empty(store, NodeLoader.of(store), keys, values);
    }

    /** The empty map over a store with an explicit loader. */
    public static <K, V> PMap<K, V> empty(NodeStore store, NodeLoader loader, Codec<K> keys, Codec<V> values) {
        return withHasher(new Backing(store, loader), KeyHasher.SHA256, keys, values);
    }

    /** A map whose root is in the store; reads the root node for its count. */
    public static <K, V> PMap<K, V> load(NodeStore store, String root, Codec<K> keys, Codec<V> values) {
        return load(store, NodeLoader.of(store), root, keys, values);
    }

    /** A map whose root the loader can provide; reads the root node for its count. */
    public static <K, V> PMap<K, V> load(NodeStore store, NodeLoader loader, String root, Codec<K> keys, Codec<V> values) {
        Objects.requireNonNull(keys, "keys");
        Objects.requireNonNull(values, "values");
        return new PMap<>(RawMap.load(new Backing(store, loader), KeyHasher.SHA256, root), keys, values);
    }

    /** The empty map with a substitute hasher; tests use it to force collisions. */
    static <K, V> PMap<K, V> withHasher(Backing backing, KeyHasher hasher, Codec<K> keys, Codec<V> values) {
        Objects.requireNonNull(keys, "keys");
        Objects.requireNonNull(values, "values");
        return new PMap<>(RawMap.empty(backing, hasher), keys, values);
    }

    // ------------------------------------------------------------------ reads

    /** The value of a key, loading nodes on demand. */
    public Optional<V> get(K key) {
        byte[] bytes = raw.get(keys.encode(Objects.requireNonNull(key, "key")));
        return bytes == null ? Optional.empty() : Optional.of(values.decode(bytes));
    }

    /** Whether the key is present. */
    public boolean containsKey(K key) {
        return raw.find(keys.encode(Objects.requireNonNull(key, "key"))) != ChampNode.NOT_FOUND;
    }

    /** The number of entries, held on the root node. */
    public long count() {
        return raw.count;
    }

    /** Whether the map has no entries. */
    public boolean isEmpty() {
        return raw.count == 0;
    }

    /** The root CID, computed lazily and cached; writes the new nodes to the store once. */
    public String root() {
        return raw.root();
    }

    /** The keys in slot order. */
    public List<K> keys() {
        List<K> out = new ArrayList<>();
        RawMap.RawIterator it = raw.entries();
        while (it.hasNext()) {
            out.add(keys.decode(it.next().key()));
        }
        return out;
    }

    /** A plain map copy in slot order. */
    public Map<K, V> toMap() {
        Map<K, V> out = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : this) {
            out.put(entry.getKey(), entry.getValue());
        }
        return out;
    }

    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
        RawMap.RawIterator it = raw.entries();
        Ctx ctx = raw.ctx();
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public Map.Entry<K, V> next() {
                RawMap.RawEntry entry = it.next();
                return new AbstractMap.SimpleImmutableEntry<>(
                        keys.decode(entry.key()), values.decode(Spill.resolve(entry.value(), ctx)));
            }
        };
    }

    // ------------------------------------------------------------------ writes

    /** A map with the key bound to the value; path copy, the receiver is unchanged. */
    public PMap<K, V> assoc(K key, V value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        RawMap next = raw.assoc(keys.encode(key), values.encode(value));
        return next == raw ? this : new PMap<>(next, keys, values);
    }

    /** A map without the key. */
    public PMap<K, V> dissoc(K key) {
        RawMap next = raw.dissoc(keys.encode(Objects.requireNonNull(key, "key")));
        return next == raw ? this : new PMap<>(next, keys, values);
    }

    /**
     * A map with the key bound to {@code f} of its value, or of null when absent; a
     * null result removes the key.
     */
    public PMap<K, V> update(K key, UnaryOperator<V> f) {
        V current = get(key).orElse(null);
        V next = f.apply(current);
        return next == null ? dissoc(key) : assoc(key, next);
    }

    /**
     * A map updated at a path: the first element is a key of this map and the rest
     * descend into the value as a plain {@link Map} by key or {@link List} by index,
     * after Clojure's {@code update-in}. A missing level becomes a map. The value
     * codec must round-trip plain maps and lists, as {@code Codec.of(Object.class)} does.
     */
    @SuppressWarnings("unchecked")
    public PMap<K, V> updateIn(List<Object> path, UnaryOperator<Object> f) {
        if (path.isEmpty()) {
            throw new IllegalArgumentException("updateIn needs a path of at least one key");
        }
        K key = (K) path.get(0);
        if (path.size() == 1) {
            return update(key, v -> (V) f.apply(v));
        }
        Object current = get(key).orElse(null);
        Object updated = updateNested(current, path, 1, f);
        return assoc(key, (V) updated);
    }

    @SuppressWarnings("unchecked")
    static Object updateNested(Object current, List<Object> path, int i, UnaryOperator<Object> f) {
        if (i == path.size()) {
            return f.apply(current);
        }
        Object step = path.get(i);
        if (current instanceof List<?> list) {
            int index = ((Number) step).intValue();
            List<Object> copy = new ArrayList<>(list);
            if (index == copy.size()) {
                copy.add(updateNested(null, path, i + 1, f));
            } else {
                copy.set(index, updateNested(copy.get(index), path, i + 1, f));
            }
            return copy;
        }
        Map<Object, Object> copy = current == null ? new LinkedHashMap<>() : new LinkedHashMap<>((Map<Object, Object>) current);
        copy.put(step, updateNested(copy.get(step), path, i + 1, f));
        return copy;
    }

    /** A transient owned by the calling thread (PD-V-05). */
    public Editor<K, V> editor() {
        return new Editor<>(raw.editor(), keys, values);
    }

    // ------------------------------------------------------------------ comparison

    /** The added, removed, and changed keys against a base, descending only where CIDs differ (PD-V-07). */
    public Diff<K, V> diff(PMap<K, V> base) {
        Objects.requireNonNull(base, "base");
        return Diff.of(this, base);
    }

    /** The three-way merge of §3.6.1 (PD-V-09): this map and {@code other} against their common {@code base}. */
    public PMap<K, V> merge3(PMap<K, V> base, PMap<K, V> other, Merge.Resolver<K, V> resolver) {
        return Merge.merge3(this, base, other, resolver);
    }

    /** Equality by root (PD-V-04). */
    @Override
    public boolean equals(Object o) {
        return o instanceof PMap<?, ?> other && root().equals(other.root());
    }

    @Override
    public int hashCode() {
        return root().hashCode();
    }

    @Override
    public String toString() {
        return "PMap[" + raw.count + " entries]";
    }

    // ------------------------------------------------------------------ internals

    RawMap raw() {
        return raw;
    }

    Codec<K> keyCodec() {
        return keys;
    }

    Codec<V> valueCodec() {
        return values;
    }

    /** The store new nodes are written to. */
    public NodeStore store() {
        return raw.backing.store();
    }

    /** The loader cold nodes are read through. */
    public NodeLoader loader() {
        return raw.backing.loader();
    }

    int depth() {
        return raw.depth();
    }
}
