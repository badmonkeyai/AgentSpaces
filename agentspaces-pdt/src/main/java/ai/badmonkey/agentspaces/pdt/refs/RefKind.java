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
package ai.badmonkey.agentspaces.pdt.refs;

import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.blob.FileEntry;
import ai.badmonkey.agentspaces.pdt.blob.PBlob;
import ai.badmonkey.agentspaces.pdt.values.Codec;
import ai.badmonkey.agentspaces.pdt.values.Merge;
import ai.badmonkey.agentspaces.pdt.values.NodeLoader;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.pdt.values.PSet;
import ai.badmonkey.agentspaces.pdt.values.PVector;

import java.util.Objects;

/**
 * What the ref engine needs from a value kind (srad/agentspaces-pdt.md §3.6):
 * how to load a version by root, the empty value, the root and size of a value,
 * whether it reads lazily through a loader, and the three-way merge of §3.6.1
 * where the kind has one. {@code map} and {@code tree} merge key-wise, {@code set}
 * merges element-wise without conflicts, and {@code vector} and {@code blob} take
 * the later head whole (PD-R-05).
 *
 * @param <T> the value type
 */
interface RefKind<T> {

    /** The kind name a head carries. */
    String name();

    /** The value at a root; a lazy kind reads its root node through the loader. */
    T load(NodeStore store, NodeLoader loader, String root);

    /** The empty value. */
    T empty(NodeStore store, NodeLoader loader);

    /** The root of a value, flushing its new nodes to its store. */
    String root(T value);

    /** The size a head carries: entries or bytes. */
    long size(T value);

    /** Whether a value reads nodes on demand, so a reader with lazy follow needs no fetch before a get. */
    boolean lazy();

    /**
     * The merge of §3.6.1 for this kind, or null when the kind takes the later
     * head whole. The resolver decides a key changed on both sides.
     */
    T merge3(T earlier, T base, T later, Merge.Resolver<Object, Object> resolver);

    /** A map or a tree (a map of file entries). */
    static <K, V> RefKind<PMap<K, V>> map(String name, Codec<K> keys, Codec<V> values) {
        Objects.requireNonNull(keys, "keys");
        Objects.requireNonNull(values, "values");
        return new RefKind<>() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public PMap<K, V> load(NodeStore store, NodeLoader loader, String root) {
                return PMap.load(store, loader, root, keys, values);
            }

            @Override
            public PMap<K, V> empty(NodeStore store, NodeLoader loader) {
                return PMap.empty(store, loader, keys, values);
            }

            @Override
            public String root(PMap<K, V> value) {
                return value.root();
            }

            @Override
            public long size(PMap<K, V> value) {
                return value.count();
            }

            @Override
            public boolean lazy() {
                return true;
            }

            @Override
            @SuppressWarnings("unchecked")
            public PMap<K, V> merge3(PMap<K, V> earlier, PMap<K, V> base, PMap<K, V> later,
                                     Merge.Resolver<Object, Object> resolver) {
                Merge.Resolver<K, V> typed = (key, b, m, t) -> (java.util.Optional<V>) (java.util.Optional<?>)
                        resolver.resolve(key, (java.util.Optional<Object>) (java.util.Optional<?>) b,
                                (java.util.Optional<Object>) (java.util.Optional<?>) m,
                                (java.util.Optional<Object>) (java.util.Optional<?>) t);
                return earlier.merge3(base, later, typed);
            }
        };
    }

    /** The map kind over untyped CBOR values. */
    static RefKind<PMap<Object, Object>> map() {
        return map(PRef.KIND_MAP, Codec.of(Object.class), Codec.of(Object.class));
    }

    /** The tree kind of PD-B-05: a map from relative path to file entry. */
    static RefKind<PMap<String, FileEntry>> tree() {
        return map(PRef.KIND_TREE, Codec.of(String.class), FileEntry.codec());
    }

    /** A set. */
    static <E> RefKind<PSet<E>> set(Codec<E> codec) {
        Objects.requireNonNull(codec, "codec");
        return new RefKind<>() {
            @Override
            public String name() {
                return PRef.KIND_SET;
            }

            @Override
            public PSet<E> load(NodeStore store, NodeLoader loader, String root) {
                return PSet.load(store, loader, root, codec);
            }

            @Override
            public PSet<E> empty(NodeStore store, NodeLoader loader) {
                return PSet.empty(store, loader, codec);
            }

            @Override
            public String root(PSet<E> value) {
                return value.root();
            }

            @Override
            public long size(PSet<E> value) {
                return value.count();
            }

            @Override
            public boolean lazy() {
                return true;
            }

            @Override
            public PSet<E> merge3(PSet<E> earlier, PSet<E> base, PSet<E> later, Merge.Resolver<Object, Object> resolver) {
                return earlier.merge3(base, later);
            }
        };
    }

    /** A vector. */
    static <E> RefKind<PVector<E>> vector(Codec<E> codec) {
        Objects.requireNonNull(codec, "codec");
        return new RefKind<>() {
            @Override
            public String name() {
                return PRef.KIND_VECTOR;
            }

            @Override
            public PVector<E> load(NodeStore store, NodeLoader loader, String root) {
                return PVector.load(store, loader, root, codec);
            }

            @Override
            public PVector<E> empty(NodeStore store, NodeLoader loader) {
                return PVector.empty(store, loader, codec);
            }

            @Override
            public String root(PVector<E> value) {
                return value.root();
            }

            @Override
            public long size(PVector<E> value) {
                return value.count();
            }

            @Override
            public boolean lazy() {
                return true;
            }

            @Override
            public PVector<E> merge3(PVector<E> earlier, PVector<E> base, PVector<E> later, Merge.Resolver<Object, Object> resolver) {
                return null;
            }
        };
    }

    /** A blob; reads need the tree held, so a reader fetches it whatever its follow mode. */
    static RefKind<PBlob> blob() {
        return new RefKind<>() {
            @Override
            public String name() {
                return PRef.KIND_BLOB;
            }

            @Override
            public PBlob load(NodeStore store, NodeLoader loader, String root) {
                return PBlob.load(store, root);
            }

            @Override
            public PBlob empty(NodeStore store, NodeLoader loader) {
                return PBlob.empty(store);
            }

            @Override
            public String root(PBlob value) {
                return value.root();
            }

            @Override
            public long size(PBlob value) {
                return value.size();
            }

            @Override
            public boolean lazy() {
                return false;
            }

            @Override
            public PBlob merge3(PBlob earlier, PBlob base, PBlob later, Merge.Resolver<Object, Object> resolver) {
                return null;
            }
        };
    }
}
