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

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 * The transient of a {@link PMap} (srad/agentspaces-pdt.md §3.2, PD-V-05): owned
 * by the thread that called {@code editor()}, it changes the nodes it created in
 * place and copies every other node, refuses use from another thread or after
 * {@link #done()}, and {@code done()} hashes the final tree once, writes the new
 * nodes to the store, and returns the value with exactly those nodes.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class Editor<K, V> {

    /**
     * What {@link #done()} returns: the value and the nodes the batch created.
     *
     * @param value the persistent map
     * @param nodes the new nodes written, CID to bytes, in post-order
     */
    public record Result<K, V>(PMap<K, V> value, Map<String, byte[]> nodes) {
    }

    private final RawMap.RawEditor raw;
    private final Codec<K> keys;
    private final Codec<V> values;

    Editor(RawMap.RawEditor raw, Codec<K> keys, Codec<V> values) {
        this.raw = raw;
        this.keys = keys;
        this.values = values;
    }

    /** The value of a key in the batch so far. */
    public Optional<V> get(K key) {
        byte[] bytes = raw.get(keys.encode(Objects.requireNonNull(key, "key")));
        return bytes == null ? Optional.empty() : Optional.of(values.decode(bytes));
    }

    /** The number of entries in the batch so far. */
    public long count() {
        return raw.count();
    }

    /** Binds a key. */
    public Editor<K, V> assoc(K key, V value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        raw.assoc(keys.encode(key), values.encode(value));
        return this;
    }

    /** Removes a key. */
    public Editor<K, V> dissoc(K key) {
        raw.dissoc(keys.encode(Objects.requireNonNull(key, "key")));
        return this;
    }

    /** Binds a key to {@code f} of its value, or of null when absent; a null result removes the key. */
    public Editor<K, V> update(K key, UnaryOperator<V> f) {
        V next = f.apply(get(key).orElse(null));
        return next == null ? dissoc(key) : assoc(key, next);
    }

    /** Freezes the batch, hashes and writes its new nodes, and returns the value with them. */
    public Result<K, V> done() {
        RawMap map = raw.done();
        Map<String, byte[]> nodes = map.flush();
        return new Result<>(new PMap<>(map, keys, values), nodes);
    }
}
