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

/**
 * The transient of a {@link PSet} (srad/agentspaces-pdt.md PD-V-05): owned by the
 * thread that called {@code editor()}, in place on the nodes it created, one delta
 * at {@link #done()}.
 *
 * @param <T> the element type
 */
public final class SetEditor<T> {

    /**
     * What {@link #done()} returns.
     *
     * @param value the persistent set
     * @param nodes the new nodes written, CID to bytes
     */
    public record Result<T>(PSet<T> value, Map<String, byte[]> nodes) {
    }

    private final RawMap.RawEditor raw;
    private final Codec<T> codec;

    SetEditor(RawMap.RawEditor raw, Codec<T> codec) {
        this.raw = raw;
        this.codec = codec;
    }

    /** Whether the element is present in the batch so far. */
    public boolean contains(T element) {
        return raw.find(codec.encode(Objects.requireNonNull(element, "element"))) != ChampNode.NOT_FOUND;
    }

    /** The number of elements in the batch so far. */
    public long count() {
        return raw.count();
    }

    /** Adds an element. */
    public SetEditor<T> conj(T element) {
        raw.assoc(codec.encode(Objects.requireNonNull(element, "element")), PSet.NIL);
        return this;
    }

    /** Removes an element. */
    public SetEditor<T> disj(T element) {
        raw.dissoc(codec.encode(Objects.requireNonNull(element, "element")));
        return this;
    }

    /** Freezes the batch, hashes and writes its new nodes, and returns the value with them. */
    public Result<T> done() {
        RawMap map = raw.done();
        Map<String, byte[]> nodes = map.flush();
        return new Result<>(new PSet<>(map, codec), nodes);
    }
}
