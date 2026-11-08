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
import java.util.concurrent.atomic.AtomicReference;

/**
 * The transient of a {@link PVector} (srad/agentspaces-pdt.md PD-V-05): owned by
 * the thread that called {@code editor()}, it changes the nodes it created in
 * place and copies every other node, refuses use from another thread or after
 * {@link #done()}, and {@code done()} hashes the final tree once.
 *
 * @param <T> the element type
 */
public final class VectorEditor<T> {

    /**
     * What {@link #done()} returns.
     *
     * @param value the persistent vector
     * @param nodes the new nodes written, CID to bytes
     */
    public record Result<T>(PVector<T> value, Map<String, byte[]> nodes) {
    }

    private final Backing backing;
    private final Codec<T> codec;
    private final AtomicReference<Thread> edit;
    private final Ctx ctx;
    private VecRoot node;

    VectorEditor(Backing backing, Codec<T> codec, VecRoot from) {
        this.backing = backing;
        this.codec = codec;
        this.edit = new AtomicReference<>(Thread.currentThread());
        this.ctx = new Ctx(backing.loader(), KeyHasher.SHA256, edit);
        this.node = from;
    }

    private void ensureEditable() {
        Thread owner = edit.get();
        if (owner == null) {
            throw new IllegalStateException("editor already done");
        }
        if (owner != Thread.currentThread()) {
            throw new IllegalStateException("editor owned by thread " + owner.getName()
                    + ", used from " + Thread.currentThread().getName());
        }
    }

    /** The number of elements in the batch so far. */
    public long count() {
        ensureEditable();
        return node.n;
    }

    /** The element at an index in the batch so far. */
    public T nth(long index) {
        ensureEditable();
        return codec.decode(Spill.resolve(node.nth((int) index, ctx), ctx));
    }

    /** Appends an element. */
    public VectorEditor<T> conj(T element) {
        ensureEditable();
        Objects.requireNonNull(element, "element");
        node = node.conj(Spill.spill(codec.encode(element), ctx), ctx);
        return this;
    }

    /** Removes the last element. */
    public VectorEditor<T> pop() {
        ensureEditable();
        node = node.pop(ctx);
        return this;
    }

    /** Replaces the element at an index. */
    public VectorEditor<T> assoc(long index, T element) {
        ensureEditable();
        Objects.requireNonNull(element, "element");
        node = node.assocN((int) index, Spill.spill(codec.encode(element), ctx), ctx);
        return this;
    }

    /** Freezes the batch, hashes and writes its new nodes, and returns the value with them. */
    public Result<T> done() {
        ensureEditable();
        edit.set(null);
        PVector<T> value = new PVector<>(backing, codec, node);
        Map<String, byte[]> nodes = value.flush();
        return new Result<>(value, nodes);
    }
}
