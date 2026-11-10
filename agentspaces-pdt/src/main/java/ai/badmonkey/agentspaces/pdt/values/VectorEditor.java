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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The transient of a {@link PVector} (srad/agentspaces-pdt.md PD-V-05): owned by
 * the thread that called {@code editor()}, it changes the nodes it created in
 * place and copies every other node, refuses use from another thread or after
 * {@link #done()}, and {@code done()} hashes the final tree once. It splices as the vector does
 * (PDT 2 phase 6, P2-R-02), and a relaxed batch runs the operations of {@link Rrb}.
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
        int i = (int) index;
        return codec.decode(Spill.resolve(node.relaxed ? Rrb.nth(node, i, ctx) : node.nth(i, ctx), ctx));
    }

    /** Appends an element. */
    public VectorEditor<T> conj(T element) {
        ensureEditable();
        Objects.requireNonNull(element, "element");
        Object item = Spill.spill(codec.encode(element), ctx);
        node = node.relaxed ? Rrb.conj(node, item, ctx) : node.conj(item, ctx);
        return this;
    }

    /** Removes the last element. */
    public VectorEditor<T> pop() {
        ensureEditable();
        node = node.relaxed ? Rrb.pop(node, ctx) : node.pop(ctx);
        return this;
    }

    /** Replaces the element at an index. */
    public VectorEditor<T> assoc(long index, T element) {
        ensureEditable();
        Objects.requireNonNull(element, "element");
        Object item = Spill.spill(codec.encode(element), ctx);
        node = node.relaxed ? Rrb.assoc(node, (int) index, item, ctx) : node.assocN((int) index, item, ctx);
        return this;
    }

    /**
     * Replaces {@code deleteCount} elements from {@code index} with {@code elements} (PDT 2 phase 6,
     * P2-R-02), as {@link PVector#splice} does.
     */
    public VectorEditor<T> splice(long index, long deleteCount, List<? extends T> elements) {
        ensureEditable();
        if (index < 0 || deleteCount < 0 || index + deleteCount > node.n) {
            throw new IndexOutOfBoundsException("range [" + index + ", " + (index + deleteCount) + ") of a vector of " + node.n);
        }
        Object[] items = new Object[elements.size()];
        for (int i = 0; i < items.length; i++) {
            items[i] = Spill.spill(codec.encode(Objects.requireNonNull(elements.get(i), "element")), ctx);
        }
        node = Rrb.splice(node, (int) index, (int) deleteCount, items, ctx);
        return this;
    }

    /** Inserts an element before an index, at most the count (PDT 2 phase 6, P2-R-02). */
    public VectorEditor<T> insertAt(long index, T element) {
        return splice(index, 0, List.of(Objects.requireNonNull(element, "element")));
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
