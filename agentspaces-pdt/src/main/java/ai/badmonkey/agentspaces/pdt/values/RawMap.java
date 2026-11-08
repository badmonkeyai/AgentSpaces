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

import ai.badmonkey.agentspaces.pdt.NodeFormatException;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A persistent map over canonical CBOR bytes (srad/agentspaces-pdt.md §3.2,
 * PD-V-01, PD-V-04, PD-V-08): the CHAMP root, the entry count the root node
 * carries as {@code cnt}, and the backing the value loads from and writes to.
 * {@link PMap} and {@link PSet} add the application codecs. The root CID is
 * computed lazily by {@link #root()}, which hashes and writes the new nodes once.
 */
final class RawMap {

    /** One entry: the key's CBOR and the value reference of {@link Spill}. */
    record RawEntry(byte[] key, Object value) {
    }

    final Backing backing;
    final KeyHasher hasher;
    final ChampNode node;
    final long count;

    RawMap(Backing backing, KeyHasher hasher, ChampNode node, long count) {
        this.backing = backing;
        this.hasher = hasher;
        this.node = node;
        this.count = count;
    }

    static RawMap empty(Backing backing, KeyHasher hasher) {
        return new RawMap(backing, hasher, ChampNode.empty(), 0);
    }

    static RawMap load(Backing backing, KeyHasher hasher, String root) {
        Objects.requireNonNull(root, "root");
        ChampNode node = ChampNode.loadRoot(root, backing.loader().load(root), hasher);
        if (node.collision) {
            throw new NodeFormatException(root, "a map root cannot be a collision node");
        }
        if (node.cnt < 0) {
            throw new NodeFormatException(root, "map root has no 'cnt'");
        }
        return new RawMap(backing, hasher, node, node.cnt);
    }

    Ctx ctx() {
        return new Ctx(backing.loader(), hasher, null);
    }

    /** The value reference for a key, or {@link ChampNode#NOT_FOUND}. */
    Object find(byte[] key) {
        return node.find(hasher.hash(key), 0, key, ctx());
    }

    byte[] get(byte[] key) {
        Object ref = find(key);
        return ref == ChampNode.NOT_FOUND ? null : Spill.resolve(ref, ctx());
    }

    RawMap assoc(byte[] key, byte[] value) {
        Spill.checkKey(key);
        Ctx ctx = ctx();
        ChampNode.Change change = new ChampNode.Change();
        ChampNode next = node.assoc(hasher.hash(key), 0, key, Spill.spill(value, ctx), ctx, change);
        if (!change.happened()) {
            return this;
        }
        return new RawMap(backing, hasher, next, change.added ? count + 1 : count);
    }

    RawMap dissoc(byte[] key) {
        Ctx ctx = ctx();
        ChampNode.Change change = new ChampNode.Change();
        ChampNode next = node.dissoc(hasher.hash(key), 0, key, ctx, change);
        if (!change.removed) {
            return this;
        }
        return new RawMap(backing, hasher, next, count - 1);
    }

    /** The root CID, hashing and writing the new nodes on first use (PD-V-08). */
    String root() {
        String cid = node.cid;
        if (cid != null) {
            return cid;
        }
        flush();
        return node.cid;
    }

    /** Hashes and writes the new nodes and returns them. */
    Map<String, byte[]> flush() {
        synchronized (node) {
            if (node.cid == null) {
                node.cnt = count;
            }
            return Flusher.flush(node, backing.store());
        }
    }

    int depth() {
        return node.depth(0, ctx());
    }

    RawIterator entries() {
        return new RawIterator(node, ctx());
    }

    RawEditor editor() {
        return new RawEditor(this);
    }

    // ------------------------------------------------------------------ iteration

    /** Entries in slot order: within a node, slot 0 to 31, an entry or a child's entries per slot. */
    static final class RawIterator implements Iterator<RawEntry> {

        private static final class Frame {
            final ChampNode node;
            final int level;
            int slot;

            Frame(ChampNode node, int level) {
                this.node = node;
                this.level = level;
            }
        }

        private final Ctx ctx;
        private final Deque<Frame> stack = new ArrayDeque<>();
        private RawEntry next;

        RawIterator(ChampNode root, Ctx ctx) {
            this.ctx = ctx;
            stack.push(new Frame(root, 0));
            advance();
        }

        private void advance() {
            next = null;
            while (!stack.isEmpty()) {
                Frame frame = stack.peek();
                ChampNode node = frame.node;
                if (node.collision) {
                    if (frame.slot < node.keys.length) {
                        int i = frame.slot++;
                        next = new RawEntry(node.keys[i], node.values[i]);
                        return;
                    }
                    stack.pop();
                    continue;
                }
                boolean pushed = false;
                while (frame.slot < 32) {
                    int bit = 1 << frame.slot++;
                    if ((node.datamap & bit) != 0) {
                        int i = Integer.bitCount(node.datamap & (bit - 1));
                        next = new RawEntry(node.keys[i], node.values[i]);
                        return;
                    }
                    if ((node.nodemap & bit) != 0) {
                        int i = Integer.bitCount(node.nodemap & (bit - 1));
                        stack.push(new Frame(node.child(i, frame.level + 1, ctx), frame.level + 1));
                        pushed = true;
                        break;
                    }
                }
                if (!pushed) {
                    stack.pop();
                }
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public RawEntry next() {
            if (next == null) {
                throw new NoSuchElementException();
            }
            RawEntry out = next;
            advance();
            return out;
        }
    }

    // ------------------------------------------------------------------ transient

    /** The transient of PD-V-05 over raw bytes. */
    static final class RawEditor {

        private final Backing backing;
        private final KeyHasher hasher;
        private final AtomicReference<Thread> edit;
        private final Ctx ctx;
        private ChampNode node;
        private long count;

        RawEditor(RawMap from) {
            this.backing = from.backing;
            this.hasher = from.hasher;
            this.edit = new AtomicReference<>(Thread.currentThread());
            this.ctx = new Ctx(backing.loader(), hasher, edit);
            this.node = from.node;
            this.count = from.count;
        }

        void ensureEditable() {
            Thread owner = edit.get();
            if (owner == null) {
                throw new IllegalStateException("editor already done");
            }
            if (owner != Thread.currentThread()) {
                throw new IllegalStateException("editor owned by thread " + owner.getName()
                        + ", used from " + Thread.currentThread().getName());
            }
        }

        long count() {
            ensureEditable();
            return count;
        }

        Object find(byte[] key) {
            ensureEditable();
            return node.find(hasher.hash(key), 0, key, ctx);
        }

        byte[] get(byte[] key) {
            Object ref = find(key);
            return ref == ChampNode.NOT_FOUND ? null : Spill.resolve(ref, ctx);
        }

        /** Returns true when the key was new. */
        boolean assoc(byte[] key, byte[] value) {
            ensureEditable();
            Spill.checkKey(key);
            ChampNode.Change change = new ChampNode.Change();
            node = node.assoc(hasher.hash(key), 0, key, Spill.spill(value, ctx), ctx, change);
            if (change.added) {
                count++;
            }
            return change.added;
        }

        /** Returns true when the key was present. */
        boolean dissoc(byte[] key) {
            ensureEditable();
            ChampNode.Change change = new ChampNode.Change();
            node = node.dissoc(hasher.hash(key), 0, key, ctx, change);
            if (change.removed) {
                count--;
            }
            return change.removed;
        }

        /** Freezes the nodes, hashes and writes the new ones, and returns the value. */
        RawMap done() {
            ensureEditable();
            edit.set(null);
            return new RawMap(backing, hasher, node, count);
        }
    }
}
