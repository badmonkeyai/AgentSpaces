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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The difference between two maps (srad/agentspaces-pdt.md PD-V-07): the keys
 * added, removed, and changed in {@code mine} against {@code base}. The walk
 * compares the two tries slot by slot and descends only into children whose CIDs
 * differ, so its cost is proportional to the number of changes times the depth;
 * {@link #nodesVisited()} reports the count. Both maps are hashed first, since the
 * pruning needs CIDs.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class Diff<K, V> {

    /**
     * A changed value.
     *
     * @param before the value in the base
     * @param after  the value in mine
     */
    public record Change<V>(V before, V after) {
    }

    /** A raw change: a key's CBOR, its base reference (null when added), and its new reference (null when removed). */
    record RawChange(byte[] key, Object before, Object after) {
        boolean added() {
            return before == null;
        }

        boolean removed() {
            return after == null;
        }
    }

    /** The raw result Merge works on. */
    record Raw(List<RawChange> changes, int nodesVisited) {
    }

    private final Raw raw;
    private final Map<K, V> added = new LinkedHashMap<>();
    private final Map<K, V> removed = new LinkedHashMap<>();
    private final Map<K, Change<V>> changed = new LinkedHashMap<>();

    private Diff(Raw raw, PMap<K, V> mine, PMap<K, V> base) {
        this.raw = raw;
        Ctx mineCtx = mine.raw().ctx();
        Ctx baseCtx = base.raw().ctx();
        Codec<K> keys = mine.keyCodec();
        Codec<V> values = mine.valueCodec();
        for (RawChange change : raw.changes()) {
            K key = keys.decode(change.key());
            if (change.added()) {
                added.put(key, values.decode(Spill.resolve(change.after(), mineCtx)));
            } else if (change.removed()) {
                removed.put(key, values.decode(Spill.resolve(change.before(), baseCtx)));
            } else {
                changed.put(key, new Change<>(values.decode(Spill.resolve(change.before(), baseCtx)),
                        values.decode(Spill.resolve(change.after(), mineCtx))));
            }
        }
    }

    static <K, V> Diff<K, V> of(PMap<K, V> mine, PMap<K, V> base) {
        return new Diff<>(raw(mine.raw(), base.raw()), mine, base);
    }

    /** Keys in mine and absent from base, with their values, in slot order. */
    public Map<K, V> added() {
        return Collections.unmodifiableMap(added);
    }

    /** Keys in base and absent from mine, with their base values, in slot order. */
    public Map<K, V> removed() {
        return Collections.unmodifiableMap(removed);
    }

    /** Keys in both whose values differ, in slot order. */
    public Map<K, Change<V>> changed() {
        return Collections.unmodifiableMap(changed);
    }

    /** Whether the maps are equal. */
    public boolean isEmpty() {
        return raw.changes().isEmpty();
    }

    /** How many nodes the walk read, over both tries. */
    public int nodesVisited() {
        return raw.nodesVisited();
    }

    @Override
    public String toString() {
        return "Diff[added=" + added.size() + ", removed=" + removed.size() + ", changed=" + changed.size() + "]";
    }

    // ------------------------------------------------------------------ the raw walk

    static Raw raw(RawMap mine, RawMap base) {
        mine.root();
        base.root();
        Walk walk = new Walk(mine.ctx(), base.ctx());
        walk.nodes(mine.node, base.node, 0);
        return new Raw(Collections.unmodifiableList(walk.out), walk.visited);
    }

    private static final class Walk {
        final Ctx mineCtx;
        final Ctx baseCtx;
        final List<RawChange> out = new ArrayList<>();
        int visited;

        Walk(Ctx mineCtx, Ctx baseCtx) {
            this.mineCtx = mineCtx;
            this.baseCtx = baseCtx;
        }

        /** Two child references: equal CIDs prune without a load; otherwise both nodes are read. */
        void nodes(Object aRef, Object bRef, int level) {
            String aCid = Spill.cidOf(aRef);
            if (aCid != null && aCid.equals(Spill.cidOf(bRef))) {
                return;
            }
            ChampNode a = aRef instanceof ChampNode node ? node : ChampNode.load((String) aRef, level, mineCtx);
            ChampNode b = bRef instanceof ChampNode node ? node : ChampNode.load((String) bRef, level, baseCtx);
            visited += 2;
            if (a.collision || b.collision) {
                collisions(a, b);
                return;
            }
            for (int slot = 0; slot < 32; slot++) {
                int bit = 1 << slot;
                boolean aEntry = (a.datamap & bit) != 0;
                boolean aChild = (a.nodemap & bit) != 0;
                boolean bEntry = (b.datamap & bit) != 0;
                boolean bChild = (b.nodemap & bit) != 0;
                if (!aEntry && !aChild && !bEntry && !bChild) {
                    continue;
                }
                if (aEntry && bEntry) {
                    int i = Integer.bitCount(a.datamap & (bit - 1));
                    int j = Integer.bitCount(b.datamap & (bit - 1));
                    if (Arrays.equals(a.keys[i], b.keys[j])) {
                        value(a.keys[i], b.values[j], a.values[i]);
                    } else {
                        out.add(new RawChange(b.keys[j], b.values[j], null));
                        out.add(new RawChange(a.keys[i], null, a.values[i]));
                    }
                } else if (aChild && bChild) {
                    int i = Integer.bitCount(a.nodemap & (bit - 1));
                    int j = Integer.bitCount(b.nodemap & (bit - 1));
                    nodes(a.children[i], b.children[j], level + 1);
                } else if (aEntry && bChild) {
                    int i = Integer.bitCount(a.datamap & (bit - 1));
                    int j = Integer.bitCount(b.nodemap & (bit - 1));
                    entryAgainstSubtree(a.keys[i], a.values[i], b.child(j, level + 1, baseCtx), level + 1, true);
                } else if (aChild && bEntry) {
                    int i = Integer.bitCount(a.nodemap & (bit - 1));
                    int j = Integer.bitCount(b.datamap & (bit - 1));
                    entryAgainstSubtree(b.keys[j], b.values[j], a.child(i, level + 1, mineCtx), level + 1, false);
                } else if (aEntry) {
                    int i = Integer.bitCount(a.datamap & (bit - 1));
                    out.add(new RawChange(a.keys[i], null, a.values[i]));
                } else if (bEntry) {
                    int j = Integer.bitCount(b.datamap & (bit - 1));
                    out.add(new RawChange(b.keys[j], b.values[j], null));
                } else if (aChild) {
                    int i = Integer.bitCount(a.nodemap & (bit - 1));
                    subtree(a.child(i, level + 1, mineCtx), level + 1, mineCtx, true, null);
                } else {
                    int j = Integer.bitCount(b.nodemap & (bit - 1));
                    subtree(b.child(j, level + 1, baseCtx), level + 1, baseCtx, false, null);
                }
            }
        }

        private void value(byte[] key, Object before, Object after) {
            if (!Spill.equalHashed(before, after)) {
                out.add(new RawChange(key, before, after));
            }
        }

        /** One entry on one side against a whole subtree on the other. */
        private void entryAgainstSubtree(byte[] key, Object value, ChampNode subtree, int level, boolean entryIsMine) {
            Ctx ctx = entryIsMine ? baseCtx : mineCtx;
            Object other = subtree.find(ctx.hasher.hash(key), level, key, ctx);
            if (other == ChampNode.NOT_FOUND) {
                out.add(entryIsMine ? new RawChange(key, null, value) : new RawChange(key, value, null));
            } else if (entryIsMine) {
                value(key, other, value);
            } else {
                value(key, value, other);
            }
            subtree(subtree, level, ctx, !entryIsMine, key);
        }

        /**
         * Every entry of a subtree as added (mine) or removed (base), skipping one key.
         * The walk keeps its own stack, and each child loads at its level, so a chain
         * deeper than the collision level is refused before the stack can overflow.
         */
        private void subtree(ChampNode root, int rootLevel, Ctx ctx, boolean isMine, byte[] skip) {
            Deque<ChampNode> nodes = new ArrayDeque<>();
            Deque<Integer> levels = new ArrayDeque<>();
            nodes.push(root);
            levels.push(rootLevel);
            while (!nodes.isEmpty()) {
                ChampNode node = nodes.pop();
                int level = levels.pop();
                visited++;
                for (int i = 0; i < node.keys.length; i++) {
                    if (skip != null && Arrays.equals(skip, node.keys[i])) {
                        continue;
                    }
                    out.add(isMine ? new RawChange(node.keys[i], null, node.values[i])
                            : new RawChange(node.keys[i], node.values[i], null));
                }
                for (int i = node.children.length - 1; i >= 0; i--) {
                    nodes.push(node.child(i, level + 1, ctx));
                    levels.push(level + 1);
                }
            }
        }

        /** Two sorted key lists, merged. */
        private void collisions(ChampNode a, ChampNode b) {
            int i = 0;
            int j = 0;
            while (i < a.keys.length || j < b.keys.length) {
                int order = i >= a.keys.length ? 1 : j >= b.keys.length ? -1 : Arrays.compareUnsigned(a.keys[i], b.keys[j]);
                if (order == 0) {
                    value(a.keys[i], b.values[j], a.values[i]);
                    i++;
                    j++;
                } else if (order < 0) {
                    out.add(new RawChange(a.keys[i], null, a.values[i]));
                    i++;
                } else {
                    out.add(new RawChange(b.keys[j], b.values[j], null));
                    j++;
                }
            }
        }
    }
}
