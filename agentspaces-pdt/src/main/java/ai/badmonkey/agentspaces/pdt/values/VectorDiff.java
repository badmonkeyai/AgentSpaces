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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The difference between two vectors as index ranges (srad/agentspaces-pdt.md
 * PD-V-07): the ranges of indices present in both whose elements differ, the
 * range appended in mine past the base's count, and the range removed from the
 * base past mine's count. The walk compares the two tries by CID and descends
 * only where they differ, so its cost is proportional to the changes times the
 * depth; the tail region is compared element by element.
 *
 * @param changed      the index ranges, in order, whose elements differ
 * @param appended     the indices in mine beyond the base's count; empty when none
 * @param removed      the indices in the base beyond mine's count; empty when none
 * @param nodesVisited how many trie nodes the walk read, over both tries
 */
public record VectorDiff(List<Range> changed, Range appended, Range removed, int nodesVisited) {

    /**
     * A half-open index range.
     *
     * @param from the first index
     * @param to   one past the last index
     */
    public record Range(long from, long to) {
        /** The number of indices. */
        public long length() {
            return to - from;
        }

        /** Whether the range holds no index. */
        public boolean isEmpty() {
            return to <= from;
        }
    }

    /** Whether the vectors are equal. */
    public boolean isEmpty() {
        return changed.isEmpty() && appended.isEmpty() && removed.isEmpty();
    }

    static <T> VectorDiff of(PVector<T> mine, PVector<T> base) {
        mine.root();
        base.root();
        Walk walk = new Walk(mine.ctx(), base.ctx());
        VecRoot a = mine.node();
        VecRoot b = base.node();
        int common = Math.min(a.n, b.n);
        int trieLimit = Math.min(a.tailoff(), b.tailoff());
        if (trieLimit > 0 && !a.cid.equals(b.cid)) {
            walk.tries(a, b, trieLimit);
        }
        for (int i = trieLimit; i < common; i++) {
            if (!Spill.equalHashed(a.nth(i, walk.mineCtx), b.nth(i, walk.baseCtx))) {
                walk.mark(i);
            }
        }
        walk.close();
        Range appended = new Range(common, a.n);
        Range removed = new Range(common, b.n);
        return new VectorDiff(Collections.unmodifiableList(walk.ranges), appended, removed, walk.visited);
    }

    private static final class Walk {
        final Ctx mineCtx;
        final Ctx baseCtx;
        final List<Range> ranges = new ArrayList<>();
        int visited;
        private long openFrom = -1;
        private long openTo = -1;

        Walk(Ctx mineCtx, Ctx baseCtx) {
            this.mineCtx = mineCtx;
            this.baseCtx = baseCtx;
        }

        void mark(long index) {
            if (openFrom >= 0 && index == openTo) {
                openTo++;
                return;
            }
            close();
            openFrom = index;
            openTo = index + 1;
        }

        void close() {
            if (openFrom >= 0) {
                ranges.add(new Range(openFrom, openTo));
                openFrom = -1;
            }
        }

        /** Compares the two tries over the indices below {@code limit}, which both tries cover. */
        void tries(VecRoot a, VecRoot b, int limit) {
            VecNode na = a.trieNode(mineCtx);
            VecNode nb = b.trieNode(baseCtx);
            int shiftA = a.shift;
            int shiftB = b.shift;
            while (shiftA > shiftB) {
                visited++;
                na = na.child(0, mineCtx);
                shiftA -= VecNode.BITS;
            }
            while (shiftB > shiftA) {
                visited++;
                nb = nb.child(0, baseCtx);
                shiftB -= VecNode.BITS;
            }
            nodes(na, nb, shiftA, 0, limit);
        }

        private void nodes(Object aRef, Object bRef, int shift, long base, int limit) {
            String aCid = Spill.cidOf(aRef);
            if (aCid != null && aCid.equals(Spill.cidOf(bRef))) {
                return;
            }
            VecNode a = aRef instanceof VecNode node ? node : VecNode.decode((String) aRef, mineCtx.loader.load((String) aRef));
            VecNode b = bRef instanceof VecNode node ? node : VecNode.decode((String) bRef, baseCtx.loader.load((String) bRef));
            visited += 2;
            if (a.leaf) {
                int n = Math.min(a.slots.length, b.slots.length);
                for (int i = 0; i < n && base + i < limit; i++) {
                    if (!Spill.equalHashed(a.slots[i], b.slots[i])) {
                        mark(base + i);
                    }
                }
                return;
            }
            int n = Math.min(a.slots.length, b.slots.length);
            for (int i = 0; i < n; i++) {
                long childBase = base + ((long) i << shift);
                if (childBase >= limit) {
                    return;
                }
                nodes(a.slots[i], b.slots[i], shift - VecNode.BITS, childBase, limit);
            }
        }
    }
}
