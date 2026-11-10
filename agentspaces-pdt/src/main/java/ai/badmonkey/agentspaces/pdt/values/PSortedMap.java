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

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.Datom;
import ai.badmonkey.agentspaces.pdt.Link;
import ai.badmonkey.agentspaces.pdt.NodeFormatException;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.kinds.DnNode;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * The persistent sorted map of PD-V-10 (PDT 2 phase 2, P2-D-11; srad/agentspaces-pdt2.md §3.1.1):
 * a B+ tree over the {@code dn} node layout with a pluggable comparator, read lazily through a
 * {@link NodeLoader}. An element is its own key, as a datom is in a DataScript index, so the
 * three indexes of a {@code db} root are instances of this map under the orders of
 * {@link DatomOrder}. A branch holds the largest key of each child in the order of its links, the
 * shape DataScript's sorted set writes, so a map written by either side reads on the other.
 *
 * <ul>
 *   <li>{@link #of} builds a map from sorted elements with even leaves of at most the branching
 *       factor, so equal contents give equal roots; {@link #root} writes the new nodes.</li>
 *   <li>{@link #seek} and {@link #contains} read only the nodes on the path to their key.</li>
 *   <li>{@link #diff} prunes every subtree whose CID both maps share, the walk-delta of §3.1.4, so
 *       its cost follows the change.</li>
 * </ul>
 *
 * <p>Values above 1 KiB leave their node as PDT 1 spills them ({@link Spill}); a read resolves
 * them, so an element in memory always holds its value. Edits stay with DataScript's own sorted
 * set in this release (decision 17); the kind {@code s} of a general key-value layout stays
 * reserved.
 */
public final class PSortedMap implements Iterable<Datom> {

    private final NodeStore store;
    private final NodeLoader loader;
    private final String index;
    private final Comparator<Datom> order;
    private final String root;
    private final Flusher pending;

    private PSortedMap(NodeStore store, NodeLoader loader, String index, Comparator<Datom> order, String root,
                       Flusher pending) {
        this.store = Objects.requireNonNull(store, "store");
        this.loader = Objects.requireNonNull(loader, "loader");
        this.index = Objects.requireNonNull(index, "index");
        this.order = Objects.requireNonNull(order, "order");
        this.root = Objects.requireNonNull(root, "root");
        this.pending = pending;
    }

    /**
     * A stored map, read on demand.
     *
     * @param store  the store a later write goes to
     * @param loader the loader of cold nodes
     * @param index  the {@code x} every node carries
     * @param order  the comparator of the keys
     * @param root   the root CID
     * @return the map
     */
    public static PSortedMap load(NodeStore store, NodeLoader loader, String index, Comparator<Datom> order, String root) {
        return new PSortedMap(store, loader, index, order, root, null);
    }

    /**
     * Builds a map from elements in strictly increasing order: leaves of between half and all of
     * {@code branching} elements each (one leaf when fewer), then branches over them the same way.
     *
     * @param store     the store {@link #root} writes to
     * @param index     the {@code x} every node carries
     * @param order     the comparator the elements follow
     * @param branching the most keys a node holds, at least 2
     * @param sorted    the elements
     * @return the map; its nodes reach the store at {@link #root}
     * @throws IllegalArgumentException when the elements are out of order or repeat
     */
    public static PSortedMap of(NodeStore store, String index, Comparator<Datom> order, int branching, List<Datom> sorted) {
        if (branching < 2) {
            throw new IllegalArgumentException("a branching factor is at least 2, found " + branching);
        }
        for (int i = 1; i < sorted.size(); i++) {
            if (order.compare(sorted.get(i - 1), sorted.get(i)) >= 0) {
                throw new IllegalArgumentException("element " + i + " does not follow element " + (i - 1) + " in the order");
            }
        }
        Flusher flusher = new Flusher(store);
        Ctx ctx = context(NodeLoader.of(store));
        List<Datom> keys = new ArrayList<>();
        List<Link> links = new ArrayList<>();
        for (List<Datom> group : groups(sorted, branching)) {
            links.add(new Link(flusher.put(DnNode.leaf(index, spilled(group, flusher, ctx)).encode(), Cids.DAG_CBOR)));
            if (!group.isEmpty()) {
                keys.add(group.get(group.size() - 1));
            }
        }
        long level = 1;
        while (links.size() > 1) {
            List<Datom> upperKeys = new ArrayList<>();
            List<Link> upperLinks = new ArrayList<>();
            int at = 0;
            for (List<Datom> group : groups(keys, branching)) {
                List<Link> children = links.subList(at, at + group.size());
                at += group.size();
                upperLinks.add(new Link(flusher.put(DnNode.branch(level, index, spilled(group, flusher, ctx), children)
                        .encode(), Cids.DAG_CBOR)));
                upperKeys.add(group.get(group.size() - 1));
            }
            keys = upperKeys;
            links = upperLinks;
            level++;
        }
        return new PSortedMap(store, NodeLoader.of(store), index, order, links.get(0).cid(), flusher);
    }

    /** Even groups of at most {@code size}: one empty group for no elements. */
    private static List<List<Datom>> groups(List<Datom> items, int size) {
        if (items.isEmpty()) {
            return List.of(List.of());
        }
        int count = (items.size() + size - 1) / size;
        List<List<Datom>> out = new ArrayList<>(count);
        int at = 0;
        for (int i = 0; i < count; i++) {
            int n = items.size() / count + (i < items.size() % count ? 1 : 0);
            out.add(items.subList(at, at + n));
            at += n;
        }
        return out;
    }

    // ------------------------------------------------------------------ values

    static Ctx context(NodeLoader loader) {
        return new Ctx(loader, KeyHasher.SHA256, null);
    }

    /** The datoms with every text or byte value above the spill limit moved to its own node. */
    static List<Datom> spilled(List<Datom> datoms, Flusher flusher, Ctx ctx) {
        List<Datom> out = new ArrayList<>(datoms.size());
        for (Datom d : datoms) {
            Object v = d.v();
            if (v instanceof String || v instanceof byte[]) {
                Object ref = Spill.spill(CborCodec.defaultCodec().toBytes(v), ctx);
                if (ref instanceof TreeNode node) {
                    out.add(new Datom(d.e(), d.a(), new Link(flusher.cidOf(node)), d.tx()));
                    continue;
                }
            }
            out.add(d);
        }
        return out;
    }

    /** A {@code dn} node read through the loader with its spilled values resolved. */
    static DnNode read(String cid, NodeLoader loader) {
        DnNode node = DnNode.decode(cid, loader.load(cid));
        List<Datom> datoms = node.datoms();
        boolean spilled = false;
        for (Datom d : datoms) {
            spilled |= d.v() instanceof Link;
        }
        if (!spilled) {
            return node;
        }
        Ctx ctx = context(loader);
        List<Datom> resolved = new ArrayList<>(datoms.size());
        for (Datom d : datoms) {
            if (d.v() instanceof Link link) {
                Object v = CborCodec.defaultCodec().fromBytes(Spill.resolve(link.cid(), ctx), Object.class);
                if (!(v instanceof String) && !(v instanceof byte[])) {
                    throw new NodeFormatException(cid, "a spilled datom value is text or bytes, found "
                            + (v == null ? "null" : v.getClass().getSimpleName()));
                }
                resolved.add(new Datom(d.e(), d.a(), v, d.tx()));
            } else {
                resolved.add(d);
            }
        }
        return new DnNode(node.level(), node.index(), resolved, node.children());
    }

    // ------------------------------------------------------------------ reading

    /**
     * The root CID, writing the nodes of a build first.
     *
     * @return the root
     */
    public String root() {
        if (pending != null) {
            pending.commit();
        }
        return root;
    }

    private DnNode node(String cid, long expectedLevel) {
        DnNode node = read(cid, loader);
        if (!index.equals(node.index())) {
            throw new NodeFormatException(cid, "a node of index '" + node.index() + "' under index '" + index + "'");
        }
        if (expectedLevel >= 0 && node.level() != expectedLevel) {
            throw new NodeFormatException(cid, "a child at level " + node.level() + " under a branch expecting level " + expectedLevel);
        }
        return node;
    }

    private DnNode rootNode() {
        return node(root(), -1);
    }

    /** Every element in order. */
    @Override
    public Iterator<Datom> iterator() {
        return seek(null);
    }

    /**
     * The elements from the first one not below {@code from}, in order; the walk reads the nodes on
     * the path to it and then each next leaf as the iteration reaches it.
     *
     * @param from the lower bound, or null for the first element
     * @return the iterator
     */
    public Iterator<Datom> seek(Datom from) {
        return new Walk(from);
    }

    /**
     * Whether an element equal under the comparator is in the map.
     *
     * @param key the element
     * @return true when held
     */
    public boolean contains(Datom key) {
        Iterator<Datom> it = seek(key);
        return it.hasNext() && order.compare(it.next(), key) == 0;
    }

    /** The number of elements; reads every leaf. */
    public long count() {
        long n = 0;
        for (Iterator<Datom> it = iterator(); it.hasNext(); it.next()) {
            n++;
        }
        return n;
    }

    /** A walk from a lower bound: one frame per level, the leaf frame last. */
    private final class Walk implements Iterator<Datom> {
        private final Deque<DnNode> nodes = new ArrayDeque<>();
        private final Deque<Integer> positions = new ArrayDeque<>();

        Walk(Datom from) {
            DnNode node = rootNode();
            while (true) {
                int at = from == null ? 0 : firstNotBelow(node.datoms(), from);
                nodes.push(node);
                positions.push(at);
                if (node.level() == 0 || at >= node.datoms().size()) {
                    break;
                }
                node = node(node.children().get(at).cid(), node.level() - 1);
            }
            settle();
        }

        private int firstNotBelow(List<Datom> keys, Datom from) {
            int low = 0;
            int high = keys.size();
            while (low < high) {
                int mid = (low + high) >>> 1;
                if (order.compare(keys.get(mid), from) < 0) {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            return low;
        }

        /** Moves to the next element when the leaf frame is spent: up to a branch with a next child, then down its leftmost path. */
        private void settle() {
            while (!nodes.isEmpty() && positions.peek() >= nodes.peek().datoms().size()) {
                nodes.pop();
                positions.pop();
                if (nodes.isEmpty()) {
                    return;
                }
                int next = positions.pop() + 1;
                positions.push(next);
                DnNode branch = nodes.peek();
                if (next < branch.datoms().size()) {
                    DnNode child = node(branch.children().get(next).cid(), branch.level() - 1);
                    while (true) {
                        nodes.push(child);
                        positions.push(0);
                        if (child.level() == 0) {
                            break;
                        }
                        child = node(child.children().get(0).cid(), child.level() - 1);
                    }
                }
            }
        }

        @Override
        public boolean hasNext() {
            return !nodes.isEmpty();
        }

        @Override
        public Datom next() {
            if (nodes.isEmpty()) {
                throw new NoSuchElementException();
            }
            int at = positions.pop();
            Datom out = nodes.peek().datoms().get(at);
            positions.push(at + 1);
            settle();
            return out;
        }
    }

    // ------------------------------------------------------------------ diff

    /**
     * The elements one map holds and another does not.
     *
     * @param removed held here and not in the other map, in order
     * @param added   held in the other map and not here, in order
     */
    public record Delta(List<Datom> removed, List<Datom> added) {
    }

    /** A node reference of a diff frontier: the CID, the level a parent gives it, and the node when already read. */
    private record Frontier(String cid, long level, DnNode node) {
    }

    /**
     * The difference to another map of the same index and order (§3.1.4): both frontiers expand
     * from the roots, the highest level first, and drop every CID they share, so a shared subtree
     * is never read; the leaves left hold every changed element.
     *
     * @param other the other map
     * @return what this map holds alone and what the other holds alone
     */
    public Delta diff(PSortedMap other) {
        String mine = root();
        String theirs = other.root();
        if (mine.equals(theirs)) {
            return new Delta(List.of(), List.of());
        }
        DnNode mineRoot = rootNode();
        DnNode theirsRoot = other.rootNode();
        List<Frontier> a = new ArrayList<>(List.of(new Frontier(mine, mineRoot.level(), mineRoot)));
        List<Frontier> b = new ArrayList<>(List.of(new Frontier(theirs, theirsRoot.level(), theirsRoot)));
        while (true) {
            dropShared(a, b);
            long top = 0;
            for (Frontier f : a) {
                top = Math.max(top, f.level());
            }
            for (Frontier f : b) {
                top = Math.max(top, f.level());
            }
            if (top == 0) {
                break;
            }
            a = expand(a, top);
            b = other.expand(b, top);
        }
        List<Datom> left = leaves(a);
        List<Datom> right = other.leaves(b);
        List<Datom> removed = new ArrayList<>();
        List<Datom> added = new ArrayList<>();
        int i = 0;
        int j = 0;
        while (i < left.size() || j < right.size()) {
            int c = i >= left.size() ? 1 : j >= right.size() ? -1 : order.compare(left.get(i), right.get(j));
            if (c < 0) {
                removed.add(left.get(i++));
            } else if (c > 0) {
                added.add(right.get(j++));
            } else {
                i++;
                j++;
            }
        }
        return new Delta(Collections.unmodifiableList(removed), Collections.unmodifiableList(added));
    }

    private static void dropShared(List<Frontier> a, List<Frontier> b) {
        java.util.Set<String> inB = new java.util.HashSet<>();
        for (Frontier f : b) {
            inB.add(f.cid());
        }
        java.util.Set<String> shared = new java.util.HashSet<>();
        for (Frontier f : a) {
            if (inB.contains(f.cid())) {
                shared.add(f.cid());
            }
        }
        a.removeIf(f -> shared.contains(f.cid()));
        b.removeIf(f -> shared.contains(f.cid()));
    }

    /** The frontier with every node at {@code level} replaced by its children, in order. */
    private List<Frontier> expand(List<Frontier> frontier, long level) {
        List<Frontier> out = new ArrayList<>();
        for (Frontier f : frontier) {
            if (f.level() != level) {
                out.add(f);
                continue;
            }
            DnNode node = f.node() != null ? f.node() : node(f.cid(), f.level());
            for (Link child : node.children()) {
                out.add(new Frontier(child.cid(), level - 1, null));
            }
        }
        return out;
    }

    /** The elements of the leaves of a frontier, in order. */
    private List<Datom> leaves(List<Frontier> frontier) {
        List<Datom> out = new ArrayList<>();
        for (Frontier f : frontier) {
            out.addAll((f.node() != null ? f.node() : node(f.cid(), 0)).datoms());
        }
        return out;
    }
}
