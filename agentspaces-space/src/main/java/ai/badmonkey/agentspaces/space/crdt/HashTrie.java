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
package ai.badmonkey.agentspaces.space.crdt;

import ai.badmonkey.agentspaces.api.entry.EntryId;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.UnaryOperator;

/**
 * The fingerprinted trie of a replicated space (srad/replication-by-entry.md §3.1, RE-M-03; SPEC annex I.6): an
 * immutable, persistent 16-ary trie of {@link Slot}s keyed by the nibbles of SHA-256(entry id), most significant
 * first. A change copies the nodes on one path, about log16(n) of them, and shares the rest with the trie it came
 * from, so a replica pays O(log n) per changed entry where it used to copy its whole map.
 *
 * <p><b>Shape.</b> The tree is stored in the canonical shape of annex I.6 over the slots it holds: a node is a leaf
 * when it holds at most {@value #LEAF_MAX} slots or sits at 64 nibbles, and an inner node otherwise. An insert that
 * takes a leaf past the threshold splits it, and a removal that brings an inner node down to it collapses the
 * subtree into one leaf, so the stored shape is a function of the set of ids alone, never of the history.
 *
 * <p><b>Fingerprints.</b> The fingerprints are those of annex I.6 over the <em>audited</em> set: the slots with at
 * least one audited line ({@link Slot#audited}). In phase 1 every stored slot is audited and the two shapes are one;
 * a slot that holds lines but none audited (phase 4's lapsed entry, RE-T-06, or an id holding a control character)
 * is stored for the map's sake but left out of every count and preimage. Each node therefore keeps two counts: the
 * slots it stores, which give the stored shape, and the audited slots, which give the audited shape. An audited node
 * is a leaf whenever its stored node is (the audited count never exceeds the stored one); a stored inner node whose
 * audited count is at most the threshold answers as one leaf of the lines under it, as the annex requires, so the
 * fingerprints are exactly the annex's over the audited set whatever the stored shape holds besides.
 *
 * <p><b>Laziness.</b> No fingerprint is computed when a trie is made. A node hashes its preimage the first time its
 * fingerprint is asked for and keeps the SHA-256 for its life; a new path's nodes start unhashed and the nodes it
 * shares are already hashed. A batch of a thousand merges then hashes nothing until the stream hash is asked for,
 * and then hashes only the nodes of the final trie that changed, never an intermediate one.
 *
 * <p>Immutable and safe to share across threads: the memo is the only state written after construction, it is
 * written whole through a volatile field, and two threads that race to fill it compute the same bytes.
 *
 * @param <C> the claim the slots carry beside each state
 */
public final class HashTrie<C> implements Iterable<Slot<C>> {

    /** Children per inner node: one per nibble. */
    public static final int FANOUT = 16;
    /** The leaf threshold T of annex I.6. */
    public static final int LEAF_MAX = 32;
    /** Nibbles in a key, the deepest path; a node there is a leaf whatever its count. */
    public static final int KEY_NIBBLES = 64;
    /** Bytes of a node's fingerprint: the first 16 of its preimage's SHA-256. */
    public static final int FINGERPRINT_BYTES = 16;
    /** Bytes of an inner node's preimage: sixteen times a fingerprint and an 8-byte count. */
    public static final int INNER_PREIMAGE_BYTES = FANOUT * (FINGERPRINT_BYTES + 8);

    private static final HashTrie<?> EMPTY = new HashTrie<>(null);
    private static final byte[] EMPTY_STREAM_HASH = Slot.sha256().digest(new byte[0]);
    /** Fingerprints computed by every trie of the process: what RE-Q-PRF-02's round test counts. */
    private static final java.util.concurrent.atomic.LongAdder FINGERPRINTS = new java.util.concurrent.atomic.LongAdder();

    /** How many node fingerprints the tries of this process have computed, each memoized one counted once. */
    static long fingerprintsComputed() {
        return FINGERPRINTS.sum();
    }
    /** Slots the line builds of every trie of the process have gathered: what the audit's cost tests count. */
    private static final java.util.concurrent.atomic.LongAdder GATHERED = new java.util.concurrent.atomic.LongAdder();

    /** How many slots the tries of this process have gathered to build lines, by any build, bounded or whole. */
    static long slotsGathered() {
        return GATHERED.sum();
    }

    /** The root, or null for the empty trie. */
    private final Node root;

    private HashTrie(Node root) {
        this.root = root;
    }

    /**
     * Returns the empty trie.
     *
     * @param <C> the claim type
     * @return the empty trie
     */
    @SuppressWarnings("unchecked")
    public static <C> HashTrie<C> empty() {
        return (HashTrie<C>) EMPTY;
    }

    /**
     * Returns a builder that starts empty, for a join: it sorts what it is given once and builds each node once.
     *
     * @param <C> the claim type
     * @return the builder
     */
    public static <C> Builder<C> builder() {
        return new Builder<>(empty());
    }

    /** Returns a builder that starts from this trie, for a batch of changes made as one. */
    public Builder<C> toBuilder() {
        return new Builder<>(this);
    }

    // ---- the map

    /**
     * Returns the slot of an id.
     *
     * @param id the entry id
     * @return the slot, or null when the trie holds none for the id
     */
    public Slot<C> get(EntryId id) {
        Objects.requireNonNull(id, "id");
        if (root == null) {
            return null;
        }
        return find(root, Slot.empty(id), 0);
    }

    /** Returns whether the trie holds a slot for the id. */
    public boolean containsKey(EntryId id) {
        return get(id) != null;
    }

    /**
     * Returns the trie with this slot in place of the id's current one; an empty slot removes the id.
     *
     * @param slot the slot
     * @return the changed trie, or this one when the id already held this very slot
     */
    public HashTrie<C> put(Slot<C> slot) {
        Objects.requireNonNull(slot, "slot");
        if (slot.isEmpty()) {
            return remove(slot.id());
        }
        Node next = root == null ? new Leaf(new Slot<?>[] {slot}) : insert(root, slot, 0);
        return next == root ? this : new HashTrie<>(next);
    }

    /**
     * Returns the trie without the id's slot.
     *
     * @param id the entry id
     * @return the changed trie, or this one when the id held no slot
     */
    public HashTrie<C> remove(EntryId id) {
        Objects.requireNonNull(id, "id");
        if (root == null) {
            return this;
        }
        Node next = delete(root, Slot.empty(id), 0);
        return next == root ? this : new HashTrie<>(next);
    }

    /**
     * Returns the trie with the id's slot changed by the operator, which is handed the current slot or an empty one;
     * an empty result removes the id.
     *
     * @param id the entry id
     * @param op the change
     * @return the changed trie, or this one when the operator returned the slot it was given
     */
    public HashTrie<C> update(EntryId id, UnaryOperator<Slot<C>> op) {
        Slot<C> current = get(id);
        Slot<C> given = current != null ? current : Slot.empty(id);
        Slot<C> next = Objects.requireNonNull(op.apply(given), "slot");
        if (next == given) {
            return this;
        }
        if (!next.id().equals(id)) {
            throw new IllegalArgumentException("the operator changed the slot's id: " + id + " to " + next.id());
        }
        return put(next);
    }

    /** Returns the number of slots the trie stores. */
    public int size() {
        return root == null ? 0 : root.size;
    }

    /** Returns whether the trie stores no slot. */
    public boolean isEmpty() {
        return root == null;
    }

    /** Returns the number of audited slots: the count of the root in annex I.6. */
    public int auditedCount() {
        return root == null ? 0 : root.audited;
    }

    /** Iterates the slots in key order, the unsigned order of SHA-256(entry id). */
    @Override
    public Iterator<Slot<C>> iterator() {
        return new SlotIterator<>(root);
    }

    // ---- the audit

    /**
     * Returns the stream hash of annex I.6: the whole SHA-256 of the audited root's preimage, the SHA-256 of zero
     * bytes for an empty set. Memoized, so asking again for an unchanged trie costs nothing.
     *
     * @return a copy of the 32-byte hash
     */
    public byte[] streamHash() {
        if (root == null || root.audited == 0) {
            return EMPTY_STREAM_HASH.clone();
        }
        return digest(root, 0).clone();
    }

    /**
     * Returns how many nibbles deep the stored tree runs: 0 for a root that is a leaf (or an empty trie), else one more
     * than its deepest inner child. A status figure (RE-Q-MNT-02): a space of a million slots runs about five deep, and
     * a depth far past that names ids crafted to share a prefix. It walks every inner node, so it is for a status page,
     * not for every round.
     *
     * @return the depth
     */
    public int depth() {
        return root == null ? 0 : depthOf(root);
    }

    private static int depthOf(Node node) {
        if (!(node instanceof Inner inner)) {
            return 0;
        }
        int deepest = 0;
        for (Node child : inner.children) {
            if (child != null) {
                deepest = Math.max(deepest, depthOf(child));
            }
        }
        return 1 + deepest;
    }

    /**
     * Returns the audited node at a path, as an audit reads it (annex I.6, <em>A tree node</em>): its count, and its
     * 384-byte preimage when it is an inner node or its lines when it is a leaf. Phase 4 answers {@code tree}
     * requests from this; phase 1 sends nothing from it.
     *
     * @param path one nibble per byte, each 0 to 15, at most 64 bytes; the root's path is empty
     * @return the node, empty when no audited slot lies under the path
     * @throws IllegalArgumentException for a byte above 15 or a path longer than 64
     */
    public AuditNode node(byte[] path) {
        checkPath(path);
        byte[] at = path.clone();
        Node node = root;
        int depth = 0;
        while (node instanceof Inner inner && depth < at.length) {
            node = inner.children[at[depth]];
            depth++;
        }
        if (node == null) {
            return AuditNode.empty(at);
        }
        if (depth == at.length) {
            if (node.audited == 0) {
                return AuditNode.empty(at);
            }
            if (auditedLeaf(node, depth)) {
                return AuditNode.leaf(at, node.audited, leafPreimage(collect(node, null, 0)));
            }
            return AuditNode.inner(at, node.audited, innerPreimage((Inner) node, depth));
        }
        // The path runs below a stored leaf: the slots of that leaf that lie under it make an audited leaf of their
        // own, since a stored leaf holds at most the threshold and every node under it does too.
        List<Slot<?>> under = collect(node, at, depth);
        int count = 0;
        for (Slot<?> slot : under) {
            if (slot.audited()) {
                count++;
            }
        }
        return count == 0 ? AuditNode.empty(at) : AuditNode.leaf(at, count, leafPreimage(under));
    }

    /**
     * Returns every audited line under a path, as one leaf's preimage would join them (annex I.6), whatever the count
     * there: what an answer of {@code lines} carries when the asker's own count chose that form for an inner node, and
     * what an asker compares a partner's lines with.
     *
     * @param path one nibble per byte, each 0 to 15, at most 64 bytes; the root's path is empty
     * @return the joined lines as UTF-8, empty when no audited line lies under the path
     * @throws IllegalArgumentException for a byte above 15 or a path longer than 64
     */
    public byte[] linesUnder(byte[] path) {
        return linesUnder(path, Long.MAX_VALUE);
    }

    /**
     * {@link #linesUnder(byte[])} when the joined lines hold at most {@code maxBytes} bytes, and null otherwise. The
     * slots are measured as they are gathered and the walk stops once the lines pass the bound, so a partner that
     * answers {@code nodes} for lines past the budget (annex I.6, step 4) never gathers and sorts the whole subtree.
     *
     * @param path     one nibble per byte, each 0 to 15, at most 64 bytes; the root's path is empty
     * @param maxBytes the most bytes the joined lines may hold
     * @return the joined lines as UTF-8, or null when they hold more than {@code maxBytes}
     * @throws IllegalArgumentException for a byte above 15 or a path longer than 64
     */
    public byte[] linesUnder(byte[] path, long maxBytes) {
        checkPath(path);
        Node node = root;
        int depth = 0;
        while (node instanceof Inner inner && depth < path.length) {
            node = inner.children[path[depth]];
            depth++;
        }
        if (node == null || node.audited == 0) {
            return new byte[0];
        }
        if (maxBytes == Long.MAX_VALUE) {
            return leafPreimage(depth == path.length ? collect(node, null, 0) : collect(node, path, depth));
        }
        if (depth == path.length && node.bytes - 1 > maxBytes) {
            // The node's exact byte total says the walk would pass the bound, so it is not made: before, each such
            // request gathered up to the bound's worth of lines and threw them away, and one body of 256 of them cost
            // a partner of a million entries about 140 ms (the TypeScript adversarial review of 2026-11-15, ADV-TS-4,
            // which Java shared). Below a stored leaf the walk reads at most the threshold of slots, so it stays.
            return null;
        }
        List<Slot<?>> slots = new ArrayList<>(Math.min(node.size, 64));
        // The joined size so far: each distinct line and the LF before it, less the LF the first line lacks.
        long[] size = {-1};
        boolean fits = collectWithin(node, depth == path.length ? null : path, depth, slots, size, maxBytes);
        GATHERED.add(slots.size());
        return fits ? leafPreimage(slots) : null;
    }

    /**
     * Returns whether a line is among the audited lines under a path: the slot of the entry id the line names lies
     * under the path and lists the line. Every audited line names its own slot's id (a state or claim line by
     * construction, an acknowledged line by {@link Slot#withAck}'s check), so this reads one slot and answers what a
     * comparison with every line under the path answers. An asker compares a partner's lines this way in proportion to
     * the answer, never to its own set (the TypeScript adversarial review of 2026-11-15, ADV-TS-3: a member of a
     * million entries spent 2.7 s per round gathering and sorting its whole set to compare one small partner's root).
     *
     * @param line the line, as a partner sent it
     * @param path one nibble per byte, each 0 to 15, at most 64 bytes; the root's path is empty
     * @return whether the trie holds the line under the path; false for a line of no state or claim shape
     * @throws IllegalArgumentException for a byte above 15 or a path longer than 64
     */
    public boolean holdsLine(String line, byte[] path) {
        checkPath(path);
        // The id a line names, as the audit reads it: between the kind's "e:" or "c:" and the last '=', since a state
        // hash is hex and a claim hash holds no '='.
        if (line.length() < 4 || line.charAt(1) != ':' || (line.charAt(0) != 'e' && line.charAt(0) != 'c')) {
            return false;
        }
        int eq = line.lastIndexOf('=');
        if (eq <= 2) {
            return false;
        }
        Slot<C> slot = get(EntryId.of(line.substring(2, eq)));
        return slot != null && slot.keyStartsWith(path, 0) && slot.auditedLines().contains(line);
    }

    /**
     * {@link #collectInto} that measures each slot's distinct audited lines as it goes and stops, answering false, once
     * the joined lines pass the bound. Lines name their entry id, so two slots never share a line, and a slot's own
     * distinct lines are exactly what it adds to the joined preimage.
     */
    private static boolean collectWithin(Node node, byte[] path, int depth, List<Slot<?>> out, long[] size,
                                         long maxBytes) {
        if (node instanceof Leaf leaf) {
            for (Slot<?> slot : leaf.slots) {
                if (path != null && !slot.keyStartsWith(path, depth)) {
                    continue;
                }
                out.add(slot);
                for (String line : new java.util.HashSet<>(slot.auditedLines())) {
                    size[0] += line.getBytes(StandardCharsets.UTF_8).length + 1L;
                }
                if (size[0] > maxBytes) {
                    return false;
                }
            }
            return true;
        }
        for (Node child : ((Inner) node).children) {
            if (child != null && !collectWithin(child, path, depth, out, size, maxBytes)) {
                return false;
            }
        }
        return true;
    }

    /** Refuses a path that is not at most 64 nibbles, each 0 to 15. */
    private static void checkPath(byte[] path) {
        Objects.requireNonNull(path, "path");
        if (path.length > KEY_NIBBLES) {
            throw new IllegalArgumentException("a path holds at most 64 nibbles: " + path.length);
        }
        for (byte b : path) {
            if (b < 0 || b > 0x0F) {
                throw new IllegalArgumentException("a path byte is a nibble from 0 to 15: " + b);
            }
        }
    }

    // ---- the text digest

    /**
     * Returns the text digest of today (TECH-SPEC §10.3; SPEC §5.3) over the slots, by collect and sort
     * (RE-M-06): every {@code e:} and {@code c:} line in the string order of its key ({@code e:<id>} or
     * {@code c:<id>}), each followed by LF, and then every acknowledged line as {@code a:<line>} in string order.
     * These are the bytes {@code ReplicatedSpace.computeDigest} writes when it is fed the same states, claims,
     * shard-dropped lines and acknowledgements; lapsed slots take part in full, as they do in the text digest.
     *
     * @return the digest, UTF-8
     */
    public byte[] legacyDigest() {
        TreeMap<String, String> summary = new TreeMap<>();
        TreeSet<String> acks = new TreeSet<>();
        for (Slot<C> slot : this) {
            slot.forEachLegacyLine(summary::put);
            acks.addAll(slot.acks());
        }
        StringBuilder sb = new StringBuilder();
        summary.forEach((k, v) -> sb.append(k).append('=').append(v).append('\n'));
        acks.forEach(line -> sb.append("a:").append(line).append('\n'));
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    // ---- checks for tests

    /**
     * Verifies the stored tree against annex I.6's canonical shape and the trie's own bookkeeping: each count is the
     * sum under it, a node is a leaf exactly when the shape rule says so, every slot lies under its node's path and in
     * key order, and no inner node holds an empty child. Package-private, for the property tests.
     *
     * @throws AssertionError at the first broken rule
     */
    void checkShape() {
        if (root != null) {
            check(root, new byte[0]);
        }
    }

    /** Whether the root's fingerprint has been computed; for the test that a batch hashes no intermediate trie. */
    boolean rootHashed() {
        return root != null && root.digest != null;
    }

    private static void check(Node node, byte[] path) {
        int depth = path.length;
        if (node instanceof Leaf leaf) {
            require(leaf.slots.length > 0, "an empty leaf at " + hex(path));
            require(leaf.size == leaf.slots.length, "a leaf's size at " + hex(path));
            require(leaf.size <= LEAF_MAX || depth == KEY_NIBBLES, "a leaf past the threshold at " + hex(path));
            int audited = 0;
            for (int i = 0; i < leaf.slots.length; i++) {
                Slot<?> slot = leaf.slots[i];
                require(!slot.isEmpty(), "an empty slot at " + hex(path));
                require(slot.keyStartsWith(path, 0), "a slot outside its path at " + hex(path));
                require(i == 0 || leaf.slots[i - 1].compareKey(slot) < 0, "slots out of key order at " + hex(path));
                if (slot.audited()) {
                    audited++;
                }
            }
            require(audited == leaf.audited, "a leaf's audited count at " + hex(path));
        } else {
            Inner inner = (Inner) node;
            require(depth < KEY_NIBBLES, "an inner node at 64 nibbles");
            require(inner.size > LEAF_MAX, "an inner node at or under the threshold at " + hex(path));
            int size = 0;
            int audited = 0;
            for (int c = 0; c < FANOUT; c++) {
                Node child = inner.children[c];
                if (child != null) {
                    byte[] sub = Arrays.copyOf(path, depth + 1);
                    sub[depth] = (byte) c;
                    check(child, sub);
                    size += child.size;
                    audited += child.audited;
                }
            }
            require(size == inner.size, "an inner node's size at " + hex(path));
            require(audited == inner.audited, "an inner node's audited count at " + hex(path));
        }
    }

    private static void require(boolean ok, String rule) {
        if (!ok) {
            throw new AssertionError("canonical shape broken: " + rule);
        }
    }

    private static String hex(byte[] path) {
        StringBuilder sb = new StringBuilder();
        for (byte b : path) {
            sb.append(Character.forDigit(b, 16));
        }
        return sb.isEmpty() ? "(root)" : sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof HashTrie<?> other) || other.size() != size()) {
            return false;
        }
        Iterator<?> a = iterator();
        Iterator<?> b = other.iterator();
        while (a.hasNext()) {
            if (!a.next().equals(b.next())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int h = 0;
        for (Slot<C> slot : this) {
            h = 31 * h + slot.hashCode();
        }
        return h;
    }

    @Override
    public String toString() {
        return "HashTrie[slots=" + size() + ", audited=" + auditedCount() + "]";
    }

    // ---- the nodes

    /** A stored node: its two counts, its lines' byte total and its memoized SHA-256. */
    private abstract static sealed class Node permits Leaf, Inner {
        final int size;
        final int audited;
        /**
         * The UTF-8 bytes of the audited lines under the node, each with one LF: the joined lines are one byte shorter.
         * Exact, since lines name their own slot's id and two slots therefore never share one, so a bounded line build
         * known to pass its bound is answered without a walk ({@link #linesUnder(byte[], long)}).
         */
        final long bytes;
        /** The SHA-256 of the audited node's preimage, filled on first use; null until then. */
        volatile byte[] digest;

        Node(int size, int audited, long bytes) {
            this.size = size;
            this.audited = audited;
            this.bytes = bytes;
        }
    }

    /** A stored leaf: at most the threshold of slots in key order, or any number at 64 nibbles. */
    private static final class Leaf extends Node {
        final Slot<?>[] slots;

        Leaf(Slot<?>[] slots) {
            this(slots, sumBytes(slots));
        }

        /** A leaf whose byte total the caller kept from the leaf it changed, so a change reads one or two slots. */
        Leaf(Slot<?>[] slots, long bytes) {
            super(slots.length, countAudited(slots), bytes);
            this.slots = slots;
        }

        private static long sumBytes(Slot<?>[] slots) {
            long bytes = 0;
            for (Slot<?> slot : slots) {
                bytes += lineBytes(slot);
            }
            return bytes;
        }

        private static int countAudited(Slot<?>[] slots) {
            int n = 0;
            for (Slot<?> slot : slots) {
                if (slot.audited()) {
                    n++;
                }
            }
            return n;
        }
    }

    /** A stored inner node: sixteen children, null where a child is empty. */
    private static final class Inner extends Node {
        final Node[] children;

        Inner(Node[] children, int size, int audited, long bytes) {
            super(size, audited, bytes);
            this.children = children;
        }

        static Inner of(Node[] children) {
            int size = 0;
            int audited = 0;
            long bytes = 0;
            for (Node child : children) {
                if (child != null) {
                    size += child.size;
                    audited += child.audited;
                    bytes += child.bytes;
                }
            }
            return new Inner(children, size, audited, bytes);
        }
    }

    // ---- lookup and change, by path copying

    @SuppressWarnings("unchecked")
    private static <C> Slot<C> find(Node node, Slot<?> probe, int depth) {
        while (node instanceof Inner inner) {
            node = inner.children[probe.nibble(depth)];
            depth++;
            if (node == null) {
                return null;
            }
        }
        Leaf leaf = (Leaf) node;
        int i = indexOf(leaf.slots, probe);
        return i >= 0 ? (Slot<C>) leaf.slots[i] : null;
    }

    private static Node insert(Node node, Slot<?> slot, int depth) {
        if (node instanceof Leaf leaf) {
            int i = indexOf(leaf.slots, slot);
            if (i >= 0) {
                if (leaf.slots[i].equals(slot)) {
                    return leaf;
                }
                Slot<?>[] replaced = leaf.slots.clone();
                replaced[i] = slot;
                return new Leaf(replaced, leaf.bytes - lineBytes(leaf.slots[i]) + lineBytes(slot));
            }
            int at = -i - 1;
            Slot<?>[] grown = new Slot<?>[leaf.slots.length + 1];
            System.arraycopy(leaf.slots, 0, grown, 0, at);
            grown[at] = slot;
            System.arraycopy(leaf.slots, at, grown, at + 1, leaf.slots.length - at);
            if (grown.length <= LEAF_MAX || depth == KEY_NIBBLES) {
                return new Leaf(grown, leaf.bytes + lineBytes(slot));
            }
            // Past the threshold the leaf splits on its next nibble, and a child that still holds too many splits again.
            return buildNode(grown, 0, grown.length, depth);
        }
        Inner inner = (Inner) node;
        int c = slot.nibble(depth);
        Node child = inner.children[c];
        Node next = child == null ? new Leaf(new Slot<?>[] {slot}) : insert(child, slot, depth + 1);
        if (next == child) {
            return inner;
        }
        Node[] children = inner.children.clone();
        children[c] = next;
        return new Inner(children, inner.size - sizeOf(child) + next.size,
                inner.audited - auditedOf(child) + next.audited, inner.bytes - bytesOf(child) + next.bytes);
    }

    /** Removes the probe's id under the node; returns the node itself when absent, and null when nothing is left. */
    private static Node delete(Node node, Slot<?> probe, int depth) {
        if (node instanceof Leaf leaf) {
            int i = indexOf(leaf.slots, probe);
            if (i < 0) {
                return leaf;
            }
            if (leaf.slots.length == 1) {
                return null;
            }
            Slot<?>[] shrunk = new Slot<?>[leaf.slots.length - 1];
            System.arraycopy(leaf.slots, 0, shrunk, 0, i);
            System.arraycopy(leaf.slots, i + 1, shrunk, i, shrunk.length - i);
            return new Leaf(shrunk, leaf.bytes - lineBytes(leaf.slots[i]));
        }
        Inner inner = (Inner) node;
        int c = probe.nibble(depth);
        Node child = inner.children[c];
        if (child == null) {
            return inner;
        }
        Node next = delete(child, probe, depth + 1);
        if (next == child) {
            return inner;
        }
        Node[] children = inner.children.clone();
        children[c] = next;
        Inner changed = new Inner(children, inner.size - child.size + sizeOf(next),
                inner.audited - child.audited + auditedOf(next), inner.bytes - child.bytes + bytesOf(next));
        if (changed.size > LEAF_MAX) {
            return changed;
        }
        // At or under the threshold the subtree collapses into one leaf; every child is then a leaf already.
        List<Slot<?>> all = collect(changed, null, 0);
        return new Leaf(all.toArray(new Slot<?>[0]), changed.bytes);
    }

    /** Builds the canonical subtree of sorted slots[from, to) at a depth. */
    private static Node buildNode(Slot<?>[] sorted, int from, int to, int depth) {
        int n = to - from;
        if (n == 0) {
            return null;
        }
        if (n <= LEAF_MAX || depth == KEY_NIBBLES) {
            return new Leaf(from == 0 && to == sorted.length ? sorted : Arrays.copyOfRange(sorted, from, to));
        }
        Node[] children = new Node[FANOUT];
        int start = from;
        while (start < to) {
            int c = sorted[start].nibble(depth);
            int end = start + 1;
            while (end < to && sorted[end].nibble(depth) == c) {
                end++;
            }
            children[c] = buildNode(sorted, start, end, depth + 1);
            start = end;
        }
        return Inner.of(children);
    }

    private static int indexOf(Slot<?>[] slots, Slot<?> probe) {
        int lo = 0;
        int hi = slots.length - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int c = slots[mid].compareKey(probe);
            if (c < 0) {
                lo = mid + 1;
            } else if (c > 0) {
                hi = mid - 1;
            } else {
                return mid;
            }
        }
        return -(lo + 1);
    }

    private static int sizeOf(Node node) {
        return node == null ? 0 : node.size;
    }

    private static int auditedOf(Node node) {
        return node == null ? 0 : node.audited;
    }

    private static long bytesOf(Node node) {
        return node == null ? 0 : node.bytes;
    }

    /**
     * A slot's share of a node's byte total: each of its audited lines' UTF-8 bytes and one LF. Its audited lines are
     * distinct, and they name its own id, so no other slot holds one of them: what {@code collectWithin} measures.
     */
    private static long lineBytes(Slot<?> slot) {
        long bytes = 0;
        for (String line : slot.auditedLines()) {
            bytes += line.getBytes(StandardCharsets.UTF_8).length + 1L; // measured as the walk measures it
        }
        return bytes;
    }

    /** The slots under a node in key order, kept to those under the path from the depth on when a path is given. */
    private static List<Slot<?>> collect(Node node, byte[] path, int depth) {
        List<Slot<?>> out = new ArrayList<>(Math.min(node.size, 64));
        collectInto(node, path, depth, out);
        GATHERED.add(out.size());
        return out;
    }

    private static void collectInto(Node node, byte[] path, int depth, List<Slot<?>> out) {
        if (node instanceof Leaf leaf) {
            for (Slot<?> slot : leaf.slots) {
                if (path == null || slot.keyStartsWith(path, depth)) {
                    out.add(slot);
                }
            }
            return;
        }
        for (Node child : ((Inner) node).children) {
            if (child != null) {
                collectInto(child, path, depth, out);
            }
        }
    }

    // ---- fingerprints

    /** Whether the audited node at this stored node is a leaf: by its audited count, or at the deepest path. */
    private static boolean auditedLeaf(Node node, int depth) {
        return node.audited <= LEAF_MAX || depth == KEY_NIBBLES;
    }

    /** The memoized SHA-256 of the audited preimage of a stored node holding at least one audited slot. */
    private static byte[] digest(Node node, int depth) {
        byte[] d = node.digest;
        if (d == null) {
            byte[] preimage = auditedLeaf(node, depth)
                    ? leafPreimage(collect(node, null, 0))
                    : innerPreimage((Inner) node, depth);
            d = Slot.sha256().digest(preimage);
            FINGERPRINTS.increment();
            node.digest = d;
        }
        return d;
    }

    /**
     * A leaf's preimage (annex I.6): every audited line of the slots, as UTF-8, distinct, sorted by unsigned bytes
     * and joined by LF with none after the last. Byte order, not Java's string order, which differs from it for
     * characters above U+FFFF against U+E000 to U+FFFF.
     */
    private static byte[] leafPreimage(List<Slot<?>> slots) {
        List<byte[]> lines = new ArrayList<>(slots.size() * 2);
        for (Slot<?> slot : slots) {
            for (String line : slot.auditedLines()) {
                lines.add(line.getBytes(StandardCharsets.UTF_8));
            }
        }
        lines.sort(Arrays::compareUnsigned);
        int total = 0;
        int kept = 0;
        for (int i = 0; i < lines.size(); i++) {
            // An acknowledged line equal to a held line is one line: the lines form a set.
            if (i > 0 && Arrays.equals(lines.get(i), lines.get(i - 1))) {
                lines.set(i, null);
                continue;
            }
            total += lines.get(i).length;
            kept++;
        }
        byte[] out = new byte[total + Math.max(0, kept - 1)];
        int at = 0;
        for (byte[] line : lines) {
            if (line == null) {
                continue;
            }
            if (at > 0) {
                out[at++] = '\n';
            }
            System.arraycopy(line, 0, out, at, line.length);
            at += line.length;
        }
        return out;
    }

    /** An inner node's 384-byte preimage: per child in nibble order, its fingerprint (zeros when empty) and its count. */
    private static byte[] innerPreimage(Inner inner, int depth) {
        byte[] out = new byte[INNER_PREIMAGE_BYTES];
        for (int c = 0; c < FANOUT; c++) {
            Node child = inner.children[c];
            int at = c * (FINGERPRINT_BYTES + 8);
            if (child != null && child.audited > 0) {
                System.arraycopy(digest(child, depth + 1), 0, out, at, FINGERPRINT_BYTES);
                long count = child.audited;
                for (int i = 7; i >= 0; i--) {
                    out[at + FINGERPRINT_BYTES + i] = (byte) count;
                    count >>>= 8;
                }
            }
        }
        return out;
    }

    // ---- iteration

    /** Walks the stored tree depth first, children in nibble order and each leaf's slots in key order. */
    private static final class SlotIterator<C> implements Iterator<Slot<C>> {
        private final ArrayDeque<Node> stack = new ArrayDeque<>();
        private Slot<?>[] leaf;
        private int next;

        SlotIterator(Node root) {
            if (root != null) {
                stack.push(root);
            }
            advance();
        }

        private void advance() {
            while ((leaf == null || next >= leaf.length) && !stack.isEmpty()) {
                Node node = stack.pop();
                if (node instanceof Leaf l) {
                    leaf = l.slots;
                    next = 0;
                } else {
                    Node[] children = ((Inner) node).children;
                    for (int c = FANOUT - 1; c >= 0; c--) {
                        if (children[c] != null) {
                            stack.push(children[c]);
                        }
                    }
                }
            }
        }

        @Override
        public boolean hasNext() {
            return leaf != null && next < leaf.length;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Slot<C> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Slot<C> slot = (Slot<C>) leaf[next++];
            if (next >= leaf.length) {
                advance();
            }
            return slot;
        }
    }

    // ---- the batch

    /**
     * Collects a batch of changes and makes one trie of them (a join, or a merged {@code PULL_RESP}). Reads through
     * {@link #get} see the batch's own changes. Nothing is hashed while the batch is made, as nothing is anywhere
     * until a fingerprint is asked for. A batch that touches a large part of its trie is built afresh from the sorted
     * slots, once per node; a smaller one is applied change by change, each copying one path.
     *
     * <p>Not thread-safe; it is meant for one writer under the space's lock.
     *
     * @param <C> the claim type
     */
    public static final class Builder<C> {
        private final HashTrie<C> base;
        /** The batch's changes by id; a null value removes the id. */
        private final Map<EntryId, Slot<C>> changes = new HashMap<>();

        private Builder(HashTrie<C> base) {
            this.base = base;
        }

        /**
         * Puts a slot, replacing the id's slot; an empty slot removes the id.
         *
         * @param slot the slot
         * @return this builder
         */
        public Builder<C> put(Slot<C> slot) {
            Objects.requireNonNull(slot, "slot");
            changes.put(slot.id(), slot.isEmpty() ? null : slot);
            return this;
        }

        /**
         * Removes an id.
         *
         * @param id the entry id
         * @return this builder
         */
        public Builder<C> remove(EntryId id) {
            changes.put(Objects.requireNonNull(id, "id"), null);
            return this;
        }

        /**
         * Changes an id's slot by an operator handed the current slot or an empty one.
         *
         * @param id the entry id
         * @param op the change
         * @return this builder
         */
        public Builder<C> update(EntryId id, UnaryOperator<Slot<C>> op) {
            Slot<C> current = get(id);
            Slot<C> given = current != null ? current : Slot.empty(id);
            Slot<C> next = Objects.requireNonNull(op.apply(given), "slot");
            if (next != given) {
                if (!next.id().equals(id)) {
                    throw new IllegalArgumentException("the operator changed the slot's id: " + id + " to " + next.id());
                }
                put(next);
            }
            return this;
        }

        /**
         * Returns the id's slot as the batch leaves it.
         *
         * @param id the entry id
         * @return the slot, or null
         */
        public Slot<C> get(EntryId id) {
            if (changes.containsKey(id)) {
                return changes.get(id);
            }
            return base.get(id);
        }

        /** Returns the trie with the batch applied. */
        public HashTrie<C> build() {
            if (changes.isEmpty()) {
                return base;
            }
            // Path copying costs about five node copies per change; building afresh costs a sort and one node per
            // sixteen slots or so. Past about a quarter of the trie the fresh build is the cheaper.
            if (changes.size() * 4L < base.size()) {
                HashTrie<C> trie = base;
                for (Map.Entry<EntryId, Slot<C>> change : changes.entrySet()) {
                    trie = change.getValue() == null ? trie.remove(change.getKey()) : trie.put(change.getValue());
                }
                return trie;
            }
            List<Slot<?>> all = new ArrayList<>(base.size() + changes.size());
            for (Slot<C> slot : base) {
                if (!changes.containsKey(slot.id())) {
                    all.add(slot);
                }
            }
            for (Slot<C> slot : changes.values()) {
                if (slot != null) {
                    all.add(slot);
                }
            }
            Slot<?>[] sorted = all.toArray(new Slot<?>[0]);
            Arrays.sort(sorted, Slot::compareKey);
            Node root = buildNode(sorted, 0, sorted.length, 0);
            return root == null ? empty() : new HashTrie<>(root);
        }
    }
}
