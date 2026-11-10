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
package ai.badmonkey.agentspaces.pdt.kinds;

import ai.badmonkey.agentspaces.pdt.Kind;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeFormatException;
import ai.badmonkey.agentspaces.pdt.TextRun;
import ai.badmonkey.agentspaces.pdt.TextRun.CharId;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * A text leaf, kind {@code t} (PDT 2 phase 0, P2-T-01; srad/agentspaces-pdt2.md §3.9): a Fugue
 * sequence stored as runs, {@code { k:"t", r: [run...], v: 1 }}. A writer puts a value that holds
 * text under a {@code PRef.v} 2 head (§6.10.3, rule 3).
 *
 * <p>One canonical form, so two replicas that hold the same characters write the same bytes: the
 * runs are maximal (a character continues the run before it exactly when it comes from the same
 * replica with the next counter, hangs as the right child of that character, and shares its
 * deleted flag), and they are sorted by the id of their first character, the replica's UTF-8
 * bytes unsigned first and then the counter. Every parent names a character of the leaf, and the
 * parents form a tree under the document root.
 *
 * <p>The text reads in Fugue order: a character's left children, then the character, then its
 * right children, same-side siblings in id order. The union of two leaves is the union of their
 * characters, deleted when either side deleted it, which is Fugue's merge (P2-T-03 builds the
 * value type and the engine's merge on it).
 *
 * @param runs the runs in canonical form
 */
public record TextLeaf(List<TextRun> runs) {

    /** The version this release writes and reads. */
    public static final long VERSION = 1;

    /**
     * The most characters, live and deleted, one leaf holds: the node byte bound (PDT 2 phase 0,
     * P2-T-01, gate finding G1). A live character costs at least one byte of text, so the node bound
     * already caps them; a deleted run costs none, so this bound caps the tombstones too, and every
     * reader checks it before it expands a run.
     */
    public static final long MAX_CHARS = NodeCodec.MAX_NODE_BYTES;

    /** The order of character ids: the replica's UTF-8 bytes unsigned, then the counter. */
    public static final Comparator<CharId> ID_ORDER = Comparator
            .comparing((CharId id) -> id.replica().getBytes(StandardCharsets.UTF_8), Arrays::compareUnsigned)
            .thenComparingLong(CharId::counter);

    private static final List<String> REQUIRED = List.of("r", "v");

    /** Checks that the runs are the canonical form of their characters and that the parents form a tree. */
    public TextLeaf {
        runs = List.copyOf(runs);
        Map<CharId, Char> chars = expand(runs);
        if (!compress(chars).equals(runs)) {
            throw new IllegalArgumentException("the runs are not in canonical form: maximal, sorted by id");
        }
        order(chars);
    }

    /** The canonical leaf of some runs, in any order and split anywhere. */
    public static TextLeaf of(Collection<TextRun> runs) {
        return new TextLeaf(compress(expand(runs)));
    }

    /**
     * The union of two leaves: every character of either, deleted when either deleted it.
     *
     * @param other the other replica's leaf
     * @return the merged leaf
     * @throws IllegalArgumentException when the two leaves give one character id two parents, sides, or characters
     */
    public TextLeaf union(TextLeaf other) {
        Map<CharId, Char> merged = expand(runs);
        for (Map.Entry<CharId, Char> entry : expand(other.runs).entrySet()) {
            merged.merge(entry.getKey(), entry.getValue(), (mine, theirs) -> mine.join(entry.getKey(), theirs));
        }
        return new TextLeaf(compress(merged));
    }

    /** The live text in Fugue order. */
    public String text() {
        Map<CharId, Char> chars = expand(runs);
        StringBuilder out = new StringBuilder();
        for (CharId id : order(chars)) {
            Char c = chars.get(id);
            if (!c.deleted) {
                out.appendCodePoint(c.codePoint);
            }
        }
        return out.toString();
    }

    /**
     * The text as pieces in Fugue order (PDT 2 phase 6, P2-T-02): each piece holds characters of
     * one run that sit next to each other in the text, deleted ones included, with the parent and
     * side of its first character, so an editor walks the text piece by piece. A run splits into
     * pieces where other characters sit between its own.
     */
    public List<TextRun> pieces() {
        Map<CharId, Char> chars = expand(runs);
        List<CharId> order = order(chars);
        List<TextRun> out = new ArrayList<>();
        int i = 0;
        while (i < order.size()) {
            CharId start = order.get(i);
            Char first = chars.get(start);
            StringBuilder text = new StringBuilder();
            int j = i;
            CharId last;
            do {
                last = order.get(j);
                Char c = chars.get(last);
                if (!c.deleted) {
                    text.appendCodePoint(c.codePoint);
                }
                j++;
            } while (j < order.size() && continues(last, order.get(j), chars.get(order.get(j)), first.deleted));
            out.add(new TextRun(start.replica(), start.counter(), first.parent, first.left ? TextRun.LEFT : TextRun.RIGHT,
                    j - i, first.deleted, text.toString()));
            i = j;
        }
        return out;
    }

    /** Whether a character continues the run of the one before it: the next counter, its right child, the same flag. */
    private static boolean continues(CharId last, CharId id, Char c, boolean deleted) {
        return id.replica().equals(last.replica()) && id.counter() == last.counter() + 1 && last.equals(c.parent)
                && !c.left && c.deleted == deleted;
    }

    /** The canonical node bytes. */
    public byte[] encode() {
        Map<String, Object> node = new LinkedHashMap<>();
        node.put(NodeCodec.KIND_KEY, Kind.T);
        node.put("r", runs);
        node.put("v", VERSION);
        return NodeCodec.encode(node);
    }

    /**
     * Reads a {@code t} node.
     *
     * @param cid   the CID, named in a refusal; may be null
     * @param bytes the node bytes
     * @return the leaf
     * @throws NodeFormatException on another kind, another version, a missing key, or runs out of canonical form
     */
    public static TextLeaf decode(String cid, byte[] bytes) {
        Map<String, Object> node = Nodes.decode(cid, bytes, Kind.T, REQUIRED);
        if (!Long.valueOf(VERSION).equals(node.get("v"))) {
            throw new NodeFormatException(cid, "text leaf version " + node.get("v") + "; this release reads version " + VERSION);
        }
        return Nodes.build(cid, () -> new TextLeaf(Nodes.list(node, "r")));
    }

    // ------------------------------------------------------------------ characters

    /** One character: its parent and side, whether it is deleted, and its code point when live. */
    private record Char(CharId parent, boolean left, boolean deleted, int codePoint) {

        Char join(CharId id, Char other) {
            if (!Objects.equals(parent, other.parent) || left != other.left
                    || (!deleted && !other.deleted && codePoint != other.codePoint)) {
                throw new IllegalArgumentException("two leaves disagree on character " + name(id));
            }
            boolean gone = deleted || other.deleted;
            return new Char(parent, left, gone, gone ? -1 : codePoint);
        }
    }

    private static String name(CharId id) {
        return id.replica() + ":" + id.counter();
    }

    private static Map<CharId, Char> expand(Collection<TextRun> runs) {
        long total = 0;
        for (TextRun run : runs) {
            if (run.length() > MAX_CHARS - total) {
                throw new IllegalArgumentException("a text leaf holds at most " + MAX_CHARS + " characters, live and deleted");
            }
            total += run.length();
        }
        Map<CharId, Char> chars = new TreeMap<>(ID_ORDER);
        for (TextRun run : runs) {
            int[] codePoints = run.text().codePoints().toArray();
            for (long i = 0; i < run.length(); i++) {
                CharId id = new CharId(run.replica(), run.counter() + i);
                CharId parent = i == 0 ? run.parent() : new CharId(run.replica(), run.counter() + i - 1);
                boolean left = i == 0 && TextRun.LEFT.equals(run.side());
                Char c = new Char(parent, left, run.deleted(), run.deleted() ? -1 : codePoints[(int) i]);
                if (chars.put(id, c) != null) {
                    throw new IllegalArgumentException("character " + name(id) + " appears twice");
                }
            }
        }
        return chars;
    }

    private static List<TextRun> compress(Map<CharId, Char> chars) {
        List<TextRun> out = new ArrayList<>();
        CharId start = null;
        Char first = null;
        CharId last = null;
        long length = 0;
        StringBuilder text = new StringBuilder();
        for (Map.Entry<CharId, Char> entry : chars.entrySet()) {
            CharId id = entry.getKey();
            Char c = entry.getValue();
            if (last == null || !continues(last, id, c, first.deleted)) {
                if (start != null) {
                    out.add(run(start, first, length, text));
                }
                start = id;
                first = c;
                length = 0;
                text.setLength(0);
            }
            length++;
            if (!c.deleted) {
                text.appendCodePoint(c.codePoint);
            }
            last = id;
        }
        if (start != null) {
            out.add(run(start, first, length, text));
        }
        return out;
    }

    private static TextRun run(CharId start, Char first, long length, StringBuilder text) {
        return new TextRun(start.replica(), start.counter(), first.parent, first.left ? TextRun.LEFT : TextRun.RIGHT,
                length, first.deleted, text.toString());
    }

    /**
     * The characters in Fugue order, walked with an explicit stack so a long run (a chain of right
     * children) costs no call depth.
     *
     * @throws IllegalArgumentException when a parent is not in the leaf or the parents form a cycle
     */
    private static List<CharId> order(Map<CharId, Char> chars) {
        Map<CharId, List<CharId>> lefts = new HashMap<>();
        Map<CharId, List<CharId>> rights = new HashMap<>();
        List<CharId> roots = new ArrayList<>();
        for (Map.Entry<CharId, Char> entry : chars.entrySet()) {
            Char c = entry.getValue();
            if (c.parent == null) {
                roots.add(entry.getKey());
                continue;
            }
            if (!chars.containsKey(c.parent)) {
                throw new IllegalArgumentException("the parent " + name(c.parent) + " of " + name(entry.getKey())
                        + " is not in the leaf");
            }
            // the entries arrive in id order, so each sibling list is already sorted
            (c.left ? lefts : rights).computeIfAbsent(c.parent, k -> new ArrayList<>()).add(entry.getKey());
        }
        List<CharId> out = new ArrayList<>(chars.size());
        // each frame is a character to expand (visit) or to emit
        Deque<Object[]> stack = new ArrayDeque<>();
        for (int i = roots.size() - 1; i >= 0; i--) {
            stack.push(new Object[]{roots.get(i), Boolean.FALSE});
        }
        while (!stack.isEmpty()) {
            Object[] frame = stack.pop();
            CharId id = (CharId) frame[0];
            if ((Boolean) frame[1]) {
                out.add(id);
                continue;
            }
            List<CharId> right = rights.getOrDefault(id, List.of());
            for (int i = right.size() - 1; i >= 0; i--) {
                stack.push(new Object[]{right.get(i), Boolean.FALSE});
            }
            stack.push(new Object[]{id, Boolean.TRUE});
            List<CharId> left = lefts.getOrDefault(id, List.of());
            for (int i = left.size() - 1; i >= 0; i--) {
                stack.push(new Object[]{left.get(i), Boolean.FALSE});
            }
        }
        if (out.size() != chars.size()) {
            throw new IllegalArgumentException("the runs' parents form a cycle that the document root does not reach");
        }
        return out;
    }
}
