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
import ai.badmonkey.agentspaces.common.codec.Multibase;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.pdt.Kind;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeFormatException;
import ai.badmonkey.agentspaces.pdt.TextRun;
import ai.badmonkey.agentspaces.pdt.TextRun.CharId;
import ai.badmonkey.agentspaces.pdt.kinds.TextLeaf;

import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * A text that merges (PDT 2 phase 6, P2-T-02 and P2-T-03; srad/agentspaces-pdt2.md §3.9): a Fugue
 * sequence of characters with ids, edited by one replica at a time, that stores as a {@code t}
 * leaf of runs ({@link TextLeaf}). Positions and lengths count Unicode code points, as the runs do.
 *
 * <p>An insert follows Fugue: the new characters hang as the right child of the character before
 * the position when it has no right children, else as the left child of the next character in
 * Fugue order (deleted ones included), and each further character as the right child of the one
 * before it. A replica numbers its characters from one more than the largest counter it holds,
 * so typing at the cursor extends one run. A delete keeps the characters' ids and drops their
 * text. {@link #merge} is the union of two texts, which every replica computes alike, so two
 * replicas converge whatever they did concurrently, and Fugue keeps each insert's characters
 * together.
 *
 * <p>The value is immutable: every edit returns a new text. A replica is any non-empty text that
 * no other writer of the document uses; {@link #replicaOf} derives eight base32 characters from
 * an agent's id, so the runs stay small.
 *
 * <p>As a value, a text is the wire encoding of its leaf's map, {@code {k:"t", r:[run...], v:1}}
 * with the map indefinite as every embedded item is (rule S8), and {@link #codec} writes and reads
 * that form strictly. A map value of text above 1 KiB spills to a {@code t} node that holds the
 * canonical node bytes of the same leaf (P2-T-02), two texts that meet in a merge take their
 * union (P2-T-03), and a head whose value holds a text is {@code PRef.v} 2 (§6.10.3, rule 3).
 */
public final class PText {

    /** The characters this release lets one text hold, live and deleted: the leaf bound. */
    public static final long MAX_CHARS = TextLeaf.MAX_CHARS;

    /** The length of a replica id {@link #replicaOf} derives. */
    public static final int REPLICA_LENGTH = 8;

    /**
     * One piece of the text in Fugue order: characters of one run that sit next to each other.
     *
     * @param lastHasRight whether the piece's last character has a right child anywhere in the text
     */
    private record Piece(String replica, long counter, long length, boolean deleted, String text, CharId parent,
                         boolean left, boolean lastHasRight) {

        long visible() {
            return deleted ? 0 : length;
        }

        CharId first() {
            return new CharId(replica, counter);
        }

        CharId last() {
            return new CharId(replica, counter + length - 1);
        }

        /** The first {@code k} characters; the last of them has the next one as its right child. */
        Piece head(long k) {
            return new Piece(replica, counter, k, deleted, deleted ? "" : text.substring(0, offset(k)), parent, left, true);
        }

        /** The characters from {@code k} on; the first of them is the right child of the one before. */
        Piece tail(long k) {
            return new Piece(replica, counter + k, length - k, deleted, deleted ? "" : text.substring(offset(k)),
                    new CharId(replica, counter + k - 1), false, lastHasRight);
        }

        Piece asDeleted() {
            return new Piece(replica, counter, length, true, "", parent, left, lastHasRight);
        }

        Piece withRight() {
            return lastHasRight ? this : new Piece(replica, counter, length, deleted, text, parent, left, true);
        }

        private int offset(long k) {
            return text.offsetByCodePoints(0, (int) k);
        }

        TextRun run() {
            return new TextRun(replica, counter, parent, left ? TextRun.LEFT : TextRun.RIGHT, length, deleted, text);
        }
    }

    private final String replica;
    private final List<Piece> pieces;
    private final long length;
    private final long chars;
    private final long next;
    private volatile TextLeaf leaf;

    private PText(String replica, List<Piece> pieces, TextLeaf leaf) {
        this.replica = checkReplica(replica);
        this.pieces = pieces;
        long live = 0;
        long all = 0;
        long top = -1;
        for (Piece p : pieces) {
            live += p.visible();
            all += p.length;
            if (p.replica.equals(replica)) {
                top = Math.max(top, p.counter + p.length - 1);
            }
        }
        this.length = live;
        this.chars = all;
        this.next = top + 1;
        this.leaf = leaf;
    }

    private static String checkReplica(String replica) {
        Objects.requireNonNull(replica, "replica");
        if (replica.isEmpty()) {
            throw new IllegalArgumentException("a replica is non-empty text");
        }
        return replica;
    }

    /** The empty text, edited by a replica. */
    public static PText empty(String replica) {
        return new PText(replica, List.of(), null);
    }

    /** The text of a leaf, edited by a replica. */
    public static PText of(TextLeaf leaf, String replica) {
        Objects.requireNonNull(leaf, "leaf");
        Set<CharId> rightParents = new HashSet<>();
        TreeMap<CharId, TextRun> runs = new TreeMap<>(TextLeaf.ID_ORDER);
        for (TextRun run : leaf.runs()) {
            runs.put(run.first(), run);
            if (run.parent() != null && TextRun.RIGHT.equals(run.side())) {
                rightParents.add(run.parent());
            }
        }
        List<Piece> pieces = new ArrayList<>();
        for (TextRun p : leaf.pieces()) {
            CharId last = new CharId(p.replica(), p.counter() + p.length() - 1);
            TextRun run = runs.floorEntry(last).getValue();
            boolean inside = last.counter() < run.counter() + run.length() - 1;
            pieces.add(new Piece(p.replica(), p.counter(), p.length(), p.deleted(), p.text(), p.parent(),
                    TextRun.LEFT.equals(p.side()), inside || rightParents.contains(last)));
        }
        return new PText(replica, Collections.unmodifiableList(pieces), leaf);
    }

    /**
     * The replica id of an agent (P2-T-02): the first five bytes of the SHA-256 of its encoded id,
     * as eight lower-case base32 characters.
     */
    public static String replicaOf(AgentId agent) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(agent.encoded().getBytes(StandardCharsets.UTF_8));
            return Multibase.base32(Arrays.copyOf(digest, 5)).substring(1);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    /**
     * The codec of texts edited by a replica: a text encodes as the wire encoding of its leaf's
     * map and decodes from exactly that form.
     */
    public static Codec<PText> codec(String replica) {
        return new TextCodec(checkReplica(replica));
    }

    /**
     * The codec {@link #codec} returns; a ref whose values pass through it writes {@code PRef.v} 2
     * heads (§6.10.3, rule 3).
     */
    public static final class TextCodec implements Codec<PText> {

        private final String replica;

        private TextCodec(String replica) {
            this.replica = replica;
        }

        @Override
        public byte[] encode(PText value) {
            return toWire(value.leaf());
        }

        @Override
        public PText decode(byte[] bytes) {
            return of(fromWire(bytes), replica);
        }
    }

    // ------------------------------------------------------------------ reads

    /** The replica that edits this text. */
    public String replica() {
        return replica;
    }

    /** The live characters, in code points. */
    public long length() {
        return length;
    }

    /** The live text. */
    @Override
    public String toString() {
        StringBuilder out = new StringBuilder();
        for (Piece p : pieces) {
            out.append(p.text);
        }
        return out.toString();
    }

    /** Every character id in Fugue order, deleted ones included. */
    public List<CharId> order() {
        List<CharId> out = new ArrayList<>((int) chars);
        for (Piece p : pieces) {
            for (long i = 0; i < p.length; i++) {
                out.add(new CharId(p.replica, p.counter + i));
            }
        }
        return out;
    }

    /** The canonical leaf of the characters, maximal runs in id order. */
    public TextLeaf leaf() {
        TextLeaf known = leaf;
        if (known == null) {
            List<TextRun> runs = new ArrayList<>(pieces.size());
            for (Piece p : pieces) {
                runs.add(p.run());
            }
            known = TextLeaf.of(runs);
            leaf = known;
        }
        return known;
    }

    // ------------------------------------------------------------------ writes

    /** This text edited by another replica. */
    public PText withReplica(String other) {
        return new PText(other, pieces, leaf);
    }

    /**
     * The text with characters inserted before a position.
     *
     * @param pos  0 to {@link #length()}
     * @param text the characters, without a lone surrogate
     * @return the text, or this one when {@code text} is empty
     * @throws IllegalArgumentException when the text would hold more than {@link #MAX_CHARS} characters
     */
    public PText insert(long pos, String text) {
        if (pos < 0 || pos > length) {
            throw new IndexOutOfBoundsException("position " + pos + " of a text of " + length);
        }
        long k = text.codePointCount(0, text.length());
        if (k == 0) {
            return this;
        }
        if (k > MAX_CHARS - chars) {
            throw new IllegalArgumentException("a text holds at most " + MAX_CHARS + " characters, live and deleted");
        }
        List<Piece> out = new ArrayList<>(pieces.size() + 3);
        if (pos == 0) {
            // the document root: a right child while the text is empty, else a left child of the first character
            out.add(fresh(k, text, pieces.isEmpty() ? null : pieces.get(0).first(), !pieces.isEmpty()));
            out.addAll(pieces);
            return edited(out);
        }
        int i = visibleIndex(pos - 1);
        long offset = pos - 1 - visibleBefore(i);
        Piece p = pieces.get(i);
        out.addAll(pieces.subList(0, i));
        if (offset < p.length - 1) {
            // the character before the position has a right child, the next one of its piece
            Piece tail = p.tail(offset + 1);
            out.add(p.head(offset + 1));
            out.add(fresh(k, text, tail.first(), true));
            out.add(tail);
        } else if (!p.lastHasRight && p.replica.equals(replica) && p.counter + p.length == next) {
            // this replica's last character, with no right child: the run goes on
            out.add(new Piece(p.replica, p.counter, p.length + k, false, p.text + text, p.parent, p.left, false));
        } else if (!p.lastHasRight) {
            out.add(p.withRight());
            out.add(fresh(k, text, p.last(), false));
        } else {
            out.add(p);
            out.add(fresh(k, text, pieces.get(i + 1).first(), true));
        }
        out.addAll(pieces.subList(i + 1, pieces.size()));
        return edited(out);
    }

    /**
     * The text with characters deleted.
     *
     * @param pos the first deleted position
     * @param len how many live characters to delete
     * @return the text, or this one when {@code len} is 0
     */
    public PText delete(long pos, long len) {
        if (pos < 0 || len < 0 || pos + len > length) {
            throw new IndexOutOfBoundsException("range [" + pos + ", " + (pos + len) + ") of a text of " + length);
        }
        if (len == 0) {
            return this;
        }
        List<Piece> out = new ArrayList<>(pieces.size() + 2);
        long at = 0;
        long end = pos + len;
        for (Piece p : pieces) {
            long from = Math.max(pos, at) - at;
            long to = Math.min(end, at + p.visible()) - at;
            at += p.visible();
            if (p.deleted || from >= to) {
                out.add(p);
                continue;
            }
            Piece rest = p;
            if (from > 0) {
                out.add(rest.head(from));
                rest = rest.tail(from);
            }
            if (to - from < rest.length) {
                out.add(rest.head(to - from).asDeleted());
                out.add(rest.tail(to - from));
            } else {
                out.add(rest.asDeleted());
            }
        }
        return edited(out);
    }

    /**
     * The union of two texts (P2-T-03): every character of either, deleted when either deleted it,
     * in Fugue order, edited by this text's replica.
     *
     * @throws IllegalArgumentException when the two texts give one character id two parents, sides, or characters
     */
    public PText merge(PText other) {
        return of(leaf().union(other.leaf()), replica);
    }

    private Piece fresh(long k, String text, CharId parent, boolean left) {
        return new Piece(replica, next, k, false, text, parent, left, false);
    }

    private PText edited(List<Piece> out) {
        return new PText(replica, Collections.unmodifiableList(out), null);
    }

    /** The piece that holds a live position. */
    private int visibleIndex(long position) {
        long at = 0;
        for (int i = 0; i < pieces.size(); i++) {
            long v = pieces.get(i).visible();
            if (position < at + v) {
                return i;
            }
            at += v;
        }
        throw new IndexOutOfBoundsException("position " + position + " of a text of " + length);
    }

    private long visibleBefore(int index) {
        long at = 0;
        for (int i = 0; i < index; i++) {
            at += pieces.get(i).visible();
        }
        return at;
    }

    /** Equality by the leaf: the same characters, ids, and deletions, whatever the replica. */
    @Override
    public boolean equals(Object o) {
        return o instanceof PText other && leaf().equals(other.leaf());
    }

    @Override
    public int hashCode() {
        return leaf().hashCode();
    }

    /** The first bytes of every text value: an indefinite map whose first key {@code k} holds {@code "t"}. */
    private static final byte[] TEXT_PREFIX = {(byte) 0xbf, 0x61, 0x6b, 0x61, 0x74};

    private static final List<String> VALUE_KEYS = List.of(NodeCodec.KIND_KEY, "r", "v");

    /** The value form of a leaf: the wire encoding of {@code {k:"t", r:[run...], v:1}}. */
    static byte[] toWire(TextLeaf leaf) {
        List<Object> runs = new ArrayList<>(leaf.runs().size());
        for (TextRun run : leaf.runs()) {
            List<Object> parent = run.parent() == null ? null : List.of(run.parent().replica(), run.parent().counter());
            runs.add(Arrays.asList(run.replica(), run.counter(), parent, run.side(), run.length(), run.deleted(), run.text()));
        }
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(NodeCodec.KIND_KEY, Kind.T);
        map.put("r", runs);
        map.put("v", TextLeaf.VERSION);
        return CborCodec.defaultCodec().toBytes(map);
    }

    /**
     * The leaf of a text value, read strictly: exactly the three keys, runs in canonical form, and
     * bytes that are the value form of the leaf they read as.
     *
     * @throws IllegalArgumentException when the bytes are not the value form of a leaf
     */
    static TextLeaf fromWire(byte[] cbor) {
        Map<?, ?> map;
        try {
            map = CborCodec.defaultCodec().fromDocument(cbor, Map.class);
        } catch (UncheckedIOException notCbor) {
            throw new IllegalArgumentException("a text value is one CBOR map in the wire encoding", notCbor);
        }
        if (map == null || !List.copyOf(map.keySet()).equals(VALUE_KEYS) || !Kind.T.equals(map.get(NodeCodec.KIND_KEY))
                || !(map.get("r") instanceof List<?> items) || !Long.valueOf(TextLeaf.VERSION).equals(number(map.get("v")))) {
            throw new IllegalArgumentException("a text value is {k:\"t\", r:[run...], v:1}");
        }
        List<TextRun> runs = new ArrayList<>(items.size());
        for (Object item : items) {
            if (!(item instanceof List<?> run) || run.size() != 7 || !(run.get(0) instanceof String replica)
                    || !(run.get(3) instanceof String side) || !(run.get(5) instanceof Boolean deleted)
                    || !(run.get(6) instanceof String text)) {
                throw new IllegalArgumentException("a text run is [replica, counter, parent, side, length, deleted, text]");
            }
            CharId parent = null;
            if (run.get(2) instanceof List<?> id && id.size() == 2 && id.get(0) instanceof String parentReplica) {
                parent = new CharId(parentReplica, number(id.get(1)));
            } else if (run.get(2) != null) {
                throw new IllegalArgumentException("a run's parent is [replica, counter] or null");
            }
            runs.add(new TextRun(replica, number(run.get(1)), parent, side, number(run.get(4)), deleted, text));
        }
        TextLeaf leaf = new TextLeaf(runs);
        if (!Arrays.equals(toWire(leaf), cbor)) {
            throw new IllegalArgumentException("the bytes are not the value form of the text they read as");
        }
        return leaf;
    }

    private static long number(Object value) {
        if (value instanceof Integer || value instanceof Long) {
            return ((Number) value).longValue();
        }
        throw new IllegalArgumentException("a text count is an integer, found " + value);
    }

    /**
     * Whether a value's CBOR is a text in its value form. {@link Spill} spills such a value to a
     * {@code t} node and {@link Merge} merges two of them by union.
     */
    static boolean isText(byte[] cbor) {
        if (cbor.length < TEXT_PREFIX.length || !Arrays.equals(cbor, 0, TEXT_PREFIX.length, TEXT_PREFIX, 0, TEXT_PREFIX.length)) {
            return false;
        }
        try {
            fromWire(cbor);
            return true;
        } catch (IllegalArgumentException notText) {
            return false;
        }
    }

    /**
     * The union of two text values, or null when they disagree on a character or when the union
     * would pass the 1 MiB node bound as a {@code t} node (PDT 2 phase 6 review), so the resolver
     * decides the key on every replica alike.
     */
    static byte[] union(byte[] a, byte[] b) {
        try {
            TextLeaf merged = fromWire(a).union(fromWire(b));
            // the node encoding refuses a leaf over the node bound
            merged.encode();
            return toWire(merged);
        } catch (IllegalArgumentException | NodeFormatException disagreeOrTooLarge) {
            return null;
        }
    }
}
