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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import java.util.function.BiConsumer;

/**
 * Everything one replica holds under one entry id, as one value of {@link HashTrie} (srad/replication-by-entry.md
 * §3.1; SPEC annex I.6, <em>The audited set</em>): the entry's state, its claim, the state line a tag-sharded replica
 * remembered when it dropped the entry, and the lines the replica acknowledges for the id. One slot therefore renders
 * every digest line of its entry, in both forms: the text digest's {@code e:}, {@code c:} and {@code a:} lines
 * ({@link #forEachLegacyLine}, {@link #acks}), and the audited lines of annex I.6, where an acknowledged line stands as
 * the line itself ({@link #auditedLines}).
 *
 * <p>The slot does not compute a state's or a claim's hash: the replicated space owns those functions and hands the
 * hash in beside the value, so the slot stays free of the wire types, and the claim's own type is left to the caller
 * as {@code C}. The hash is kept, so a leaf rehashed after a neighbour changed does not recompute it.
 *
 * <p>Immutable; every {@code with} returns a new slot, or this one when nothing changes. A slot that holds nothing
 * ({@link #isEmpty}) is not stored: {@link HashTrie#put} removes the id instead.
 *
 * @param <C> the claim the caller keeps beside the entry's state
 */
public final class Slot<C> {

    /** The slot is marked lapsed (phase 4, RE-T-06). */
    private static final byte LAPSED = 1;
    /** The state's hash is held as {@link #stateCode}, whose lower-case hex is the text the line names. */
    private static final byte CODE = 2;

    private final EntryId id;
    // The first 64 bits of the key, SHA-256 of the id's UTF-8 bytes, most significant first: no array header per slot,
    // and a nibble or a comparison is a shift on a field. The rest of the key is computed again from the id on the rare
    // occasion it is needed: a path past 16 nibbles, which a trie reaches only past 16^16 slots or for ids ground to
    // share a 64-bit prefix, or two ids whose first 64 bits agree. Keeping the whole key took 24 bytes more per slot,
    // and a slot is most of what the map costs per entry (RE-Q-PRF-06).
    private final long k0;
    private final EntryState state;
    /** The state's hash as an int, when {@link #CODE} is set: the text digest's hash is the hex of an int. */
    private final int stateCode;
    private final byte flags;
    /**
     * What a slot holds beyond its state, kept out of line because almost no slot holds it: a claim, a remembered
     * dropped line, acknowledged lines, or a state hash given as text. Null when there is none of these.
     */
    private final Extras<C> extras;

    private Slot(EntryId id, long k0, EntryState state, int stateCode, byte flags, Extras<C> extras) {
        this.id = id;
        this.k0 = k0;
        this.state = state;
        this.stateCode = stateCode;
        this.flags = flags;
        this.extras = extras;
    }

    /** The rarely held part of a slot; immutable, never empty (an empty one is null in the slot). */
    private record Extras<C>(String stateHash, C claim, String claimHash, String droppedHash, List<String> acks) {

        static <C> Extras<C> of(String stateHash, C claim, String claimHash, String droppedHash, List<String> acks) {
            if (stateHash == null && claim == null && droppedHash == null && acks.isEmpty()) {
                return null;
            }
            return new Extras<>(stateHash, claim, claimHash, droppedHash, acks);
        }
    }

    /**
     * Returns a slot for the id that holds nothing yet, the start of every slot.
     *
     * @param id  the entry id
     * @param <C> the claim type
     * @return the empty slot
     */
    public static <C> Slot<C> empty(EntryId id) {
        Objects.requireNonNull(id, "id");
        byte[] key = keyOf(id);
        return new Slot<>(id, longAt(key, 0), null, 0, (byte) 0, null);
    }

    /**
     * The key of an entry id in the trie: SHA-256 of its UTF-8 bytes (annex I.6, <em>The key and the path</em>).
     *
     * @param id the entry id
     * @return the 32-byte key
     */
    public static byte[] keyOf(EntryId id) {
        return sha256().digest(id.value().getBytes(StandardCharsets.UTF_8));
    }

    // ---- reading

    /** Returns the entry id. */
    public EntryId id() {
        return id;
    }

    /** Returns a copy of the 32-byte key. */
    public byte[] key() {
        return keyOf(id);
    }

    /** Returns the entry's state, or null when this replica holds none. */
    public EntryState state() {
        return state;
    }

    /** Returns the hash the state line names, or null with no state. */
    public String stateHash() {
        if (state == null) {
            return null;
        }
        return (flags & CODE) != 0 ? Integer.toHexString(stateCode) : extras.stateHash();
    }

    /** Returns the claim, or null when this replica holds none. */
    public C claim() {
        return extras == null ? null : extras.claim();
    }

    /** Returns the hash the claim line names, or null with no claim. */
    public String claimHash() {
        return extras == null ? null : extras.claimHash();
    }

    /** Returns the remembered state hash of a shard-dropped entry, or null; it counts only while no state is held. */
    public String droppedHash() {
        return extras == null ? null : extras.droppedHash();
    }

    /** Returns the acknowledged lines, each without the {@code a:} prefix, in ascending string order; unmodifiable. */
    public List<String> acks() {
        return extras == null ? List.of() : extras.acks();
    }

    /**
     * Returns whether the entry's write lease has lapsed while it is present, as the caller judged it by its clock.
     * Nothing sets this in phase 1; phase 4 sets it from the expiry index (RE-T-06), and then the state and the claim
     * leave the audited lines while the acknowledged ones stay.
     */
    public boolean lapsed() {
        return (flags & LAPSED) != 0;
    }

    /** Returns whether the slot holds nothing at all, so the trie keeps no slot for it. */
    public boolean isEmpty() {
        return state == null && extras == null;
    }

    /**
     * Returns the state line {@code e:<id>=<hash>}: the held state's when there is one, else the remembered line of a
     * shard-dropped entry, as the text digest chooses today; null when there is neither.
     */
    public String stateLine() {
        String hash = lineHash();
        return hash == null ? null : "e:" + id.value() + "=" + hash;
    }

    /** Returns the claim line {@code c:<id>=<hash>}, or null with no claim. */
    public String claimLine() {
        String hash = claimHash();
        return hash == null ? null : "c:" + id.value() + "=" + hash;
    }

    /** The hash the state line names: the held state's, else the remembered dropped one. */
    private String lineHash() {
        String hash = stateHash();
        return hash != null ? hash : droppedHash();
    }

    /**
     * Hands the slot's text-digest lines to the caller as key and value, the key being {@code e:<id>} or
     * {@code c:<id>} and the value the hash, the way the text digest collects them before it sorts by key. The
     * acknowledged lines are not among them: the text digest writes those in a block of its own ({@link #acks}).
     *
     * @param sink receives each key and value
     */
    public void forEachLegacyLine(BiConsumer<String, String> sink) {
        String hash = lineHash();
        if (hash != null) {
            sink.accept("e:" + id.value(), hash);
        }
        String claimHash = claimHash();
        if (claimHash != null) {
            sink.accept("c:" + id.value(), claimHash);
        }
    }

    /**
     * Returns the slot's lines in the audited set of annex I.6, distinct, in no particular order: the state line and
     * the claim line unless the slot is {@link #lapsed} on a held state, and every acknowledged line as the line
     * itself. A line that holds a control character is left out, which leaves out every line of an id that holds
     * one: the audited set never holds such an id (annex I.6, <em>Entry ids in the set</em>), so the joined lines of
     * a leaf stay unambiguous even while the replica still keeps such an entry for the text digest.
     */
    public List<String> auditedLines() {
        List<String> acks = acks();
        List<String> lines = new ArrayList<>(2 + acks.size());
        if (!hidesHeldLines()) {
            addAudited(lines, stateLine());
            addAudited(lines, claimLine());
        }
        for (String ack : acks) {
            addAudited(lines, ack);
        }
        return lines;
    }

    /** Returns whether the slot has at least one audited line, which is what makes it count in the audited set. */
    public boolean audited() {
        if (hasControl(id.value())) {
            return false;
        }
        if (!hidesHeldLines() && (state != null || droppedHash() != null || claimHash() != null)) {
            return true;
        }
        for (String ack : acks()) {
            if (!hasControl(ack)) {
                return true;
            }
        }
        return false;
    }

    /**
     * A lapse is judged on a held state, so it hides the held state's line and the claim on it; a slot marked lapsed
     * with no state keeps its lines, since a remembered shard-dropped line has no lease to lapse.
     */
    private boolean hidesHeldLines() {
        return lapsed() && state != null;
    }

    // ---- changing

    /**
     * Returns the slot holding this state.
     *
     * @param newState the state
     * @param hash     the hash its state line names
     * @return the changed slot
     */
    public Slot<C> withState(EntryState newState, String hash) {
        Objects.requireNonNull(newState, "state");
        Objects.requireNonNull(hash, "hash");
        if (newState.equals(state) && hash.equals(stateHash())) {
            return this;
        }
        return new Slot<>(id, k0, newState, 0, (byte) (flags & ~CODE),
                Extras.of(hash, claim(), claimHash(), droppedHash(), acks()));
    }

    /**
     * Returns the slot holding this state, its hash given as the int whose lower-case hex the state line names: the
     * compact form the replicated space uses, which keeps the hash text out of the heap until a digest asks for it.
     *
     * @param newState the state
     * @param code     the hash, as an int
     * @return the changed slot
     */
    public Slot<C> withState(EntryState newState, int code) {
        Objects.requireNonNull(newState, "state");
        if ((flags & CODE) != 0 && code == stateCode && newState.equals(state)) {
            return this;
        }
        return new Slot<>(id, k0, newState, code, (byte) (flags | CODE),
                Extras.of(null, claim(), claimHash(), droppedHash(), acks()));
    }

    /** Returns the slot without a state. */
    public Slot<C> withoutState() {
        return state == null ? this
                : new Slot<>(id, k0, null, 0, (byte) (flags & ~CODE),
                        Extras.of(null, claim(), claimHash(), droppedHash(), acks()));
    }

    /**
     * Returns the slot holding this claim.
     *
     * @param newClaim the claim
     * @param hash     the hash its claim line names
     * @return the changed slot
     */
    public Slot<C> withClaim(C newClaim, String hash) {
        Objects.requireNonNull(newClaim, "claim");
        Objects.requireNonNull(hash, "hash");
        if (newClaim.equals(claim()) && hash.equals(claimHash())) {
            return this;
        }
        return withExtras(Extras.of(textHash(), newClaim, hash, droppedHash(), acks()));
    }

    /** Returns the slot without a claim. */
    public Slot<C> withoutClaim() {
        return claim() == null ? this : withExtras(Extras.of(textHash(), null, null, droppedHash(), acks()));
    }

    /**
     * Returns the slot remembering a shard-dropped state's hash, or forgetting it when the hash is null.
     *
     * @param hash the remembered hash, or null
     * @return the changed slot
     */
    public Slot<C> withDropped(String hash) {
        return Objects.equals(hash, droppedHash()) ? this
                : withExtras(Extras.of(textHash(), claim(), claimHash(), hash, acks()));
    }

    /**
     * Returns the slot acknowledging one more line, given without the {@code a:} prefix.
     *
     * @param line an {@code e:} or {@code c:} line naming this slot's id
     * @return the changed slot
     * @throws IllegalArgumentException when the line names no state or claim of this id
     */
    public Slot<C> withAck(String line) {
        checkAck(line);
        List<String> acks = acks();
        if (acks.contains(line)) {
            return this;
        }
        TreeSet<String> sorted = new TreeSet<>(acks);
        sorted.add(line);
        return withExtras(Extras.of(textHash(), claim(), claimHash(), droppedHash(), List.copyOf(sorted)));
    }

    /**
     * Returns the slot acknowledging exactly these lines, replacing the ones it had.
     *
     * @param lines lines without the {@code a:} prefix, each naming this slot's id
     * @return the changed slot
     */
    public Slot<C> withAcks(Collection<String> lines) {
        TreeSet<String> sorted = new TreeSet<>();
        for (String line : lines) {
            checkAck(line);
            sorted.add(line);
        }
        List<String> next = List.copyOf(sorted);
        return next.equals(acks()) ? this
                : withExtras(Extras.of(textHash(), claim(), claimHash(), droppedHash(), next));
    }

    /**
     * Returns the slot no longer acknowledging a line.
     *
     * @param line the line without the {@code a:} prefix
     * @return the changed slot
     */
    public Slot<C> withoutAck(String line) {
        List<String> acks = acks();
        if (!acks.contains(line)) {
            return this;
        }
        List<String> next = new ArrayList<>(acks);
        next.remove(line);
        return withExtras(Extras.of(textHash(), claim(), claimHash(), droppedHash(), List.copyOf(next)));
    }

    /**
     * Returns the slot marked lapsed or not (RE-T-06, phase 4).
     *
     * @param value whether the held entry's write lease has lapsed by the replica's clock
     * @return the changed slot
     */
    public Slot<C> withLapsed(boolean value) {
        return value == lapsed() ? this
                : new Slot<>(id, k0, state, stateCode, (byte) (value ? flags | LAPSED : flags & ~LAPSED), extras);
    }

    /** The state hash when it is held as text rather than as a code, the one part of the extras a state brings. */
    private String textHash() {
        return extras == null ? null : extras.stateHash();
    }

    private Slot<C> withExtras(Extras<C> next) {
        return new Slot<>(id, k0, state, stateCode, flags, next);
    }

    // ---- the key, for the trie

    /** The nibble of the key at a position from 0 to 63, most significant first. */
    int nibble(int position) {
        // Past 16 nibbles the key is computed again: rare, see the key's comment.
        long word = position < 16 ? k0 : longAt(keyOf(id), 8 * (position >>> 4));
        return (int) (word >>> (60 - 4 * (position & 15))) & 0xF;
    }

    /** Orders slots by key, unsigned, and by id after that, so two ids whose keys collide still have one order. */
    int compareKey(Slot<?> other) {
        int c = Long.compareUnsigned(k0, other.k0);
        if (c == 0 && !id.equals(other.id)) {
            // The first 64 bits agree: compare the rest of both keys, computed again.
            byte[] mine = keyOf(id);
            byte[] theirs = keyOf(other.id);
            c = Arrays.compareUnsigned(mine, 8, 32, theirs, 8, 32);
        }
        return c != 0 ? c : id.value().compareTo(other.id.value());
    }

    /** Whether this slot's key begins with the path, from nibble {@code from} on. */
    boolean keyStartsWith(byte[] path, int from) {
        for (int i = from; i < path.length; i++) {
            if (nibble(i) != path[i]) {
                return false;
            }
        }
        return true;
    }

    // ---- helpers

    private void checkAck(String line) {
        Objects.requireNonNull(line, "line");
        String tail = id.value() + "=";
        if (!(line.startsWith("e:") || line.startsWith("c:")) || !line.startsWith(tail, 2)) {
            throw new IllegalArgumentException("an acknowledged line names a state or claim of " + id.value()
                    + " without the a: prefix: " + line);
        }
    }

    private static void addAudited(List<String> lines, String line) {
        if (line != null && !hasControl(line) && !lines.contains(line)) {
            lines.add(line);
        }
    }

    /** Whether text holds U+0000 to U+001F or U+007F, the characters annex I.6 keeps out of the audited set. */
    static boolean hasControl(String text) {
        for (int i = 0; i < text.length(); i++) {
            char ch = text.charAt(i);
            if (ch < 0x20 || ch == 0x7F) {
                return true;
            }
        }
        return false;
    }

    static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required of every Java platform", e);
        }
    }

    private static long longAt(byte[] b, int at) {
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (b[at + i] & 0xFF);
        }
        return v;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Slot<?> other) || !id.equals(other.id) || lapsed() != other.lapsed()
                || !Objects.equals(state, other.state)) {
            return false;
        }
        boolean sameHash = (flags & CODE) != 0 && (other.flags & CODE) != 0
                ? stateCode == other.stateCode
                : Objects.equals(stateHash(), other.stateHash());
        return sameHash
                && Objects.equals(claim(), other.claim())
                && Objects.equals(claimHash(), other.claimHash())
                && Objects.equals(droppedHash(), other.droppedHash())
                && acks().equals(other.acks());
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, state, stateHash(), claim(), claimHash(), droppedHash(), acks(), lapsed());
    }

    @Override
    public String toString() {
        return "Slot[" + id.value() + ", state=" + stateHash() + ", claim=" + claimHash() + ", dropped="
                + droppedHash() + ", acks=" + acks().size() + (lapsed() ? ", lapsed" : "") + "]";
    }
}
