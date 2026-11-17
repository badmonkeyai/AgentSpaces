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
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.common.hlc.HybridLogicalClock;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The replica state of one space as a delta-mergeable CRDT (spec §7.3): an OR-Set
 * of entries keyed by {@link EntryId}, each carrying its own
 * {@link EntryState}. Replicas that have seen the same updates are identical, and
 * partitions merge deterministically; the replicated space grows on this class,
 * and the CRDT laws are pinned by randomized convergence tests.
 *
 * <p>Immutable; operations return new states. Tombstone garbage collection
 * ({@link #gc}) drops entries that are no longer live once their horizon of
 * SPEC §7.3 has passed ({@link #keepMillis}), and leaves merge semantics
 * unchanged.
 */
public final class SpaceStateCrdt {

    private static final SpaceStateCrdt EMPTY = new SpaceStateCrdt(HashTrie.empty(), 0, 0, 0);

    /**
     * The map: the fingerprinted trie of srad/replication-by-entry.md §3.1 (RE-M-03), one slot per entry id. It
     * replaces the hash map that was copied whole on every change (RE-M-02's owned map is gone with it): a change now
     * copies one path of about log16(n) nodes. A slot holds the entry's state and, for the replicated space, the
     * other digest lines of the same id (its claim, a shard-dropped line, acknowledged lines; RE-M-07), so one root
     * covers every line. The CRDT itself reads and merges only the states: two states are equal when their entries
     * are, and a merge carries only the other side's entries, so the laws are those of the map it replaced.
     */
    private final HashTrie<Object> trie;
    /** Slots holding a state: the entries, tombstones included. */
    private final int states;
    /** Slots holding a claim. */
    private final int claims;
    /** Slots holding anything besides a state: a claim, a dropped line or an acknowledgement. */
    private final int sides;
    /**
     * The live records, built on the first read (swap amplification, ROBUSTNESS.md §10.8): the state never changes, and
     * a replica reads it many times between two changes, so each read used to rebuild every present record. The
     * replicated space keeps its own live index beside the map (RE-M-04) and does not read this one.
     */
    private volatile List<EntryRecord> live;
    private volatile Set<EntryId> ids;

    private SpaceStateCrdt(HashTrie<Object> trie, int states, int claims, int sides) {
        this.trie = trie;
        this.states = states;
        this.claims = claims;
        this.sides = sides;
    }

    /**
     * How many trie node fingerprints this process has computed so far (annex I.6), each memoized one once: a count that
     * only grows, for the round-cost measure of srad/replication-by-entry.md RE-Q-PRF-02 and a status page.
     *
     * @return the count
     */
    public static long fingerprintsComputed() {
        return HashTrie.fingerprintsComputed();
    }

    /** Returns the empty state. */
    public static SpaceStateCrdt empty() {
        return EMPTY;
    }

    /**
     * Wraps one entry's exact state as a mergeable value. Replication uses this
     * to merge a remote replica's per-entry state into the local replica.
     *
     * @param entryId the entry
     * @param state   the entry's state
     * @return a one-entry CRDT holding exactly that state
     */
    public static SpaceStateCrdt of(EntryId entryId, EntryState state) {
        Objects.requireNonNull(entryId, "entryId");
        Objects.requireNonNull(state, "state");
        return EMPTY.putState(entryId, state);
    }

    /**
     * Applies a local write.
     *
     * @param record the written record
     * @param addDot the writing replica's dot
     * @param lease  the write lease register
     * @return the updated state
     */
    public SpaceStateCrdt add(EntryRecord record, Dot addDot, LwwRegister<LeaseInfo> lease) {
        Objects.requireNonNull(record, "record");
        EntryState added = EntryState.added(record, addDot, lease);
        EntryState current = stateOrNull(record.entryId());
        return putState(record.entryId(), current == null ? added : current.merge(added));
    }

    /**
     * Withdraws an entry by removing its observed add dots.
     *
     * @param entryId the entry to withdraw
     * @return the updated state; unchanged when the entry is unknown here
     */
    public SpaceStateCrdt remove(EntryId entryId) {
        return update(entryId, EntryState::withObservedRemoved);
    }

    /**
     * Marks an entry completed. Monotone: merges never resurrect it.
     *
     * @param entryId the completed entry
     * @return the updated state; unchanged when the entry is unknown here
     */
    public SpaceStateCrdt complete(EntryId entryId) {
        return update(entryId, EntryState::withCompleted);
    }

    /**
     * Applies a lease write (take claim, renewal, reappearance).
     *
     * @param entryId    the entry
     * @param leaseWrite the lease write, stamped by the writer's HLC
     * @return the updated state; unchanged when the entry is unknown here
     */
    public SpaceStateCrdt setLease(EntryId entryId, LwwRegister<LeaseInfo> leaseWrite) {
        return update(entryId, state -> state.withLease(leaseWrite));
    }

    /**
     * Merges another replica's state into this one.
     *
     * @param other the other replica's state
     * @return the merged state
     */
    public SpaceStateCrdt merge(SpaceStateCrdt other) {
        Objects.requireNonNull(other, "other");
        if (other.states == 0) {
            return this;
        }
        if (states == 0 && sides == 0 && other.sides == 0) {
            return other; // immutable, so the other state can be shared as it is
        }
        if ((long) other.states * 4 < trie.size()) {
            // A few states into a large map: one path copy each (RE-Q-PRF-01).
            SpaceStateCrdt merged = this;
            for (Slot<Object> theirs : other.trie) {
                if (theirs.state() != null) {
                    merged = merged.merge(theirs.id(), theirs.state());
                }
            }
            return merged;
        }
        // A merge as large as the map, a join or a partition healing: one batch, built once.
        HashTrie.Builder<Object> batch = trie.toBuilder();
        Counts counts = new Counts(states, claims, sides);
        for (Slot<Object> theirs : other.trie) {
            EntryState incoming = theirs.state();
            if (incoming == null) {
                continue;
            }
            Slot<Object> mine = batch.get(theirs.id());
            EntryState held = mine == null ? null : mine.state();
            EntryState merged = held == null ? incoming : held.merge(incoming);
            if (held != null && merged.equals(held)) {
                continue; // a state already held costs no copy
            }
            Slot<Object> before = mine == null ? Slot.empty(theirs.id()) : mine;
            Slot<Object> after = before.withState(merged, stateCode(merged));
            counts.change(mine, after);
            batch.put(after);
        }
        return counts.of(batch.build());
    }

    /**
     * Merges one entry's state from another replica, as {@code merge(SpaceStateCrdt.of(entryId, state))} does,
     * without building the one-entry state first. Internal to the replicated space.
     *
     * @param entryId  the entry
     * @param incoming the other replica's state of it
     * @return the merged state; this one when the state was already held
     */
    public SpaceStateCrdt merge(EntryId entryId, EntryState incoming) {
        Objects.requireNonNull(entryId, "entryId");
        Objects.requireNonNull(incoming, "state");
        EntryState held = stateOrNull(entryId);
        if (held == null) {
            return putState(entryId, incoming);
        }
        EntryState merged = held.merge(incoming);
        return merged.equals(held) ? this : putState(entryId, merged);
    }

    /** Returns the records of currently present entries. */
    public List<EntryRecord> liveRecords() {
        List<EntryRecord> built = live;
        if (built == null) {
            List<EntryRecord> records = new ArrayList<>();
            for (Slot<Object> slot : trie) {
                EntryState state = slot.state();
                if (state != null && state.present()) {
                    records.add(liveRecord(state));
                }
            }
            built = Collections.unmodifiableList(records);
            live = built;
        }
        return built;
    }

    /**
     * The record of a present entry as a reader sees it: the written record under the entry's current lease. The
     * written record itself when the lease never changed, so an unrenewed entry costs no second record.
     *
     * @param state the entry's state
     * @return the record under the current lease
     */
    public static EntryRecord liveRecord(EntryState state) {
        LeaseInfo lease = state.lease().value();
        return lease.equals(state.record().lease()) ? state.record() : state.record().withLease(lease);
    }

    /**
     * Returns one entry's state.
     *
     * @param entryId the entry
     * @return the state, when known to this replica
     */
    public Optional<EntryState> state(EntryId entryId) {
        return Optional.ofNullable(stateOrNull(entryId));
    }

    /** Returns the ids of every entry this replica knows about, tombstones included; unmodifiable. */
    public Set<EntryId> entryIds() {
        Set<EntryId> view = ids;
        if (view == null) {
            view = Collections.unmodifiableSet(new StateIds());
            ids = view;
        }
        return view;
    }

    /** Returns the number of entries this replica knows about, tombstones included. */
    public int knownEntries() {
        return states;
    }

    /**
     * Whether one entry's state is a collectable tombstone (spec §7.3): the
     * entry is no longer live (completed, withdrawn, or its write lease has
     * lapsed) and its horizon under the legacy rule has passed. A completion
     * keeps twice the maximum lease past its lease expiry, so every replica
     * that could still learn of it has had two full lease periods to do so; a
     * withdrawal keeps the same, and never less than the clock drift ceiling;
     * a lapse keeps the lapse horizon.
     *
     * @param state          the entry's state
     * @param nowMillis      the current time, epoch milliseconds
     * @param maxLeaseMillis the longest lease observed for the space
     * @return {@code true} when the entry may be dropped
     */
    public static boolean collectable(EntryState state, long nowMillis, long maxLeaseMillis) {
        return collectable(state, nowMillis, maxLeaseMillis, Horizon.LEGACY);
    }

    /**
     * Which collection rule of SPEC §7.3 a replica applies (RE-H-04 of srad/replication-by-entry.md). The replicated
     * space reads it from its membership view, never from a caller: {@link #PER_ENTRY} only while this node and every
     * member of its view advertise {@code wire.horizon=1}, and {@link #LEGACY} otherwise.
     */
    public enum Horizon {
        /**
         * Some member lacks the hint, or this node keeps the old rule: a completed entry keeps {@code 2 x maxLease}
         * past its lease expiry, a withdrawn one, or one both withdrawn and completed, {@code max(2 x maxLease,
         * MAX_DRIFT)}, and a lapsed one the lapse horizon. This is the rule of v0.1.34 and earlier for completions and withdrawals, with the drift floor of
         * v0.1.35 under a withdrawal (owner decision 22).
         */
        LEGACY,
        /**
         * Every member advertises {@code wire.horizon=1}: a withdrawn entry keeps {@code max(5 min, MAX_DRIFT)} past
         * its own lease expiry L, whatever the maximum lease. Completed entries keep {@code 2 x maxLease}, since their
         * safety rests on delivery, not on the lease (docs/proofs/per-entry-horizon.md), and one both withdrawn and
         * completed keeps at least {@code MAX_DRIFT} as well.
         */
        PER_ENTRY
    }

    /**
     * Whether one entry's state is a collectable tombstone under the given rule (SPEC §7.3, v0.1.35): the entry is no
     * longer live and its horizon ({@link #collectionHorizon(EntryState, long, Horizon)}) has passed. The three
     * argument form is this one under {@link Horizon#LEGACY}.
     *
     * @param state          the entry's state
     * @param nowMillis      the current time, epoch milliseconds
     * @param maxLeaseMillis the longest lease observed for the space
     * @param horizon        the rule the replica's view allows
     * @return {@code true} when the entry may be dropped
     */
    public static boolean collectable(EntryState state, long nowMillis, long maxLeaseMillis, Horizon horizon) {
        Objects.requireNonNull(state, "state");
        long leaseExpiry = state.lease().value().expiresAtMillis();
        if (state.present() && leaseExpiry > nowMillis) {
            return false;
        }
        long at = leaseExpiry + keepMillis(state, maxLeaseMillis, horizon);
        return at <= nowMillis && at >= leaseExpiry; // guard overflow
    }

    /**
     * The time from which {@link #collectable(EntryState, long, long)} holds for a state, or {@link Long#MAX_VALUE}
     * when it never does (the horizon overflowed): the lease expiry plus what the state keeps. A present entry is
     * never collectable before its lease lapses, and its horizon is never earlier than that, so for every state
     * {@code collectable(state, now, maxLease)} is exactly {@code now >= collectionHorizon(state, maxLease)}. The
     * replicated space files each tombstone under this time, so its sweep visits a tombstone only once it is due.
     * This is the rule under {@link Horizon#LEGACY}, the one every member without {@code wire.horizon=1} keeps.
     *
     * @param state          the entry's state
     * @param maxLeaseMillis the longest lease observed for the space
     * @return the collection horizon, epoch milliseconds
     */
    public static long collectionHorizon(EntryState state, long maxLeaseMillis) {
        return collectionHorizon(state, maxLeaseMillis, Horizon.LEGACY);
    }

    /**
     * The collection horizon under the given rule, or {@link Long#MAX_VALUE} when the sum overflows; for every state
     * {@code collectable(state, now, maxLease, horizon)} is exactly {@code now >= collectionHorizon(state, maxLease,
     * horizon)}.
     *
     * @param state          the entry's state
     * @param maxLeaseMillis the longest lease observed for the space
     * @param horizon        the rule the replica's view allows
     * @return the collection horizon, epoch milliseconds
     */
    public static long collectionHorizon(EntryState state, long maxLeaseMillis, Horizon horizon) {
        Objects.requireNonNull(state, "state");
        long leaseExpiry = state.lease().value().expiresAtMillis();
        long at = leaseExpiry + keepMillis(state, maxLeaseMillis, horizon);
        return at >= leaseExpiry ? at : Long.MAX_VALUE; // the same overflow guard as collectable
    }

    /**
     * How long a state is kept past its own lease expiry before it is collectable (SPEC §7.3, v0.1.35).
     * <ul>
     *   <li>A completed entry keeps {@code 2 x maxLease} under both rules. A completion is a transition every replica
     *       must hear, and an issuer cut off from the completer keeps renewing, so a shorter horizon would let a live
     *       copy back after the cut heals. A state both withdrawn and completed counts as completed, and as a
     *       withdrawal keeps at least {@link #WITHDRAWN_DRIFT_FLOOR_MILLIS} as well, so that it never goes before a
     *       copy that is withdrawn alone.</li>
     *   <li>A withdrawn entry keeps {@code 2 x maxLease} under {@link Horizon#LEGACY}, and under
     *       {@link Horizon#PER_ENTRY} the five minutes of {@link #LAPSED_GRACE_MILLIS}; under both, never less than
     *       {@link #WITHDRAWN_DRIFT_FLOOR_MILLIS}, the clock drift ceiling. Only its issuer writes its lease and a
     *       renewal never shortens it (SPEC §7.2), so the withdrawal's own expiry L is the largest any copy carried;
     *       at L + 5 min every present copy is past its own lapse horizon, which is never longer than five minutes,
     *       and this replica's door refuses it by the same clock. The floor covers the replica that never heard of the
     *       withdrawal: its clock trails this one by at most the drift ceiling, so once this replica's clock reads
     *       L + MAX_DRIFT, that replica's reads at least L, and a copy carrying L or less is already lapsed there when
     *       it arrives. Without the floor, a replica trailing by more than five minutes could be handed an old copy
     *       after every tombstone was gone and show it live until L by its own clock. The keep does not move with the
     *       observed maximum lease under the per-entry rule, so a withdrawal collected under it stays refused when
     *       that maximum grows later, which the legacy rule cannot promise.</li>
     *   <li>A present entry whose lease lapsed keeps the lapse horizon under both rules.</li>
     * </ul>
     *
     * @param state          the entry's state
     * @param maxLeaseMillis the longest lease observed for the space
     * @param horizon        the rule the replica's view allows
     * @return the time kept past the lease expiry, milliseconds
     */
    public static long keepMillis(EntryState state, long maxLeaseMillis, Horizon horizon) {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(horizon, "horizon");
        if (state.completed()) {
            long keep = 2 * Math.max(0, maxLeaseMillis);
            // A state both withdrawn and completed is a withdrawn tombstone too, so it keeps the drift floor. Without
            // it, such a tombstone collected at 2 x maxLease would let a copy that is withdrawn alone, which keeps the
            // floor, back through the door after it (the model's RefusedAtEveryDoor, 2026-11-15).
            return state.removes().containsAll(state.adds()) ? Math.max(keep, WITHDRAWN_DRIFT_FLOOR_MILLIS) : keep;
        }
        if (state.present()) {
            // A lapse is judged by the lease stamp, which every replica reads alike, so a lapsed entry needs only a
            // grace past clock skew, never two maximum leases (found 2026-11-07).
            return lapsedKeepMillis(maxLeaseMillis);
        }
        long keep = horizon == Horizon.PER_ENTRY ? LAPSED_GRACE_MILLIS : 2 * Math.max(0, maxLeaseMillis);
        return Math.max(keep, WITHDRAWN_DRIFT_FLOOR_MILLIS);
    }

    /**
     * The least a withdrawn entry keeps past its own lease expiry under either rule (SPEC §7.3, v0.1.35, owner
     * decision 22 of srad/replication-by-entry.md): the clock drift ceiling of SPEC §11, ten minutes. Any two
     * conforming clocks are within it, so when one replica collects at L + MAX_DRIFT by its own clock, every other
     * replica's clock is at L or later, and no copy of the entry, whose expiry is at most L, can be live anywhere.
     */
    public static final long WITHDRAWN_DRIFT_FLOOR_MILLIS = HybridLogicalClock.MAX_DRIFT_MILLIS;

    /**
     * How long a present entry's tombstone stays after its lease lapsed, at most: five minutes. A lapse needs no more
     * than a grace, since every replica judges it by the same lease stamp and an entry that is still live on a
     * trailing clock is genuinely live there; a withdrawal, which a trailing replica may never hear of, keeps at least
     * {@link #WITHDRAWN_DRIFT_FLOOR_MILLIS}.
     */
    public static final long LAPSED_GRACE_MILLIS = 5L * 60 * 1000;

    /**
     * How long a lapsed entry, present but past its lease, is kept before it is collectable: the lapse horizon of SPEC
     * §7.3, {@code min(2 x maxLease, 5 min)} (RE-H-03 of srad/replication-by-entry.md). A lapse is judged by the lease
     * stamp, which every replica reads alike, so it needs only a grace past clock skew, never two maximum leases.
     *
     * @param maxLeaseMillis the longest lease observed for the space
     * @return the time a lapsed entry is kept past its lease expiry, milliseconds
     */
    public static long lapsedKeepMillis(long maxLeaseMillis) {
        return Math.min(2 * Math.max(0, maxLeaseMillis), LAPSED_GRACE_MILLIS);
    }

    /**
     * The lease expiry a renewal sets (SPEC §7.2, RE-H-02 of srad/replication-by-entry.md): at least the given
     * extension from now, and never less than the lease already had, {@code max(current, now + extension)}. The
     * per-entry horizon rests on it: only the issuer writes the lease and a renewal never shortens it, so the
     * expiry a withdrawal carries is the largest any copy of the entry ever held. A caller that wants a shorter lease
     * cancels and writes again. The sum saturates rather than wrapping, so a very long extension cannot turn into a
     * lease in the past.
     *
     * @param currentExpiryMillis the lease's expiry now, epoch milliseconds
     * @param nowMillis           the current time, epoch milliseconds
     * @param extensionMillis     the extension the caller asked for, milliseconds
     * @return the expiry the renewal sets, epoch milliseconds
     */
    public static long renewedExpiry(long currentExpiryMillis, long nowMillis, long extensionMillis) {
        long asked;
        try {
            asked = Math.addExact(nowMillis, extensionMillis);
        } catch (ArithmeticException overflow) {
            asked = extensionMillis > 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
        }
        return Math.max(currentExpiryMillis, asked);
    }

    /**
     * Returns the ids of entries {@link #collectable} would drop now.
     *
     * @param nowMillis      the current time, epoch milliseconds
     * @param maxLeaseMillis the longest lease observed for the space
     * @return the collectable entry ids
     */
    public Set<EntryId> collectable(long nowMillis, long maxLeaseMillis) {
        return collectable(nowMillis, maxLeaseMillis, Horizon.LEGACY);
    }

    /**
     * Returns the ids of entries {@link #collectable(EntryState, long, long, Horizon)} would drop now under the
     * given rule.
     *
     * @param nowMillis      the current time, epoch milliseconds
     * @param maxLeaseMillis the longest lease observed for the space
     * @param horizon        the rule the replica's view allows
     * @return the collectable entry ids
     */
    public Set<EntryId> collectable(long nowMillis, long maxLeaseMillis, Horizon horizon) {
        Set<EntryId> found = new HashSet<>();
        for (Slot<Object> slot : trie) {
            if (slot.state() != null && collectable(slot.state(), nowMillis, maxLeaseMillis, horizon)) {
                found.add(slot.id());
            }
        }
        return found;
    }

    /**
     * Garbage-collects tombstones (spec §7.3): drops every entry that is no
     * longer live and whose legacy horizon ({@link #keepMillis}) has passed.
     * Merge semantics are unchanged; a collected tombstone re-offered by a
     * lagging replica is simply collectable again, and the replicated space
     * refuses such offers at the door.
     *
     * @param nowMillis      the current time, epoch milliseconds
     * @param maxLeaseMillis the longest lease observed for the space
     * @return the state without collectable entries; {@code this} when nothing qualifies
     */
    public SpaceStateCrdt gc(long nowMillis, long maxLeaseMillis) {
        return without(collectable(nowMillis, maxLeaseMillis));
    }

    /**
     * Returns a state without the given entries. Only their states go: a claim or an acknowledgement the replicated
     * space keeps for the same id stays until the space drops it itself, as it did when those lived in maps of their
     * own.
     *
     * @param ids the entries to drop
     * @return the reduced state; {@code this} when none were known
     */
    public SpaceStateCrdt without(Set<EntryId> ids) {
        Objects.requireNonNull(ids, "ids");
        if (ids.isEmpty() || states == 0) {
            return this;
        }
        SpaceStateCrdt reduced = this;
        if ((long) ids.size() * 4 < trie.size()) {
            for (EntryId entryId : ids) {
                reduced = reduced.dropState(entryId);
            }
            return reduced;
        }
        HashTrie.Builder<Object> batch = trie.toBuilder();
        Counts counts = new Counts(states, claims, sides);
        for (EntryId entryId : ids) {
            Slot<Object> mine = batch.get(entryId);
            if (mine != null && mine.state() != null) {
                Slot<Object> after = mine.withoutState();
                counts.change(mine, after);
                batch.put(after);
            }
        }
        return counts.states == states ? this : counts.of(batch.build());
    }

    /**
     * Equal when the two hold the same entries in the same states, as the map it replaced compared. The other lines a
     * slot may hold for the replicated space are replica-local bookkeeping and take no part.
     */
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof SpaceStateCrdt other) || other.states != states) {
            return false;
        }
        // Both tries walk in key order, so the entries of the two pair off as they come.
        java.util.Iterator<Slot<Object>> theirs = other.trie.iterator();
        for (Slot<Object> mine : trie) {
            if (mine.state() == null) {
                continue;
            }
            Slot<Object> next = null;
            while (theirs.hasNext()) {
                Slot<Object> candidate = theirs.next();
                if (candidate.state() != null) {
                    next = candidate;
                    break;
                }
            }
            if (next == null || !next.id().equals(mine.id()) || !next.state().equals(mine.state())) {
                return false;
            }
        }
        return true;
    }

    /** The hash a map of the same entries gives: the sum of each id's hash XOR its state's. */
    @Override
    public int hashCode() {
        int h = 0;
        for (Slot<Object> slot : trie) {
            if (slot.state() != null) {
                h += slot.id().hashCode() ^ slot.state().hashCode();
            }
        }
        return h;
    }

    @Override
    public String toString() {
        return "SpaceStateCrdt[known=" + states + ", live=" + liveRecords().size() + "]";
    }

    // ---- the text digest's hash of a state

    /**
     * The hash a text digest's state line names for a state (TECH-SPEC §10.3), {@code e:<id>=<hash>}: the lower-case
     * hex of {@link #stateCode}. Kept here so the slot that holds a state holds its hash too. Internal to the
     * replicated space.
     *
     * @param state the state
     * @return the hash text
     */
    public static String stateHash(EntryState state) {
        return Integer.toHexString(stateCode(state));
    }

    /**
     * The int whose hex {@link #stateHash} is: the hash of the add dots, the remove dots, the lease stamp and the
     * completion flag, as the digest has named it since v0.1.
     *
     * @param state the state
     * @return the hash, as an int
     */
    public static int stateCode(EntryState state) {
        return Objects.hash(state.adds(), state.removes(), state.lease().stamp(), state.completed());
    }

    // ---- the other lines of a slot, kept for the replicated space (RE-M-07); internal

    /**
     * Returns the claim held for an entry, of the replicated space's own type, or null.
     *
     * @param entryId the entry
     * @return the claim, or null
     */
    public Object claim(EntryId entryId) {
        Slot<Object> slot = trie.get(Objects.requireNonNull(entryId, "entryId"));
        return slot == null ? null : slot.claim();
    }

    /**
     * Returns the state holding this claim for the entry; its claim line names the hash. Internal to the replicated
     * space, which keeps its own merge rules for claims.
     *
     * @param entryId the entry
     * @param claim   the claim
     * @param hash    the hash the claim line names
     * @return the changed state
     */
    public SpaceStateCrdt withClaim(EntryId entryId, Object claim, String hash) {
        Objects.requireNonNull(claim, "claim");
        Objects.requireNonNull(hash, "hash");
        return change(entryId, slot -> slot.withClaim(claim, hash));
    }

    /**
     * Returns the state without the entry's claim.
     *
     * @param entryId the entry
     * @return the changed state; this one when no claim was held
     */
    public SpaceStateCrdt withoutClaim(EntryId entryId) {
        return change(entryId, Slot::withoutClaim);
    }

    /**
     * Returns the state remembering the line of an entry a tag-sharded replica dropped, or forgetting it when the
     * hash is null. The line counts in the digest only while no state is held for the id.
     *
     * @param entryId the entry
     * @param hash    the remembered state hash, or null
     * @return the changed state
     */
    public SpaceStateCrdt withDropped(EntryId entryId, String hash) {
        return change(entryId, slot -> slot.withDropped(hash));
    }

    /**
     * Returns the state acknowledging a refused line of an entry, given without the {@code a:} prefix.
     *
     * @param entryId the entry the line names
     * @param line    the {@code e:} or {@code c:} line
     * @return the changed state
     */
    public SpaceStateCrdt withAck(EntryId entryId, String line) {
        return change(entryId, slot -> slot.withAck(line));
    }

    /**
     * Returns the state no longer acknowledging a line.
     *
     * @param entryId the entry the line names
     * @param line    the line without the {@code a:} prefix
     * @return the changed state
     */
    public SpaceStateCrdt withoutAck(EntryId entryId, String line) {
        return change(entryId, slot -> slot.withoutAck(line));
    }

    /** Returns the number of claims held. */
    public int claimCount() {
        return claims;
    }

    /** What {@link #forEachHeld} hands over for one id: its state and its claim, either of which may be null. */
    @FunctionalInterface
    public interface HeldVisitor {
        /**
         * Visits one id.
         *
         * @param entryId   the entry
         * @param state     its state, or null
         * @param stateHash the hash its state line names, or null with no state
         * @param claim     its claim, or null
         * @param claimHash the hash its claim line names, or null with no claim
         */
        void visit(EntryId entryId, EntryState state, String stateHash, Object claim, String claimHash);
    }

    /**
     * Visits every id holding a state or a claim, in key order; ids holding only a dropped line or acknowledgements
     * are skipped. Internal to the replicated space, which walks its lines this way instead of the maps it kept.
     *
     * @param visitor receives each id
     */
    public void forEachHeld(HeldVisitor visitor) {
        for (Slot<Object> slot : trie) {
            if (slot.state() != null || slot.claim() != null) {
                visitor.visit(slot.id(), slot.state(), slot.stateHash(), slot.claim(), slot.claimHash());
            }
        }
    }

    // ---- the digests

    /**
     * The text digest of today (TECH-SPEC §10.3; SPEC §5.3), collected from the slots and sorted (RE-M-06): every
     * {@code e:} and {@code c:} line in string order, then the {@code a:} block. Internal to the replicated space.
     *
     * @return the digest, UTF-8
     */
    public byte[] legacyDigest() {
        return trie.legacyDigest();
    }

    /** How many nibbles deep the trie runs (RE-Q-MNT-02, a status figure); see {@link HashTrie#depth()}. */
    public int trieDepth() {
        return trie.depth();
    }

    /**
     * The stream hash of SPEC annex I.6 (working draft) over the slots: the SHA-256 of the audited root's preimage.
     * Phase 4 of srad/replication-by-entry.md puts it on the wire under {@code wire.ae=2}; until then nothing sends it.
     * Internal.
     *
     * @return a copy of the 32-byte hash
     */
    public byte[] streamHash() {
        return trie.streamHash();
    }

    /**
     * The audited node at a path (annex I.6), for phase 4's {@code tree} answers; nothing sends it yet. Internal.
     *
     * @param path one nibble per byte, at most 64
     * @return the node
     */
    public AuditNode node(byte[] path) {
        return trie.node(path);
    }

    /**
     * Every audited line under a path, joined as a leaf's preimage, whatever the count there (annex I.6). Internal.
     *
     * @param path one nibble per byte, at most 64
     * @return the joined lines as UTF-8
     */
    public byte[] auditLines(byte[] path) {
        return trie.linesUnder(path);
    }

    /**
     * {@link #auditLines(byte[])} when the joined lines hold at most {@code maxBytes} bytes, and null otherwise, with
     * the gathering stopped as soon as they pass the bound. Internal.
     *
     * @param path     one nibble per byte, at most 64
     * @param maxBytes the most bytes the joined lines may hold
     * @return the joined lines as UTF-8, or null
     */
    public byte[] auditLines(byte[] path, long maxBytes) {
        return trie.linesUnder(path, maxBytes);
    }

    /**
     * Which of a partner's lines are among the audited lines under a path, each read in the one slot it names
     * ({@link HashTrie#holdsLine}), so an asker's comparison costs what the answer holds. Internal.
     *
     * @param path  one nibble per byte, at most 64
     * @param lines the partner's lines
     * @return the lines held
     */
    public java.util.Set<String> auditHeld(byte[] path, java.util.Collection<String> lines) {
        java.util.Set<String> held = new java.util.HashSet<>();
        for (String line : lines) {
            if (trie.holdsLine(line, path)) {
                held.add(line);
            }
        }
        return held;
    }

    /**
     * Whether the entry's slot is marked lapsed (RE-T-06): its held state's write lease ran out by the replica's
     * clock while it is present, so its state and claim lines are outside the audited set. Internal.
     *
     * @param entryId the entry
     * @return the mark; false for an id with no slot
     */
    public boolean lapsed(EntryId entryId) {
        Slot<Object> slot = trie.get(Objects.requireNonNull(entryId, "entryId"));
        return slot != null && slot.lapsed();
    }

    /**
     * Returns the state with the entry's lapse mark set or cleared (RE-T-06). The replicated space sets it from its own
     * clock; the mark moves the stream hash and nothing else, so the text digest and every read are unchanged.
     * Internal.
     *
     * @param entryId the entry, which holds a state when the mark is set
     * @param value   whether its write lease has lapsed by the replica's clock
     * @return the changed state; this one when nothing changed or the id has no slot
     */
    public SpaceStateCrdt withLapsed(EntryId entryId, boolean value) {
        Slot<Object> slot = trie.get(Objects.requireNonNull(entryId, "entryId"));
        if (slot == null || slot.lapsed() == value) {
            return this;
        }
        return change(entryId, s -> s.withLapsed(value));
    }

    // ---- internals

    private EntryState stateOrNull(EntryId entryId) {
        Slot<Object> slot = trie.get(Objects.requireNonNull(entryId, "entryId"));
        return slot == null ? null : slot.state();
    }

    private SpaceStateCrdt update(EntryId entryId, java.util.function.UnaryOperator<EntryState> op) {
        EntryState current = stateOrNull(entryId);
        if (current == null) {
            return this;
        }
        // A null from the operator is refused, as the map it replaced refused one.
        return putState(entryId, Objects.requireNonNull(op.apply(current), "state"));
    }

    private SpaceStateCrdt putState(EntryId entryId, EntryState state) {
        int code = stateCode(state);
        return change(entryId, slot -> slot.withState(state, code));
    }

    private SpaceStateCrdt dropState(EntryId entryId) {
        return change(entryId, Slot::withoutState);
    }

    /** One slot changed by an operator, with the counts kept in step; this state when the slot is unchanged. */
    private SpaceStateCrdt change(EntryId entryId, java.util.function.UnaryOperator<Slot<Object>> op) {
        Objects.requireNonNull(entryId, "entryId");
        Slot<Object> before = trie.get(entryId);
        Slot<Object> given = before == null ? Slot.empty(entryId) : before;
        Slot<Object> after = op.apply(given);
        if (after == given) {
            return this;
        }
        Counts counts = new Counts(states, claims, sides);
        counts.change(before, after);
        return counts.of(trie.put(after));
    }

    /** The three counts, moved by each slot that changes. */
    private static final class Counts {
        int states;
        int claims;
        int sides;

        Counts(int states, int claims, int sides) {
            this.states = states;
            this.claims = claims;
            this.sides = sides;
        }

        void change(Slot<Object> before, Slot<Object> after) {
            if (before != null) {
                add(before, -1);
            }
            if (after != null && !after.isEmpty()) {
                add(after, 1);
            }
        }

        private void add(Slot<Object> slot, int sign) {
            if (slot.state() != null) {
                states += sign;
            }
            if (slot.claim() != null) {
                claims += sign;
            }
            if (slot.claim() != null || slot.droppedHash() != null || !slot.acks().isEmpty()) {
                sides += sign;
            }
        }

        SpaceStateCrdt of(HashTrie<Object> trie) {
            return trie.isEmpty() ? EMPTY : new SpaceStateCrdt(trie, states, claims, sides);
        }
    }

    /** The ids holding a state, as a read-only view over the trie. */
    private final class StateIds extends AbstractSet<EntryId> {
        @Override
        public java.util.Iterator<EntryId> iterator() {
            java.util.Iterator<Slot<Object>> slots = trie.iterator();
            return new java.util.Iterator<>() {
                private EntryId next = advance();

                private EntryId advance() {
                    while (slots.hasNext()) {
                        Slot<Object> slot = slots.next();
                        if (slot.state() != null) {
                            return slot.id();
                        }
                    }
                    return null;
                }

                @Override
                public boolean hasNext() {
                    return next != null;
                }

                @Override
                public EntryId next() {
                    if (next == null) {
                        throw new java.util.NoSuchElementException();
                    }
                    EntryId current = next;
                    next = advance();
                    return current;
                }
            };
        }

        @Override
        public int size() {
            return states;
        }

        @Override
        public boolean contains(Object o) {
            return o instanceof EntryId entryId && stateOrNull(entryId) != null;
        }
    }
}
