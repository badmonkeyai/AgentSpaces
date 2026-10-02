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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
 * ({@link #gc}) drops entries that are no longer live once twice the maximum
 * lease has passed since their lease expired, and leaves merge semantics
 * unchanged.
 */
public final class SpaceStateCrdt {

    private static final SpaceStateCrdt EMPTY = new SpaceStateCrdt(Map.of());

    private final Map<EntryId, EntryState> entries;

    private SpaceStateCrdt(Map<EntryId, EntryState> entries) {
        this.entries = Map.copyOf(entries);
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
        return new SpaceStateCrdt(Map.of(
                Objects.requireNonNull(entryId, "entryId"),
                Objects.requireNonNull(state, "state")));
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
        Map<EntryId, EntryState> updated = new HashMap<>(entries);
        updated.merge(record.entryId(), EntryState.added(record, addDot, lease), EntryState::merge);
        return new SpaceStateCrdt(updated);
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
        Map<EntryId, EntryState> merged = new HashMap<>(entries);
        for (Map.Entry<EntryId, EntryState> e : other.entries.entrySet()) {
            merged.merge(e.getKey(), e.getValue(), EntryState::merge);
        }
        return new SpaceStateCrdt(merged);
    }

    /** Returns the records of currently present entries. */
    public List<EntryRecord> liveRecords() {
        return entries.values().stream()
                .filter(EntryState::present)
                .map(state -> state.record().withLease(state.lease().value()))
                .toList();
    }

    /**
     * Returns one entry's state.
     *
     * @param entryId the entry
     * @return the state, when known to this replica
     */
    public Optional<EntryState> state(EntryId entryId) {
        return Optional.ofNullable(entries.get(entryId));
    }

    /** Returns the number of entries this replica knows about, tombstones included. */
    public int knownEntries() {
        return entries.size();
    }

    /**
     * Whether one entry's state is a collectable tombstone (spec §7.3): the
     * entry is no longer live (completed, withdrawn, or its write lease has
     * lapsed) and its lease expired at least twice the maximum lease ago, so
     * every replica that could still learn of the transition has had two full
     * lease periods to do so.
     *
     * @param state          the entry's state
     * @param nowMillis      the current time, epoch milliseconds
     * @param maxLeaseMillis the longest lease observed for the space
     * @return {@code true} when the entry may be dropped
     */
    public static boolean collectable(EntryState state, long nowMillis, long maxLeaseMillis) {
        Objects.requireNonNull(state, "state");
        long leaseExpiry = state.lease().value().expiresAtMillis();
        if (state.present() && leaseExpiry > nowMillis) {
            return false;
        }
        long horizon = leaseExpiry + 2 * Math.max(0, maxLeaseMillis);
        return horizon <= nowMillis && horizon >= leaseExpiry; // guard overflow
    }

    /**
     * Returns the ids of entries {@link #collectable} would drop now.
     *
     * @param nowMillis      the current time, epoch milliseconds
     * @param maxLeaseMillis the longest lease observed for the space
     * @return the collectable entry ids
     */
    public Set<EntryId> collectable(long nowMillis, long maxLeaseMillis) {
        Set<EntryId> ids = new HashSet<>();
        for (Map.Entry<EntryId, EntryState> e : entries.entrySet()) {
            if (collectable(e.getValue(), nowMillis, maxLeaseMillis)) {
                ids.add(e.getKey());
            }
        }
        return ids;
    }

    /**
     * Garbage-collects tombstones (spec §7.3): drops every entry that is no
     * longer live and whose lease expired more than twice the maximum lease
     * ago. Merge semantics are unchanged; a collected tombstone re-offered by a
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
     * Returns a state without the given entries.
     *
     * @param ids the entries to drop
     * @return the reduced state; {@code this} when none were known
     */
    public SpaceStateCrdt without(Set<EntryId> ids) {
        Objects.requireNonNull(ids, "ids");
        if (ids.isEmpty()) {
            return this;
        }
        Map<EntryId, EntryState> reduced = new HashMap<>(entries);
        boolean changed = reduced.keySet().removeAll(ids);
        return changed ? new SpaceStateCrdt(reduced) : this;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof SpaceStateCrdt other && entries.equals(other.entries);
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }

    @Override
    public String toString() {
        return "SpaceStateCrdt[known=" + entries.size() + ", live=" + liveRecords().size() + "]";
    }

    private SpaceStateCrdt update(EntryId entryId, java.util.function.UnaryOperator<EntryState> op) {
        Objects.requireNonNull(entryId, "entryId");
        EntryState current = entries.get(entryId);
        if (current == null) {
            return this;
        }
        Map<EntryId, EntryState> updated = new HashMap<>(entries);
        updated.put(entryId, op.apply(current));
        return new SpaceStateCrdt(updated);
    }
}
