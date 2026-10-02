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

import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * The replicated state of one entry (spec §7.3): observed-remove add/remove dot
 * sets, a last-writer-wins lease register, and a monotone completed flag. The
 * completed flag is what makes double {@code complete} impossible to merge back
 * in: once any replica records completion, every merge preserves it.
 *
 * <p>Immutable; every operation returns a new state, and {@link #merge} is
 * commutative, associative, and idempotent by construction.
 *
 * @param record    the immutable entry record as written (its lease field is
 *                  superseded by {@code lease})
 * @param adds      observed add dots
 * @param removes   observed remove dots
 * @param lease     the current lease, LWW by HLC
 * @param completed monotone completion flag
 */
public record EntryState(
        EntryRecord record,
        Set<Dot> adds,
        Set<Dot> removes,
        LwwRegister<LeaseInfo> lease,
        boolean completed) {

    public EntryState {
        Objects.requireNonNull(record, "record");
        Objects.requireNonNull(lease, "lease");
        adds = Set.copyOf(Objects.requireNonNull(adds, "adds"));
        removes = Set.copyOf(Objects.requireNonNull(removes, "removes"));
    }

    /**
     * Creates the state for a freshly written entry.
     *
     * @param record  the written record
     * @param addDot  the writing replica's dot
     * @param stamp   the write's lease register
     * @return the new state
     */
    public static EntryState added(EntryRecord record, Dot addDot, LwwRegister<LeaseInfo> stamp) {
        return new EntryState(record, Set.of(addDot), Set.of(), stamp, false);
    }

    /** Returns whether the entry is present: some add dot is unremoved and it is not completed. */
    public boolean present() {
        if (completed) {
            return false;
        }
        for (Dot add : adds) {
            if (!removes.contains(add)) {
                return true;
            }
        }
        return false;
    }

    /** Returns a state with all currently observed add dots removed (withdrawal). */
    public EntryState withObservedRemoved() {
        Set<Dot> newRemoves = new HashSet<>(removes);
        newRemoves.addAll(adds);
        return new EntryState(record, adds, newRemoves, lease, completed);
    }

    /** Returns a state marked completed. Monotone: no operation clears it. */
    public EntryState withCompleted() {
        return new EntryState(record, adds, removes, lease, true);
    }

    /**
     * Returns a state with a lease write merged in (LWW by HLC).
     *
     * @param leaseWrite the lease write
     * @return the updated state
     */
    public EntryState withLease(LwwRegister<LeaseInfo> leaseWrite) {
        return new EntryState(record, adds, removes, lease.merge(leaseWrite), completed);
    }

    /**
     * Merges two replicas' states for the same entry.
     *
     * @param other the other replica's state
     * @return the merged state
     */
    public EntryState merge(EntryState other) {
        Objects.requireNonNull(other, "other");
        Set<Dot> mergedAdds = new HashSet<>(adds);
        mergedAdds.addAll(other.adds);
        Set<Dot> mergedRemoves = new HashSet<>(removes);
        mergedRemoves.addAll(other.removes);
        return new EntryState(record, mergedAdds, mergedRemoves,
                lease.merge(other.lease), completed || other.completed);
    }
}
