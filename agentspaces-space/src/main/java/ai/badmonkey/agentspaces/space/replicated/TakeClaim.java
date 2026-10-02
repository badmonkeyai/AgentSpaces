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
package ai.badmonkey.agentspaces.space.replicated;

import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.SpaceId;

import java.util.Objects;

/**
 * A take claim (spec §7.4) as a join-semilattice value, so replicas that merge
 * the same claims agree on the winner with no coordination. The same lattice
 * carries both strategies: under {@code LEASE_RACE} every bid is zero and the
 * arbitration is by stamp; under {@code AUCTION} the bid is the claimant's cost
 * and the lowest bid wins, which turns the space into a decentralized
 * least-cost allocator.
 *
 * <p>The lattice order: a higher {@code epoch} always supersedes (an epoch
 * increments when a lapsed claim's entry is re-claimed); within an epoch the
 * lowest bid wins; among equal bids the lowest {@code (stamp, holder)} wins,
 * which is the spec's deterministic arbitration; and among claims by the same
 * holder the later expiry survives, which is how renewal works.
 *
 * <p>The claim is bound to its entry: {@code entryId} and {@code spaceId} are
 * signed components, so a signed claim cannot be transplanted onto a different
 * entry (the {@code claimEntry} map key it travels under is unsigned relay
 * context, and was the only binding before). A verifier that authenticates a
 * claim also checks these two fields against the entry it is being applied to.
 *
 * @param entryId         the entry this claim is for (a signed binding)
 * @param spaceId         the space the entry lives in (a signed binding)
 * @param epoch           the claim generation for its entry; starts at 1
 * @param stamp           the claimant's HLC stamp
 * @param holder          the claiming agent
 * @param bid             the claimant's cost; 0 under LEASE_RACE, lower wins
 * @param expiresAtMillis when this claim's TAKE hold lapses
 */
public record TakeClaim(EntryId entryId, SpaceId spaceId, long epoch, HlcTimestamp stamp,
                        AgentId holder, double bid, long expiresAtMillis) {

    public TakeClaim {
        Objects.requireNonNull(entryId, "entryId");
        Objects.requireNonNull(spaceId, "spaceId");
        Objects.requireNonNull(stamp, "stamp");
        Objects.requireNonNull(holder, "holder");
        if (epoch <= 0) {
            throw new IllegalArgumentException("epoch must be positive: " + epoch);
        }
        if (Double.isNaN(bid) || Double.isInfinite(bid)) {
            throw new IllegalArgumentException("bid must be finite: " + bid);
        }
    }

    /**
     * Merges two claims for the same entry by a total preference order: higher
     * epoch first; within an epoch the lower {@code (stamp, holder)} (the spec's
     * deterministic arbitration); for the same holder and stamp, the later expiry
     * (renewal). A total order makes the merge commutative, associative, and
     * idempotent, and the winner is always one of the inputs, so a claim's
     * holder signature stays attached to it through any merge.
     *
     * @param a one claim; may be {@code null}
     * @param b the other claim; may be {@code null}
     * @return the winning claim (always {@code a} or {@code b})
     */
    public static TakeClaim merge(TakeClaim a, TakeClaim b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        if (a.epoch != b.epoch) {
            return a.epoch > b.epoch ? a : b;
        }
        int byBid = Double.compare(a.bid, b.bid);
        if (byBid != 0) {
            return byBid < 0 ? a : b;
        }
        int byStamp = a.stamp.compareTo(b.stamp);
        if (byStamp != 0) {
            return byStamp < 0 ? a : b;
        }
        if (!a.holder.equals(b.holder)) {
            return a.holder.encoded().compareTo(b.holder.encoded()) <= 0 ? a : b;
        }
        return a.expiresAtMillis >= b.expiresAtMillis ? a : b;
    }

    /**
     * Tests whether this claim's hold has lapsed.
     *
     * @param nowMillis the current time, epoch milliseconds
     * @return {@code true} when lapsed
     */
    public boolean expired(long nowMillis) {
        return nowMillis >= expiresAtMillis;
    }
}
