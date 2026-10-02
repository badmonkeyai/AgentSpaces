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
package ai.badmonkey.agentspaces.api.space;

/**
 * The conflict-resolution strategies a space may declare for exclusive
 * {@code take} (spec §7.4). A space fixes one strategy for all of its entries;
 * applications needing several semantics use several spaces.
 */
public enum ConflictStrategyType {

    /**
     * Default. Takers race claims and a gossip settle-window arbitrates
     * deterministically by lowest {@code (hlc, peerId)}. At most one take completes
     * per entry; transient duplicate work is possible during partitions. Suits
     * idempotent tasks.
     */
    LEASE_RACE,

    /**
     * Cost-aware allocation: takes carry bids, a CBAA/CBBA-style auction converges
     * to a conflict-free least-cost assignment in O(diameter) gossip rounds.
     */
    AUCTION,

    /**
     * Strict serialization through an ordered-log capability (a group-scoped Raft
     * quorum). Exactly-once takes at the price of quorum liveness; reserve it for
     * non-idempotent, high-stakes entries (spec P1).
     */
    ORDERED
}
