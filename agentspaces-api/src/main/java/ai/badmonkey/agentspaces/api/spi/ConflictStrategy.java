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
package ai.badmonkey.agentspaces.api.spi;

import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.space.ConflictStrategyType;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.common.id.AgentId;

import java.util.concurrent.CompletionStage;

/**
 * The exclusive-take arbitration SPI (spec §7.4). Exclusive take is the one
 * operation CRDTs cannot express, so it is the strategy point: a replicated space
 * delegates each take claim here, and the strategy decides, after its own protocol
 * (settle-window race, auction rounds, ordered log), whether this claimant holds
 * the entry.
 *
 * <p>Contract stability: SPI. The signature is expected to grow a bid parameter
 * when the AUCTION strategy lands (plan M2); implementors should extend
 * this interface rather than depend on its exact shape staying fixed.
 */
public interface ConflictStrategy {

    /** Returns which declared strategy this implementation provides. */
    ConflictStrategyType type();

    /**
     * Claims an exclusive take on an entry.
     *
     * @param entry     the entry record being claimed
     * @param claimant  the agent claiming the take
     * @param takeLease the TAKE lease requested
     * @return a stage resolving {@code true} when the claim is awarded to this
     *         claimant, {@code false} when another claimant holds the entry
     */
    CompletionStage<Boolean> claim(EntryRecord entry, AgentId claimant, Lease takeLease);
}
