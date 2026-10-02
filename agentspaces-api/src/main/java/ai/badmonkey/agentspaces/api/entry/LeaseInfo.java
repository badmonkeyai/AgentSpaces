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
package ai.badmonkey.agentspaces.api.entry;

import ai.badmonkey.agentspaces.common.id.AgentId;

import java.time.Instant;
import java.util.Objects;

/**
 * The lease currently attached to an entry record (spec §7.1).
 *
 * @param holder          the agent holding the lease
 * @param expiresAtMillis lease expiry, epoch milliseconds
 * @param kind            whether this is the write lease or a take hold
 */
public record LeaseInfo(AgentId holder, long expiresAtMillis, LeaseKind kind) {

    public LeaseInfo {
        Objects.requireNonNull(holder, "holder");
        Objects.requireNonNull(kind, "kind");
        if (expiresAtMillis <= 0) {
            throw new IllegalArgumentException("expiresAtMillis must be positive: " + expiresAtMillis);
        }
    }

    /**
     * Tests whether this lease has lapsed at the given moment.
     *
     * @param now the moment to test
     * @return {@code true} when the lease expiry is at or before {@code now}
     */
    public boolean expired(Instant now) {
        return now.toEpochMilli() >= expiresAtMillis;
    }
}
