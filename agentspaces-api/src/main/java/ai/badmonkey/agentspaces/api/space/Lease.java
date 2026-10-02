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

import java.time.Duration;
import java.util.Objects;

/**
 * A requested lease duration. Everything shared in AgentSpaces is leased (spec P2):
 * entries, takes, subscriptions, advertisements, and memberships all carry a TTL and
 * vanish unless their owner renews them. There is deliberately no unbounded lease;
 * an owner that wants state to live keeps renewing it, and failure handling reduces
 * to the absence of renewal.
 *
 * @param duration the requested lease duration; must be positive
 */
public record Lease(Duration duration) {

    public Lease {
        Objects.requireNonNull(duration, "duration");
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException("lease duration must be positive: " + duration);
        }
    }

    /**
     * Creates a lease of the given duration.
     *
     * @param duration the requested duration; must be positive
     * @return the lease
     */
    public static Lease of(Duration duration) {
        return new Lease(duration);
    }
}
