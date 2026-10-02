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

import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;

import java.util.Objects;

/**
 * A last-writer-wins register ordered by hybrid logical clock (spec §7.3). The HLC
 * total order (physical, logical, node) makes the merge deterministic across the
 * whole fleet, which is exactly what {@code LEASE_RACE} arbitration needs.
 *
 * @param stamp the write's HLC timestamp
 * @param value the written value
 * @param <V>   the value type
 */
public record LwwRegister<V>(HlcTimestamp stamp, V value) {

    public LwwRegister {
        Objects.requireNonNull(stamp, "stamp");
        Objects.requireNonNull(value, "value");
    }

    /**
     * Merges two registers: the higher-stamped write survives. Commutative,
     * associative, and idempotent because the HLC order is total.
     *
     * @param other the other register; may be {@code null}
     * @return the surviving register
     */
    public LwwRegister<V> merge(LwwRegister<V> other) {
        if (other == null) {
            return this;
        }
        return stamp.compareTo(other.stamp) >= 0 ? this : other;
    }
}
