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

import java.util.Objects;

/**
 * A dot: one replica's uniquely numbered event, the unit of causality in the
 * OR-Set (spec §7.3). A replica never reuses a counter value.
 *
 * @param replica the replica identifier
 * @param counter the replica-local event number; positive and never reused
 */
public record Dot(String replica, long counter) {

    public Dot {
        Objects.requireNonNull(replica, "replica");
        if (counter <= 0) {
            throw new IllegalArgumentException("counter must be positive: " + counter);
        }
    }
}
