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
package ai.badmonkey.agentspaces.test;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A manually advanced {@link InstantSource} for deterministic tests. Lease expiry
 * throughout AgentSpaces is driven by an injected clock, so a test advances time
 * explicitly and asserts lapse behavior with no sleeping and no flakiness.
 */
public final class TestClock implements InstantSource {

    private final AtomicReference<Instant> now;

    private TestClock(Instant start) {
        this.now = new AtomicReference<>(start);
    }

    /**
     * Creates a clock starting at the given instant.
     *
     * @param start the starting instant
     * @return the clock
     */
    public static TestClock startingAt(Instant start) {
        return new TestClock(Objects.requireNonNull(start, "start"));
    }

    /** Creates a clock starting at a fixed, arbitrary epoch instant. */
    public static TestClock create() {
        return new TestClock(Instant.ofEpochMilli(1_000_000_000_000L));
    }

    /**
     * Advances the clock.
     *
     * @param amount how far to advance; must be non-negative
     */
    public void advance(Duration amount) {
        Objects.requireNonNull(amount, "amount");
        if (amount.isNegative()) {
            throw new IllegalArgumentException("cannot advance backwards: " + amount);
        }
        now.updateAndGet(instant -> instant.plus(amount));
    }

    @Override
    public Instant instant() {
        return now.get();
    }
}
