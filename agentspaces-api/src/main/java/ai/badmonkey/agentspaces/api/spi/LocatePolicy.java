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

import java.time.Duration;
import java.util.Optional;

/**
 * How a node treats a locator's answers (SRAD DB-N-03, DB-C-07, DB-C-10,
 * DB-C-12): how often at least and at most it re-asks the locator while its
 * view of the group is empty, whether a first answer with nothing in it
 * fails the join, and whether every locate is traced at INFO. A locator
 * declares its policy through {@link BeaconLocator#policy()}; a chain built
 * from {@code agentspaces.bootstrap.*} carries the operator's values, and
 * {@link #DEFAULT} leaves everything to the node's own settings.
 *
 * @param minInterval the re-ask floor, or empty for the node's default
 * @param maxInterval the re-ask ceiling, or empty for the node's default
 * @param require     fail the join when the first locate answers nothing
 * @param trace       log every locate's rows at INFO, not only the first and the changes
 */
public record LocatePolicy(Optional<Duration> minInterval, Optional<Duration> maxInterval, boolean require,
                           boolean trace) {

    /** The node's own settings, no requirement, no extra tracing. */
    public static final LocatePolicy DEFAULT = new LocatePolicy(Optional.empty(), Optional.empty(), false, false);

    public LocatePolicy {
        minInterval = minInterval == null ? Optional.empty() : minInterval;
        maxInterval = maxInterval == null ? Optional.empty() : maxInterval;
    }

    /** A policy with the given pacing. */
    public static LocatePolicy of(Duration minInterval, Duration maxInterval) {
        return new LocatePolicy(Optional.ofNullable(minInterval), Optional.ofNullable(maxInterval), false, false);
    }

    public LocatePolicy withRequire(boolean value) {
        return new LocatePolicy(minInterval, maxInterval, value, trace);
    }

    public LocatePolicy withTrace(boolean value) {
        return new LocatePolicy(minInterval, maxInterval, require, value);
    }
}
