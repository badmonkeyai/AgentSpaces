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
package ai.badmonkey.agentspaces.pdt.refs;

import java.util.Objects;

/**
 * A key changed on both sides of a merge to different values, or a whole head
 * taken over another (srad/agentspaces-pdt.md §3.6.1, PD-R-05, PD-R-06). A
 * reader records one event per decision, delivers it to the listeners of
 * {@link Ref#onConflict}, and counts it in {@code Pdt.stats().conflicts()}.
 *
 * @param name       the ref name
 * @param key        the key the decision was about, or null when a whole head was taken
 * @param winnerRoot the root of the head whose value was kept
 * @param loserRoot  the root of the head whose value was dropped
 * @param reason     why: the later issued HLC, a registered merge function, or the
 *                   missing common base of PD-R-06
 */
public record PConflict(String name, String key, String winnerRoot, String loserRoot, String reason) {

    /** The reason when both sides changed a key and the later issued head took it. */
    public static final String LATER_HLC = "both sides changed the key; the head with the later issued HLC wins";

    /** The reason when a vector or blob has no registered merge function. */
    public static final String WHOLE_HEAD = "the kind merges by whole head; the head with the later issued HLC wins";

    /** The reason when the base walk of PD-R-06 found no common base within its bound. */
    public static final String NO_COMMON_BASE = "no common base within 64 steps or a base not held; the later head wins whole";

    /** The reason when a registered merge function threw or returned null, so the rule of §3.6.1 decided instead. */
    public static final String MERGE_FUNCTION_FAILED = "the registered merge function failed; the rule of 3.6.1 decides";

    public PConflict {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(winnerRoot, "winnerRoot");
        Objects.requireNonNull(loserRoot, "loserRoot");
        Objects.requireNonNull(reason, "reason");
    }
}
