/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package ai.badmonkey.agentspaces.constraints;

import java.time.Instant;
import java.util.List;

/** A goal posed per key (FR-2): achieve a type, optionally guarded, by a deadline. Reserved {@code Goal#v1}. */
public record Goal(int v, String book, long version, String unit, String key, String achieve, List<String> guard,
                   Instant deadline, String posedBy, String state) {

    public static final int VERSION = 1;
    public static final String OPEN = "OPEN";
    public static final String ACHIEVED = "ACHIEVED";
    public static final String STUCK = "STUCK";

    public Goal {
        guard = guard == null ? List.of() : List.copyOf(guard);
        state = state == null || state.isBlank() ? OPEN : state;
    }
}
