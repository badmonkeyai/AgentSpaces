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
import java.util.Map;

/**
 * Reserved for the jury (FR-11): the verdict on a juried unit for one key. The gate admits on {@code allow},
 * refuses otherwise; the proposer that writes it arrives with a later spiral.
 */
public record JuryDecision(int v, String book, long version, String unit, String key, String motion, String verdict,
                           List<Map<String, Object>> ballots, Instant at) {

    public static final int VERSION = 1;
    public static final String ALLOW = "allow";
    public static final String REFUSE = "refuse";

    public JuryDecision {
        ballots = ballots == null ? List.of() : List.copyOf(ballots);
    }
}
