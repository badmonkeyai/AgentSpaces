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

import java.util.Map;

/**
 * One version's named conditions (ISSUE-Constraints FR-2), the situations a book's sentences name: {@code
 * conditions} maps a condition id to its text in the grammar of {@link ConditionGrammar}. Reserved entry type
 * {@code ai.badmonkey.agentspaces.constraints.Vocabulary#v1}; tagged {@code charter} and {@code charterVersion}.
 */
public record Vocabulary(int v, String book, long version, String hash, Map<String, String> conditions) {

    public static final int VERSION = 1;

    public Vocabulary {
        conditions = conditions == null ? Map.of() : Map.copyOf(conditions);
    }

    public Map<String, String> tags() {
        return Map.of(RuleTags.CHARTER, book, RuleTags.CHARTER_VERSION, hash);
    }
}
