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


package ai.badmonkey.agentspaces.guardrail;

import java.util.List;
import java.util.Map;

/**
 * The checks a group applies (FR-4): in order, each naming its provider, kind, where it applies ({@code gateway}
 * or {@code door}), the action on a hit ({@code refuse}, {@code redact}, {@code quarantine}, {@code finding}), and
 * open options ({@code classes}, {@code spaces}, {@code threshold}, {@code deny}, {@code allow}, {@code fields}).
 * {@code onError} is what a throwing or slow provider yields: {@code refuse} or {@code pass}.
 */
public record GuardrailPolicy(int schemaVersion, String group, long version, List<Check> checks, String onError) {

    public static final int VERSION = 1;
    public static final String GATEWAY = "gateway";
    public static final String DOOR = "door";
    public static final String REFUSE = "refuse";
    public static final String PASS = "pass";
    public static final String REDACT = "redact";
    public static final String QUARANTINE = "quarantine";
    public static final String FINDING = "finding";

    public record Check(String provider, String kind, String where, String action, Map<String, String> options) {
        public Check {
            provider = provider == null || provider.isBlank() ? "builtin" : provider;
            kind = kind == null ? "" : kind;
            where = where == null || where.isBlank() ? GATEWAY : where;
            action = action == null || action.isBlank() ? REFUSE : action;
            options = options == null ? Map.of() : Map.copyOf(options);
        }

        /** The policy's spaces option for a door check, comma-separated; empty means every space the door watches. */
        public List<String> spaces() {
            String s = options.get("spaces");
            return s == null || s.isBlank() ? List.of() : List.of(s.split(","));
        }
    }

    public GuardrailPolicy {
        group = group == null ? "" : group;
        checks = checks == null ? List.of() : List.copyOf(checks);
        onError = onError == null || onError.isBlank() ? REFUSE : onError;
    }

    public List<Check> at(String where) {
        return checks.stream().filter(c -> where.equals(c.where())).toList();
    }
}
