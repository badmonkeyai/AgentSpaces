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

import ai.badmonkey.agentspaces.api.space.FieldFilter;

import java.util.ArrayList;
import java.util.List;

/** Guard evaluation shared by the grammar and the templates: a type match and every clause true. */
final class Guards {

    private Guards() {
    }

    static boolean sameType(Event e, String type) {
        if (type == null || type.isBlank()) {
            return false;
        }
        return type.equals(e.type()) || type.equals(e.wireType()) || Event.simpleName(type).equals(e.type());
    }

    static boolean matches(Event e, String type, List<FieldFilter> guard) {
        if (!sameType(e, type)) {
            return false;
        }
        for (FieldFilter f : guard) {
            Object v = e.fields().get(f.field());
            if (v == null) {
                v = e.tags().get(f.field());
            }
            if (!f.matches(v)) {
                return false;
            }
        }
        return true;
    }

    static List<FieldFilter> parse(List<String> clauses) {
        List<FieldFilter> out = new ArrayList<>();
        for (String c : clauses) {
            out.add(FieldFilter.parse(c));
        }
        return out;
    }

    static boolean compare(long actual, String op, long n) {
        return switch (op) {
            case "=" -> actual == n;
            case "!=" -> actual != n;
            case "<" -> actual < n;
            case "<=" -> actual <= n;
            case ">" -> actual > n;
            case ">=" -> actual >= n;
            default -> false;
        };
    }
}
