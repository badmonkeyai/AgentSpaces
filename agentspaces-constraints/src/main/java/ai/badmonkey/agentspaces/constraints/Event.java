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

import ai.badmonkey.agentspaces.api.space.Space;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One event of a key in issue order (FR-6): the entry's simple and wire type, its id, when it was issued, its
 * fields (a record's components or a map's entries), its tags, and whether it is a write or a claim.
 */
public record Event(String type, String wireType, String entryId, Instant issued, Map<String, Object> fields,
                    Map<String, String> tags, Kind kind) {

    public enum Kind { WRITTEN, CLAIMED }

    public Event {
        fields = fields == null ? Map.of() : Map.copyOf(fields);
        tags = tags == null ? Map.of() : Map.copyOf(tags);
    }

    /** The simple name of a wire type: {@code a.b.Outer$Claim#v1} is {@code Claim}. */
    public static String simpleName(String wire) {
        if (wire == null) {
            return "";
        }
        String t = wire.contains("#") ? wire.substring(0, wire.indexOf('#')) : wire;
        int i = Math.max(t.lastIndexOf('.'), t.lastIndexOf('$'));
        return i < 0 ? t : t.substring(i + 1);
    }

    /** An event from an entry view. */
    public static Event of(Space.Entry<?> entry, Kind kind, Instant fallback) {
        Instant issued = entry.issued() == null ? fallback : Instant.ofEpochMilli(entry.issued().physical());
        return new Event(simpleName(entry.type()), entry.type(), entry.entryId().value(), issued, fieldsOf(entry.value()),
                entry.tags(), kind);
    }

    /** An event from a value, for a result the binder is about to write. */
    public static Event of(Object value, String wireType, Map<String, String> tags, Instant at, Kind kind) {
        return new Event(simpleName(wireType), wireType, "", at, fieldsOf(value), tags, kind);
    }

    /** The key's value as the constraint names it: {@code field:<name>} or {@code tag:<name>}. */
    public String keyValue(String key) {
        if (key == null || key.isBlank()) {
            return null;
        }
        if (key.startsWith("tag:")) {
            return tags.get(key.substring(4));
        }
        String name = key.startsWith("field:") ? key.substring(6) : key;
        Object v = fields.get(name);
        if (v == null && tags.containsKey(name)) {
            return tags.get(name);
        }
        return v == null ? null : String.valueOf(v);
    }

    /** A record's components or a map's entries as fields; anything else has none. */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> fieldsOf(Object value) {
        if (value instanceof Map<?, ?> m) {
            Map<String, Object> out = new LinkedHashMap<>();
            m.forEach((k, v) -> out.put(String.valueOf(k), v));
            return out;
        }
        if (value != null && value.getClass().isRecord()) {
            Map<String, Object> out = new LinkedHashMap<>();
            for (var c : value.getClass().getRecordComponents()) {
                try {
                    var accessor = c.getAccessor();
                    if (!accessor.canAccess(value)) {
                        accessor.trySetAccessible(); // a record nested in a package-private class
                    }
                    out.put(c.getName(), accessor.invoke(value));
                } catch (ReflectiveOperationException | RuntimeException ignored) {
                    // the component stays absent
                }
            }
            return out;
        }
        return Map.of();
    }

    /** Issue order with ties by entry id (FR-6). */
    public static List<Event> ordered(List<Event> events) {
        return events.stream().sorted(java.util.Comparator.comparing(Event::issued).thenComparing(Event::entryId)).toList();
    }
}
