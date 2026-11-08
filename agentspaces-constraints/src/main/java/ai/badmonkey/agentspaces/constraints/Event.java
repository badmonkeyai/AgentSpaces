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
 * fields (a record's components or a map's entries), its tags, and whether it is a write or a claim. A value whose
 * wire type is {@value #MAPPED_RECORD} and that carries the mapped shape (a {@code type} string and a {@code fields}
 * map) is typed by that logical type, see {@link #of(String, String, Instant, Object, Map, Kind)}.
 */
public record Event(String type, String wireType, String entryId, Instant issued, Map<String, Object> fields,
                    Map<String, String> tags, Kind kind) {

    public enum Kind { WRITTEN, CLAIMED }

    /**
     * The simple name of the wire type under which a writer records a type it has no class of: the value carries the
     * logical type's name as {@code type}, the record as {@code fields}, and the correlation key as {@code key}. The
     * core reads that shape from the value's map form and needs nothing from the writer's platform.
     */
    public static final String MAPPED_RECORD = "MappedRecord";

    public Event {
        // a record's component may be null (a response with no error, a message in reply to nothing): kept as null, since
        // Map.copyOf refuses one and the gate then died on every such write (found 2026-11-07 behind a model gateway)
        fields = fields == null ? Map.of() : java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(fields));
        tags = tags == null ? Map.of() : java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(tags));
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
        return of(entry.type(), entry.entryId().value(), issued, entry.value(), entry.tags(), kind);
    }

    /** An event from a value, for a result the binder is about to write. */
    public static Event of(Object value, String wireType, Map<String, String> tags, Instant at, Kind kind) {
        return of(wireType, "", at, value, tags, kind);
    }

    /**
     * An event from a wire type and a value: the type is the wire type's simple name and the fields are the value's.
     * A {@value #MAPPED_RECORD} that carries a {@code type} string and a {@code fields} map (a flow step's result, a
     * reaction's outcome) is the event of that logical type, so a rule book can name it: its fields are the mapped
     * fields with the record's {@code key} (a mapped field named {@code key} stands), and the wire type stays as it is.
     */
    public static Event of(String wireType, String entryId, Instant issued, Object value, Map<String, String> tags, Kind kind) {
        String type = simpleName(wireType);
        Map<String, Object> fields = fieldsOf(value);
        if (MAPPED_RECORD.equals(type) && fields.get("type") instanceof String logical && !logical.isBlank()
                && fields.get("fields") instanceof Map<?, ?> mapped) {
            Map<String, Object> out = fieldsOf(mapped);
            if (fields.get("key") instanceof String key && !key.isBlank()) {
                out.putIfAbsent("key", key);
            }
            return new Event(simpleName(logical), wireType, entryId, issued, out, tags, kind);
        }
        return new Event(type, wireType, entryId, issued, fields, tags, kind);
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
