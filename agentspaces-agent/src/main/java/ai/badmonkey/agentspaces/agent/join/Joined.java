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
package ai.badmonkey.agentspaces.agent.join;

import ai.badmonkey.agentspaces.api.space.Space;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

/**
 * The parts of one case, as handed to a {@code @SpaceJoin} method (issue #16):
 * the key they share and, per part type, the matching entries newest first.
 * {@link #get(Class)} is the common case, the newest part of a type;
 * {@link #find(Class)} is for optional parts; {@link #all(Class)} for every
 * matching entry; {@link #entry(Class)} when the metadata (issuer, tags, lease)
 * matters.
 */
public final class Joined {

    private final String key;
    private final Map<Class<?>, List<Space.Entry<?>>> parts;

    Joined(String key, Map<Class<?>, List<Space.Entry<?>>> parts) {
        this.key = Objects.requireNonNull(key, "key");
        Map<Class<?>, List<Space.Entry<?>>> copy = new LinkedHashMap<>();
        parts.forEach((type, entries) -> copy.put(type, List.copyOf(entries)));
        this.parts = Collections.unmodifiableMap(copy);
    }

    /** The key the parts share. */
    public String key() {
        return key;
    }

    /**
     * The newest part of a type.
     *
     * @throws NoSuchElementException when no part of the type is present (an
     *                                optional part that did not arrive; use {@link #find(Class)})
     */
    public <P> P get(Class<P> type) {
        return find(type).orElseThrow(() -> new NoSuchElementException(
                "no " + type.getSimpleName() + " part for key '" + key + "'; present: "
                        + parts.keySet().stream().map(Class::getSimpleName).toList()));
    }

    /** The newest part of a type, or empty when none arrived. */
    public <P> Optional<P> find(Class<P> type) {
        List<Space.Entry<?>> entries = parts.get(type);
        return entries == null || entries.isEmpty()
                ? Optional.empty() : Optional.of(type.cast(entries.get(0).value()));
    }

    /** Every part of a type, newest first; empty when none arrived. */
    public <P> List<P> all(Class<P> type) {
        List<Space.Entry<?>> entries = parts.get(type);
        if (entries == null) {
            return List.of();
        }
        List<P> values = new ArrayList<>(entries.size());
        for (Space.Entry<?> entry : entries) {
            values.add(type.cast(entry.value()));
        }
        return values;
    }

    /** Whether at least one part of the type is present. */
    public boolean has(Class<?> type) {
        List<Space.Entry<?>> entries = parts.get(type);
        return entries != null && !entries.isEmpty();
    }

    /** The newest part of a type with its metadata. */
    @SuppressWarnings("unchecked")
    public <P> Space.Entry<P> entry(Class<P> type) {
        List<Space.Entry<?>> entries = parts.get(type);
        if (entries == null || entries.isEmpty()) {
            throw new NoSuchElementException("no " + type.getSimpleName() + " part for key '" + key + "'");
        }
        return (Space.Entry<P>) entries.get(0);
    }

    /** Every part with its metadata, by type, newest first. */
    public Map<Class<?>, List<Space.Entry<?>>> parts() {
        return parts;
    }

    @Override
    public String toString() {
        return "Joined[" + key + ", parts=" + parts.keySet().stream().map(Class::getSimpleName).toList() + "]";
    }
}
