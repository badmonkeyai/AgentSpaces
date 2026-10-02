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
package ai.badmonkey.agentspaces.space.local;

import ai.badmonkey.agentspaces.api.spi.SchemaRegistry;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The default schema registry: names a type {@code <fully.qualified.Name>#v1}.
 * Writer/reader schema evolution (spec §14) will refine this; the name format is
 * already versioned so that refinement stays compatible.
 */
public final class SimpleSchemaRegistry implements SchemaRegistry {

    private static final String VERSION_SUFFIX = "#v1";

    private final Map<String, Class<?>> byName = new ConcurrentHashMap<>();

    @Override
    public String register(Class<?> entryType) {
        Objects.requireNonNull(entryType, "entryType");
        String name = entryType.getName() + VERSION_SUFFIX;
        byName.putIfAbsent(name, entryType);
        return name;
    }

    @Override
    public String schemaNameOf(Class<?> entryType) {
        Objects.requireNonNull(entryType, "entryType");
        String name = entryType.getName() + VERSION_SUFFIX;
        if (!byName.containsKey(name)) {
            throw new IllegalArgumentException("unregistered entry type: " + entryType.getName());
        }
        return name;
    }

    @Override
    public Optional<Class<?>> classFor(String schemaName) {
        return Optional.ofNullable(byName.get(schemaName));
    }
}
