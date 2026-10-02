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
package ai.badmonkey.agentspaces.api.spi;

import java.util.Optional;

/**
 * Maps entry classes to stable schema names and back (spec §7.1, P5). The schema
 * name travels in every {@code EntryRecord}, so mixed-version fleets can name the
 * shape of data without exchanging classes. Schema evolution across versions is an
 * open spec question (§14); this SPI is where the answer will land.
 */
public interface SchemaRegistry {

    /**
     * Registers an entry type and returns its schema name. Idempotent.
     *
     * @param entryType the entry class
     * @return the schema name
     */
    String register(Class<?> entryType);

    /**
     * Returns the schema name of a registered type.
     *
     * @param entryType the entry class
     * @return the schema name
     * @throws IllegalArgumentException if the type was never registered
     */
    String schemaNameOf(Class<?> entryType);

    /**
     * Resolves a schema name back to a registered class, when this JVM knows it.
     *
     * @param schemaName the schema name
     * @return the class, or empty when unknown here
     */
    Optional<Class<?>> classFor(String schemaName);
}
