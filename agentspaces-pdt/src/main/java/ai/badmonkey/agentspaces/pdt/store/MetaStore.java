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
package ai.badmonkey.agentspaces.pdt.store;

import java.util.Optional;
import java.util.Set;

/**
 * Small named records of a node store's owner, kept beside the nodes and across a restart (PDT 2
 * phase 1, P2-V-03; srad/agentspaces-pdt2.md §3.10): {@code NodeStore} keeps its pins under
 * {@code pins} and {@link Retention} keeps each ref's retained versions under
 * {@code retention:<hex of the sha-256 of the ref name>}. {@link PackNodeStore} implements it with meta records
 * in its packs; a backend without it keeps that state in memory only.
 */
public interface MetaStore {

    /**
     * The latest record of a name.
     *
     * @param name the name
     * @return the bytes, or empty when none was written
     */
    Optional<byte[]> meta(String name);

    /**
     * Replaces the record of a name.
     *
     * @param name  the name, 1 to 200 of {@code [A-Za-z0-9._:-]}
     * @param value the bytes
     */
    void putMeta(String name, byte[] value);

    /** The names of the records written. */
    Set<String> metaNames();
}
