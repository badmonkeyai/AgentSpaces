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
package ai.badmonkey.agentspaces.content;

import ai.badmonkey.agentspaces.agent.content.Manifest;

/**
 * A read-only backend that serves artifacts from where they already lie: files on
 * this host ({@link InPlaceStore}) or objects behind a remote service, read by
 * range. The swarm never writes to it. A host hashes the artifact, then calls
 * {@link Content#publishExternal(ExternalStore, String, ContentRoots.Hashed, PublishOptions)},
 * which builds the manifest and registers it here under the host's key; every piece
 * is hashed whole at its first serve ({@link PieceStore#external()}).
 */
public interface ExternalStore extends PieceStore {

    /**
     * Registers an artifact as complete under a key this backend understands: a
     * file path, an object key.
     *
     * @param manifest the manifest the swarm built for the artifact
     * @param key      where the artifact lies, in this backend's terms
     */
    void register(Manifest manifest, String key);

    @Override
    default boolean external() {
        return true;
    }
}
