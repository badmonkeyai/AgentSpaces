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
package ai.badmonkey.agentspaces.discovery;

import java.util.concurrent.CompletableFuture;

/**
 * Where a publisher puts a stub card's parts document and a reader fetches it (SPEC §6.1a.5, v0.1.32;
 * ISSUE-AppCardOriginFormats §9.8; AppCard origin and formats, 2026-11-09): a content swarm that serves the group.
 * {@code ContentSpillStore} in {@code agentspaces-content} implements it; {@link DiscoveryService#spillStore} installs
 * it, and with none installed a publisher never spills and {@link DiscoveryService#resolve} fails a stub.
 */
public interface SpillStore {

    /**
     * Publishes a parts document as a content-swarm artifact in {@code CardSpill.PIECE_SIZE} pieces, with manifest
     * {@code name}, media type {@code application/cbor}, and sealing {@code none}, and keeps it served until
     * {@link #release}. The same bytes give the same root, and the store keeps one artifact for them.
     *
     * @param name     the manifest's name, the card's id
     * @param document the parts document's canonical bytes
     * @return the artifact's content root
     */
    CompletableFuture<String> put(String name, byte[] document);

    /**
     * Fetches a parts document from its holders, every piece verified against the root. The future fails with an
     * {@link IllegalArgumentException} when the artifact's length is not {@code size}, which a store finds from the
     * artifact's manifest before it fetches a piece, so a stub cannot make a reader fetch more than it states; and it
     * fails with a {@link java.util.concurrent.TimeoutException} when no holder serves the document within the store's
     * deadline, so a document nobody serves never holds a reader (AppCard origin and formats, 2026-11-09).
     *
     * @param root the content root the stub names
     * @param size the length the stub states
     * @return the document's bytes
     */
    CompletableFuture<byte[]> get(String root, long size);

    /**
     * Stops keeping a root served on behalf of the cards that named it, so the artifact lapses.
     *
     * @param root the content root
     */
    void release(String root);
}
