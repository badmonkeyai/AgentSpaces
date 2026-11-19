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
package ai.badmonkey.agentspaces.pdt;

import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.peering.blocks.MemoryBlockStore;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.InstantSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A held node whose bytes do not hash to its CID (the PDT second-swap ticket, 2026-11-17): it used to throw from every
 * walk, so every tick of a peer holding one failed for as long as the process ran. It is dropped, counted and read as
 * not held, and the real node, fetched again and verified, takes its place.
 */
class NodeStoreCorruptNodeTest {

    @Test
    void aCorruptHeldNodeIsDroppedCountedAndReadAsMissingThenRepairedByAVerifiedPut() {
        MemoryBlockStore backend = new MemoryBlockStore();
        NodeStore store = new NodeStore(backend, InstantSource.system());
        byte[] node = "the real node".getBytes(StandardCharsets.UTF_8);
        String cid = Cids.of(node, Cids.RAW);
        // bytes that do not hash to the CID, held under it (what a second store on a shared directory left behind)
        backend.put(cid, "other bytes".getBytes(StandardCharsets.UTF_8), 0);

        assertThat(store.stored(cid)).as("a corrupt node reads as not held").isEmpty();
        assertThat(store.complete(cid)).as("a walk sees it missing rather than throwing").isFalse();
        assertThat(store.corruptDropped()).isEqualTo(1);
        assertThat(backend.cids()).as("the corrupt bytes are gone from the backend").doesNotContain(cid);

        store.putVerified(cid, node);
        assertThat(store.stored(cid)).hasValueSatisfying(b -> assertThat(b).isEqualTo(node));
        assertThat(store.complete(cid)).isTrue();
        assertThat(store.corruptDropped()).isEqualTo(1);
    }
}
