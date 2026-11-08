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

import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.Link;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/** Builds small blob trees (kind {@code b} over raw chunks) for the store tests. */
final class TreeFixtures {

    private TreeFixtures() {
    }

    /** Random bytes of a size. */
    static byte[] chunk(Random random, int size) {
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        return bytes;
    }

    /** Encodes a {@code b} node over children with their sizes. */
    static byte[] blobNode(int height, List<String> children, List<Long> sizes) {
        List<Object> links = new ArrayList<>();
        for (String child : children) {
            links.add(new Link(child));
        }
        List<Object> z = new ArrayList<>(sizes);
        return NodeCodec.encode(Map.of("k", "b", "h", height, "c", links, "z", z));
    }

    /** Stores a chunk and returns its CID. */
    static String putChunk(NodeStore store, byte[] bytes) {
        return store.put(bytes, Cids.RAW);
    }

    /** Stores a {@code b} node over children and returns its CID. */
    static String putParent(NodeStore store, int height, List<String> children) {
        List<Long> sizes = new ArrayList<>();
        for (int i = 0; i < children.size(); i++) {
            sizes.add(1L);
        }
        return store.put(blobNode(height, children, sizes), Cids.DAG_CBOR);
    }

    /**
     * Stores a two-level tree of {@code leaves} random chunks under {@code fanout}-wide
     * parents and one root; returns the root.
     */
    static String putTree(NodeStore store, Random random, int leaves, int fanout, int chunkSize) {
        List<String> parents = new ArrayList<>();
        List<String> batch = new ArrayList<>();
        for (int i = 0; i < leaves; i++) {
            batch.add(putChunk(store, chunk(random, chunkSize)));
            if (batch.size() == fanout) {
                parents.add(putParent(store, 1, batch));
                batch = new ArrayList<>();
            }
        }
        if (!batch.isEmpty()) {
            parents.add(putParent(store, 1, batch));
        }
        return putParent(store, 2, parents);
    }
}
