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
package ai.badmonkey.agentspaces.content.tree;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * What the tree exchange needs from a node store (srad/agentspaces-pdt.md §3.4,
 * §3.5, §6.9): nodes by CID, the links of a node, and the completeness of a root.
 * The content module serves {@code TREE_WANT} and {@code NODE_WANT} (PD-X-03) and
 * verifies {@code NODES} answers (PD-X-04) through this interface alone, so it
 * never depends on {@code agentspaces-pdt}; the pdt module adapts its
 * {@code NodeStore} to it. Every CID is the string form {@link Cids} produces.
 *
 * <p>Implementations are called from the swarm's handler threads and must be
 * safe for concurrent use.
 */
public interface TreeStore {

    /**
     * Whether a node is held, without reading it.
     *
     * @param cid the node's CID
     * @return true when {@link #get} would return the bytes
     */
    boolean has(String cid);

    /**
     * Reads a held node.
     *
     * @param cid the node's CID
     * @return the node bytes, or empty when the node is not held
     */
    Optional<byte[]> get(String cid);

    /**
     * Stores a node the caller has already verified against its CID (PD-X-04:
     * the exchange recomputes the CID through {@link Cids} before calling this).
     *
     * @param cid   the node's CID
     * @param bytes the node bytes
     * @throws StoreRefusedException when the store refuses the node, past a quota for one; the fetch that
     *                               received it ends with the refusal (PDT 2 phase 7, P2-X-08)
     */
    void putVerified(String cid, byte[] bytes);

    /**
     * The links of a node, in the node's field order, so a breadth-first walk
     * (PD-X-03) and the verification of a child against its parent (PD-X-04)
     * share one reading of the node format. A raw chunk has no links.
     *
     * @param cid   the node's CID
     * @param bytes the node bytes
     * @return the CIDs the node links to, empty for a leaf
     * @throws IllegalArgumentException when the bytes are not a well-formed node
     */
    List<String> links(String cid, byte[] bytes);

    /**
     * Whether every node reachable from a root is held (PD-S-06).
     *
     * @param root the root CID
     * @return true when the root and its whole subtree are held
     */
    boolean complete(String root);

    /**
     * The CIDs reachable from a root through held nodes that are not held, so a
     * fetcher continues from them (S3 of §6.4).
     *
     * @param root the root CID
     * @return the missing CIDs; empty when {@link #complete} is true
     */
    Set<String> missing(String root);

    /**
     * Every CID reachable from a root through held nodes, the root included when
     * held. A holder computes this once per {@code haves} root to prune a walk
     * (PD-X-03), bounded by the walk cap of PD-P-02.
     *
     * @param root the root CID
     * @return the reachable held CIDs; empty when the root is not held
     */
    Set<String> reachable(String root);

    /**
     * The intra-chunk deltas this store keeps that rebuild a held chunk, each a {@code dcz} frame over a base
     * chunk (PDT 2 phase 4, P2-Z-02; srad/agentspaces-pdt2.md §3.8). A holder sends one in place of the chunk
     * to a level-2 asker whose haves hold the base. A store that keeps none returns an empty list.
     *
     * @param cid the chunk's CID
     * @return the deltas, newest first
     */
    default List<ai.badmonkey.agentspaces.content.Messages.NodeDelta> deltas(String cid) {
        return List.of();
    }

    /**
     * The identity attributes of a plaintext flat artifact (PDT 2 phase 4, P2-B-06, P2-H-05; rule 11 of
     * srad/agentspaces-pdt2.md §6.10.3): {@code pdt.blake3}, the BLAKE3 identity of the content, and {@code pdt.bao},
     * the root of its outboard, which this store keeps and serves by the tree exchange. A store that builds no
     * outboard returns an empty map, and the manifest names no identity.
     *
     * @param content the artifact's bytes, read to the end and not closed
     * @return the attributes, or an empty map
     * @throws IOException when the stream fails
     */
    default Map<String, String> outboard(InputStream content) throws IOException {
        return Map.of();
    }
}
