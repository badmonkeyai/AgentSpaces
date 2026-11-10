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
package ai.badmonkey.agentspaces.pdt.blob;

import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.Link;
import ai.badmonkey.agentspaces.pdt.kinds.BoNode;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Builds a blob's BLAKE3 identity and its Bao outboard as {@code bo} nodes (PDT 2 phase 0, P2-B-02;
 * srad/agentspaces-pdt2.md §3.5) in one pass over the bytes. The chunk groups are 16 KiB (decision
 * 10), so the outboard costs 64 bytes per group, about 0.4 % of the blob, and a Rust peer reads it
 * with {@code bao-tree} at block size 4. {@link BoNode} fixes how the tree splits above 8 MiB.
 *
 * <p>The pass hashes the groups on several threads ({@link GroupHasher}; PDT 2 phase 4 measures, P2-B-05,
 * §10.14), all cores by default, with a result that does not depend on the thread count. It holds two
 * batches of 8 MiB and one 32-byte chaining value per group, so a 10 GiB blob costs about 20 MiB of
 * chaining values in memory; the nodes it returns are the outboard itself. The identity is the root of
 * the same tree of chaining values (PDT 2 phase 4, P2-B-05): the top parent pair hashed with the root
 * flag, or the one group hashed as the root, which the BLAKE3 specification makes equal to the digest
 * of the whole content, so the pass hashes each byte once.
 */
public final class BaoOutboard {

    /**
     * An outboard.
     *
     * @param root     the CID of the root {@code bo} node, the value of the attribute {@code pdt.bao}
     * @param identity the blob's BLAKE3 identity CID, the value of the attribute {@code pdt.blake3}
     * @param size     the blob's bytes
     * @param nodes    every {@code bo} node by CID, children before their parent and the root last
     */
    public record Built(String root, String identity, long size, Map<String, byte[]> nodes) {
        public Built {
            nodes = Collections.unmodifiableMap(new LinkedHashMap<>(nodes));
        }
    }

    private BaoOutboard() {
    }

    /**
     * Reads a blob to its end and builds its identity and outboard on every core of the host.
     *
     * @param in the blob's bytes; read to the end, not closed
     * @return the outboard
     * @throws IOException when the stream fails
     */
    public static Built of(InputStream in) throws IOException {
        return of(in, Runtime.getRuntime().availableProcessors());
    }

    /**
     * Reads a blob to its end and builds its identity and outboard, hashing on at most {@code threads} threads
     * (PDT 2 phase 4 measures, P2-B-05, §10.14). Every thread count gives the same outboard.
     *
     * @param in      the blob's bytes; read to the end, not closed
     * @param threads the most threads that hash at once, the caller included; at least 1
     * @return the outboard
     * @throws IOException when the stream fails
     */
    public static Built of(InputStream in, int threads) throws IOException {
        Objects.requireNonNull(in, "in");
        if (threads < 1) {
            throw new IllegalArgumentException("an outboard hashes on at least one thread, got threads = " + threads);
        }
        GroupHasher.Hashed groups = GroupHasher.hash(in, threads);
        Map<String, byte[]> nodes = new LinkedHashMap<>();
        String root = build(groups.cvs(), 0, groups.size(), nodes).cid;
        byte[] hash;
        if (groups.only() != null) {
            // one group holds the whole blob: its bytes hash as the root
            hash = Blake3.treeRoot(groups.only());
        } else {
            byte[] top = BoNode.decode(root, nodes.get(root)).parents();
            hash = Blake3.parent(Arrays.copyOfRange(top, 0, Blake3.DIGEST_BYTES),
                    Arrays.copyOfRange(top, Blake3.DIGEST_BYTES, BoNode.PARENT_BYTES), true);
        }
        return new Built(root, Blake3.identity(hash), groups.size(), nodes);
    }

    /** A built node: its CID and the chaining value of the subtree it covers. */
    private record Node(String cid, byte[] cv) {
    }

    /** Builds the node over the groups from {@code firstGroup} that hold {@code size} bytes. */
    private static Node build(List<byte[]> groupCvs, int firstGroup, long size, Map<String, byte[]> nodes) {
        List<Long> blocks = BoNode.childSizes(size);
        List<byte[]> leaves = new ArrayList<>();
        List<Link> children = new ArrayList<>();
        if (blocks.isEmpty()) {
            leaves.addAll(groupCvs.subList(firstGroup, firstGroup + (int) BoNode.groups(size)));
        } else {
            int group = firstGroup;
            for (long block : blocks) {
                Node child = build(groupCvs, group, block, nodes);
                children.add(new Link(child.cid));
                leaves.add(child.cv);
                group += (int) BoNode.groups(block);
            }
        }
        byte[] parents = new byte[(leaves.size() - 1) * BoNode.PARENT_BYTES];
        byte[] cv = preOrder(leaves, 0, leaves.size(), parents, new int[1]);
        byte[] bytes = new BoNode(size, parents, children).encode();
        String cid = Cids.of(bytes, Cids.DAG_CBOR);
        nodes.put(cid, bytes);
        return new Node(cid, cv);
    }

    /**
     * Writes the parent pairs over {@code leaves[from, to)} in pre-order and returns the subtree's
     * chaining value. BLAKE3 puts the largest power of two strictly below the count on the left; the
     * leaves of an upper node are whole aligned blocks, so the same rule applies to them.
     */
    private static byte[] preOrder(List<byte[]> leaves, int from, int to, byte[] parents, int[] cursor) {
        if (to - from == 1) {
            return leaves.get(from);
        }
        int at = cursor[0];
        cursor[0] += BoNode.PARENT_BYTES;
        int mid = from + Integer.highestOneBit(to - from - 1);
        byte[] left = preOrder(leaves, from, mid, parents, cursor);
        byte[] right = preOrder(leaves, mid, to, parents, cursor);
        System.arraycopy(left, 0, parents, at, Blake3.DIGEST_BYTES);
        System.arraycopy(right, 0, parents, at + Blake3.DIGEST_BYTES, Blake3.DIGEST_BYTES);
        return Blake3.parent(left, right, false);
    }
}
