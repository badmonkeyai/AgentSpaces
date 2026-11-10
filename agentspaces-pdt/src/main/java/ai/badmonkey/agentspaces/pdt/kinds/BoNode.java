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
package ai.badmonkey.agentspaces.pdt.kinds;

import ai.badmonkey.agentspaces.pdt.Kind;
import ai.badmonkey.agentspaces.pdt.Link;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeFormatException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A node of a blob's Bao outboard, kind {@code bo} (PDT 2 phase 0, P2-B-02; srad/agentspaces-pdt2.md
 * §3.5): {@code { k:"bo", g: 4, n: size, p: parent chaining values, c: [links to child bo nodes] }},
 * over chunk groups of 2^{@code g} BLAKE3 chunks (16 KiB, decision 10). A {@code bo} tree is reached
 * only through the attribute {@code pdt.bao} (§6.10.3, rule 5).
 *
 * <p>The build pins the split rule of §3.5, so every port cuts the same tree. The owner ruled on
 * 2026-11-09 that a leaf covers 8 MiB, about 32 KiB of outboard, so a 2 MiB verified window pays at
 * most two leaves (PDT 2 phase 4 measures, P2-Q-PRF-05, §10.14); the phase 0 rule cut leaves of 256 MiB.
 * <ul>
 *   <li>{@code p} is one byte string, the concatenated parent pairs (left CV then right CV, 64 bytes)
 *       in pre-order, as a Bao outboard stores them. A list of 64-byte strings would spend two bytes
 *       of header per parent.</li>
 *   <li>A node over at most {@link #LEAF_GROUPS} groups (8 MiB) is a leaf: {@code p} holds every
 *       parent of its BLAKE3 subtree, {@code groups - 1} of them, and {@code c} is empty. A full
 *       leaf encodes to 32,728 bytes.</li>
 *   <li>A larger node over G groups takes the smallest j &gt;= 1 with G &lt;= {@link #LEAF_GROUPS} x
 *       {@link #FANOUT}^j, cuts its subtree into aligned blocks of {@link #LEAF_GROUPS} x
 *       {@link #FANOUT}^(j-1) groups, links one child {@code bo} node per block in order, and holds
 *       in {@code p} the parents above the cut, one fewer than its children. BLAKE3's tree puts
 *       every left subtree on a power of two, so each block is a whole subtree. An upper node of
 *       16 children encodes to about 1.6 KiB, so the path above a window's leaves stays short.</li>
 * </ul>
 * The shape is a function of {@code n} alone, which the constructor checks.
 *
 * @param size     the bytes the node covers
 * @param parents  the parent pairs, {@link #PARENT_BYTES} each, in pre-order
 * @param children the child {@code bo} nodes, empty on a leaf
 */
public record BoNode(long size, byte[] parents, List<Link> children) {

    /** log2 of the BLAKE3 chunks in one group: 16 chunks of 1 KiB (decision 10). */
    public static final int GROUP_LOG = 4;

    /** The bytes in one chunk group. */
    public static final int GROUP_BYTES = 1024 << GROUP_LOG;

    /** The bytes of one parent pair: two 32-byte chaining values. */
    public static final int PARENT_BYTES = 64;

    /** The most groups a leaf covers: 2^9, 8 MiB of content in 511 parents (PDT 2 phase 4 measures, §10.14). */
    public static final int LEAF_GROUPS = 1 << 9;

    /** The most children an upper node links: 16, well under {@link NodeCodec#MAX_LINKS}. */
    public static final int FANOUT = 1 << 4;

    private static final List<String> REQUIRED = List.of("g", "n", "p", "c");

    /** Checks that the parents and children are the ones the size implies. */
    public BoNode {
        parents = parents.clone();
        children = List.copyOf(children);
        if (size < 0) {
            throw new IllegalArgumentException("a bo size is non-negative");
        }
        long groups = groups(size);
        List<Long> blocks = childSizes(size);
        if (children.size() != blocks.size()) {
            throw new IllegalArgumentException("a bo node over " + groups + " groups has " + blocks.size()
                    + " children, found " + children.size());
        }
        long expected = (blocks.isEmpty() ? groups : blocks.size()) - 1;
        if (parents.length != expected * PARENT_BYTES) {
            throw new IllegalArgumentException("a bo node's " + (blocks.isEmpty() ? groups + " groups" : blocks.size() + " children")
                    + " take " + expected + " parents, found " + parents.length + " bytes");
        }
    }

    /** The chunk groups a blob of {@code size} bytes holds: at least one, since an empty blob is one empty chunk. */
    public static long groups(long size) {
        return Math.max(1, (size + GROUP_BYTES - 1) / GROUP_BYTES);
    }

    /**
     * The sizes, in bytes, of the child blocks of a node over {@code size} bytes; empty for a leaf.
     *
     * @param size the bytes the node covers
     * @return the byte count of each child in order
     */
    public static List<Long> childSizes(long size) {
        long groups = groups(size);
        if (groups <= LEAF_GROUPS) {
            return List.of();
        }
        long block = LEAF_GROUPS;
        while (groups > block * FANOUT) {
            block *= FANOUT;
        }
        long blockBytes = block * GROUP_BYTES;
        List<Long> sizes = new ArrayList<>();
        for (long start = 0; start < size; start += blockBytes) {
            sizes.add(Math.min(blockBytes, size - start));
        }
        return sizes;
    }

    /** The canonical node bytes. */
    public byte[] encode() {
        Map<String, Object> node = new LinkedHashMap<>();
        node.put(NodeCodec.KIND_KEY, Kind.BO);
        node.put("g", (long) GROUP_LOG);
        node.put("n", size);
        node.put("p", parents);
        node.put("c", children);
        return NodeCodec.encode(node);
    }

    /**
     * Reads a {@code bo} node.
     *
     * @param cid   the CID, named in a refusal; may be null
     * @param bytes the node bytes
     * @return the node
     * @throws NodeFormatException on another kind, another group size, a missing key, or parents
     *                             and children the size does not imply
     */
    public static BoNode decode(String cid, byte[] bytes) {
        Map<String, Object> node = Nodes.decode(cid, bytes, Kind.BO, REQUIRED);
        if (!Long.valueOf(GROUP_LOG).equals(node.get("g"))) {
            throw new NodeFormatException(cid, "a bo node of group size 2^" + node.get("g") + " chunks; this release reads 2^"
                    + GROUP_LOG + " (decision 10)");
        }
        return Nodes.build(cid, () -> new BoNode((Long) node.get("n"), (byte[]) node.get("p"), Nodes.list(node, "c")));
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof BoNode b && size == b.size && Arrays.equals(parents, b.parents) && children.equals(b.children);
    }

    @Override
    public int hashCode() {
        return (Long.hashCode(size) * 31 + Arrays.hashCode(parents)) * 31 + children.hashCode();
    }

    @Override
    public String toString() {
        return "BoNode(" + size + " bytes, " + parents.length / PARENT_BYTES + " parents, " + children.size() + " children)";
    }

    /** The parent pairs, copied. */
    @Override
    public byte[] parents() {
        return parents.clone();
    }
}
