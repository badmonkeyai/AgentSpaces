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

import ai.badmonkey.agentspaces.common.codec.CborCodec;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * A node format for the content module's tests, so nothing here depends on
 * {@code agentspaces-pdt}: a node is a CBOR list of child CID strings followed
 * by one payload byte string. Trees are built bottom up from seeded payloads, and
 * a variant re-seeds a chosen set of leaves so the two versions share every
 * other node (the shape of srad/agentspaces-pdt.md scenario 2).
 */
public final class NodeFixtures {

    static final CborCodec CODEC = CborCodec.defaultCodec();

    /** A built tree: its root, every node by CID, and the leaf-to-root depth. */
    public record Tree(String root, Map<String, byte[]> nodes, int depth) {
        public long bytes() {
            long total = 0;
            for (byte[] node : nodes.values()) {
                total += node.length;
            }
            return total;
        }

        /** The bytes of the nodes this tree has and {@code other} lacks. */
        public long bytesNotIn(Tree other) {
            long total = 0;
            for (Map.Entry<String, byte[]> e : nodes.entrySet()) {
                if (!other.nodes.containsKey(e.getKey())) {
                    total += e.getValue().length;
                }
            }
            return total;
        }

        public int nodesNotIn(Tree other) {
            int n = 0;
            for (String cid : nodes.keySet()) {
                if (!other.nodes.containsKey(cid)) {
                    n++;
                }
            }
            return n;
        }
    }

    private NodeFixtures() {
    }

    public static byte[] node(List<String> children, byte[] payload) {
        List<Object> items = new ArrayList<>(children);
        items.add(payload);
        return CODEC.toBytes(items);
    }

    public static String cid(byte[] node) {
        return Cids.of(node, Cids.DAG_CBOR);
    }

    /** The child CIDs of a node; throws when the bytes are not a node of this format. */
    public static List<String> linksOf(byte[] node) {
        List<?> items;
        try {
            items = CODEC.fromBytes(node, List.class);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("not a test node", e);
        }
        if (items == null || items.isEmpty() || !(items.get(items.size() - 1) instanceof byte[])) {
            throw new IllegalArgumentException("not a test node: no payload");
        }
        List<String> links = new ArrayList<>();
        for (int i = 0; i < items.size() - 1; i++) {
            if (!(items.get(i) instanceof String s) || !Cids.isCid(s)) {
                throw new IllegalArgumentException("not a test node: child " + i);
            }
            links.add(s);
        }
        return links;
    }

    /** A full tree of {@code fanout} children per interior node, {@code depth} levels below the root. */
    public static Tree tree(long seed, int depth, int fanout, int leafBytes) {
        return tree(seed, depth, fanout, leafBytes, Set.of(), 0);
    }

    /**
     * The tree of {@code tree(seed, ...)} with the leaves at {@code changedLeaves} (indexes from the
     * left) re-seeded from {@code changeSeed}; every node off their paths is shared with the base.
     */
    public static Tree tree(long seed, int depth, int fanout, int leafBytes, Set<Integer> changedLeaves, long changeSeed) {
        Map<String, byte[]> nodes = new LinkedHashMap<>();
        int leaves = 1;
        for (int i = 0; i < depth; i++) {
            leaves *= fanout;
        }
        List<String> level = new ArrayList<>();
        for (int i = 0; i < leaves; i++) {
            byte[] payload = new byte[leafBytes];
            new Random((changedLeaves.contains(i) ? changeSeed : seed) * 1_000_003L + i).nextBytes(payload);
            byte[] leaf = node(List.of(), payload);
            String cid = cid(leaf);
            nodes.put(cid, leaf);
            level.add(cid);
        }
        while (level.size() > 1) {
            List<String> next = new ArrayList<>();
            for (int i = 0; i < level.size(); i += fanout) {
                List<String> children = level.subList(i, Math.min(level.size(), i + fanout));
                byte[] interior = node(children, new byte[]{(byte) 'i'});
                String cid = cid(interior);
                nodes.put(cid, interior);
                next.add(cid);
            }
            level = next;
        }
        return new Tree(level.get(0), nodes, depth);
    }
}
