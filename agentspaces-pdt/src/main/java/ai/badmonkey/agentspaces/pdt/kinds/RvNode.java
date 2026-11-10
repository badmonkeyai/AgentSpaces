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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An interior node of an RRB vector, kind {@code rv} (PDT 2 phase 0, P2-R-01;
 * srad/agentspaces-pdt2.md §3.7): {@code { k:"rv", c: [link...], z: [cumulative sizes] }}, with
 * {@code z} present only on a relaxed node, one whose children are not all full. A reader indexes
 * a relaxed node through {@code z} (P2-R-04) and a dense one by radix.
 *
 * @param children the children, 1 to {@link RrRoot#WIDTH}
 * @param sizes    the cumulative element counts, one per child and strictly increasing; null on a dense node
 */
public record RvNode(List<Link> children, List<Long> sizes) {

    private static final List<String> REQUIRED = List.of("c");

    /** Checks the size table. */
    public RvNode {
        children = List.copyOf(children);
        sizes = sizes == null ? null : List.copyOf(sizes);
        if (children.isEmpty() || children.size() > RrRoot.WIDTH) {
            throw new IllegalArgumentException("an rv node holds 1 to 32 children, found " + children.size());
        }
        if (sizes != null) {
            if (sizes.size() != children.size()) {
                throw new IllegalArgumentException("an rv size table holds one size per child");
            }
            long previous = 0;
            for (long size : sizes) {
                if (size <= previous) {
                    throw new IllegalArgumentException("an rv size table is strictly increasing from above 0");
                }
                previous = size;
            }
        }
    }

    /** The canonical node bytes; a dense node carries no {@code z} key. */
    public byte[] encode() {
        Map<String, Object> node = new LinkedHashMap<>();
        node.put(NodeCodec.KIND_KEY, Kind.RV);
        node.put("c", children);
        if (sizes != null) {
            node.put("z", sizes);
        }
        return NodeCodec.encode(node);
    }

    /**
     * Reads an {@code rv} node.
     *
     * @param cid   the CID, named in a refusal; may be null
     * @param bytes the node bytes
     * @return the node
     * @throws ai.badmonkey.agentspaces.pdt.NodeFormatException on another kind, a missing key, or a broken size table
     */
    public static RvNode decode(String cid, byte[] bytes) {
        Map<String, Object> node = Nodes.decode(cid, bytes, Kind.RV, REQUIRED);
        List<Long> sizes = node.containsKey("z") ? Nodes.list(node, "z") : null;
        return Nodes.build(cid, () -> new RvNode(Nodes.list(node, "c"), sizes));
    }
}
