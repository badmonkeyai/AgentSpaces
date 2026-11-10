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

import ai.badmonkey.agentspaces.pdt.Embedded;
import ai.badmonkey.agentspaces.pdt.Kind;
import ai.badmonkey.agentspaces.pdt.Link;
import ai.badmonkey.agentspaces.pdt.NodeCodec;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The root of an RRB vector that still holds a relaxed node, kind {@code rr} (PDT 2 phase 0,
 * P2-R-01; srad/agentspaces-pdt2.md §3.7): {@code { k:"rr", n: count, s: shift, r: link or null,
 * t: [tail] }}, the {@code vr} shape with the trie made of {@link RvNode} interior nodes over
 * {@code v} leaves. A packed tree is a {@code vr} tree, so a head whose tree holds only packed
 * nodes stays {@code PRef.v} 1 and a head whose tree holds an {@code rr} or {@code rv} node is
 * {@code PRef.v} 2 (§6.10.3, rule 2).
 *
 * @param count the elements
 * @param shift the trie shift, a positive multiple of {@link #BITS}
 * @param trie  the trie root, or null when every element sits in the tail
 * @param tail  the tail, at most {@link #WIDTH} elements
 */
public record RrRoot(long count, long shift, Link trie, List<Embedded> tail) {

    /** The branching factor m of §3.7. */
    public static final int WIDTH = 32;

    /** log2 of {@link #WIDTH}. */
    public static final int BITS = 5;

    private static final List<String> REQUIRED = List.of("n", "s", "r", "t");

    /** Checks the shape. */
    public RrRoot {
        tail = List.copyOf(tail);
        if (shift < BITS || shift % BITS != 0) {
            throw new IllegalArgumentException("an rr shift is a positive multiple of " + BITS + ", found " + shift);
        }
        if (tail.size() > WIDTH) {
            throw new IllegalArgumentException("an rr tail holds at most " + WIDTH + " elements, found " + tail.size());
        }
        if (count < tail.size() || (trie == null && count != tail.size())) {
            throw new IllegalArgumentException("an rr count of " + count + " does not fit a tail of " + tail.size()
                    + (trie == null ? " and no trie" : ""));
        }
        if (trie != null && count == tail.size()) {
            // PDT 2 phase 0 (P2-R-01): an rv node holds at least one child, so a linked trie holds an element
            throw new IllegalArgumentException("an rr root links a trie but its count of " + count
                    + " leaves the trie no element");
        }
    }

    /** The canonical node bytes. */
    public byte[] encode() {
        Map<String, Object> node = new LinkedHashMap<>();
        node.put(NodeCodec.KIND_KEY, Kind.RR);
        node.put("n", count);
        node.put("s", shift);
        node.put("r", trie);
        node.put("t", tail);
        return NodeCodec.encode(node);
    }

    /**
     * Reads an {@code rr} root.
     *
     * @param cid   the CID, named in a refusal; may be null
     * @param bytes the node bytes
     * @return the root
     * @throws ai.badmonkey.agentspaces.pdt.NodeFormatException on another kind, a missing key, or a broken shape
     */
    public static RrRoot decode(String cid, byte[] bytes) {
        Map<String, Object> node = Nodes.decode(cid, bytes, Kind.RR, REQUIRED);
        return Nodes.build(cid, () -> new RrRoot((Long) node.get("n"), (Long) node.get("s"), (Link) node.get("r"),
                Nodes.list(node, "t")));
    }
}
