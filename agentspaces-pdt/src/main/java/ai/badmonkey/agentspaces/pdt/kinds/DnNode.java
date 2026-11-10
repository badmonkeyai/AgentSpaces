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

import ai.badmonkey.agentspaces.pdt.Datom;
import ai.badmonkey.agentspaces.pdt.Kind;
import ai.badmonkey.agentspaces.pdt.Link;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeFormatException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A DataScript sorted-set node, kind {@code dn} (PDT 2 phase 0, P2-D-01; srad/agentspaces-pdt2.md
 * §3.1.1): a leaf {@code { k:"dn", l: 0, x: index, e: [datom...] }}, a branch
 * {@code { k:"dn", l: level >= 1, x: index, e: [datom...], c: [link...] }} whose {@code e} holds
 * the largest key of each child in the order of {@code c}, or a tail
 * {@code { k:"dn", l: 0, x: "tail", e: [datom...] }} with the datoms of one or more transactions
 * in transaction order. The comparator that orders a leaf or a branch is fixed by {@code x}; the
 * reader of P2-D-09 checks the order, since it owns the comparators.
 *
 * @param level    0 for a leaf or a tail, the height above the leaves for a branch
 * @param index    {@link #EAVT}, {@link #AEVT}, {@link #AVET}, or {@link #TAIL}
 * @param datoms   the keys
 * @param children one link per key on a branch; empty on a leaf or a tail
 */
public record DnNode(long level, String index, List<Datom> datoms, List<Link> children) {

    public static final String EAVT = "eavt";
    public static final String AEVT = "aevt";
    public static final String AVET = "avet";
    public static final String TAIL = "tail";

    private static final Set<String> INDEXES = Set.of(EAVT, AEVT, AVET, TAIL);
    private static final List<String> REQUIRED = List.of("l", "x", "e");

    /** Checks the shape rules of §3.1.1. */
    public DnNode {
        Objects.requireNonNull(index, "index");
        datoms = List.copyOf(datoms);
        children = List.copyOf(children);
        if (!INDEXES.contains(index)) {
            throw new IllegalArgumentException("a dn node names index '" + index + "'; the indexes are eavt, aevt, avet and tail");
        }
        if (level < 0) {
            throw new IllegalArgumentException("a dn level is non-negative");
        }
        if (TAIL.equals(index) && level != 0) {
            throw new IllegalArgumentException("a tail is a leaf, found level " + level);
        }
        if (TAIL.equals(index) && datoms.isEmpty()) {
            throw new IllegalArgumentException("an empty tail is no node: the db root's 't' is null instead");
        }
        if (level == 0 && !children.isEmpty()) {
            throw new IllegalArgumentException("a leaf carries no 'c'");
        }
        if (level > 0 && (datoms.isEmpty() || children.size() != datoms.size())) {
            throw new IllegalArgumentException("a branch holds one link per key, found " + children.size()
                    + " links for " + datoms.size() + " keys");
        }
    }

    /** A leaf of an index. */
    public static DnNode leaf(String index, List<Datom> datoms) {
        return new DnNode(0, index, datoms, List.of());
    }

    /** A branch of an index. */
    public static DnNode branch(long level, String index, List<Datom> datoms, List<Link> children) {
        return new DnNode(level, index, datoms, children);
    }

    /** A tail: the datoms of transactions not yet folded into the indexes. */
    public static DnNode tail(List<Datom> datoms) {
        return new DnNode(0, TAIL, datoms, List.of());
    }

    /** The canonical node bytes; a leaf and a tail carry no {@code c} key. */
    public byte[] encode() {
        Map<String, Object> node = new LinkedHashMap<>();
        node.put(NodeCodec.KIND_KEY, Kind.DN);
        node.put("l", level);
        node.put("x", index);
        node.put("e", datoms);
        if (level > 0) {
            node.put("c", children);
        }
        return NodeCodec.encode(node);
    }

    /**
     * Reads a {@code dn} node.
     *
     * @param cid   the CID, named in a refusal; may be null
     * @param bytes the node bytes
     * @return the node
     * @throws NodeFormatException on another kind, a missing key, or a broken shape rule
     */
    public static DnNode decode(String cid, byte[] bytes) {
        Map<String, Object> node = Nodes.decode(cid, bytes, Kind.DN, REQUIRED);
        long level = (Long) node.get("l");
        if (level > 0 && !node.containsKey("c")) {
            throw new NodeFormatException(cid, "a 'dn' node lacks 'c': a branch links its children");
        }
        if (level == 0 && node.containsKey("c")) {
            throw new NodeFormatException(cid, "a leaf carries no 'c'");
        }
        List<Link> children = node.containsKey("c") ? Nodes.list(node, "c") : List.of();
        return Nodes.build(cid, () -> new DnNode(level, (String) node.get("x"), Nodes.list(node, "e"), children));
    }
}
