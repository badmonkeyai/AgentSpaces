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

import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.pdt.Kind;
import ai.badmonkey.agentspaces.pdt.Link;
import ai.badmonkey.agentspaces.pdt.NodeCodec;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A node of the persisted version graph, kind {@code vg} (PDT 2 phase 0, P2-V-01;
 * srad/agentspaces-pdt2.md §3.10): {@code { k:"vg", r: root, b: [base roots], i: issuer,
 * t: issued, p: [links to earlier vg nodes] }}. A head names its node by the reserved attribute
 * {@code pdt.ancestry}; no value root links a {@code vg} node and no head carries one inline
 * (§6.10.3, rule 4).
 *
 * <p>The build pins two points §3.10 leaves open. The roots {@code r} and {@code b} are CIDs held
 * as text, never links, so walking or holding a chain of {@code vg} nodes reaches the chain alone
 * and never the value trees of every past version. And {@code p} is a list, at most one link per
 * base, naming the {@code vg} node of each base the writer knows, so a merge keeps the lineage of
 * both parents and the base walk of P2-V-02 reaches every ancestor; a base written without
 * ancestry has no entry.
 *
 * @param root     the root of this version
 * @param bases    the roots this version was computed from, at most {@link PRef#MAX_BASE}
 * @param issuer   the writing agent, {@code <peerId>/<localName>}
 * @param issued   the canonical HLC stamp of the head that published this version
 * @param previous the {@code vg} nodes of the bases, at most one per base
 */
public record VgNode(String root, List<String> bases, String issuer, String issued, List<Link> previous) {

    private static final List<String> REQUIRED = List.of("r", "b", "i", "t", "p");

    /** Checks the fields. */
    public VgNode {
        Objects.requireNonNull(root, "root");
        Objects.requireNonNull(issuer, "issuer");
        Objects.requireNonNull(issued, "issued");
        bases = List.copyOf(bases);
        previous = List.copyOf(previous);
        if (bases.size() > PRef.MAX_BASE) {
            throw new IllegalArgumentException("a vg node names at most " + PRef.MAX_BASE + " bases, found " + bases.size());
        }
        // canonical base32, padding bits zero, as every port reads a CID's text (gate finding G2)
        if (!PRef.isCid(root) || !bases.stream().allMatch(PRef::isCid)) {
            throw new IllegalArgumentException("a vg node names its root and bases by CID");
        }
        if (previous.size() > bases.size()) {
            throw new IllegalArgumentException("a vg node links at most one earlier vg node per base");
        }
        for (Link link : previous) {
            if (!link.structured()) {
                throw new IllegalArgumentException("a vg node links earlier vg nodes, which are dag-cbor");
            }
        }
        try {
            AgentId.parse(issuer);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("a vg issuer is <peerId>/<localName>: " + e.getMessage(), e);
        }
        try {
            HlcTimestamp.parse(issued);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("a vg issued stamp is canonical: " + e.getMessage(), e);
        }
    }

    /** The canonical node bytes. */
    public byte[] encode() {
        Map<String, Object> node = new LinkedHashMap<>();
        node.put(NodeCodec.KIND_KEY, Kind.VG);
        node.put("r", root);
        node.put("b", bases);
        node.put("i", issuer);
        node.put("t", issued);
        node.put("p", previous);
        return NodeCodec.encode(node);
    }

    /**
     * Reads a {@code vg} node.
     *
     * @param cid   the CID, named in a refusal; may be null
     * @param bytes the node bytes
     * @return the node
     * @throws ai.badmonkey.agentspaces.pdt.NodeFormatException on another kind, a missing key, or a broken field
     */
    public static VgNode decode(String cid, byte[] bytes) {
        Map<String, Object> node = Nodes.decode(cid, bytes, Kind.VG, REQUIRED);
        return Nodes.build(cid, () -> new VgNode((String) node.get("r"), Nodes.list(node, "b"), (String) node.get("i"),
                (String) node.get("t"), Nodes.list(node, "p")));
    }
}
