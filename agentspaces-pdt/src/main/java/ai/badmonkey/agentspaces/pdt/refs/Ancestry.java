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
package ai.badmonkey.agentspaces.pdt.refs;

import ai.badmonkey.agentspaces.agent.pdt.PRef;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.NodeFormatException;
import ai.badmonkey.agentspaces.pdt.kinds.VgNode;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A head's ancestry, the reserved attribute {@link #ATTR} (PDT 2 phase 0, P2-V-01;
 * srad/agentspaces-pdt2.md §3.10 and §6.10.3, rule 9). Revision 1 of the design gave {@code PRef}
 * an {@code ancestry} field; a head is a record every member reads and no writer can gate, and a
 * strict reader refuses a field it does not know, so the CID of the head's {@code vg} node rides in
 * {@code attrs} beside {@code pdt.issued} and {@code pdt.issuer}. The {@code PRef} schema keeps its
 * twelve fields, and a level-1 reader passes the attribute through as text.
 *
 * <p>The attribute is advisory. A reader uses it only when its text is a CIDv1 of codec
 * {@code dag-cbor} over sha2-256 in its one base32 form, and the node it names hashes to it and
 * decodes as a {@code vg} node of the head's own root; otherwise the head reads as one without
 * ancestry (P2-V-02), and nothing is refused or struck.
 */
public final class Ancestry {

    /** The reserved attribute naming the head's {@code vg} node. */
    public static final String ATTR = "pdt.ancestry";

    private Ancestry() {
    }

    /**
     * A head that names its {@code vg} node, with every other attribute kept.
     *
     * @param head the head
     * @param vg   the CID of the head's {@code vg} node
     * @return the head with {@link #ATTR} set
     */
    public static PRef with(PRef head, String vg) {
        Objects.requireNonNull(vg, "vg");
        Map<String, Object> attrs = new LinkedHashMap<>();
        if (head.attrs() != null) {
            attrs.putAll(head.attrs());
        }
        attrs.put(ATTR, vg);
        return head.withAttrs(attrs);
    }

    /**
     * The CID a head names under {@link #ATTR}, when the text is a usable one.
     *
     * @param head the head
     * @return the CID of a dag-cbor sha2-256 node in canonical base32, or empty
     */
    public static Optional<String> cid(PRef head) {
        if (head.attrs() == null || !(head.attrs().get(ATTR) instanceof String text) || !Cids.isCid(text)) {
            return Optional.empty();
        }
        Cids.Parsed parsed = Cids.parse(text);
        if (parsed.codec() != Cids.DAG_CBOR || !Cids.ofDigest(parsed.digest(), Cids.DAG_CBOR).equals(text)) {
            return Optional.empty();
        }
        return Optional.of(text);
    }

    /**
     * The {@code vg} node of a head, from the bytes a store or a fetch returned for {@link #cid}.
     *
     * @param head  the head
     * @param bytes the bytes of the node the head names
     * @return the node, or empty when the head names none, the bytes hash to another CID, or they
     *         do not decode as a {@code vg} node of the head's root
     */
    public static Optional<VgNode> read(PRef head, byte[] bytes) {
        return cid(head).flatMap(cid -> read(cid, head.root(), bytes));
    }

    /**
     * A {@code vg} node from the bytes held or fetched for a CID (PDT 2 phase 1, P2-V-02): a head's
     * node must describe the head's root; a node a chain link names may describe any root.
     *
     * @param cid   the CID the bytes were asked for
     * @param root  the root the node must describe, or null for a chain link
     * @param bytes the bytes
     * @return the node, or empty when the bytes hash to another CID, decode as no {@code vg} node,
     *         or describe another root
     */
    public static Optional<VgNode> read(String cid, String root, byte[] bytes) {
        if (!Cids.of(bytes, Cids.DAG_CBOR).equals(cid)) {
            return Optional.empty();
        }
        VgNode node;
        try {
            node = VgNode.decode(cid, bytes);
        } catch (NodeFormatException notAVgNode) {
            return Optional.empty();
        }
        return root == null || node.root().equals(root) ? Optional.of(node) : Optional.empty();
    }
}
