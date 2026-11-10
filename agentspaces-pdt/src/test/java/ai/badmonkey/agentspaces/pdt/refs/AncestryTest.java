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
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.kinds.VgNode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PDT 2 phase 0 (P2-V-01; srad/agentspaces-pdt2.md §6.10.3, rule 9): a head names its version graph
 * node by the reserved attribute {@code pdt.ancestry}, so the {@code PRef} schema keeps its twelve
 * fields and a level-1 reader passes the attribute through. The attribute is advisory: a reader
 * uses it only when its text is a dag-cbor sha2-256 CIDv1 whose node decodes as a {@code vg} node
 * of the head's own root, and otherwise treats the head as one without ancestry.
 */
class AncestryTest {

    private static final String WRITER = "zQ3shPeer/writer";

    private static String cid(String text, int codec) {
        return Cids.of(text.getBytes(StandardCharsets.UTF_8), codec);
    }

    private static PRef head(String root) {
        return new PRef(PRef.VERSION, "facts", PRef.KIND_MAP, root, List.of(), 1, 0, null, PRef.MODE_MULTI,
                PRef.SEALING_NONE, null, null);
    }

    @Test
    void aHeadCarriesItsAncestryAsAnAttributeAndKeepsItsSchema() {
        String root = cid("root", Cids.DAG_CBOR);
        byte[] vg = new VgNode(root, List.of(), WRITER, "1792320000000:0:golden", List.of()).encode();
        String vgCid = Cids.of(vg, Cids.DAG_CBOR);
        PRef withAncestry = Ancestry.with(head(root), vgCid);
        assertThat(withAncestry.attrs().get(Ancestry.ATTR)).isEqualTo(vgCid);
        assertThat(withAncestry.violation()).isEmpty();
        byte[] bytes = CborCodec.defaultCodec().toBytes(withAncestry);
        PRef read = CborCodec.defaultCodec().fromDocument(bytes, PRef.class);
        assertThat(Ancestry.cid(read)).contains(vgCid);
        assertThat(Ancestry.read(read, vg)).contains(VgNode.decode(vgCid, vg));
    }

    @Test
    void anAttributeAReaderCannotUseReadsAsNoAncestry() {
        String root = cid("root", Cids.DAG_CBOR);
        byte[] vg = new VgNode(root, List.of(), WRITER, "1792320000000:0:golden", List.of()).encode();
        String vgCid = Cids.of(vg, Cids.DAG_CBOR);
        assertThat(Ancestry.cid(head(root))).as("no attribute").isEmpty();
        assertThat(Ancestry.cid(Ancestry.with(head(root), cid("x", Cids.RAW)))).as("a raw CID names no vg node").isEmpty();
        assertThat(Ancestry.cid(Ancestry.with(head(root), vgCid.toUpperCase()))).as("one text per CID").isEmpty();
        assertThat(Ancestry.cid(Ancestry.with(head(root), "ancestry"))).isEmpty();
        Map<String, Object> nonText = new HashMap<>();
        nonText.put(Ancestry.ATTR, 7);
        assertThat(Ancestry.cid(head(root).withAttrs(nonText))).isEmpty();

        PRef named = Ancestry.with(head(root), vgCid);
        assertThat(Ancestry.read(named, "not a node".getBytes(StandardCharsets.UTF_8))).as("bytes of another CID").isEmpty();
        byte[] otherRoot = new VgNode(cid("other", Cids.DAG_CBOR), List.of(), WRITER, "1:0:n", List.of()).encode();
        assertThat(Ancestry.read(Ancestry.with(head(root), Cids.of(otherRoot, Cids.DAG_CBOR)), otherRoot))
                .as("a vg node of another root").isEmpty();
        byte[] mapNode = ai.badmonkey.agentspaces.pdt.golden.PdtVectors.mapSmall();
        assertThat(Ancestry.read(Ancestry.with(head(root), Cids.of(mapNode, Cids.DAG_CBOR)), mapNode))
                .as("a node of another kind").isEmpty();
    }

    /** PDT 2 phase 1, P2-V-02: a chain link may name a vg node of any root; a head's must name its own. */
    @Test
    void aChainLinkReadsAnyRootAndAHeadOnlyItsOwn() {
        String root = cid("root", Cids.DAG_CBOR);
        byte[] vg = new VgNode(root, List.of(), WRITER, "1792320000000:0:golden", List.of()).encode();
        String vgCid = Cids.of(vg, Cids.DAG_CBOR);
        assertThat(Ancestry.read(vgCid, null, vg)).as("a chain link").contains(VgNode.decode(vgCid, vg));
        assertThat(Ancestry.read(vgCid, root, vg)).as("the head's own root").isPresent();
        assertThat(Ancestry.read(vgCid, cid("other", Cids.DAG_CBOR), vg)).as("another root").isEmpty();
        assertThat(Ancestry.read(cid("x", Cids.DAG_CBOR), null, vg)).as("bytes of another CID").isEmpty();
        byte[] mapNode = ai.badmonkey.agentspaces.pdt.golden.PdtVectors.mapSmall();
        assertThat(Ancestry.read(Cids.of(mapNode, Cids.DAG_CBOR), null, mapNode)).as("a node of another kind").isEmpty();
    }
}
