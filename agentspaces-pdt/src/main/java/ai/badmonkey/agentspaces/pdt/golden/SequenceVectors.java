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
package ai.badmonkey.agentspaces.pdt.golden;

import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.values.Codec;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.pdt.values.PText;
import ai.badmonkey.agentspaces.pdt.values.PVector;
import ai.badmonkey.agentspaces.pdt.values.VectorEditor;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HexFormat;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The golden vectors of PDT 2 phase 6, groups R and T (srad/agentspaces-pdt2.md §3.7, §3.9), which
 * {@link PdtVectors#pdt2Vectors()} adds after the earlier phases'. Fields are comma separated.
 *
 * <ul>
 *   <li>{@code pdt_rr_splice} (P2-R-03, P2-R-04): the elements 0..1099 as embedded {@code CborCodec}
 *       integers with -1 inserted before index 500. The root CID, then the root CID of the packed
 *       form (the {@code vr} vector of the same 1,101 elements), then every node of the relaxed tree
 *       (hex), the root first and the rest in the order a depth-first walk of the links meets them.
 *       A port reads and packs this tree; its own splices need not give the same shape, since a
 *       relaxed tree's shape follows its history.</li>
 *   <li>{@code pdt_text_edits} (P2-T-01, P2-T-02): Fugue's insert rule over edits. Alice types
 *       {@code "Hello world"} at 0 and {@code ";"} at 5, giving the base {@code "Hello; world"}; she
 *       then deletes 1 at 0 and types {@code "J"} at 0. Bob, from the base, types {@code "!"} at 12
 *       and {@code "big "} at 7. Alice's leaf, Bob's leaf, and their union (hex each), then the
 *       union's text {@code "Jello; big world!"}. Replicas {@code alice} and {@code bob}.</li>
 *   <li>{@code pdt_text_value} (P2-T-02): the value form of the leaf of {@code pdt_text_runs} (hex): the
 *       wire encoding of {@code {k:"t", r:[run...], v:1}}, the map indefinite, a run the array
 *       {@code [replica, counter, parent, side, length, deleted, text]} and a parent
 *       {@code [replica, counter]} or null.</li>
 *   <li>{@code pdt_text_spill} (P2-T-02): the root node (hex) of the map {@code {"doc": 1,100 times "x"
 *       typed by alice, "note": the text of pdt_text_runs}} of texts, then the {@code t} node (hex)
 *       its {@code doc} value spilled to; the note stays inline in its value form.</li>
 *   <li>{@code pdt_text_replica} (P2-T-02): the agent id of {@link PdtVectors#GOLDEN_WRITER}, then its
 *       replica id: the first five bytes of the SHA-256 of the id's text, eight base32 characters.</li>
 * </ul>
 */
public final class SequenceVectors {

    /** The elements of {@code pdt_rr_splice} before the insertion. */
    public static final int SPLICE_ELEMENTS = 1100;

    /** Where {@code pdt_rr_splice} inserts its element. */
    public static final int SPLICE_AT = 500;

    /** The element {@code pdt_rr_splice} inserts. */
    public static final int SPLICE_ELEMENT = -1;

    private static final Codec<Integer> INTS = Codec.of(Integer.class);

    private SequenceVectors() {
    }

    /** The phase 6 vectors, in emission order. */
    static Map<String, String> vectors() {
        Map<String, String> out = new LinkedHashMap<>();
        NodeStore store = NodeStore.inMemory();
        PVector<Integer> spliced = splice(store);
        String root = spliced.root();
        out.put("pdt_rr_splice", root + "," + spliced.packed().root() + "," + reachable(store, root).stream()
                .map(SequenceVectors::hex).collect(Collectors.joining(",")));
        PText alice = textAlice();
        PText bob = textBob();
        out.put("pdt_text_edits", hex(alice.leaf().encode()) + "," + hex(bob.leaf().encode()) + ","
                + hex(alice.merge(bob).leaf().encode()) + "," + alice.merge(bob));
        out.put("pdt_text_value", hex(PText.codec("alice").encode(PText.of(PdtVectors.textRuns(), "alice"))));
        NodeStore texts = NodeStore.inMemory();
        String map = textMap(texts).root();
        List<String> spill = reachable(texts, map).stream().map(SequenceVectors::hex).toList();
        out.put("pdt_text_spill", String.join(",", spill));
        out.put("pdt_text_replica", PdtVectors.GOLDEN_WRITER + "," + PText.replicaOf(AgentId.parse(PdtVectors.GOLDEN_WRITER)));
        return out;
    }

    /** The vector of {@code pdt_rr_splice}, over a store. */
    public static PVector<Integer> splice(NodeStore store) {
        VectorEditor<Integer> editor = PVector.empty(store, INTS).editor();
        for (int i = 0; i < SPLICE_ELEMENTS; i++) {
            editor.conj(i);
        }
        return editor.done().value().insertAt(SPLICE_AT, SPLICE_ELEMENT);
    }

    /** The base of {@code pdt_text_edits}: {@code "Hello; world"}. */
    public static PText textBase() {
        return PText.empty("alice").insert(0, "Hello world").insert(5, ";");
    }

    /** Alice's text of {@code pdt_text_edits}: {@code "Jello; world"}. */
    public static PText textAlice() {
        return textBase().delete(0, 1).insert(0, "J");
    }

    /** Bob's text of {@code pdt_text_edits}: {@code "Hello; big world!"}. */
    public static PText textBob() {
        return textBase().withReplica("bob").insert(12, "!").insert(7, "big ");
    }

    /** The map of {@code pdt_text_spill}, over a store. */
    public static PMap<String, PText> textMap(NodeStore store) {
        return PMap.empty(store, Codec.of(String.class), PText.codec("alice"))
                .assoc("doc", PText.empty("alice").insert(0, "x".repeat(1100)))
                .assoc("note", PText.of(PdtVectors.textRuns(), "alice"));
    }

    private static List<byte[]> reachable(NodeStore store, String root) {
        List<byte[]> out = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        Deque<String> stack = new ArrayDeque<>(List.of(root));
        while (!stack.isEmpty()) {
            String cid = stack.pop();
            if (!seen.add(cid)) {
                continue;
            }
            byte[] node = store.get(cid).orElseThrow();
            out.add(node);
            List<String> links = NodeCodec.links(cid, node);
            for (int i = links.size() - 1; i >= 0; i--) {
                stack.push(links.get(i));
            }
        }
        return out;
    }

    private static String hex(byte[] bytes) {
        return HexFormat.of().formatHex(bytes);
    }
}
