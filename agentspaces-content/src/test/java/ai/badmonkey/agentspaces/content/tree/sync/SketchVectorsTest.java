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
package ai.badmonkey.agentspaces.content.tree.sync;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.content.Messages;
import ai.badmonkey.agentspaces.content.tree.Cids;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The golden vectors of PDT 2 phase 5 for the sketch forms (P2-K-02, P2-K-05, rules 12 and 13 of §6.10), so the
 * Python, TypeScript, Ruby, and Rust ports pin the same bytes in phase 7:
 *
 * <ul>
 * <li>{@code content_tree_want_sketch_cbor}: a sketch {@code TREE_WANT}, req 42, {@code root ""}, no haves, budget
 * 65536, whose sketch is the negentropy initiation over the asker's set: the dag-cbor CIDs of the UTF-8 texts
 * {@code "aspace:sketch:<i>"} for i in 0 to 39, each at timestamp 0 ({@link SketchSet}).</li>
 * <li>{@code content_tree_end_sketch_cbor}: the holder's {@code TREE_END}, req 42, reason 5, an empty frontier, and
 * the negentropy reply in the appended {@code sketch}, over the CIDs of i in 0 to 29 and 40 to 49.</li>
 * </ul>
 * The RIBLT vector of the phase, {@code pdt_riblt_symbols}, is generated and checked with the other {@code pdt_*}
 * vectors in {@code agentspaces-pdt} ({@code PushVectors}).
 */
class SketchVectorsTest {

    private static final List<Path> GOLDEN_PATHS = List.of(
            Path.of("..", "tools", "golden", "golden.json"),
            Path.of("..", "..", "agentspaces-spec", "golden.json"));
    private static final CborCodec CODEC = CborCodec.defaultCodec();
    private static Map<String, Object> golden;

    @BeforeAll
    @SuppressWarnings("unchecked")
    static void load() throws Exception {
        Path found = GOLDEN_PATHS.stream().filter(Files::exists).findFirst().orElse(null);
        if (Boolean.getBoolean("golden.required")) {
            assertThat(found).as("golden.json is required (golden.required=true)").isNotNull();
        }
        assumeTrue(found != null, "golden.json not present");
        golden = new ObjectMapper().readValue(found.toFile(), Map.class);
    }

    private static List<String> cids(int from, int to) {
        List<String> out = new ArrayList<>();
        for (int i = from; i < to; i++) {
            out.add(Cids.of(("aspace:sketch:" + i).getBytes(StandardCharsets.UTF_8), Cids.DAG_CBOR));
        }
        return out;
    }

    static Map<String, String> computed() {
        byte[] want = new Negentropy(SketchSet.of(cids(0, 40)).storage(), SketchServer.MAX_SKETCH_BYTES).initiate();
        List<String> holder = new ArrayList<>(cids(0, 30));
        holder.addAll(cids(40, 50));
        byte[] reply = new Negentropy(SketchSet.of(holder).storage(), SketchServer.MAX_SKETCH_BYTES).reconcile(want);
        HexFormat hex = HexFormat.of();
        return Map.of(
                "content_tree_want_sketch_cbor", hex.formatHex(CODEC.toBytes(new Messages.TreeWant(Messages.TREE_WANT, 42, "",
                        List.of(), SketchServer.MAX_SKETCH_BYTES, want))),
                "content_tree_end_sketch_cbor", hex.formatHex(CODEC.toBytes(new Messages.TreeEnd(Messages.TREE_END, 42, List.of(),
                        Messages.END_SKETCH, reply))));
    }

    @Test
    void goldenJsonHoldsTheSketchVectorsAsThisBuildComputesThem() {
        Map<String, String> computed = computed();
        computed.forEach((name, value) -> System.out.println("vector " + name + " = " + value));
        for (Map.Entry<String, String> vector : computed.entrySet()) {
            assertThat(golden).as(vector.getKey()).containsEntry(vector.getKey(), vector.getValue());
        }
    }

    @Test
    void theSketchFormsReadStrictlyAndRoundTripTheirBytes() {
        Map<String, String> computed = computed();
        byte[] wantBytes = HexFormat.of().parseHex(computed.get("content_tree_want_sketch_cbor"));
        Messages.TreeWant want = Messages.decode(CODEC, wantBytes, Messages.TreeWant.class).orElseThrow();
        assertThat(want.reconciles()).isTrue();
        assertThat(want.violation()).isEmpty();
        assertThat(CODEC.toBytes(want)).isEqualTo(wantBytes);
        byte[] endBytes = HexFormat.of().parseHex(computed.get("content_tree_end_sketch_cbor"));
        Messages.TreeEnd end = Messages.decode(CODEC, endBytes, Messages.TreeEnd.class).orElseThrow();
        assertThat(end.violation()).isEmpty();
        assertThat(end.reason()).isEqualTo(Messages.END_SKETCH);
        assertThat(CODEC.toBytes(end)).isEqualTo(endBytes);
        // the asker finds the holder's ten extra roots and its own ten
        List<byte[]> have = new ArrayList<>();
        List<byte[]> need = new ArrayList<>();
        Negentropy asker = new Negentropy(SketchSet.of(cids(0, 40)).storage(), SketchServer.MAX_SKETCH_BYTES);
        asker.initiate();
        assertThat(asker.reconcile(end.sketch(), have, need)).isEmpty();
        assertThat(have).hasSize(10);
        assertThat(need).hasSize(10);
    }
}
