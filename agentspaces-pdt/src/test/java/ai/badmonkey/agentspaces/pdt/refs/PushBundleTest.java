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

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.pdt.golden.PushVectors;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@link PushBundle} record (PDT 2 phase 5, P2-P-01, §6.10.4, rule 16): a receiver reads it strictly, and it
 * refuses a bundle outside its bounds by name; the announcing attribute {@code pdt.push} counts in canonical decimal.
 * {@code PdtVectorsConformanceTest} checks its golden vector {@code pdt_push_bundle_cbor} ({@code PushVectors}).
 */
class PushBundleTest {

    private static final CborCodec CODEC = CborCodec.defaultCodec();

    private static PushBundle vector() {
        return PushVectors.pushBundle();
    }

    @Test
    void theBundleOfTheGoldenVectorReadsBackStrictlyAndKeepsItsBytes() {
        byte[] bytes = CODEC.toBytes(vector());
        PushBundle read = CODEC.fromWire(bytes, PushBundle.class);
        assertThat(read.violation()).isEmpty();
        assertThat(read.root()).isEqualTo(vector().root());
        assertThat(read.nodes()).singleElement().isEqualTo(vector().nodes().get(0));
        assertThat(CODEC.toBytes(read)).isEqualTo(bytes);
    }
    @Test
    void aBundleOutsideItsSchemaOrBoundsIsRefused() {
        PushBundle good = vector();
        Map<String, Object> extra = new LinkedHashMap<>();
        extra.put("v", 1);
        extra.put("root", good.root());
        extra.put("seq", 3L);
        extra.put("part", 0);
        extra.put("parts", 1);
        extra.put("nodes", good.nodes());
        extra.put("x", 1);
        assertThatThrownBy(() -> CODEC.fromWire(CODEC.toBytes(extra), PushBundle.class)).as("a field the schema does not name")
                .isInstanceOf(RuntimeException.class);
        byte[] node = good.nodes().get(0);
        assertThat(new PushBundle(2, good.root(), 3, 0, 1, List.of(node)).violation()).get().asString().contains("version");
        assertThat(new PushBundle(1, "root", 3, 0, 1, List.of(node)).violation()).get().asString().contains("CID");
        assertThat(new PushBundle(1, good.root(), -1, 0, 1, List.of(node)).violation()).get().asString().contains("seq");
        assertThat(new PushBundle(1, good.root(), 3, 1, 1, List.of(node)).violation()).get().asString().contains("part 1 of 1");
        assertThat(new PushBundle(1, good.root(), 3, 0, PushBundle.MAX_PARTS + 1, List.of(node)).violation()).isPresent();
        assertThat(new PushBundle(1, good.root(), 3, 0, 1, List.of()).violation()).get().asString().contains("at least one");
        assertThat(new PushBundle(1, good.root(), 3, 0, 1, List.of(new byte[PushBundle.MAX_BYTES + 1])).violation())
                .get().asString().contains("at most " + PushBundle.MAX_BYTES);
        assertThat(PushPublisher.announced(good(Map.of(PushPublisher.ATTR, "3")))).isEqualTo(3);
        assertThat(PushPublisher.announced(good(Map.of(PushPublisher.ATTR, "03")))).as("not canonical decimal").isZero();
        assertThat(PushPublisher.announced(good(Map.of(PushPublisher.ATTR, "65")))).as("past the part bound").isZero();
        assertThat(PushPublisher.announced(good(Map.of()))).isZero();
    }

    private ai.badmonkey.agentspaces.agent.pdt.PRef good(Map<String, ?> attrs) {
        return new ai.badmonkey.agentspaces.agent.pdt.PRef(1, "n", "map", vector().root(), List.of(), 1, 1, null,
                ai.badmonkey.agentspaces.agent.pdt.PRef.MODE_MULTI, ai.badmonkey.agentspaces.agent.pdt.PRef.SEALING_NONE, null,
                attrs.isEmpty() ? null : attrs);
    }
}
