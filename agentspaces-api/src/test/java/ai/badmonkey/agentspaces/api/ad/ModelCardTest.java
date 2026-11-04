/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package ai.badmonkey.agentspaces.api.ad;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ISSUE-ModelCard FR-1: the card round-trips through the codec and matches the golden vector {@code model_card_cbor}. */
class ModelCardTest {

    static final PeerId PEER = PeerId.of("zModelIssuer");
    static final GroupId GROUP = GroupId.of("zModelGroup");
    static final Instant ISSUED = Instant.parse("2026-11-04T00:00:00Z");

    static ModelCard card() {
        Map<String, String> provenance = new LinkedHashMap<>();
        provenance.put("upstream", "hf://badmonkey/bm-classify-1b");
        provenance.put("weightRoot", "bafyroot");
        provenance.put("weightHash", "sha256:abc");
        provenance.put("admittedBy", "zRegistry");
        return new ModelCard("aspace://zModelGroup/model/bm-classify-1b@1.2", PEER, GROUP, ISSUED, Duration.ofMinutes(15),
                "bm-classify-1b", "1.2", List.of("classify", "embed"), "text", Map.of("context", "8192", "output", "512"), "Apache-2.0",
                provenance, Map.of("harness:claims-v3", "0.91"), Map.of("pricePerThousandTokens", "0.002"),
                Map.of("sites", "on-premises,dc-east", "classifications", "internal,secret", "releasable", "false"), "onnx",
                List.of(PeerId.of("zServerA"), PeerId.of("zServerB")));
    }

    @Test
    void roundTripsAndMatchesTheGoldenVector() throws IOException {
        CborCodec codec = CborCodec.defaultCodec();
        ModelCard card = card();
        byte[] bytes = codec.toBytes(card);
        assertThat(codec.fromBytes(bytes, ModelCard.class)).isEqualTo(card);
        assertThat(bytes.length).as("PR-1: under 4 KiB").isLessThan(4096);
        Path vector = Path.of("src/test/resources/vectors/model_card_cbor.hex");
        String hex = HexFormat.of().formatHex(bytes);
        if (!Files.exists(vector)) {
            Files.createDirectories(vector.getParent());
            Files.writeString(vector, hex + "\n", StandardCharsets.UTF_8);
        }
        assertThat(hex).as("the golden vector pins the canonical bytes; regenerate it only with a spec revision")
                .isEqualTo(Files.readString(vector, StandardCharsets.UTF_8).trim());
    }

    @Test
    void theCardAnswersTaskResidencyAndPriceQuestions() {
        ModelCard card = card();
        assertThat(card.serves("classify")).isTrue();
        assertThat(card.serves("chat")).isFalse();
        assertThat(card.residentAt("dc-east")).isTrue();
        assertThat(card.residentAt("cloud")).isFalse();
        assertThat(card.admitsClassification("secret")).isTrue();
        assertThat(card.admitsClassification("top-secret")).isFalse();
        assertThat(card.pricePerThousandTokens(1.0)).isEqualTo(0.002);
        assertThat(card.expiresAt()).isEqualTo(ISSUED.plus(Duration.ofMinutes(15)));
        ModelCard open = new ModelCard("aspace://g/model/x@1", PEER, GROUP, ISSUED, Duration.ofMinutes(1), "x", null, null, null, null, null, null,
                null, null, null, "tgi", null);
        assertThat(open.residentAt("anywhere")).isTrue();
        assertThat(open.admitsClassification("any")).isTrue();
        assertThat(open.pricePerThousandTokens(0.5)).isEqualTo(0.5);
        assertThat(open.engine()).as("CR-3: reserved engines are admitted").isEqualTo("tgi");
        assertThatThrownBy(() -> card.tasks().add("x")).isInstanceOf(UnsupportedOperationException.class);
        Map<String, String> evals = new LinkedHashMap<>();
        for (int i = 0; i <= ModelCard.MAX_EVALS; i++) {
            evals.put("e" + i, "1");
        }
        assertThatThrownBy(() -> new ModelCard("aspace://g/model/x@1", PEER, GROUP, ISSUED, Duration.ofMinutes(1), "x", "1", List.of(), "", Map.of(), "",
                Map.of(), evals, Map.of(), Map.of(), "onnx", List.of())).isInstanceOf(IllegalArgumentException.class);
    }
}
