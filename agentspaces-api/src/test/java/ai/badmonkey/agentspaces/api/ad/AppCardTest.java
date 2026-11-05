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
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ISSUE-AppCard FR-1 to FR-4: the card round-trips, keeps its limits, omits ui when absent, and matches the golden vector. */
class AppCardTest {

    static AppCard card() {
        return new AppCard("aspace://zApps/app/zPeerA/adjuster-desk", PeerId.of("zPeerA"), GroupId.of("zApps"), Instant.parse("2026-11-04T10:00:00Z"),
                Duration.ofMinutes(15), "adjuster-desk", "fed", "web", "claims-ops", List.of("claims-fleet/intake", "claims-fleet/views"),
                List.of("approve", "escalate"), "bma-app-adjuster-desk", "https://desk.example/", "bma-example-desk", "1.2",
                List.of("claims", "claim-detail"), new AppCard.Ui("a2ui", "v0.9.1", List.of("https://a2ui.org/specification/v0_9/catalogs/basic/catalog.json", "bma"),
                        "claim-panel", "views", List.of("claim-77", "claim-78")));
    }

    @Test
    void roundTripsAndMatchesTheGoldenVector() throws IOException {
        CborCodec codec = CborCodec.defaultCodec();
        AppCard card = card();
        byte[] bytes = codec.toBytes(card);
        assertThat(codec.fromBytes(bytes, AppCard.class)).isEqualTo(card);
        assertThat(bytes.length).as("under 4 KiB").isLessThan(4096);
        Path vector = Path.of("src/test/resources/vectors/app_card_cbor.hex");
        String hex = HexFormat.of().formatHex(bytes);
        if (!Files.exists(vector)) {
            Files.createDirectories(vector.getParent());
            Files.writeString(vector, hex + "\n", StandardCharsets.UTF_8);
        }
        assertThat(hex).as("the golden vector pins the canonical bytes; regenerate it only with a spec revision")
                .isEqualTo(Files.readString(vector, StandardCharsets.UTF_8).trim());
    }

    @Test
    void uiIsOmittedWhenAbsentAndTheLimitsHold() throws IOException {
        CborCodec codec = CborCodec.defaultCodec();
        AppCard plain = card().withUi(null);
        @SuppressWarnings("unchecked")
        Map<String, Object> keys = codec.fromBytes(codec.toBytes(plain), Map.class);
        assertThat(keys).doesNotContainKey("ui").containsKeys("name", "kind", "shape", "reads", "actions", "views");
        assertThat(plain.drawsSurfaces()).isFalse();
        assertThat(card().drawsSurfaces()).isTrue();
        assertThatThrownBy(() -> new AppCard("a", PeerId.of("zP"), GroupId.of("zG"), Instant.EPOCH, Duration.ofMinutes(1), "x", "fed", "a2ui", "", List.of(), List.of(), "", "", "", "1", List.of(), null))
                .hasMessageContaining("a2ui app names its surfaces");
        assertThatThrownBy(() -> new AppCard("a", PeerId.of("zP"), GroupId.of("zG"), Instant.EPOCH, Duration.ofMinutes(1), "x", "robot", "web", "", List.of(), List.of(), "", "", "", "1", List.of(), null))
                .hasMessageContaining("kind must be one of");
        assertThatThrownBy(() -> new AppCard.Ui("a2ui", "v0.9.1", List.of(), "agent", "carrier-pigeon", List.of())).hasMessageContaining("transport");
        assertThatThrownBy(() -> new AppCard("a", PeerId.of("zP"), GroupId.of("zG"), Instant.EPOCH, Duration.ofMinutes(1), "x", "fed", "web", "",
                java.util.Collections.nCopies(AppCard.MAX_TERMS + 1, "g/s"), List.of(), "", "", "", "1", List.of(), null)).hasMessageContaining("at most");
    }
}
