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
import ai.badmonkey.agentspaces.common.id.AgentId;
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

/**
 * ISSUE-UiComponents FR-1 to FR-4, FR-7: the ui declaration round-trips, is omitted when absent (a card without it is
 * byte-identical to v0.1.25), derives its catalogs, keeps its limits, refuses a control that is not an action, and
 * matches the golden vectors on both cards.
 */
class AgentCardUiTest {

    static final String SHARED_OBJECT = "https://badmonkey.ai/ns/apps/v1/SharedObject";
    static final String CHAT_MESSAGE = "https://badmonkey.ai/ns/apps/v1/ChatMessage";

    static UiDeclaration declaration() {
        return UiDeclaration.of(List.of(
                new UiDeclaration.Component("ObjectShare", "bma-widgets", "ObjectShare", SHARED_OBJECT, List.of("open", "share"), "panel"),
                new UiDeclaration.Component("chat", "bma-widgets", "ChatInput", CHAT_MESSAGE, List.of(), "control")),
                new UiDeclaration.Entries(List.of(SHARED_OBJECT, CHAT_MESSAGE), List.of(CHAT_MESSAGE)));
    }

    static AgentCard card(UiDeclaration ui) {
        return new AgentCard("aspace://zApps/agent/zPeerA/notify", PeerId.of("zPeerA"), GroupId.of("zApps"), Instant.parse("2026-11-05T10:00:00Z"),
                Duration.ofMinutes(15), new AgentId(PeerId.of("zPeerA"), "notify"), "Writes the notice for a decision", List.of("every decision gets a notice"),
                List.of("Decision"), List.of("Notice"), Map.of(), Map.of("Decision", "desk"), null,
                List.of(new CardAction("open", "Opens a shared object", List.of("SharedObject"), List.of(), "shared", CardAction.TAKE),
                        new CardAction("share", "Shares an object", List.of("SharedObject"), List.of("SharedObject"), "shared", CardAction.TAKE)),
                null, ui);
    }

    @Test
    void roundTripsAndMatchesTheGoldenVector() throws IOException {
        CborCodec codec = CborCodec.defaultCodec();
        AgentCard card = card(declaration());
        byte[] bytes = codec.toBytes(card);
        assertThat(codec.fromBytes(bytes, AgentCard.class)).isEqualTo(card);
        assertThat(card.ui().catalogIds()).as("catalogs derived from the components").containsExactly("bma-widgets");
        assertThat(card.declaresUi()).isTrue();
        assertThat(bytes.length).as("under 4 KiB").isLessThan(4096);
        Path vector = Path.of("src/test/resources/vectors/agent_card_ui_cbor.hex");
        String hex = HexFormat.of().formatHex(bytes);
        if (!Files.exists(vector)) {
            Files.createDirectories(vector.getParent());
            Files.writeString(vector, hex + "\n", StandardCharsets.UTF_8);
        }
        assertThat(hex).as("the golden vector pins the canonical bytes; regenerate it only with a spec revision")
                .isEqualTo(Files.readString(vector, StandardCharsets.UTF_8).trim());
    }

    @Test
    void aCardWithoutTheDeclarationIsByteIdenticalToBefore() throws IOException {
        CborCodec codec = CborCodec.defaultCodec();
        AgentCard plain = card(null);
        @SuppressWarnings("unchecked")
        Map<String, Object> keys = codec.fromBytes(codec.toBytes(plain), Map.class);
        assertThat(keys).doesNotContainKey("ui").containsKeys("agent", "actions");
        assertThat(plain.declaresUi()).isFalse();
        assertThat(card(declaration()).withUi(null)).isEqualTo(plain);
        assertThat(plain.withActions(plain.actions()).ui()).isNull();
    }

    @Test
    void theLimitsAndTheControlRuleHold() {
        assertThatThrownBy(() -> card(UiDeclaration.of(List.of(new UiDeclaration.Component("x", "bma-widgets", "ObjectShare", "", List.of("fly"), "panel")), null)))
                .hasMessageContaining("control 'fly'");
        assertThatThrownBy(() -> new UiDeclaration.Component("x", "bma-widgets", "ObjectShare", "", List.of(), "banner")).hasMessageContaining("role");
        assertThatThrownBy(() -> UiDeclaration.of(List.of(new UiDeclaration.Component("same", "c", "A", "", List.of(), "panel"),
                new UiDeclaration.Component("same", "c", "B", "", List.of(), "panel")), null)).hasMessageContaining("unique");
        List<UiDeclaration.Component> many = new java.util.ArrayList<>();
        for (int i = 0; i <= UiDeclaration.MAX_COMPONENTS; i++) {
            many.add(new UiDeclaration.Component("c" + i, "c", "A", "", List.of(), "row"));
        }
        assertThatThrownBy(() -> UiDeclaration.of(many, null)).hasMessageContaining("at most");
    }

    @Test
    void anAppCardCarriesTheSameComponentsAfterItsSurfaces() throws IOException {
        CborCodec codec = CborCodec.defaultCodec();
        AppCard app = new AppCard("aspace://zApps/app/zPeerA/assembled", PeerId.of("zPeerA"), GroupId.of("zApps"), Instant.parse("2026-11-05T10:00:00Z"),
                Duration.ofMinutes(15), "assembled-desk", "fed", "a2ui", "console", List.of("zApps/conversations"), List.of("open"), "", "", "", "1",
                List.of("conversations"), new AppCard.Ui("a2ui", "v0.9.1", List.of("bma-widgets"), "console", "views", List.of("desk-1"), declaration().components()));
        byte[] bytes = codec.toBytes(app);
        assertThat(codec.fromBytes(bytes, AppCard.class)).isEqualTo(app);
        @SuppressWarnings("unchecked")
        Map<String, Object> keys = codec.fromBytes(bytes, Map.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> ui = (Map<String, Object>) keys.get("ui");
        assertThat(ui).containsKey("components");
        AppCard plain = app.withUi(new AppCard.Ui("a2ui", "v0.9.1", List.of("bma-widgets"), "console", "views", List.of("desk-1")));
        @SuppressWarnings("unchecked")
        Map<String, Object> plainUi = (Map<String, Object>) codec.fromBytes(codec.toBytes(plain), Map.class).get("ui");
        assertThat(plainUi).as("an app card without components is byte-identical to v0.1.25").doesNotContainKey("components");
        Path vector = Path.of("src/test/resources/vectors/app_card_components_cbor.hex");
        String hex = HexFormat.of().formatHex(bytes);
        if (!Files.exists(vector)) {
            Files.writeString(vector, hex + "\n", StandardCharsets.UTF_8);
        }
        assertThat(hex).isEqualTo(Files.readString(vector, StandardCharsets.UTF_8).trim());
    }
}
