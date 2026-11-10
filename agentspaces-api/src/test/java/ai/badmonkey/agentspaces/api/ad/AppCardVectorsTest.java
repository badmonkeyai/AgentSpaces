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
import org.junit.jupiter.api.Test;

import java.util.HexFormat;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SPEC annex I.3 (v0.1.32; AppCard origin and formats, 2026-11-09): the strict reader decodes every accept vector of
 * the AppCard and the card spill to exactly its bytes, completes each stub from its parts document, and refuses each of
 * the eighteen refusal vectors. With {@code -Dgolden.required=true} a missing entry fails.
 */
class AppCardVectorsTest {

    private static final CborCodec CODEC = CborCodec.defaultCodec();
    private static final List<String> APP_CARD_REFUSALS = List.of(
            "adv_app_card_kind_realm_cbor", "adv_app_card_sign_on_tenant_cbor", "adv_app_card_formats_empty_cbor",
            "adv_app_card_origin_attrs_empty_cbor", "adv_app_card_formats_bound_cbor", "adv_app_card_formats_upper_cbor",
            "adv_app_card_formats_duplicate_cbor", "adv_app_card_formats_parameter_cbor", "adv_app_card_origin_system_cbor",
            "adv_app_card_origin_attrs_prefix_cbor", "adv_app_card_origin_ref_bound_cbor", "adv_app_card_spill_placeholder_cbor",
            "adv_app_card_spill_parts_order_cbor", "adv_app_card_spill_parts_unknown_cbor", "adv_app_card_spill_parts_empty_cbor",
            "adv_app_card_spill_root_codec_cbor", "adv_app_card_spill_size_zero_cbor");

    private static byte[] bytes(String key) {
        return HexFormat.of().parseHex(GoldenJson.entry(key));
    }

    @Test
    void everyAcceptVectorDecodesToItsOwnBytes() {
        for (String key : List.of("app_card_host_cbor", "app_card_origin_formats_cbor", "app_card_spill_cbor")) {
            byte[] wire = bytes(key);
            assertThat(CODEC.toBytes(CODEC.fromWire(wire, AppCard.class))).as(key).isEqualTo(wire);
        }
        byte[] agent = bytes("agent_card_spill_cbor");
        assertThat(CODEC.toBytes(CODEC.fromWire(agent, AgentCard.class))).isEqualTo(agent);
        assertThat(CODEC.fromWire(bytes("agent_card_ui_cbor"), AgentCard.class).ui().catalogIds()).containsExactly("example-widgets");
        assertThat(CODEC.fromWire(bytes("app_card_host_cbor"), AppCard.class).host().signOn()).isEqualTo("idp");
    }

    @Test
    void eachStubCompletesFromItsPartsDocumentAndTheRootNamesTheDocument() {
        byte[] appParts = bytes("app_card_parts_cbor");
        AppCard appStub = CODEC.fromWire(bytes("app_card_spill_cbor"), AppCard.class);
        assertThat(appStub.spill().size()).isEqualTo(appParts.length);
        assertThat(appStub.spill().root()).isEqualTo(CardSpillTest.rootOf(appParts));
        AppCard app = appStub.complete(CODEC.fromWire(appParts, AppCard.Parts.class));
        assertThat(app.spill()).isNull();
        assertThat(app.ui().components()).hasSize(2);
        byte[] agentParts = bytes("agent_card_parts_cbor");
        AgentCard agentStub = CODEC.fromWire(bytes("agent_card_spill_cbor"), AgentCard.class);
        assertThat(agentStub.spill().root()).isEqualTo(CardSpillTest.rootOf(agentParts));
        AgentCard agent = agentStub.complete(CODEC.fromWire(agentParts, AgentCard.Parts.class));
        assertThat(agent.actions()).singleElement().extracting(CardAction::contract).isNotNull();
        assertThat(agent.ui()).isEqualTo(CODEC.fromWire(bytes("agent_card_ui_cbor"), AgentCard.class).ui());
    }

    @Test
    void everyRefusalVectorIsUndecodable() {
        for (String key : APP_CARD_REFUSALS) {
            byte[] wire = bytes(key);
            assertThatThrownBy(() -> CODEC.fromWire(wire, AppCard.class)).as(key).isInstanceOf(RuntimeException.class);
        }
        byte[] agent = bytes("adv_agent_card_spill_actions_cbor");
        assertThatThrownBy(() -> CODEC.fromWire(agent, AgentCard.class)).isInstanceOf(RuntimeException.class);
        assertThat(GoldenJson.entry("card_spill_threshold_bytes")).isEqualTo("65536");
        assertThat(GoldenJson.entry("card_spill_warn_bytes")).isEqualTo("57344");
    }
}
