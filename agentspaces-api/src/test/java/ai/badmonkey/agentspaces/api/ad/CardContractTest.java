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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ISSUE-CardContract FR-1 to FR-3: the contract is additive, omitted when empty, and pinned by a golden vector. */
class CardContractTest {

    static CardAction withContract() {
        return new CardAction("scheduleVisit", "Schedules a site visit for a claim", List.of("claims.SiteVisitRequest#v1"),
                List.of("claims.SiteVisit#v1"), "visits", CardAction.TAKE,
                new CardAction.Contract(List.of("adjuster-assigned", "exists(Claim where amount > 1000)"), List.of("claims.SiteVisit#v1", "visit-scheduled"), "PT2H", List.of()));
    }

    @Test
    void anActionWithoutAContractIsTheOldAction() throws IOException {
        CborCodec codec = CborCodec.defaultCodec();
        CardAction plain = new CardAction("one", "", List.of("A#v1"), List.of(), null, CardAction.TAKE);
        assertThat(plain.contract()).isNull();
        @SuppressWarnings("unchecked")
        Map<String, Object> keys = codec.fromBytes(codec.toBytes(plain), Map.class);
        assertThat(keys).doesNotContainKey("contract").containsKeys("name", "description", "consumes", "produces", "kind");
        assertThat(CardAction.Contract.of(List.of(), List.of(), "")).as("an empty contract never reaches the wire").isNull();
        assertThat(plain.withContract(CardAction.Contract.of(List.of("ready"), List.of(), null)).contract().pre()).containsExactly("ready");
        assertThatThrownBy(() -> new CardAction.Contract(java.util.Collections.nCopies(CardAction.MAX_CONTRACT_TERMS + 1, "x"), List.of(), "", List.of()))
                .hasMessageContaining("at most");
        assertThatThrownBy(() -> new CardAction.Contract(List.of(), List.of(), "c".repeat(CardAction.MAX_COST_LENGTH + 1), List.of()))
                .hasMessageContaining("cost hint");
    }

    @Test
    void roundTripsAndMatchesTheGoldenVector() throws IOException {
        CborCodec codec = CborCodec.defaultCodec();
        CardAction action = withContract();
        byte[] bytes = codec.toBytes(action);
        assertThat(codec.fromBytes(bytes, CardAction.class)).isEqualTo(action);
        Path vector = Path.of("src/test/resources/vectors/card_action_contract_cbor.hex");
        String hex = HexFormat.of().formatHex(bytes);
        if (!Files.exists(vector)) {
            Files.createDirectories(vector.getParent());
            Files.writeString(vector, hex + "\n", StandardCharsets.UTF_8);
        }
        assertThat(hex).as("the golden vector pins the canonical bytes; regenerate it only with a spec revision")
                .isEqualTo(Files.readString(vector, StandardCharsets.UTF_8).trim());
    }
}
