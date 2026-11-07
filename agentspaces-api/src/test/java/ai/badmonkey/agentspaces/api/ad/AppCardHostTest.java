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

/** v0.1.28 (spiral 5, S5-P-01): the host group on an AppCard, additive and pinned. */
class AppCardHostTest {

    private final CborCodec codec = CborCodec.defaultCodec();
    private final Instant at = Instant.parse("2026-11-06T09:00:00Z");

    private AppCard plain() {
        return new AppCard("aspace://zApps/app/zPeerA/claims-chat", PeerId.of("zPeerA"), GroupId.of("zApps"), at,
                Duration.ofMinutes(15), "Claims chat", "fed", "a2ui", "claims", List.of("zApps/conversations"), List.of("ask"), "", "", "", "1",
                List.of("conversations"), new AppCard.Ui("a2ui", "v0.9.1", List.of("bma-widgets", "bma-viz"), "summariser", "views", List.of("chat-1")));
    }

    @Test
    void aPublishedAppCarriesItsHostLastAndAnUnpublishedOneKeepsItsBytes() throws Exception {
        AppCard published = plain().withHost(new AppCard.Host("https://chat.acme.example", 7801, "zHostPeer", "/", "tenant", "summariser", at, "serving"));
        byte[] bytes = codec.toBytes(published);
        assertThat(codec.fromBytes(bytes, AppCard.class)).isEqualTo(published);
        assertThat(published.servesEndUsers()).isTrue();
        assertThat(plain().servesEndUsers()).isFalse();
        @SuppressWarnings("unchecked")
        Map<String, Object> keys = codec.fromBytes(bytes, Map.class);
        assertThat(keys).containsKey("host");
        assertThat(new java.util.ArrayList<>(keys.keySet())).last().isEqualTo("host");
        @SuppressWarnings("unchecked")
        Map<String, Object> withoutHost = codec.fromBytes(codec.toBytes(plain()), Map.class);
        assertThat(withoutHost).as("a card without a host is byte-identical to v0.1.27").doesNotContainKey("host");
        Path vector = Path.of("src/test/resources/vectors/app_card_host_cbor.hex");
        String hex = HexFormat.of().formatHex(bytes);
        if (!Files.exists(vector)) {
            Files.writeString(vector, hex + "\n", StandardCharsets.UTF_8);
        }
        assertThat(hex).isEqualTo(Files.readString(vector, StandardCharsets.UTF_8).trim());
    }

    @Test
    void aHostIsBounded() {
        assertThatThrownBy(() -> new AppCard.Host("", 0, "", "/", "tenant", "a", at, "serving")).hasMessageContaining("url");
        assertThatThrownBy(() -> new AppCard.Host("http://x", 70000, "", "/", "tenant", "a", at, "serving")).hasMessageContaining("port");
        assertThatThrownBy(() -> new AppCard.Host("http://x", 0, "", "/", "password", "a", at, "serving")).hasMessageContaining("signOn");
        assertThatThrownBy(() -> new AppCard.Host("http://x", 0, "", "/", "none", "a", at, "gone")).hasMessageContaining("status");
        AppCard.Host defaults = new AppCard.Host("http://127.0.0.1:7801", 7801, null, null, null, null, at, null);
        assertThat(defaults.path()).isEqualTo("/");
        assertThat(defaults.signOn()).isEqualTo("tenant");
        assertThat(defaults.status()).isEqualTo("serving");
    }
}
