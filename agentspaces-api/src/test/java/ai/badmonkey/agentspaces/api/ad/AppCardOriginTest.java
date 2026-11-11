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

import ai.badmonkey.agentspaces.api.ad.CardMirrors.RawAppCard;
import ai.badmonkey.agentspaces.api.ad.CardMirrors.RawOrigin;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * ISSUE-AppCardOriginFormats FR-1 to FR-4 and FR-8 (AppCard origin and formats, 2026-11-09): the closed kind with
 * {@code assembled}'s new meaning and no {@code realm}, the open {@code origin} and {@code formats} appended after
 * {@code host}, their bounds and grammars refused at decode, the builders, the predicates, and the golden vector.
 */
class AppCardOriginTest {

    private static final CborCodec CODEC = CborCodec.defaultCodec();

    /**
     * The card of golden vector app_card_origin_formats_cbor (neutral vectors, 2026-11-09; the realm values of SPEC
     * v0.1.33, REALM.md §5 revision 4, 2026-11-11): the realm's semver in {@code origin.version}, the form and the
     * specification commit in {@code realm.format}, {@code realm.commit}, {@code realm.digest} (the SHA-256 of the
     * document {@code "realm:\n  name: example-realm\n  version: 1.0.0\n"}), and the app link as {@code url}.
     */
    static AppCard imported() {
        Map<String, String> attrs = new LinkedHashMap<>();
        attrs.put("realm.namespace", "urn:realm:example-realm#");
        attrs.put("realm.reads", "Item,Order");
        attrs.put("realm.format", "yaml@89faba629f66e715421ba0f1b535142e4d2ef3aa");
        attrs.put("realm.digest", "sha256:ae7219d1e1e24b382d3ffc38aeb92052628db2d0f55d5dc75b3a72fd19965637");
        attrs.put("realm.commit", "0123456789abcdef0123456789abcdef01234567");
        return new AppCard("aspace://zApps/app/zPeerA/example-app", PeerId.of("zPeerA"), GroupId.of("zApps"),
                Instant.parse("2026-11-09T09:00:00Z"), Duration.ofMinutes(15), "example-app", "fed", "static", "example-realm",
                List.of(), List.of(), "", "app://example-realm/example-app", "", "1", List.of(), null)
                .withOrigin(AppCard.Origin.of("realm", "example-realm/example-app", "1.0.0", attrs))
                .withFormats(List.of("text/html", "application/geo+json", "urn:realm:example-realm#"));
    }

    private static void refused(RawAppCard raw) {
        byte[] bytes = CardMirrors.bytes(raw);
        assertThatThrownBy(() -> CODEC.fromWire(bytes, AppCard.class)).as("undecodable: %s", raw).isInstanceOf(RuntimeException.class);
    }

    @Test
    void originAndFormatsRoundTripAfterTheHostAndPinTheGoldenVector() throws Exception {
        AppCard card = imported();
        byte[] bytes = CODEC.toBytes(card);
        assertThat(CODEC.fromWire(bytes, AppCard.class)).isEqualTo(card);
        assertThat(CardMirrors.bytes(CardMirrors.raw(card))).as("the mirror encodes the real card's bytes").isEqualTo(bytes);
        @SuppressWarnings("unchecked")
        Map<String, Object> keys = CODEC.fromBytes(CODEC.toBytes(card.withHost(AppCardHostTest.published(card.issued()).host())), Map.class);
        List<String> order = new ArrayList<>(keys.keySet());
        assertThat(order.subList(order.size() - 3, order.size())).containsExactly("host", "origin", "formats");
        @SuppressWarnings("unchecked")
        Map<String, Object> origin = (Map<String, Object>) keys.get("origin");
        @SuppressWarnings("unchecked")
        Map<String, Object> attrs = (Map<String, Object>) origin.get("attrs");
        assertThat(new ArrayList<>(attrs.keySet())).as("the P2 key order: shorter UTF-8 first, then bytewise")
                .containsExactly("realm.reads", "realm.commit", "realm.digest", "realm.format", "realm.namespace");
        Path vector = Path.of("src/test/resources/vectors/app_card_origin_formats_cbor.hex");
        String hex = HexFormat.of().formatHex(bytes);
        if (!Files.exists(vector)) {
            Files.writeString(vector, hex + "\n", StandardCharsets.UTF_8);
        }
        assertThat(hex).isEqualTo(Files.readString(vector, StandardCharsets.UTF_8).trim());
        assertThat(GoldenJson.entry("app_card_origin_formats_cbor")).isEqualTo(hex);
    }

    @Test
    void aCardWithoutTheNewFieldsKeepsItsV0131Bytes() {
        AppCard card = AppCardHostTest.published(Instant.parse("2026-11-06T09:00:00Z"));
        AppCard legacy = new AppCard(card.id(), card.issuer(), card.group(), card.issued(), card.ttl(), card.name(), card.kind(),
                card.shape(), card.owner(), card.reads(), card.actions(), card.idpClient(), card.url(), card.pkg(), card.version(),
                card.views(), card.ui(), card.host());
        assertThat(CODEC.toBytes(legacy)).isEqualTo(CODEC.toBytes(card));
        @SuppressWarnings("unchecked")
        Map<String, Object> keys = CODEC.fromBytes(CODEC.toBytes(card), Map.class);
        assertThat(keys).doesNotContainKeys("origin", "formats", "spill");
    }

    @Test
    void theKindIsClosedAndNoKindIsTiedToAnOrigin() {
        assertThat(AppCard.KINDS).containsExactly("peer", "fed", "gateway", "assembled");
        RawAppCard raw = CardMirrors.raw(imported());
        refused(raw.withKind("realm"));
        refused(raw.withKind("drafted"));
        for (String kind : AppCard.KINDS) {
            byte[] bytes = CardMirrors.bytes(raw.withKind(kind));
            assertThat(CODEC.fromWire(bytes, AppCard.class).kind()).as("kind %s with origin realm decodes", kind).isEqualTo(kind);
        }
        AppCard drafted = imported().withOrigin(AppCard.Origin.of("console", "example-console/example-board", "", null));
        assertThat(CODEC.fromWire(CODEC.toBytes(drafted), AppCard.class).fromOrigin("console")).isTrue();
        assertThatThrownBy(() -> new AppCard("a", PeerId.of("zP"), GroupId.of("zG"), Instant.EPOCH, Duration.ofMinutes(1), "x", "realm",
                "web", "", List.of(), List.of(), "", "", "", "1", List.of(), null)).hasMessageContaining("kind must be one of");
    }

    @Test
    void theOriginsBoundsAndGrammarsAreRefusedAtDecode() {
        RawAppCard raw = CardMirrors.raw(imported());
        RawOrigin ok = raw.origin();
        refused(raw.withOrigin(new RawOrigin("Realm Two", ok.ref(), "", null)));
        refused(raw.withOrigin(new RawOrigin("", ok.ref(), "", null)));
        refused(raw.withOrigin(new RawOrigin("-realm", ok.ref(), "", null)));
        refused(raw.withOrigin(new RawOrigin("realm.", ok.ref(), "", null)));
        refused(raw.withOrigin(new RawOrigin("r".repeat(65), ok.ref(), "", null)));
        refused(raw.withOrigin(new RawOrigin("realm", "", "", null)));
        refused(raw.withOrigin(new RawOrigin("realm", "x".repeat(257), "", null)));
        refused(raw.withOrigin(new RawOrigin("realm", "a\u0007b", "", null)));
        refused(raw.withOrigin(new RawOrigin("realm", ok.ref(), "v".repeat(65), null)));
        refused(raw.withOrigin(new RawOrigin("realm", ok.ref(), "1\n", null)));
        refused(raw.withOrigin(new RawOrigin("realm", ok.ref(), "", Map.of())));
        refused(raw.withOrigin(new RawOrigin("realm", ok.ref(), "", Map.of("namespace", "x"))));
        refused(raw.withOrigin(new RawOrigin("realm", ok.ref(), "", Map.of("realm.", "x"))));
        refused(raw.withOrigin(new RawOrigin("realm", ok.ref(), "", Map.of("realm.Reads", "x"))));
        refused(raw.withOrigin(new RawOrigin("realm", ok.ref(), "", Map.of("other.reads", "x"))));
        refused(raw.withOrigin(new RawOrigin("realm", ok.ref(), "", Map.of("realm." + "k".repeat(123), "x"))));
        refused(raw.withOrigin(new RawOrigin("realm", ok.ref(), "", Map.of("realm.reads", "x".repeat(257)))));
        refused(raw.withOrigin(new RawOrigin("realm", ok.ref(), "", Map.of("realm.reads", "a\u007fb"))));
        Map<String, String> nine = new LinkedHashMap<>();
        for (int i = 0; i < 9; i++) {
            nine.put("realm.k" + i, "v");
        }
        refused(raw.withOrigin(new RawOrigin("realm", ok.ref(), "", nine)));
        // the edges that hold: 64-unit system, 256-unit ref, 64-unit version, 128-unit key, 256-unit value, eight entries
        Map<String, String> eight = new LinkedHashMap<>(nine);
        eight.remove("realm.k8");
        eight.put("realm.k0", "v".repeat(256));
        String system = "com.example." + "s".repeat(52);
        AppCard edge = imported().withOrigin(AppCard.Origin.of(system, "é".repeat(256), "v".repeat(64),
                Map.of(system + "." + "k".repeat(128 - system.length() - 1), "")));
        assertThat(CODEC.fromWire(CODEC.toBytes(edge), AppCard.class)).isEqualTo(edge);
        AppCard full = imported().withOrigin(AppCard.Origin.of("realm", "r", "", eight));
        assertThat(CODEC.fromWire(CODEC.toBytes(full), AppCard.class)).isEqualTo(full);
        assertThat(AppCard.Origin.of("realm", "r", null, Map.of())).isEqualTo(new AppCard.Origin("realm", "r", "", null));
        assertThatThrownBy(() -> new AppCard.Origin("realm", "r", "", Map.of())).hasMessageContaining("attrs");
    }

    @Test
    void theFormatsBoundsAndGrammarsAreRefusedAtDecode() {
        RawAppCard raw = CardMirrors.raw(imported());
        refused(raw.withFormats(List.of()));
        refused(raw.withFormats(List.of("text/html", "text/html")));
        refused(raw.withFormats(List.of("Application/Geo+JSON")));
        refused(raw.withFormats(List.of("text/html;charset=utf-8")));
        refused(raw.withFormats(List.of("text/html; charset=utf-8")));
        refused(raw.withFormats(List.of("text")));
        refused(raw.withFormats(List.of("text/")));
        refused(raw.withFormats(List.of("")));
        refused(raw.withFormats(List.of("urn:" + "x".repeat(253))));
        refused(raw.withFormats(List.of("1urn:x")));
        refused(raw.withFormats(List.of("https://example.org/a b")));
        refused(raw.withFormats(List.of("https://example.org/a b")));
        refused(raw.withFormats(List.of("HTTPS://example.org/")));
        List<String> seventeen = new ArrayList<>();
        for (int i = 0; i < 17; i++) {
            seventeen.add("text/x-" + i);
        }
        refused(raw.withFormats(seventeen));
        List<String> sixteen = new ArrayList<>(seventeen.subList(0, 16));
        sixteen.set(15, "urn:" + "x".repeat(252));
        AppCard edge = imported().withFormats(sixteen);
        assertThat(CODEC.fromWire(CODEC.toBytes(edge), AppCard.class)).isEqualTo(edge);
    }

    @Test
    void theBuilderNormalisesFormatsAndTheCanonicalConstructorRefusesTheRest() {
        AppCard card = imported().withFormats(List.of("Text/HTML", "text/html", "urn:Realm:X#", "application/geo+json"));
        assertThat(card.formats()).containsExactly("text/html", "urn:Realm:X#", "application/geo+json");
        assertThat(imported().withFormats(List.of()).formats()).isNull();
        assertThat(imported().withFormats(null).formats()).isNull();
        AppCard base = imported().withFormats(null);
        assertThatThrownBy(() -> new AppCard(base.id(), base.issuer(), base.group(), base.issued(), base.ttl(), base.name(), base.kind(),
                base.shape(), base.owner(), base.reads(), base.actions(), base.idpClient(), base.url(), base.pkg(), base.version(),
                base.views(), base.ui(), base.host(), base.origin(), List.of("Text/HTML"), null)).hasMessageContaining("lower-case");
        assertThatThrownBy(() -> imported().withFormats(List.of("text/html;q=1"))).hasMessageContaining("no parameter");
    }

    @Test
    void thePredicatesReadOriginAndFormats() {
        AppCard card = imported();
        assertThat(card.fromOrigin("realm")).isTrue();
        assertThat(card.fromOrigin("console")).isFalse();
        assertThat(card.fromOrigin("realm", "example-realm/*")).isTrue();
        assertThat(card.fromOrigin("realm", "example-realm/example-app")).isTrue();
        assertThat(card.fromOrigin("realm", "example-realm/other")).isFalse();
        assertThat(card.fromOrigin("realm", "example/*")).as("the prefix ends at the slash").isFalse();
        assertThat(card.handles("application/geo+json")).isTrue();
        assertThat(card.handles("Application/GEO+json")).isTrue();
        assertThat(card.handles("application/*")).isTrue();
        assertThat(card.handles("image/*")).isFalse();
        assertThat(card.handles("urn:realm:example-realm#")).isTrue();
        assertThat(card.handles("URN:realm:example-realm#")).as("an IRI matches exactly").isFalse();
        AppCard bare = card.withOrigin(null).withFormats(null);
        assertThat(bare.fromOrigin("realm")).isFalse();
        assertThat(bare.handles("text/html")).isFalse();
    }
}
