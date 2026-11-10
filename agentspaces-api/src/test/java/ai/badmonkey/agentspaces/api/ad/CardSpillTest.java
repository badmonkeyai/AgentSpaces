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

import ai.badmonkey.agentspaces.api.ad.CardMirrors.RawAgentCard;
import ai.badmonkey.agentspaces.api.ad.CardMirrors.RawAppCard;
import ai.badmonkey.agentspaces.api.ad.CardMirrors.RawSpill;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.codec.Multibase;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * ISSUE-AppCardOriginFormats FR-12, FR-14, and FR-15 at the records (SPEC §6.1a.5; AppCard origin and formats,
 * 2026-11-09): a card's parts, its stub, and its completion round-trip on both kinds; every refusal of FR-14 holds at
 * decode; and the stubs and parts documents equal their golden vectors, each document's root over 65,536-byte pieces
 * being its stub's {@code spill.root}.
 */
class CardSpillTest {

    private static final CborCodec CODEC = CborCodec.defaultCodec();
    private static final Instant AT = Instant.parse("2026-11-09T09:00:00Z");
    private static final String ITEM = "https://example.org/ns/demo/v1/SharedItem";
    private static final String MESSAGE = "https://example.org/ns/demo/v1/Message";

    /** The two example-widgets components of the neutral vectors (neutral vectors, 2026-11-09). */
    static List<UiDeclaration.Component> components() {
        return List.of(new UiDeclaration.Component("share", "example-widgets", "ItemPanel", ITEM, List.of("research"), "panel"),
                new UiDeclaration.Component("ask", "example-widgets", "MessageInput", MESSAGE, List.of(), "control"));
    }

    /** The full card behind golden vectors app_card_spill_cbor and app_card_parts_cbor. */
    static AppCard board() {
        return new AppCard("aspace://zApps/app/zPeerA/example-board", PeerId.of("zPeerA"), GroupId.of("zApps"), AT, Duration.ofMinutes(15),
                "example-board", "fed", "a2ui", "example", List.of("zApps/items", "zApps/orders"), List.of(), "", "", "", "1",
                List.of("board", "items", "orders"),
                new AppCard.Ui("a2ui", "v0.9.1", List.of("example-widgets"), "board", "views", List.of("board-1"), components()))
                .withOrigin(AppCard.Origin.of("console", "example-console/example-board", "", null));
    }

    /** The full card behind golden vectors agent_card_spill_cbor and agent_card_parts_cbor. */
    static AgentCard researcher() {
        PeerId peer = PeerId.of("zPeerA");
        return new AgentCard("aspace://zApps/agent/zPeerA/researcher", peer, GroupId.of("zApps"), AT, Duration.ofMinutes(15),
                new AgentId(peer, "researcher"), "Researches a topic and writes a finding", List.of("every topic gets a finding"),
                List.of("Task"), List.of("Finding"), Map.of(), Map.of("Task", "tasks"), null,
                List.of(new CardAction("research", "Researches a topic and writes a finding", List.of("Task"), List.of("Finding"), "tasks",
                        CardAction.TAKE, new CardAction.Contract(List.of("topic-open"), List.of("finding-written"), "low", List.of()))),
                null, UiDeclaration.of(components(), new UiDeclaration.Entries(List.of(ITEM), List.of(MESSAGE))));
    }

    /** The content root of a document over 65,536-byte pieces: a CIDv1 (raw, sha2-256) over the piece table. */
    static String rootOf(byte[] document) {
        ByteArrayOutputStream table = new ByteArrayOutputStream();
        for (int at = 0; at < document.length; at += CardSpill.PIECE_SIZE) {
            table.writeBytes(Digests.sha256(Arrays.copyOfRange(document, at, Math.min(document.length, at + CardSpill.PIECE_SIZE))));
        }
        byte[] digest = Digests.sha256(table.toByteArray());
        byte[] cid = new byte[36];
        cid[0] = 0x01;
        cid[1] = 0x55;
        cid[2] = 0x12;
        cid[3] = 0x20;
        System.arraycopy(digest, 0, cid, 4, 32);
        return Multibase.base32(cid);
    }

    static AppCard boardStub() {
        byte[] document = CODEC.toBytes(board().parts());
        return board().toStub(new CardSpill(rootOf(document), document.length, board().parts().nonEmpty()));
    }

    static AgentCard researcherStub() {
        byte[] document = CODEC.toBytes(researcher().parts());
        return researcher().toStub(new CardSpill(rootOf(document), document.length, researcher().parts().nonEmpty()));
    }

    private static void refused(Object raw, Class<?> type) {
        byte[] bytes = CardMirrors.bytes(raw);
        assertThatThrownBy(() -> CODEC.fromWire(bytes, type)).as("undecodable: %s", raw).isInstanceOf(RuntimeException.class);
    }

    private static String pin(String name, byte[] bytes) throws Exception {
        Path vector = Path.of("src/test/resources/vectors/" + name + ".hex");
        String hex = HexFormat.of().formatHex(bytes);
        if (!Files.exists(vector)) {
            Files.writeString(vector, hex + "\n", StandardCharsets.UTF_8);
        }
        assertThat(hex).as(name).isEqualTo(Files.readString(vector, StandardCharsets.UTF_8).trim());
        return hex;
    }

    @Test
    void anAppCardsPartsStubAndCompletionRoundTrip() {
        AppCard full = board();
        assertThat(full.parts().nonEmpty()).as("actions is empty, so it is no part").containsExactly("reads", "views", "ui");
        AppCard stub = boardStub();
        assertThat(stub.reads()).isEmpty();
        assertThat(stub.actions()).isEmpty();
        assertThat(stub.views()).isEmpty();
        assertThat(stub.ui()).isNull();
        assertThat(stub.origin()).as("origin and formats never spill").isEqualTo(full.origin());
        assertThat(CODEC.fromWire(CODEC.toBytes(stub), AppCard.class)).isEqualTo(stub);
        AppCard.Parts parts = CODEC.fromWire(CODEC.toBytes(full.parts()), AppCard.Parts.class);
        assertThat(stub.complete(parts)).isEqualTo(full);
        assertThatThrownBy(stub::parts).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> full.complete(parts)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> stub.complete(new AppCard.Parts(List.of("zApps/items"), List.of(), List.of(), null)))
                .hasMessageContaining("parts document holds");
        CardSpill partial = new CardSpill(stub.spill().root(), stub.spill().size(), List.of("reads", "ui"));
        assertThatThrownBy(() -> full.toStub(partial)).hasMessageContaining("every non-empty part");
        @SuppressWarnings("unchecked")
        Map<String, Object> keys = CODEC.fromBytes(CODEC.toBytes(stub), Map.class);
        assertThat(new java.util.ArrayList<>(keys.keySet())).last().isEqualTo("spill");
    }

    @Test
    void anAgentCardsPartsStubAndCompletionRoundTrip() {
        AgentCard full = researcher();
        assertThat(full.parts().nonEmpty()).containsExactly("actions", "ui");
        AgentCard stub = researcherStub();
        assertThat(stub.actions()).isNull();
        assertThat(stub.ui()).isNull();
        assertThat(stub.goals()).as("goals and schemas stay inline").isEqualTo(full.goals());
        assertThat(CODEC.fromWire(CODEC.toBytes(stub), AgentCard.class)).isEqualTo(stub);
        AgentCard.Parts parts = CODEC.fromWire(CODEC.toBytes(full.parts()), AgentCard.Parts.class);
        assertThat(stub.complete(parts)).isEqualTo(full);
        AgentCard uiOnly = full.withUi(null).withActions(null).withUi(UiDeclaration.of(List.of(), null));
        assertThat(uiOnly.parts().nonEmpty()).containsExactly("ui");
        assertThat(new AgentCard.Parts(List.of(), null).nonEmpty()).as("an empty list is no part").isEmpty();
    }

    @Test
    void everyStubRuleIsRefusedAtDecode() {
        AppCard stub = boardStub();
        RawAppCard raw = CardMirrors.raw(stub);
        RawSpill spill = raw.spill();
        refused(raw.withUi(board().ui()), AppCard.class);
        RawAppCard full = CardMirrors.raw(board());
        refused(full.withSpill(spill), AppCard.class);
        refused(raw.withSpill(new RawSpill(spill.root(), spill.size(), List.of("ui", "reads"))), AppCard.class);
        refused(raw.withSpill(new RawSpill(spill.root(), spill.size(), List.of("reads", "reads"))), AppCard.class);
        refused(raw.withSpill(new RawSpill(spill.root(), spill.size(), List.of("host"))), AppCard.class);
        refused(raw.withSpill(new RawSpill(spill.root(), spill.size(), List.of())), AppCard.class);
        refused(raw.withSpill(new RawSpill(spill.root(), 0, spill.parts())), AppCard.class);
        refused(raw.withSpill(new RawSpill(spill.root(), CardSpill.MAX_SIZE + 1, spill.parts())), AppCard.class);
        byte[] cid = new byte[36];
        cid[0] = 0x01;
        cid[1] = 0x71;
        cid[2] = 0x12;
        cid[3] = 0x20;
        refused(raw.withSpill(new RawSpill(Multibase.base32(cid), spill.size(), spill.parts())), AppCard.class);
        refused(raw.withSpill(new RawSpill(spill.root().toUpperCase(java.util.Locale.ROOT), spill.size(), spill.parts())), AppCard.class);
        refused(raw.withSpill(new RawSpill(spill.root().substring(0, 58), spill.size(), spill.parts())), AppCard.class);
        refused(raw.withSpill(new RawSpill(padBitsSet(spill.root()), spill.size(), spill.parts())), AppCard.class);
        // FR-14: every list of the spill set holds its placeholder [] on a stub, so a stub that keeps reads, actions, or views
        // inline is undecodable even without a ui (AppCard origin and formats, 2026-11-09)
        refused(new RawAppCard(raw.id(), raw.issuer(), raw.group(), raw.issued(), raw.ttl(), raw.name(), raw.kind(), raw.shape(),
                raw.owner(), List.of("zApps/items"), List.of(), raw.idpClient(), raw.url(), raw.pkg(), raw.version(), List.of(), null,
                raw.host(), raw.origin(), raw.formats(), spill), AppCard.class);
        refused(new RawAppCard(raw.id(), raw.issuer(), raw.group(), raw.issued(), raw.ttl(), raw.name(), raw.kind(), raw.shape(),
                raw.owner(), List.of(), List.of("open"), raw.idpClient(), raw.url(), raw.pkg(), raw.version(), List.of(), null,
                raw.host(), raw.origin(), raw.formats(), spill), AppCard.class);
        refused(new RawAppCard(raw.id(), raw.issuer(), raw.group(), raw.issued(), raw.ttl(), raw.name(), raw.kind(), raw.shape(),
                raw.owner(), List.of(), List.of(), raw.idpClient(), raw.url(), raw.pkg(), raw.version(), List.of("board"), null,
                raw.host(), raw.origin(), raw.formats(), spill), AppCard.class);
        // a stub of an a2ui app without ui in parts names no surfaces at all
        refused(raw.withSpill(new RawSpill(spill.root(), spill.size(), List.of("reads", "views"))), AppCard.class);
        assertThat(CODEC.fromWire(CardMirrors.bytes(raw.withSpill(new RawSpill(spill.root(), CardSpill.MAX_SIZE, spill.parts()))),
                AppCard.class).spill().size()).isEqualTo(CardSpill.MAX_SIZE);

        RawAgentCard agent = CardMirrors.raw(researcherStub());
        refused(new RawAgentCard(agent.id(), agent.issuer(), agent.group(), agent.issued(), agent.ttl(), agent.agent(), agent.description(),
                agent.goals(), agent.consumes(), agent.produces(), agent.costHints(), agent.spaceBindings(), null, List.of(), null, null,
                agent.spill()), AgentCard.class);
        refused(new RawAgentCard(agent.id(), agent.issuer(), agent.group(), agent.issued(), agent.ttl(), agent.agent(), agent.description(),
                agent.goals(), agent.consumes(), agent.produces(), agent.costHints(), agent.spaceBindings(), null, null, null, null,
                new RawSpill(agent.spill().root(), agent.spill().size(), List.of("ui", "actions"))), AgentCard.class);
        refused(new RawAgentCard(agent.id(), agent.issuer(), agent.group(), agent.issued(), agent.ttl(), agent.agent(), agent.description(),
                agent.goals(), agent.consumes(), agent.produces(), agent.costHints(), agent.spaceBindings(), null, null, null, null,
                new RawSpill(agent.spill().root(), agent.spill().size(), List.of("views"))), AgentCard.class);
    }

    /** The root with its two pad bits set: the last character carries three data bits and two pad bits. */
    private static String padBitsSet(String root) {
        String alphabet = "abcdefghijklmnopqrstuvwxyz234567";
        int last = alphabet.indexOf(root.charAt(root.length() - 1));
        return root.substring(0, root.length() - 1) + alphabet.charAt(last | 0b01);
    }

    @Test
    void theStubsAndDocumentsArePinnedAndEqualTheGoldenVectors() throws Exception {
        byte[] appDocument = CODEC.toBytes(board().parts());
        byte[] agentDocument = CODEC.toBytes(researcher().parts());
        assertThat(boardStub().spill().root()).isEqualTo(rootOf(appDocument)).hasSize(CardSpill.ROOT_LENGTH).startsWith("bafkrei");
        assertThat(boardStub().spill().size()).isEqualTo(appDocument.length);
        assertThat(researcherStub().spill().root()).isEqualTo(rootOf(agentDocument));
        String appStub = pin("app_card_spill_cbor", CODEC.toBytes(boardStub()));
        String appParts = pin("app_card_parts_cbor", appDocument);
        String agentStub = pin("agent_card_spill_cbor", CODEC.toBytes(researcherStub()));
        String agentParts = pin("agent_card_parts_cbor", agentDocument);
        assertThat(GoldenJson.entry("app_card_spill_cbor")).isEqualTo(appStub);
        assertThat(GoldenJson.entry("app_card_parts_cbor")).isEqualTo(appParts);
        assertThat(GoldenJson.entry("agent_card_spill_cbor")).isEqualTo(agentStub);
        assertThat(GoldenJson.entry("agent_card_parts_cbor")).isEqualTo(agentParts);
        assertThat(GoldenJson.entry("card_spill_threshold_bytes")).isEqualTo(String.valueOf(CardSpill.SPILL_THRESHOLD_BYTES));
        assertThat(GoldenJson.entry("card_spill_warn_bytes")).isEqualTo(String.valueOf(CardSpill.WARN_THRESHOLD_BYTES));
    }

    @Test
    void theRootGrammarIsExact() {
        String root = boardStub().spill().root();
        assertThat(CardSpill.isRoot(root)).isTrue();
        assertThat(CardSpill.isRoot(padBitsSet(root))).isFalse();
        assertThat(CardSpill.isRoot("z" + root.substring(1))).isFalse();
        assertThat(CardSpill.isRoot(root + "a")).isFalse();
        assertThat(CardSpill.isRoot(null)).isFalse();
        assertThat(CardSpill.SPILL_THRESHOLD_BYTES).isEqualTo(65_536);
        assertThat(CardSpill.WARN_THRESHOLD_BYTES).isEqualTo(57_344);
        assertThat(CardSpill.MAX_SIZE).isEqualTo(9_007_199_254_740_991L);
    }
}
