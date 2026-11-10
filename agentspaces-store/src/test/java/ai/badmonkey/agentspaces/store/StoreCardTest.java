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
package ai.badmonkey.agentspaces.store;

import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.connect.AssetInvocations;
import ai.badmonkey.agentspaces.connect.DataQueryClient;
import ai.badmonkey.agentspaces.content.Messages;
import ai.badmonkey.agentspaces.planner.StoreChooser;
import ai.badmonkey.agentspaces.store.golden.StoreVectors;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The store as an asset (PDT 2 phase 3, P2-S-01, P2-S-09, P2-S-13): its card carries text values only and matches
 * its golden vector, its {@code free} follows what it holds, the {@code status}, {@code pin}, {@code challenge},
 * and {@code unpin} invokes do what the entries do, its ledger reads as rows through a {@code DataQuery}, and
 * closing the store withdraws its card.
 */
class StoreCardTest {

    private static final List<Path> GOLDEN = List.of(Path.of("..", "tools", "golden", "golden.json"),
            Path.of("..", "..", "agentspaces-spec", "golden.json"));

    private final StoreFleet fleet = new StoreFleet();

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    void theCardAndTheChallengeMatchTheirGoldenVectorsAndReadStrictly() throws Exception {
        CborCodec codec = CborCodec.defaultCodec();
        Map<String, String> vectors = StoreVectors.vectors();
        AssetCard card = StoreVectors.card();
        byte[] cardBytes = HexFormat.of().parseHex(vectors.get("asset_card_content_store_cbor"));
        AssetCard back = codec.fromWire(cardBytes, AssetCard.class);
        assertThat(codec.toBytes(back)).isEqualTo(cardBytes);
        assertThat(back.access()).containsEntry("kind", "content-store").containsEntry("capacity", Long.toString(2L << 40))
                .containsEntry("free", Long.toString((2L << 40) - (50L << 30))).containsEntry("blind", "false")
                .containsEntry("codecs", "dag-cbor,raw").containsEntry("invoke-ops", "pin,unpin,status,challenge");
        assertThat(back.costHints()).containsExactly(Map.entry(StoreChooser.PRICE_HINT, "2"));
        assertThat(card.access().values()).allMatch(v -> !v.contains(" "), "lists carry no spaces");
        byte[] challengeBytes = HexFormat.of().parseHex(vectors.get("content_challenge_cbor"));
        Messages.Challenge challenge = Messages.decode(codec, challengeBytes, Messages.Challenge.class).orElseThrow();
        assertThat(challenge).isEqualTo(StoreVectors.challenge());
        assertThat(challenge.violation()).isEmpty();

        Path found = GOLDEN.stream().filter(Files::exists).findFirst().orElse(null);
        if (Boolean.getBoolean("golden.required")) {
            assertThat(found).as("golden.json is required").isNotNull();
        }
        assumeTrue(found != null, "golden.json not present");
        Map<String, Object> golden = new ObjectMapper().readValue(found.toFile(), Map.class);
        vectors.forEach((name, value) -> assertThat(golden.get(name)).as(name).isEqualTo(value));
    }

    @Test
    void theInvokesAndRowsDoWhatTheEntriesDoAndClosingWithdrawsTheCard() throws Exception {
        StoreFleet.Peer store = fleet.newPeer("store", 71, StoreFleet.Spec.store(StorePolicy.of(1L << 30)
                .acceptRequesters(StorePolicy.Accept.OPEN)));
        StoreFleet.Peer member = fleet.newPeer("member", 72, StoreFleet.Spec.member(), "store");
        fleet.settle(6);
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> card(member).isPresent())).isTrue();
        AssetCard card = card(member).orElseThrow();
        long freeBefore = Long.parseLong(card.access().get("free"));

        AssetInvocations.AssetInvocationResult status = invoke(member, card, ContentStore.OP_STATUS, Map.of());
        assertThat(status.ok()).isTrue();
        assertThat(status.result()).containsEntry("capacity", Long.toString(1L << 30)).containsEntry("roots", "0");

        String root = StoreFleet.facts(member, "facts", 400, 2).root();
        member.content().holdTree(root);
        AssetInvocations.AssetInvocationResult pinned = invoke(member, card, ContentStore.OP_PIN, Map.of("root", root, "lease", "3600"));
        assertThat(pinned.ok()).as(pinned.toString()).isTrue();
        assertThat(fleet.runUntil(Duration.ofSeconds(30), () -> StoreFleet.receiptHolders(member, root).contains(store.id()))).isTrue();
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> card(member).map(c -> Long.parseLong(c.access().get("free")) < freeBefore)
                .orElse(false))).as("the card's free follows what the store holds").isTrue();

        String node = member.store().reachable(root).iterator().next();
        AssetInvocations.AssetInvocationResult proof = invoke(member, card, ContentStore.OP_CHALLENGE, Map.of("root", root, "picks", node));
        assertThat(proof.ok()).isTrue();
        assertThat(Base64.getDecoder().decode(proof.result().get(node))).isEqualTo(member.store().stored(node).orElseThrow());

        DataQueryClient client = new DataQueryClient(member.contentSpace(), "analyst");
        CompletableFuture<Optional<DataQueryClient.Fetched>> rows = CompletableFuture.supplyAsync(() ->
                client.fetch(store.contentStore().assetName(), Map.of(), Duration.ofSeconds(20)));
        assertThat(fleet.runUntil(Duration.ofSeconds(25), rows::isDone)).isTrue();
        assertThat(rows.get().orElseThrow().result().rows()).singleElement().satisfies(row -> assertThat(row)
                .containsEntry("root", root).containsEntry("kind", "tree").containsEntry("requester", member.id().value())
                .containsKeys("bytes", "until", "copies", "challengesPassed"));

        AssetInvocations.AssetInvocationResult unpinned = invoke(member, card, ContentStore.OP_UNPIN, Map.of("root", root));
        assertThat(unpinned.ok()).isTrue();
        assertThat(store.contentStore().ledger().holds(root)).isFalse();
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> StoreFleet.receiptHolders(member, root).isEmpty())).isTrue();

        store.contentStore().close();
        fleet.settle(4);
        assertThat(card(member)).as("the withdrawal card offers no room").get()
                .satisfies(c -> assertThat(c.access()).containsEntry("free", "0"));
        fleet.settle(20);
        assertThat(card(member)).as("the card lapses after two holding intervals, long before its 15 minutes").isEmpty();
    }

    private Optional<AssetCard> card(StoreFleet.Peer peer) {
        java.time.Instant now = fleet.clock.instant();
        return peer.discovery().find(AssetCard.class, c -> StoreChooser.KIND.equals(c.access().get("kind")))
                .stream().filter(c -> c.expiresAt().isAfter(now)).max(java.util.Comparator.comparing(AssetCard::issued));
    }

    private AssetInvocations.AssetInvocationResult invoke(StoreFleet.Peer peer, AssetCard card, String op, Map<String, String> params)
            throws Exception {
        CompletableFuture<AssetInvocations.AssetInvocationResult> answer = AssetInvocations.invoke(peer.contentSpace(), card, op, params);
        assertThat(fleet.runUntil(Duration.ofSeconds(30), answer::isDone)).as(op).isTrue();
        return answer.get();
    }
}
