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

import ai.badmonkey.agentspaces.agent.content.StorePin;
import ai.badmonkey.agentspaces.api.ad.AssetCard;
import ai.badmonkey.agentspaces.common.crypto.GroupKey;
import ai.badmonkey.agentspaces.content.FetchOptions;
import ai.badmonkey.agentspaces.content.tree.TreeFetch;
import ai.badmonkey.agentspaces.pdt.stores.Stores;
import ai.badmonkey.agentspaces.pdt.values.PMap;
import ai.badmonkey.agentspaces.planner.StoreChooser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A blind store (PDT 2 phase 3, P2-S-08, P2-Q-SEC-01; srad/agentspaces-pdt2.md §3.2.5): without the content key
 * it holds a sealed-nodes tree, serves it to a key holder who opens it, answers a challenge, and advertises
 * {@code blind}; a wire tap at the store and a read of its node store find no plaintext. It refuses a plaintext
 * tree, and the planner never chooses it for plain content.
 */
class BlindStoreTest {

    private static final byte[] MARKER = "secret-value-".getBytes(StandardCharsets.UTF_8);

    private final StoreFleet fleet = new StoreFleet();

    @AfterEach
    void tearDown() {
        fleet.close();
    }

    private static boolean contains(byte[] haystack, byte[] needle) {
        outer:
        for (int i = 0; i + needle.length <= haystack.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return true;
        }
        return false;
    }

    @Test
    void aBlindStoreServesASealedTreeAKeyHolderOpensAndSeesNoPlaintext() throws Exception {
        GroupKey key = GroupKey.generate();
        StoreFleet.Peer blind = fleet.newPeer("blind", 51, StoreFleet.Spec.store(StorePolicy.of(1L << 30).blind(true)).sealedNodes(null));
        StoreFleet.Peer writer = fleet.newPeer("writer", 52, StoreFleet.Spec.member().sealedNodes(key), "blind");
        StoreFleet.Peer reader = fleet.newPeer("reader", 53, StoreFleet.Spec.member().sealedNodes(key), "blind");
        ByteArrayOutputStream tapped = new ByteArrayOutputStream();
        for (String other : List.of("writer", "reader")) {
            fleet.network.tap(other, "blind", tapped::writeBytes);
            fleet.network.tap("blind", other, tapped::writeBytes);
        }
        fleet.settle(6);
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> !writer.discovery().find(AssetCard.class,
                c -> StoreChooser.KIND.equals(c.access().get("kind"))).isEmpty())).isTrue();
        AssetCard card = writer.discovery().find(AssetCard.class, c -> StoreChooser.KIND.equals(c.access().get("kind"))).get(0);
        assertThat(card.access()).containsEntry("blind", "true").containsEntry("sealing", "sealed-nodes");

        PMap<String, String> value = StoreFleet.facts(writer, "sealed", 1_000, 9);
        String root = value.root();
        Stores.Pinning pinning = writer.pdt().storeFor(root, StoreChooser.Need.of(0, 1), Duration.ofDays(1));
        assertThat(pinning.chosen()).extracting(AssetCard::issuer).containsExactly(blind.id());
        assertThat(fleet.runUntil(Duration.ofSeconds(60), () -> StoreFleet.receiptHolders(writer, root).size() == 1))
                .as(blind.contentStore().status().toString()).isTrue();
        assertThat(writer.pdt().stores().receipts(root).get(0).blind()).isTrue();
        var challenge = writer.pdt().challenge(root, blind.id());
        assertThat(fleet.runUntil(Duration.ofSeconds(20), challenge::isDone)).isTrue();
        assertThat(challenge.get().passed()).as("a challenge over sealed nodes needs no key").isTrue();

        fleet.kill(writer);
        TreeFetch fetch = reader.content().fetchTree(root, List.of(), FetchOptions.defaults().prefer(blind.id()));
        assertThat(fleet.runUntil(Duration.ofSeconds(60), fetch::complete)).as(fetch.trace().toString()).isTrue();
        assertThat(PMap.load(reader.store(), root, StoreFleet.STR, StoreFleet.STR).get("key-7").orElseThrow()).startsWith("secret-value-7-");

        assertThat(tapped.size()).as("the tap saw the transfer").isGreaterThan(10_000);
        assertThat(contains(tapped.toByteArray(), MARKER)).as("no plaintext crossed the store's links").isFalse();
        for (String cid : blind.store().backend().cids()) {
            byte[] stored = blind.store().backend().get(cid, 0).orElseThrow();
            assertThat(contains(stored, MARKER)).as("the store's node " + cid).isFalse();
        }
    }

    @Test
    void aBlindStoreRefusesAPlaintextTreeAndThePlannerSkipsItForPlainContent() throws Exception {
        StoreFleet.Peer blind = fleet.newPeer("blind", 61, StoreFleet.Spec.store(StorePolicy.of(1L << 30).blind(true)
                .acceptRequesters(StorePolicy.Accept.OPEN)));
        StoreFleet.Peer writer = fleet.newPeer("writer", 62, StoreFleet.Spec.member(), "blind");
        fleet.settle(6);
        assertThat(fleet.runUntil(Duration.ofSeconds(20), () -> !writer.discovery().find(AssetCard.class,
                c -> StoreChooser.KIND.equals(c.access().get("kind"))).isEmpty())).isTrue();
        String root = StoreFleet.facts(writer, "plain", 300, 3).root();
        assertThatThrownBy(() -> writer.pdt().storeFor(root, StoreChooser.Need.of(0, 1), Duration.ofDays(1)))
                .hasMessageContaining("no content store fits");

        writer.content().holdTree(root);
        StorePin pin = new StorePin(StorePin.VERSION, root, StorePin.KIND_TREE, 3600, 1, writer.id(), List.of(),
                Map.of(Stores.STORES_ATTR, blind.id().value()));
        fleet.writePin(writer, pin, Duration.ofMinutes(10));
        assertThat(fleet.runUntil(Duration.ofSeconds(30), () -> blind.contentStore().status().abandoned() > 0)).isTrue();
        assertThat(blind.contentStore().ledger().holds(root)).isFalse();
        assertThat(blind.store().complete(root)).as("it fetched the root node alone, then stopped").isFalse();
    }
}
