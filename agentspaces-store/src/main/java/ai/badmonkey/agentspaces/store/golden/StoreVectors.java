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
package ai.badmonkey.agentspaces.store.golden;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.content.Messages;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.blob.Blake3;
import ai.badmonkey.agentspaces.pdt.golden.PdtVectors;
import ai.badmonkey.agentspaces.store.ContentStore;
import ai.badmonkey.agentspaces.store.StorePolicy;

import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The golden vectors of PDT 2 phase 3 (srad/agentspaces-pdt2.md §6.10.4), added to {@code golden.json} after the
 * {@code pdt_*} entries and emitted there by {@code tools/golden/GoldenVectors.java}. Their inputs:
 * <ul>
 *   <li>{@code content_challenge_cbor} (P2-S-07): the canonical CBOR of a tag 12 {@code CHALLENGE} (hex), req 42,
 *       root = the root CID of {@code pdt_map_deep}, picks: the node {@code pdt_map_small}'s root (no offset and
 *       no length), then the chunk group at offset 0 and length 1024 of the BLAKE3 identity of the empty
 *       stream.</li>
 *   <li>{@code asset_card_content_store_cbor} (P2-S-01, rule 19): the canonical CBOR of the unsigned
 *       {@code content-store} card (hex) of a store named {@code site-a} in group {@code zGolden}, issued by the
 *       requester peer of {@code pdt_store_pin_cbor} at 1792320000000 ms with a 15-minute time to live and a
 *       10-second holding interval, under capacity 2 TiB, reserve 50 GiB, max-object 64 GiB, leases 30 d and
 *       365 d, 3 copies, both kinds, the {@code grant} rule, not blind, the hot tier, a price of 2 per GiB-month,
 *       no content key, and nothing held, so {@code free} is the capacity less the reserve.</li>
 * </ul>
 */
public final class StoreVectors {

    private StoreVectors() {
    }

    /** The vectors in emission order, each value hex. */
    public static Map<String, String> vectors() {
        CborCodec codec = CborCodec.defaultCodec();
        Map<String, String> out = new LinkedHashMap<>();
        out.put("content_challenge_cbor", HexFormat.of().formatHex(codec.toBytes(challenge())));
        out.put("asset_card_content_store_cbor", HexFormat.of().formatHex(codec.toBytes(card())));
        return out;
    }

    /** The challenge of {@code content_challenge_cbor}. */
    public static Messages.Challenge challenge() {
        return new Messages.Challenge(42, Cids.of(PdtVectors.mapDeep(), Cids.DAG_CBOR), List.of(
                Messages.Pick.node(Cids.of(PdtVectors.mapSmall(), Cids.DAG_CBOR)),
                Messages.Pick.chunkGroup(Blake3.identity(Blake3.digest(new byte[0])), 0L, 1024)));
    }

    /** The policy of {@code asset_card_content_store_cbor}. */
    public static StorePolicy policy() {
        return StorePolicy.of(2L << 40).reserve(50L << 30).price(2);
    }

    /** The card of {@code asset_card_content_store_cbor}. */
    public static ai.badmonkey.agentspaces.api.ad.AssetCard card() {
        StorePolicy policy = policy();
        return ContentStore.card(GroupId.of("zGolden"), PdtVectors.storePin().requester(), Instant.ofEpochMilli(1792320000000L),
                Duration.ofMinutes(15), "site-a", policy, policy.usableBytes(), false, Duration.ofSeconds(10), false);
    }
}
