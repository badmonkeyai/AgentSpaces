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
package ai.badmonkey.agentspaces.discovery;

import ai.badmonkey.agentspaces.api.ad.AppCard;
import ai.badmonkey.agentspaces.api.ad.CardSpill;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.codec.Multibase;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.UnaryOperator;

/**
 * An in-memory {@link SpillStore} for the spill tests (AppCard origin and formats, 2026-11-09): it computes the content
 * root a swarm would, records every call, and can be told to serve other bytes than it took.
 */
final class MemorySpillStore implements SpillStore {

    final Map<String, byte[]> documents = new ConcurrentHashMap<>();
    final List<String> puts = new CopyOnWriteArrayList<>();
    final List<String> gets = new CopyOnWriteArrayList<>();
    final List<String> released = new CopyOnWriteArrayList<>();
    volatile UnaryOperator<byte[]> serve = UnaryOperator.identity();

    @Override
    public CompletableFuture<String> put(String name, byte[] document) {
        String root = rootOf(document);
        documents.put(root, document.clone());
        puts.add(root);
        return CompletableFuture.completedFuture(root);
    }

    @Override
    public CompletableFuture<byte[]> get(String root, long size) {
        gets.add(root);
        byte[] held = documents.get(root);
        return held == null ? CompletableFuture.failedFuture(new IllegalStateException("no holder of " + root))
                : CompletableFuture.completedFuture(serve.apply(held.clone()));
    }

    @Override
    public void release(String root) {
        released.add(root);
    }

    /** The content root over {@link CardSpill#PIECE_SIZE} pieces: a CIDv1 (raw, sha2-256) over the piece table. */
    static String rootOf(byte[] document) {
        ByteArrayOutputStream table = new ByteArrayOutputStream();
        for (int at = 0; at < document.length; at += CardSpill.PIECE_SIZE) {
            table.writeBytes(Digests.sha256(Arrays.copyOfRange(document, at, Math.min(document.length, at + CardSpill.PIECE_SIZE))));
        }
        byte[] cid = new byte[36];
        cid[0] = 0x01;
        cid[1] = 0x55;
        cid[2] = 0x12;
        cid[3] = 0x20;
        System.arraycopy(Digests.sha256(table.toByteArray()), 0, cid, 4, 32);
        return Multibase.base32(cid);
    }

    /**
     * An a2ui AppCard whose canonical encoding is exactly {@code size} bytes, grown through its 64 view names.
     *
     * @param issuer the issuer
     * @param group  the group
     * @param id     the card's local name
     * @param issued the issue instant
     * @param size   the encoded size, at least 8 KiB
     * @return the card
     */
    static AppCard appCardOfSize(PeerId issuer, GroupId group, String id, Instant issued, int size) {
        CborCodec codec = CborCodec.defaultCodec();
        int each = size / AppCard.MAX_TERMS;
        List<String> views = new ArrayList<>();
        for (int i = 0; i < AppCard.MAX_TERMS; i++) {
            views.add(("v" + i + "-").concat("x".repeat(Math.max(300, each - 200))));
        }
        for (int round = 0; round < 8; round++) {
            AppCard card = sized(issuer, group, id, issued, views);
            int delta = size - codec.toBytes(card).length;
            if (delta == 0) {
                return card;
            }
            String last = views.get(views.size() - 1);
            views.set(views.size() - 1, delta > 0 ? last + "x".repeat(delta) : last.substring(0, last.length() + delta));
        }
        throw new IllegalStateException("no card of " + size + " bytes");
    }

    private static AppCard sized(PeerId issuer, GroupId group, String id, Instant issued, List<String> views) {
        return new AppCard("aspace://" + group.value() + "/app/" + issuer.value() + "/" + id, issuer, group, issued, Duration.ofMinutes(15),
                id, "fed", "a2ui", "example", List.of(group.value() + "/items"), List.of("open"), "", "", "", "1", views,
                new AppCard.Ui("a2ui", "v0.9.1", List.of("example-widgets"), "board", "views", List.of("board-1")))
                .withOrigin(AppCard.Origin.of("console", "example-console/" + id, "", null))
                .withFormats(List.of("application/geo+json"));
    }
}
