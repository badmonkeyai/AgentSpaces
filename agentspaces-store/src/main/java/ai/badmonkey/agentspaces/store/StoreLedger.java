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

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.pdt.store.MetaStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * What a content store holds and for whom (PDT 2 phase 3, P2-S-06; srad/agentspaces-pdt2.md §3.2.3): one
 * {@link Held} per root and requester, with the bytes and the lease's end. A root two requesters pinned is held
 * once and charged to each in full; {@link #dedupBytes} reports the difference (§6.8, store capacity
 * accounting). Over a {@link MetaStore} (a pack node store) the ledger persists under
 * {@code store-ledger:<name>} on every change, so a restarted store keeps its holdings and leases (P2-Q-REL-02).
 */
public final class StoreLedger {

    /**
     * One holding.
     *
     * @param root      the root
     * @param kind      {@code tree} or {@code artifact}
     * @param bytes     the bytes the root holds
     * @param requester the requester
     * @param until     epoch milliseconds the lease ends
     * @param copies    the copies the requester asked for
     */
    public record Held(String root, String kind, long bytes, PeerId requester, long until, int copies) {
        public Held {
            Objects.requireNonNull(root, "root");
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(requester, "requester");
        }
    }

    /**
     * Bytes and roots charged.
     *
     * @param bytes the bytes, each root in full
     * @param roots the roots
     */
    public record Totals(long bytes, int roots) {
    }

    /** The persisted form: a version and the rows. */
    private record Saved(int v, List<Row> rows) {
    }

    private record Row(String root, String kind, long bytes, String requester, long until, int copies) {
    }

    private static final CborCodec CODEC = CborCodec.defaultCodec();

    private final String group;
    private final MetaStore meta;
    private final String metaName;
    private final Map<String, Map<PeerId, Held>> byRoot = new LinkedHashMap<>();

    /**
     * A ledger.
     *
     * @param group the group the store serves
     * @param meta  where the ledger persists, or null to keep it in memory
     * @param name  the store's name, which names the record
     */
    public StoreLedger(String group, MetaStore meta, String name) {
        this.group = Objects.requireNonNull(group, "group");
        this.meta = meta;
        this.metaName = "store-ledger:" + Objects.requireNonNull(name, "name").replaceAll("[^A-Za-z0-9._-]", "_");
        if (meta != null) {
            meta.meta(metaName).ifPresent(this::restore);
        }
    }

    private void restore(byte[] bytes) {
        Saved saved = CODEC.fromBytes(bytes, Saved.class);
        for (Row row : saved.rows()) {
            Held held = new Held(row.root(), row.kind(), row.bytes(), PeerId.of(row.requester()), row.until(), row.copies());
            byRoot.computeIfAbsent(held.root(), r -> new LinkedHashMap<>()).put(held.requester(), held);
        }
    }

    private void persist() {
        if (meta == null) {
            return;
        }
        List<Row> rows = new ArrayList<>();
        for (Held h : holdingsUnlocked()) {
            rows.add(new Row(h.root(), h.kind(), h.bytes(), h.requester().value(), h.until(), h.copies()));
        }
        meta.putMeta(metaName, CODEC.toBytes(new Saved(1, rows)));
    }

    /** Records a holding, replacing the requester's earlier one of the root. */
    public synchronized void hold(Held held) {
        byRoot.computeIfAbsent(held.root(), r -> new LinkedHashMap<>()).put(held.requester(), held);
        persist();
    }

    /** Removes a requester's holding of a root. */
    public synchronized Optional<Held> release(String root, PeerId requester) {
        Map<PeerId, Held> holders = byRoot.get(root);
        Held removed = holders == null ? null : holders.remove(requester);
        if (holders != null && holders.isEmpty()) {
            byRoot.remove(root);
        }
        if (removed != null) {
            persist();
        }
        return Optional.ofNullable(removed);
    }

    /** A requester's holding of a root. */
    public synchronized Optional<Held> holding(String root, PeerId requester) {
        Map<PeerId, Held> holders = byRoot.get(root);
        return holders == null ? Optional.empty() : Optional.ofNullable(holders.get(requester));
    }

    /** Whether any requester holds the root here. */
    public synchronized boolean holds(String root) {
        return byRoot.containsKey(root);
    }

    /** Every holding. */
    public synchronized List<Held> holdings() {
        return holdingsUnlocked();
    }

    private List<Held> holdingsUnlocked() {
        List<Held> out = new ArrayList<>();
        byRoot.values().forEach(m -> out.addAll(m.values()));
        return out;
    }

    /** The holdings whose lease ended before an instant. */
    public synchronized List<Held> lapsed(long cutoffMillis) {
        return holdingsUnlocked().stream().filter(h -> h.until() < cutoffMillis).toList();
    }

    /** The bytes held, each root once. */
    public synchronized long usedBytes() {
        long total = 0;
        for (Map<PeerId, Held> holders : byRoot.values()) {
            total += holders.values().stream().mapToLong(Held::bytes).max().orElse(0);
        }
        return total;
    }

    /** The bytes charged beyond the bytes held: the roots two or more requesters share. */
    public synchronized long dedupBytes() {
        return holdingsUnlocked().stream().mapToLong(Held::bytes).sum() - usedBytes();
    }

    /** Bytes and roots per requester, each root charged in full. */
    public synchronized Map<PeerId, Totals> byRequester() {
        Map<PeerId, Totals> out = new HashMap<>();
        for (Held h : holdingsUnlocked()) {
            out.merge(h.requester(), new Totals(h.bytes(), 1), (a, b) -> new Totals(a.bytes() + b.bytes(), a.roots() + b.roots()));
        }
        return out;
    }

    /** Bytes held and roots per group; a store serves one group. */
    public synchronized Map<String, Totals> byGroup() {
        return Map.of(group, new Totals(usedBytes(), byRoot.size()));
    }
}
