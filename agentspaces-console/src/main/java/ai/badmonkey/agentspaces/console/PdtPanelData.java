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
package ai.badmonkey.agentspaces.console;

import ai.badmonkey.agentspaces.content.tree.TreeFetch;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.Pdt;
import ai.badmonkey.agentspaces.pdt.refs.Heads;
import ai.badmonkey.agentspaces.pdt.refs.PConflict;
import ai.badmonkey.agentspaces.pdt.refs.Refs;
import ai.badmonkey.agentspaces.pdt.store.NodeCollector;
import ai.badmonkey.agentspaces.pdt.store.Retention;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;

/**
 * What the console's PDT panel shows for one group's {@link Pdt} facade
 * (srad/agentspaces-pdt.md PD-C-08): the refs with their heads and versions,
 * each head's delta (inline, fetched with its byte count and round trips, or
 * held), the inline and fetched delta counts and the other counters of
 * {@link Pdt.Stats}, the conflicts (the count and the most recent decisions
 * fed through {@link #onConflict}), the node store's use, and the last
 * collection this panel observed. The shape is a plain map, so the JSON and
 * HTML renderings need no more; {@link PdtPanel} wraps one per group.
 *
 * <p>The facade keeps no list of conflict events and no record of a finished
 * collection, so the panel keeps both: register the panel data with every ref
 * whose decisions should show ({@code ref.onConflict(data::onConflict)}), and
 * read the panel on the overview cadence so a collection cycle's end is seen
 * as the collector's phase returns to idle between two reads.
 */
public final class PdtPanelData {

    /** How many conflict decisions the panel keeps. */
    public static final int RECENT_CONFLICTS = 32;

    private final Pdt pdt;
    private final Deque<Map<String, Object>> recentConflicts = new ArrayDeque<>();
    private NodeCollector.Phase lastPhase;
    private int lastNodes;
    private long lastBytes;
    private Map<String, Object> lastCollection;

    public PdtPanelData(Pdt pdt) {
        this.pdt = Objects.requireNonNull(pdt, "pdt");
    }

    /** The facade this panel reads. */
    public Pdt pdt() {
        return pdt;
    }

    /**
     * Records one conflict decision (PD-R-05): the most recent
     * {@value #RECENT_CONFLICTS} show on the panel. Register it as a listener
     * with {@code ref.onConflict(data::onConflict)}.
     *
     * @param conflict the decision
     */
    public void onConflict(PConflict conflict) {
        Objects.requireNonNull(conflict, "conflict");
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("at", Instant.ofEpochMilli(pdt.store().clock().millis()).toString());
        row.put("ref", conflict.name());
        row.put("key", conflict.key() == null ? "" : conflict.key());
        row.put("winnerRoot", conflict.winnerRoot());
        row.put("loserRoot", conflict.loserRoot());
        row.put("reason", conflict.reason());
        synchronized (recentConflicts) {
            recentConflicts.addFirst(row);
            while (recentConflicts.size() > RECENT_CONFLICTS) {
                recentConflicts.removeLast();
            }
        }
    }

    /**
     * Records a collection cycle a host ran to its end through
     * {@code pdt.collector().collect()}, as the last collection. A cycle the
     * facade's tick runs over several ticks (a store larger than one tick's
     * visits) is observed without this, as the collector's phase returns to
     * idle between two reads; a small store finishes a cycle inside one tick,
     * which no read can see.
     *
     * @param cycle what the cycle did
     */
    public synchronized void recordCollection(NodeCollector.Cycle cycle) {
        Objects.requireNonNull(cycle, "cycle");
        Pdt.Stats stats = pdt.stats();
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("observedAt", Instant.ofEpochMilli(pdt.store().clock().millis()).toString());
        row.put("ticks", cycle.ticks());
        row.put("marked", cycle.marked());
        row.put("nodesRemoved", cycle.removed());
        row.put("bytesFreed", cycle.bytesFreed());
        row.put("nodesHeld", stats.nodesHeld());
        row.put("bytesHeld", stats.bytesHeld());
        lastCollection = row;
    }

    /** The panel document. */
    public synchronized Map<String, Object> data() {
        Map<String, Object> doc = new LinkedHashMap<>();
        Pdt.Stats stats = pdt.stats();
        Refs refs = pdt.refs();
        NodeStore store = pdt.store();

        List<Map<String, Object>> refRows = new ArrayList<>();
        if (refs != null) {
            for (String name : new TreeSet<>(refs.names())) {
                Heads heads = refs.heads(name);
                Map<String, Object> r = new LinkedHashMap<>();
                r.put("name", name);
                r.put("heads", heads.heads().size());
                r.put("versionsKnown", refs.versionsKnown(name));
                List<Map<String, Object>> headRows = new ArrayList<>();
                for (Heads.Head head : heads.heads()) {
                    headRows.add(head(refs, name, head, store));
                }
                r.put("headList", headRows);
                refRows.add(r);
            }
        }
        doc.put("refs", refRows);

        Map<String, Object> deltas = new LinkedHashMap<>();
        deltas.put("inline", stats.deltasInline());
        deltas.put("fetched", stats.deltasFetched());
        deltas.put("bytesReceived", stats.bytesReceived());
        deltas.put("basesFetched", stats.basesFetched());
        doc.put("deltas", deltas);

        Map<String, Object> conflicts = new LinkedHashMap<>();
        conflicts.put("count", stats.conflicts());
        conflicts.put("mergeFunctionFailures", stats.mergeFunctionFailures());
        synchronized (recentConflicts) {
            conflicts.put("recent", new ArrayList<>(recentConflicts));
        }
        doc.put("conflicts", conflicts);

        Map<String, Object> counters = new LinkedHashMap<>();
        counters.put("refs", stats.refs());
        counters.put("heads", stats.heads());
        counters.put("headsIgnored", stats.headsIgnored());
        counters.put("headsRewritten", stats.headsRewritten());
        counters.put("nodesDropped", stats.nodesDropped());
        counters.put("entriesTruncated", stats.entriesTruncated());
        doc.put("counters", counters);

        Map<String, Object> storeDoc = new LinkedHashMap<>();
        storeDoc.put("nodesHeld", stats.nodesHeld());
        storeDoc.put("bytesHeld", stats.bytesHeld());
        storeDoc.put("pinnedRoots", store.pinned().size());
        Retention retention = pdt.retention();
        storeDoc.put("retainedRoots", retention.retainedRoots().size());
        storeDoc.put("retainVersions", retention.versions());
        storeDoc.put("retainAge", retention.age().toString());
        doc.put("store", storeDoc);

        doc.put("collector", collector(stats));
        return doc;
    }

    private Map<String, Object> head(Refs refs, String name, Heads.Head head, NodeStore store) {
        Map<String, Object> h = new LinkedHashMap<>();
        h.put("root", head.root());
        h.put("seq", head.seq());
        h.put("size", head.size());
        h.put("kind", head.kind());
        h.put("mode", head.mode());
        h.put("issuer", head.issuer() == null ? "" : head.issuer().toString());
        h.put("issued", head.issued() == null ? "" : head.issued().toString());
        h.put("base", head.base());
        h.put("entryId", head.entryId() == null ? "" : head.entryId().toString());
        h.put("expiresAtMillis", head.expiresAtMillis());
        Optional<TreeFetch.Progress> fetched = refs.fetched(name, head.root());
        Map<String, Object> delta = new LinkedHashMap<>();
        if (fetched.isPresent()) {
            TreeFetch.Progress p = fetched.get();
            delta.put("how", "fetched");
            delta.put("nodes", p.nodesVerified());
            delta.put("bytes", p.bytesReceived());
            delta.put("roundTrips", p.roundTrips());
            delta.put("sources", p.bytesBySource().size());
        } else {
            delta.put("how", store.complete(head.root()) ? "inline" : "pending");
            delta.put("nodes", store.reachable(head.root()).size());
        }
        h.put("delta", delta);
        return h;
    }

    private Map<String, Object> collector(Pdt.Stats stats) {
        Map<String, Object> c = new LinkedHashMap<>();
        NodeCollector collector = pdt.collector();
        NodeCollector.Phase phase = collector == null ? NodeCollector.Phase.IDLE : collector.phase();
        c.put("phase", phase.name());
        c.put("roots", collector == null ? 0 : collector.roots().size());
        c.put("visitsPerTick", collector == null ? 0 : collector.visitsPerTick());
        // A cycle ended between two reads when the phase came back to IDLE: record what it freed.
        if (lastPhase != null && lastPhase != NodeCollector.Phase.IDLE && phase == NodeCollector.Phase.IDLE) {
            Map<String, Object> cycle = new LinkedHashMap<>();
            cycle.put("observedAt", Instant.ofEpochMilli(pdt.store().clock().millis()).toString());
            cycle.put("nodesRemoved", Math.max(0, lastNodes - stats.nodesHeld()));
            cycle.put("bytesFreed", Math.max(0L, lastBytes - stats.bytesHeld()));
            cycle.put("nodesHeld", stats.nodesHeld());
            cycle.put("bytesHeld", stats.bytesHeld());
            lastCollection = cycle;
        }
        lastPhase = phase;
        lastNodes = stats.nodesHeld();
        lastBytes = stats.bytesHeld();
        c.put("lastCollection", lastCollection == null ? Map.of() : lastCollection);
        return c;
    }
}
