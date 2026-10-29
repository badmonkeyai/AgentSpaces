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
package ai.badmonkey.agentspaces.content;

import ai.badmonkey.agentspaces.agent.content.Holding;
import ai.badmonkey.agentspaces.agent.content.Manifest;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * What the console's Content panel shows for one group (LB-O-03): the roots the
 * replica knows with their holder counts and this peer's share, the store's
 * bytes and quota, the active fetches with progress and sources, and the
 * capability's counters. The console module wraps this in a {@code ConsolePanel};
 * the shape is a plain map so the JSON and HTML renderings need no more.
 */
public final class ContentPanelData {

    private final Content content;

    public ContentPanelData(Content content) {
        this.content = Objects.requireNonNull(content, "content");
    }

    /** The panel document. */
    public Map<String, Object> data() {
        Map<String, Object> doc = new LinkedHashMap<>();
        PieceStore store = content.store();
        Map<String, Object> storeDoc = new LinkedHashMap<>();
        storeDoc.put("kind", store.kind());
        storeDoc.put("bytesUsed", store.bytesUsed());
        storeDoc.put("quota", store.quota());
        storeDoc.put("roots", store.roots().size());
        doc.put("store", storeDoc);

        List<Map<String, Object>> roots = new ArrayList<>();
        for (Manifest m : content.list(1_000)) {
            roots.add(root(m, true));
        }
        for (String held : store.roots()) {
            if (content.spaces().manifest(held).isEmpty()) {
                store.manifest(held).ifPresent(m -> roots.add(root(m, false)));
            }
        }
        doc.put("roots", roots);

        List<Map<String, Object>> fetches = new ArrayList<>();
        for (Fetch fetch : content.activeFetches()) {
            Fetch.Progress p = fetch.progress();
            Map<String, Object> f = new LinkedHashMap<>();
            f.put("root", fetch.root());
            f.put("piecesVerified", p.piecesVerified());
            f.put("pieces", p.pieces());
            f.put("bytesReceived", p.bytesReceived());
            f.put("sourcesActive", p.sourcesActive());
            f.put("waiting", p.waiting());
            Map<String, Long> bySource = new LinkedHashMap<>();
            p.bytesBySource().forEach((peer, bytes) -> bySource.put(peer.display(), bytes));
            f.put("bytesBySource", bySource);
            fetches.add(f);
        }
        doc.put("fetches", fetches);

        List<Map<String, Object>> collections = new ArrayList<>();
        for (CollectionAssetProvider provider : content.collections()) {
            for (CollectionAssetProvider.Collection c : provider.collections()) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("asset", CollectionAssetProvider.assetName(c.name()));
                row.put("description", c.description());
                row.put("roots", provider.rowCount(c));
                collections.add(row);
            }
        }
        doc.put("collections", collections); // LB-K-07

        Content.Stats stats = content.stats();
        Map<String, Object> counters = new LinkedHashMap<>();
        counters.put("bytesServed", stats.bytesServed());
        counters.put("bytesReceived", stats.bytesReceived());
        counters.put("wantsRefused", stats.wantsRefused());
        counters.put("piecesStruck", stats.piecesStruck());
        doc.put("counters", counters);
        return doc;
    }

    private Map<String, Object> root(Manifest m, boolean replicated) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("root", m.root());
        r.put("name", m.name() == null ? "" : m.name());
        r.put("mediaType", m.mediaType() == null ? "" : m.mediaType());
        r.put("size", m.size());
        r.put("pieces", m.pieces());
        r.put("sealing", m.sealing());
        r.put("replicated", replicated);
        int complete = 0;
        int partial = 0;
        for (Holding h : content.spaces().holdings(m.root())) {
            if (h.complete()) {
                complete++;
            } else {
                partial++;
            }
        }
        r.put("completeHolders", complete);
        r.put("partialHolders", partial);
        r.put("heldHere", content.store().have(m.root()).cardinality());
        return r;
    }
}
