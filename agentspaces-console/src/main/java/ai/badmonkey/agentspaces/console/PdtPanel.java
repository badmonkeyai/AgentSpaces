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

import ai.badmonkey.agentspaces.pdt.Pdt;
import ai.badmonkey.agentspaces.peering.node.PeerNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * The PDT panel (srad/agentspaces-pdt.md PD-C-08): per group, the refs with
 * their heads, the delta sizes with the inline and fetched counts, the
 * conflicts, the node store's use, and the last collection, read from the
 * group's {@link Pdt} facade through a {@link PdtPanelData} kept per group so
 * the conflict list and the observed collection persist across reads. Served
 * at {@code /api/v1/panels/pdt}.
 */
public final class PdtPanel implements ConsolePanel {

    private final PeerNode node;
    private final Supplier<Map<String, Pdt>> pdts;
    private final Map<String, PdtPanelData> byGroup = new ConcurrentHashMap<>();

    private PdtPanel(PeerNode node, Supplier<Map<String, Pdt>> pdts) {
        this.node = Objects.requireNonNull(node, "node");
        this.pdts = Objects.requireNonNull(pdts, "pdts");
    }

    /**
     * A panel over the persistent-tree facades of a node, one per group.
     *
     * @param node the node, for its peer id
     * @param pdts the facades by group name, read on every render so late groups appear
     * @return the panel
     */
    public static PdtPanel of(PeerNode node, Supplier<Map<String, Pdt>> pdts) {
        return new PdtPanel(node, pdts);
    }

    /**
     * The per-group data of a facade this panel renders, for registering it with a ref's
     * conflicts ({@code ref.onConflict(panel.dataFor("fleet")::onConflict)}).
     *
     * @param group the group name
     * @return the data, created on first use
     * @throws IllegalArgumentException when the group has no facade
     */
    public PdtPanelData dataFor(String group) {
        Pdt pdt = pdts.get().get(Objects.requireNonNull(group, "group"));
        if (pdt == null) {
            throw new IllegalArgumentException("group '" + group + "' has no Pdt facade; groups with one: " + pdts.get().keySet());
        }
        return dataOf(group, pdt);
    }

    private PdtPanelData dataOf(String group, Pdt pdt) {
        PdtPanelData data = byGroup.get(group);
        if (data == null || data.pdt() != pdt) {
            data = new PdtPanelData(pdt);
            byGroup.put(group, data);
        }
        return data;
    }

    @Override
    public String id() {
        return "pdt";
    }

    @Override
    public String title() {
        return "Persistent trees";
    }

    @Override
    public Object data() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("peer", node.peerId().value());
        List<Map<String, Object>> groups = new ArrayList<>();
        for (Map.Entry<String, Pdt> entry : pdts.get().entrySet()) {
            Map<String, Object> g = new LinkedHashMap<>();
            g.put("group", entry.getKey());
            g.putAll(dataOf(entry.getKey(), entry.getValue()).data());
            groups.add(g);
        }
        doc.put("groups", groups);
        return doc;
    }
}
