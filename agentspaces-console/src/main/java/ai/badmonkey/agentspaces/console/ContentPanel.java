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

import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.ContentPanelData;
import ai.badmonkey.agentspaces.peering.node.PeerNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * The Content panel (srad/large-networks-bulk-data.md LB-O-03): per group, the
 * roots the replica knows with holder counts and this peer's share, the store's
 * bytes and quota, the fetches in progress with their sources, the capability's
 * counters, and the node's relay-path counters (forwarded to a relay, dropped
 * unreachable, dropped for a bad path, unreachable signals received). Served at
 * {@code /api/v1/panels/content}.
 */
public final class ContentPanel implements ConsolePanel {

    private final PeerNode node;
    private final Supplier<List<Content>> contents;

    private ContentPanel(PeerNode node, Supplier<List<Content>> contents) {
        this.node = Objects.requireNonNull(node, "node");
        this.contents = Objects.requireNonNull(contents, "contents");
    }

    /**
     * A panel over the content capabilities of a node, one per group.
     *
     * @param node     the node, for the relay-path counters
     * @param contents the capabilities, read on every render so late groups appear
     * @return the panel
     */
    public static ContentPanel of(PeerNode node, Supplier<List<Content>> contents) {
        return new ContentPanel(node, contents);
    }

    @Override
    public String id() {
        return "content";
    }

    @Override
    public String title() {
        return "Content swarm";
    }

    @Override
    public Object data() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("peer", node.peerId().value());
        List<Map<String, Object>> groups = new ArrayList<>();
        for (Content content : contents.get()) {
            Map<String, Object> g = new LinkedHashMap<>();
            g.put("group", content.group().value());
            g.putAll(new ContentPanelData(content).data());
            PeerNode.RelayStats relay = node.relayStats().get(content.group());
            Map<String, Object> paths = new LinkedHashMap<>();
            paths.put("forwardedToRelay", relay == null ? 0 : relay.forwardedToRelay());
            paths.put("droppedUnreachable", relay == null ? 0 : relay.droppedUnreachable());
            paths.put("droppedBadPath", relay == null ? 0 : relay.droppedBadPath());
            g.put("relayPaths", paths);
            groups.add(g);
        }
        doc.put("groups", groups);
        doc.put("unreachableSignalsReceived", node.unreachableSignalsReceived());
        return doc;
    }
}
