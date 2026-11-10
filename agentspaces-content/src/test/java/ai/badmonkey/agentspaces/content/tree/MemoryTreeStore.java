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
package ai.badmonkey.agentspaces.content.tree;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link TreeStore} over a map, reading the {@link NodeFixtures} node format;
 * the content module's test double. {@link #corrupt} makes it serve altered bytes
 * for one CID, the holder of PD-X-04's failure path.
 */
public final class MemoryTreeStore implements TreeStore {

    private final Map<String, byte[]> nodes = new ConcurrentHashMap<>();
    private final Map<String, byte[]> served = new ConcurrentHashMap<>();
    private final Map<String, List<ai.badmonkey.agentspaces.content.Messages.NodeDelta>> deltas = new ConcurrentHashMap<>();

    public MemoryTreeStore putAll(NodeFixtures.Tree tree) {
        nodes.putAll(tree.nodes());
        return this;
    }

    /** Keeps one intra-chunk delta for its chunk, in place of any kept before (PDT 2 phase 4, P2-Z-02). */
    public void keepDelta(ai.badmonkey.agentspaces.content.Messages.NodeDelta delta) {
        deltas.put(delta.cid(), List.of(delta));
    }

    @Override
    public List<ai.badmonkey.agentspaces.content.Messages.NodeDelta> deltas(String cid) {
        return deltas.getOrDefault(cid, List.of());
    }

    /** Serves these bytes for a CID in place of the stored node. */
    public void corrupt(String cid, byte[] bytes) {
        served.put(cid, bytes);
    }

    public int size() {
        return nodes.size();
    }

    /** Forgets a node, so this store holds a tree in part. */
    public void remove(String cid) {
        nodes.remove(cid);
    }

    @Override
    public boolean has(String cid) {
        return nodes.containsKey(cid);
    }

    @Override
    public Optional<byte[]> get(String cid) {
        byte[] altered = served.get(cid);
        if (altered != null) {
            return Optional.of(altered);
        }
        return Optional.ofNullable(nodes.get(cid));
    }

    @Override
    public void putVerified(String cid, byte[] bytes) {
        nodes.put(cid, bytes);
    }

    @Override
    public List<String> links(String cid, byte[] bytes) {
        if (Cids.parse(cid).codec() == Cids.RAW) {
            return List.of(); // a raw chunk is a leaf
        }
        return NodeFixtures.linksOf(bytes);
    }

    @Override
    public boolean complete(String root) {
        return has(root) && missing(root).isEmpty();
    }

    @Override
    public Set<String> missing(String root) {
        Set<String> missing = new LinkedHashSet<>();
        walk(root, missing);
        return missing;
    }

    @Override
    public Set<String> reachable(String root) {
        return walk(root, new LinkedHashSet<>());
    }

    private Set<String> walk(String root, Set<String> missing) {
        Set<String> seen = new LinkedHashSet<>();
        Deque<String> queue = new ArrayDeque<>();
        queue.add(root);
        while (!queue.isEmpty()) {
            String cid = queue.poll();
            byte[] node = nodes.get(cid);
            if (node == null) {
                missing.add(cid);
                continue;
            }
            if (!seen.add(cid)) {
                continue;
            }
            for (String child : links(cid, node)) {
                if (!seen.contains(child)) {
                    queue.add(child);
                }
            }
        }
        return seen;
    }
}
