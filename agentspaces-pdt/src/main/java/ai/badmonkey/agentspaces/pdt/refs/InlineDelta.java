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
package ai.badmonkey.agentspaces.pdt.refs;

import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.content.tree.Cids;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.peering.blocks.BlockStore;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The inline delta of srad/agentspaces-pdt.md §3.5 (PD-X-07) at both ends. On
 * the writer, {@link #recording} wraps the node store so that every node a swap
 * writes is captured, and {@link #collect} keeps the captured nodes reachable
 * from the new root in parent-first order: the delta of the version. On the
 * reader, {@link #accept} walks from the head's root through the inline set,
 * verifies each node it reaches against the CID that reached it, stores the
 * verified ones, and drops the rest (threat 4 of §6.7): a node that no link
 * reaches, or whose bytes do not hash to the CID that names it, never enters
 * the store. The walk descends only into inline nodes; a link the store already
 * holds is a shared subtree and is not entered.
 */
final class InlineDelta {

    /** What a reader kept of a head's inline delta. */
    record Accepted(int stored, int dropped, long bytes) {
    }

    private InlineDelta() {
    }

    /** A node store over the same backend that records every node put through it. */
    static Recording recording(NodeStore store) {
        return new Recording(store);
    }

    /** The captured nodes reachable from {@code root} through captured nodes, parents first. */
    static Map<String, byte[]> collect(Map<String, byte[]> captured, String root) {
        Map<String, byte[]> delta = new LinkedHashMap<>();
        if (!captured.containsKey(root)) {
            return delta;
        }
        Deque<String> frontier = new ArrayDeque<>();
        frontier.add(root);
        while (!frontier.isEmpty()) {
            String cid = frontier.poll();
            if (delta.containsKey(cid)) {
                continue;
            }
            byte[] bytes = captured.get(cid);
            if (bytes == null) {
                continue;
            }
            delta.put(cid, bytes);
            for (String link : links(cid, bytes)) {
                if (captured.containsKey(link) && !delta.containsKey(link)) {
                    frontier.add(link);
                }
            }
        }
        return delta;
    }

    /**
     * Verifies and stores the inline nodes of a head that the walk from its root
     * reaches, and drops every other.
     */
    static Accepted accept(NodeStore store, String root, List<byte[]> nodes) {
        Map<DigestKey, byte[]> byDigest = new HashMap<>();
        for (byte[] node : nodes) {
            if (node != null && node.length > 0) {
                byDigest.putIfAbsent(new DigestKey(Digests.sha256(node)), node);
            }
        }
        Set<byte[]> used = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        Set<String> seen = new HashSet<>();
        Deque<String> frontier = new ArrayDeque<>();
        frontier.add(root);
        int stored = 0;
        long bytes = 0;
        while (!frontier.isEmpty()) {
            String cid = frontier.poll();
            if (!seen.add(cid)) {
                continue;
            }
            Cids.Parsed parsed;
            try {
                parsed = Cids.parse(cid);
            } catch (IllegalArgumentException e) {
                continue;
            }
            byte[] node = byDigest.get(new DigestKey(parsed.digest()));
            if (node == null) {
                continue;
            }
            if (store.has(cid)) {
                // a shared node the store already holds: matched, and its subtree is not entered
                used.add(node);
                continue;
            }
            try {
                store.putVerified(cid, node);
            } catch (RuntimeException refused) {
                continue;
            }
            used.add(node);
            stored++;
            bytes += node.length;
            frontier.addAll(links(cid, node));
        }
        int distinct = byDigest.size();
        return new Accepted(stored, distinct - used.size() + (nodes.size() - distinct), bytes);
    }

    private static List<String> links(String cid, byte[] bytes) {
        try {
            return NodeCodec.links(cid, bytes);
        } catch (RuntimeException leaf) {
            return List.of();
        }
    }

    /** A sha2-256 digest as a map key. */
    private record DigestKey(byte[] digest) {
        @Override
        public boolean equals(Object o) {
            return o instanceof DigestKey other && Arrays.equals(digest, other.digest);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(digest);
        }
    }

    /**
     * A node store over the backend of another that records the nodes put through
     * it. Reads, pins, and completeness see the shared backend, so a value built
     * over it reads every node the real store holds and writes new nodes where
     * every other value finds them; only the record is extra.
     */
    static final class Recording {

        private final NodeStore store;
        private final Map<String, byte[]> captured = new ConcurrentHashMap<>();

        private Recording(NodeStore real) {
            BlockStore backend = real.backend();
            BlockStore recording = new BlockStore() {
                @Override
                public Optional<byte[]> get(String cid, long nowMillis) {
                    return backend.get(cid, nowMillis);
                }

                @Override
                public boolean put(String cid, byte[] bytes, long nowMillis) {
                    captured.put(cid, bytes);
                    return backend.put(cid, bytes, nowMillis);
                }

                @Override
                public long remove(String cid) {
                    return backend.remove(cid);
                }

                @Override
                public Set<String> cids() {
                    return backend.cids();
                }

                @Override
                public long lastAccessMillis(String cid) {
                    return backend.lastAccessMillis(cid);
                }

                @Override
                public int size() {
                    return backend.size();
                }

                @Override
                public long bytesUsed() {
                    return backend.bytesUsed();
                }

                @Override
                public String kind() {
                    return backend.kind();
                }
            };
            this.store = real.over(recording);
        }

        /** The store to build the swap's value over. */
        NodeStore store() {
            return store;
        }

        /** The nodes put so far, CID to bytes. */
        Map<String, byte[]> captured() {
            return new LinkedHashMap<>(captured);
        }

        /** The captured nodes reachable from a root, parents first, with their total bytes. */
        List<Map.Entry<String, byte[]>> delta(String root) {
            return new ArrayList<>(collect(captured, root).entrySet());
        }
    }
}
