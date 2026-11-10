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
package ai.badmonkey.agentspaces.pdt.swarm;

import ai.badmonkey.agentspaces.content.tree.TreeStore;
import ai.badmonkey.agentspaces.pdt.NodeCodec;
import ai.badmonkey.agentspaces.pdt.NodeFormatException;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.blob.BaoOutboard;
import ai.badmonkey.agentspaces.pdt.blob.BlobIdentity;
import ai.badmonkey.agentspaces.pdt.store.QuotaNodeStore;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The {@link NodeStore} seen through the content swarm's {@link TreeStore}
 * (srad/agentspaces-pdt.md §3.4, §6.9): the one adapter that lets
 * {@code agentspaces-content} serve {@code TREE_WANT} and {@code NODE_WANT}
 * (PD-X-03) and verify {@code NODES} (PD-X-04) over pdt nodes without depending
 * on this module. Links come from {@link NodeCodec#links(String, byte[])}, a
 * received node is stored through {@link NodeStore#putVerified}, and
 * completeness, the missing frontier, and reachability are the store's own walks
 * (PD-S-06).
 *
 * <p>Two translations keep the {@link TreeStore} contract. A node whose bytes the
 * codec refuses makes {@link #links} throw {@link IllegalArgumentException}, which
 * the exchange treats as a node without links; when such a node sits inside a
 * tree, the store's walk refuses the whole tree with {@link NodeFormatException},
 * so this adapter walks the tree again with the same leaf rule, and
 * {@link #complete}, {@link #missing}, and {@link #reachable} agree with
 * {@link #links}. The values layer still refuses the node by name when a value
 * reads it (PD-N-04). A held block that no longer hashes to its CID, which
 * {@link NodeStore#get} refuses, reads as absent, so a holder never serves it and
 * a fetcher asks for it again. The store is safe for concurrent use, as the
 * exchange's handler threads require.
 *
 * <p>With a quota ({@code agentspaces.pdt.store.quota}; PDT 2 phase 7, P2-X-08, PD-S-05) every node the swarm
 * hands {@link #putVerified} passes a {@link QuotaNodeStore} first: a node that would carry the store past the
 * quota is refused with a {@link ai.badmonkey.agentspaces.pdt.store.QuotaExceededException}, which ends the
 * fetch that received it, and a warning is logged. The facade's own writes and pushed bundles do not pass here.
 */
public final class NodeStoreTreeStore implements TreeStore {

    private static final System.Logger LOG = System.getLogger(NodeStoreTreeStore.class.getName());

    private final NodeStore store;
    /** The quota received nodes pass, or null when unbounded. */
    private final QuotaNodeStore quota;

    /**
     * The adapter without a quota.
     *
     * @param store the node store the swarm serves from and fetches into
     */
    public NodeStoreTreeStore(NodeStore store) {
        this(store, QuotaNodeStore.UNBOUNDED);
    }

    /**
     * The adapter behind a quota on received nodes (PDT 2 phase 7, P2-X-08).
     *
     * @param store      the node store the swarm serves from and fetches into
     * @param quotaBytes the most bytes the store may hold after a received node, or {@link QuotaNodeStore#UNBOUNDED}
     */
    public NodeStoreTreeStore(NodeStore store, long quotaBytes) {
        this.store = Objects.requireNonNull(store, "store");
        this.quota = quotaBytes == QuotaNodeStore.UNBOUNDED ? null
                : new QuotaNodeStore(store, quotaBytes, refusal -> LOG.log(System.Logger.Level.WARNING,
                        "the PDT store refused a received node: {0}", refusal.getMessage()));
    }

    /** The quota received nodes pass, {@link QuotaNodeStore#UNBOUNDED} when none. */
    public long quota() {
        return quota == null ? QuotaNodeStore.UNBOUNDED : quota.quota();
    }

    /** The node store behind this adapter. */
    public NodeStore store() {
        return store;
    }

    @Override
    public boolean has(String cid) {
        return store.has(cid);
    }

    /** The bytes as held and named by the CID: in the sealed-nodes mode of §3.8 the envelope, served to any member. */
    @Override
    public Optional<byte[]> get(String cid) {
        try {
            return store.stored(cid);
        } catch (NodeFormatException e) {
            return Optional.empty();
        }
    }

    @Override
    public void putVerified(String cid, byte[] bytes) {
        if (quota == null) {
            store.putVerified(cid, bytes);
        } else {
            quota.putVerified(cid, bytes);
        }
    }

    @Override
    public List<String> links(String cid, byte[] bytes) {
        try {
            return NodeCodec.links(cid, bytes);
        } catch (NodeFormatException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    @Override
    public boolean complete(String root) {
        return missing(root).isEmpty();
    }

    @Override
    public Set<String> missing(String root) {
        try {
            return store.missing(root);
        } catch (NodeFormatException e) {
            Set<String> missing = new LinkedHashSet<>();
            lenientWalk(root, missing);
            return Collections.unmodifiableSet(missing);
        }
    }

    @Override
    public Set<String> reachable(String root) {
        try {
            return store.reachable(root);
        } catch (NodeFormatException e) {
            return lenientWalk(root, null);
        }
    }

    /**
     * A flat artifact's identity and outboard (PDT 2 phase 4, P2-B-06): the {@code bo} nodes go into the store, pinned
     * so the collector keeps them for the artifact, and the attributes name them. A sealed store names none, since a
     * sealed group's content key seals the plaintext the identity would name.
     */
    @Override
    public Map<String, String> outboard(InputStream content) throws IOException {
        if (store instanceof ai.badmonkey.agentspaces.pdt.seal.SealedNodeStore) {
            return Map.of();
        }
        BaoOutboard.Built outboard = BaoOutboard.of(content);
        store.putAll(outboard.nodes());
        store.pin(outboard.root());
        return BlobIdentity.of(outboard).attrs();
    }

    /** The frames the store's edits kept for a chunk (PDT 2 phase 4, P2-Z-02), which a holder may send in its place. */
    @Override
    public List<ai.badmonkey.agentspaces.content.Messages.NodeDelta> deltas(String cid) {
        return store.deltas().deltas(cid);
    }

    /**
     * The store's breadth-first walk with the exchange's leaf rule: a node the codec
     * refuses, or a node past the depth bound, links to nothing.
     */
    private Set<String> lenientWalk(String root, Set<String> missing) {
        Set<String> seen = new LinkedHashSet<>();
        Set<String> queued = new HashSet<>();
        Deque<String> queue = new ArrayDeque<>();
        Deque<Integer> depths = new ArrayDeque<>();
        queue.add(root);
        depths.add(1);
        queued.add(root);
        while (!queue.isEmpty()) {
            String cid = queue.poll();
            int depth = depths.poll();
            seen.add(cid);
            Optional<byte[]> bytes = get(cid);
            if (bytes.isEmpty()) {
                if (missing != null) {
                    missing.add(cid);
                }
                continue;
            }
            if (depth >= NodeStore.MAX_DEPTH) {
                continue;
            }
            List<String> links;
            try {
                links = links(cid, bytes.get());
            } catch (IllegalArgumentException e) {
                continue;
            }
            for (String link : links) {
                if (queued.add(link)) {
                    queue.add(link);
                    depths.add(depth + 1);
                }
            }
        }
        return Collections.unmodifiableSet(seen);
    }

    @Override
    public String toString() {
        return "NodeStoreTreeStore(" + store.size() + " nodes)";
    }
}
