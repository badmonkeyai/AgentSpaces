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

import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.FetchOptions;
import ai.badmonkey.agentspaces.content.tree.TreeFetch;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.store.StoreThenFetchLoader;
import ai.badmonkey.agentspaces.pdt.values.NodeLoader;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * A node store attached to a group's content swarm (srad/agentspaces-pdt.md
 * §3.5, §6.2, §6.9): the one object a host builds to hold versions for the
 * group, fetch versions from it, and read values lazily through it.
 *
 * <p>The content swarm learns its tree store when it is built, so the host gives
 * {@code Content.Builder.treeStore} a {@link NodeStoreTreeStore} over the node
 * store first ({@link #treeStore} makes one) and then calls {@link #attach};
 * attach refuses a swarm built over any other tree store. From then on the swarm
 * serves {@code TREE_WANT} and {@code NODE_WANT} from the store (PD-X-03),
 * {@link #hold} pins a root in the store and publishes its holding (PD-S-06),
 * {@link #fetchTree} fetches a version pruned by the bases the store holds
 * (S2 of §6.4), and {@link #loader} is the {@link NodeLoader} of PD-V-06, a
 * {@link StoreThenFetchLoader} over a {@link SwarmFetcher}, which a value reads
 * through so that a cold {@code get} fetches only the nodes on its path.
 */
public final class PdtSwarm {

    private final Content content;
    private final NodeStore store;
    private final SwarmFetcher fetcher;
    private final StoreThenFetchLoader loader;

    private PdtSwarm(Content content, NodeStore store, Duration deadline) {
        this.content = content;
        this.store = store;
        this.fetcher = new SwarmFetcher(content, store, deadline);
        this.loader = new StoreThenFetchLoader(store, fetcher, deadline);
    }

    /**
     * The tree store to give {@code Content.Builder.treeStore} before building the
     * swarm that {@link #attach} will join to this node store.
     *
     * @param store the node store
     * @return the adapter
     */
    public static NodeStoreTreeStore treeStore(NodeStore store) {
        return new NodeStoreTreeStore(store);
    }

    /**
     * Attaches a node store to the content swarm built over it.
     *
     * @param content  the swarm, built with {@code treeStore(new NodeStoreTreeStore(store))}
     * @param store    the node store
     * @param deadline how long a lazy load waits for one node; also the swarm's fetch
     *                 timeout for a node with no reachable holder (PD-X-08)
     * @return the attachment
     * @throws IllegalStateException when the swarm's tree store is not an adapter over {@code store}
     */
    public static PdtSwarm attach(Content content, NodeStore store, Duration deadline) {
        Objects.requireNonNull(content, "content");
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(deadline, "deadline");
        return new PdtSwarm(content, store, deadline);
    }

    /** Attaches with the loader's default deadline. */
    public static PdtSwarm attach(Content content, NodeStore store) {
        return attach(content, store, StoreThenFetchLoader.DEFAULT_DEADLINE);
    }

    /** The content swarm. */
    public Content content() {
        return content;
    }

    /** The node store. */
    public NodeStore store() {
        return store;
    }

    /** The fetcher bare batches and tree fetches go through. */
    public SwarmFetcher fetcher() {
        return fetcher;
    }

    /** The loader a value reads through: the store first, then the swarm within the deadline (PD-V-06). */
    public StoreThenFetchLoader loader() {
        return loader;
    }

    /**
     * Pins a root in the store and publishes this peer's holding for it, so the
     * swarm serves it and a collector keeps it (PD-S-04, PD-S-06); its holders
     * become sources for lazy loads.
     *
     * @param root the root CID
     * @return the pin count after the pin
     */
    public int hold(String root) {
        Objects.requireNonNull(root, "root");
        int count = store.pin(root);
        fetcher.follow(root);
        content.holdTree(root);
        return count;
    }

    /**
     * Releases one pin on a root; when it was the last, withdraws the holding and
     * stops following the root.
     *
     * @param root the root CID
     * @return the pin count after the release, or -1 when the root was not pinned
     */
    public int release(String root) {
        Objects.requireNonNull(root, "root");
        int left = store.unpin(root);
        if (left <= 0) {
            content.releaseTree(root);
            fetcher.unfollow(root);
        }
        return left;
    }

    /**
     * Fetches a version into the store, pruned by the bases the store holds; the
     * swarm publishes the holding on completion (unless the seed mode says never).
     *
     * @param root  the root CID
     * @param haves roots this store holds complete; at most 64
     * @return the handle
     */
    public TreeFetch fetchTree(String root, List<String> haves) {
        return fetcher.fetchTree(root, haves);
    }

    /** As {@link #fetchTree(String, List)} with no bases. */
    public TreeFetch fetchTree(String root) {
        return fetcher.fetchTree(root, List.of());
    }

    /**
     * As {@link #fetchTree(String, List)} with explicit options.
     *
     * @param root    the root CID
     * @param haves   roots this store holds complete; at most 64
     * @param options the swarm's options
     * @return the handle
     */
    public TreeFetch fetchTree(String root, List<String> haves, FetchOptions options) {
        Objects.requireNonNull(options, "options");
        List<String> bases = haves == null ? List.of() : List.copyOf(haves);
        fetcher.follow(root);
        bases.forEach(fetcher::follow);
        return content.fetchTree(root, bases, options);
    }

    @Override
    public String toString() {
        return "PdtSwarm(" + store.size() + " nodes, " + store.pinned().size() + " pinned, "
                + fetcher.inFlight().size() + " batches in flight)";
    }
}
