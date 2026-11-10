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

import ai.badmonkey.agentspaces.agent.content.Holding;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.content.Content;
import ai.badmonkey.agentspaces.content.FetchOptions;
import ai.badmonkey.agentspaces.content.tree.TreeFetch;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.NodeUnavailableException;
import ai.badmonkey.agentspaces.pdt.store.Fetcher;
import ai.badmonkey.agentspaces.pdt.store.StoreThenFetchLoader;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The production {@link Fetcher} of PD-V-06 (srad/agentspaces-pdt.md §3.5, §6.9):
 * a node the store lacks comes from the content swarm's tree exchange. A single
 * node goes out as the bare batch of {@link Content#fetchNodes}, which follows no
 * links, so a lazy {@code get} on a value costs the nodes on its path and nothing
 * more; a whole version goes out as {@link Content#fetchTree} with the roots this
 * store holds as {@code haves}, so a new version costs its delta (S2 of §6.4).
 *
 * <p>Sources. A bare batch names no root, and the swarm draws its sources from the
 * holders of the CIDs themselves plus the preferred peers of its options. A node on
 * a path is held by whoever holds the version's root, so this fetcher keeps the roots
 * it {@linkplain #follow follows} and names the current holders of each as preferred
 * sources on every batch (PD-X-05); {@link #prefer} adds peers by hand. A root is
 * followed when a caller says so, when {@link #fetchTree} fetches it or names it as a
 * base, and when {@link PdtSwarm#hold} pins it.
 *
 * <p>Deadline. Every bare batch carries the fetcher's deadline as the swarm's fetch
 * timeout, so a node with no reachable holder fails after it with an exception
 * naming the CID (PD-X-08), and the future of the batch settles when the batch
 * completes or fails. {@link StoreThenFetchLoader} bounds its caller's wait and
 * leaves the batch running, so a node that is merely late still lands in the store
 * (swarm retries, ROBUSTNESS.md §10.3 P3). Concurrent loads of one CID share one
 * batch, and a load after a caller's deadline joins the batch still in flight.
 */
public final class SwarmFetcher implements Fetcher {

    /** Fetches whose sources {@link #sourcesTried} still remembers after they end. */
    static final int RECENT_CAPACITY = 64;

    private final Content content;
    private final NodeStore store;
    private final Duration deadline;
    private final Set<String> followed = ConcurrentHashMap.newKeySet();
    private final Set<PeerId> preferred = ConcurrentHashMap.newKeySet();
    private final Map<String, CompletableFuture<byte[]>> inFlight = new ConcurrentHashMap<>();
    private final Map<String, TreeFetch> fetches = new ConcurrentHashMap<>();
    private final Map<String, List<String>> recent = new LinkedHashMap<>(RECENT_CAPACITY, 0.75f, false) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, List<String>> eldest) {
            return size() > RECENT_CAPACITY;
        }
    };

    /**
     * A fetcher.
     *
     * @param content  the content swarm of the group, built with a
     *                 {@link NodeStoreTreeStore} over {@code store}
     * @param store    the node store a fetched node lands in
     * @param deadline how long a single node may take; zero waits without bound
     * @throws IllegalStateException when the swarm's tree store is not this store
     */
    public SwarmFetcher(Content content, NodeStore store, Duration deadline) {
        this.content = Objects.requireNonNull(content, "content");
        this.store = Objects.requireNonNull(store, "store");
        this.deadline = Objects.requireNonNull(deadline, "deadline");
        if (deadline.isNegative()) {
            throw new IllegalArgumentException("deadline must not be negative");
        }
        Optional<ai.badmonkey.agentspaces.content.tree.TreeStore> trees = content.treeStore();
        if (trees.isEmpty() || !(trees.get() instanceof NodeStoreTreeStore adapter) || adapter.store() != store) {
            throw new IllegalStateException("the content swarm must be built with Content.Builder.treeStore("
                    + "new NodeStoreTreeStore(store)) over this node store before a fetcher attaches to it");
        }
    }

    /** The content swarm. */
    public Content content() {
        return content;
    }

    /** The node store. */
    public NodeStore store() {
        return store;
    }

    /** The deadline of a single node. */
    public Duration deadline() {
        return deadline;
    }

    // ------------------------------------------------------------ sources

    /**
     * Names a root whose holders serve as sources for bare batches from now on.
     *
     * @param root the root CID
     * @return this
     */
    public SwarmFetcher follow(String root) {
        followed.add(Objects.requireNonNull(root, "root"));
        return this;
    }

    /**
     * Forgets a root named by {@link #follow}.
     *
     * @param root the root CID
     * @return this
     */
    public SwarmFetcher unfollow(String root) {
        followed.remove(root);
        return this;
    }

    /** The roots whose holders are sources. */
    public Set<String> followed() {
        return Set.copyOf(followed);
    }

    /**
     * Names peers preferred as sources on every fetch, whatever they hold.
     *
     * @param peers the peers
     * @return this
     */
    public SwarmFetcher prefer(PeerId... peers) {
        for (PeerId peer : peers) {
            preferred.add(Objects.requireNonNull(peer, "peer"));
        }
        return this;
    }

    /** The peers named by {@link #prefer}. */
    public Set<PeerId> preferred() {
        return Set.copyOf(preferred);
    }

    /**
     * The options of the next bare batch: the preferred peers plus the current
     * holders of every followed root, and the deadline as the fetch timeout.
     *
     * @return the options
     */
    public FetchOptions batchOptions() {
        return FetchOptions.defaults().withTimeout(deadline).prefer(sources().toArray(PeerId[]::new));
    }

    /**
     * The options of a tree fetch: the preferred peers alone, since the swarm
     * reads the holders of the root and the haves itself, and no timeout, so the
     * fetch waits for a holding to appear (PD-X-08).
     *
     * @return the options
     */
    public FetchOptions treeOptions() {
        return FetchOptions.defaults().prefer(preferred.toArray(PeerId[]::new));
    }

    private Set<PeerId> sources() {
        Set<PeerId> sources = new LinkedHashSet<>(preferred);
        for (String root : followed) {
            for (Holding holding : content.spaces().holdings(root)) {
                sources.add(holding.holder());
            }
        }
        return sources;
    }

    // ------------------------------------------------------------ fetching

    /**
     * Fetches one node as a bare batch and completes with its bytes once the
     * store holds them.
     *
     * @param cid the CID
     * @return the bytes, or a failure naming the CID and the sources tried
     */
    @Override
    public CompletableFuture<byte[]> fetch(String cid) {
        Objects.requireNonNull(cid, "cid");
        Optional<byte[]> held = store.get(cid);
        if (held.isPresent()) {
            return CompletableFuture.completedFuture(held.get());
        }
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        CompletableFuture<byte[]> raced = inFlight.putIfAbsent(cid, future);
        if (raced != null) {
            return raced;
        }
        // registered before the batch starts: on the inline fabric a batch can complete inside fetchNodes,
        // and the bookkeeping of settle removes the map entry, which a computeIfAbsent would forbid
        start(cid, future);
        return future;
    }

    private void start(String cid, CompletableFuture<byte[]> future) {
        TreeFetch fetch;
        try {
            fetch = content.fetchNodes(List.of(cid), batchOptions());
        } catch (RuntimeException e) {
            inFlight.remove(cid, future);
            future.completeExceptionally(new NodeUnavailableException(cid, List.of(), "batch refused: " + e.getMessage(), e));
            return;
        }
        fetches.put(cid, fetch);
        // a caller that cancels the future (an interrupted load) forgets the batch and cancels it
        future.whenComplete((bytes, failure) -> {
            forget(cid, fetch, future);
            if (!fetch.isDoneOrFailed()) {
                fetch.cancel();
            }
        });
        if (fetch.isDoneOrFailed()) {
            settle(cid, fetch, future);
            return;
        }
        Thread.ofVirtual().name("pdt-fetch-" + cid.substring(Math.max(0, cid.length() - 8))).start(() -> {
            try {
                fetch.await(Duration.ZERO); // until the batch completes or fails: its own timeout bounds it (PD-X-08)
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (java.util.concurrent.CancellationException cancelled) {
                // a cancelled batch settles too, or its CID stays in flight and every later load joins a batch that
                // never ends (swarm retries, the gate's adversarial review, ROBUSTNESS.md §10.6)
            }
            settle(cid, fetch, future);
        });
    }

    /** Settles the future from the batch's state; the bookkeeping runs first, so a woken waiter sees no batch in flight. */
    private void settle(String cid, TreeFetch fetch, CompletableFuture<byte[]> future) {
        if (future.isDone()) {
            return;
        }
        forget(cid, fetch, future);
        List<String> sources = sourcesOf(fetch);
        if (fetch.complete()) {
            Optional<byte[]> bytes = store.get(cid);
            if (bytes.isPresent()) {
                future.complete(bytes.get());
                return;
            }
            future.completeExceptionally(new NodeUnavailableException(cid, sources, "the batch completed but the store lacks the node"));
            return;
        }
        Optional<Throwable> failure = fetch.failure();
        if (failure.isPresent()) {
            future.completeExceptionally(new NodeUnavailableException(cid, sources, failure.get().getMessage(), failure.get()));
            return;
        }
        String why = fetch.cancelled() ? "fetch cancelled"
                : "the wait was interrupted (" + fetch.progress().requestsSent() + " requests sent)";
        future.completeExceptionally(new NodeUnavailableException(cid, sources, why));
    }

    /**
     * Fetches every node under a root the store lacks, pruned by the bases the
     * store holds, and follows the root and the bases.
     *
     * @param root  the root CID
     * @param haves roots this store holds complete; at most 64
     * @return the handle
     */
    public TreeFetch fetchTree(String root, List<String> haves) {
        Objects.requireNonNull(root, "root");
        List<String> bases = haves == null ? List.of() : List.copyOf(haves);
        follow(root);
        bases.forEach(this::follow);
        return content.fetchTree(root, bases, treeOptions());
    }

    /**
     * The sources a fetch for a CID has tried: the peers that answered a batch in
     * flight, or the last ended batch for the CID.
     *
     * @param cid the CID
     * @return source names
     */
    @Override
    public List<String> sourcesTried(String cid) {
        TreeFetch fetch = fetches.get(cid);
        if (fetch != null) {
            return sourcesOf(fetch);
        }
        synchronized (recent) {
            return recent.getOrDefault(cid, List.of());
        }
    }

    /** The batches in flight, by CID. */
    public Map<String, TreeFetch> inFlight() {
        return Map.copyOf(fetches);
    }

    private static List<String> sourcesOf(TreeFetch fetch) {
        List<String> names = new ArrayList<>();
        for (PeerId peer : fetch.progress().bytesBySource().keySet()) {
            names.add(peer.display());
        }
        return names;
    }

    /** Drops a batch from the in-flight maps and keeps its sources for {@link #sourcesTried}; idempotent. */
    private void forget(String cid, TreeFetch fetch, CompletableFuture<byte[]> future) {
        if (fetches.remove(cid, fetch)) {
            remember(cid, sourcesOf(fetch));
        }
        inFlight.remove(cid, future);
    }

    private void remember(String cid, List<String> sources) {
        synchronized (recent) {
            recent.put(cid, List.copyOf(sources));
        }
    }
}
