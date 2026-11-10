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
package ai.badmonkey.agentspaces.pdt.store;

import ai.badmonkey.agentspaces.pdt.NodeFormatException;
import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.NodeUnavailableException;
import ai.badmonkey.agentspaces.pdt.values.NodeLoader;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The node loader of PD-V-06 (srad/agentspaces-pdt.md §3.2): a value asks for a
 * node by CID, the loader answers from the {@link NodeStore} when it holds the node,
 * and otherwise asks the {@link Fetcher} and waits at most the deadline (10 s by
 * default, {@code agentspaces.pdt.load-timeout}). A fetched node is verified against
 * its CID and stored before it is returned, so the next load is local. When the
 * deadline passes, the fetch fails, or the bytes do not hash to the CID, the loader
 * throws {@link NodeUnavailableException} naming the CID and the sources the fetcher
 * reports. The deadline ends the caller's wait alone: the fetch goes on, so a node
 * that is merely late lands in the store and the next load of it finds it there or
 * joins the same fetch (swarm retries, ROBUSTNESS.md §10.3 P3). The tree exchange supplies the production fetcher in phase 2; the
 * persistent values of §3.2 call {@link #load} through the {@link NodeLoader}
 * interface this class implements.
 */
public final class StoreThenFetchLoader implements NodeLoader {

    /** The default deadline. */
    public static final Duration DEFAULT_DEADLINE = Duration.ofSeconds(10);

    private final NodeStore store;
    private final Fetcher fetcher;
    private final Duration deadline;

    /**
     * A loader.
     *
     * @param store    the store read first
     * @param fetcher  where a missing node comes from
     * @param deadline how long a load waits on the fetcher
     */
    public StoreThenFetchLoader(NodeStore store, Fetcher fetcher, Duration deadline) {
        this.store = Objects.requireNonNull(store, "store");
        this.fetcher = Objects.requireNonNull(fetcher, "fetcher");
        this.deadline = Objects.requireNonNull(deadline, "deadline");
        if (deadline.isNegative()) {
            throw new IllegalArgumentException("deadline must not be negative");
        }
    }

    /** A loader with the default deadline. */
    public StoreThenFetchLoader(NodeStore store, Fetcher fetcher) {
        this(store, fetcher, DEFAULT_DEADLINE);
    }

    /** The store. */
    public NodeStore store() {
        return store;
    }

    /** The fetcher. */
    public Fetcher fetcher() {
        return fetcher;
    }

    /** The deadline. */
    public Duration deadline() {
        return deadline;
    }

    /**
     * Returns a node from the store when held, without fetching.
     *
     * @param cid the CID
     * @return the bytes, when held
     */
    public Optional<byte[]> loadIfHeld(String cid) {
        return store.get(cid);
    }

    /**
     * Loads a node: from the store, or from the fetcher within the deadline.
     *
     * @param cid the CID
     * @return the bytes, verified against the CID
     * @throws NodeUnavailableException when the node cannot be produced in time
     */
    @Override
    public byte[] load(String cid) {
        Objects.requireNonNull(cid, "cid");
        Optional<byte[]> held = store.get(cid);
        if (held.isPresent()) {
            return held.get();
        }
        CompletableFuture<byte[]> future;
        try {
            future = fetcher.fetch(cid);
        } catch (RuntimeException e) {
            throw new NodeUnavailableException(cid, fetcher.sourcesTried(cid), "fetch refused: " + e.getMessage(), e);
        }
        if (future == null) {
            throw new NodeUnavailableException(cid, fetcher.sourcesTried(cid), "fetcher returned nothing");
        }
        byte[] bytes;
        try {
            bytes = future.get(deadline.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new NodeUnavailableException(cid, fetcher.sourcesTried(cid),
                    "deadline of " + deadline.toMillis() + " ms passed", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            if (cause instanceof NodeUnavailableException unavailable) {
                throw unavailable;
            }
            throw new NodeUnavailableException(cid, fetcher.sourcesTried(cid), "fetch failed: " + cause.getMessage(), cause);
        } catch (CancellationException e) {
            throw new NodeUnavailableException(cid, fetcher.sourcesTried(cid), "fetch cancelled", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            throw new NodeUnavailableException(cid, fetcher.sourcesTried(cid), "interrupted", e);
        }
        if (bytes == null) {
            throw new NodeUnavailableException(cid, fetcher.sourcesTried(cid), "fetch produced no bytes");
        }
        try {
            store.putVerified(cid, bytes);
        } catch (NodeFormatException e) {
            throw new NodeUnavailableException(cid, fetcher.sourcesTried(cid), "fetched bytes refused: " + e.rule(), e);
        }
        return bytes;
    }
}
