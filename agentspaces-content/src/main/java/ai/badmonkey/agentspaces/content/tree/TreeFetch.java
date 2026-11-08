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

import ai.badmonkey.agentspaces.common.id.PeerId;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The handle of a tree fetch (srad/agentspaces-pdt.md §3.5, PD-X-04 to PD-X-08):
 * what {@code Content.fetchTree} and {@code Content.fetchNodes} return. It reads
 * like the piece fetch handle: progress, completion, cancellation, a trace.
 */
public interface TreeFetch {

    /** The root being fetched, or the first CID of a bare node batch. */
    String root();

    /** Where the fetch stands. */
    Progress progress();

    /**
     * Waits for completion. As {@code Fetch.await}, a zero or negative timeout
     * waits without bound.
     *
     * @param timeout how long to wait; zero waits forever
     * @return whether the fetch completed within the timeout
     * @throws InterruptedException when interrupted
     */
    boolean await(Duration timeout) throws InterruptedException;

    /** Whether every node reachable from the root (or every node of the batch) is held and verified. */
    boolean complete();

    /** Whether the fetch has ended in any way: complete, failed, or cancelled. */
    boolean isDoneOrFailed();

    /** The failure, when the fetch ended without completing. */
    Optional<Throwable> failure();

    /** Whether the fetch was cancelled. */
    boolean cancelled();

    /** Stops the fetch; verified nodes stay in the store. */
    void cancel();

    /** The scheduler's trace, newest last, bounded. */
    List<String> trace();

    /**
     * A snapshot of progress.
     *
     * @param nodesVerified nodes received in this fetch that verified against their CID, each counted once
     * @param bytesReceived node bytes received that verified against their CID, a duplicate delivery
     *                      included, so the cost of the endgame is visible
     * @param bytesBySource verified bytes per source, each node counted once
     * @param sourcesActive sources with a request in flight
     * @param requestsSent  {@code NODE_WANT} and {@code TREE_WANT} messages sent so far
     * @param roundTrips    the longest chain of requests in which each needed an earlier answer:
     *                      the sequential round trips of the fetch, whatever ran in parallel (PD-Q-PRF-06)
     * @param waiting       whether no usable source is known at the moment (PD-X-08)
     */
    record Progress(int nodesVerified, long bytesReceived, Map<PeerId, Long> bytesBySource,
                    int sourcesActive, int requestsSent, int roundTrips, boolean waiting) {
    }
}
