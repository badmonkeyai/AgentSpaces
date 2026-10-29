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

import ai.badmonkey.agentspaces.agent.content.Manifest;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The handle of a fetch (LB-S-01): progress, completion, cancellation, the
 * artifact once complete, and a trace of what the scheduler did.
 */
public interface Fetch {

    /** The root being fetched. */
    String root();

    /**
     * How full each source's window was, on average, while the source held pieces this fetch
     * lacked (LB-Q-PRF-04): 1.0 means the scheduler always had a request in every slot.
     */
    default java.util.Map<ai.badmonkey.agentspaces.common.id.PeerId, Double> windowFill() {
        return java.util.Map.of();
    }

    /** The manifest, once known. */
    Optional<Manifest> manifest();

    /** Where the fetch stands. */
    Progress progress();

    /**
     * Waits for completion.
     *
     * @param timeout how long to wait
     * @return whether the fetch completed within the timeout
     * @throws InterruptedException when interrupted
     */
    boolean await(Duration timeout) throws InterruptedException;

    /** Whether every piece has verified. */
    boolean complete();

    /** Whether the fetch has ended in any way: complete, failed, or cancelled. */
    boolean isDoneOrFailed();

    /** The failure, when the fetch ended without completing. */
    Optional<Throwable> failure();

    /** Whether the fetch was cancelled. */
    boolean cancelled();

    /** Stops the fetch; verified pieces stay in the store. */
    void cancel();

    /**
     * Opens the complete artifact.
     *
     * @return a read-only channel
     * @throws IOException when the artifact is incomplete
     */
    SeekableByteChannel open() throws IOException;

    /** The scheduler's trace, newest last, bounded. */
    List<String> trace();

    /**
     * A snapshot of progress.
     *
     * @param piecesVerified pieces verified so far
     * @param pieces         pieces in the artifact, 0 until the manifest is known
     * @param bytesReceived  payload bytes received and verified
     * @param bytesBySource  verified bytes per source
     * @param sourcesActive  sources with a request in flight or a window open
     * @param waiting        whether no source is reachable at the moment
     * @param etaMillis      the estimated time to completion at the rate so far (LB-S-01); -1 while unknown
     */
    record Progress(int piecesVerified, int pieces, long bytesReceived, Map<PeerId, Long> bytesBySource,
                    int sourcesActive, boolean waiting, long etaMillis) {

        /** The form without an estimate, as 0.5.0's first builds made it. */
        public Progress(int piecesVerified, int pieces, long bytesReceived, Map<PeerId, Long> bytesBySource,
                        int sourcesActive, boolean waiting) {
            this(piecesVerified, pieces, bytesReceived, bytesBySource, sourcesActive, waiting, -1);
        }

        /** Completion as a fraction, 0 to 1. */
        public double fraction() {
            return pieces == 0 ? 0 : (double) piecesVerified / pieces;
        }
    }
}
