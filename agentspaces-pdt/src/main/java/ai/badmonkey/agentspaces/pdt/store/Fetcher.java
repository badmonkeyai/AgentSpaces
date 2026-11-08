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

import ai.badmonkey.agentspaces.pdt.NodeUnavailableException;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Where a {@link StoreThenFetchLoader} turns when the store lacks a node
 * (srad/agentspaces-pdt.md PD-V-06). The tree exchange of §3.5 supplies the
 * production implementation in phase 2; until then a test supplies one.
 * The future completes with the node's bytes, which the loader verifies against the
 * CID before it stores them, or fails with the exception the fetch met.
 */
@FunctionalInterface
public interface Fetcher {

    /**
     * Starts a fetch.
     *
     * @param cid the CID wanted
     * @return the bytes, once they arrive
     */
    CompletableFuture<byte[]> fetch(String cid);

    /**
     * The sources a fetch for a CID has tried, for the message of
     * {@link NodeUnavailableException}; empty by default.
     *
     * @param cid the CID
     * @return source names
     */
    default List<String> sourcesTried(String cid) {
        return List.of();
    }

    /** A fetcher that holds nothing: every fetch fails at once. */
    static Fetcher none() {
        return cid -> CompletableFuture.failedFuture(new NodeUnavailableException(cid, List.of(), "no fetcher"));
    }
}
