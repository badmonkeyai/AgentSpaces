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
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;

/**
 * Who may fetch and serve, how a peer seeds, and what it spends
 * (srad/large-networks-bulk-data.md §3.6, group P). The defaults admit any
 * member, seed what the peer completes, and bound nothing but the window and
 * session counts.
 */
public interface ContentPolicy {

    /** How a peer publishes holdings after a fetch. */
    enum SeedMode {
        /** Publish a holding from the first verified piece and keep serving after completion. */
        AUTO,
        /** Publish only once complete. */
        COMPLETE_ONLY,
        /** Never publish a holding and refuse every want. */
        NEVER
    }

    /** Whether this peer may fetch a root. Consulted once per fetch. */
    default boolean mayFetch(GroupId group, PeerId requester, Manifest manifest) {
        return true;
    }

    /** Whether this peer serves a root to a requester. Consulted once per source per root. */
    default boolean mayServe(GroupId group, PeerId requester, Manifest manifest) {
        return true;
    }

    /** The seed mode (decision 9). */
    default SeedMode seedMode() {
        return SeedMode.AUTO;
    }

    /** Bytes this peer serves per UTC day across all roots; zero or less is unbounded. */
    default long uploadBytesPerDay() {
        return 0;
    }

    /** Sources per fetch. */
    /**
     * The bytes this peer may request from holders per UTC day across its fetches (LB-P-06), for a
     * metered link; 0, the default, is unbounded. A fetch that reaches it waits until the next day.
     */
    default long downloadBytesPerDay() {
        return 0;
    }

    default int maxSessions() {
        return 16;
    }

    /** Ranges in flight per source. */
    default int maxWindow() {
        return 32;
    }

    /** Concurrent fetches per peer; further fetches queue. */
    default int maxFetches() {
        return 4;
    }

    /** The default policy. */
    ContentPolicy DEFAULT = new ContentPolicy() {
    };
}
