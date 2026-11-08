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
package ai.badmonkey.agentspaces.pdt.seal;

import ai.badmonkey.agentspaces.pdt.NodeFormatException;

/**
 * A node this peer holds but cannot open, or a node it cannot seal: the ring lacks
 * a key for the epoch (srad/agentspaces-pdt.md §3.8, PD-P-05). A
 * {@link NodeFormatException} so that every reader that refuses a malformed node
 * refuses a sealed one the same way; the epoch tells a key agent what to fetch.
 */
public final class SealedNodeException extends NodeFormatException {

    private final long epoch;

    /**
     * @param cid     the CID, or null for a seal refusal
     * @param epoch   the epoch, or -1 when none is in effect
     * @param message the rule
     */
    public SealedNodeException(String cid, long epoch, String message) {
        super(cid, message);
        this.epoch = epoch;
    }

    /** The epoch the node was sealed under, or -1 when no epoch is in effect. */
    public long epoch() {
        return epoch;
    }
}
