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
package ai.badmonkey.agentspaces.api.spi;

import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.util.Optional;

/**
 * The seam through which a space escalates a payload too large for one block
 * (srad/large-networks-bulk-data.md group E). A payload above the space's
 * escalation threshold is handed here on write and comes back as a reference
 * (a content root, multibase {@code b}) that travels in {@code payloadRef}; a
 * reader asks here for the bytes once they are complete locally and starts
 * acquiring them otherwise. The content swarm implements it; the space never
 * names the implementation.
 */
public interface BulkPayloads {

    /**
     * Publishes a payload and returns its reference.
     *
     * @param payload the bytes, sealed already when the space seals
     * @param lease   the entry's lease; the payload stays pinned while it lives
     * @return the reference, beginning with the multibase {@code b} prefix
     */
    String publish(byte[] payload, Lease lease);

    /**
     * The payload when it is complete here.
     *
     * @param ref the reference
     * @return the bytes, or empty when not yet complete
     */
    Optional<byte[]> local(String ref);

    /**
     * Starts or continues acquiring a payload and returns at once; the callback
     * runs once the payload is complete here.
     *
     * @param ref       the reference
     * @param issuer    the entry's issuer, a likely holder
     * @param onArrival what to run when the bytes are complete; may be a no-op
     */
    void ensure(String ref, PeerId issuer, Runnable onArrival);

    /**
     * Releases a reference once its entry is collected, so the payload may be evicted.
     *
     * @param ref the reference
     */
    default void release(String ref) {
    }

    /** Whether a reference is a bulk reference rather than a single block's CID. */
    static boolean isBulk(String ref) {
        return ref != null && ref.length() > 1 && ref.charAt(0) == 'b';
    }
}
