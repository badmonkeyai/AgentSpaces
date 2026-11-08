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

import java.util.Collection;
import java.util.List;
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
     * As {@link #ensure(String, PeerId, Runnable)}, and runs {@code onFailure} when the
     * acquisition gives up (every source unreachable past the seam's deadline), so the
     * space can retry it later with a back-off instead of waiting for good
     * (srad/agentspaces-pdt.md PD-Q-AVL-01). The default never reports a failure,
     * which is what a seam whose transfers never give up needs.
     *
     * @param ref       the reference
     * @param issuer    the entry's issuer, a likely holder
     * @param onArrival what to run when the bytes are complete
     * @param onFailure what to run when this attempt gave up; the space may call {@code ensure} again
     */
    default void ensure(String ref, PeerId issuer, Runnable onArrival, Runnable onFailure) {
        ensure(ref, issuer, onArrival);
    }

    /**
     * Releases a reference once its entry is collected, so the payload may be evicted.
     *
     * @param ref the reference
     */
    default void release(String ref) {
    }

    /**
     * Whether this seam carries the reference (srad/agentspaces-pdt.md PD-E-01):
     * a space with a primary seam and a fallback asks each in turn, so a reader
     * reassembles a reference through the seam that published it. The default
     * accepts every bulk reference; a seam that names its own form (the tree
     * seam accepts dag-cbor roots only) narrows it.
     *
     * @param ref the reference
     * @return true when {@link #local}, {@link #ensure}, and {@link #release} apply to it
     */
    default boolean accepts(String ref) {
        return isBulk(ref);
    }

    /**
     * The members of a group that cannot receive a payload through this seam
     * right now (srad/agentspaces-pdt.md PD-E-06): a writer escalates through
     * this seam only when the list is empty and falls back otherwise, and a
     * space that requires the seam refuses the write naming them. The default
     * names nobody, since the flat seam carries any size to any member.
     *
     * @param members the current members of the group, without the writer
     * @return the members the seam cannot reach, empty when the seam is usable now
     */
    default List<PeerId> unable(Collection<PeerId> members) {
        return List.of();
    }

    /** Whether a reference is a bulk reference rather than a single block's CID. */
    static boolean isBulk(String ref) {
        return ref != null && ref.length() > 1 && ref.charAt(0) == 'b';
    }
}
