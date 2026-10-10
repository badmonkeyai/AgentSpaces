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
package ai.badmonkey.agentspaces.api.entry;

import ai.badmonkey.agentspaces.common.hlc.HlcTimestamp;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.SpaceId;

import java.util.Map;
import java.util.Objects;

/**
 * The wrapper the space keeps around every written entry (spec §7.1). Small
 * payloads travel inline as canonical CBOR; large payloads are content-addressed
 * via {@link #payloadRef()} and fetched out of band, so the space replicates only
 * the record.
 *
 * @param entryId    issuer-generated identifier
 * @param spaceId    the space this record belongs to
 * @param type       schema name (and version) of the payload
 * @param payload    canonical serialized entry, inline; {@code null} when
 *                   {@code payloadRef} is set
 * @param payloadRef content hash (CID) of an out-of-band payload; {@code null}
 *                   when the payload is inline
 * @param issuer     the writing agent
 * @param issued     hybrid-logical-clock issue timestamp
 * @param lease      the lease currently attached to the entry
 * @param tags       indexable string tags
 * @param sig        issuer signature over the record; {@code null} in unsigned
 *                   local-only spaces
 */
public record EntryRecord(
        EntryId entryId,
        SpaceId spaceId,
        String type,
        byte[] payload,
        String payloadRef,
        AgentId issuer,
        HlcTimestamp issued,
        LeaseInfo lease,
        Map<String, String> tags,
        byte[] sig,
        // v0.1.13 (SPEC §11a.1): the content-key epoch the payload is sealed
        // under; omitted for epoch 0 (and for unencrypted spaces), so such a
        // record is byte-identical to the v0.1.12 record and its signed view.
        @com.fasterxml.jackson.annotation.JsonInclude(
                com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        Long keyEpoch) {

    /** The v0.1.12 shape: no key epoch (unencrypted, or sealed under epoch 0). */
    public EntryRecord(EntryId entryId, SpaceId spaceId, String type, byte[] payload,
                       String payloadRef, AgentId issuer, HlcTimestamp issued, LeaseInfo lease,
                       Map<String, String> tags, byte[] sig) {
        this(entryId, spaceId, type, payload, payloadRef, issuer, issued, lease, tags, sig, null);
    }

    /** Inline payload size limit; larger payloads must be content-addressed (spec §7.1). */
    public static final int INLINE_PAYLOAD_LIMIT = 64 * 1024;

    public EntryRecord {
        Objects.requireNonNull(entryId, "entryId");
        Objects.requireNonNull(spaceId, "spaceId");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(issuer, "issuer");
        Objects.requireNonNull(issued, "issued");
        Objects.requireNonNull(lease, "lease");
        Objects.requireNonNull(tags, "tags");
        if ((payload == null) == (payloadRef == null)) {
            throw new IllegalArgumentException("exactly one of payload or payloadRef must be set");
        }
        if (payload != null && payload.length > INLINE_PAYLOAD_LIMIT) {
            throw new IllegalArgumentException(
                    "inline payload exceeds " + INLINE_PAYLOAD_LIMIT + " bytes ("
                            + payload.length + "); use a content-addressed payloadRef");
        }
        // The record signature covers the tags in iteration order (SignView, TECH-SPEC
        // §7.2), so the copy must keep the order the map arrived in: Map.copyOf
        // iterates in an order salted per JVM start, and a receiver that rebuilt
        // the view from such a copy would refuse every honest record with two or
        // more tags about half the time (issue #16, found by the ticket vector).
        tags = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(tags));
        if (keyEpoch != null && keyEpoch < 1) {
            throw new IllegalArgumentException("a record names key epoch 1 or later, or none: "
                    + keyEpoch);
        }
    }

    /** The epoch the payload is sealed under: 0 when the record names none. */
    public long sealedEpoch() {
        return keyEpoch == null ? 0 : keyEpoch;
    }

    /**
     * Returns a copy of this record with a different lease, preserving everything
     * else. Lease transitions (take, reappear, renew) go through this method.
     *
     * @param newLease the replacement lease
     * @return the updated record
     */
    public EntryRecord withLease(LeaseInfo newLease) {
        return new EntryRecord(entryId, spaceId, type, payload, payloadRef,
                issuer, issued, newLease, tags, sig, keyEpoch);
    }
}
