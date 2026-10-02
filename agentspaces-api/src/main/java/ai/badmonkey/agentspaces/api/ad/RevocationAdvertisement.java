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
package ai.badmonkey.agentspaces.api.ad;

import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * The authoritative eject (security remediation plan §9): a statement by the
 * group's trust root that a peer's identity is no longer legitimate. Gossiped
 * peer-to-peer over the existing rumor and anti-entropy channels as a
 * convenience cache, it is authoritative only because of <em>who signed it</em>
 * — the group founder today, an identity-provider assertion or CA statement
 * under the enterprise profiles — which is what keeps the revocation channel
 * itself from becoming an attack surface: no accusation consensus, no
 * reputation math, exactly one authorized issuer. Receivers refuse the revoked
 * peer's frames permanently, evict it from the membership view, and re-gossip.
 *
 * <p>Identity <em>rotation</em> is a revocation with a {@link #successor}: the
 * trust root re-binds the principal to a fresh PeerID (self-certifying
 * identities cannot rotate their key in place) and names the new identity, so
 * higher layers can carry attribution across the change. The successor gains
 * nothing from being named — it joins and is authorized exactly like any new
 * peer — which keeps this simplest rotation shape easy to augment or replace
 * with richer succession protocols without touching enforcement.
 *
 * <p>The TTL bounds how long caches re-gossip the advertisement, not how long
 * the revocation holds: a receiver that has accepted a revocation refuses the
 * peer for as long as it runs, and anti-entropy converges late joiners from any
 * member's retained set.
 *
 * @param id        URI-style identifier
 * @param issuer    the trust root issuing the revocation
 * @param group     the group the revocation is scoped to
 * @param issued    issue instant
 * @param ttl       re-gossip lifetime (not the revocation's duration)
 * @param revoked   the peer whose identity is withdrawn
 * @param reason    operator-facing reason, e.g. {@code compromised}, {@code rotation}
 * @param successor the replacement identity under rotation; {@code null} for a
 *                  plain revocation
 * @param evidence  CA evidence that the revoked peer's channel certificate is
 *                  revoked (CBOR {@code RevocationEvidence}: the chain and,
 *                  optionally, the CRL), for a revocation rooted in the CA
 *                  rather than the founder (SPEC §6.1, v0.1.13); {@code null}
 *                  otherwise, and then omitted from the bytes
 */
public record RevocationAdvertisement(
        String id,
        PeerId issuer,
        GroupId group,
        Instant issued,
        Duration ttl,
        PeerId revoked,
        String reason,
        PeerId successor,
        @com.fasterxml.jackson.annotation.JsonInclude(
                com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        byte[] evidence) implements Advertisement {

    /** Most evidence bytes a revocation may carry (96 KiB). */
    public static final int MAX_EVIDENCE = 96 * 1024;

    /**
     * A revocation without evidence: the founder's, as before v0.1.13.
     *
     * @param id        URI-style identifier
     * @param issuer    the trust root issuing the revocation
     * @param group     the group the revocation is scoped to
     * @param issued    issue instant
     * @param ttl       re-gossip lifetime
     * @param revoked   the peer whose identity is withdrawn
     * @param reason    operator-facing reason
     * @param successor the replacement identity under rotation, or null
     */
    public RevocationAdvertisement(String id, PeerId issuer, GroupId group, Instant issued,
                                   Duration ttl, PeerId revoked, String reason, PeerId successor) {
        this(id, issuer, group, issued, ttl, revoked, reason, successor, null);
    }

    public RevocationAdvertisement {
        if (evidence != null && evidence.length > MAX_EVIDENCE) {
            throw new IllegalArgumentException("revocation evidence exceeds " + MAX_EVIDENCE + " bytes");
        }
        evidence = evidence == null ? null : evidence.clone();
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(issuer, "issuer");
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(issued, "issued");
        Objects.requireNonNull(ttl, "ttl");
        Objects.requireNonNull(revoked, "revoked");
        reason = reason == null ? "" : reason;
    }

    @Override
    public byte[] evidence() {
        return evidence == null ? null : evidence.clone();
    }
}
