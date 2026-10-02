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

import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Set;

/**
 * A revocation of something certified other than a peer identity (SPEC §6.1,
 * v0.1.13): an agent, one agent key, an X.509 channel leaf, or a join
 * credential. Peer revocations keep their own {@link RevocationAdvertisement}
 * and stream, unchanged. Like those, this is authoritative because of who
 * signed it (the founder; for an agent or agent key, also the agent's own
 * peer), and its TTL bounds re-gossip, not how long the revocation holds.
 *
 * <p>The {@link #reason()} decides what still verifies (the freeze rule):
 * under {@link #KEY_COMPROMISE} nothing the target signs is accepted after the
 * revocation arrives, whatever its claimed signing time, because a stolen key
 * can back-date; under any other reason a signature whose signing time
 * precedes {@link #effectiveFrom()} still verifies, so a late joiner receives
 * the target's history. Either way what a replica already holds stays.
 *
 * @param id            {@code aspace://<group>/revocation/<kind>/<canonical target>}
 * @param issuer        the revoking peer
 * @param group         the group the revocation is scoped to
 * @param issued        issue instant
 * @param ttl           re-gossip lifetime (not the revocation's duration)
 * @param target        what is revoked
 * @param reason        one of {@link #REASONS}
 * @param effectiveFrom from when signatures stop verifying under a non-compromise
 *                      reason; null means {@code issued}
 * @param evidence      CA evidence for a trust-root-issued revocation (SPEC §6.1), or null
 */
public record CredentialRevocation(
        String id,
        PeerId issuer,
        GroupId group,
        Instant issued,
        Duration ttl,
        Target target,
        String reason,
        @JsonInclude(JsonInclude.Include.NON_NULL) Instant effectiveFrom,
        @JsonInclude(JsonInclude.Include.NON_NULL) byte[] evidence) implements Advertisement {

    /** A stolen key: refuse everything it signs from now on. */
    public static final String KEY_COMPROMISE = "key-compromise";
    /** Retired in the ordinary course. */
    public static final String RETIRED = "retired";
    /** Replaced by a newer credential. */
    public static final String SUPERSEDED = "superseded";
    /** No longer entitled. */
    public static final String PRIVILEGE_WITHDRAWN = "privilege-withdrawn";
    /** No reason given. */
    public static final String UNSPECIFIED = "unspecified";
    /** The reasons a revocation may carry. */
    public static final Set<String> REASONS =
            Set.of(KEY_COMPROMISE, RETIRED, SUPERSEDED, PRIVILEGE_WITHDRAWN, UNSPECIFIED);

    /** Most evidence bytes a revocation may carry (96 KiB). */
    public static final int MAX_EVIDENCE = 96 * 1024;

    public CredentialRevocation {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(issuer, "issuer");
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(issued, "issued");
        Objects.requireNonNull(ttl, "ttl");
        Objects.requireNonNull(target, "target");
        reason = reason == null || reason.isBlank() ? UNSPECIFIED : reason;
        if (!REASONS.contains(reason)) {
            throw new IllegalArgumentException("unknown revocation reason: " + reason);
        }
        if (evidence != null && evidence.length > MAX_EVIDENCE) {
            throw new IllegalArgumentException("revocation evidence exceeds " + MAX_EVIDENCE + " bytes");
        }
        evidence = evidence == null ? null : evidence.clone();
    }

    /**
     * A revocation with the canonical id, effective from its issue instant.
     *
     * @param issuer the revoking peer
     * @param group  the group
     * @param issued issue instant
     * @param ttl    re-gossip lifetime
     * @param target what is revoked
     * @param reason one of {@link #REASONS}
     * @return the revocation
     */
    public static CredentialRevocation of(PeerId issuer, GroupId group, Instant issued, Duration ttl,
                                          Target target, String reason) {
        return new CredentialRevocation(idFor(group, target), issuer, group, issued, ttl, target,
                reason, null, null);
    }

    /**
     * The canonical id of a revocation of a target in a group.
     *
     * @param group  the group
     * @param target the target
     * @return the id
     */
    public static String idFor(GroupId group, Target target) {
        return "aspace://" + group.value() + "/revocation/" + target.kind().toLowerCase() + "/"
                + target.canonical();
    }

    /** Whether the reason is {@link #KEY_COMPROMISE}. */
    public boolean compromise() {
        return KEY_COMPROMISE.equals(reason);
    }

    /** From when signatures stop verifying: {@link #effectiveFrom()}, else {@link #issued()}. */
    public Instant effectiveSince() {
        return effectiveFrom == null ? issued : effectiveFrom;
    }

    /**
     * Whether a signature made at {@code signingTime} is refused under this
     * revocation: always under {@link #KEY_COMPROMISE} or when the signing
     * time is unknown (null), else when it is at or after {@link #effectiveSince()}.
     *
     * @param signingTime the signature's signing time, or null when unknown
     * @return whether the signature is refused
     */
    public boolean refusesAt(Instant signingTime) {
        return compromise() || signingTime == null || !signingTime.isBefore(effectiveSince());
    }

    @Override
    public byte[] evidence() {
        return evidence == null ? null : evidence.clone();
    }

    /**
     * What is revoked. Fields a kind does not use are null.
     *
     * @param kind                   one of {@link #AGENT}, {@link #AGENT_KEY},
     *                               {@link #X509_LEAF}, {@link #JOIN_CREDENTIAL}
     * @param agent                  the agent (AGENT, AGENT_KEY)
     * @param keyFingerprint         SHA-256 of the agent's raw public key (AGENT_KEY)
     * @param x509Issuer             the leaf's issuer DN (X509_LEAF)
     * @param x509Serial             the leaf's serial, hex (X509_LEAF)
     * @param certificateFingerprint SHA-256 of the leaf's DER (X509_LEAF)
     * @param credentialHash         SHA-256 of the join credential's bytes (JOIN_CREDENTIAL)
     */
    public record Target(
            String kind,
            @JsonInclude(JsonInclude.Include.NON_NULL) AgentId agent,
            @JsonInclude(JsonInclude.Include.NON_NULL) byte[] keyFingerprint,
            @JsonInclude(JsonInclude.Include.NON_NULL) String x509Issuer,
            @JsonInclude(JsonInclude.Include.NON_NULL) String x509Serial,
            @JsonInclude(JsonInclude.Include.NON_NULL) byte[] certificateFingerprint,
            @JsonInclude(JsonInclude.Include.NON_NULL) byte[] credentialHash) {

        /** Every key of one agent. */
        public static final String AGENT = "AGENT";
        /** One key of one agent. */
        public static final String AGENT_KEY = "AGENT_KEY";
        /** One X.509 channel leaf. */
        public static final String X509_LEAF = "X509_LEAF";
        /** One join credential. */
        public static final String JOIN_CREDENTIAL = "JOIN_CREDENTIAL";

        public Target {
            Objects.requireNonNull(kind, "kind");
            switch (kind) {
                case AGENT -> require(agent != null, "an AGENT target names the agent");
                case AGENT_KEY -> require(agent != null && sha256Sized(keyFingerprint),
                        "an AGENT_KEY target names the agent and a 32-byte key fingerprint");
                case X509_LEAF -> require(sha256Sized(certificateFingerprint),
                        "an X509_LEAF target carries a 32-byte certificate fingerprint");
                case JOIN_CREDENTIAL -> require(sha256Sized(credentialHash),
                        "a JOIN_CREDENTIAL target carries a 32-byte credential hash");
                default -> throw new IllegalArgumentException("unknown revocation target kind: " + kind);
            }
            keyFingerprint = keyFingerprint == null ? null : keyFingerprint.clone();
            certificateFingerprint = certificateFingerprint == null ? null : certificateFingerprint.clone();
            credentialHash = credentialHash == null ? null : credentialHash.clone();
        }

        /** Every key of an agent. */
        public static Target agent(AgentId agent) {
            return new Target(AGENT, agent, null, null, null, null, null);
        }

        /** One agent key, by the SHA-256 of its raw public key. */
        public static Target agentKey(AgentId agent, byte[] keyFingerprint) {
            return new Target(AGENT_KEY, agent, keyFingerprint, null, null, null, null);
        }

        /** One X.509 leaf. */
        public static Target x509Leaf(String issuerDn, String serialHex, byte[] certificateFingerprint) {
            return new Target(X509_LEAF, null, null, issuerDn, serialHex, certificateFingerprint, null);
        }

        /** One join credential, by the SHA-256 of its bytes. */
        public static Target joinCredential(byte[] credentialHash) {
            return new Target(JOIN_CREDENTIAL, null, null, null, null, null, credentialHash);
        }

        /** The target's canonical text, unique per target within its kind. */
        public String canonical() {
            HexFormat hex = HexFormat.of();
            return switch (kind) {
                case AGENT -> agent.encoded();
                case AGENT_KEY -> agent.encoded() + "#" + hex.formatHex(keyFingerprint);
                case X509_LEAF -> hex.formatHex(certificateFingerprint);
                default -> hex.formatHex(credentialHash);
            };
        }

        /** The registry key: kind and canonical target. */
        public String key() {
            return kind + ":" + canonical();
        }

        @Override
        public byte[] keyFingerprint() {
            return keyFingerprint == null ? null : keyFingerprint.clone();
        }

        @Override
        public byte[] certificateFingerprint() {
            return certificateFingerprint == null ? null : certificateFingerprint.clone();
        }

        @Override
        public byte[] credentialHash() {
            return credentialHash == null ? null : credentialHash.clone();
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Target other && key().equals(other.key());
        }

        @Override
        public int hashCode() {
            return key().hashCode();
        }

        private static boolean sha256Sized(byte[] value) {
            return value != null && value.length == 32;
        }

        private static void require(boolean condition, String message) {
            if (!condition) {
                throw new IllegalArgumentException(message);
            }
        }
    }
}
