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
package ai.badmonkey.agentspaces.api.security;

import ai.badmonkey.agentspaces.common.id.AgentId;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

/**
 * The peer vouches that a key belongs to one of its agents (spec §4.2, QA4
 * A4-7). Canonical CBOR of the four unsigned fields is what the peer signs;
 * {@code peerSignature} is that signature, and the peer's own key travels beside
 * the certificate wherever it is carried, so a verifier checks that the key
 * hashes to {@code agent.peer()} before trusting the signature. Mirrors
 * {@code SignedAdvertisement}: a body, a signer, a signature, nothing clever.
 *
 * <p>The certificate has a lifetime, so a leaked agent key has one too; the
 * peer re-issues on its ordinary refresh cadence. A certificate is
 * <em>attribution</em>, not eligibility: it proves which agent on a peer signed,
 * and nothing about whether that agent may do anything (spec §8, §11).
 *
 * @param agent          the agent whose key this certifies
 * @param agentPublicKey the agent's raw Ed25519 public key
 * @param issued         when the peer issued it
 * @param ttl            how long it is good for
 * @param peerSignature  the peer's signature over the unsigned body, or null
 *                       for the body a peer is about to sign
 */
public record AgentCertificate(AgentId agent, byte[] agentPublicKey, Instant issued,
                               Duration ttl, byte[] peerSignature,
                               // v0.1.13 (SPEC §11a.2): the agent's own X25519 key, certified
                               // with its signing key so a key holder can seal content keys
                               // to exactly this agent; omitted when absent, so a certificate
                               // without one is byte-identical to the v0.1.12 certificate.
                               @com.fasterxml.jackson.annotation.JsonInclude(
                                       com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                               byte[] encryptionPublicKey) {

    /** The v0.1.12 shape: no encryption key. */
    public AgentCertificate(AgentId agent, byte[] agentPublicKey, Instant issued, Duration ttl,
                            byte[] peerSignature) {
        this(agent, agentPublicKey, issued, ttl, peerSignature, null);
    }

    public AgentCertificate {
        Objects.requireNonNull(agent, "agent");
        Objects.requireNonNull(agentPublicKey, "agentPublicKey");
        Objects.requireNonNull(issued, "issued");
        Objects.requireNonNull(ttl, "ttl");
        if (ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("certificate ttl must be positive: " + ttl);
        }
        agentPublicKey = agentPublicKey.clone();
        peerSignature = peerSignature == null ? null : peerSignature.clone();
        if (encryptionPublicKey != null && encryptionPublicKey.length != 32) {
            throw new IllegalArgumentException("an encryption key is a raw 32-byte X25519 key");
        }
        encryptionPublicKey = encryptionPublicKey == null ? null : encryptionPublicKey.clone();
    }

    /** The body the peer signs: this certificate without its signature. */
    public AgentCertificate unsigned() {
        return peerSignature == null ? this
                : new AgentCertificate(agent, agentPublicKey, issued, ttl, null, encryptionPublicKey);
    }

    /** Returns a copy carrying the peer's signature. */
    public AgentCertificate signed(byte[] signature) {
        return new AgentCertificate(agent, agentPublicKey, issued, ttl,
                Objects.requireNonNull(signature, "signature"), encryptionPublicKey);
    }

    /** The instant after which this certificate certifies nothing. */
    public Instant expiresAt() {
        return issued.plus(ttl);
    }

    /**
     * Whether the certificate has lapsed at the given instant.
     *
     * @param now the instant to judge at
     * @return true once {@code now} is at or past {@link #expiresAt()}
     */
    public boolean expired(Instant now) {
        return !now.isBefore(expiresAt());
    }

    /**
     * Whether this certificate's validity window contains an instant (SPEC §4.2,
     * v0.1.13): from {@code issued} inclusive to {@link #expiresAt()} exclusive.
     * A signature is judged by whether its certificate covered its signing time.
     *
     * @param signingTime the instant the certified key signed at
     * @return true when the window contains it
     */
    public boolean covers(Instant signingTime) {
        // Signing times are HLC stamps, whole milliseconds; an issue instant may
        // carry sub-millisecond precision (Instant.now()), so the window opens at
        // the issue instant's millisecond, or a record signed in the very
        // millisecond its certificate was issued would read as signed before it.
        return !signingTime.isBefore(issued.truncatedTo(java.time.temporal.ChronoUnit.MILLIS))
                && signingTime.isBefore(expiresAt());
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof AgentCertificate that
                && agent.equals(that.agent)
                && Arrays.equals(agentPublicKey, that.agentPublicKey)
                && issued.equals(that.issued)
                && ttl.equals(that.ttl)
                && Arrays.equals(peerSignature, that.peerSignature)
                && Arrays.equals(encryptionPublicKey, that.encryptionPublicKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(agent, Arrays.hashCode(agentPublicKey), issued, ttl,
                Arrays.hashCode(peerSignature), Arrays.hashCode(encryptionPublicKey));
    }

    @Override
    public String toString() {
        return "AgentCertificate[" + agent.encoded() + ", issued=" + issued + ", ttl=" + ttl
                + (peerSignature == null ? ", unsigned]" : "]");
    }
}
