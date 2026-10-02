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

import ai.badmonkey.agentspaces.api.security.AgentCertificate;
import ai.badmonkey.agentspaces.common.id.AgentId;

import java.util.Optional;

/**
 * Who signs on behalf of one agent (spec §4.2, QA4 A4-7). An agent is
 * {@code peer/localName}; what makes that name mean something is the key its
 * signatures verify under. Two shipped shapes exist and callers should not
 * care which they hold: a <em>peer-signed</em> identity signs with the peer's
 * own key and carries no certificate, which is how every agent worked before
 * subordinate keys; a <em>subordinate</em> identity holds a key of its own and
 * carries the peer's {@link AgentCertificate} binding that key to the name.
 *
 * <p>An interface rather than a class so an HSM-backed or test identity is
 * possible, and so the private key stays where the implementation put it:
 * callers ask it to sign and never see the material. Obtain one from
 * {@code PeerIdentity.agentIdentity(name)} or {@code PeerIdentity.subordinate(name)}.
 */
public interface AgentIdentity {

    /** The agent this identity signs as. */
    AgentId id();

    /**
     * The raw Ed25519 public key a verifier checks this agent's signatures
     * against: the peer key for a peer-signed identity, the agent's own key for
     * a subordinate one.
     */
    byte[] publicKey();

    /**
     * Signs bytes as this agent.
     *
     * @param bytes the canonical bytes to sign
     * @return the Ed25519 signature
     */
    byte[] sign(byte[] bytes);

    /**
     * The peer's certification of this agent's key, or empty when the agent
     * signs with the peer key itself and there is nothing separate to certify.
     */
    Optional<AgentCertificate> certificate();

    /** Whether this identity signs with its own certified key rather than the peer's. */
    default boolean isSubordinate() {
        return certificate().isPresent();
    }

    /**
     * The certificate whose validity window contains a signing time (SPEC §4.2,
     * v0.1.13): what a signer attaches to a signature made at {@code signingTime},
     * since receivers judge the certificate at the signature's own time. Empty
     * when no held certificate covers it (and always for a peer-signed identity).
     * A renewing identity keeps the certificates it issued, so it can still
     * certify a renewal of a claim stamped under an earlier one.
     *
     * @param signingTime the instant the signature is (or was) made at
     * @return the covering certificate
     */
    default Optional<AgentCertificate> certificateCovering(java.time.Instant signingTime) {
        return certificate().filter(certificate -> certificate.covers(signingTime));
    }

    /**
     * Re-issues this identity's certificate if it is past half its lifetime
     * (SPEC §4.2, v0.1.13); a no-op for identities that do not renew.
     *
     * @param now the current instant
     */
    default void renewIfDue(java.time.Instant now) {
    }

    /**
     * The agent's own X25519 key, when its certificate certifies one (SPEC
     * §11a.2, v0.1.13): what a key holder seals content keys to for this agent.
     *
     * @return the raw X25519 public key
     */
    default Optional<byte[]> encryptionPublicKey() {
        return certificate().map(AgentCertificate::encryptionPublicKey);
    }

    /**
     * X25519 key agreement with the agent's own encryption key, so a sealed
     * content key opens without the private key leaving the identity.
     *
     * @param remotePublicKey the raw X25519 key of the other side
     * @return the shared secret
     * @throws UnsupportedOperationException when the agent has no encryption key
     */
    default byte[] agreeEncryption(byte[] remotePublicKey) {
        throw new UnsupportedOperationException(id().encoded() + " has no encryption key");
    }
}
