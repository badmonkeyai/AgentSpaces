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
package ai.badmonkey.agentspaces.identity;

import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.security.KeyPair;
import java.security.PublicKey;
import java.util.Objects;

/**
 * A peer's cryptographic identity (spec §4.1): its Ed25519 keypair and the PeerID
 * derived from the raw public key. The private key never leaves this object;
 * signing goes through {@link #sign(byte[])}.
 */
public final class PeerIdentity {

    private final KeyPair keys;
    private final PeerId peerId;

    private PeerIdentity(KeyPair keys) {
        this.keys = keys;
        this.peerId = PeerId.fromPublicKey(Ed25519.rawPublicKey(keys.getPublic()));
    }

    /** Generates a fresh identity with a new keypair. */
    public static PeerIdentity generate() {
        return new PeerIdentity(Ed25519.generate());
    }

    /**
     * Wraps an existing keypair as an identity.
     *
     * @param keys the Ed25519 keypair
     * @return the identity
     */
    public static PeerIdentity of(KeyPair keys) {
        return new PeerIdentity(Objects.requireNonNull(keys, "keys"));
    }

    /** Returns the PeerID derived from the public key. */
    public PeerId peerId() {
        return peerId;
    }

    /** Returns the public key. */
    public PublicKey publicKey() {
        return keys.getPublic();
    }

    /** Returns the raw 32-byte public key, the form PeerIDs are derived from. */
    public byte[] rawPublicKey() {
        return Ed25519.rawPublicKey(keys.getPublic());
    }

    /**
     * Names a logical agent hosted by this peer.
     *
     * @param localName the agent's local name
     * @return the AgentID
     */
    public AgentId agent(String localName) {
        return new AgentId(peerId, localName);
    }

    /**
     * An agent identity that signs with this peer's own key and carries no
     * certificate: today's behaviour, made explicit (spec §4.2, QA4 A4-7). Every
     * existing space and binder that took a peer identity plus a name is
     * defined in terms of this.
     *
     * @param localName the agent's local name
     * @return the peer-signed agent identity
     */
    public ai.badmonkey.agentspaces.api.spi.AgentIdentity agentIdentity(String localName) {
        return new PeerSignedAgentIdentity(this, agent(localName));
    }

    /**
     * A subordinate agent identity: a fresh Ed25519 key of its own, certified by
     * this peer for one hour from now (spec §4.2, QA4 A4-7). The certificate is
     * attribution, not eligibility.
     *
     * @param localName the agent's local name
     * @return the subordinate identity
     */
    public ai.badmonkey.agentspaces.api.spi.AgentIdentity subordinate(String localName) {
        return subordinate(localName, java.time.Instant.now(), java.time.Duration.ofHours(1));
    }

    /**
     * A subordinate agent identity with a fresh key and an explicit certificate
     * lifetime.
     *
     * @param localName the agent's local name
     * @param issued    the certificate's issue instant
     * @param ttl       the certificate's lifetime
     * @return the subordinate identity
     */
    public ai.badmonkey.agentspaces.api.spi.AgentIdentity subordinate(String localName,
            java.time.Instant issued, java.time.Duration ttl) {
        return subordinate(localName, Ed25519.generate(), issued, ttl);
    }

    /**
     * A subordinate agent identity over a key pair the caller holds, for
     * persistence or hardware-backed keys.
     *
     * @param localName the agent's local name
     * @param agentKeys the agent's Ed25519 key pair
     * @param issued    the certificate's issue instant
     * @param ttl       the certificate's lifetime
     * @return the subordinate identity
     */
    public ai.badmonkey.agentspaces.api.spi.AgentIdentity subordinate(String localName,
            KeyPair agentKeys, java.time.Instant issued, java.time.Duration ttl) {
        Objects.requireNonNull(agentKeys, "agentKeys");
        AgentId id = agent(localName);
        ai.badmonkey.agentspaces.api.security.AgentCertificate body =
                new ai.badmonkey.agentspaces.api.security.AgentCertificate(id,
                        Ed25519.rawPublicKey(agentKeys.getPublic()), issued, ttl, null);
        return new SubordinateAgentIdentity(id, agentKeys, new AgentCertificates().sign(body, this));
    }

    /**
     * A subordinate agent identity whose certificate renews itself (SPEC §4.2,
     * v0.1.13): the key is fixed, and the peer re-issues the certificate on the
     * given clock once half of {@code ttl} has passed, keeping earlier ones so
     * signatures made under them stay certifiable. Receivers judge each
     * certificate at the signing time of what it certifies, so an agent can
     * run indefinitely and its history stays verifiable.
     *
     * @param localName the agent's local name
     * @param agentKeys the agent's Ed25519 key pair (persist it to keep the agent's key)
     * @param ttl       each certificate's lifetime
     * @param clock     the clock certificates are issued and renewed on
     * @return the renewing identity
     */
    public ai.badmonkey.agentspaces.api.spi.AgentIdentity renewingSubordinate(String localName,
            KeyPair agentKeys, java.time.Duration ttl, java.time.InstantSource clock) {
        return new RenewingAgentIdentity(agent(localName), agentKeys, this, ttl, clock);
    }

    /**
     * A renewing subordinate identity whose certificates also certify an X25519
     * key, so key-wrap holders can seal group content keys to the agent itself
     * (SPEC §11a.2 per-agent wrap, v0.1.13).
     *
     * @param localName      the agent's local name
     * @param agentKeys      the agent's Ed25519 signing pair
     * @param encryptionKeys the agent's X25519 pair
     * @param ttl            each certificate's lifetime
     * @param clock          the clock certificates are issued and renewed on
     * @return the renewing identity
     */
    public ai.badmonkey.agentspaces.api.spi.AgentIdentity renewingSubordinate(String localName,
            KeyPair agentKeys, KeyPair encryptionKeys, java.time.Duration ttl,
            java.time.InstantSource clock) {
        return new RenewingAgentIdentity(agent(localName), agentKeys,
                Objects.requireNonNull(encryptionKeys, "encryptionKeys"), this, ttl, clock);
    }

    /**
     * A renewing subordinate identity over a fresh key.
     *
     * @param localName the agent's local name
     * @param ttl       each certificate's lifetime
     * @param clock     the clock certificates are issued and renewed on
     * @return the renewing identity
     */
    public ai.badmonkey.agentspaces.api.spi.AgentIdentity renewingSubordinate(String localName,
            java.time.Duration ttl, java.time.InstantSource clock) {
        return renewingSubordinate(localName, Ed25519.generate(), ttl, clock);
    }

    /**
     * Signs bytes with this peer's private key.
     *
     * @param bytes the bytes to sign
     * @return the signature
     */
    public byte[] sign(byte[] bytes) {
        return Ed25519.sign(keys.getPrivate(), bytes);
    }

    /**
     * Verifies a signature allegedly made by this peer.
     *
     * @param bytes the signed bytes
     * @param sig   the signature
     * @return {@code true} when valid
     */
    public boolean verify(byte[] bytes, byte[] sig) {
        return Ed25519.verify(keys.getPublic(), bytes, sig);
    }

    /** Returns the keypair, for persistence by {@link FileKeystore}. */
    KeyPair keys() {
        return keys;
    }

    @Override
    public String toString() {
        return peerId.display();
    }
}
