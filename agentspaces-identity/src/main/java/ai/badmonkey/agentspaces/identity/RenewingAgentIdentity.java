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

import ai.badmonkey.agentspaces.api.security.AgentCertificate;
import ai.badmonkey.agentspaces.api.spi.AgentIdentity;
import ai.badmonkey.agentspaces.common.crypto.Ed25519;
import ai.badmonkey.agentspaces.common.id.AgentId;

import java.security.KeyPair;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * A subordinate agent whose key is fixed and whose certificate the peer
 * re-issues at half its lifetime (SPEC §4.2, v0.1.13, TODO-9-10-11 B3).
 * Renewal is lazy: every {@link #sign}, {@link #certificate}, and
 * {@link #certificateCovering} first renews if due, so no scheduler is needed,
 * and the binder's card refresh is merely an extra driver. Every certificate
 * issued is kept (up to {@link #RETAINED}) because a signer attaches the
 * certificate that covered its signing time, and a take claim keeps its first
 * stamp through every renewal.
 */
final class RenewingAgentIdentity implements AgentIdentity {

    /** How many issued certificates are kept: 128 days at a 24-hour lifetime. */
    static final int RETAINED = 256;

    private final AgentId id;
    private final KeyPair keys;
    /** The agent's X25519 pair, when it has one (per-agent key wrap, SPEC §11a.2). */
    private final KeyPair encryption;
    private final PeerIdentity peer;
    private final Duration ttl;
    private final InstantSource clock;
    private final AgentCertificates certificates = new AgentCertificates();
    private final NavigableMap<Instant, AgentCertificate> issued = new TreeMap<>();
    /**
     * When each held certificate (by issue instant) was last asked for. Past the
     * cap the least recently used one goes, never the newest (review M-1): a
     * take held for months keeps its first stamp through every renewal, and each
     * renewal asks for the certificate covering it, so that one stays.
     */
    private final Map<Instant, Instant> lastUsed = new java.util.HashMap<>();

    RenewingAgentIdentity(AgentId id, KeyPair keys, PeerIdentity peer, Duration ttl,
                          InstantSource clock) {
        this(id, keys, null, peer, ttl, clock);
    }

    RenewingAgentIdentity(AgentId id, KeyPair keys, KeyPair encryption, PeerIdentity peer,
                          Duration ttl, InstantSource clock) {
        this.id = Objects.requireNonNull(id, "id");
        this.keys = Objects.requireNonNull(keys, "keys");
        this.encryption = encryption;
        this.peer = Objects.requireNonNull(peer, "peer");
        this.ttl = Objects.requireNonNull(ttl, "ttl");
        if (ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("certificate lifetime must be positive: " + ttl);
        }
        this.clock = Objects.requireNonNull(clock, "clock");
        issue(clock.instant());
    }

    private synchronized void issue(Instant at) {
        AgentCertificate body = new AgentCertificate(id, Ed25519.rawPublicKey(keys.getPublic()),
                at, ttl, null, encryption == null ? null
                        : ai.badmonkey.agentspaces.common.crypto.X25519.rawPublicKey(encryption.getPublic()));
        issued.put(at, certificates.sign(body, peer));
        lastUsed.put(at, at);
        while (issued.size() > RETAINED) {
            Instant newest = issued.lastKey();
            Instant evict = null;
            for (Instant key : issued.keySet()) {
                if (!key.equals(newest) && (evict == null
                        || lastUsed.getOrDefault(key, key).isBefore(lastUsed.getOrDefault(evict, evict)))) {
                    evict = key;
                }
            }
            issued.remove(evict);
            lastUsed.remove(evict);
        }
    }

    @Override
    public synchronized void renewIfDue(Instant now) {
        AgentCertificate newest = issued.lastEntry().getValue();
        if (!now.isBefore(newest.issued().plus(ttl.dividedBy(2)))) {
            issue(now);
        }
    }

    @Override
    public AgentId id() {
        return id;
    }

    @Override
    public byte[] publicKey() {
        return Ed25519.rawPublicKey(keys.getPublic());
    }

    @Override
    public byte[] sign(byte[] bytes) {
        renewIfDue(clock.instant());
        return Ed25519.sign(keys.getPrivate(), bytes);
    }

    @Override
    public synchronized Optional<AgentCertificate> certificate() {
        renewIfDue(clock.instant());
        return Optional.of(issued.lastEntry().getValue());
    }

    @Override
    public synchronized Optional<AgentCertificate> certificateCovering(Instant signingTime) {
        renewIfDue(clock.instant());
        // Newest first: the latest certificate that covers the time is the one to attach.
        for (Map.Entry<Instant, AgentCertificate> entry : issued.descendingMap().entrySet()) {
            if (entry.getValue().covers(signingTime)) {
                lastUsed.put(entry.getKey(), clock.instant());
                return Optional.of(entry.getValue());
            }
        }
        return Optional.empty();
    }

    @Override
    public byte[] agreeEncryption(byte[] remotePublicKey) {
        if (encryption == null) {
            throw new UnsupportedOperationException(id.encoded() + " has no encryption key");
        }
        return ai.badmonkey.agentspaces.common.crypto.X25519.agree(encryption.getPrivate(), remotePublicKey);
    }

    /** How many certificates are held, for tests. */
    synchronized int heldCertificates() {
        return issued.size();
    }

    @Override
    public String toString() {
        return id.encoded() + " (subordinate, renewing every " + ttl.dividedBy(2) + ")";
    }
}
