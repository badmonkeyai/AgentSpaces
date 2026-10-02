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
import java.util.Optional;

/** An agent with a key of its own and its peer's certificate for it. */
final class SubordinateAgentIdentity implements AgentIdentity {

    private final AgentId id;
    private final KeyPair keys;
    private final AgentCertificate certificate;

    SubordinateAgentIdentity(AgentId id, KeyPair keys, AgentCertificate certificate) {
        this.id = id;
        this.keys = keys;
        this.certificate = certificate;
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
        return Ed25519.sign(keys.getPrivate(), bytes);
    }

    @Override
    public Optional<AgentCertificate> certificate() {
        return Optional.of(certificate);
    }

    @Override
    public String toString() {
        return id.encoded() + " (subordinate, certificate until " + certificate.expiresAt() + ")";
    }
}
