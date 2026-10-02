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
import ai.badmonkey.agentspaces.common.id.AgentId;

import java.util.Optional;

/** An agent that signs with its peer's key: the pre-subordinate-key behaviour, named. */
final class PeerSignedAgentIdentity implements AgentIdentity {

    private final PeerIdentity peer;
    private final AgentId id;

    PeerSignedAgentIdentity(PeerIdentity peer, AgentId id) {
        this.peer = peer;
        this.id = id;
    }

    @Override
    public AgentId id() {
        return id;
    }

    @Override
    public byte[] publicKey() {
        return peer.rawPublicKey();
    }

    @Override
    public byte[] sign(byte[] bytes) {
        return peer.sign(bytes);
    }

    @Override
    public Optional<AgentCertificate> certificate() {
        return Optional.empty();
    }

    @Override
    public String toString() {
        return id.encoded() + " (peer-signed)";
    }
}
