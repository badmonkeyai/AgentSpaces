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
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.time.Instant;

/**
 * One group's accepted revocations as every enforcement point asks about them
 * (SPEC §6.1, v0.1.13): peers by identity, and agents and agent keys under the
 * freeze rule of {@code CredentialRevocation}. Implemented in peering over the
 * group's registries; the interface lives here so the space, discovery, and
 * capability layers can consult it without depending on peering.
 */
public interface RevocationView {

    /** A view that revokes nothing. */
    RevocationView NONE = new RevocationView() {
        @Override
        public boolean revoked(PeerId peer) {
            return false;
        }

        @Override
        public boolean refuses(AgentId agent, byte[] agentKey, Instant signingTime) {
            return false;
        }
    };

    /**
     * Whether a peer's identity is revoked.
     *
     * @param peer the peer
     * @return whether it is revoked
     */
    boolean revoked(PeerId peer);

    /**
     * Whether a newly arriving signature by an agent is refused: its peer is
     * revoked, or the agent (or the key it signed with) is revoked and the
     * revocation's freeze rule refuses this signing time. What a replica
     * already holds is never re-judged.
     *
     * @param agent       the signing agent
     * @param agentKey    the agent's raw public key when it signed with a key of
     *                    its own, or null for a peer-signed agent (then only
     *                    agent-wide revocations apply)
     * @param signingTime when the signature was made, or null when that is not
     *                    trustworthy (a claim, whose stamp a renewal keeps), which
     *                    refuses under any revocation
     * @return whether the signature is refused
     */
    boolean refuses(AgentId agent, byte[] agentKey, Instant signingTime);

    /**
     * Whether an agent is revoked at all, at any time (its peer, the agent, or
     * the given key): the test for acting now, such as serving it a key or
     * counting its ballot.
     *
     * @param agent    the agent
     * @param agentKey its raw public key, or null
     * @return whether anything revokes it
     */
    default boolean revoked(AgentId agent, byte[] agentKey) {
        return refuses(agent, agentKey, null);
    }
}
