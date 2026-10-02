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

import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.PeerId;

/**
 * The authorization SPI (security remediation plan §6): answers "may peer P
 * perform operation O in scope S?" for the fleet's privileged operations. The
 * signature machinery authenticates <em>who</em> sent a frame or wrote an
 * entry; an {@code Authorizer} decides whether that authenticated identity is
 * <em>allowed</em> to do what it is attempting, which is the gap the security
 * review's authorization findings named.
 *
 * <p>Two shipped implementations cover the security profiles:
 * {@code MembershipAuthorizer} (in the peering module) roots authority in the
 * group's admitted membership with optional explicit per-operation grants —
 * the {@code mtls} posture, needing no external service — and
 * {@code OidcAuthorizer} (in the {@code agentspaces-auth-oidc} module) defers
 * the policy to JWT scopes minted by the organization's identity provider —
 * the {@code mtls-oidc} posture. Enforcement points consult the authorizer;
 * they never interpret tokens or membership themselves.
 */
@FunctionalInterface
public interface Authorizer {

    /** The privileged operations an authorizer arbitrates. */
    enum Operation {
        /** Participate in the ordered-log Raft quorum: vote, lead, replicate. */
        RAFT_VOTER,
        /** Issue command-and-control directives workers obey. */
        DIRECTIVE_ISSUER,
        /** Serve data-query results a fleet consumes as source-of-truth. */
        CONNECTOR_SERVE,
        /** Hold and distribute a group content key. */
        KEY_HOLDER,
        /** Write entries into a space (SPEC §7.5 {@code AUTHORIZER} admission); the scope is the space name. */
        SPACE_WRITE,
        /** Take and complete entries in a space (SPEC §7.5 {@code AUTHORIZER} admission); the scope is the space name. */
        SPACE_TAKE,
        /**
         * Cast a ballot that counts in a QUORUM decision (SPEC §8); the scope
         * is the vote space's name. A tally counts a ballot only from a peer
         * this permits, so an identity provider can name the electorate the
         * same way it names Raft voters and directive issuers.
         */
        VOTE,
        /**
         * Serve model calls a fleet accepts as authoritative: a model server's
         * responses and stream chunks count only from a peer this permits. The
         * scope is the model name, or an empty string for every model.
         */
        MODEL_SERVE,
        /**
         * Rotate the group content key: mint a new epoch and announce its
         * cutover (SPEC §11a.3, v0.1.13); the scope is the group id. Unlike the
         * other operations, an authorizer with no explicit grant permits nobody;
         * the group's founder may always rotate.
         */
        KEY_ROTATE
    }

    /**
     * Decides one authorization question. Implementations must be safe for
     * concurrent use and fast enough to sit on frame-dispatch paths; expensive
     * lookups (token validation, JWKS refresh) belong in caches behind this
     * call, never inline.
     *
     * @param peer      the authenticated peer asking to act
     * @param operation the privileged operation
     * @param scope     the operation's scope: a group id, a space name, or an
     *                  empty string for fleet-wide operations
     * @return whether the peer may perform the operation in that scope
     */
    boolean permits(PeerId peer, Operation operation, String scope);

    /**
     * The finest identity an authorizer can speak about for an operation and
     * scope (QA4 A4-7 phase 2). {@code PEER} is today's answer and the default:
     * grants and tokens name PeerIds, so every agent on a peer shares its
     * peer's privileges. {@code AGENT} means this authorizer holds agent-level
     * grants for the operation in that scope, so an answer to
     * {@link #permits(AgentId, Operation, String)} can differ between two agents
     * of one peer. A consumer that counts identities (a QUORUM tally) counts at
     * this granularity, which is what keeps counting and authorization agreeing.
     */
    enum Granularity {
        /** Privileges are per peer; agents inherit their peer's answer. */
        PEER,
        /** Privileges are per agent; an agent must be named to be permitted. */
        AGENT
    }

    /**
     * Whether an agent may perform an operation in a scope. The default
     * delegates to the agent's peer, which is exactly today's behaviour;
     * an implementation that holds agent-level grants overrides it and
     * reports {@link Granularity#AGENT} for that operation and scope.
     *
     * @param agent     the agent being judged
     * @param operation the privileged operation
     * @param scope     the operation's scope
     * @return whether the agent is permitted
     */
    default boolean permits(AgentId agent, Operation operation, String scope) {
        return permits(agent.peer(), operation, scope);
    }

    /**
     * The granularity at which this authorizer answers for an operation and
     * scope; {@link Granularity#PEER} unless agent-level grants exist.
     *
     * @param operation the privileged operation
     * @param scope     the operation's scope
     * @return the granularity
     */
    default Granularity granularity(Operation operation, String scope) {
        return Granularity.PEER;
    }
}
