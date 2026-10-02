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
package ai.badmonkey.agentspaces.peering.membership;

import ai.badmonkey.agentspaces.api.security.RevocationView;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.util.Objects;

/**
 * An authorizer scoped to one group (SPEC §11, v0.1.13, review M-4): it
 * refuses whatever the group has revoked, peers and agents alike, and can
 * require membership of the group, before asking the authorizer it wraps. An
 * identity provider's authorizer is shared by every group a node joins; this
 * wrapper is what keeps a peer revoked or absent in one group from being
 * granted there on the strength of a token that is valid everywhere.
 */
public final class GroupScopedAuthorizer implements Authorizer {

    private final Authorizer inner;
    private final RevocationView revocations;
    private final GroupMembership membership;
    private final PeerId self;

    /**
     * @param inner       the authorizer to consult after the group's own checks
     * @param revocations the group's revocations
     * @param membership  the group's membership, when membership is required; else null
     * @param self        this node, which is always a member of its own groups
     */
    public GroupScopedAuthorizer(Authorizer inner, RevocationView revocations,
                                 GroupMembership membership, PeerId self) {
        this.inner = Objects.requireNonNull(inner, "inner");
        this.revocations = Objects.requireNonNull(revocations, "revocations");
        this.membership = membership;
        this.self = Objects.requireNonNull(self, "self");
    }

    /** The wrapped authorizer. */
    public Authorizer inner() {
        return inner;
    }

    @Override
    public boolean permits(PeerId peer, Operation operation, String scope) {
        return !revocations.revoked(peer) && member(peer) && inner.permits(peer, operation, scope);
    }

    @Override
    public boolean permits(AgentId agent, Operation operation, String scope) {
        return !revocations.revoked(agent, null) && member(agent.peer())
                && inner.permits(agent, operation, scope);
    }

    @Override
    public Granularity granularity(Operation operation, String scope) {
        return inner.granularity(operation, scope);
    }

    private boolean member(PeerId peer) {
        return membership == null || peer.equals(self) || membership.member(peer).isPresent();
    }
}
