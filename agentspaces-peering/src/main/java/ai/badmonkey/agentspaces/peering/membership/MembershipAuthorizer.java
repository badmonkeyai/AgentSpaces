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

import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.util.Collection;
import java.util.EnumMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The default {@link Authorizer} (security remediation plan §6): authority is
 * rooted in the group's admitted membership, with optional explicit grants
 * narrowing individual operations to named peers. This is the {@code mtls}
 * profile's posture — the group's admission perimeter (founder-rooted INVITE
 * credentials or the POLICY validator) already decides who belongs, and
 * privileged operations follow that decision with no external dependency.
 *
 * <p>Ungranted operations default to <em>any admitted member</em>; an operation
 * with an explicit grant is permitted to exactly the granted peers (who must
 * still be admitted members — a revoked or evicted peer loses its grants with
 * its membership). A fleet that wants only its console issuing directives
 * grants {@code DIRECTIVE_ISSUER} to the console peer and leaves the rest
 * defaulted.
 */
public final class MembershipAuthorizer implements Authorizer {

    private final GroupMembership membership;
    private final PeerId self;
    private final Map<Operation, Set<PeerId>> grants;
    private final Map<Operation, Set<AgentId>> agentGrants;

    /**
     * Creates an authorizer over a group's membership with no explicit grants:
     * every admitted member may perform every operation.
     *
     * @param membership the group's membership view
     * @param self       this node's own peer id (always permitted; a node's
     *                   view never contains itself)
     */
    public MembershipAuthorizer(GroupMembership membership, PeerId self) {
        this(membership, self, Map.of());
    }

    /**
     * Creates an authorizer with explicit per-operation grants.
     *
     * @param membership the group's membership view
     * @param self       this node's own peer id
     * @param grants     operations narrowed to named peers; absent operations
     *                   default to any admitted member
     */
    public MembershipAuthorizer(GroupMembership membership, PeerId self,
                                Map<Operation, Set<PeerId>> grants) {
        this(membership, self, grants, Map.of());
    }

    /**
     * Creates an authorizer with per-peer and per-agent grants (QA4 A4-7 phase 2).
     * A bare PeerId grant admits every agent on that peer at {@code PEER}
     * granularity; an AgentId grant admits that agent alone and switches the
     * operation to {@code AGENT} granularity, under which an agent must be
     * named to be permitted (its peer, asked at peer level, is permitted
     * because it hosts a granted agent).
     *
     * @param membership  the group's membership view
     * @param self        the local peer, always admitted
     * @param grants      per-operation PeerId grants
     * @param agentGrants per-operation AgentId grants
     */
    public MembershipAuthorizer(GroupMembership membership, PeerId self,
                                Map<Operation, Set<PeerId>> grants,
                                Map<Operation, Set<AgentId>> agentGrants) {
        this.membership = Objects.requireNonNull(membership, "membership");
        this.self = Objects.requireNonNull(self, "self");
        Map<Operation, Set<PeerId>> copied = new EnumMap<>(Operation.class);
        grants.forEach((operation, peers) -> copied.put(operation, Set.copyOf(peers)));
        this.grants = copied;
        Map<Operation, Set<AgentId>> copiedAgents = new EnumMap<>(Operation.class);
        agentGrants.forEach((operation, agents) -> copiedAgents.put(operation, Set.copyOf(agents)));
        this.agentGrants = copiedAgents;
        // An operation with only agent grants is still narrowed: no bare peer is granted.
        copiedAgents.keySet().forEach(operation -> copied.putIfAbsent(operation, Set.of()));
    }

    /**
     * Parses grant lists that mix bare PeerIds and {@code peer/localName}
     * AgentIds, as the starter's {@code agentspaces.security.grants.*} do, into
     * an authorizer.
     *
     * @param membership the group's membership view
     * @param self       the local peer
     * @param encoded    per-operation lists of PeerId or AgentId strings
     * @return the authorizer
     * @throws IllegalArgumentException on an entry that is neither
     */
    public static MembershipAuthorizer parsing(GroupMembership membership, PeerId self,
                                               Map<Operation, ? extends Collection<String>> encoded) {
        Map<Operation, Set<PeerId>> peers = new EnumMap<>(Operation.class);
        Map<Operation, Set<AgentId>> agents = new EnumMap<>(Operation.class);
        encoded.forEach((operation, values) -> {
            Set<PeerId> peerSet = new LinkedHashSet<>();
            Set<AgentId> agentSet = new LinkedHashSet<>();
            for (String value : values) {
                String trimmed = Objects.requireNonNull(value, "grant").trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                if (trimmed.contains("/")) {
                    agentSet.add(AgentId.parse(trimmed));
                } else {
                    peerSet.add(PeerId.of(trimmed));
                }
            }
            if (!peerSet.isEmpty() || !agentSet.isEmpty()) {
                peers.put(operation, peerSet);       // an operation with only agent grants
                if (!agentSet.isEmpty()) {           // still narrows: no bare peer is granted
                    agents.put(operation, agentSet);
                }
            }
        });
        return new MembershipAuthorizer(membership, self, peers, agents);
    }

    @Override
    public boolean permits(PeerId peer, Operation operation, String scope) {
        Objects.requireNonNull(peer, "peer");
        Objects.requireNonNull(operation, "operation");
        if (!peer.equals(self) && membership.member(peer).isEmpty()) {
            return false; // not admitted (or no longer): no privileges at all
        }
        Set<PeerId> granted = grants.get(operation);
        if (granted == null) {
            // KEY_ROTATE needs an explicit grant (the founder is permitted by the
            // key distributor itself); every other operation defaults to members.
            return operation != Operation.KEY_ROTATE;
        }
        if (granted.contains(peer)) {
            return true;
        }
        // A peer hosting a granted agent is permitted at peer level: it must be,
        // or castBallot's own fail-fast would stop the granted agent from voting.
        Set<AgentId> agents = agentGrants.get(operation);
        return agents != null && agents.stream().anyMatch(agent -> agent.peer().equals(peer));
    }

    @Override
    public boolean permits(AgentId agent, Operation operation, String scope) {
        Objects.requireNonNull(agent, "agent");
        Objects.requireNonNull(operation, "operation");
        PeerId peer = agent.peer();
        if (!peer.equals(self) && membership.member(peer).isEmpty()) {
            return false;
        }
        Set<PeerId> granted = grants.get(operation);
        if (granted == null) {
            return operation != Operation.KEY_ROTATE; // as at peer level: rotation needs a grant
        }
        if (granted.contains(peer)) {
            return true; // the whole peer is granted: every agent on it
        }
        Set<AgentId> agents = agentGrants.get(operation);
        return agents != null && agents.contains(agent);
    }

    @Override
    public Granularity granularity(Operation operation, String scope) {
        Set<AgentId> agents = agentGrants.get(operation);
        return agents == null || agents.isEmpty() ? Granularity.PEER : Granularity.AGENT;
    }
}
