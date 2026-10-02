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
package ai.badmonkey.agentspaces.space.replicated;

import ai.badmonkey.agentspaces.api.ad.SpaceAdvertisement;
import ai.badmonkey.agentspaces.api.spi.Authorizer;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.util.Objects;
import java.util.Set;

/**
 * The space admission rule (SPEC §7.5, TECH-SPEC §7.10): whether an agent may
 * write, take, or complete in a space right now. A {@link ReplicatedSpace}
 * asks its rule on every local mutation and on every inbound record or claim,
 * after the signature has been verified, so a refusal is always attributable
 * to an authenticated agent.
 *
 * <p>Four rules ship. {@link #group()} admits every group member (the
 * default). {@link #allowlist(Set)} admits the listed AgentIDs; the list is
 * local configuration, so a remote refusal is misbehaviour and strikes.
 * {@link #credentials(PeerId, CredentialIndex)} admits the credential issuer
 * always and any other agent while a live {@link SpaceCredential} naming it
 * and the scope is present in the local replica; a refusal never strikes,
 * because the credential may simply not have arrived yet, and anti-entropy
 * re-offers the dropped delta on the next round. {@link #authorizer(Authorizer,
 * String)} defers to the fleet's {@link Authorizer} with
 * {@link Authorizer.Operation#SPACE_WRITE} or {@code SPACE_TAKE} in the scope
 * of the space name; an authorizer's evidence (an OIDC token) may also be
 * late, so a refusal does not strike either.
 */
public interface SpaceAdmission {

    /** What an agent is asking to do. A completion requires {@link #TAKE}. */
    enum Scope { WRITE, TAKE }

    /**
     * Whether the agent may perform the scope now.
     *
     * @param agent     the authenticated agent
     * @param scope     the operation: {@code WRITE} for a record, {@code TAKE}
     *                  for a claim or a completion
     * @param nowMillis the space clock, so leased evidence can be judged live
     * @return whether the agent is admitted
     */
    boolean admits(AgentId agent, Scope scope, long nowMillis);

    /**
     * The rule this admission implements, carried in the space's
     * {@link SpaceAdvertisement} so members learn how the space admits before
     * attaching.
     *
     * @return the advertised rule
     */
    SpaceAdvertisement.Admission rule();

    /**
     * Whether a remote refusal is witnessed misbehaviour the replica should
     * report. {@code true} only when the rule rests on local configuration that
     * cannot be late (the allowlist); rules that rest on replicated or external
     * evidence drop the delta silently and let anti-entropy re-offer it.
     *
     * @return whether a refused remote record or claim strikes its author
     */
    default boolean strikesOnRefusal() {
        return false;
    }

    /** Every group member may write, take, and complete: today's {@code GROUP}. */
    static SpaceAdmission group() {
        return GroupAdmission.INSTANCE;
    }

    /**
     * Only the listed agents may write, take, and complete: today's
     * {@code ALLOWLIST}. A refusal strikes, since the list is local.
     *
     * @param agents the admitted agents
     * @return the rule
     */
    static SpaceAdmission allowlist(Set<AgentId> agents) {
        return new AllowlistAdmission(Set.copyOf(Objects.requireNonNull(agents, "agents")));
    }

    /**
     * Credential-based admission: the issuer is always admitted, and any other
     * agent is admitted for a scope while the index holds a live
     * {@link SpaceCredential} for it. The space maintains the index from its
     * own replicated state; pass a fresh {@link CredentialIndex} per space.
     *
     * @param issuer the peer whose signed credential entries the space honours
     * @param index  the index the space maintains and this rule reads
     * @return the rule
     */
    static SpaceAdmission credentials(PeerId issuer, CredentialIndex index) {
        return new CredentialAdmission(Objects.requireNonNull(issuer, "issuer"),
                Objects.requireNonNull(index, "index"));
    }

    /**
     * Authorizer-based admission: the agent's peer must be permitted
     * {@link Authorizer.Operation#SPACE_WRITE} or {@code SPACE_TAKE} in the
     * scope of the space name.
     *
     * @param authorizer the fleet's authorizer
     * @param spaceName  the space name, passed as the operation's scope
     * @return the rule
     */
    static SpaceAdmission authorizer(Authorizer authorizer, String spaceName) {
        return new AuthorizerAdmission(Objects.requireNonNull(authorizer, "authorizer"),
                Objects.requireNonNull(spaceName, "spaceName"));
    }

    /** {@code GROUP}: everyone. */
    final class GroupAdmission implements SpaceAdmission {
        static final GroupAdmission INSTANCE = new GroupAdmission();

        private GroupAdmission() {
        }

        @Override
        public boolean admits(AgentId agent, Scope scope, long nowMillis) {
            return true;
        }

        @Override
        public SpaceAdvertisement.Admission rule() {
            return SpaceAdvertisement.Admission.GROUP;
        }

        @Override
        public String toString() {
            return "GROUP";
        }
    }

    /** {@code ALLOWLIST}: the listed agents, and a refusal strikes. */
    final class AllowlistAdmission implements SpaceAdmission {
        private final Set<AgentId> agents;

        private AllowlistAdmission(Set<AgentId> agents) {
            this.agents = agents;
        }

        /** The admitted agents. */
        public Set<AgentId> agents() {
            return agents;
        }

        @Override
        public boolean admits(AgentId agent, Scope scope, long nowMillis) {
            return agents.contains(agent);
        }

        @Override
        public SpaceAdvertisement.Admission rule() {
            return SpaceAdvertisement.Admission.ALLOWLIST;
        }

        @Override
        public boolean strikesOnRefusal() {
            return true;
        }

        @Override
        public String toString() {
            return "ALLOWLIST" + agents;
        }
    }

    /** {@code CREDENTIAL}: the issuer, plus holders of a live credential. */
    final class CredentialAdmission implements SpaceAdmission {
        private final PeerId issuer;
        private final CredentialIndex index;

        private CredentialAdmission(PeerId issuer, CredentialIndex index) {
            this.issuer = issuer;
            this.index = index;
        }

        /** The peer whose credential entries the space honours. */
        public PeerId issuer() {
            return issuer;
        }

        /** The index the space maintains from its credential entries. */
        public CredentialIndex index() {
            return index;
        }

        @Override
        public boolean admits(AgentId agent, Scope scope, long nowMillis) {
            return issuer.equals(agent.peer()) || index.admits(agent, scope, nowMillis);
        }

        @Override
        public SpaceAdvertisement.Admission rule() {
            return SpaceAdvertisement.Admission.CREDENTIAL;
        }

        @Override
        public String toString() {
            return "CREDENTIAL[issuer=" + issuer.display() + "]";
        }
    }

    /** {@code AUTHORIZER}: one {@code permits} question per scope. */
    final class AuthorizerAdmission implements SpaceAdmission {
        private final Authorizer authorizer;
        private final String spaceName;

        private AuthorizerAdmission(Authorizer authorizer, String spaceName) {
            this.authorizer = authorizer;
            this.spaceName = spaceName;
        }

        @Override
        public boolean admits(AgentId agent, Scope scope, long nowMillis) {
            Authorizer.Operation operation = scope == Scope.TAKE
                    ? Authorizer.Operation.SPACE_TAKE : Authorizer.Operation.SPACE_WRITE;
            return authorizer.permits(agent.peer(), operation, spaceName);
        }

        @Override
        public SpaceAdvertisement.Admission rule() {
            return SpaceAdvertisement.Admission.AUTHORIZER;
        }

        @Override
        public String toString() {
            return "AUTHORIZER[" + spaceName + "]";
        }
    }
}
