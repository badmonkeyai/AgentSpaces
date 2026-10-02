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
package ai.badmonkey.agentspaces.spring;

import ai.badmonkey.agentspaces.agent.AgentSpaces;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.console.DirectiveGate;

import java.util.Objects;

/**
 * Attaches {@link DirectiveGate}s under the profile's {@link Authorizers}
 * (TODO-EFG §4): a worker calls {@link #attach(Space, String)} on the control
 * space and obeys whichever peers the group's authorizer permits
 * {@code DIRECTIVE_ISSUER} in the scope of the group id — under the membership
 * profiles the console peer (granted automatically where command-and-control
 * runs, or listed in {@code agentspaces.security.grants.directive-issuer} on
 * worker nodes), under the OIDC profiles the identity provider's
 * {@code aspace:directive-issuer[:<group>]} scope — without naming a PeerID.
 */
public final class DirectiveGates {

    private final Authorizers authorizers;
    private final AgentSpaces spaces;

    /**
     * Creates the factory.
     *
     * @param authorizers the profile's authorizer selection
     * @param spaces      the fabric facade, to find a space's group
     */
    public DirectiveGates(Authorizers authorizers, AgentSpaces spaces) {
        this.authorizers = Objects.requireNonNull(authorizers, "authorizers");
        this.spaces = Objects.requireNonNull(spaces, "spaces");
    }

    /**
     * Attaches a gate for a worker on a control space registered with one of
     * the configured groups; that group's authorizer decides, scoped by its
     * group id.
     *
     * @param controlSpace the control space (a registered space of some group)
     * @param worker       this worker's name
     * @return the gate
     * @throws IllegalArgumentException when no configured group registers the space
     */
    public DirectiveGate attach(Space controlSpace, String worker) {
        Objects.requireNonNull(controlSpace, "controlSpace");
        for (String groupName : spaces.groupNames()) {
            AgentSpaces.GroupContext group = spaces.group(groupName);
            for (String spaceName : group.spaceNames()) {
                if (group.space(spaceName) == controlSpace) {
                    return attach(groupName, controlSpace, worker);
                }
            }
        }
        throw new IllegalArgumentException("the control space is registered with no"
                + " configured group; use attach(groupName, space, worker)");
    }

    /**
     * Attaches a gate for a worker under a named group's authorizer, scoped by
     * that group's id.
     *
     * @param groupName    the configured group name
     * @param controlSpace the control space
     * @param worker       this worker's name
     * @return the gate
     */
    public DirectiveGate attach(String groupName, Space controlSpace, String worker) {
        Objects.requireNonNull(groupName, "groupName");
        return DirectiveGate.attach(controlSpace, worker, authorizers.forGroup(groupName),
                spaces.group(groupName).id().value(), spaces.group(groupName).clock());
    }
}
