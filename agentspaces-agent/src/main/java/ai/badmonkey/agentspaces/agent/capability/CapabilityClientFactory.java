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
package ai.badmonkey.agentspaces.agent.capability;

import ai.badmonkey.agentspaces.agent.AgentSpaces;

/**
 * Produces a typed client for one capability service in one group (spec
 * §10.5): {@code spaces.group("g").capability(VoteClient.class)} looks up the
 * factory whose {@link #clientType()} is {@code VoteClient} and asks it to
 * {@link #create} a client over that group's spaces, discovery, and locally
 * registered providers. Factories are found through {@link java.util.ServiceLoader}
 * ({@code META-INF/services/ai.badmonkey.agentspaces.agent.capability.CapabilityClientFactory})
 * and through {@link AgentSpaces#clientFactory explicit registration}; third
 * parties ship a client for a capability they define by implementing this.
 *
 * @param <C> the client type
 */
public interface CapabilityClientFactory<C> {

    /** Returns the client type this factory produces. */
    Class<C> clientType();

    /**
     * Creates the client for one group. Called once per group and client type;
     * the facade caches the result.
     *
     * @param group the group context
     * @return the client
     * @throws IllegalStateException when the capability cannot be resolved in
     *                               this group (no local provider, no
     *                               advertisement, or a missing space)
     */
    C create(AgentSpaces.GroupContext group);
}
