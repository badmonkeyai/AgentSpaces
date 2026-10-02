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
package ai.badmonkey.agentspaces.agent;

import ai.badmonkey.agentspaces.api.spi.AgentIdentity;

import java.util.function.Function;

/**
 * Supplies the identity each bound agent signs as (SPEC §4.2, v0.1.13): the
 * peer-signed form, a renewing subordinate key, a key loaded from the agent
 * keystore, or a hardware-backed {@link AgentIdentity} of the application's
 * own. Under Spring Boot the starter declares one from
 * {@code agentspaces.identity.*}; declare your own bean to replace it. The
 * identity returned for a name must be that agent of this peer.
 */
@FunctionalInterface
public interface AgentIdentityFactory extends Function<String, AgentIdentity> {

    /**
     * The identity the agent with this local name signs as.
     *
     * @param localName the agent's local name
     * @return its identity
     */
    AgentIdentity identityFor(String localName);

    @Override
    default AgentIdentity apply(String localName) {
        return identityFor(localName);
    }
}
