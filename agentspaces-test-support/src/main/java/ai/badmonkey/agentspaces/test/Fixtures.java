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
package ai.badmonkey.agentspaces.test;

import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.PeerId;

/** Shared test fixtures: simple entry types and identities used across module tests. */
public final class Fixtures {

    private Fixtures() {
    }

    /** A simple task entry used across tests. */
    public record TaskEntry(String topic, int priority) {
    }

    /** A simple result entry used across tests. */
    public record FindingEntry(String topic, String summary) {
    }

    /**
     * A deterministic test agent identity.
     *
     * @param name the agent's local name
     * @return an AgentId on a fixed test peer
     */
    public static AgentId agent(String name) {
        return new AgentId(PeerId.of("zTestPeer1111"), name);
    }
}
