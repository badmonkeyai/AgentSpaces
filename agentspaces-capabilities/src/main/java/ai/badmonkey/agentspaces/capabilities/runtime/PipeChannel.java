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
package ai.badmonkey.agentspaces.capabilities.runtime;

import ai.badmonkey.agentspaces.common.id.PeerId;

import java.util.function.BiConsumer;

/**
 * The direct-pipe surface a capability protocol needs (spec §8 pipes, TECH-SPEC
 * §8.1): register a handler for one capability type's frames and send frames
 * to a peer. {@link CapabilityPipes} is the group-runtime implementation;
 * relays, recorders, and deterministic test doubles that reorder or delay
 * frames implement it too.
 */
public interface PipeChannel {

    /**
     * Registers the handler for one capability's frames.
     *
     * @param capabilityType the capability type URI
     * @param handler        receives (sender, payload)
     */
    void onCapability(String capabilityType, BiConsumer<PeerId, byte[]> handler);

    /**
     * Sends one capability frame to a peer.
     *
     * @param to             the destination peer
     * @param capabilityType the capability type URI
     * @param payload        the capability-specific payload
     */
    void send(PeerId to, String capabilityType, byte[] payload);
}
