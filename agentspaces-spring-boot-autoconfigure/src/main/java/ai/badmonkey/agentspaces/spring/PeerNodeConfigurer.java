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

import ai.badmonkey.agentspaces.peering.node.PeerNode;

/**
 * A hook on the built node, applied in bean order after the transports are
 * registered and before any group is joined: the place to register a
 * transport the starter does not know, or to listen on a second address.
 */
@FunctionalInterface
public interface PeerNodeConfigurer {

    /**
     * Configures the node.
     *
     * @param node the node, listening on its configured transports, no group joined yet
     */
    void configure(PeerNode node);
}
