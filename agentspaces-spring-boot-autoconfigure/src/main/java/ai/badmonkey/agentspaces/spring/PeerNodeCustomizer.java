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
 * A hook on the node's builder, applied in bean order before {@code build()}:
 * the way to change the node's tunables (advertisement TTL, rate limits,
 * channel trust) from an application without replacing the {@code PeerNode}
 * bean and the security wiring that comes with it. The Boot idiom of
 * {@code WebServerFactoryCustomizer}.
 */
@FunctionalInterface
public interface PeerNodeCustomizer {

    /**
     * Customizes the builder.
     *
     * @param builder the node builder, with the starter's settings applied
     */
    void customize(PeerNode.Builder builder);
}
