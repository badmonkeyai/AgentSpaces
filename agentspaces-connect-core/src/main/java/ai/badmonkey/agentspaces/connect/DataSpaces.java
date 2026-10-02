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
package ai.badmonkey.agentspaces.connect;

import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.identity.PeerIdentity;
import ai.badmonkey.agentspaces.peering.blocks.BlockExchange;
import ai.badmonkey.agentspaces.peering.node.GroupRuntime;
import ai.badmonkey.agentspaces.space.replicated.ReplicatedSpace;

import java.util.Objects;

/**
 * Builds the spaces the connector protocols flow through (spec §6.1a). A
 * catalog result is inline when small and content-addressed when bulk, and
 * {@link ReplicatedSpace} routes payloads over the inline limit through a
 * {@link BlockExchange} only when its builder is given one; a space built
 * without one fails fast on a large {@code DataResult}. This factory attaches
 * the exchange so bulk results travel by reference on the connector's side and
 * are read through the exchange on the agent's side.
 */
public final class DataSpaces {

    /** The conventional name of the space queries, results, and changes flow through. */
    public static final String DEFAULT_NAME = "data";

    private DataSpaces() {
    }

    /**
     * Starts building a data space with a new {@link BlockExchange} attached.
     * A {@code BlockExchange} is one per group runtime; a host that already
     * holds one for other spaces should pass it to
     * {@link #builder(GroupRuntime, String, PeerIdentity, String, BlockExchange)}
     * instead.
     *
     * @param runtime   the group runtime
     * @param name      the space name within the group
     * @param identity  the local peer identity
     * @param agentName the local agent name writes are attributed to
     * @return the builder, with content addressing wired
     */
    public static ReplicatedSpace.Builder builder(GroupRuntime runtime, String name,
                                                  PeerIdentity identity, String agentName) {
        return builder(runtime, name, identity, agentName,
                new BlockExchange(runtime, CborCodec.defaultCodec()));
    }

    /**
     * Starts building a data space over an existing {@link BlockExchange}.
     *
     * @param runtime   the group runtime
     * @param name      the space name within the group
     * @param identity  the local peer identity
     * @param agentName the local agent name writes are attributed to
     * @param blocks    the group runtime's block exchange
     * @return the builder, with content addressing wired
     */
    public static ReplicatedSpace.Builder builder(GroupRuntime runtime, String name,
                                                  PeerIdentity identity, String agentName,
                                                  BlockExchange blocks) {
        return ReplicatedSpace.builder(runtime, name, identity, agentName)
                .blocks(Objects.requireNonNull(blocks, "blocks"));
    }
}
