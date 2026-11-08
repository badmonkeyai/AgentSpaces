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
package ai.badmonkey.agentspaces.pdt.values;

import ai.badmonkey.agentspaces.pdt.NodeStore;
import ai.badmonkey.agentspaces.pdt.NodeUnavailableException;

import java.util.List;
import java.util.Objects;

/**
 * Loads the bytes of a node by CID on demand (srad/agentspaces-pdt.md PD-V-06). A
 * value calls this for every child it has not yet decoded, so a cold {@code get}
 * touches only the nodes on its path. The loader of {@link #of} reads a
 * {@link NodeStore} and throws {@link NodeUnavailableException} when the store lacks
 * the node; the store layer wraps a tree fetch with a deadline behind this same
 * interface.
 */
@FunctionalInterface
public interface NodeLoader {

    /**
     * Loads a node.
     *
     * @param cid the CID
     * @return the verified bytes
     * @throws NodeUnavailableException when the node cannot be had
     */
    byte[] load(String cid);

    /** A loader that reads one store and names no sources when the store lacks a node. */
    static NodeLoader of(NodeStore store) {
        Objects.requireNonNull(store, "store");
        return cid -> store.get(cid).orElseThrow(() -> new NodeUnavailableException(cid, List.of()));
    }
}
