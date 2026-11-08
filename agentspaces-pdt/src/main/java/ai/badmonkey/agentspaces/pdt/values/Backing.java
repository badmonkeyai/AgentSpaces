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

import java.util.Objects;

/**
 * What a value needs from the outside (srad/agentspaces-pdt.md §3.4, PD-V-06,
 * PD-V-08): the store its new nodes are written to and the loader its cold nodes
 * come from. A derived value keeps the backing of the value it came from.
 *
 * @param store  the node store new nodes are written to
 * @param loader the loader cold nodes are read through
 */
record Backing(NodeStore store, NodeLoader loader) {

    Backing {
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(loader, "loader");
    }

    static Backing of(NodeStore store) {
        return new Backing(store, NodeLoader.of(store));
    }
}
