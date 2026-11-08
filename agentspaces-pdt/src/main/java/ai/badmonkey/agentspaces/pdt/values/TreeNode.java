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

/**
 * An in-memory node of a value's tree (srad/agentspaces-pdt.md §3.1, PD-V-08). A
 * node created by an operation has no CID until a {@link Flusher} hashes it; a node
 * decoded from the store carries the CID it was read under. A reference to a child
 * is either a {@code TreeNode} in memory or a {@link String} CID not yet loaded.
 */
abstract class TreeNode {

    /** The CID once hashed or loaded; null while the node is new and unhashed. */
    volatile String cid;

    /**
     * Encodes this node to canonical bytes, asking the flusher for the CID of every
     * child reference, which hashes new children first.
     */
    abstract byte[] encode(Flusher flusher);
}
