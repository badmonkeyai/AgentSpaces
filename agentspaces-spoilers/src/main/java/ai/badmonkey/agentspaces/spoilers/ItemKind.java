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
package ai.badmonkey.agentspaces.spoilers;

/**
 * The item vocabulary of the level-2 matcher (srad/spoiler-peers.md §3.3, SP-Q-MNT-03): what
 * {@link WireItems} decodes inside a frame. A script names items by these constants, so a renamed item
 * breaks the script at compile time.
 */
public enum ItemKind {
    /** One entry's replicated state (a write, renew, cancel or completion), in a rumor or a pull answer. */
    STATE,
    /** A signed take claim. */
    CLAIM,
    /** A state whose completion flag is set. */
    COMPLETION,
    /** A state whose removes cover its adds: a cancel or a take's removal. */
    CANCEL,
    /** One PDT node in a content-swarm {@code NODES} answer; its {@code kind} attribute is the node kind, {@code vg} for a chain node. */
    NODE,
    /** A content-swarm request for nodes by CID ({@code NODE_WANT}); its id is the first CID. */
    NODE_WANT,
    /** A content-swarm request for a tree under a root ({@code TREE_WANT}): a head or base fetch. */
    TREE_WANT
}
