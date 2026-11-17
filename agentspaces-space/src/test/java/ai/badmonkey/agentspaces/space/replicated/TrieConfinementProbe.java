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
package ai.badmonkey.agentspaces.space.replicated;

import ai.badmonkey.agentspaces.space.crdt.HashTrie;

/**
 * A class outside the crdt package that names the trie, which is what RE-Q-MNT-01 forbids, kept in the tests only so
 * {@code TrieConfinementTest} can show its rule catches one.
 */
public final class TrieConfinementProbe {

    private TrieConfinementProbe() {
    }

    /** Names the trie, the violation. */
    public static int size() {
        return HashTrie.empty().size();
    }
}
