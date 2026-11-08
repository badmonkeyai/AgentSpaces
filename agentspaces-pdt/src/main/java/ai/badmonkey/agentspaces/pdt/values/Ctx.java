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

import java.util.concurrent.atomic.AtomicReference;

/**
 * The context of one operation over a trie (srad/agentspaces-pdt.md §3.2, PD-V-05,
 * PD-V-06): the loader for cold nodes, the hasher for keys, and the edit token of
 * a transient, which is null for a persistent operation. A node whose own token is
 * this context's token belongs to the running transient and may change in place.
 */
final class Ctx {

    final NodeLoader loader;
    final KeyHasher hasher;
    final AtomicReference<Thread> edit;

    Ctx(NodeLoader loader, KeyHasher hasher, AtomicReference<Thread> edit) {
        this.loader = loader;
        this.hasher = hasher;
        this.edit = edit;
    }

    /** Whether a node carrying {@code token} may change in place under this context. */
    boolean owns(AtomicReference<Thread> token) {
        return edit != null && token == edit;
    }
}
