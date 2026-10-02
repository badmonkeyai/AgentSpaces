/*
 * Copyright 2026 Bad Monkey, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.badmonkey.agentspaces.capabilities.keywrap;

import ai.badmonkey.agentspaces.common.crypto.GroupKey;
import ai.badmonkey.agentspaces.common.crypto.GroupKeyWrap;

/** Test access to the bound wrap, so hostile holders can be scripted by hand. */
final class GroupKeyWrapAccess {

    record Wrapped(byte[] ephemeral, byte[] sealed) {
    }

    private GroupKeyWrapAccess() {
    }

    static Wrapped wrap(GroupKey key, byte[] recipient, byte[] binding) {
        GroupKeyWrap.WrappedKey wrapped = GroupKeyWrap.wrapBound(key, recipient, binding);
        return new Wrapped(wrapped.ephemeralPublicKey(), wrapped.sealed());
    }
}
