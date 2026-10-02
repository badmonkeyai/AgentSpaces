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
package ai.badmonkey.agentspaces.api.space;

import ai.badmonkey.agentspaces.api.entry.EntryId;

import java.time.Duration;

/**
 * A writer's handle on a published entry: the means of renewing the write lease or
 * withdrawing the entry early.
 */
public interface EntryHandle {

    /** Returns the entry's identifier. */
    EntryId entryId();

    /**
     * Extends the write lease by the given duration from now.
     *
     * @param extension how much longer the entry should live
     * @throws ai.badmonkey.agentspaces.api.error.LeaseExpiredException if the lease already lapsed
     */
    void renew(Duration extension);

    /** Withdraws the entry from the space immediately. Idempotent. */
    void cancel();
}
