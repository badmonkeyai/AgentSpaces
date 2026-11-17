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
     * Extends the write lease to at least the given duration from now. It never shortens the lease: the new
     * expiry is the later of the current one and now plus {@code extension} (SPEC §7.2), so renewing for less than
     * the lease has left changes nothing. To end an entry sooner, {@link #cancel() cancel} it, and write it again
     * for a shorter lease if it should stay.
     *
     * <p>Only the entry's issuer renews it, and only while it is present and unexpired. Renewing for less than the
     * lease has left is counted and logged once per space, so a caller that still relies on shortening shows up.
     *
     * @param extension how long from now the entry should live at least
     * @throws ai.badmonkey.agentspaces.api.error.LeaseExpiredException if the entry is gone (withdrawn, completed,
     *         or never held here) or its lease already lapsed
     * @throws IllegalStateException if the caller is not the entry's issuer, such as another agent of the same peer
     *         acting on an agent-attested entry
     */
    void renew(Duration extension);

    /** Withdraws the entry from the space immediately. Idempotent. */
    void cancel();
}
