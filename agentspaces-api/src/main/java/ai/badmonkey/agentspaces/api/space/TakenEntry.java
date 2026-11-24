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
 * An exclusive, leased hold on a taken entry. The taker works on the entry, then
 * either {@link Space#complete(TakenEntry) completes} it (consuming it permanently,
 * optionally writing a result) or lets the TAKE lease lapse, after which the entry
 * reappears for other takers. Long-running work renews the lease as it goes.
 *
 * @param <T> the entry type
 */
public interface TakenEntry<T> {

    /** Returns the taken entry value. */
    T entry();

    /** Returns the entry's identifier. */
    EntryId entryId();

    /**
     * Extends the TAKE lease to at least the given duration from now. Like an entry's write lease, it never
     * shortens: the new expiry is the later of the current one and now plus {@code extension}. To give the entry
     * back early, let the lease lapse.
     *
     * @param extension how long from now the hold should last at least
     * @throws ai.badmonkey.agentspaces.api.error.LeaseExpiredException if the lease already lapsed
     */
    void renew(Duration extension);

    /**
     * Whether this taker still holds the take where it was made: its claim stands, is this taker's at the take's
     * epoch, and has not expired (SPEC v0.1.37 §7.4, a take held across a wake). A take whose claim is no longer its
     * own has lapsed, whatever its own lease end says: another holder's claim displaced it, or the ordered log decided
     * another. Reading it changes nothing. The default cannot tell and answers true, so only the lease end judges.
     *
     * @return whether the take is still held
     */
    default boolean held() {
        return true;
    }
}
