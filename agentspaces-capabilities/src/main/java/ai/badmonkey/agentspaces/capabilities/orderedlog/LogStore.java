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
package ai.badmonkey.agentspaces.capabilities.orderedlog;

import ai.badmonkey.agentspaces.capabilities.orderedlog.RaftMessages.LogEntry;
import ai.badmonkey.agentspaces.common.id.PeerId;

import java.io.IOException;
import java.util.List;

/**
 * The durable state of a Raft voter (srad/agentspaces-js.md JS-N-06, the
 * Java twin of {@code @agentspaces/ordered}'s {@code LogStore}): the term, the
 * vote, and the log entries, each written by {@link RaftLog} <em>before</em>
 * any reply that depends on it leaves, so a voter that restarts never votes
 * twice in a term and never loses an entry it acknowledged. One shape for
 * Java and JavaScript, so membership change and snapshots later land once in
 * both.
 *
 * <p>Every method is synchronous and returns only once the write is durable to
 * the store's own standard: {@link MemoryLogStore} is durable until the
 * process ends, {@link FileLogStore} until the disk does. A method that throws
 * leaves the log's in-memory state exactly as it was before the attempted
 * write, and the reply unsent; the log counts the failure and carries on.
 *
 * <p>Indexes are 1-based, as in {@link RaftLog}: the first entry is index 1.
 */
public interface LogStore {

    /**
     * What a store holds; an empty store is term 0, no vote, no entries.
     *
     * @param term     the last durable term
     * @param votedFor the vote cast in that term, or {@code null} for none
     * @param entries  the durable entries, index 1 first
     */
    record State(long term, PeerId votedFor, List<LogEntry> entries) {

        /** The state of an empty store. */
        public static State empty() {
            return new State(0, null, List.of());
        }

        /** Null-safe: a null entries list reads as no entries. */
        public State {
            entries = entries == null ? List.of() : List.copyOf(entries);
        }
    }

    /**
     * Reads the store.
     *
     * @return the durable state
     * @throws IOException when the store cannot be read
     */
    State load() throws IOException;

    /**
     * Persists the term and the vote; returns once they are durable.
     *
     * @param term     the term
     * @param votedFor the vote cast in it, or {@code null} for none
     * @throws IOException when the write did not become durable
     */
    void saveTermVote(long term, PeerId votedFor) throws IOException;

    /**
     * Appends entries from 1-based {@code fromIndex}, first removing any entry
     * at or after it; returns once the entries are durable.
     *
     * @param fromIndex the index the first of {@code entries} takes
     * @param entries   the entries to append
     * @throws IOException when the write did not become durable
     */
    void append(long fromIndex, List<LogEntry> entries) throws IOException;

    /**
     * Removes the entries at and after 1-based {@code index}.
     *
     * @param index the first index to drop
     * @throws IOException when the write did not become durable
     */
    void truncateFrom(long index) throws IOException;
}
