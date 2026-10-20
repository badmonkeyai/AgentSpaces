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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A {@link LogStore} in memory: the default every {@link RaftLog} constructor
 * without a store uses, so a host that accepts amnesia on restart behaves
 * exactly as before JS-N-06 (srad/agentspaces-js.md). Tests count its writes
 * to check write-before-reply and inject one failing write to check that a
 * failed write leaves the reply unsent.
 */
public final class MemoryLogStore implements LogStore {

    private long term;
    private PeerId votedFor;
    private final List<LogEntry> entries = new ArrayList<>();
    private long writes;
    private IOException failNextWrite;

    /** An empty store: term 0, no vote, no entries. */
    public MemoryLogStore() {
    }

    /**
     * A store pre-loaded with state, as a restart would find it.
     *
     * @param initial the state to hold
     */
    public MemoryLogStore(State initial) {
        Objects.requireNonNull(initial, "initial");
        this.term = initial.term();
        this.votedFor = initial.votedFor();
        this.entries.addAll(initial.entries());
    }

    /** How many durable writes happened, for tests that check write-before-reply. */
    public synchronized long writes() {
        return writes;
    }

    /**
     * Arms a fault: the next write throws {@code failure} and changes nothing.
     *
     * @param failure the exception the next write throws
     */
    public synchronized void failNextWrite(IOException failure) {
        this.failNextWrite = Objects.requireNonNull(failure, "failure");
    }

    private void write() throws IOException {
        if (failNextWrite != null) {
            IOException failure = failNextWrite;
            failNextWrite = null;
            throw failure;
        }
        writes++;
    }

    @Override
    public synchronized State load() {
        return new State(term, votedFor, entries);
    }

    @Override
    public synchronized void saveTermVote(long term, PeerId votedFor) throws IOException {
        write();
        this.term = term;
        this.votedFor = votedFor;
    }

    @Override
    public synchronized void append(long fromIndex, List<LogEntry> entries) throws IOException {
        if (fromIndex < 1 || fromIndex > this.entries.size() + 1) {
            throw new IllegalArgumentException("append at " + fromIndex + " would leave a gap in "
                    + this.entries.size() + " entries");
        }
        write();
        truncate(fromIndex);
        this.entries.addAll(entries);
    }

    @Override
    public synchronized void truncateFrom(long index) throws IOException {
        write();
        truncate(index);
    }

    private void truncate(long index) {
        int keep = (int) Math.max(0, Math.min(entries.size(), index - 1));
        entries.subList(keep, entries.size()).clear();
    }
}
