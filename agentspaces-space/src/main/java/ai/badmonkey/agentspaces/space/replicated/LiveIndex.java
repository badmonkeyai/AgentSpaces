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

import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * The live index of a replicated space (RE-M-04, srad/replication-by-entry.md): the record of every present entry,
 * under its current lease, by entry id, so a read walks the present entries alone. It is a derived cache of the map,
 * kept in step by the space under its lock, and the only structure beside the trie (RE-Q-MNT-01).
 *
 * <p>An open-addressed table of records, probed linearly on the id's hash, where a hash map would keep a node of
 * 32 bytes per entry: the table costs one reference per slot at a load between a quarter and a half, about 8 to 16
 * bytes per present entry, which is what keeps the map and this index under 100 bytes an entry (RE-Q-PRF-06). A
 * removal shifts the entries after it back into place, so the table never holds a deletion marker.
 *
 * <p>Not thread-safe: the space reads and changes it under its lock only.
 */
final class LiveIndex {

    private static final int MIN_CAPACITY = 16;

    private EntryRecord[] table = new EntryRecord[MIN_CAPACITY];
    private int size;

    /** Returns the number of present entries. */
    int size() {
        return size;
    }

    /**
     * Returns the record filed for an entry.
     *
     * @param entryId the entry
     * @return the record, or null when the entry is not present
     */
    EntryRecord get(EntryId entryId) {
        int i = find(table, entryId);
        return i < 0 ? null : table[i];
    }

    /**
     * Files a record under its entry id, replacing the one filed before.
     *
     * @param record the record under its current lease
     */
    void put(EntryRecord record) {
        int i = find(table, record.entryId());
        if (i >= 0) {
            table[i] = record;
            return;
        }
        if ((size + 1) * 2 > table.length) {
            resize(table.length * 2);
        }
        insert(table, record);
        size++;
    }

    /**
     * Takes an entry out.
     *
     * @param entryId the entry
     */
    void remove(EntryId entryId) {
        int i = find(table, entryId);
        if (i < 0) {
            return;
        }
        int mask = table.length - 1;
        table[i] = null;
        size--;
        // Shift every record of the run after the hole back, if its probe started at or before the hole, so a lookup
        // never stops short at an empty slot.
        int hole = i;
        int next = (i + 1) & mask;
        while (table[next] != null) {
            int home = home(table[next].entryId(), mask);
            if (((next - home) & mask) >= ((next - hole) & mask)) {
                table[hole] = table[next];
                table[next] = null;
                hole = next;
            }
            next = (next + 1) & mask;
        }
        if (table.length > MIN_CAPACITY && size * 8 < table.length) {
            resize(table.length / 2); // a space that drained gives its table back
        }
    }

    /** Returns the records, in table order; a new list the caller owns. */
    List<EntryRecord> values() {
        List<EntryRecord> out = new ArrayList<>(size);
        for (EntryRecord record : table) {
            if (record != null) {
                out.add(record);
            }
        }
        return out;
    }

    /** Calls the action with each record, in table order, without making a list. */
    void forEach(java.util.function.Consumer<EntryRecord> action) {
        for (EntryRecord record : table) {
            if (record != null) {
                action.accept(record);
            }
        }
    }

    private void resize(int capacity) {
        EntryRecord[] old = table;
        EntryRecord[] grown = new EntryRecord[capacity];
        for (EntryRecord record : old) {
            if (record != null) {
                insert(grown, record);
            }
        }
        table = grown;
    }

    private static void insert(EntryRecord[] into, EntryRecord record) {
        int mask = into.length - 1;
        int i = home(record.entryId(), mask);
        while (into[i] != null) {
            i = (i + 1) & mask;
        }
        into[i] = record;
    }

    private static int find(EntryRecord[] in, EntryId entryId) {
        int mask = in.length - 1;
        int i = home(entryId, mask);
        while (in[i] != null) {
            if (in[i].entryId().equals(entryId)) {
                return i;
            }
            i = (i + 1) & mask;
        }
        return -1;
    }

    /** The slot an id's probe starts at: its hash, mixed so that a run of near hashes does not cluster, under the mask. */
    private static int home(EntryId entryId, int mask) {
        int h = entryId.hashCode() * 0x9E3779B9;
        return (h ^ (h >>> 16)) & mask;
    }
}
