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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.KeyFiles;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.InstantSource;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * The verifier's record of first admissions (ISSUE-PeerRobustness-2, R2-1):
 * per group, per peer, the join credential hash and delegation hash the peer
 * was admitted under, and when it was last seen. {@code PeerNode} consults it
 * when a delegated credential fails verification only because its delegation
 * expired: a peer the memory holds under the identical record is re-admitted,
 * since the invite window bounds who may join and says nothing about who may
 * stay; a peer it does not hold is a first admission and is refused. The
 * revocation sweep reads it to evict, as the {@code admittedUnder} map it
 * replaces was read.
 *
 * <p>Bounded per group (the oldest entry by last-seen goes first past the
 * bound), and persisted beside the keystore when the node has a state
 * directory: one file per group, {@code <state>/admitted/<groupId>.json},
 * holding peer ids and hashes only, written to a temp file and moved into
 * place with owner-only permissions where the platform has them, at most once
 * per gossip period and only after an entry was added, changed, or removed. A
 * node with no state directory keeps the memory in memory alone; the node says
 * so once per INVITE group it joins.
 */
final class AdmissionMemory {

    private static final System.Logger LOG = System.getLogger(AdmissionMemory.class.getName());

    /** The default bound on remembered peers per group. */
    static final int DEFAULT_BOUND = 10_000;

    /** The file format version. */
    static final int FILE_VERSION = 1;

    /** The sub-directory of the state directory the group files live in. */
    static final String DIRECTORY = "admitted";

    /** A group id that is safe as a file name: no separators, no traversal, bounded. */
    private static final Pattern SAFE_NAME = Pattern.compile("[A-Za-z0-9._-]{1,128}");

    private static final ObjectMapper JSON = new ObjectMapper();

    /** The file form: {@code {"v":1,"group":"z...","peers":{"<peerId>":{"record":"<hex>[,<hex>]","lastSeen":<millis>}}}}. */
    record FileForm(int v, String group, Map<String, PeerForm> peers) {
    }

    record PeerForm(String record, long lastSeen) {
    }

    /** One remembered peer. */
    private static final class Entry {
        String record;
        long lastSeenMillis;

        Entry(String record, long lastSeenMillis) {
            this.record = record;
            this.lastSeenMillis = lastSeenMillis;
        }
    }

    /** One group's memory; every access synchronizes on the instance. */
    private static final class Group {
        final Map<PeerId, Entry> peers = new HashMap<>();
        boolean dirty;
        Long lastWriteMillis;
    }

    private final Path directory;
    private final int bound;
    private final InstantSource clock;
    private final Map<GroupId, Group> groups = new ConcurrentHashMap<>();
    /** Groups whose file could not be written, told once. */
    private final Set<GroupId> warnedUnwritable = ConcurrentHashMap.newKeySet();

    /**
     * Creates the memory.
     *
     * @param stateDirectory the node's state directory, or null to keep the memory in memory alone
     * @param bound          the bound on remembered peers per group
     * @param clock          the node's clock, for last-seen
     */
    AdmissionMemory(Path stateDirectory, int bound, InstantSource clock) {
        if (bound <= 0) {
            throw new IllegalArgumentException("the admission memory bound must be positive: " + bound);
        }
        this.directory = stateDirectory == null ? null : stateDirectory.resolve(DIRECTORY);
        this.bound = bound;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** Whether the memory persists across restarts. */
    boolean persistent() {
        return directory != null;
    }

    /** The bound on remembered peers per group. */
    int bound() {
        return bound;
    }

    /** The file a group's memory lives in, or null without a state directory or for an unsafe id. */
    Path fileFor(GroupId groupId) {
        if (directory == null) {
            return null;
        }
        String name = groupId.value();
        if (!SAFE_NAME.matcher(name).matches() || name.equals(".") || name.equals("..")) {
            LOG.log(System.Logger.Level.WARNING, "the admission memory of group " + name
                    + " is not persisted: its id is not a safe file name");
            return null;
        }
        return directory.resolve(name + ".json");
    }

    /**
     * Loads a group's memory from its file, replacing whatever was held for the
     * group; a missing, unreadable, or foreign file leaves the group empty.
     *
     * @param groupId the group
     */
    void load(GroupId groupId) {
        Group group = new Group();
        Path file = fileFor(groupId);
        if (file != null && Files.exists(file)) {
            try {
                FileForm form = JSON.readValue(Files.readAllBytes(file), FileForm.class);
                if (form == null || form.v() != FILE_VERSION || !groupId.value().equals(form.group())) {
                    LOG.log(System.Logger.Level.WARNING, "ignoring " + file
                            + ": not an admission memory of group " + groupId.value());
                } else if (form.peers() != null) {
                    for (Map.Entry<String, PeerForm> entry : form.peers().entrySet()) {
                        if (entry.getValue() == null || entry.getValue().record() == null) {
                            continue;
                        }
                        try {
                            group.peers.put(PeerId.of(entry.getKey()),
                                    new Entry(entry.getValue().record(), entry.getValue().lastSeen()));
                        } catch (RuntimeException e) {
                            // Not a peer id: skipped, the rest still load.
                        }
                    }
                    LOG.log(System.Logger.Level.INFO, "loaded the admission memory of group " + groupId.value()
                            + ": " + group.peers.size() + " member(s) from " + file);
                }
            } catch (IOException | RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "cannot read the admission memory " + file
                        + "; starting empty: " + e);
            }
        }
        groups.put(groupId, group);
    }

    /**
     * Whether the memory holds a peer in a group under exactly this record.
     *
     * @param groupId the group
     * @param peer    the peer
     * @param record  the credential hash and delegation hash, as {@link #record} stored them
     * @return whether the identical record is held
     */
    boolean holds(GroupId groupId, PeerId peer, String record) {
        Group group = groups.get(groupId);
        if (group == null) {
            return false;
        }
        synchronized (group) {
            Entry entry = group.peers.get(peer);
            return entry != null && entry.record.equals(record);
        }
    }

    /**
     * Records a peer's admission under a record, refreshing last-seen. A new
     * or changed record marks the group dirty; past the bound the entry least
     * recently seen goes first.
     *
     * @param groupId the group
     * @param peer    the peer
     * @param record  the credential hash and delegation hash
     */
    void record(GroupId groupId, PeerId peer, String record) {
        Objects.requireNonNull(record, "record");
        Group group = groups.computeIfAbsent(groupId, g -> new Group());
        long now = clock.millis();
        synchronized (group) {
            Entry entry = group.peers.get(peer);
            if (entry == null) {
                if (group.peers.size() >= bound) {
                    evictOldest(group);
                }
                group.peers.put(peer, new Entry(record, now));
                group.dirty = true;
            } else {
                if (!entry.record.equals(record)) {
                    entry.record = record;
                    group.dirty = true;
                }
                entry.lastSeenMillis = now;
            }
        }
    }

    /**
     * Refreshes a remembered peer's last-seen without touching its record or
     * the dirty mark.
     *
     * @param groupId the group
     * @param peer    the peer
     */
    void touch(GroupId groupId, PeerId peer) {
        Group group = groups.get(groupId);
        if (group == null) {
            return;
        }
        synchronized (group) {
            Entry entry = group.peers.get(peer);
            if (entry != null) {
                entry.lastSeenMillis = clock.millis();
            }
        }
    }

    /**
     * Forgets a peer in a group.
     *
     * @param groupId the group
     * @param peer    the peer
     * @return whether it was held
     */
    boolean remove(GroupId groupId, PeerId peer) {
        Group group = groups.get(groupId);
        if (group == null) {
            return false;
        }
        synchronized (group) {
            if (group.peers.remove(peer) == null) {
                return false;
            }
            group.dirty = true;
            return true;
        }
    }

    /**
     * The records held for a group, peer to record, a snapshot.
     *
     * @param groupId the group
     * @return the snapshot
     */
    Map<PeerId, String> forGroup(GroupId groupId) {
        Group group = groups.get(groupId);
        if (group == null) {
            return Map.of();
        }
        synchronized (group) {
            Map<PeerId, String> copy = new LinkedHashMap<>();
            group.peers.forEach((peer, entry) -> copy.put(peer, entry.record));
            return copy;
        }
    }

    /**
     * Forgets a group entirely and deletes its file.
     *
     * @param groupId the group
     */
    void drop(GroupId groupId) {
        groups.remove(groupId);
        warnedUnwritable.remove(groupId);
        Path file = fileFor(groupId);
        if (file != null) {
            try {
                Files.deleteIfExists(file);
            } catch (IOException e) {
                LOG.log(System.Logger.Level.WARNING, "cannot delete the admission memory " + file + ": " + e);
            }
        }
    }

    /**
     * Writes a group's memory when it changed and at least {@code periodMillis}
     * passed since the last write; called from the node's tick.
     *
     * @param groupId      the group
     * @param nowMillis    the node's clock
     * @param periodMillis the group's gossip period
     */
    void flushIfDue(GroupId groupId, long nowMillis, long periodMillis) {
        Group group = groups.get(groupId);
        if (group == null || directory == null) {
            return;
        }
        synchronized (group) {
            if (!group.dirty || (group.lastWriteMillis != null && nowMillis - group.lastWriteMillis < periodMillis)) {
                return;
            }
        }
        write(groupId, group, nowMillis);
    }

    /** Writes every changed group now; called when the node closes. */
    void flushAll() {
        if (directory == null) {
            return;
        }
        long now = clock.millis();
        groups.forEach((groupId, group) -> {
            boolean dirty;
            synchronized (group) {
                dirty = group.dirty;
            }
            if (dirty) {
                write(groupId, group, now);
            }
        });
    }

    private void write(GroupId groupId, Group group, long nowMillis) {
        Path file = fileFor(groupId);
        if (file == null) {
            return;
        }
        Map<String, PeerForm> peers = new LinkedHashMap<>();
        synchronized (group) {
            group.peers.forEach((peer, entry) -> peers.put(peer.value(),
                    new PeerForm(entry.record, entry.lastSeenMillis)));
            group.dirty = false;
            group.lastWriteMillis = nowMillis;
        }
        try {
            // The same atomic, owner-only write the keystore uses (R2-S2).
            KeyFiles.write(file, JSON.writeValueAsBytes(new FileForm(FILE_VERSION, groupId.value(), peers)), true);
            warnedUnwritable.remove(groupId);
        } catch (IOException | RuntimeException e) {
            synchronized (group) {
                group.dirty = true; // try again next period
            }
            if (warnedUnwritable.add(groupId)) {
                LOG.log(System.Logger.Level.WARNING, "cannot write the admission memory " + file
                        + "; the memory of group " + groupId.value() + " is held in memory until it can be: " + e);
            }
        }
    }

    private static void evictOldest(Group group) {
        PeerId oldest = null;
        long oldestSeen = Long.MAX_VALUE;
        for (Map.Entry<PeerId, Entry> entry : group.peers.entrySet()) {
            if (entry.getValue().lastSeenMillis < oldestSeen) {
                oldestSeen = entry.getValue().lastSeenMillis;
                oldest = entry.getKey();
            }
        }
        if (oldest != null) {
            group.peers.remove(oldest);
        }
    }
}
