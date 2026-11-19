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
package ai.badmonkey.agentspaces.peering.node;

import ai.badmonkey.agentspaces.api.ad.PeerAdvertisement;
import ai.badmonkey.agentspaces.common.id.GroupId;
import ai.badmonkey.agentspaces.common.id.PeerId;
import ai.badmonkey.agentspaces.identity.KeyFiles;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * The members a node stopped retrying (reviews/LOST-MEMBER-RETIREMENT-2026-11-18.md): per group, each retired member's
 * last endpoints and the issue time of its last card, on its own clock. A member enters only after it was admitted,
 * lost, and retried without an answer until it retired, so a stranger cannot fill the list; a fresh card from it, or
 * its revocation, takes it off. The list is read only as a last resort, when a group's view is empty and neither its
 * lost members, its seeds, nor its locator brought anyone back, and only when the node opted in (post-retirement).
 *
 * <p>Bounded per group (the entry seen least recently goes first), aged out after a horizon, and persisted beside the
 * admission memory when the node has a state directory: {@code <state>/retired/<groupId>.json}, written with the
 * keystore's atomic owner-only write, at most once per gossip period and only after a change. A restarted node whose
 * locators find nobody can then still reach the members it once had.
 */
final class RetirementList {

    private static final System.Logger LOG = System.getLogger(RetirementList.class.getName());

    /** The default bound per group. */
    static final int DEFAULT_BOUND = 256;
    /** How long a retired member is remembered after it was last seen. */
    static final long HORIZON_MILLIS = 30L * 24 * 60 * 60_000L;
    static final int FILE_VERSION = 1;
    static final String DIRECTORY = "retired";
    private static final Pattern SAFE_NAME = Pattern.compile("[A-Za-z0-9._-]{1,128}");
    private static final ObjectMapper JSON = new ObjectMapper();

    /** One retired member. */
    record Retired(PeerId peer, List<PeerAdvertisement.Endpoint> endpoints, long cardIssuedMillis, long lastSeenMillis,
                   long retiredAtMillis) {
    }

    record FileForm(int v, String group, List<RetiredForm> retired) {
    }

    record EndpointForm(String transport, String address, int priority) {
    }

    record RetiredForm(String peer, List<EndpointForm> endpoints, long cardIssued, long lastSeen, long retiredAt) {
    }

    private static final class Group {
        final Map<PeerId, Retired> retired = new LinkedHashMap<>();
        boolean dirty;
        Long lastWriteMillis;
    }

    private final Path directory;
    private final int bound;
    private final Map<GroupId, Group> groups = new ConcurrentHashMap<>();
    private final Set<GroupId> warnedUnwritable = ConcurrentHashMap.newKeySet();

    RetirementList(Path stateDirectory, int bound) {
        if (bound <= 0) {
            throw new IllegalArgumentException("the retirement list bound must be positive: " + bound);
        }
        this.directory = stateDirectory == null ? null : stateDirectory.resolve(DIRECTORY);
        this.bound = bound;
    }

    private Path fileFor(GroupId groupId) {
        if (directory == null) {
            return null;
        }
        String name = groupId.value();
        if (!SAFE_NAME.matcher(name).matches() || name.equals(".") || name.equals("..")) {
            return null;
        }
        return directory.resolve(name + ".json");
    }

    private Group group(GroupId groupId) {
        return groups.computeIfAbsent(groupId, g -> new Group());
    }

    /** Loads a group's list from its file; a missing, unreadable, or foreign file leaves it empty. */
    void load(GroupId groupId, long nowMillis) {
        Group group = new Group();
        Path file = fileFor(groupId);
        if (file != null && Files.exists(file)) {
            try {
                FileForm form = JSON.readValue(Files.readAllBytes(file), FileForm.class);
                if (form != null && form.v() == FILE_VERSION && groupId.value().equals(form.group())
                        && form.retired() != null) {
                    for (RetiredForm r : form.retired()) {
                        try {
                            if (r == null || nowMillis - r.lastSeen() > HORIZON_MILLIS || r.endpoints() == null) {
                                continue;
                            }
                            List<PeerAdvertisement.Endpoint> endpoints = new ArrayList<>();
                            for (EndpointForm e : r.endpoints()) {
                                endpoints.add(new PeerAdvertisement.Endpoint(e.transport(), e.address(), e.priority()));
                            }
                            PeerId peer = PeerId.of(r.peer());
                            group.retired.put(peer, new Retired(peer, List.copyOf(endpoints), r.cardIssued(),
                                    r.lastSeen(), r.retiredAt()));
                        } catch (RuntimeException e) {
                            // one malformed entry is skipped; the rest still load
                        }
                    }
                } else {
                    LOG.log(System.Logger.Level.WARNING, "ignoring " + file + ": not a retirement list of group "
                            + groupId.value());
                }
            } catch (IOException | RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "cannot read the retirement list " + file + "; starting empty: " + e);
            }
        }
        groups.put(groupId, group);
    }

    /** Retires a member: on the list with its last endpoints and card time, the least recently seen leaving first. */
    void retire(GroupId groupId, Retired retired) {
        Group group = group(groupId);
        synchronized (group) {
            group.retired.put(retired.peer(), retired);
            while (group.retired.size() > bound) {
                group.retired.values().stream().min(Comparator.comparingLong(Retired::lastSeenMillis))
                        .ifPresent(oldest -> group.retired.remove(oldest.peer()));
            }
            group.dirty = true;
        }
    }

    /** Takes a member off the list: a fresh card from it, or its revocation. */
    boolean remove(GroupId groupId, PeerId peer) {
        Group group = groups.get(groupId);
        if (group == null) {
            return false;
        }
        synchronized (group) {
            boolean removed = group.retired.remove(peer) != null;
            group.dirty |= removed;
            return removed;
        }
    }

    Optional<Retired> get(GroupId groupId, PeerId peer) {
        Group group = groups.get(groupId);
        if (group == null) {
            return Optional.empty();
        }
        synchronized (group) {
            return Optional.ofNullable(group.retired.get(peer));
        }
    }

    /** The group's retired members within the horizon, the most recently seen first; older ones are dropped. */
    List<Retired> list(GroupId groupId, long nowMillis) {
        Group group = groups.get(groupId);
        if (group == null) {
            return List.of();
        }
        synchronized (group) {
            group.dirty |= group.retired.values().removeIf(r -> nowMillis - r.lastSeenMillis() > HORIZON_MILLIS);
            List<Retired> out = new ArrayList<>(group.retired.values());
            out.sort(Comparator.comparingLong(Retired::lastSeenMillis).reversed());
            return out;
        }
    }

    /** Forgets a group in memory (the node left it); its file stays for a later join. */
    void forget(GroupId groupId) {
        groups.remove(groupId);
    }

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

    void flushAll(long nowMillis) {
        if (directory == null) {
            return;
        }
        groups.forEach((groupId, group) -> {
            boolean dirty;
            synchronized (group) {
                dirty = group.dirty;
            }
            if (dirty) {
                write(groupId, group, nowMillis);
            }
        });
    }

    private void write(GroupId groupId, Group group, long nowMillis) {
        Path file = fileFor(groupId);
        if (file == null) {
            return;
        }
        List<RetiredForm> forms = new ArrayList<>();
        synchronized (group) {
            for (Retired r : group.retired.values()) {
                List<EndpointForm> endpoints = new ArrayList<>();
                for (PeerAdvertisement.Endpoint e : r.endpoints()) {
                    endpoints.add(new EndpointForm(e.transport(), e.address(), e.priority()));
                }
                forms.add(new RetiredForm(r.peer().value(), endpoints, r.cardIssuedMillis(), r.lastSeenMillis(),
                        r.retiredAtMillis()));
            }
            group.dirty = false;
            group.lastWriteMillis = nowMillis;
        }
        try {
            KeyFiles.write(file, JSON.writeValueAsBytes(new FileForm(FILE_VERSION, groupId.value(), forms)), true);
            warnedUnwritable.remove(groupId);
        } catch (IOException | RuntimeException e) {
            synchronized (group) {
                group.dirty = true;
            }
            if (warnedUnwritable.add(groupId)) {
                LOG.log(System.Logger.Level.WARNING, "cannot write the retirement list " + file + "; held in memory: " + e);
            }
        }
    }

    /** Whether the list persists across restarts. */
    boolean persistent() {
        return directory != null;
    }

    /** For tests: the bound. */
    int bound() {
        return bound;
    }

    static RetirementList inMemory() {
        return new RetirementList(null, DEFAULT_BOUND);
    }
}
