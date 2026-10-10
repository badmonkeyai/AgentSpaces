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
package ai.badmonkey.agentspaces.agent.join;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;

import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.orderedlog.OrderedTakes;
import ai.badmonkey.agentspaces.space.replicated.TakeClaim;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The per-key ticket a fan-in or a fold serializes on, shared by
 * {@link JoinBinding} and the reduce binding (ISSUE-SpaceReduce §9.3). A
 * {@link JoinTicket} is written when a key needs serving, once per writer per
 * ticket lease, and the younger of two tickets written in the same instant is
 * withdrawn, so a duplicate needs a partition and never just a race. Under
 * {@code ORDERED} the ordered log's commit order decides which ticket is the
 * key's first: the committed claims are resolved to keys in order, waiting for
 * a claim's record to replicate, so every member decides identically.
 */
public final class KeyTickets implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(KeyTickets.class.getName());
    private static final int READ_LIMIT = 256;
    /**
     * ORDERED: how long a committed claim may wait for its record before it is
     * dropped as unresolvable. Measured in real time, since replication lag is
     * a real-time quantity and a test clock that runs ahead of the wall must
     * not drop a claim whose record is still in flight.
     */
    private static final Duration PENDING_CLAIM_MAX_AGE = Duration.ofSeconds(30);

    private final String name;
    private final Space space;
    private final Lease ticketLease;
    private final Lease takeLease;
    private final OrderedTakes coordinator;
    private final InstantSource clock;
    private final Template<JoinTicket> template;

    private final Object lock = new Object();
    /** When this writer last wrote a ticket for the key. */
    private final Map<String, Long> requested;
    /** ORDERED: the ticket whose claim committed first for the key. */
    private final Map<String, EntryId> firstCommitted;
    /** ORDERED: every committed ticket claim per key, in commit order (the reduce's lock rule). */
    private final Map<String, List<EntryId>> committed;
    /** ORDERED: committed claims whose record had not replicated here yet, in commit order. */
    private final List<PendingClaim> pendingClaims = new ArrayList<>();
    private AutoCloseable commitListener;

    private record PendingClaim(EntryId entryId, long sinceNanos) {
    }

    /**
     * @param name        the join's or reduce's name, which the tickets carry
     * @param space       the space the tickets live in
     * @param ticketLease the ticket's write lease
     * @param takeLease   the ticket's take lease; half of it bounds the ORDERED wait
     * @param coordinator the ordered-log coordinator, or null outside ORDERED
     * @param clock       the binder's clock
     * @param maxOpen     the bound on remembered keys
     */
    public KeyTickets(String name, Space space, Lease ticketLease, Lease takeLease,
                      OrderedTakes coordinator, InstantSource clock, int maxOpen) {
        this.name = Objects.requireNonNull(name, "name");
        this.space = Objects.requireNonNull(space, "space");
        this.ticketLease = Objects.requireNonNull(ticketLease, "ticketLease");
        this.takeLease = Objects.requireNonNull(takeLease, "takeLease");
        this.coordinator = coordinator;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.requested = boundedMap(maxOpen);
        this.firstCommitted = boundedMap(maxOpen);
        this.committed = boundedMap(maxOpen);
        this.template = Template.of(JoinTicket.class).whereTag(JoinTicket.JOIN_TAG, eq(name));
    }

    /** Every ticket of this name. */
    public Template<JoinTicket> template() {
        return template;
    }

    /** The tickets of one key. */
    public Template<JoinTicket> template(String key) {
        return template.whereTag(JoinTicket.KEY_TAG, eq(key));
    }

    /** The tags a ticket of this name carries for a key. */
    public Map<String, String> tags(String key) {
        return Map.of(JoinTicket.JOIN_TAG, name, JoinTicket.KEY_TAG, key);
    }

    /** ORDERED: starts following the log's committed claims. */
    public void start() {
        if (coordinator != null) {
            commitListener = coordinator.onCommitted(this::onCommittedClaim);
        }
    }

    @Override
    public void close() {
        if (commitListener != null) {
            try {
                commitListener.close();
            } catch (Exception e) {
                // nothing to do: the coordinator is closing too
            }
        }
    }

    /**
     * Writes a ticket for the key unless this writer wrote one within the
     * ticket lease or another writer's ticket is already visible; withdraws
     * its own when an older one exists.
     *
     * @return whether a ticket for the key is live here afterwards, as far as
     *         this writer can see
     */
    public boolean request(String key) {
        long now = clock.instant().toEpochMilli();
        synchronized (lock) {
            Long last = requested.get(key);
            if (last != null && now - last < ticketLease.duration().toMillis()) {
                return true; // this writer already asked; its ticket is live or being served
            }
            requested.put(key, now);
        }
        try {
            if (space.read(template(key)).isPresent()) {
                LOG.log(System.Logger.Level.DEBUG, name + ": a ticket for key '" + key + "' is already visible; not writing");
                return true; // another writer asked first and its ticket replicated here
            }
            EntryHandle mine = space.write(new JoinTicket(name, key), ticketLease, tags(key));
            // Two writers that saw the key at the same moment both wrote a ticket
            // before either could see the other's. The later one withdraws: the
            // oldest ticket by issue stamp stands, so a duplicate needs a
            // partition, never just a race.
            Space.Entry<?> own = null;
            Space.Entry<?> oldest = null;
            for (Space.Entry<JoinTicket> ticket : space.readAllEntries(template(key), READ_LIMIT)) {
                if (ticket.entryId().equals(mine.entryId())) {
                    own = ticket;
                }
                if (oldest == null || ticket.issued().compareTo(oldest.issued()) < 0) {
                    oldest = ticket;
                }
            }
            if (own != null && oldest != null && !oldest.entryId().equals(mine.entryId())) {
                mine.cancel();
                LOG.log(System.Logger.Level.DEBUG, name + ": withdrew a duplicate ticket for key '" + key + "'");
            } else {
                LOG.log(System.Logger.Level.DEBUG, name + ": ticket written for key '" + key + "'");
            }
            return true;
        } catch (RuntimeException e) {
            synchronized (lock) {
                requested.remove(key);
            }
            LOG.log(System.Logger.Level.WARNING, name + ": could not write the ticket for key '" + key + "'", e);
            return false;
        }
    }

    /** Forgets that this writer asked for the key, so the next request writes again. */
    public void forget(String key) {
        synchronized (lock) {
            requested.remove(key);
        }
    }

    /** ORDERED: the first committed ticket for the key, if the log has decided it here. */
    public Optional<EntryId> decided(String key) {
        synchronized (lock) {
            return Optional.ofNullable(firstCommitted.get(key));
        }
    }

    /**
     * ORDERED: the entry id of the first committed ticket for the key, waiting up
     * to half the take lease for the committed claims before it to resolve;
     * null when still undecided, which the caller treats as "release and retry".
     */
    public EntryId awaitFirstCommitted(String key) {
        long wait = Math.max(Duration.ofSeconds(1).toNanos(), takeLease.duration().toNanos() / 2);
        long deadline = System.nanoTime() + wait;
        while (true) {
            resolvePendingClaims();
            synchronized (lock) {
                EntryId firing = firstCommitted.get(key);
                if (firing != null) {
                    return firing;
                }
            }
            if (System.nanoTime() >= deadline) {
                return null;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
    }

    /**
     * ORDERED: the earliest committed ticket for the key that is still live
     * (claimed or not, but not completed or lapsed), which is the one lock a
     * reduce drains under; every member answers identically from the log's
     * order and the replica's state.
     */
    public Optional<EntryId> earliestLive(String key) {
        resolvePendingClaims();
        synchronized (lock) {
            List<EntryId> order = committed.get(key);
            if (order == null) {
                return Optional.empty();
            }
            var it = order.iterator();
            while (it.hasNext()) {
                EntryId id = it.next();
                if (coordinator.space().recordOf(id, false).isPresent()) {
                    return Optional.of(id);
                }
                it.remove(); // completed or lapsed: no longer a lock
            }
            return Optional.empty();
        }
    }

    /**
     * ORDERED: waits up to half the take lease for the key's committed claims
     * to resolve and returns {@link #earliestLive}, or null when the claim
     * {@code mine} has not resolved by then (release and retry).
     */
    public EntryId awaitEarliestLive(String key, EntryId mine) {
        long wait = Math.max(Duration.ofSeconds(1).toNanos(), takeLease.duration().toNanos() / 2);
        long deadline = System.nanoTime() + wait;
        while (true) {
            Optional<EntryId> earliest = earliestLive(key);
            boolean mineResolved;
            synchronized (lock) {
                mineResolved = committed.getOrDefault(key, List.of()).contains(mine);
            }
            if (earliest.isPresent() && mineResolved) {
                return earliest.get();
            }
            if (System.nanoTime() >= deadline) {
                return null;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
    }

    private void onCommittedClaim(EntryId entryId, TakeClaim claim) {
        synchronized (lock) {
            pendingClaims.add(new PendingClaim(entryId, System.nanoTime()));
            if (pendingClaims.size() > READ_LIMIT * 4) {
                pendingClaims.remove(0);
            }
        }
        resolvePendingClaims();
    }

    /**
     * Resolves committed claims to keys in commit order, stopping at the first
     * claim whose record this member has not seen yet so that "the first
     * committed ticket for a key" is judged in the log's order; a claim whose
     * record never arrives is dropped after {@link #PENDING_CLAIM_MAX_AGE}.
     */
    private void resolvePendingClaims() {
        long now = System.nanoTime();
        synchronized (lock) {
            var it = pendingClaims.iterator();
            while (it.hasNext()) {
                PendingClaim pending = it.next();
                EntryId entryId = pending.entryId();
                // A completed ticket still answers: the taker may have finished before
                // this member learned of the claim, and the key must still be read.
                Optional<EntryRecord> record = coordinator.space().recordOf(entryId, true);
                if (record.isEmpty()) {
                    if (now - pending.sinceNanos() > PENDING_CLAIM_MAX_AGE.toNanos()) {
                        LOG.log(System.Logger.Level.DEBUG, name + ": committed claim on " + entryId
                                + " dropped; its record never replicated here within " + PENDING_CLAIM_MAX_AGE);
                        it.remove(); // never replicated here; stop blocking the keys behind it
                        continue;
                    }
                    break; // not replicated here yet; keep the order and try again later
                }
                it.remove();
                Map<String, String> tags = record.get().tags();
                String key = tags.get(JoinTicket.KEY_TAG);
                if (name.equals(tags.get(JoinTicket.JOIN_TAG)) && key != null) {
                    boolean first = firstCommitted.putIfAbsent(key, entryId) == null;
                    LOG.log(System.Logger.Level.DEBUG, name + ": committed claim on ticket " + entryId + " for key '"
                            + key + "' resolved" + (first ? " as the first" : " (not the first)"));
                    List<EntryId> order = committed.computeIfAbsent(key, k -> new ArrayList<>());
                    if (!order.contains(entryId)) {
                        order.add(entryId);
                    }
                }
            }
        }
    }

    /** A map that forgets its eldest entry beyond {@code max}, for per-key state. */
    public static <V> Map<String, V> boundedMap(int max) {
        return new LinkedHashMap<>(16, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, V> eldest) {
                return size() > max;
            }
        };
    }
}
