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
package ai.badmonkey.agentspaces.agent.reduce;

import static ai.badmonkey.agentspaces.api.space.Matchers.eq;

import ai.badmonkey.agentspaces.agent.Entries;
import ai.badmonkey.agentspaces.agent.Keys;
import ai.badmonkey.agentspaces.agent.Tagged;
import ai.badmonkey.agentspaces.agent.annotation.SpaceReduce;
import ai.badmonkey.agentspaces.agent.join.JoinTicket;
import ai.badmonkey.agentspaces.agent.join.KeyTickets;
import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Matchers;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.Subscription;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.capabilities.orderedlog.OrderedTakes;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * The runtime behind {@code @SpaceReduce} (ISSUE-SpaceReduce §9.3): a fold
 * whose accumulator is an entry beside its elements. A step takes one element,
 * invokes the method with the key's current accumulator, and completes the
 * element with the new accumulator as the result, atomically; the previous
 * accumulator is then retired. A {@link JoinTicket} per key, taken before a
 * drain and re-armed by completion after it, serializes the steps of one key
 * across reducers; under {@link SpaceReduce.Mode#ORDERED} every take goes
 * through the ordered-log coordinator and the log decides the ticket's holder.
 *
 * <p>Constructed and started by {@code AgentBinder}; the step and the
 * side-output dispatch it is given are the binder's method invocation and
 * return handling, so this class never reflects on the agent itself.
 */
public final class ReduceBinding implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(ReduceBinding.class.getName());
    private static final int READ_LIMIT = 256;

    /** What the binder configured. */
    public record Config(String name, SpaceReduce.Mode mode, Space space, Class<?> elementType,
                         Template<?> elementTemplate, Class<?> accumulatorType, String keyField, String keyTag,
                         Lease takeLease, Duration pollTimeout, Lease accumulatorLease, Lease subscriptionLease,
                         int maxOpen, OrderedTakes coordinator) {

        public Config {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(mode, "mode");
            Objects.requireNonNull(space, "space");
            Objects.requireNonNull(elementType, "elementType");
            Objects.requireNonNull(elementTemplate, "elementTemplate");
            Objects.requireNonNull(accumulatorType, "accumulatorType");
            Objects.requireNonNull(takeLease, "takeLease");
            Objects.requireNonNull(pollTimeout, "pollTimeout");
            Objects.requireNonNull(accumulatorLease, "accumulatorLease");
            Objects.requireNonNull(subscriptionLease, "subscriptionLease");
            if (maxOpen <= 0) {
                throw new IllegalArgumentException("maxOpen must be positive: " + maxOpen);
            }
            if (mode == SpaceReduce.Mode.ORDERED && coordinator == null) {
                throw new IllegalArgumentException("reduce '" + name + "' is ORDERED but no coordinator was given");
            }
            if (accumulatorLease.duration().compareTo(takeLease.duration()) < 0) {
                throw new IllegalArgumentException("reduce '" + name + "': accumulatorLease "
                        + accumulatorLease.duration() + " is shorter than the take lease " + takeLease.duration());
            }
        }

        boolean keyedByTag() {
            return keyTag != null && !keyTag.isEmpty();
        }

        boolean keyedByField() {
            return keyField != null && !keyField.isEmpty();
        }
    }

    private final Config config;
    private final BiFunction<Object, Object, Object> step;
    private final Consumer<Object> sideOutputs;
    private final InstantSource clock;
    private final Method keyAccessor;
    private final KeyTickets tickets;

    private final Object lock = new Object();
    /** Keys with elements to fold, in arrival order. */
    private final LinkedHashSet<String> pending = new LinkedHashSet<>();
    /** The accumulators this reducer wrote, by key, for a cheap retirement. */
    private final Map<String, EntryHandle> written;
    /** LEASED: the live tickets of this reduce as the events show them, by key then issue stamp. */
    private final Map<String, java.util.TreeMap<ai.badmonkey.agentspaces.common.hlc.HlcTimestamp, EntryId>> liveTickets;
    private final List<Subscription> subscriptions = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();
    private volatile boolean running = true;
    private volatile boolean started;

    /**
     * @param config      what the binder resolved from the annotation
     * @param step        invokes the agent's method on (accumulator or null, element) and returns its result
     * @param sideOutputs the binder's return handling for a fork's other elements
     * @param clock       the binder's clock
     */
    public ReduceBinding(Config config, BiFunction<Object, Object, Object> step, Consumer<Object> sideOutputs,
                         InstantSource clock) {
        this.config = Objects.requireNonNull(config, "config");
        this.step = Objects.requireNonNull(step, "step");
        this.sideOutputs = Objects.requireNonNull(sideOutputs, "sideOutputs");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.keyAccessor = config.keyedByField() ? Keys.accessor(config.elementType(), config.keyField()) : null;
        this.written = KeyTickets.boundedMap(config.maxOpen());
        this.liveTickets = KeyTickets.boundedMap(config.maxOpen());
        this.tickets = new KeyTickets(config.name(), config.space(), config.accumulatorLease(), config.takeLease(),
                config.mode() == SpaceReduce.Mode.ORDERED ? config.coordinator() : null, clock, config.maxOpen());
    }

    /** The reduce's name, as tickets, accumulators, and the card carry it. */
    public String name() {
        return config.name();
    }

    /** Seeds the pending keys from the space, subscribes to the elements, and starts the drain. */
    public void start() {
        if (started) {
            throw new IllegalStateException("reduce '" + config.name() + "' already started");
        }
        started = true;
        tickets.start();
        int seeded = sweep();
        subscriptions.add(subscribe());
        subscriptions.add(subscribeTickets());
        threads.add(Thread.ofVirtual().name("space-reduce-" + config.name()).start(this::drain));
        threads.add(Thread.ofVirtual().name("space-reduce-keep-" + config.name()).start(this::keep));
        LOG.log(System.Logger.Level.INFO, config.name() + ": " + config.mode() + " reduce of "
                + config.elementType().getSimpleName() + " into " + config.accumulatorType().getSimpleName()
                + " on '" + config.space().name() + "', keyed by "
                + (config.keyedByTag() ? "tag " + config.keyTag()
                        : config.keyedByField() ? "field " + config.keyField() : "nothing (one accumulator)")
                + ", " + seeded + " key(s) pending");
    }

    @Override
    public void close() {
        running = false;
        threads.forEach(Thread::interrupt);
        subscriptions.forEach(Subscription::close);
        tickets.close();
    }

    // ------------------------------------------------------------------ elements arriving

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Subscription subscribe() {
        return config.space().notify((Template) config.elementTemplate(), event -> {
            SpaceEvent<?> spaceEvent = (SpaceEvent<?>) event;
            if (spaceEvent.kind() != SpaceEvent.Kind.WRITTEN && spaceEvent.kind() != SpaceEvent.Kind.REAPPEARED) {
                return;
            }
            String key = keyOf(spaceEvent.entry(), spaceEvent.tags());
            if (key != null) {
                enqueue(key);
            }
        }, config.subscriptionLease());
    }

    /**
     * LEASED: follows the tickets of this reduce so a reducer that takes a
     * ticket can tell whether an older one is live for the key, even while that
     * older ticket is claimed and so invisible to a read. Exact within one
     * process; best effort across replicas, which is the LEASED contract.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Subscription subscribeTickets() {
        return config.space().notify((Template) tickets.template(), event -> {
            SpaceEvent<?> spaceEvent = (SpaceEvent<?>) event;
            String key = spaceEvent.tags().get(JoinTicket.KEY_TAG);
            if (key == null || spaceEvent.details() == null) {
                return;
            }
            synchronized (lock) {
                switch (spaceEvent.kind()) {
                    case WRITTEN -> liveTickets.computeIfAbsent(key, k -> new java.util.TreeMap<>())
                            .put(spaceEvent.details().issued(), spaceEvent.entryId());
                    case COMPLETED, EXPIRED -> {
                        var live = liveTickets.get(key);
                        if (live != null) {
                            live.values().remove(spaceEvent.entryId());
                        }
                    }
                    default -> { }
                }
            }
            // A reappeared ticket is a key to serve, whoever wrote it: a ticket released by
            // its lease (an undecided ORDERED claim) comes back with no element left to raise
            // its key, and would otherwise linger until its write lease. A written ticket is
            // not: its writer serves it, and a held ticket is invisible to a read, so reacting
            // to the write would make the other reducer write a ticket of its own.
            if (spaceEvent.kind() == SpaceEvent.Kind.REAPPEARED) {
                enqueue(key);
            }
        }, config.subscriptionLease());
    }

    /** LEASED: whether a ticket older than {@code mine} is live for the key, as far as the events show. */
    private boolean olderTicketLive(String key, EntryId mine) {
        synchronized (lock) {
            var live = liveTickets.get(key);
            if (live == null) {
                return false;
            }
            for (var entry : live.entrySet()) {
                if (entry.getValue().equals(mine)) {
                    return false; // mine is the oldest live
                }
                return true;      // an older one is live, claimed or not
            }
            return false;
        }
    }

    /** Reads every available element and queues its key; returns how many keys are pending. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private int sweep() {
        for (Space.Entry<?> entry : (List<Space.Entry<?>>) (List) config.space()
                .readAllEntries((Template) config.elementTemplate(), READ_LIMIT)) {
            String key = keyOf(entry.value(), entry.tags());
            if (key != null) {
                enqueue(key);
            }
        }
        synchronized (lock) {
            return pending.size();
        }
    }

    private void enqueue(String key) {
        synchronized (lock) {
            pending.add(key);
            lock.notifyAll();
        }
    }

    private String keyOf(Object value, Map<String, String> tags) {
        if (config.keyedByTag()) {
            return tags == null ? null : tags.get(config.keyTag());
        }
        if (keyAccessor == null) {
            return Reductions.GLOBAL_KEY;
        }
        try {
            Object key = keyAccessor.invoke(value);
            return key == null ? null : String.valueOf(key);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("failed reading the reduce key of " + config.elementType().getName(), e);
        }
    }

    // ------------------------------------------------------------------ the drain

    private void drain() {
        while (running) {
            String key;
            synchronized (lock) {
                while (running && pending.isEmpty()) {
                    try {
                        lock.wait(config.pollTimeout().toMillis());
                    } catch (InterruptedException e) {
                        return;
                    }
                    if (pending.isEmpty()) {
                        sweep(); // an element written while this reducer was down, or an event it missed
                    }
                }
                if (!running) {
                    return;
                }
                var it = pending.iterator();
                key = it.next();
                it.remove();
            }
            try {
                drainKey(key);
            } catch (RuntimeException | Error e) {
                if (!running) {
                    return;
                }
                LOG.log(System.Logger.Level.WARNING, config.name() + ": draining key '" + key + "' failed", e);
            }
        }
    }

    /**
     * Holds a ticket for the key while folding every available element of the
     * key, then completes it; a later element raises a new ticket. A reducer
     * whose ticket is not the key's oldest live one backs off and completes it
     * bare, so two tickets written in one instant converge on one lock.
     */
    private void drainKey(String key) {
        if (!tickets.request(key)) {
            return;
        }
        Optional<TakenEntry<JoinTicket>> ticket = take(tickets.template(key));
        if (ticket.isEmpty()) {
            return; // another reducer holds the key; its events or the sweep raise it again
        }
        EntryId mine = ticket.get().entryId();
        if (config.mode() == SpaceReduce.Mode.ORDERED) {
            EntryId lock = tickets.awaitEarliestLive(key, mine);
            if (lock == null) {
                LOG.log(System.Logger.Level.DEBUG, config.name() + ": key '" + key
                        + "' undecided; releasing the ticket by its lease");
                return; // the take lease lapses and the ticket reappears, decided
            }
            if (!lock.equals(mine)) {
                config.space().complete(ticket.get()); // a duplicate: the earliest live ticket is the lock
                tickets.forget(key);
                return;
            }
        } else if (olderTicketLive(key, mine)) {
            config.space().complete(ticket.get());
            tickets.forget(key);
            LOG.log(System.Logger.Level.DEBUG, config.name() + ": key '" + key
                    + "' has an older live ticket; backing off");
            return;
        }
        int steps = 0;
        boolean held = true;
        try {
            while (running) {
                // The ticket is the lock only while its TAKE lease holds: renew it before
                // every step, and when the renewal fails the lease lapsed, another reducer
                // may hold the key, and this drain stops without completing anything more.
                try {
                    ticket.get().renew(config.takeLease().duration());
                } catch (RuntimeException e) {
                    held = false;
                    LOG.log(System.Logger.Level.DEBUG, config.name() + ": lost the ticket for key '" + key
                            + "' (" + e.getMessage() + "); stopping this drain");
                    break;
                }
                Optional<? extends TakenEntry<?>> element = take(elementQuery(key));
                if (element.isEmpty()) {
                    break;
                }
                if (!fold(key, element.get())) {
                    break; // the method threw: the element reappears by its lease; stop the drain here
                }
                steps++;
            }
        } finally {
            if (held) {
                try {
                    config.space().complete(ticket.get());
                } catch (RuntimeException e) {
                    LOG.log(System.Logger.Level.WARNING, config.name() + ": could not complete the ticket for key '"
                            + key + "'; it reappears by its lease", e);
                }
            }
            tickets.forget(key);
        }
        if (steps > 0) {
            LOG.log(System.Logger.Level.DEBUG, config.name() + ": key '" + key + "' folded " + steps + " element(s)");
        }
    }

    /**
     * One step: the method on (current, element), then the element completed
     * with the new accumulator, then the previous accumulator retired and the
     * fork's side outputs written. Returns false when the method threw.
     */
    private boolean fold(String key, TakenEntry<?> element) {
        Optional<? extends Space.Entry<?>> current = Reductions.current(config.space(), config.accumulatorType(),
                config.name(), key);
        Object result;
        try {
            result = step.apply(current.map(Space.Entry::value).orElse(null), element.entry());
        } catch (RuntimeException | Error e) {
            LOG.log(System.Logger.Level.WARNING, config.name() + ": the fold threw for key '" + key
                    + "'; the element's take lease will lapse", e);
            return false;
        }
        if (result == null) {
            config.space().complete(element);
            return true;
        }
        Object accumulator = result;
        Map<String, String> tags = new LinkedHashMap<>();
        List<Object> others = List.of();
        if (result instanceof Entries entries) {
            List<Object> rest = new ArrayList<>();
            Object found = null;
            for (Object candidate : entries.elements()) {
                Object value = candidate instanceof Tagged<?> tagged ? tagged.entry() : candidate;
                if (config.accumulatorType().isInstance(value)) {
                    if (found != null) {
                        throw new IllegalArgumentException(config.name() + " returned a fork with two "
                                + config.accumulatorType().getSimpleName() + " accumulators");
                    }
                    found = candidate;
                } else {
                    rest.add(candidate);
                }
            }
            if (found == null) {
                throw new IllegalArgumentException(config.name() + " returned a fork with no "
                        + config.accumulatorType().getSimpleName() + " accumulator");
            }
            accumulator = found;
            others = rest;
        }
        if (accumulator instanceof Tagged<?> tagged) {
            tags.putAll(tagged.tags());
            accumulator = tagged.entry();
        }
        if (!config.accumulatorType().isInstance(accumulator)) {
            throw new IllegalArgumentException(config.name() + " returned a " + accumulator.getClass().getSimpleName()
                    + "; the accumulator is a " + config.accumulatorType().getSimpleName());
        }
        long stepCount = current.map(Reductions::steps).orElse(0L) + 1;
        tags.put(Reductions.REDUCE_TAG, config.name());
        tags.put(Reductions.KEY_TAG, key);
        tags.put(Reductions.STEPS_TAG, Long.toString(stepCount));
        EntryHandle handle = config.space().complete(element, accumulator, config.accumulatorLease(), tags);
        EntryHandle previous;
        synchronized (lock) {
            previous = written.put(key, handle);
        }
        current.ifPresent(predecessor -> retire(key, predecessor, previous));
        for (Object other : others) {
            sideOutputs.accept(other);
        }
        return true;
    }

    /** Removes the predecessor: a cancel when this reducer wrote it, a take and a bare completion otherwise. */
    private void retire(String key, Space.Entry<?> predecessor, EntryHandle ownHandle) {
        try {
            if (ownHandle != null && ownHandle.entryId().equals(predecessor.entryId())) {
                ownHandle.cancel();
                return;
            }
            Template<?> stale = Reductions.template(config.accumulatorType(), config.name(), key)
                    .whereTag(Reductions.STEPS_TAG, eq(Long.toString(Reductions.steps(predecessor))));
            take(stale).ifPresent(taken -> config.space().complete(taken));
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, config.name() + ": could not retire accumulator "
                    + predecessor.entryId() + " of key '" + key + "'; readers choose the highest steps", e);
        }
    }

    private Template<?> elementQuery(String key) {
        if (config.keyedByTag()) {
            return config.elementTemplate().whereTag(config.keyTag(), eq(key));
        }
        if (keyAccessor == null) {
            return config.elementTemplate();
        }
        return config.elementTemplate().where(config.keyField(),
                Matchers.predicate(value -> value != null && key.equals(String.valueOf(value))));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <T> Optional<TakenEntry<T>> take(Template<T> template) {
        return config.mode() == SpaceReduce.Mode.ORDERED
                ? config.coordinator().take(template, config.takeLease(), config.pollTimeout())
                : config.space().take(template, config.takeLease(), config.pollTimeout());
    }

    // ------------------------------------------------------------------ housekeeping

    private void keep() {
        long halfLease = Math.max(1L, config.subscriptionLease().duration().toMillis() / 2);
        while (running) {
            try {
                Thread.sleep(halfLease);
            } catch (InterruptedException e) {
                return;
            }
            if (!running) {
                return;
            }
            for (Subscription subscription : subscriptions) {
                try {
                    subscription.renew(config.subscriptionLease().duration());
                } catch (RuntimeException e) {
                    LOG.log(System.Logger.Level.WARNING, config.name()
                            + ": subscription renewal failed; the reduce stops reacting", e);
                    return;
                }
            }
        }
    }

    /** The pending keys, for tests. */
    Set<String> pendingKeys() {
        synchronized (lock) {
            return Set.copyOf(pending);
        }
    }
}
