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

import ai.badmonkey.agentspaces.agent.Keys;
import ai.badmonkey.agentspaces.agent.annotation.SpaceJoin;
import ai.badmonkey.agentspaces.api.entry.EntryId;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The binder's runtime for one {@code @SpaceJoin} method (SPEC §10.3, issue
 * #16). It owns what a hand-written join owns: a subscription per part, the
 * per-key state of which parts have arrived, the seed of that state from the
 * space on start (and again for any key it meets for the first time, so a key
 * forgotten after {@code within} is picked up from what the space still
 * holds), the re-read of the parts by key when a key completes, and the "once"
 * rule of the mode: an in-memory set in {@link SpaceJoin.Mode#LOCAL}, a
 * {@link JoinTicket} taken under a lease in {@link SpaceJoin.Mode#LEASED}, and
 * a ticket taken through the ordered-log coordinator in
 * {@link SpaceJoin.Mode#ORDERED}, where the first committed ticket claim per
 * key, as every member sees it in the same order, is the one that fires.
 *
 * <p>Constructed and started by {@code AgentBinder}; the action and the
 * dispatch it is given are the binder's method invocation and return handling,
 * so this class never reflects on the agent itself.
 */
public final class JoinBinding implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(JoinBinding.class.getName());
    private static final int READ_LIMIT = 256;

    /**
     * One part as the binder resolved it: its type, its filtered template, the
     * space it lives in (the join's by default; a part may name another), and
     * where its key is.
     */
    public record PartSpec(Class<?> type, Template<?> template, Space space, String keyField,
                           String keyTag, boolean optional, int atLeast, String countedByType,
                           String countedByField) {

        /** A part needing one entry, keyed by field or tag. */
        public PartSpec(Class<?> type, Template<?> template, Space space, String keyField,
                        String keyTag, boolean optional) {
            this(type, template, space, keyField, keyTag, optional, 1, null, null);
        }

        public PartSpec {
            Objects.requireNonNull(type, "type");
            Objects.requireNonNull(template, "template");
            Objects.requireNonNull(space, "space");
            if (atLeast < 1) {
                throw new IllegalArgumentException("a part needs at least one entry: " + type.getName());
            }
            if ((keyField == null || keyField.isEmpty()) == (keyTag == null || keyTag.isEmpty())) {
                throw new IllegalArgumentException("a part is keyed by a field or by a tag: "
                        + type.getName());
            }
        }

        boolean keyedByTag() {
            return keyTag != null && !keyTag.isEmpty();
        }

        /**
         * Resolves the accessor a field key is read through, by the rule
         * {@code Template.where} uses: a record component, else a {@code getX},
         * {@code isX}, or plain {@code x()} method.
         *
         * @throws IllegalArgumentException when the type has no such field
         */
        public static Method accessor(Class<?> type, String field) {
            try {
                return Keys.accessor(type, field);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(e.getMessage()
                        + " as the join key; name the part's key or keyTag", e);
            }
        }
    }

    /** What the binder configured. */
    public record Config(String name, SpaceJoin.Mode mode, Space space, List<PartSpec> parts,
                         Duration within, int maxOpen, Lease subscriptionLease, Lease ticketLease,
                         Lease takeLease, Duration pollTimeout, OrderedTakes coordinator,
                         Duration settle) {

        /** A join that fires as soon as a key is complete. */
        public Config(String name, SpaceJoin.Mode mode, Space space, List<PartSpec> parts,
                      Duration within, int maxOpen, Lease subscriptionLease, Lease ticketLease,
                      Lease takeLease, Duration pollTimeout, OrderedTakes coordinator) {
            this(name, mode, space, parts, within, maxOpen, subscriptionLease, ticketLease, takeLease,
                    pollTimeout, coordinator, Duration.ZERO);
        }

        public Config {
            settle = settle == null ? Duration.ZERO : settle;
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(mode, "mode");
            Objects.requireNonNull(space, "space");
            parts = List.copyOf(parts);
            if (parts.isEmpty()) {
                throw new IllegalArgumentException("a join needs at least one part: '" + name + "'");
            }
            if (maxOpen <= 0) {
                throw new IllegalArgumentException("maxOpen must be positive: " + maxOpen);
            }
            if (mode == SpaceJoin.Mode.ORDERED && coordinator == null) {
                throw new IllegalArgumentException("join '" + name
                        + "' is ORDERED but no coordinator was given");
            }
        }
    }

    private static final class KeyState {
        /** The entry ids seen per part type: a count that survives redelivery. */
        final Map<Class<?>, Set<EntryId>> seen = new HashMap<>();
        /** Counts read from a {@code countedBy} part, by the counted part's type. */
        final Map<Class<?>, Integer> expected = new HashMap<>();
        long touchedMillis;
        /** Settle mode: whether this key has already fired or been handed to a ticket. */
        boolean released;
    }

    private final Config config;
    private final Function<Joined, Object> action;
    private final Consumer<Object> dispatch;
    private final InstantSource clock;
    private final Map<Class<?>, Method> accessors = new LinkedHashMap<>();
    /** countedBy: the counted part type to (the counting part type, its field accessor). */
    private final Map<Class<?>, Map.Entry<Class<?>, Method>> counters = new LinkedHashMap<>();
    /** LEASED and ORDERED: the per-key tickets, shared with the reduce binding. */
    private final KeyTickets tickets;

    private final Object lock = new Object();
    private final LinkedHashMap<String, KeyState> open;
    private final Set<String> fired;

    private final List<Subscription> subscriptions = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();
    private volatile boolean running = true;
    private volatile boolean started;

    /**
     * @param config   what the binder resolved from the annotation
     * @param action   invokes the agent's method on the bag and returns its result
     * @param dispatch the binder's return handling (entry, Tagged, Contribution, null)
     * @param clock    the binder's clock
     */
    public JoinBinding(Config config, Function<Joined, Object> action, Consumer<Object> dispatch,
                       InstantSource clock) {
        this.config = Objects.requireNonNull(config, "config");
        this.action = Objects.requireNonNull(action, "action");
        this.dispatch = Objects.requireNonNull(dispatch, "dispatch");
        this.clock = Objects.requireNonNull(clock, "clock");
        for (PartSpec part : config.parts()) {
            if (!part.keyedByTag()) {
                accessors.put(part.type(), PartSpec.accessor(part.type(), part.keyField()));
            }
            if (part.countedByType() != null) {
                PartSpec counting = config.parts().stream()
                        .filter(p -> p.type().getSimpleName().equals(part.countedByType()))
                        .findFirst().orElseThrow(() -> new IllegalArgumentException("part "
                                + part.type().getSimpleName() + " is countedBy '" + part.countedByType()
                                + "." + part.countedByField() + "' but no part has that type"));
                counters.put(part.type(), Map.entry(counting.type(),
                        Keys.accessor(counting.type(), part.countedByField())));
            }
        }
        this.open = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, KeyState> eldest) {
                if (size() > config.maxOpen()) {
                    LOG.log(System.Logger.Level.WARNING, config.name() + ": more than "
                            + config.maxOpen() + " keys open; forgetting '" + eldest.getKey()
                            + "' (its parts stay in the space and are found again when a new one arrives)");
                    return true;
                }
                return false;
            }
        };
        this.fired = bounded(config.maxOpen());
        this.tickets = new KeyTickets(config.name(), config.space(), config.ticketLease(), config.takeLease(),
                config.mode() == SpaceJoin.Mode.ORDERED ? config.coordinator() : null, clock, config.maxOpen());
    }

    /** The join's name, as tickets and the card carry it. */
    public String name() {
        return config.name();
    }

    /** Seeds the per-key state from the space, subscribes to every part, and starts the mode's loop. */
    public void start() {
        if (started) {
            throw new IllegalStateException("join '" + config.name() + "' already started");
        }
        started = true;
        tickets.start();
        int seeded = 0;
        for (PartSpec part : config.parts()) {
            seeded += seed(part);
        }
        if (!settling()) {
            for (String key : completeKeys()) {
                trigger(key);
            }
        }
        for (PartSpec part : config.parts()) {
            subscriptions.add(subscribe(part));
        }
        threads.add(Thread.ofVirtual().name("space-join-keep-" + config.name()).start(this::keep));
        if (config.mode() != SpaceJoin.Mode.LOCAL) {
            threads.add(Thread.ofVirtual().name("space-join-tickets-" + config.name())
                    .start(this::serveTickets));
        }
        LOG.log(System.Logger.Level.INFO, config.name() + ": " + config.mode() + " join of "
                + config.parts().stream().map(p -> p.type().getSimpleName() + "@" + p.space().name())
                        .toList() + ", tickets on '" + config.space().name() + "', " + seeded
                + " part(s) seeded, "
                + open.size() + " key(s) open");
    }

    @Override
    public void close() {
        running = false;
        threads.forEach(Thread::interrupt);
        subscriptions.forEach(Subscription::close);
        tickets.close();
    }

    // ------------------------------------------------------------------ parts arriving

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Subscription subscribe(PartSpec part) {
        return part.space().notify((Template) part.template(), event -> {
            SpaceEvent<?> spaceEvent = (SpaceEvent<?>) event;
            if (spaceEvent.kind() != SpaceEvent.Kind.WRITTEN
                    && spaceEvent.kind() != SpaceEvent.Kind.REAPPEARED) {
                return;
            }
            String key = keyOf(part, spaceEvent.entry(), spaceEvent.tags());
            if (key == null) {
                LOG.log(System.Logger.Level.DEBUG, config.name() + ": a " + part.type().getSimpleName()
                        + " carries no key; ignored");
                return;
            }
            if (record(part, key, spaceEvent.entryId(), spaceEvent.entry()) && !settling()) {
                trigger(key);
            }
        }, config.subscriptionLease());
    }

    private int seed(PartSpec part) {
        int count = 0;
        for (Space.Entry<?> entry : readAll(part, part.template())) {
            String key = keyOf(part, entry.value(), entry.tags());
            if (key != null) {
                record(part, key, entry.entryId(), entry.value());
                count++;
            }
        }
        return count;
    }

    private String keyOf(PartSpec part, Object value, Map<String, String> tags) {
        if (part.keyedByTag()) {
            return tags == null ? null : tags.get(part.keyTag());
        }
        try {
            Object key = accessors.get(part.type()).invoke(value);
            return key == null ? null : String.valueOf(key);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("failed reading the join key of " + part.type().getName(), e);
        }
    }

    /** Records a part under its key; returns whether the key is now complete. */
    private boolean record(PartSpec part, String key, EntryId entryId, Object value) {
        synchronized (lock) {
            KeyState state = open.get(key);
            if (state == null) {
                state = new KeyState();
                open.put(key, state);
                // A key met for the first time (or forgotten after `within`): what
                // the space already holds for it counts, so nothing is lost by
                // forgetting, and a late part completes a key the sweep dropped.
                for (PartSpec other : config.parts()) {
                    if (other == part) {
                        continue;
                    }
                    for (Space.Entry<?> held : readAll(other, queryFor(other, key))) {
                        note(state, other, held.entryId(), held.value());
                    }
                }
            }
            note(state, part, entryId, value);
            state.touchedMillis = clock.instant().toEpochMilli();
            return isComplete(state);
        }
    }

    /** Counts one entry of a part for a key, and reads the count it declares for other parts. */
    private void note(KeyState state, PartSpec part, EntryId entryId, Object value) {
        state.seen.computeIfAbsent(part.type(), t -> new HashSet<>()).add(entryId);
        for (Map.Entry<Class<?>, Map.Entry<Class<?>, Method>> counter : counters.entrySet()) {
            if (counter.getValue().getKey() == part.type()) {
                try {
                    Object declared = counter.getValue().getValue().invoke(value);
                    if (declared instanceof Number number) {
                        state.expected.put(counter.getKey(), number.intValue());
                    }
                } catch (ReflectiveOperationException e) {
                    throw new IllegalStateException("failed reading the count a "
                            + part.type().getName() + " declares", e);
                }
            }
        }
    }

    private boolean isComplete(KeyState state) {
        for (PartSpec part : config.parts()) {
            if (part.optional()) {
                continue;
            }
            int have = state.seen.getOrDefault(part.type(), Set.of()).size();
            Integer declared = state.expected.get(part.type());
            int need = declared != null ? Math.max(declared, 1) : part.atLeast();
            if (have < need) {
                return false;
            }
        }
        return true;
    }

    private boolean settling() {
        return !config.settle().isZero();
    }

    private List<String> completeKeys() {
        List<String> complete = new ArrayList<>();
        synchronized (lock) {
            open.forEach((key, state) -> {
                if (isComplete(state) && !state.released) {
                    complete.add(key);
                }
            });
        }
        return complete;
    }

    // ------------------------------------------------------------------ firing

    private void trigger(String key) {
        switch (config.mode()) {
            case LOCAL -> {
                boolean first;
                synchronized (lock) {
                    first = fired.add(key);
                }
                if (first) {
                    Thread.ofVirtual().name("space-join-" + config.name()).start(() -> {
                        try {
                            if (!fire(key)) {
                                synchronized (lock) {
                                    fired.remove(key); // a part lapsed or has not landed: the next part fires it
                                }
                            }
                        } catch (RuntimeException | Error e) {
                            synchronized (lock) {
                                fired.remove(key);
                            }
                            LOG.log(System.Logger.Level.WARNING, config.name() + ": join failed for key '"
                                    + key + "'", e);
                        }
                    });
                }
            }
            case LEASED, ORDERED -> tickets.request(key);
        }
    }

    /**
     * Invokes the method on the parts as the space holds them now. Returns false,
     * writing nothing, when a required part is no longer readable (its lease
     * lapsed between completion and firing).
     */
    private boolean fire(String key) {
        Joined joined = assemble(key);
        if (joined == null) {
            LOG.log(System.Logger.Level.DEBUG, config.name() + ": key '" + key
                    + "' completed but a required part is no longer readable; nothing fired");
            return false;
        }
        dispatch.accept(action.apply(joined));
        return true;
    }

    /**
     * Fires the key once every required part is readable here, waiting up to
     * the lesser of half the ticket's TAKE lease and five seconds for parts
     * that replicated later than the ticket; false when they never came. The
     * wait stays inside the lease, so a successful firing can still complete
     * the ticket it holds.
     */
    private boolean fireWhenPartsReadable(String key) {
        long wait = Math.min(config.takeLease().duration().toNanos() / 2, Duration.ofSeconds(5).toNanos());
        long deadline = System.nanoTime() + wait;
        while (true) {
            if (fire(key)) {
                return true;
            }
            if (System.nanoTime() >= deadline || !running) {
                return false;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Joined assemble(String key) {
        Map<Class<?>, List<Space.Entry<?>>> parts = new LinkedHashMap<>();
        for (PartSpec part : config.parts()) {
            List<Space.Entry<?>> entries = new ArrayList<>(readAll(part, queryFor(part, key)));
            entries.sort((a, b) -> b.issued().compareTo(a.issued()));
            if (entries.isEmpty() && !part.optional()) {
                return null;
            }
            if (!entries.isEmpty()) {
                parts.put(part.type(), entries);
            }
        }
        return new Joined(key, parts);
    }

    private Template<?> queryFor(PartSpec part, String key) {
        if (part.keyedByTag()) {
            return part.template().whereTag(part.keyTag(), eq(key));
        }
        return part.template().where(part.keyField(),
                Matchers.predicate(value -> value != null && key.equals(String.valueOf(value))));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private List<Space.Entry<?>> readAll(PartSpec part, Template<?> template) {
        return readAll(part.space(), template);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static List<Space.Entry<?>> readAll(Space space, Template<?> template) {
        return (List) space.readAllEntries((Template) template, READ_LIMIT);
    }

    // ------------------------------------------------------------------ tickets (LEASED, ORDERED)

    private void serveTickets() {
        while (running) {
            Optional<TakenEntry<JoinTicket>> taken;
            try {
                taken = config.mode() == SpaceJoin.Mode.ORDERED
                        ? config.coordinator().take(tickets.template(), config.takeLease(), config.pollTimeout())
                        : config.space().take(tickets.template(), config.takeLease(), config.pollTimeout());
            } catch (RuntimeException e) {
                if (!running) {
                    return;
                }
                // ORDERED: a leader election in flight is routine and the next poll
                // resubmits; LEASED: a failed take is worth a warning.
                LOG.log(config.mode() == SpaceJoin.Mode.ORDERED
                        ? System.Logger.Level.DEBUG : System.Logger.Level.WARNING,
                        config.name() + ": taking a ticket failed; retrying", e);
                continue;
            }
            if (taken.isEmpty()) {
                continue;
            }
            String key = taken.get().entry().key();
            LOG.log(System.Logger.Level.DEBUG, config.name() + ": took ticket " + taken.get().entryId()
                    + " for key '" + key + "'");
            if (config.mode() == SpaceJoin.Mode.ORDERED && tickets.awaitFirstCommitted(key) == null) {
                // Undecided: an earlier committed claim for this key has a record this
                // member has not seen yet. Neither fire nor complete; the ticket's TAKE
                // lease lapses, it reappears, and the next taker decides with the
                // record in hand. Once per key fleet-wide costs this retry, never a duplicate.
                LOG.log(System.Logger.Level.DEBUG, config.name() + ": key '" + key
                        + "' undecided; releasing the ticket by its lease");
                continue;
            }
            boolean fires = shouldFire(key, taken.get().entryId());
            LOG.log(System.Logger.Level.DEBUG, config.name() + ": ticket " + taken.get().entryId() + " for key '"
                    + key + "' " + (fires ? "fires" : "completes without firing (decided: "
                    + tickets.decided(key).map(EntryId::toString).orElse("none") + ")"));
            try {
                if (fires && !fireWhenPartsReadable(key)) {
                    // The ticket was written by a member that saw every part; this member,
                    // which won the ticket, may not hold them all yet. Neither fire nor
                    // complete: the key is forgotten here, the ticket's TAKE lease lapses,
                    // and the ticket reappears for a member that has the parts (under
                    // ORDERED the same ticket stays the key's first committed one).
                    synchronized (lock) {
                        fired.remove(key);
                    }
                    LOG.log(System.Logger.Level.DEBUG, config.name() + ": ticket " + taken.get().entryId()
                            + " for key '" + key + "' fired nothing: a required part is not readable here;"
                            + " releasing the ticket by its lease");
                    continue;
                }
                config.space().complete(taken.get());
            } catch (RuntimeException | Error e) {
                if (fires) {
                    synchronized (lock) {
                        fired.remove(key);
                    }
                }
                // The crash idiom: no completion, the ticket's TAKE lease lapses, and
                // the ticket reappears for this joiner or another.
                LOG.log(System.Logger.Level.WARNING, config.name() + ": join failed for key '" + key
                        + "'; the ticket's lease will lapse", e);
            }
        }
    }

    private boolean shouldFire(String key, EntryId ticket) {
        if (config.mode() == SpaceJoin.Mode.ORDERED) {
            Optional<EntryId> firing = tickets.decided(key);
            if (firing.isPresent() && !firing.get().equals(ticket)) {
                return false; // a later ticket for a key the log already decided
            }
        }
        synchronized (lock) {
            return fired.add(key);
        }
    }

    // ------------------------------------------------------------------ housekeeping

    private void keep() {
        long halfLease = Math.max(1L, config.subscriptionLease().duration().toMillis() / 2);
        long sweep = Math.max(1L, Math.min(config.within().toMillis() / 4, Duration.ofSeconds(30).toMillis()));
        long step = Math.min(halfLease, sweep);
        if (settling()) {
            step = Math.max(1L, Math.min(step, config.settle().toMillis() / 2));
        }
        long lastRenew = System.currentTimeMillis();
        while (running) {
            try {
                Thread.sleep(step);
            } catch (InterruptedException e) {
                return;
            }
            if (!running) {
                return;
            }
            forgetStale();
            if (settling()) {
                for (String key : settledKeys()) {
                    trigger(key);
                }
            }
            if (System.currentTimeMillis() - lastRenew >= halfLease) {
                lastRenew = System.currentTimeMillis();
                for (Subscription subscription : subscriptions) {
                    try {
                        subscription.renew(config.subscriptionLease().duration());
                    } catch (RuntimeException e) {
                        LOG.log(System.Logger.Level.WARNING, config.name()
                                + ": subscription renewal failed; the join stops", e);
                        return;
                    }
                }
            }
        }
    }

    /** Settle mode: complete keys no part has touched for {@code settle}, each handed out once. */
    private List<String> settledKeys() {
        long cutoff = clock.instant().toEpochMilli() - config.settle().toMillis();
        List<String> settled = new ArrayList<>();
        synchronized (lock) {
            open.forEach((key, state) -> {
                if (!state.released && state.touchedMillis <= cutoff && isComplete(state)) {
                    state.released = true;
                    settled.add(key);
                }
            });
        }
        return settled;
    }

    private void forgetStale() {
        long cutoff = clock.instant().toEpochMilli() - config.within().toMillis();
        synchronized (lock) {
            var it = open.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, KeyState> entry = it.next();
                if (entry.getValue().touchedMillis < cutoff) {
                    LOG.log(System.Logger.Level.DEBUG, config.name() + ": forgetting key '" + entry.getKey()
                            + "' after " + config.within() + " without all its parts");
                    it.remove();
                }
            }
        }
    }

    private static Set<String> bounded(int max) {
        return Collections.newSetFromMap(KeyTickets.boundedMap(max));
    }
}
