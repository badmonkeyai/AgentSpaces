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
package ai.badmonkey.agentspaces.space.local;

import ai.badmonkey.agentspaces.api.entry.EntryId;
import ai.badmonkey.agentspaces.api.entry.EntryRecord;
import ai.badmonkey.agentspaces.api.entry.LeaseInfo;
import ai.badmonkey.agentspaces.api.entry.LeaseKind;
import ai.badmonkey.agentspaces.api.error.LeaseExpiredException;
import ai.badmonkey.agentspaces.api.error.SpaceClosedException;
import ai.badmonkey.agentspaces.api.space.EntryHandle;
import ai.badmonkey.agentspaces.api.space.Lease;
import ai.badmonkey.agentspaces.api.space.Space;
import ai.badmonkey.agentspaces.api.space.SpaceEvent;
import ai.badmonkey.agentspaces.api.space.SpaceListener;
import ai.badmonkey.agentspaces.api.space.Subscription;
import ai.badmonkey.agentspaces.api.space.TakenEntry;
import ai.badmonkey.agentspaces.api.space.Template;
import ai.badmonkey.agentspaces.api.spi.SchemaRegistry;
import ai.badmonkey.agentspaces.common.codec.CborCodec;
import ai.badmonkey.agentspaces.common.codec.Multibase;
import ai.badmonkey.agentspaces.common.crypto.Digests;
import ai.badmonkey.agentspaces.common.hlc.HybridLogicalClock;
import ai.badmonkey.agentspaces.common.id.AgentId;
import ai.badmonkey.agentspaces.common.id.SpaceId;
import ai.badmonkey.agentspaces.space.EntryView;
import ai.badmonkey.agentspaces.space.crdt.SpaceStateCrdt;

import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A complete single-JVM implementation of the {@link Space} API (plan §6): the
 * full lease semantics of the model, entirely in process. The Embabel extension,
 * the examples, and application tests all run against {@code LocalSpace} first;
 * the replicated space implements the same interface, so swapping it in later is
 * configuration, and replication stays orthogonal to application code.
 *
 * <p>Time is injected: lease expiry is evaluated against the configured
 * {@link InstantSource} lazily at every operation, so deterministic tests drive a
 * {@code TestClock} with no sleeping. For wall-clock deployments the builder can
 * additionally start a background sweeper so expiry events fire without traffic.
 *
 * <p>Listeners run on the mutating thread after the mutation commits; they must
 * return promptly (see {@link SpaceListener}).
 */
public final class LocalSpace implements Space, AutoCloseable {

    private enum State {
        AVAILABLE, TAKEN
    }

    private static final class Stored {
        EntryRecord record;
        Object entry;
        State state = State.AVAILABLE;
        long writeExpiryMillis;
        long takeExpiryMillis;
        long takeToken;
        /** Whether the writer was an agent with its own certified key (v0.1.13, {@link #as}). */
        boolean attested;
    }

    private final String name;
    private final SpaceId id;
    private final AgentId issuer;
    private final InstantSource clock;
    /** The base of this space's subscription leases (EL-C-04, §10.18): see {@link #localMillis()}. */
    private final ai.badmonkey.agentspaces.common.time.TimeSource localTime;
    private final HybridLogicalClock hlc;
    private final CborCodec codec;
    private final SchemaRegistry schemas;
    /** The sweeper's time source and its next one-shot (EL-T-12, EL-T-13); null without {@code sweepEvery}. */
    private final ai.badmonkey.agentspaces.common.time.TimeSource sweepTime;
    private final Duration sweepInterval;
    private volatile ai.badmonkey.agentspaces.common.time.TimeSource.Scheduled sweeper;

    private static final System.Logger LOG = System.getLogger(LocalSpace.class.getName());

    /**
     * Renewals that asked for less than the lease had left (RE-H-01 and RE-H-02, srad/replication-by-entry.md §3.6).
     * A renewal never shortens a lease (SPEC §7.2), so each such call kept the longer lease.
     */
    private final AtomicLong renewShortenings = new AtomicLong();
    /** Whether the first shortening renewal is logged ({@link Builder#renewShorteningLog}). */
    private final boolean renewShorteningLog;
    /** Whether the first shortening renewal of this space was logged; each later one is only counted. */
    private final AtomicBoolean renewShorteningLogged = new AtomicBoolean();

    private final Object lock = new Object();
    /** Moves on every change this space signals its waiters with (srad/agentspaces-event-loop.md DW-2). */
    private final java.util.concurrent.atomic.AtomicLong changes = new java.util.concurrent.atomic.AtomicLong();
    /** Told after each change; each only wakes (a registration's wake or a flag). */
    private final java.util.List<Runnable> changeListeners = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final Map<EntryId, Stored> entries = new HashMap<>();
    /**
     * The type-partitioned index (PERF1 phase 2): the same {@link Stored}
     * instances as {@code entries}, bucketed by entry class in insertion
     * order, so matching scans only the buckets a template's type accepts and
     * candidates within a type arrive first-in first-out.
     */
    private final Map<Class<?>, LinkedHashMap<EntryId, Stored>> buckets = new HashMap<>();
    private final List<Sub> subscriptions = new CopyOnWriteArrayList<>();
    private volatile boolean closed;

    private LocalSpace(Builder builder) {
        this.name = builder.name;
        this.id = SpaceId.local(builder.name);
        this.issuer = builder.issuer;
        this.clock = builder.clock;
        this.localTime = ai.badmonkey.agentspaces.common.time.TimeSources.of(builder.clock);
        this.hlc = new HybridLogicalClock(builder.clock, builder.issuer.encoded());
        this.codec = CborCodec.defaultCodec();
        this.schemas = builder.schemas;
        this.renewShorteningLog = builder.renewShorteningLog;
        this.sweepInterval = builder.sweepInterval;
        this.sweepTime = builder.time != null ? builder.time
                : builder.clock instanceof ai.badmonkey.agentspaces.common.time.TimeSource source ? source
                : ai.badmonkey.agentspaces.common.time.TimeSource.system();
        if (sweepInterval != null) {
            armSweep();
        }
    }

    /**
     * Starts building a space.
     *
     * @param name   the space name
     * @param issuer the agent identity local writes are attributed to
     * @return the builder
     */
    public static Builder builder(String name, AgentId issuer) {
        return new Builder(name, issuer);
    }

    /** Builder for {@link LocalSpace}. */
    public static final class Builder {
        private final String name;
        private final AgentId issuer;
        private InstantSource clock = InstantSource.system();
        private SchemaRegistry schemas = new SimpleSchemaRegistry();
        private Duration sweepInterval;
        private ai.badmonkey.agentspaces.common.time.TimeSource time;
        private boolean renewShorteningLog = true;

        private Builder(String name, AgentId issuer) {
            this.name = Objects.requireNonNull(name, "name");
            this.issuer = Objects.requireNonNull(issuer, "issuer");
        }

        /**
         * Injects the time source lease expiry is evaluated against.
         *
         * @param clock the time source
         * @return this builder
         */
        public Builder clock(InstantSource clock) {
            this.clock = Objects.requireNonNull(clock, "clock");
            return this;
        }

        /**
         * Uses a specific schema registry.
         *
         * @param schemas the registry
         * @return this builder
         */
        public Builder schemaRegistry(SchemaRegistry schemas) {
            this.schemas = Objects.requireNonNull(schemas, "schemas");
            return this;
        }

        /**
         * Starts a background sweeper so expiry and reappearance events fire even
         * with no space traffic. Deterministic tests omit this and rely on lazy
         * expiry at each operation.
         *
         * @param interval the sweep period
         * @return this builder
         */
        public Builder sweepEvery(Duration interval) {
            this.sweepInterval = Objects.requireNonNull(interval, "interval");
            return this;
        }

        /**
         * The time source the sweeper runs on (srad/agentspaces-event-loop.md EL-T-12); by default the clock when it
         * is one, else the system's.
         *
         * @param time the time source
         * @return this builder
         */
        public Builder time(ai.badmonkey.agentspaces.common.time.TimeSource time) {
            this.time = Objects.requireNonNull(time, "time");
            return this;
        }

        /**
         * Whether the first renewal of this space that asks for less than its lease has left is logged at INFO
         * ({@code agentspaces.space.renew-shortening-log}, RE-H-01); on by default. The count runs either way.
         *
         * @param log whether to log it
         * @return this builder
         */
        public Builder renewShorteningLog(boolean log) {
            this.renewShorteningLog = log;
            return this;
        }

        /** Builds the space. */
        public LocalSpace build() {
            return new LocalSpace(this);
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public java.util.Optional<AgentId> writer() {
        return java.util.Optional.of(issuer);
    }

    @Override
    public SpaceId id() {
        return id;
    }

    @Override
    public <T> EntryHandle write(T entry, Lease lease) {
        return write(entry, lease, Map.of());
    }

    @Override
    public <T> EntryHandle write(T entry, Lease lease, Map<String, String> tags) {
        return writeAs(issuer, false, entry, lease, tags);
    }

    /**
     * This space seen as another agent of the same peer (SPEC §4.2, v0.1.13): a
     * view whose writes, and results, are attributed to {@code actor}, and
     * reported {@code AGENT_ATTESTED} by {@link #readAllIssued} when the actor
     * has a key of its own. A local space signs nothing, so this is the
     * replicated space's attribution model without the cryptography, which is
     * what tests and the REPL need to see the same model.
     *
     * @param actor an agent of this space's peer
     * @return the view
     */
    public Space as(ai.badmonkey.agentspaces.api.spi.AgentIdentity actor) {
        Objects.requireNonNull(actor, "actor");
        if (!actor.id().peer().equals(issuer.peer())) {
            throw new IllegalArgumentException(actor.id().encoded() + " is not an agent of "
                    + issuer.peer().display());
        }
        return new View(actor.id(), actor.isSubordinate());
    }

    private <T> EntryHandle writeAs(AgentId writer, boolean attested, T entry, Lease lease,
                                    Map<String, String> tags) {
        Objects.requireNonNull(entry, "entry");
        Objects.requireNonNull(lease, "lease");
        Objects.requireNonNull(tags, "tags");
        ensureOpen();
        List<Runnable> events;
        EntryId entryId = EntryId.newId();
        synchronized (lock) {
            events = sweepLocked();
            long now = nowMillis();
            Stored stored = new Stored();
            stored.entry = entry;
            stored.writeExpiryMillis = now + lease.duration().toMillis();
            stored.record = buildRecord(entryId, entry, stored.writeExpiryMillis, tags, writer);
            stored.attested = attested;
            indexPut(entryId, stored);
            events.add(eventsFor(SpaceEvent.Kind.WRITTEN, entryId, stored));
            signalChangeLocked();
        }
        deliver(events);
        return new Handle(entryId, writer);
    }

    @Override
    public <T> Optional<T> read(Template<T> template) {
        Objects.requireNonNull(template, "template");
        ensureOpen();
        List<Runnable> events;
        Optional<T> result;
        synchronized (lock) {
            events = sweepLocked();
            result = firstMatchLocked(template);
        }
        deliver(events);
        return result;
    }

    @Override
    public <T> Optional<T> read(Template<T> template, Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        return awaiting(timeout, () -> read(template));
    }

    @Override
    public <T> List<T> readAll(Template<T> template, int limit) {
        Objects.requireNonNull(template, "template");
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive: " + limit);
        }
        ensureOpen();
        List<Runnable> events;
        List<T> results = new ArrayList<>();
        synchronized (lock) {
            events = sweepLocked();
            outer:
            for (LinkedHashMap<EntryId, Stored> bucket : bucketsFor(template.type())) {
                for (Stored stored : bucket.values()) {
                    if (stored.state == State.AVAILABLE && matchesLocked(template, stored)) {
                        results.add(template.type().cast(stored.entry));
                        if (results.size() == limit) {
                            break outer;
                        }
                    }
                }
            }
        }
        deliver(events);
        return results;
    }

    @Override
    public <T> List<Issued<T>> readAllIssued(Template<T> template, int limit) {
        Objects.requireNonNull(template, "template");
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive: " + limit);
        }
        ensureOpen();
        List<Runnable> events;
        List<Issued<T>> results = new ArrayList<>();
        synchronized (lock) {
            events = sweepLocked();
            outer:
            for (LinkedHashMap<EntryId, Stored> bucket : bucketsFor(template.type())) {
                for (Stored stored : bucket.values()) {
                    if (stored.state == State.AVAILABLE && matchesLocked(template, stored)) {
                        results.add(new Issued<>(template.type().cast(stored.entry),
                                stored.record.issuer(), stored.attested
                                        ? Attestation.AGENT_ATTESTED : Attestation.PEER_ASSERTED));
                        if (results.size() == limit) {
                            break outer;
                        }
                    }
                }
            }
        }
        deliver(events);
        return results;
    }

    @Override
    public <T> Optional<TakenEntry<T>> take(Template<T> template, Lease takeLease, Duration timeout) {
        Objects.requireNonNull(template, "template");
        Objects.requireNonNull(takeLease, "takeLease");
        Objects.requireNonNull(timeout, "timeout");
        return awaiting(timeout, () -> tryTake(template, takeLease));
    }

    @Override
    public void complete(TakenEntry<?> taken) {
        completeInternal(taken, null, null, Map.of());
    }

    @Override
    public <R> EntryHandle complete(TakenEntry<?> taken, R result, Lease resultLease) {
        return complete(taken, result, resultLease, Map.of());
    }

    @Override
    public <R> EntryHandle complete(TakenEntry<?> taken, R result, Lease resultLease,
                                    Map<String, String> tags) {
        Objects.requireNonNull(result, "result");
        Objects.requireNonNull(resultLease, "resultLease");
        Objects.requireNonNull(tags, "tags");
        return completeInternal(taken, result, resultLease, tags);
    }

    @Override
    public <T> List<Entry<T>> readAllEntries(Template<T> template, int limit) {
        Objects.requireNonNull(template, "template");
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive: " + limit);
        }
        ensureOpen();
        List<Runnable> events;
        List<Entry<T>> results = new ArrayList<>();
        synchronized (lock) {
            events = sweepLocked();
            outer:
            for (LinkedHashMap<EntryId, Stored> bucket : bucketsFor(template.type())) {
                for (Stored stored : bucket.values()) {
                    if (stored.state == State.AVAILABLE && matchesLocked(template, stored)) {
                        results.add(viewOf(stored, template.type().cast(stored.entry)));
                        if (results.size() == limit) {
                            break outer;
                        }
                    }
                }
            }
        }
        deliver(events);
        return results;
    }

    /**
     * The schema name this space registers {@code type} under (issue #16 §10.2),
     * registering it first when it is new. A binder compares this with the name
     * it advertises so a card and the space agree on a type's name.
     *
     * @param type the entry type
     * @return the schema name
     */
    public String schemaNameOf(Class<?> type) {
        Objects.requireNonNull(type, "type");
        return schemas.register(type);
    }

    @Override
    public <T> Subscription notify(Template<T> template, SpaceListener<T> listener, Lease lease) {
        Objects.requireNonNull(template, "template");
        Objects.requireNonNull(listener, "listener");
        Objects.requireNonNull(lease, "lease");
        ensureOpen();
        Sub sub = new Sub(template, listener, localMillis() + lease.duration().toMillis());
        subscriptions.add(sub);
        return sub;
    }

    /** Runs an expiry sweep now, firing any pending expiry or reappearance events. */
    public void sweepNow() {
        List<Runnable> events;
        synchronized (lock) {
            events = sweepLocked();
        }
        deliver(events);
    }

    /**
     * Arms the next sweep: a one-shot on the time source, re-armed from the end of each sweep on its virtual thread
     * (srad/agentspaces-event-loop.md EL-T-12, EL-T-13), in place of a platform thread at a fixed rate.
     */
    private void armSweep() {
        if (closed) {
            return;
        }
        sweeper = sweepTime.schedule(sweepInterval, () -> {
            try {
                sweepNow();
            } catch (RuntimeException e) {
                // a sweep that failed is retried at the next interval
            } finally {
                armSweep();
            }
        });
    }

    /** Closes the space: pending blockers wake and further operations fail. */
    @Override
    public void close() {
        closed = true;
        ai.badmonkey.agentspaces.common.time.TimeSource.Scheduled pending = sweeper;
        if (pending != null) {
            pending.cancel();
        }
        synchronized (lock) {
            signalChangeLocked();
        }
    }

    // ---------------------------------------------------------------- internals

    private <T> Optional<TakenEntry<T>> tryTake(Template<T> template, Lease takeLease) {
        ensureOpen();
        List<Runnable> events;
        Optional<TakenEntry<T>> result = Optional.empty();
        synchronized (lock) {
            events = sweepLocked();
            outer:
            for (LinkedHashMap<EntryId, Stored> bucket : bucketsFor(template.type())) {
                for (Map.Entry<EntryId, Stored> candidate : bucket.entrySet()) {
                    Stored stored = candidate.getValue();
                    if (stored.state != State.AVAILABLE || !matchesLocked(template, stored)) {
                        continue;
                    }
                    long now = nowMillis();
                    stored.state = State.TAKEN;
                    stored.takeExpiryMillis = now + takeLease.duration().toMillis();
                    stored.takeToken++;
                    stored.record = stored.record.withLease(
                            new LeaseInfo(issuer, stored.takeExpiryMillis, LeaseKind.TAKE));
                    T value = template.type().cast(stored.entry);
                    events.add(eventsFor(SpaceEvent.Kind.TAKEN, candidate.getKey(), stored));
                    result = Optional.of(new Taken<>(candidate.getKey(), value, stored.takeToken));
                    break outer;
                }
            }
        }
        deliver(events);
        return result;
    }

    private EntryHandle completeInternal(TakenEntry<?> taken, Object result, Lease resultLease,
                                         Map<String, String> tags) {
        return completeInternal(taken, result, resultLease, tags, null, false);
    }

    private EntryHandle completeInternal(TakenEntry<?> taken, Object result, Lease resultLease,
                                         Map<String, String> tags, AgentId resultWriter,
                                         boolean resultAttested) {
        Objects.requireNonNull(taken, "taken");
        ensureOpen();
        if (!(taken instanceof Taken<?> t)) {
            throw new IllegalArgumentException("foreign TakenEntry implementation: " + taken.getClass());
        }
        List<Runnable> events;
        EntryHandle resultHandle = null;
        EntryId resultId = result == null ? null : EntryId.newId();
        synchronized (lock) {
            events = sweepLocked();
            Stored stored = entries.get(t.entryId());
            if (stored == null || stored.state != State.TAKEN || stored.takeToken != t.token) {
                throw new LeaseExpiredException(
                        "take lease lapsed for entry " + t.entryId() + "; the entry reappeared or expired");
            }
            // Spec §7.2 atomicity: build and validate the result record BEFORE the
            // completion commits, so a result that cannot be serialized never leaves
            // the entry completed-but-resultless.
            Stored written = null;
            if (result != null) {
                long now = nowMillis();
                written = new Stored();
                written.entry = result;
                written.writeExpiryMillis = now + resultLease.duration().toMillis();
                written.record = buildRecord(resultId, result, written.writeExpiryMillis,
                        tags, resultWriter == null ? issuer : resultWriter);
                written.attested = resultAttested;
            }
            indexRemove(t.entryId(), stored);
            events.add(eventsFor(SpaceEvent.Kind.COMPLETED, t.entryId(), stored));
            if (written != null) {
                indexPut(resultId, written);
                events.add(eventsFor(SpaceEvent.Kind.WRITTEN, resultId, written));
                resultHandle = new Handle(resultId, written.record.issuer());
            }
            signalChangeLocked();
        }
        deliver(events);
        return resultHandle;
    }

    /**
     * Polls an operation until it yields a value or the timeout elapses, waking on every space mutation. The wait is
     * reckoned and parked on the space's time source ({@link ai.badmonkey.agentspaces.common.time.Waits#base}: the
     * clock itself when it is a time source, a virtual one among them, else the system's), never longer than
     * {@link #AWAIT_LOOK_MILLIS} between looks, so a lease the injected clock lapses is seen (group W, EL-C-10).
     */
    private <V> Optional<V> awaiting(Duration timeout, java.util.function.Supplier<Optional<V>> poll) {
        ai.badmonkey.agentspaces.common.time.TimeSource waits = ai.badmonkey.agentspaces.common.time.Waits.base(clock);
        long deadline = ai.badmonkey.agentspaces.common.time.Waits.deadline(waits, timeout);
        while (true) {
            Optional<V> result = poll.get();
            if (result.isPresent()) {
                return result;
            }
            long now = waits.monoNanos();
            if (deadline - now <= 0 || closed) {
                return Optional.empty();
            }
            synchronized (lock) {
                try {
                    waits.waitOn(lock, Math.min(deadline, now + AWAIT_LOOK_MILLIS * 1_000_000L));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return Optional.empty();
                }
            }
        }
    }

    /** The longest a waiting take or read goes between looks: a lease lapses on the clock, which signals nobody. */
    static final long AWAIT_LOOK_MILLIS = 50;

    /**
     * Signals a change to the waiters and the change listeners (DW-2): every place that woke the waiters does so here.
     * Callers hold {@link #lock}.
     */
    private void signalChangeLocked() {
        changes.incrementAndGet();
        lock.notifyAll();
        for (Runnable listener : changeListeners) {
            try {
                listener.run();
            } catch (RuntimeException e) {
                // a listener only wakes; one that throws does not stop the others
            }
        }
    }

    @Override
    public long changeSignals() {
        return changes.get();
    }

    @Override
    public void awaitChange(long seen, Duration max) throws InterruptedException {
        ai.badmonkey.agentspaces.common.time.TimeSource waits = ai.badmonkey.agentspaces.common.time.Waits.base(clock);
        long deadline = ai.badmonkey.agentspaces.common.time.Waits.deadline(waits, max);
        synchronized (lock) {
            while (changes.get() == seen && !closed && deadline - waits.monoNanos() > 0) {
                waits.waitOn(lock, deadline);
            }
        }
    }

    @Override
    public AutoCloseable onChangeSignal(Runnable listener) {
        java.util.Objects.requireNonNull(listener, "listener");
        changeListeners.add(listener);
        return () -> changeListeners.remove(listener);
    }

    private <T> Optional<T> firstMatchLocked(Template<T> template) {
        for (LinkedHashMap<EntryId, Stored> bucket : bucketsFor(template.type())) {
            for (Stored stored : bucket.values()) {
                if (stored.state == State.AVAILABLE && matchesLocked(template, stored)) {
                    return Optional.of(template.type().cast(stored.entry));
                }
            }
        }
        return Optional.empty();
    }

    /**
     * The buckets a template's type accepts (PERF1 phase 2). The common case,
     * a concrete entry class, resolves to its single bucket; a supertype or
     * interface template walks the few distinct entry classes in use and
     * collects the assignable ones. Callers hold the lock.
     */
    private List<LinkedHashMap<EntryId, Stored>> bucketsFor(Class<?> type) {
        LinkedHashMap<EntryId, Stored> exact = buckets.get(type);
        List<LinkedHashMap<EntryId, Stored>> assignable = null;
        for (Map.Entry<Class<?>, LinkedHashMap<EntryId, Stored>> bucket : buckets.entrySet()) {
            if (bucket.getKey() != type && type.isAssignableFrom(bucket.getKey())) {
                if (assignable == null) {
                    assignable = new ArrayList<>(2);
                    if (exact != null) {
                        assignable.add(exact);
                    }
                }
                assignable.add(bucket.getValue());
            }
        }
        if (assignable != null) {
            return assignable;
        }
        return exact == null ? List.of() : List.of(exact);
    }

    /** Adds an entry to both the id map and its type bucket. Callers hold the lock. */
    private void indexPut(EntryId entryId, Stored stored) {
        entries.put(entryId, stored);
        buckets.computeIfAbsent(stored.entry.getClass(), type -> new LinkedHashMap<>())
                .put(entryId, stored);
    }

    /** Removes an entry from both maps, dropping emptied buckets. Callers hold the lock. */
    private void indexRemove(EntryId entryId, Stored stored) {
        entries.remove(entryId);
        bucketRemove(entryId, stored);
    }

    private void bucketRemove(EntryId entryId, Stored stored) {
        Class<?> type = stored.entry.getClass();
        LinkedHashMap<EntryId, Stored> bucket = buckets.get(type);
        if (bucket != null && bucket.remove(entryId) != null && bucket.isEmpty()) {
            buckets.remove(type);
        }
    }

    /** Applies lease lapses. Callers hold the lock; returned events run after release. */
    private List<Runnable> sweepLocked() {
        List<Runnable> events = new ArrayList<>();
        long now = nowMillis();
        var iterator = entries.entrySet().iterator();
        while (iterator.hasNext()) {
            var e = iterator.next();
            Stored stored = e.getValue();
            if (stored.writeExpiryMillis <= now) {
                iterator.remove();
                bucketRemove(e.getKey(), stored);
                events.add(eventsFor(SpaceEvent.Kind.EXPIRED, e.getKey(), stored));
                continue;
            }
            if (stored.state == State.TAKEN && stored.takeExpiryMillis <= now) {
                stored.state = State.AVAILABLE;
                stored.takeToken++;
                // The write lease is the writer's (buildRecord), not this peer's, so the restored lease names
                // the record's issuer: a View's agent stays the holder after a take lapses.
                stored.record = stored.record.withLease(
                        new LeaseInfo(stored.record.issuer(), stored.writeExpiryMillis, LeaseKind.WRITE));
                events.add(eventsFor(SpaceEvent.Kind.REAPPEARED, e.getKey(), stored));
            }
        }
        long local = localMillis();
        subscriptions.removeIf(sub -> sub.expiryMillis <= local || sub.closedFlag);
        if (!events.isEmpty()) {
            signalChangeLocked();
        }
        return events;
    }

    private EntryRecord buildRecord(EntryId entryId, Object entry, long writeExpiry,
                                    Map<String, String> tags) {
        return buildRecord(entryId, entry, writeExpiry, tags, issuer);
    }

    private EntryRecord buildRecord(EntryId entryId, Object entry, long writeExpiry,
                                    Map<String, String> tags, AgentId writer) {
        String schemaName = schemas.register(entry.getClass());
        byte[] payload = codec.toBytes(entry);
        String payloadRef = null;
        if (payload.length > EntryRecord.INLINE_PAYLOAD_LIMIT) {
            payloadRef = Multibase.base58btc(Digests.sha256(payload));
            payload = null;
        }
        return new EntryRecord(entryId, id, schemaName, payload, payloadRef, writer,
                hlc.now(), new LeaseInfo(writer, writeExpiry, LeaseKind.WRITE), tags, null);
    }

    /**
     * Issue #16 §9.2 matching order: type (the bucket), then the record's tags,
     * then the fields. A local space holds the value undecoded, so "before
     * decode" costs nothing here, but the order is the replicated space's.
     * Callers hold the lock.
     */
    private static boolean matchesLocked(Template<?> template, Stored stored) {
        return template.matchesTags(stored.record.tags()) && template.matchesType(stored.record.type())
                && template.matches(stored.entry);
    }

    /** The metadata view over a stored entry (issue #16 §9.2). Callers hold the lock. */
    private static <T> EntryView<T> viewOf(Stored stored, T value) {
        return EntryView.of(stored.record, value, stored.attested
                ? Attestation.AGENT_ATTESTED : Attestation.PEER_ASSERTED);
    }

    /**
     * Queues an event for the subscriptions whose template accepts the entry's
     * type, tags and fields. The record and value are captured now, under the
     * lock, so the view a listener sees is the one in force when the event
     * happened; the event's issuer is the record's writer, which is the view's
     * agent for a write made through {@link #as}.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Runnable eventsFor(SpaceEvent.Kind kind, EntryId entryId, Stored stored) {
        Object entry = stored.entry;
        EntryRecord record = stored.record;
        EntryView<Object> details = viewOf(stored, entry);
        return () -> {
            for (Sub sub : subscriptions) {
                if (!sub.closedFlag && sub.template.matchesTags(record.tags()) && sub.template.matchesType(record.type())
                        && sub.template.matches(entry)) {
                    sub.listener.onEvent(new SpaceEvent(kind, entryId, entry, record.issuer(),
                            record.issuer(), details));
                }
            }
        };
    }

    private static void deliver(List<Runnable> events) {
        for (Runnable event : events) {
            event.run();
        }
    }

    private long nowMillis() {
        return clock.instant().toEpochMilli();
    }

    /**
     * The local now a subscription's lease is judged on (srad/agentspaces-event-loop.md EL-C-04, §10.18): the clock's
     * monotonic base, as the renewer's, so a wall step or a suspend lapses no subscription; a plain clock a test moves
     * by hand keeps its subscriptions on that clock.
     */
    private long localMillis() {
        return localTime.monoMillis();
    }

    /**
     * Renewals of a write or take lease in this space that asked for less than the lease had left (RE-H-01). A
     * renewal never shortens a lease (SPEC §7.2, RE-H-02), so each of these kept the longer lease.
     */
    public long renewShortenings() {
        return renewShortenings.get();
    }

    /** Counts a renewal that asked for less than its lease had left, and logs the first one of this space. */
    private void noteShortening(EntryId entryId, String lease, long currentExpiryMillis, long requestedExpiryMillis) {
        if (requestedExpiryMillis >= currentExpiryMillis) {
            return;
        }
        renewShortenings.incrementAndGet();
        if (renewShorteningLog && renewShorteningLogged.compareAndSet(false, true)) {
            LOG.log(System.Logger.Level.INFO, "space " + name + ": the " + lease + " lease of entry " + entryId.value()
                    + " was renewed for " + (currentExpiryMillis - requestedExpiryMillis)
                    + " ms less than it had left, and kept the longer lease: a renewal never shortens a lease"
                    + " (SPEC §7.2). Cancel and write again for a shorter one. Logged once for this space;"
                    + " renewShortenings() counts every such renewal.");
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new SpaceClosedException("space '" + name + "' is closed");
        }
    }

    /**
     * A handle on an entry held by another agent than its writer, for the tests of the issuer rule (SPEC §7.2): the
     * API hands a handle only to the entry's writer, so no caller reaches this case any other way.
     */
    EntryHandle handleAs(EntryId entryId, AgentId agent) { // package-private for tests
        return new Handle(entryId, agent);
    }

    private final class Handle implements EntryHandle {
        private final EntryId entryId;
        /** The agent that wrote the entry, the only one that renews it (SPEC §7.2). */
        private final AgentId writer;

        private Handle(EntryId entryId, AgentId writer) {
            this.entryId = entryId;
            this.writer = writer;
        }

        @Override
        public EntryId entryId() {
            return entryId;
        }

        @Override
        public void renew(Duration extension) {
            Objects.requireNonNull(extension, "extension");
            ensureOpen();
            List<Runnable> events;
            synchronized (lock) {
                events = sweepLocked();
                Stored stored = entries.get(entryId);
                long now = nowMillis();
                // SPEC §7.2 (RE-H-02): renewal needs the entry present and unexpired, and its issuer acting, as on a
                // replicated space. The sweep above drops a lapsed entry, so the second check guards the same rule.
                if (stored == null || stored.writeExpiryMillis <= now) {
                    throw new LeaseExpiredException("write lease lapsed for entry " + entryId);
                }
                if (!stored.record.issuer().equals(writer)) {
                    throw new IllegalStateException(writer.encoded() + " cannot act on "
                            + stored.record.issuer().encoded() + "'s entry " + entryId);
                }
                noteShortening(entryId, "write", stored.writeExpiryMillis,
                        SpaceStateCrdt.renewedExpiry(Long.MIN_VALUE, now, extension.toMillis()));
                stored.writeExpiryMillis = SpaceStateCrdt.renewedExpiry(stored.writeExpiryMillis, now,
                        extension.toMillis());
                // The record carries the lease readers see (readAllEntries, events), so it moves with the expiry;
                // a taken entry's record shows the take hold instead, and the sweep restores the write lease
                // from writeExpiryMillis when that hold lapses.
                if (stored.state == State.AVAILABLE) {
                    stored.record = stored.record.withLease(new LeaseInfo(stored.record.issuer(),
                            stored.writeExpiryMillis, LeaseKind.WRITE));
                }
            }
            deliver(events);
        }

        @Override
        public void cancel() {
            List<Runnable> events;
            synchronized (lock) {
                events = sweepLocked();
                Stored stored = entries.get(entryId);
                if (stored != null) {
                    indexRemove(entryId, stored);
                    events.add(eventsFor(SpaceEvent.Kind.EXPIRED, entryId, stored));
                    signalChangeLocked();
                }
            }
            deliver(events);
        }
    }

    private final class Taken<T> implements TakenEntry<T> {
        private final EntryId entryId;
        private final T value;
        private final long token;

        private Taken(EntryId entryId, T value, long token) {
            this.entryId = entryId;
            this.value = value;
            this.token = token;
        }

        @Override
        public T entry() {
            return value;
        }

        @Override
        public EntryId entryId() {
            return entryId;
        }

        @Override
        public void renew(Duration extension) {
            Objects.requireNonNull(extension, "extension");
            ensureOpen();
            List<Runnable> events;
            synchronized (lock) {
                events = sweepLocked();
                Stored stored = entries.get(entryId);
                if (stored == null || stored.state != State.TAKEN || stored.takeToken != token) {
                    throw new LeaseExpiredException("take lease lapsed for entry " + entryId);
                }
                long now = nowMillis();
                noteShortening(entryId, "take", stored.takeExpiryMillis,
                        SpaceStateCrdt.renewedExpiry(Long.MIN_VALUE, now, extension.toMillis()));
                stored.takeExpiryMillis = SpaceStateCrdt.renewedExpiry(stored.takeExpiryMillis, now,
                        extension.toMillis());
            }
            deliver(events);
        }
    }

    private final class Sub implements Subscription {
        private final Template<?> template;
        @SuppressWarnings("rawtypes")
        private final SpaceListener listener;
        private volatile long expiryMillis;
        private volatile boolean closedFlag;

        private Sub(Template<?> template, SpaceListener<?> listener, long expiryMillis) {
            this.template = template;
            this.listener = listener;
            this.expiryMillis = expiryMillis;
        }

        @Override
        public synchronized void renew(Duration extension) {
            // Synchronized so that two renewals at once cannot both read the old expiry and the shorter one write
            // last: the max rule promises the lease never shortens, and the read and the write must be one step.
            Objects.requireNonNull(extension, "extension");
            long now = localMillis();
            if (closedFlag || expiryMillis <= now) {
                throw new LeaseExpiredException("subscription lease lapsed");
            }
            // The same reading of renew as an entry's lease: at least the extension from now, never shorter.
            expiryMillis = SpaceStateCrdt.renewedExpiry(expiryMillis, now, extension.toMillis());
        }

        @Override
        public void close() {
            closedFlag = true;
            subscriptions.remove(this);
        }
    }

    /** The space as one agent of the same peer: its writes and results are that agent's. */
    private final class View implements Space {
        private final AgentId actor;
        private final boolean attested;

        private View(AgentId actor, boolean attested) {
            this.actor = actor;
            this.attested = attested;
        }

        @Override public String name() { return LocalSpace.this.name(); }
        @Override public SpaceId id() { return LocalSpace.this.id(); }
        @Override public java.util.Optional<AgentId> writer() { return java.util.Optional.of(actor); }
        @Override public <T> EntryHandle write(T entry, Lease lease) {
            return writeAs(actor, attested, entry, lease, Map.of());
        }
        @Override public <T> EntryHandle write(T entry, Lease lease, Map<String, String> tags) {
            return writeAs(actor, attested, entry, lease, tags);
        }
        @Override public <T> Optional<T> read(Template<T> template) { return LocalSpace.this.read(template); }
        @Override public <T> Optional<T> read(Template<T> template, Duration timeout) {
            return LocalSpace.this.read(template, timeout);
        }
        @Override public <T> List<T> readAll(Template<T> template, int limit) {
            return LocalSpace.this.readAll(template, limit);
        }
        @Override public <T> List<Issued<T>> readAllIssued(Template<T> template, int limit) {
            return LocalSpace.this.readAllIssued(template, limit);
        }
        @Override public <T> Optional<TakenEntry<T>> take(Template<T> template, Lease takeLease,
                                                          Duration timeout) {
            return LocalSpace.this.take(template, takeLease, timeout);
        }
        @Override public <T> List<Entry<T>> readAllEntries(Template<T> template, int limit) {
            return LocalSpace.this.readAllEntries(template, limit);
        }
        @Override public void complete(TakenEntry<?> taken) { LocalSpace.this.complete(taken); }
        @Override public <R> EntryHandle complete(TakenEntry<?> taken, R result, Lease resultLease) {
            return complete(taken, result, resultLease, Map.of());
        }
        @Override public <R> EntryHandle complete(TakenEntry<?> taken, R result, Lease resultLease,
                                                  Map<String, String> tags) {
            Objects.requireNonNull(result, "result");
            Objects.requireNonNull(resultLease, "resultLease");
            Objects.requireNonNull(tags, "tags");
            return completeInternal(taken, result, resultLease, tags, actor, attested);
        }
        @Override public <T> Subscription notify(Template<T> template, SpaceListener<T> listener,
                                                 Lease lease) {
            return LocalSpace.this.notify(template, listener, lease);
        }
    }
}
